"""
Amazon Listing 模块
提供 Listing 抓取、上架、修改、删除路由
以及数据库同步、分页查询（供前端展示）

上架/修改时图片 URL 通过阿里云 OSS 上传服务获取
"""
import json
import os
import time

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient
from services.oss_uploader import upload_image_for_listing
from services.mysql_service import get_db_connection

amazon_listing_bp = Blueprint('amazon_listing', __name__, url_prefix='/api')

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


def _get_client(marketplace_id=None, region=None):
    """获取 Amazon SP-API 客户端实例"""
    return AmazonSpApiClient(
        marketplace_id=marketplace_id or MARKETPLACE_ID,
        region=region
    )


# ========================
# 路由：数据库查询（前端展示用）
# ========================

@amazon_listing_bp.route('/amazon/listings', methods=['GET'])
@login_required
def get_listings():
    """
    从数据库分页查询 Listing 列表（默认走数据库，减少 SP-API 请求）
    查询参数:
        sku           - 按 SKU 精确筛选
        asin          - 按 ASIN 筛选
        product_type  - 按商品类型筛选
        status        - 按状态筛选（如 DISCOVERABLE）
        parent_sku    - 按父 SKU 筛选
        keyword       - 按标题/品牌模糊搜索
        page          - 页码，默认 1
        page_size     - 每页数量，默认 20
    """
    try:
        sku = request.args.get('sku', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        product_type = request.args.get('product_type', '').strip() or None
        status = request.args.get('status', '').strip() or None
        parent_sku = request.args.get('parent_sku', '').strip() or None
        keyword = request.args.get('keyword', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_listings_from_db(
            sku=sku, asin=asin, product_type=product_type,
            status=status, parent_sku=parent_sku, keyword=keyword,
            page=page, page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['GET'])
@login_required
def get_listing_detail(sku):
    """
    从数据库查询单个 Listing 详情（含子表数据：图片、五点、issues、报价）
    如需强制刷新，可调用同步接口
    """
    try:
        result = _get_listing_detail_from_db(sku=sku, marketplace_id=MARKETPLACE_ID)
        if not result:
            return jsonify({"status": "error", "message": "Listing 不存在"}), 404

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing DB] 详情查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/sync/listings', methods=['POST'])
@login_required
def sync_listings():
    """
    手动触发 Listing 同步（从 SP-API 写入数据库）
    请求体可选:
        included_data  - 额外包含数据，默认 ["summaries", "attributes", "issues"]
        page_size      - 每页拉取数量，默认 20
    """
    try:
        data = request.get_json() or {}
        included_data = data.get('included_data', ["summaries", "attributes", "issues"])
        page_size = data.get('page_size', 20)

        result = _sync_listings(
            included_data=included_data,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条 Listing",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing Sync] 同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========================
# 路由：SP-API 直连操作
# ========================

@amazon_listing_bp.route('/amazon/listings/spapi/<sku>', methods=['GET'])
@login_required
def get_listing_detail_spapi(sku):
    """
    实时从 SP-API 抓取单个 Listing 详情（不走数据库缓存）
    查询参数:
        included_data  - 额外包含数据
    """
    try:
        included_data = request.args.get('included_data', '').strip() or None
        included_data_list = None
        if included_data:
            included_data_list = [x.strip() for x in included_data.split(',') if x.strip()]

        client = _get_client()
        result = client.get_listings_item(sku=sku, included_data=included_data_list)

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing SP-API] 实时抓取异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings', methods=['POST'])
@login_required
def create_listing():
    """
    上架 Listing（创建新商品）
    请求体 JSON:
        sku, product_type, attributes, requirements, condition_type
    """
    try:
        data = request.get_json() or {}
        sku = data.get('sku', '').strip()
        product_type = data.get('product_type', '').strip()
        attributes = data.get('attributes')
        requirements = data.get('requirements', 'LISTING')
        condition_type = data.get('condition_type')

        if not sku:
            return jsonify({"status": "error", "message": "缺少必填字段: sku"}), 400
        if not product_type:
            return jsonify({"status": "error", "message": "缺少必填字段: product_type"}), 400
        if not attributes or not isinstance(attributes, dict):
            return jsonify({"status": "error", "message": "缺少必填字段: attributes（必须为对象）"}), 400

        client = _get_client()
        result = client.put_listings_item(
            sku=sku,
            product_type=product_type,
            attributes=attributes,
            requirements=requirements,
            condition_type=condition_type
        )

        return jsonify({
            "status": "success",
            "message": "Listing 上架成功",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing] 上架异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['PUT'])
@login_required
def update_listing(sku):
    """
    修改 Listing（完全覆盖式更新）
    请求体 JSON:
        product_type, attributes, requirements, condition_type
    """
    try:
        data = request.get_json() or {}
        product_type = data.get('product_type', '').strip()
        attributes = data.get('attributes')
        requirements = data.get('requirements', 'LISTING')
        condition_type = data.get('condition_type')

        if not product_type:
            return jsonify({"status": "error", "message": "缺少必填字段: product_type"}), 400
        if not attributes or not isinstance(attributes, dict):
            return jsonify({"status": "error", "message": "缺少必填字段: attributes（必须为对象）"}), 400

        client = _get_client()
        result = client.put_listings_item(
            sku=sku,
            product_type=product_type,
            attributes=attributes,
            requirements=requirements,
            condition_type=condition_type
        )

        return jsonify({
            "status": "success",
            "message": "Listing 更新成功",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing] 更新异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['PATCH'])
@login_required
def patch_listing(sku):
    """
    部分更新 Listing（JSON Patch）
    请求体 JSON:
        patches, product_type
    """
    try:
        data = request.get_json() or {}
        patches = data.get('patches')
        product_type = data.get('product_type')

        if not patches or not isinstance(patches, list):
            return jsonify({"status": "error", "message": "缺少必填字段: patches（必须为数组）"}), 400

        client = _get_client()
        result = client.patch_listings_item(
            sku=sku,
            patches=patches,
            product_type=product_type
        )

        return jsonify({
            "status": "success",
            "message": "Listing 部分更新成功",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing] 部分更新异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['DELETE'])
@login_required
def delete_listing(sku):
    """
    删除 Listing
    查询参数:
        marketplace_ids - 可选，逗号分隔
    """
    try:
        marketplace_ids = request.args.get('marketplace_ids', '').strip() or None
        marketplace_ids_list = None
        if marketplace_ids:
            marketplace_ids_list = [x.strip() for x in marketplace_ids.split(',') if x.strip()]

        client = _get_client()
        result = client.delete_listings_item(sku=sku, marketplace_ids=marketplace_ids_list)

        return jsonify({
            "status": "success",
            "message": "Listing 删除成功",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing] 删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/upload-image', methods=['POST'])
@login_required
def upload_listing_image():
    """
    上传 Listing 图片到阿里云 OSS，返回可用于亚马逊 SP-API 的 HTTPS URL
    请求方式: multipart/form-data
    字段: image - 图片文件
    """
    try:
        if 'image' not in request.files:
            return jsonify({"status": "error", "message": "缺少文件字段: image"}), 400

        file = request.files['image']
        if file.filename == '':
            return jsonify({"status": "error", "message": "未选择文件"}), 400

        result = upload_image_for_listing(file, filename=file.filename)

        if not result.get('success'):
            return jsonify({
                "status": "error",
                "message": result.get('error', '上传失败')
            }), 500

        return jsonify({
            "status": "success",
            "message": "图片上传成功",
            "data": {
                "url": result['url'],
                "oss_key": result['oss_key'],
                "filename": result['filename']
            }
        })

    except Exception as e:
        print(f"[Amazon Listing] 图片上传异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/delete-image', methods=['DELETE'])
@login_required
def delete_listing_image():
    """
    删除 OSS 上的 Listing 图片（前端点击删除时立即调用，防止冗余）
    请求方式: application/json
    字段（二选一）:
        oss_key - OSS 对象键，如 amazon/listing/images/20260509/abc123.jpg
        url     - 完整 HTTPS URL
    """
    try:
        data = request.get_json() or {}
        oss_key = data.get('oss_key', '').strip()
        url = data.get('url', '').strip()

        if not oss_key and not url:
            return jsonify({"status": "error", "message": "缺少参数: 请提供 oss_key 或 url"}), 400

        from services.oss_uploader import delete_object, delete_object_by_url

        if oss_key:
            result = delete_object(oss_key)
        else:
            result = delete_object_by_url(url)

        if not result.get('success'):
            return jsonify({
                "status": "error",
                "message": result.get('error', '删除失败')
            }), 500

        return jsonify({
            "status": "success",
            "message": "图片删除成功"
        })

    except Exception as e:
        print(f"[Amazon Listing] 图片删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/cleanup-images', methods=['POST'])
@login_required
def cleanup_listing_images():
    """
    清理 OSS 中未被任何 Listing 引用的孤儿图片（兜底机制）
    请求体可选:
        dry_run - 默认 true，只统计不真正删除。设为 false 时执行真实删除
    """
    try:
        data = request.get_json() or {}
        dry_run = data.get('dry_run', True)

        # 允许传字符串 "false" 或布尔值 false
        if isinstance(dry_run, str):
            dry_run = dry_run.lower() not in ('false', '0', 'no', 'off')

        from services.oss_uploader import cleanup_orphan_listing_images

        result = cleanup_orphan_listing_images(dry_run=bool(dry_run))

        if not result.get('success'):
            return jsonify({
                "status": "error",
                "message": "清理过程发生异常",
                "data": result
            }), 500

        return jsonify({
            "status": "success",
            "message": f"扫描完成: 共扫描 {result['scanned']} 个文件，发现 {result['orphan']} 个孤儿文件" +
                       (f"，已删除 {result['deleted']} 个" if not dry_run else "（试运行，未删除）"),
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Listing] 清理异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========================
# 数据库同步与查询函数
# ========================

def _extract_first_lang_value(attr_list):
    """从属性数组中提取第一个带 language_tag 的 value"""
    if not attr_list or not isinstance(attr_list, list):
        return None
    for item in attr_list:
        if isinstance(item, dict) and 'value' in item:
            return item['value']
    return None


def _extract_first_plain_value(attr_list):
    """从属性数组中提取第一个纯 value（不带 language_tag）"""
    if not attr_list or not isinstance(attr_list, list):
        return None
    for item in attr_list:
        if isinstance(item, dict) and 'value' in item:
            return item['value']
    return None


def _extract_first_media_location(attr_list):
    """提取图片 locator 中的 media_location"""
    if not attr_list or not isinstance(attr_list, list):
        return None
    for item in attr_list:
        if isinstance(item, dict) and item.get('media_location'):
            return item['media_location']
    return None


def _parse_listing_item(item, marketplace_id, seller_id):
    """解析单个 Listing item，返回主表数据和子表数据"""
    sku = item.get('sku', '')
    summaries = item.get('summaries', [])
    attributes = item.get('attributes', {})
    issues = item.get('issues', [])

    # 取第一个 summary
    summary = summaries[0] if summaries and isinstance(summaries, list) else {}

    # 状态数组转字符串
    status_list = summary.get('status', []) or []
    status_str = ','.join(status_list) if isinstance(status_list, list) else str(status_list)

    # 图片
    main_image = summary.get('mainImage', {}) or {}

    # 从 attributes 提取常见字段
    brand = _extract_first_lang_value(attributes.get('brand'))
    item_name = _extract_first_lang_value(attributes.get('item_name'))
    product_description = _extract_first_lang_value(attributes.get('product_description'))
    manufacturer = _extract_first_lang_value(attributes.get('manufacturer'))
    country_of_origin = _extract_first_plain_value(attributes.get('country_of_origin'))
    number_of_items = _extract_first_plain_value(attributes.get('number_of_items'))
    variation_theme = None
    if attributes.get('variation_theme') and isinstance(attributes['variation_theme'], list):
        variation_theme = attributes['variation_theme'][0].get('name')

    # 变体关系
    parent_sku = None
    parentage_level = None
    child_relationship_type = None
    rel_list = attributes.get('child_parent_sku_relationship', [])
    if rel_list and isinstance(rel_list, list):
        rel = rel_list[0]
        parent_sku = rel.get('parent_sku')
        parentage_level = _extract_first_plain_value(attributes.get('parentage_level'))
        child_relationship_type = rel.get('child_relationship_type')

    # 价格
    list_price = None
    list_price_currency = None
    lp_list = attributes.get('list_price', [])
    if lp_list and isinstance(lp_list, list):
        list_price = lp_list[0].get('value')
        list_price_currency = lp_list[0].get('currency')

    # 主表数据
    main_row = {
        'marketplace_id': marketplace_id,
        'seller_id': seller_id,
        'sku': sku,
        'asin': summary.get('asin'),
        'product_type': summary.get('productType'),
        'condition_type': summary.get('conditionType'),
        'status': status_str,
        'fn_sku': summary.get('fnSku'),
        'item_name': item_name or summary.get('itemName'),
        'brand': brand,
        'created_date': _iso_to_datetime(summary.get('createdDate')),
        'last_updated_date': _iso_to_datetime(summary.get('lastUpdatedDate')),
        'main_image_url': main_image.get('link'),
        'main_image_height': main_image.get('height'),
        'main_image_width': main_image.get('width'),
        'list_price': list_price,
        'list_price_currency': list_price_currency,
        'number_of_items': number_of_items,
        'parent_sku': parent_sku,
        'parentage_level': parentage_level,
        'child_relationship_type': child_relationship_type,
        'variation_theme': variation_theme,
        'country_of_origin': country_of_origin,
        'manufacturer': manufacturer,
        'product_description': product_description,
        'attributes_json': json.dumps(attributes, ensure_ascii=False) if attributes else '{}',
        'issues_json': json.dumps(issues, ensure_ascii=False) if issues else '[]',
    }

    # 子表：五点描述
    bullets = []
    bp_list = attributes.get('bullet_point', [])
    if bp_list and isinstance(bp_list, list):
        for idx, bp in enumerate(bp_list):
            if isinstance(bp, dict) and bp.get('value'):
                bullets.append({
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'sort_order': idx,
                    'content': bp['value'],
                    'language_tag': bp.get('language_tag', 'en_US')
                })

    # 子表：图片
    images = []
    img_keys = ['main_product_image_locator', 'other_product_image_locator_1',
                'other_product_image_locator_2', 'other_product_image_locator_3',
                'other_product_image_locator_4', 'other_product_image_locator_5']
    for sort_idx, key in enumerate(img_keys):
        if key in attributes:
            loc = _extract_first_media_location(attributes[key])
            if loc:
                img_type = 'main' if key == 'main_product_image_locator' else key.replace('other_product_image_locator_', 'other_')
                images.append({
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'image_type': img_type,
                    'media_location': loc,
                    'sort_order': sort_idx
                })

    # 子表：issues
    issue_rows = []
    if issues and isinstance(issues, list):
        for iss in issues:
            if isinstance(iss, dict):
                issue_rows.append({
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'issue_code': iss.get('code'),
                    'message': iss.get('message'),
                    'severity': iss.get('severity')
                })

    # 子表：报价
    offers = []
    offer_list = attributes.get('purchasable_offer', [])
    if offer_list and isinstance(offer_list, list):
        for off in offer_list:
            if isinstance(off, dict):
                our_price = None
                op = off.get('our_price', [])
                if op and isinstance(op, list) and op[0].get('schedule'):
                    sched = op[0]['schedule']
                    if sched and isinstance(sched, list):
                        our_price = sched[0].get('value_with_tax')

                offers.append({
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'currency': off.get('currency'),
                    'audience': off.get('audience', 'ALL'),
                    'our_price': our_price,
                    'start_at': _iso_to_datetime(off.get('start_at', {}).get('value')) if isinstance(off.get('start_at'), dict) else None,
                    'end_at': _iso_to_datetime(off.get('end_at', {}).get('value')) if isinstance(off.get('end_at'), dict) else None,
                })

    return main_row, bullets, images, issue_rows, offers


def _sync_listings(included_data=None, page_size=20):
    """
    同步 Listing 数据到数据库（自动处理分页）
    """
    client = _get_client()
    seller_id = client.seller_id or ''
    marketplace_id = client.marketplace_id

    all_items = []
    next_token = None
    page = 0
    total_fetched = 0

    try:
        while True:
            page += 1
            print(f"[Listing Sync] 正在获取第 {page} 页...")

            result = client.get_listings_items(
                included_data=included_data,
                page_size=page_size,
                next_token=next_token
            )

            payload = result if isinstance(result, dict) else {}
            items = payload.get('items', [])
            if not items:
                items = payload.get('payload', {}).get('items', [])

            all_items.extend(items)
            total_fetched += len(items)

            next_token = payload.get('nextToken') or payload.get('pagination', {}).get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        # 写入数据库
        synced_count, error = sync_listings_to_db(marketplace_id, seller_id, all_items)

        return {
            "synced_count": synced_count,
            "total_fetched": total_fetched,
            "error": error,
            "next_token": None
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": total_fetched,
            "error": str(e),
            "next_token": next_token
        }


def sync_listings_to_db(marketplace_id, seller_id, items):
    """
    批量同步 Listing 数据到数据库（含子表）
    """
    if not items:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for item in items:
                main_row, bullets, images, issues, offers = _parse_listing_item(
                    item, marketplace_id, seller_id
                )

                sku = main_row['sku']

                # 1. 写入/更新主表
                sql_main = """
                    INSERT INTO amazon_listings (
                        marketplace_id, seller_id, sku, asin, product_type, condition_type,
                        status, fn_sku, item_name, brand, created_date, last_updated_date,
                        main_image_url, main_image_height, main_image_width,
                        list_price, list_price_currency, number_of_items,
                        parent_sku, parentage_level, child_relationship_type, variation_theme,
                        country_of_origin, manufacturer, product_description,
                        attributes_json, issues_json, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        asin = VALUES(asin),
                        product_type = VALUES(product_type),
                        condition_type = VALUES(condition_type),
                        status = VALUES(status),
                        fn_sku = VALUES(fn_sku),
                        item_name = VALUES(item_name),
                        brand = VALUES(brand),
                        created_date = VALUES(created_date),
                        last_updated_date = VALUES(last_updated_date),
                        main_image_url = VALUES(main_image_url),
                        main_image_height = VALUES(main_image_height),
                        main_image_width = VALUES(main_image_width),
                        list_price = VALUES(list_price),
                        list_price_currency = VALUES(list_price_currency),
                        number_of_items = VALUES(number_of_items),
                        parent_sku = VALUES(parent_sku),
                        parentage_level = VALUES(parentage_level),
                        child_relationship_type = VALUES(child_relationship_type),
                        variation_theme = VALUES(variation_theme),
                        country_of_origin = VALUES(country_of_origin),
                        manufacturer = VALUES(manufacturer),
                        product_description = VALUES(product_description),
                        attributes_json = VALUES(attributes_json),
                        issues_json = VALUES(issues_json),
                        sync_time = NOW()
                """
                cursor.execute(sql_main, (
                    main_row['marketplace_id'], main_row['seller_id'], main_row['sku'],
                    main_row['asin'], main_row['product_type'], main_row['condition_type'],
                    main_row['status'], main_row['fn_sku'], main_row['item_name'],
                    main_row['brand'], main_row['created_date'], main_row['last_updated_date'],
                    main_row['main_image_url'], main_row['main_image_height'], main_row['main_image_width'],
                    main_row['list_price'], main_row['list_price_currency'], main_row['number_of_items'],
                    main_row['parent_sku'], main_row['parentage_level'], main_row['child_relationship_type'],
                    main_row['variation_theme'],
                    main_row['country_of_origin'], main_row['manufacturer'], main_row['product_description'],
                    main_row['attributes_json'], main_row['issues_json']
                ))

                # 2. 清空并重建子表数据
                cursor.execute(
                    "DELETE FROM amazon_listing_bullets WHERE sku = %s AND marketplace_id = %s",
                    (sku, marketplace_id)
                )
                for b in bullets:
                    cursor.execute(
                        """INSERT INTO amazon_listing_bullets
                           (sku, marketplace_id, sort_order, content, language_tag)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (b['sku'], b['marketplace_id'], b['sort_order'], b['content'], b['language_tag'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_images WHERE sku = %s AND marketplace_id = %s",
                    (sku, marketplace_id)
                )
                for img in images:
                    cursor.execute(
                        """INSERT INTO amazon_listing_images
                           (sku, marketplace_id, image_type, media_location, sort_order)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (img['sku'], img['marketplace_id'], img['image_type'], img['media_location'], img['sort_order'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_issues WHERE sku = %s AND marketplace_id = %s",
                    (sku, marketplace_id)
                )
                for iss in issues:
                    cursor.execute(
                        """INSERT INTO amazon_listing_issues
                           (sku, marketplace_id, issue_code, message, severity)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (iss['sku'], iss['marketplace_id'], iss['issue_code'], iss['message'], iss['severity'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_offers WHERE sku = %s AND marketplace_id = %s",
                    (sku, marketplace_id)
                )
                for off in offers:
                    cursor.execute(
                        """INSERT INTO amazon_listing_offers
                           (sku, marketplace_id, currency, audience, our_price, start_at, end_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (off['sku'], off['marketplace_id'], off['currency'], off['audience'],
                         off['our_price'], off['start_at'], off['end_at'])
                    )

                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def _get_listings_from_db(sku=None, asin=None, product_type=None, status=None,
                          parent_sku=None, keyword=None, page=1, page_size=20):
    """从数据库分页查询 Listing 列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if sku:
                conditions.append("sku = %s")
                params.append(sku)
            if asin:
                conditions.append("asin = %s")
                params.append(asin)
            if product_type:
                conditions.append("product_type = %s")
                params.append(product_type)
            if status:
                conditions.append("status LIKE %s")
                params.append(f"%{status}%")
            if parent_sku:
                conditions.append("parent_sku = %s")
                params.append(parent_sku)
            if keyword:
                conditions.append("(item_name LIKE %s OR brand LIKE %s OR sku LIKE %s)")
                like = f"%{keyword}%"
                params.extend([like, like, like])

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_listings WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    id, marketplace_id, seller_id, sku, asin, product_type,
                    condition_type, status, fn_sku, item_name, brand,
                    created_date, last_updated_date,
                    main_image_url, main_image_height, main_image_width,
                    list_price, list_price_currency, number_of_items,
                    parent_sku, parentage_level, child_relationship_type, variation_theme,
                    country_of_origin, manufacturer,
                    sync_time, created_at, updated_at
                FROM amazon_listings
                WHERE {where_clause}
                ORDER BY last_updated_date DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()


def _get_listing_detail_from_db(sku, marketplace_id):
    """从数据库查询单个 Listing 详情（含子表）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM amazon_listings
                WHERE sku = %s AND marketplace_id = %s
            """, (sku, marketplace_id))
            row = cursor.fetchone()

            if not row:
                return None

            # 五点描述
            cursor.execute("""
                SELECT sort_order, content, language_tag
                FROM amazon_listing_bullets
                WHERE sku = %s AND marketplace_id = %s
                ORDER BY sort_order ASC
            """, (sku, marketplace_id))
            row['bullets'] = cursor.fetchall()

            # 图片
            cursor.execute("""
                SELECT image_type, media_location, sort_order
                FROM amazon_listing_images
                WHERE sku = %s AND marketplace_id = %s
                ORDER BY sort_order ASC
            """, (sku, marketplace_id))
            row['images'] = cursor.fetchall()

            # Issues
            cursor.execute("""
                SELECT issue_code, message, severity
                FROM amazon_listing_issues
                WHERE sku = %s AND marketplace_id = %s
                ORDER BY id ASC
            """, (sku, marketplace_id))
            row['issues'] = cursor.fetchall()

            # 报价
            cursor.execute("""
                SELECT currency, audience, our_price, start_at, end_at
                FROM amazon_listing_offers
                WHERE sku = %s AND marketplace_id = %s
                ORDER BY id ASC
            """, (sku, marketplace_id))
            row['offers'] = cursor.fetchall()

            return row
    finally:
        conn.close()


def _iso_to_datetime(iso_str):
    """将 ISO 8601 时间字符串转为 MySQL DATETIME 格式"""
    if not iso_str:
        return None
    if isinstance(iso_str, str):
        iso_str = iso_str.replace('Z', '')
        if '+' in iso_str:
            iso_str = iso_str.split('+')[0]
        # 截断毫秒部分（MySQL DATETIME 默认不支持小数秒，或最多 6 位）
        if '.' in iso_str:
            iso_str = iso_str.split('.')[0]
    return iso_str
