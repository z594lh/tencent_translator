"""
Amazon Listing 模块（多店铺支持版）
提供 Listing 抓取、上架、修改、删除路由
以及数据库同步、分页查询（供前端展示）

上架/修改时图片 URL 通过阿里云 OSS 上传服务获取

注意：所有接口必须传入 shop_id，不传直接返回 400
"""
import json
import time
import threading

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_sp_api_client
from services.oss_uploader import upload_image_for_listing
from services.mysql_service import get_db_connection
from services.deepseekAI import generate_declaration_info

amazon_listing_bp = Blueprint('amazon_listing', __name__, url_prefix='/api')


def _require_shop_id() -> int:
    """强制获取 shop_id，不传则抛异常"""
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


def _require_shop_id_from_body(data: dict) -> int:
    """从请求体中强制获取 shop_id，不传则抛异常"""
    shop_id = data.get('shop_id')
    if shop_id is None or shop_id == '':
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except (ValueError, TypeError):
        raise ValueError("shop_id 必须是整数")


# ========================
# 路由：数据库查询（前端展示用）
# ========================

@amazon_listing_bp.route('/amazon/listings', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def get_listings():
    """
    从数据库分页查询 Listing 列表
    查询参数（必填）:
        shop_id       - 店铺ID
    查询参数（可选）:
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
        shop_id = _require_shop_id()
        sku = request.args.get('sku', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        product_type = request.args.get('product_type', '').strip() or None
        status = request.args.get('status', '').strip() or None
        parent_sku = request.args.get('parent_sku', '').strip() or None
        keyword = request.args.get('keyword', '').strip() or None
        has_issues = request.args.get('has_issues', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_listings_from_db(
            shop_id=shop_id,
            sku=sku, asin=asin, product_type=product_type,
            status=status, parent_sku=parent_sku, keyword=keyword,
            has_issues=has_issues,
            page=page, page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def get_listing_detail(sku):
    """
    从数据库查询单个 Listing 详情（含子表数据：图片、五点、issues、报价）
    查询参数（必填）:
        shop_id  - 店铺ID
    """
    try:
        shop_id = _require_shop_id()
        result = _get_listing_detail_from_db(shop_id=shop_id, sku=sku)
        if not result:
            return jsonify({"status": "error", "message": "Listing 不存在"}), 404

        return jsonify({
            "status": "success",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing DB] 详情查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/sync/listings', methods=['POST'])
@login_required
@permission_required('amazon_listings:sync')
def sync_listings():
    """
    手动触发 Listing 同步（从 SP-API 写入数据库）
    请求体（必填）:
        shop_id  - 店铺ID
    请求体（可选）:
        included_data  - 额外包含数据，默认 ["summaries", "attributes", "issues"]
        page_size      - 每页拉取数量，默认 20
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)
        included_data = data.get('included_data', ["summaries", "attributes", "issues"])
        page_size = data.get('page_size', 20)

        result = _sync_listings(
            shop_id=shop_id,
            included_data=included_data,
            page_size=page_size
        )

        msg = f"同步完成，共处理 {result.get('synced_count', 0)} 条 Listing"
        deleted = result.get('deleted_listings', 0)
        if deleted:
            msg += f"，标记 {deleted} 条已删除"
        return jsonify({
            "status": "success",
            "message": msg,
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing Sync] 同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========================
# 路由：同步单条 Listing
# ========================

@amazon_listing_bp.route('/amazon/listings/<sku>/sync', methods=['POST'])
@login_required
@permission_required('amazon_listings:sync')
def sync_single_listing(sku):
    """
    从 SP-API 实时拉取单条 Listing 并写入本地数据库
    适用于修改（标题/价格/描述/五点）后刷新验证
    请求体（必填）:
        shop_id  - 店铺ID
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)
        included_data = data.get('included_data', ["summaries", "attributes", "issues"])

        client = get_sp_api_client(shop_id=shop_id)
        seller_id = client.seller_id or ''
        marketplace_id = client.marketplace_id

        result = client.get_listings_item(sku=sku, included_data=included_data)

        if not result or not isinstance(result, dict) or not result.get('sku'):
            return jsonify({
                "status": "error",
                "message": f"SP-API 未返回 Listing {sku} 数据，可能该 SKU 尚未同步到亚马逊或参数错误"
            }), 404

        synced_count, error, _ = sync_listings_to_db(
            shop_id, marketplace_id, seller_id, [result]
        )

        if error:
            return jsonify({
                "status": "error",
                "message": f"同步到数据库失败: {error}"
            }), 500

        detail = _get_listing_detail_from_db(shop_id=shop_id, sku=sku)

        return jsonify({
            "status": "success",
            "message": f"Listing {sku} 同步成功",
            "data": detail
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing Sync Single] 同步单条异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========================
# 路由：同步 Listing 到产品表
# ========================

def _extract_model_from_attrs(attrs):
    model_list = attrs.get('model', [])
    if not model_list:
        model_list = attrs.get('model_number', [])
    if not model_list:
        model_list = attrs.get('model_name', [])
    if not model_list:
        return ''
    for item in model_list:
        if isinstance(item, dict) and item.get('value'):
            return item['value']
    if isinstance(model_list, list) and len(model_list) > 0 and isinstance(model_list[0], str):
        return model_list[0]
    return ''


def _generate_model(declare_name_en, dimensions_cm, listing_model=''):
    if listing_model:
        return listing_model
    if not declare_name_en:
        return ''
    words = declare_name_en.split()
    initials = ''.join(w[0].upper() for w in words if w)
    if not initials:
        return ''
    if dimensions_cm:
        return f"{initials}-{dimensions_cm}"
    return initials


def _sync_product_from_listing(shop_id, sku, decl_info=None):
    listing = _get_listing_detail_from_db(shop_id, sku)
    if not listing:
        return {"status": "error", "message": f"Listing {sku} 不存在"}

    attrs = {}
    raw = listing.get('attributes_json', '{}')
    if raw and raw != '{}':
        try:
            attrs = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            pass

    weight_kg = _parse_weight_kg(attrs)
    dimensions_cm = _parse_dimensions_cm(attrs)

    material_cn = _extract_material_cn(attrs)

    if decl_info:
        product_name = decl_info.get('name_cn') or ''
        declare_name_cn = decl_info.get('name_cn') or ''
        declare_name_en = decl_info.get('name_en') or ''
        material_cn = decl_info.get('material_cn') or material_cn
        material_en = decl_info.get('material_en') or ''
        hs_code = decl_info.get('hs_code') or ''
        purpose = decl_info.get('purpose') or ''
    else:
        product_description = listing.get('product_description', '') or ''
        decl = generate_declaration_info(product_description, material_cn)

        product_name = decl.get('name_cn') or ''
        declare_name_cn = decl.get('name_cn') or ''
        declare_name_en = decl.get('name_en') or ''
        material_cn = decl.get('material_cn') or material_cn
        material_en = decl.get('material_en') or ''
        hs_code = decl.get('hs_code') or ''
        purpose = decl.get('purpose') or ''

    listing_model = _extract_model_from_attrs(attrs)
    model = _generate_model(declare_name_en, dimensions_cm, listing_model)

    shared_decl = {
        "name_cn": declare_name_cn,
        "name_en": declare_name_en,
        "material_cn": material_cn,
        "material_en": material_en,
        "hs_code": hs_code,
        "purpose": purpose,
    }

    brand = listing.get('brand') or ''
    if brand.strip().lower() == 'generic':
        brand = '无'

    images = listing.get('images', []) or []
    image_urls_list = [img['media_location'] for img in images
                       if img.get('media_location') and img.get('image_type') != 'main']
    image_urls = json.dumps(image_urls_list, ensure_ascii=False) if image_urls_list else None

    asin = (listing.get('asin') or '').strip()
    sales_url = f"https://www.amazon.com/dp/{asin}" if asin else ''

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM products WHERE seller_sku = %s", (sku,))
            existing = cursor.fetchone()

            if existing:
                cursor.execute("""
                    UPDATE products SET
                        product_name = %s,
                        brand = %s,
                        declare_name_cn = %s,
                        declare_name_en = %s,
                        material_cn = %s,
                        material_en = %s,
                        purpose = %s,
                        model = %s,
                        hs_code = %s,
                        declare_value = %s,
                        currency = %s,
                        asin = %s,
                        fnsku = %s,
                        image_url = %s,
                        image_urls = %s,
                        weight_kg = %s,
                        dimensions_cm = %s,
                        sales_url = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (
                    product_name,
                    brand,
                    declare_name_cn,
                    declare_name_en,
                    material_cn,
                    material_en,
                    purpose,
                    model,
                    hs_code,
                    listing.get('list_price'),
                    listing.get('list_price_currency', 'USD'),
                    asin,
                    listing.get('fn_sku') or '',
                    listing.get('main_image_url') or '',
                    image_urls,
                    weight_kg,
                    dimensions_cm,
                    sales_url,
                    existing['id']
                ))
                conn.commit()
                return {
                    "status": "success",
                    "message": f"产品 {sku} 已更新",
                    "data": {"id": existing['id'], "action": "updated"},
                    "decl_info": shared_decl
                }
            else:
                cursor.execute("""
                    INSERT INTO products
                        (seller_sku, product_name, brand,
                         declare_name_cn, declare_name_en, material_cn, material_en,
                         purpose, model, hs_code,
                         declare_value, currency, asin, fnsku,
                         image_url, image_urls, weight_kg, dimensions_cm, sales_url,
                         status)
                    VALUES
                        (%s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s,
                         %s, %s, %s, %s,
                         %s, %s, %s, %s, %s,
                         1)
                """, (
                    sku,
                    product_name,
                    brand,
                    declare_name_cn,
                    declare_name_en,
                    material_cn,
                    material_en,
                    purpose,
                    model,
                    hs_code,
                    listing.get('list_price'),
                    listing.get('list_price_currency', 'USD'),
                    asin,
                    listing.get('fn_sku') or '',
                    listing.get('main_image_url') or '',
                    image_urls,
                    weight_kg,
                    dimensions_cm,
                    sales_url,
                ))
                conn.commit()
                new_id = cursor.lastrowid
                return {
                    "status": "success",
                    "message": f"产品 {sku} 已创建",
                    "data": {"id": new_id, "action": "created"},
                    "decl_info": shared_decl
                }
    finally:
        conn.close()


@amazon_listing_bp.route('/amazon/listings/<sku>/sync-to-product', methods=['POST'])
@login_required
@permission_required('amazon_listings:sync')
def sync_listing_to_product(sku):
    """
    将指定 Listing 同步到 products 表：sku 存在则更新，不存在则新增。
    如果 sku 是父体，则同步该父体下所有子体（共用同一份申报信息）。
    请求体（必填）:
        shop_id  - 店铺ID
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT parentage_level, parent_sku FROM amazon_listings WHERE shop_id = %s AND sku = %s",
                    (shop_id, sku)
                )
                row = cursor.fetchone()
        finally:
            conn.close()

        if not row:
            return jsonify({"status": "error", "message": f"Listing {sku} 不存在"}), 404

        level = (row.get('parentage_level') or '').strip().lower()

        if level == 'parent' or level == 'variation_parent':
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT sku FROM amazon_listings WHERE shop_id = %s AND parent_sku = %s",
                        (shop_id, sku)
                    )
                    children = [r['sku'] for r in cursor.fetchall()]
            finally:
                conn.close()

            if not children:
                return jsonify({"status": "error", "message": f"父体 {sku} 下没有子体"}), 400

            first_result = _sync_product_from_listing(shop_id, children[0])
            if first_result.get('status') == 'error':
                return jsonify(first_result), 500
            decl_info = first_result.get('decl_info')

            results = [first_result]
            for child_sku in children[1:]:
                try:
                    r = _sync_product_from_listing(shop_id, child_sku, decl_info=decl_info)
                    results.append(r)
                except Exception as e:
                    results.append({"status": "error", "sku": child_sku, "message": str(e)})

            return jsonify({
                "status": "success",
                "message": f"父体 {sku} 下 {len(children)} 个子体已同步",
                "data": {
                    "parent_sku": sku,
                    "child_count": len(children),
                    "results": results
                }
            })
        else:
            result = _sync_product_from_listing(shop_id, sku)
            if result.get('status') == 'error':
                code = 404 if '不存在' in result.get('message', '') else 500
                return jsonify(result), code
            return jsonify(result)

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Sync to Product] 同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _parse_weight_kg(attrs):
    """从 attributes 中提取重量并转换为 kg"""
    item_weight = attrs.get('item_weight') or attrs.get('item_package_weight')
    if item_weight and isinstance(item_weight, list) and len(item_weight) > 0:
        w = item_weight[0]
        if isinstance(w, dict):
            value = w.get('value')
            unit = (w.get('unit') or '').lower()
            if value is not None:
                value = float(value)
                if unit == 'pounds':
                    return round(value * 0.453592, 3)
                elif unit == 'kg':
                    return round(value, 3)
                return value
    return None


def _extract_material_cn(attrs):
    """从 attributes_json 解析出的 dict 中提取中文材质，多个用、连接"""
    material_list = attrs.get('material', [])
    if not material_list:
        return ""
    values = [item.get('value', '') for item in material_list if item.get('language_tag') == 'zh_CN']
    return "、".join(values)


def _parse_dimensions_cm(attrs):
    """从 attributes 中提取长宽高并转换为 cm，返回 'L*W*H' 格式字符串"""
    dims = attrs.get('item_length_width_height') or attrs.get('item_package_dimensions')
    if dims and isinstance(dims, list) and len(dims) > 0:
        d = dims[0]
        if isinstance(d, dict):
            length = _get_dim_value(d.get('length'))
            width = _get_dim_value(d.get('width'))
            height = _get_dim_value(d.get('height'))
            if length and width and height:
                return f"{length}*{width}*{height}"
    return None


def _get_dim_value(dim):
    """解析单维度值，统一转换为 cm"""
    if not isinstance(dim, dict):
        return None
    value = dim.get('value')
    unit = (dim.get('unit') or '').lower()
    if value is not None:
        value = float(value)
        if unit == 'inches':
            return round(value * 2.54, 2)
        elif unit == 'cm':
            return round(value, 2)
        return value
    return None


# ========================
# 路由：SP-API 直连操作
# ========================

@amazon_listing_bp.route('/amazon/listings/spapi/<sku>', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def get_listing_detail_spapi(sku):
    """
    实时从 SP-API 抓取单个 Listing 详情（不走数据库缓存）
    查询参数（必填）:
        shop_id  - 店铺ID
    查询参数（可选）:
        included_data  - 额外包含数据
    """
    try:
        shop_id = _require_shop_id()
        included_data = request.args.get('included_data', '').strip() or None
        included_data_list = None
        if included_data:
            included_data_list = [x.strip() for x in included_data.split(',') if x.strip()]

        client = get_sp_api_client(shop_id=shop_id)
        result = client.get_listings_item(sku=sku, included_data=included_data_list)

        return jsonify({
            "status": "success",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing SP-API] 实时抓取异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings', methods=['POST'])
@login_required
@permission_required('amazon_listings:create')
def create_listing():
    """
    上架 Listing（创建新商品）
    请求体（必填）:
        shop_id, sku, product_type, attributes
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)
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

        client = get_sp_api_client(shop_id=shop_id)
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

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing] 上架异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['PUT'])
@login_required
@permission_required('amazon_listings:edit')
def update_listing(sku):
    """
    修改 Listing（完全覆盖式更新）
    请求体（必填）:
        shop_id, product_type, attributes
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)
        product_type = data.get('product_type', '').strip()
        attributes = data.get('attributes')
        requirements = data.get('requirements', 'LISTING')
        condition_type = data.get('condition_type')

        if not product_type:
            return jsonify({"status": "error", "message": "缺少必填字段: product_type"}), 400
        if not attributes or not isinstance(attributes, dict):
            return jsonify({"status": "error", "message": "缺少必填字段: attributes（必须为对象）"}), 400

        client = get_sp_api_client(shop_id=shop_id)
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

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing] 更新异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['PATCH'])
@login_required
@permission_required('amazon_listings:edit')
def patch_listing(sku):
    """
    部分更新 Listing（JSON Patch）
    请求体（必填）:
        shop_id, patches
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)
        patches = data.get('patches')
        product_type = data.get('product_type')

        if not patches or not isinstance(patches, list):
            return jsonify({"status": "error", "message": "缺少必填字段: patches（必须为数组）"}), 400

        client = get_sp_api_client(shop_id=shop_id)
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

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing] 部分更新异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/<sku>', methods=['DELETE'])
@login_required
@permission_required('amazon_listings:delete')
def delete_listing(sku):
    """
    删除 Listing
    查询参数（必填）:
        shop_id  - 店铺ID
    查询参数（可选）:
        marketplace_ids - 可选，逗号分隔
    """
    try:
        shop_id = _require_shop_id()
        marketplace_ids = request.args.get('marketplace_ids', '').strip() or None
        marketplace_ids_list = None
        if marketplace_ids:
            marketplace_ids_list = [x.strip() for x in marketplace_ids.split(',') if x.strip()]

        client = get_sp_api_client(shop_id=shop_id)
        result = client.delete_listings_item(sku=sku, marketplace_ids=marketplace_ids_list)

        return jsonify({
            "status": "success",
            "message": "Listing 删除成功",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Listing] 删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/listings/upload-image', methods=['POST'])
@login_required
@permission_required('amazon_listings:upload_image')
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
@permission_required('amazon_listings:delete_image')
def delete_listing_image():
    """
    删除 OSS 上的 Listing 图片
    请求方式: application/json
    字段（二选一）:
        oss_key - OSS 对象键
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
@permission_required('amazon_listings:delete_image')
def cleanup_listing_images():
    """
    清理 OSS 中未被任何 Listing 引用的孤儿图片（兜底机制）
    请求体可选:
        dry_run - 默认 true，只统计不真正删除
    """
    try:
        data = request.get_json() or {}
        dry_run = data.get('dry_run', True)

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
# 路由：上架工作流辅助接口
# ========================

@amazon_listing_bp.route('/amazon/catalog/search', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def search_catalog():
    """
    搜索亚马逊目录，检查商品是否已存在（上架流程 Step 1）
    查询参数（必填）:
        shop_id  - 店铺ID
    查询参数（至少一个）:
        keywords  - 关键词搜索
        upc       - UPC 条码
        ean       - EAN 条码
    """
    try:
        shop_id = _require_shop_id()
        keywords = request.args.get('keywords', '').strip() or None
        upc = request.args.get('upc', '').strip() or None
        ean = request.args.get('ean', '').strip() or None

        if not keywords and not upc and not ean:
            return jsonify({"status": "error", "message": "请提供 keywords / upc / ean 至少一个参数"}), 400

        client = get_sp_api_client(shop_id=shop_id)

        # 构建搜索关键词
        search_keywords = []
        if keywords:
            search_keywords.append(keywords)
        if upc:
            search_keywords.append(upc)
        if ean:
            search_keywords.append(ean)

        result = client.search_catalog_items(
            keywords=search_keywords,
            included_data=["summaries"]
        )

        items = result.get('items', [])
        simplified = []
        for item in items:
            s = (item.get('summaries', [{}]) or [{}])[0]
            simplified.append({
                "asin": item.get('asin', ''),
                "title": s.get('itemName', ''),
                "brand": s.get('brand', ''),
                "product_type": s.get('productType', ''),
                "main_image_url": (s.get('mainImage') or {}).get('link', ''),
            })

        return jsonify({
            "status": "success",
            "data": {
                "items": simplified,
                "total_results": len(simplified)
            }
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Catalog Search] 异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/product-types/search', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def search_product_types():
    """
    搜索 Product Type 分类（上架流程 Step 2）
    查询参数（必填）:
        shop_id   - 店铺ID
        keywords  - 搜索关键词，如 "pool vacuum"
    查询参数（可选）:
        item_name - 按商品名称搜索
    """
    try:
        shop_id = _require_shop_id()
        keywords = request.args.get('keywords', '').strip()
        item_name = request.args.get('item_name', '').strip() or None

        if not keywords:
            return jsonify({"status": "error", "message": "缺少必填参数: keywords"}), 400

        client = get_sp_api_client(shop_id=shop_id)
        result = client.search_product_types(keywords=keywords, item_name=item_name)

        product_types = result.get('productTypes', [])
        simplified = []
        for pt in product_types:
            simplified.append({
                "name": pt.get('name', ''),
                "display_name": pt.get('displayName', ''),
                "marketplace_ids": pt.get('marketplaceIds', []),
            })

        return jsonify({
            "status": "success",
            "data": {
                "product_types": simplified,
                "total_results": len(simplified)
            }
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Product Type Search] 异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_listing_bp.route('/amazon/product-types/<product_type>/schema', methods=['GET'])
@login_required
@permission_required('amazon_listings:page')
def get_product_type_schema(product_type):
    """
    获取分类 Schema（精简版，供前端动态表单渲染）（上架流程 Step 3）
    路径参数:
        product_type - 如 POOL_VACUUM
    查询参数（必填）:
        shop_id - 店铺ID
    """
    try:
        shop_id = _require_shop_id()
        client = get_sp_api_client(shop_id=shop_id)

        # 1. 获取 product type 定义（含 S3 schema 链接 + propertyGroups）
        definition = client.get_product_type_definition(product_type)

        # 2. 从 S3 下载完整 JSON Schema
        import requests as req
        schema_link = definition.get('schema', {}).get('link', {}).get('resource', '')
        full_schema = {}
        if schema_link:
            try:
                resp = req.get(schema_link, timeout=30)
                resp.raise_for_status()
                full_schema = resp.json()
            except Exception as e:
                print(f"[Schema Download] S3 下载失败: {e}")

        # 3. 解析：收集所有字段定义和必填字段
        prop_schemas = {}
        req_attrs = set()
        defs = full_schema.get('$defs', {})

        # 从顶层 properties 收集
        top_props = full_schema.get('properties', {})
        prop_schemas.update(top_props)
        req_attrs.update(full_schema.get('required', []))

        # 从 allOf 收集（包括 $ref 引用和内联 properties/required）
        all_of = full_schema.get('allOf', [])
        for node in all_of:
            # 1) $ref 引用 → 跟踪定义
            ref = node.get('$ref', '')
            if ref.startswith('#/$defs/'):
                def_name = ref.split('#/$defs/')[1]
                d = defs.get(def_name, {})
                prop_schemas.update(d.get('properties', {}))
                req_attrs.update(d.get('required', []))
            # 2) 内联 properties / required
            if 'properties' in node:
                prop_schemas.update(node['properties'])
            if 'required' in node:
                req_attrs.update(node['required'])
            # 3) if/then 条件规则中的 required
            for key in ('if', 'then', 'else'):
                cond = node.get(key, {})
                if isinstance(cond, dict):
                    cond_all_of = cond.get('allOf', [])
                    for cn in cond_all_of:
                        for sk in ('required',):
                            if sk in cn:
                                req_attrs.update(cn[sk])
                        cn_props = cn.get('properties', {})
                        prop_schemas.update(cn_props)
                        # 嵌套 definitions ref
                        cn_ref = cn.get('$ref', '')
                        if cn_ref.startswith('#/$defs/'):
                            cn_def = defs.get(cn_ref.split('#/$defs/')[1], {})
                            prop_schemas.update(cn_def.get('properties', {}))
                            req_attrs.update(cn_def.get('required', []))
                    # then 块可能有 required
                    cond_req = cond.get('required', [])
                    req_attrs.update(cond_req)

        # 4. 构建精简字段列表
        fields = {}
        for name, defn in prop_schemas.items():
            info = _simplify_field_schema(name, defn, defs)
            if info:
                info['required'] = name in req_attrs
                fields[name] = info

        # 5. propertyGroups
        groups_raw = definition.get('propertyGroups', {})
        groups = {}
        for gk, gv in groups_raw.items():
            groups[gk] = {
                "title": gv.get('title', gk),
                "description": gv.get('description', ''),
                "fields": gv.get('propertyNames', [])
            }

        return jsonify({
            "status": "success",
            "data": {
                "product_type": product_type,
                "display_name": definition.get('displayName', ''),
                "requirements_enforced": definition.get('requirementsEnforced', ''),
                "required": sorted(req_attrs),
                "fields": fields,
                "groups": groups
            }
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Product Type Schema] 异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def _simplify_field_schema(name, defn, defs):
    """
    将单个字段的 JSON Schema 定义精简为前端可用格式。
    递归跟踪 items.$ref 链提取 enum / maxLength / type。
    """
    if not isinstance(defn, dict):
        return None

    info = {
        "name": name,
        "label": defn.get('title', name),
        "description": defn.get('description', ''),
    }

    # 递归提取深层 value schema
    value_schema = _extract_value_schema(defn, defs)

    json_type = value_schema.get('type', defn.get('type', 'string'))
    if 'enum' in value_schema:
        info['enum'] = value_schema['enum']
        info['enum_names'] = value_schema.get('enumNames', value_schema['enum'])
    if 'maxLength' in value_schema:
        info['max_length'] = value_schema['maxLength']
    if 'examples' in value_schema or 'examples' in defn:
        info['examples'] = value_schema.get('examples') or defn.get('examples')

    # 映射前端控件类型
    if any(kw in name for kw in ('media_locator', 'image_locator')):
        info['type'] = 'image'
    elif any(kw in name for kw in ('dimensions', 'length_width_height')):
        info['type'] = 'dimensions'
    elif 'weight' in name:
        info['type'] = 'weight'
    elif 'price' in name:
        info['type'] = 'price'
    elif 'enum' in info:
        info['type'] = 'select'
    elif json_type in ('boolean',):
        info['type'] = 'boolean'
    elif name.startswith(('is_', 'has_')) or 'batteries' in name:
        info['type'] = 'boolean'
    elif json_type in ('integer', 'number'):
        info['type'] = json_type
    else:
        info['type'] = 'string'

    return info


def _extract_value_schema(defn, defs):
    """
    递归跟踪 items.$ref 链，直到找到 value 属性定义。
    处理两级 ref: field → items:$ref → properties.value
    """
    items = defn.get('items', {})
    if not isinstance(items, dict):
        return {}

    # 情况1：items 直接有 properties.value
    iprops = items.get('properties', {})
    if 'value' in iprops and isinstance(iprops['value'], dict):
        return iprops['value']

    # 情况2：items.$ref → 定义 → properties.value
    iref = items.get('$ref', '')
    if iref.startswith('#/$defs/'):
        ref_def = defs.get(iref.split('#/$defs/')[1], {})
        rprops = ref_def.get('properties', {})
        if 'value' in rprops and isinstance(rprops['value'], dict):
            return rprops['value']

    # 情况3：items.properties.value 本身有 enum
    if 'value' in iprops:
        return iprops['value'] if isinstance(iprops['value'], dict) else {}

    return {}


# ========================
# 数据库同步与查询函数
# ========================

# （后半部分：_parse_listing_item、_sync_listings、sync_listings_to_db、_get_listings_from_db、_get_listing_detail_from_db）


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


def _parse_listing_item(item, shop_id, marketplace_id, seller_id):
    """解析单个 Listing item，返回主表数据和子表数据"""
    sku = item.get('sku', '')
    summaries = item.get('summaries', [])
    attributes = item.get('attributes', {})
    issues = item.get('issues', [])

    summary = summaries[0] if summaries and isinstance(summaries, list) else {}

    status_list = summary.get('status', []) or []
    status_str = ','.join(status_list) if isinstance(status_list, list) else str(status_list)

    main_image = summary.get('mainImage', {}) or {}

    brand = _extract_first_lang_value(attributes.get('brand'))
    item_name = _extract_first_lang_value(attributes.get('item_name'))
    product_description = _extract_first_lang_value(attributes.get('product_description'))
    manufacturer = _extract_first_lang_value(attributes.get('manufacturer'))
    country_of_origin = _extract_first_plain_value(attributes.get('country_of_origin'))
    number_of_items = _extract_first_plain_value(attributes.get('number_of_items'))
    variation_theme = None
    if attributes.get('variation_theme') and isinstance(attributes['variation_theme'], list):
        variation_theme = attributes['variation_theme'][0].get('name')

    parent_sku = None
    parentage_level = None
    child_relationship_type = None
    rel_list = attributes.get('child_parent_sku_relationship', [])
    if rel_list and isinstance(rel_list, list):
        rel = rel_list[0]
        parent_sku = rel.get('parent_sku')
        parentage_level = _extract_first_plain_value(attributes.get('parentage_level'))
        child_relationship_type = rel.get('child_relationship_type')

    list_price = None
    list_price_currency = None
    lp_list = attributes.get('list_price', [])
    if lp_list and isinstance(lp_list, list):
        list_price = lp_list[0].get('value')
        list_price_currency = lp_list[0].get('currency')

    main_row = {
        'shop_id': shop_id,
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

    bullets = []
    bp_list = attributes.get('bullet_point', [])
    if bp_list and isinstance(bp_list, list):
        for idx, bp in enumerate(bp_list):
            if isinstance(bp, dict) and bp.get('value'):
                bullets.append({
                    'shop_id': shop_id,
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'sort_order': idx,
                    'content': bp['value'],
                    'language_tag': bp.get('language_tag', 'en_US')
                })

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
                    'shop_id': shop_id,
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'image_type': img_type,
                    'media_location': loc,
                    'sort_order': sort_idx
                })

    issue_rows = []
    if issues and isinstance(issues, list):
        for iss in issues:
            if isinstance(iss, dict):
                issue_rows.append({
                    'shop_id': shop_id,
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'issue_code': iss.get('code'),
                    'message': iss.get('message'),
                    'severity': iss.get('severity')
                })

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

                discounted_price = None
                dp = off.get('discounted_price', [])
                if dp and isinstance(dp, list) and dp[0].get('schedule'):
                    sched = dp[0]['schedule']
                    if sched and isinstance(sched, list):
                        discounted_price = sched[0].get('value_with_tax')

                offers.append({
                    'shop_id': shop_id,
                    'sku': sku,
                    'marketplace_id': marketplace_id,
                    'currency': off.get('currency'),
                    'audience': off.get('audience', 'ALL'),
                    'our_price': our_price,
                    'discounted_price': discounted_price,
                    'start_at': _iso_to_datetime(off.get('start_at', {}).get('value')) if isinstance(off.get('start_at'), dict) else None,
                    'end_at': _iso_to_datetime(off.get('end_at', {}).get('value')) if isinstance(off.get('end_at'), dict) else None,
                })

    return main_row, bullets, images, issue_rows, offers


def _auto_sync_products(shop_id, skus):
    if not skus:
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(skus))
            cursor.execute(
                f"SELECT sku, parentage_level, parent_sku FROM amazon_listings WHERE shop_id = %s AND sku IN ({placeholders})",
                [shop_id] + list(skus)
            )
            rows = cursor.fetchall()
    finally:
        conn.close()

    child_by_parent = {}
    for row in rows:
        level = (row.get('parentage_level') or '').strip().lower()
        parent_sku = (row.get('parent_sku') or '').strip()
        sku = row['sku']

        if level == 'parent' or level == 'variation_parent':
            print(f"[AutoSync] 跳过父体 [{sku}]")
            continue

        if not parent_sku:
            child_by_parent.setdefault('__no_parent__', []).append(sku)
        else:
            child_by_parent.setdefault(parent_sku, []).append(sku)

    print(f"[AutoSync][shop_id={shop_id}] 开始异步同步产品，共 {len(child_by_parent)} 个分组...")

    for parent_sku, child_skus in child_by_parent.items():
        if not child_skus:
            continue
        first_sku = child_skus[0]

        try:
            result = _sync_product_from_listing(shop_id, first_sku)
            action = result.get('data', {}).get('action', '?') if result.get('status') == 'success' else 'error'
            print(f"[AutoSync][shop_id={shop_id}] 产品同步 [{first_sku}]: {action}")
            decl_info = result.get('decl_info') if result.get('status') == 'success' else None
        except Exception as e:
            print(f"[AutoSync][shop_id={shop_id}] 产品同步异常 [{first_sku}]: {e}")
            decl_info = None

        for sku in child_skus[1:]:
            try:
                result = _sync_product_from_listing(shop_id, sku, decl_info=decl_info)
                action = result.get('data', {}).get('action', '?') if result.get('status') == 'success' else 'error'
                print(f"[AutoSync][shop_id={shop_id}] 产品同步 [{sku}]: {action}")
            except Exception as e:
                print(f"[AutoSync][shop_id={shop_id}] 产品同步异常 [{sku}]: {e}")

    print(f"[AutoSync][shop_id={shop_id}] 异步产品同步完成")


def _sync_listings(shop_id, included_data=None, page_size=20):
    """
    同步 Listing 数据到数据库（自动处理分页）
    """
    client = get_sp_api_client(shop_id=shop_id)
    seller_id = client.seller_id or ''
    marketplace_id = client.marketplace_id

    all_items = []
    next_token = None
    page = 0
    total_fetched = 0

    try:
        while True:
            page += 1
            print(f"[Listing Sync][shop_id={shop_id}] 正在获取第 {page} 页...")

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

            if not items:
                break
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error, new_skus = sync_listings_to_db(shop_id, marketplace_id, seller_id, all_items)

        deleted_count = 0
        if not error:
            amazon_skus = set()
            for item in all_items:
                sku = item.get('sku', '')
                if sku:
                    amazon_skus.add(sku)

            if amazon_skus:
                conn = get_db_connection()
                try:
                    with conn.cursor() as cursor:
                        placeholders = ','.join(['%s'] * len(amazon_skus))
                        cursor.execute(
                            f"UPDATE amazon_listings SET is_deleted = 1 WHERE shop_id = %s AND sku NOT IN ({placeholders})",
                            [shop_id] + list(amazon_skus)
                        )
                        deleted_count = cursor.rowcount
                    conn.commit()
                except Exception as e:
                    print(f"[Listing Sync][shop_id={shop_id}] 标记已删除 Listing 失败: {str(e)}")
                finally:
                    conn.close()

            if deleted_count:
                print(f"[Listing Sync][shop_id={shop_id}] 标记 {deleted_count} 条 Amazon 已删除的 Listing")

        if new_skus:
            print(f"[Listing Sync][shop_id={shop_id}] 检测到 {len(new_skus)} 个新增 Listing，后台异步同步到产品表...")
            threading.Thread(
                target=_auto_sync_products,
                args=(shop_id, new_skus),
                daemon=True
            ).start()

        return {
            "synced_count": synced_count,
            "total_fetched": total_fetched,
            "new_listings": len(new_skus),
            "deleted_listings": deleted_count,
            "error": error,
            "next_token": None
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": total_fetched,
            "new_listings": 0,
            "error": str(e),
            "next_token": next_token
        }


def sync_listings_to_db(shop_id, marketplace_id, seller_id, items):
    """
    批量同步 Listing 数据到数据库（含子表）
    返回: (synced_count, error, new_skus)
    """
    if not items:
        return 0, None, []

    conn = get_db_connection()
    count = 0
    new_skus = []
    try:
        with conn.cursor() as cursor:
            all_skus = []
            for item in items:
                main_row, _, _, _, _ = _parse_listing_item(
                    item, shop_id, marketplace_id, seller_id
                )
                all_skus.append(main_row['sku'])

            existing_skus = set()
            if all_skus:
                placeholders = ','.join(['%s'] * len(all_skus))
                cursor.execute(
                    f"SELECT sku FROM amazon_listings WHERE shop_id = %s AND sku IN ({placeholders})",
                    [shop_id] + all_skus
                )
                existing_skus = {row['sku'] for row in cursor.fetchall()}

            for item in items:
                main_row, bullets, images, issues, offers = _parse_listing_item(
                    item, shop_id, marketplace_id, seller_id
                )

                sku = main_row['sku']

                if sku not in existing_skus:
                    new_skus.append(sku)

                # 1. 写入/更新主表
                sql_main = """
                    INSERT INTO amazon_listings (
                        shop_id, marketplace_id, seller_id, sku, asin, product_type, condition_type,
                        status, fn_sku, item_name, brand, created_date, last_updated_date,
                        main_image_url, main_image_height, main_image_width,
                        list_price, list_price_currency, number_of_items,
                        parent_sku, parentage_level, child_relationship_type, variation_theme,
                        country_of_origin, manufacturer, product_description,
                        attributes_json, issues_json, sync_time, is_deleted
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, NOW(), 0
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
                        is_deleted = 0,
                        sync_time = NOW()
                """
                cursor.execute(sql_main, (
                    main_row['shop_id'], main_row['marketplace_id'], main_row['seller_id'], main_row['sku'],
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

                # 2. 清空并重建子表数据（带 shop_id）
                cursor.execute(
                    "DELETE FROM amazon_listing_bullets WHERE shop_id = %s AND sku = %s AND marketplace_id = %s",
                    (shop_id, sku, marketplace_id)
                )
                for b in bullets:
                    cursor.execute(
                        """INSERT INTO amazon_listing_bullets
                           (shop_id, sku, marketplace_id, sort_order, content, language_tag)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (b['shop_id'], b['sku'], b['marketplace_id'], b['sort_order'], b['content'], b['language_tag'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_images WHERE shop_id = %s AND sku = %s AND marketplace_id = %s",
                    (shop_id, sku, marketplace_id)
                )
                for img in images:
                    cursor.execute(
                        """INSERT INTO amazon_listing_images
                           (shop_id, sku, marketplace_id, image_type, media_location, sort_order)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (img['shop_id'], img['sku'], img['marketplace_id'], img['image_type'], img['media_location'], img['sort_order'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_issues WHERE shop_id = %s AND sku = %s AND marketplace_id = %s",
                    (shop_id, sku, marketplace_id)
                )
                for iss in issues:
                    cursor.execute(
                        """INSERT INTO amazon_listing_issues
                           (shop_id, sku, marketplace_id, issue_code, message, severity)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (iss['shop_id'], iss['sku'], iss['marketplace_id'], iss['issue_code'], iss['message'], iss['severity'])
                    )

                cursor.execute(
                    "DELETE FROM amazon_listing_offers WHERE shop_id = %s AND sku = %s AND marketplace_id = %s",
                    (shop_id, sku, marketplace_id)
                )
                for off in offers:
                    cursor.execute(
                        """INSERT INTO amazon_listing_offers
                           (shop_id, sku, marketplace_id, currency, audience, our_price, discounted_price, start_at, end_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (off['shop_id'], off['sku'], off['marketplace_id'], off['currency'], off['audience'],
                         off['our_price'], off['discounted_price'], off['start_at'], off['end_at'])
                    )

                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e), new_skus
    finally:
        conn.close()

    return count, None, new_skus


def _get_listings_from_db(shop_id, sku=None, asin=None, product_type=None, status=None,
                          parent_sku=None, keyword=None, has_issues=None, page=1, page_size=20):
    """从数据库分页查询 Listing 列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s", "is_deleted = 0"]
            params = [shop_id]

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
            if has_issues and has_issues.lower() in ('1', 'true', 'yes', 'on'):
                conditions.append(
                    "EXISTS (SELECT 1 FROM amazon_listing_issues WHERE shop_id = amazon_listings.shop_id AND sku = amazon_listings.sku)"
                )

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_listings WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    id, shop_id, marketplace_id, seller_id, sku, asin, product_type,
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

            # 批量查询关联的 issues
            if rows:
                skus = [r['sku'] for r in rows]
                placeholders = ','.join(['%s'] * len(skus))
                cursor.execute(f"""
                    SELECT sku, issue_code, message, severity
                    FROM amazon_listing_issues
                    WHERE shop_id = %s AND sku IN ({placeholders})
                    ORDER BY id ASC
                """, (shop_id, *skus))
                issues = cursor.fetchall()

                issues_map = {}
                for iss in issues:
                    issues_map.setdefault(iss['sku'], []).append({
                        'issue_code': iss['issue_code'],
                        'message': iss['message'],
                        'severity': iss['severity']
                    })

                for r in rows:
                    r['issues'] = issues_map.get(r['sku'], [])

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()


def _get_listing_detail_from_db(shop_id, sku):
    """从数据库查询单个 Listing 详情（含子表）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM amazon_listings
                WHERE shop_id = %s AND sku = %s AND is_deleted = 0
            """, (shop_id, sku))
            row = cursor.fetchone()

            if not row:
                return None

            cursor.execute("""
                SELECT sort_order, content, language_tag
                FROM amazon_listing_bullets
                WHERE shop_id = %s AND sku = %s
                ORDER BY sort_order ASC
            """, (shop_id, sku))
            row['bullets'] = cursor.fetchall()

            cursor.execute("""
                SELECT image_type, media_location, sort_order
                FROM amazon_listing_images
                WHERE shop_id = %s AND sku = %s
                ORDER BY sort_order ASC
            """, (shop_id, sku))
            row['images'] = cursor.fetchall()

            cursor.execute("""
                SELECT issue_code, message, severity
                FROM amazon_listing_issues
                WHERE shop_id = %s AND sku = %s
                ORDER BY id ASC
            """, (shop_id, sku))
            row['issues'] = cursor.fetchall()

            cursor.execute("""
                SELECT currency, audience, our_price, discounted_price, start_at, end_at
                FROM amazon_listing_offers
                WHERE shop_id = %s AND sku = %s
                ORDER BY id ASC
            """, (shop_id, sku))
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
        if '.' in iso_str:
            iso_str = iso_str.split('.')[0]
    return iso_str
