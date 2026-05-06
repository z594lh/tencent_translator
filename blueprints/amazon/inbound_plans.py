"""
Amazon 入库计划模块
提供入库计划及箱子查询与同步路由，以及底层数据库操作
"""
import os
import time
import json
import re

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient
from services.mysql_service import get_db_connection

amazon_inbound_plans_bp = Blueprint('amazon_inbound_plans', __name__, url_prefix='/api')

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


def _get_client(marketplace_id=None, region=None):
    """获取 Amazon SP-API 客户端实例"""
    return AmazonSpApiClient(
        marketplace_id=marketplace_id or MARKETPLACE_ID,
        region=region
    )


# ==================== 路由（前端调用）====================

@amazon_inbound_plans_bp.route('/amazon/inbound-plans', methods=['GET'])
@login_required
def amazon_inbound_plans():
    """
    从数据库分页查询入库计划列表
    查询参数:
        status       - 按状态筛选，如 ACTIVE, VOIDED
        page         - 页码，默认 1
        page_size    - 每页数量，默认 20
    """
    try:
        status = request.args.get('status', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_inbound_plans(
            status=status,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Inbound Plans DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/inbound-plans/<plan_id>/boxes', methods=['GET'])
@login_required
def amazon_inbound_plan_boxes(plan_id):
    """
    从数据库分页查询指定入库计划的箱子列表
    查询参数:
        page       - 页码，默认 1
        page_size  - 每页数量，默认 20
    """
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_inbound_plan_boxes(
            plan_id=plan_id,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Inbound Plan Boxes DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/inbound-plan-boxes', methods=['GET'])
@login_required
def amazon_inbound_plan_boxes_by_shipment():
    """
    根据货件编号查询入库计划箱子列表详情
    查询参数:
        shipment_id  - 货件编号（必填）
        page         - 页码，默认 1
        page_size    - 每页数量，默认 20
    """
    try:
        shipment_id = request.args.get('shipment_id', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if not shipment_id:
            return jsonify({"status": "error", "message": "shipment_id 必填"}), 400

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = get_inbound_plan_boxes_by_shipment_id_from_db(
            shipment_id=shipment_id,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Inbound Plan Boxes By Shipment] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/sync/inbound-plans', methods=['POST'])
@login_required
def sync_amazon_inbound_plans():
    """
    手动触发入库计划数据同步（从 API 写入数据库）
    """
    try:
        result = _sync_inbound_plans()

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 入库计划同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/sync/inbound-plans/<plan_id>/boxes', methods=['POST'])
@login_required
def sync_amazon_inbound_plan_boxes(plan_id):
    """
    手动触发指定入库计划的箱子数据同步（从 API 写入数据库）
    """
    try:
        result = _sync_inbound_plan_boxes(plan_id)

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 入库计划箱子同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/sync/inbound-plans-all', methods=['POST'])
@login_required
def sync_amazon_inbound_plans_all():
    """
    一键同步所有入库计划及其箱子数据
    """
    try:
        results = {}
        results['plans'] = _sync_inbound_plans()
        results['boxes'] = _sync_all_inbound_plan_boxes()

        return jsonify({
            "status": "success",
            "message": "入库计划全量同步完成",
            "data": results
        })

    except Exception as e:
        print(f"[Amazon Sync] 入库计划全量同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 分割线 ====================


# ==================== 同步与数据库操作 ====================

def _sync_inbound_plans(status=None):
    """同步入库计划列表（自动处理分页）"""
    client = _get_client()
    all_plans = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Inbound Plans Sync] 正在获取第 {page} 页...")

            result = client.list_inbound_plans(
                marketplace_id=MARKETPLACE_ID,
                status=status,
                page_size=20,
                pagination_token=next_token
            )

            plans = result.get('inboundPlans', [])
            all_plans.extend(plans)

            next_token = result.get('pagination', {}).get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_inbound_plans_to_db(MARKETPLACE_ID, all_plans)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_plans),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_plans),
            "error": str(e)
        }


def _get_inbound_plans(status=None, page=1, page_size=20):
    """从数据库查询入库计划列表（支持分页）"""
    return get_inbound_plans_from_db(
        marketplace_id=MARKETPLACE_ID,
        status=status,
        page=page,
        page_size=page_size
    )


def _get_inbound_plan_ids(status=None):
    """从数据库获取所有入库计划ID"""
    return get_inbound_plan_ids_from_db(marketplace_id=MARKETPLACE_ID, status=status)


def _sync_inbound_plan_boxes(plan_id):
    """同步指定入库计划的箱子列表（自动处理分页）"""
    client = _get_client()
    all_boxes = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Inbound Plan Boxes Sync] Plan {plan_id} 正在获取第 {page} 页...")

            result = client.list_inbound_plan_boxes(
                inbound_plan_id=plan_id,
                page_size=100,
                pagination_token=next_token
            )

            boxes = result.get('boxes', [])
            all_boxes.extend(boxes)

            next_token = result.get('pagination', {}).get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_inbound_plan_boxes_to_db(plan_id, all_boxes)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_boxes),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_boxes),
            "error": str(e)
        }


def _sync_all_inbound_plan_boxes():
    """批量同步所有入库计划的箱子列表"""
    plan_ids = _get_inbound_plan_ids()
    if not plan_ids:
        return {"total_synced": 0, "total_plans": 0, "errors": []}

    total_synced = 0
    errors = []

    for plan_id in plan_ids:
        result = _sync_inbound_plan_boxes(plan_id)
        total_synced += result.get('synced_count', 0)
        if result.get('error'):
            errors.append({"plan_id": plan_id, "error": result['error']})
        time.sleep(0.3)

    return {
        "total_synced": total_synced,
        "total_plans": len(plan_ids),
        "errors": errors
    }


def _get_inbound_plan_boxes(plan_id=None, page=1, page_size=20):
    """从数据库查询入库计划箱子列表（支持分页）"""
    return get_inbound_plan_boxes_from_db(
        inbound_plan_id=plan_id,
        page=page,
        page_size=page_size
    )


# ==================== 数据库操作 ====================

def _iso_to_datetime(iso_str):
    """将 ISO 8601 时间字符串转为 MySQL DATETIME 格式"""
    if not iso_str:
        return None
    if isinstance(iso_str, str):
        iso_str = iso_str.replace('Z', '')
        if '+' in iso_str:
            iso_str = iso_str.split('+')[0]
    return iso_str


def _extract_shipment_id_from_box(box):
    """
    从 box 信息中提取货件编号（shipment_id）
    优先根据 box_id 去除末尾 U+数字 箱号后缀，
    其次尝试 externalContainerIdentifier，无法解析则返回 None
    """
    box_id = box.get('boxId') or ''
    if box_id:
        shipment_id = re.sub(r'U\d+$', '', box_id)
        if shipment_id and shipment_id != box_id:
            return shipment_id
    ext_id = box.get('externalContainerIdentifier') or ''
    if ext_id and not re.search(r'U\d+$', ext_id):
        return ext_id
    return None


def sync_inbound_plans_to_db(marketplace_id, plans):
    """
    同步入库计划列表到数据库
    """
    if not plans:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for plan in plans:
                source = plan.get('sourceAddress', {})
                marketplace_ids = plan.get('marketplaceIds', [])

                sql = """
                    INSERT INTO amazon_inbound_plans (
                        inbound_plan_id, marketplace_id, marketplace_ids, name, status,
                        created_at, last_updated_at,
                        source_address_line1, source_address_line2, source_city,
                        source_company_name, source_country_code, source_email,
                        source_name, source_phone_number, source_postal_code,
                        source_state_or_province_code, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        marketplace_ids = VALUES(marketplace_ids),
                        name = VALUES(name),
                        status = VALUES(status),
                        created_at = VALUES(created_at),
                        last_updated_at = VALUES(last_updated_at),
                        source_address_line1 = VALUES(source_address_line1),
                        source_address_line2 = VALUES(source_address_line2),
                        source_city = VALUES(source_city),
                        source_company_name = VALUES(source_company_name),
                        source_country_code = VALUES(source_country_code),
                        source_email = VALUES(source_email),
                        source_name = VALUES(source_name),
                        source_phone_number = VALUES(source_phone_number),
                        source_postal_code = VALUES(source_postal_code),
                        source_state_or_province_code = VALUES(source_state_or_province_code),
                        sync_time = NOW()
                """

                params = (
                    plan.get('inboundPlanId'),
                    marketplace_id,
                    json.dumps(marketplace_ids) if marketplace_ids else '[]',
                    plan.get('name'),
                    plan.get('status'),
                    _iso_to_datetime(plan.get('createdAt')),
                    _iso_to_datetime(plan.get('lastUpdatedAt')),
                    source.get('addressLine1'),
                    source.get('addressLine2'),
                    source.get('city'),
                    source.get('companyName'),
                    source.get('countryCode'),
                    source.get('email'),
                    source.get('name'),
                    source.get('phoneNumber'),
                    source.get('postalCode'),
                    source.get('stateOrProvinceCode'),
                )

                cursor.execute(sql, params)
                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def get_inbound_plan_boxes_by_shipment_id_from_db(shipment_id=None, page=1, page_size=20):
    """
    根据货件编号从数据库分页查询入库计划箱子列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if shipment_id:
                conditions.append("shipment_id = %s")
                params.append(shipment_id)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_inbound_plan_boxes WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT * FROM amazon_inbound_plan_boxes
                WHERE {where_clause}
                ORDER BY sync_time DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()

            _enrich_boxes_with_product_names(rows)

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()

    return count, None


def get_inbound_plans_from_db(marketplace_id=None, status=None, page=1, page_size=20):
    """
    从数据库分页查询入库计划列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if status:
                conditions.append("status = %s")
                params.append(status)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_inbound_plans WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT * FROM amazon_inbound_plans
                WHERE {where_clause}
                ORDER BY created_at DESC
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


def get_inbound_plan_ids_from_db(marketplace_id=None, status=None):
    """
    从数据库获取所有入库计划ID列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = []
            params = []
            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if status:
                conditions.append("status = %s")
                params.append(status)
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
                sql = f"SELECT inbound_plan_id FROM amazon_inbound_plans {where_clause}"
                cursor.execute(sql, tuple(params))
            else:
                sql = "SELECT inbound_plan_id FROM amazon_inbound_plans"
                cursor.execute(sql)
            return [row['inbound_plan_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def sync_inbound_plan_boxes_to_db(plan_id, boxes):
    """
    同步入库计划箱子列表到数据库
    """
    if not boxes:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for box in boxes:
                dest = box.get('destinationRegion', {})
                dims = box.get('dimensions', {})
                weight = box.get('weight', {})
                items = box.get('items', [])

                shipment_id = _extract_shipment_id_from_box(box)

                sql = """
                    INSERT INTO amazon_inbound_plan_boxes (
                        inbound_plan_id, box_id, package_id, shipment_id, content_information_source,
                        destination_region_country_code, destination_region_state, destination_region_warehouse_id,
                        dimensions_height, dimensions_length, dimensions_unit, dimensions_width,
                        external_container_identifier, external_container_identifier_type,
                        quantity, template_name, weight_unit, weight_value,
                        items_json, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        shipment_id = VALUES(shipment_id),
                        content_information_source = VALUES(content_information_source),
                        destination_region_country_code = VALUES(destination_region_country_code),
                        destination_region_state = VALUES(destination_region_state),
                        destination_region_warehouse_id = VALUES(destination_region_warehouse_id),
                        dimensions_height = VALUES(dimensions_height),
                        dimensions_length = VALUES(dimensions_length),
                        dimensions_unit = VALUES(dimensions_unit),
                        dimensions_width = VALUES(dimensions_width),
                        external_container_identifier = VALUES(external_container_identifier),
                        external_container_identifier_type = VALUES(external_container_identifier_type),
                        quantity = VALUES(quantity),
                        template_name = VALUES(template_name),
                        weight_unit = VALUES(weight_unit),
                        weight_value = VALUES(weight_value),
                        items_json = VALUES(items_json),
                        sync_time = NOW()
                """

                params = (
                    plan_id,
                    box.get('boxId'),
                    box.get('packageId'),
                    shipment_id,
                    box.get('contentInformationSource'),
                    dest.get('countryCode'),
                    dest.get('state'),
                    dest.get('warehouseId'),
                    dims.get('height'),
                    dims.get('length'),
                    dims.get('unitOfMeasurement'),
                    dims.get('width'),
                    box.get('externalContainerIdentifier'),
                    box.get('externalContainerIdentifierType'),
                    box.get('quantity', 0),
                    box.get('templateName'),
                    weight.get('unit'),
                    weight.get('value'),
                    json.dumps(items) if items else '[]',
                )

                cursor.execute(sql, params)
                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def _enrich_boxes_with_product_names(rows):
    """
    为箱子列表中的 items_json 解析并添加 SKU 中文名
    根据 msku 关联 products 表，优先取 declare_name_cn，其次 product_name
    """
    if not rows:
        return

    all_mskus = set()
    for row in rows:
        items_str = row.get('items_json') or '[]'
        try:
            items = json.loads(items_str)
            if isinstance(items, list):
                for item in items:
                    msku = item.get('msku')
                    if msku:
                        all_mskus.add(msku)
        except Exception:
            continue

    if not all_mskus:
        for row in rows:
            row['items'] = []
        return

    product_map = {}
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(all_mskus))
            sql = f"""
                SELECT seller_sku, product_name, declare_name_cn
                FROM products
                WHERE seller_sku IN ({placeholders})
            """
            cursor.execute(sql, tuple(all_mskus))
            for prod in cursor.fetchall():
                name = prod.get('declare_name_cn') or prod.get('product_name') or ''
                product_map[prod['seller_sku']] = name
    finally:
        conn.close()

    for row in rows:
        items_str = row.get('items_json') or '[]'
        try:
            items = json.loads(items_str)
            if isinstance(items, list):
                for item in items:
                    item['sku_name_cn'] = product_map.get(item.get('msku'), '')
                row['items'] = items
            else:
                row['items'] = []
        except Exception:
            row['items'] = []


def get_inbound_plan_boxes_from_db(inbound_plan_id=None, page=1, page_size=20):
    """
    从数据库分页查询入库计划箱子列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if inbound_plan_id:
                conditions.append("inbound_plan_id = %s")
                params.append(inbound_plan_id)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_inbound_plan_boxes WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT * FROM amazon_inbound_plan_boxes
                WHERE {where_clause}
                ORDER BY sync_time DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()

            _enrich_boxes_with_product_names(rows)

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()
