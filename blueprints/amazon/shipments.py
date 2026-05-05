"""
Amazon 货件模块
提供货件、货件商品、仓库查询与同步路由，以及底层数据库操作
"""
import os
import time
import json
from datetime import datetime, timedelta

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient
from services.mysql_service import get_db_connection

amazon_shipments_bp = Blueprint('amazon_shipments', __name__, url_prefix='/api')

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


def _get_client(marketplace_id=None, region=None):
    """获取 Amazon SP-API 客户端实例"""
    return AmazonSpApiClient(
        marketplace_id=marketplace_id or MARKETPLACE_ID,
        region=region
    )


# ==================== 路由（前端调用）====================

@amazon_shipments_bp.route('/amazon/warehouses', methods=['GET'])
@login_required
def amazon_warehouses():
    """
    查询 FBA 目的仓库列表（用于前端下拉筛选）
    """
    try:
        result = _get_fba_warehouses()

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Warehouses DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/shipments', methods=['GET'])
@login_required
def amazon_shipments():
    """
    从数据库分页查询货件列表数据
    查询参数:
        status          - 按货件状态筛选，如 WORKING, SHIPPED, RECEIVING
        destination_fc  - 按目的仓库筛选，如 IND9, SCK4
        page            - 页码，默认 1
        page_size       - 每页数量，默认 20
    """
    try:
        status = request.args.get('status', '').strip() or None
        destination_fc = request.args.get('destination_fc', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_shipments(
            shipment_status=status,
            destination_fc=destination_fc,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Shipments DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/shipments/<shipment_id>/items', methods=['GET'])
@login_required
def amazon_shipment_items(shipment_id):
    """
    从数据库分页查询指定货件的商品数据
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

        result = _get_shipment_items(
            shipment_id=shipment_id,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Shipment Items DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/sync/shipments', methods=['POST'])
@login_required
def sync_amazon_shipments():
    """
    手动触发货件数据同步（从 API 写入数据库）
    请求体可选:
        status_list      - 状态列表，如 ["WORKING", "SHIPPED"]
        last_update_after - 开始时间，ISO8601
    """
    try:
        data = request.get_json() or {}

        result = _sync_all_shipments(
            shipment_status_list=data.get('status_list'),
            last_update_after=data.get('last_update_after')
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 货件同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/sync/shipments/<shipment_id>/items', methods=['POST'])
@login_required
def sync_amazon_shipment_items(shipment_id):
    """
    手动触发指定货件的商品数据同步（从 API 写入数据库）
    """
    try:
        result = _sync_shipment_items_by_id(shipment_id)

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 货件商品同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/sync/all', methods=['POST'])
@login_required
def sync_amazon_all():
    """
    一键同步所有 Amazon 数据（库存 + 货件 + 货件商品）
    """
    try:
        result = _sync_all()

        return jsonify({
            "status": "success",
            "message": "全量同步完成",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 全量同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/shipments/<shipment_id>/labels', methods=['GET'])
@login_required
def amazon_shipment_labels(shipment_id):
    """
    获取指定货件的 FBA 箱贴标签
    查询参数:
        box_id      - 指定箱子编号，传了则打印单箱唛；不传则打印该货件所有箱子
        page_type   - 标签页面类型，默认 PackageLabel_Thermal_NonPCP
        label_type  - 标签类型，默认 UNIQUE
    """
    try:
        page_type = request.args.get('page_type', 'PackageLabel_Thermal_NonPCP').strip() or 'PackageLabel_Thermal_NonPCP'
        label_type = request.args.get('label_type', 'UNIQUE').strip() or 'UNIQUE'
        box_id = request.args.get('box_id', '').strip() or None

        if box_id:
            box_ids = [box_id]
        else:
            box_ids = _get_box_ids_by_shipment_id(shipment_id)
            if not box_ids:
                return jsonify({
                    "status": "error",
                    "message": f"未找到货件 {shipment_id} 的箱子记录，请先同步入库计划箱子数据"
                }), 404

        client = _get_client()
        labels = client.get_shipment_labels(
            shipment_id=shipment_id,
            carton_ids=box_ids,
            page_type=page_type,
            label_type=label_type
        )

        return jsonify({
            "status": "success",
            "data": labels
        })

    except Exception as e:
        print(f"[Amazon Shipment Labels] 获取异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 分割线 ====================


# ==================== 同步与数据库操作 ====================

def _get_box_ids_by_shipment_id(shipment_id):
    """
    根据货件编号从 amazon_inbound_plan_boxes 表查询所有 box_id
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT box_id FROM amazon_inbound_plan_boxes
                WHERE shipment_id = %s AND box_id IS NOT NULL AND box_id != ''
                ORDER BY box_id
            """
            cursor.execute(sql, (shipment_id,))
            rows = cursor.fetchall()
            return [row['box_id'] for row in rows]
    finally:
        conn.close()


def _sync_all_shipments(
    shipment_status_list=None,
    last_update_after=None,
    last_update_before=None
):
    """同步货件列表数据（自动处理分页）"""
    client = _get_client()
    all_shipments = []
    seen_shipment_ids = set()
    next_token = None
    page = 0
    max_pages = 50

    if last_update_after or last_update_before:
        query_type = "DATE_RANGE"
        if not last_update_before:
            last_update_before = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    elif shipment_status_list:
        query_type = "SHIPMENT"
    else:
        query_type = "SHIPMENT"
        shipment_status_list = ['WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED', 'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN']

    try:
        while True:
            page += 1
            if page > max_pages:
                print(f"[Shipments Sync] 达到最大分页限制 {max_pages}，停止获取")
                break

            print(f"[Shipments Sync] 正在获取第 {page} 页...")

            result = client.get_shipments(
                shipment_status_list=shipment_status_list,
                last_update_after=last_update_after,
                last_update_before=last_update_before,
                query_type=query_type,
                next_token=next_token
            )

            payload = result.get('payload', {})
            shipments = payload.get('ShipmentData', [])
            page_ids = [s.get('ShipmentId') for s in shipments]
            print(f"[Shipments Sync] 第 {page} 页 ShipmentIds: {page_ids}")

            new_shipments = []
            for s in shipments:
                sid = s.get('ShipmentId')
                if sid and sid not in seen_shipment_ids:
                    seen_shipment_ids.add(sid)
                    new_shipments.append(s)

            all_shipments.extend(new_shipments)
            print(f"[Shipments Sync] 第 {page} 页原始 {len(shipments)} 条，新增 {len(new_shipments)} 条，累计不重复 {len(all_shipments)} 条")

            if len(shipments) > 0 and len(new_shipments) == 0:
                print(f"[Shipments Sync] 检测到重复数据，终止分页")
                break

            next_token = payload.get('NextToken')
            print(f"[Shipments Sync] NextToken: {'有' if next_token else '无'} ({str(next_token)[:30]}..." if next_token else "[Shipments Sync] NextToken: 无")
            if not next_token:
                print(f"[Shipments Sync] 分页结束，共 {page} 页")
                break

            time.sleep(0.5)

    except Exception as e:
        print(f"[Shipments Sync] 异常: {e}")
        return {
            "synced_count": 0,
            "total_fetched": len(all_shipments),
            "error": str(e)
        }

    synced_count, error = sync_shipments_to_db(MARKETPLACE_ID, all_shipments)

    warehouse_ids = list({
        s.get('DestinationFulfillmentCenterId')
        for s in all_shipments
        if s.get('DestinationFulfillmentCenterId')
    })
    if warehouse_ids:
        w_count, w_error = sync_fba_warehouses_to_db(MARKETPLACE_ID, warehouse_ids)
        print(f"[Shipments Sync] 同步仓库 {len(warehouse_ids)} 个，成功 {w_count} 个，错误: {w_error}")

    return {
        "synced_count": synced_count,
        "total_fetched": len(all_shipments),
        "error": error
    }


def _get_shipments(shipment_status=None, destination_fc=None, page=1, page_size=20):
    """从数据库查询货件列表（支持分页）"""
    return get_shipments_from_db(
        marketplace_id=MARKETPLACE_ID,
        shipment_status=shipment_status,
        destination_fc=destination_fc,
        page=page,
        page_size=page_size
    )


def _get_fba_warehouses():
    """从数据库查询 FBA 仓库列表"""
    return get_fba_warehouses_from_db(marketplace_id=MARKETPLACE_ID)


def _sync_shipment_items_by_id(shipment_id):
    """同步指定货件的商品数据"""
    client = _get_client()
    try:
        result = client.get_shipment_items(shipment_id)
        payload = result.get('payload', {})
        items = payload.get('ItemData', [])

        synced_count, error = sync_shipment_items_to_db(shipment_id, items)

        return {
            "synced_count": synced_count,
            "total_fetched": len(items),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": 0,
            "error": str(e)
        }


def _sync_all_shipment_items(shipment_ids):
    """批量同步多个货件的商品数据"""
    total_synced = 0
    errors = []

    for shipment_id in shipment_ids:
        result = _sync_shipment_items_by_id(shipment_id)
        total_synced += result.get('synced_count', 0)
        if result.get('error'):
            errors.append({"shipment_id": shipment_id, "error": result['error']})
        time.sleep(0.3)

    return {
        "total_synced": total_synced,
        "errors": errors
    }


def _get_shipment_items(shipment_id=None, seller_sku=None, page=1, page_size=20):
    """从数据库查询货件商品（支持分页）"""
    return get_shipment_items_from_db(
        shipment_id=shipment_id,
        seller_sku=seller_sku,
        page=page,
        page_size=page_size
    )


def _get_shipment_items_for_shipments(shipment_ids):
    """根据货件ID列表批量查询商品（不分页，返回全部）"""
    return get_shipment_items_by_shipment_ids_from_db(shipment_ids)


def _sync_all(sync_inventory_flag=True, sync_shipments_flag=True, sync_items_flag=True,
              sync_inbound_plans_flag=True, sync_inbound_boxes_flag=True):
    """一键同步所有数据"""
    from blueprints.amazon.inventory import _sync_inventory
    from blueprints.amazon.inbound_plans import _sync_inbound_plans, _sync_all_inbound_plan_boxes

    results = {}

    if sync_inventory_flag:
        print("=" * 50)
        print("开始同步库存数据...")
        results['inventory'] = _sync_inventory()
        print(f"库存同步完成: {results['inventory']}")

    if sync_shipments_flag:
        print("=" * 50)
        print("开始同步货件数据...")
        last_update_after = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        default_statuses = ['WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED', 'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN']
        results['shipments'] = _sync_all_shipments(
            shipment_status_list=default_statuses,
            last_update_after=last_update_after
        )
        print(f"货件同步完成: {results['shipments']}")

    if sync_items_flag and results.get('shipments', {}).get('synced_count', 0) > 0:
        print("=" * 50)
        print("开始同步货件商品数据...")
        shipment_list = _get_shipments(page=1, page_size=1000)
        shipment_ids = [s['shipment_id'] for s in shipment_list.get('list', [])]
        results['shipment_items'] = _sync_all_shipment_items(shipment_ids)
        print(f"货件商品同步完成: {results['shipment_items']}")

    if sync_inbound_plans_flag:
        print("=" * 50)
        print("开始同步入库计划数据...")
        results['inbound_plans'] = _sync_inbound_plans()
        print(f"入库计划同步完成: {results['inbound_plans']}")

    if sync_inbound_boxes_flag and results.get('inbound_plans', {}).get('synced_count', 0) > 0:
        print("=" * 50)
        print("开始同步入库计划箱子数据...")
        results['inbound_plan_boxes'] = _sync_all_inbound_plan_boxes()
        print(f"入库计划箱子同步完成: {results['inbound_plan_boxes']}")

    return results


# ==================== 数据库操作 ====================

def sync_shipments_to_db(marketplace_id, shipment_data):
    """
    同步货件列表数据到数据库
    """
    if not shipment_data:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for shipment in shipment_data:
                address = shipment.get('ShipFromAddress', {})

                sql = """
                    INSERT INTO amazon_shipments (
                        marketplace_id, shipment_id, shipment_name,
                        ship_from_name, ship_from_address_line1, ship_from_address_line2,
                        ship_from_district, ship_from_city, ship_from_state,
                        ship_from_country, ship_from_postal_code,
                        destination_fulfillment_center_id, shipment_status,
                        label_prep_type, box_contents_source, sync_time
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        shipment_name = VALUES(shipment_name),
                        ship_from_name = VALUES(ship_from_name),
                        ship_from_address_line1 = VALUES(ship_from_address_line1),
                        ship_from_address_line2 = VALUES(ship_from_address_line2),
                        ship_from_district = VALUES(ship_from_district),
                        ship_from_city = VALUES(ship_from_city),
                        ship_from_state = VALUES(ship_from_state),
                        ship_from_country = VALUES(ship_from_country),
                        ship_from_postal_code = VALUES(ship_from_postal_code),
                        destination_fulfillment_center_id = VALUES(destination_fulfillment_center_id),
                        shipment_status = VALUES(shipment_status),
                        label_prep_type = VALUES(label_prep_type),
                        box_contents_source = VALUES(box_contents_source),
                        sync_time = NOW()
                """

                params = (
                    marketplace_id,
                    shipment.get('ShipmentId'),
                    shipment.get('ShipmentName'),
                    address.get('Name'),
                    address.get('AddressLine1'),
                    address.get('AddressLine2'),
                    address.get('DistrictOrCounty'),
                    address.get('City'),
                    address.get('StateOrProvinceCode'),
                    address.get('CountryCode'),
                    address.get('PostalCode'),
                    shipment.get('DestinationFulfillmentCenterId'),
                    shipment.get('ShipmentStatus'),
                    shipment.get('LabelPrepType'),
                    shipment.get('BoxContentsSource'),
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


def get_shipments_from_db(marketplace_id=None, shipment_status=None, destination_fc=None, page=1, page_size=20):
    """
    从数据库分页查询货件列表数据
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if shipment_status:
                conditions.append("shipment_status = %s")
                params.append(shipment_status)
            if destination_fc:
                conditions.append("destination_fulfillment_center_id = %s")
                params.append(destination_fc)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_shipments WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT * FROM amazon_shipments
                WHERE {where_clause}
                ORDER BY sync_time DESC
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


def sync_fba_warehouses_to_db(marketplace_id, warehouse_ids):
    """
    同步 FBA 仓库列表到数据库
    """
    if not warehouse_ids:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO fba_warehouses (warehouse_id, marketplace_id, sync_time)
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE sync_time = NOW()
            """
            for wid in warehouse_ids:
                if wid:
                    cursor.execute(sql, (wid, marketplace_id))
                    count += 1
            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def get_fba_warehouses_from_db(marketplace_id=None):
    """
    查询 FBA 仓库列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if marketplace_id:
                sql = """
                    SELECT warehouse_id, marketplace_id, sync_time
                    FROM fba_warehouses
                    WHERE marketplace_id = %s
                    ORDER BY warehouse_id
                """
                cursor.execute(sql, (marketplace_id,))
            else:
                sql = """
                    SELECT warehouse_id, marketplace_id, sync_time
                    FROM fba_warehouses
                    ORDER BY warehouse_id
                """
                cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()


def sync_shipment_items_to_db(shipment_id, item_data):
    """
    同步货件商品数据到数据库
    """
    if not item_data:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for item in item_data:
                prep_details = item.get('PrepDetailsList', [])
                prep_json = json.dumps(prep_details) if prep_details else '[]'

                sql = """
                    INSERT INTO amazon_shipment_items (
                        shipment_id, fulfillment_network_sku, seller_sku,
                        quantity_in_case, quantity_received, quantity_shipped,
                        prep_details, sync_time
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        quantity_in_case = VALUES(quantity_in_case),
                        quantity_received = VALUES(quantity_received),
                        quantity_shipped = VALUES(quantity_shipped),
                        prep_details = VALUES(prep_details),
                        sync_time = NOW()
                """

                params = (
                    shipment_id,
                    item.get('FulfillmentNetworkSKU'),
                    item.get('SellerSKU'),
                    item.get('QuantityInCase', 0),
                    item.get('QuantityReceived', 0),
                    item.get('QuantityShipped', 0),
                    prep_json,
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


def get_shipment_items_from_db(shipment_id=None, seller_sku=None, page=1, page_size=20):
    """
    从数据库分页查询货件商品数据
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if shipment_id:
                conditions.append("shipment_id = %s")
                params.append(shipment_id)
            if seller_sku:
                conditions.append("seller_sku = %s")
                params.append(seller_sku)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_shipment_items WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    s.*,
                    p.product_name,
                    p.declare_name_cn,
                    p.declare_name_en
                FROM amazon_shipment_items s
                LEFT JOIN products p ON s.seller_sku = p.seller_sku
                WHERE {where_clause}
                ORDER BY s.sync_time DESC
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


def get_shipment_items_by_shipment_ids_from_db(shipment_ids):
    """
    根据货件ID列表批量查询商品数据
    """
    if not shipment_ids:
        return []

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(shipment_ids))
            sql = f"""
                SELECT * FROM amazon_shipment_items
                WHERE shipment_id IN ({placeholders})
                ORDER BY shipment_id, seller_sku
            """
            cursor.execute(sql, tuple(shipment_ids))
            return cursor.fetchall()
    finally:
        conn.close()
