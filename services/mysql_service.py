"""
MySQL 数据库服务模块
统一的数据库连接管理和常用数据库操作
其他服务模块应从此模块导入 get_db_connection，而非从 geminiAi/doubaoAI
"""
import os
import json
import pymysql
from dotenv import load_dotenv


def get_db_connection():
    """获取 MySQL 数据库连接"""
    load_dotenv(override=True)
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "remote_user"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "ai_image_project"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


# ==================== ai_images 表操作 ====================

def save_image_to_db(image_id, url, local_path, prompt, history, user_id=None, model=None):
    """
    保存 AI 生成图片信息到数据库
    兼容 geminiAi 和 doubaoAI 的调用方式
    
    Args:
        model: 模型名称（可选，doubaoAI 会传入，目前暂存入 history_snapshot）
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 序列化历史记录
            history_data = []
            if isinstance(history, list):
                for h in history:
                    if hasattr(h, 'to_json'):
                        history_data.append(json.loads(h.to_json()))
                    elif isinstance(h, dict):
                        history_data.append(h)
                    else:
                        try:
                            history_data.append(json.loads(json.dumps(h, default=lambda o: o.__dict__)))
                        except:
                            continue
            elif isinstance(history, dict):
                history_data = [history]

            history_json = json.dumps(history_data)

            sql = """INSERT INTO ai_images (id, user_id, image_url, local_path, prompt, history_snapshot)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (image_id, user_id, url, local_path, prompt, history_json))
        conn.commit()
        print(f"📖 数据库记录已同步: Image ID {image_id}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()


def get_image_relative_path_by_id(image_id):
    """根据 ID 从数据库获取图片的本地存储相对路径"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT local_path FROM ai_images WHERE id = %s"
            cursor.execute(sql, (image_id,))
            result = cursor.fetchone()
            if result:
                return result['local_path']
            return None
    finally:
        conn.close()


# ==================== Amazon SP-API 数据操作 ====================

def sync_inventory_summaries(marketplace_id, inventory_items):
    """
    同步库存汇总数据到数据库
    
    Args:
        marketplace_id: 市场ID，如 ATVPDKIKX0DER
        inventory_items: get_inventory_summaries 返回的 inventorySummaries 列表
    
    Returns:
        tuple: (成功插入/更新数量, 错误信息)
    """
    if not inventory_items:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for item in inventory_items:
                details = item.get('inventoryDetails', {})
                reserved = details.get('reservedQuantity', {})
                researching = details.get('researchingQuantity', {})
                unfulfillable = details.get('unfulfillableQuantity', {})
                future_supply = details.get('futureSupplyQuantity', {})

                # researchingQuantityBreakdown 转为 JSON
                researching_breakdown = researching.get('researchingQuantityBreakdown', [])

                sql = """
                    INSERT INTO amazon_inventory (
                        marketplace_id, asin, fn_sku, seller_sku, condition_status,
                        fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity,
                        inbound_receiving_quantity, reserved_total, reserved_pending_customer_order,
                        reserved_pending_transshipment, reserved_fc_processing,
                        researching_total, researching_breakdown,
                        unfulfillable_total, unfulfillable_customer_damaged, unfulfillable_warehouse_damaged,
                        unfulfillable_distributor_damaged, unfulfillable_carrier_damaged,
                        unfulfillable_defective, unfulfillable_expired,
                        future_supply_reserved, future_supply_buyable,
                        last_updated_time, product_name, total_quantity, stores, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        condition_status = VALUES(condition_status),
                        fulfillable_quantity = VALUES(fulfillable_quantity),
                        inbound_working_quantity = VALUES(inbound_working_quantity),
                        inbound_shipped_quantity = VALUES(inbound_shipped_quantity),
                        inbound_receiving_quantity = VALUES(inbound_receiving_quantity),
                        reserved_total = VALUES(reserved_total),
                        reserved_pending_customer_order = VALUES(reserved_pending_customer_order),
                        reserved_pending_transshipment = VALUES(reserved_pending_transshipment),
                        reserved_fc_processing = VALUES(reserved_fc_processing),
                        researching_total = VALUES(researching_total),
                        researching_breakdown = VALUES(researching_breakdown),
                        unfulfillable_total = VALUES(unfulfillable_total),
                        unfulfillable_customer_damaged = VALUES(unfulfillable_customer_damaged),
                        unfulfillable_warehouse_damaged = VALUES(unfulfillable_warehouse_damaged),
                        unfulfillable_distributor_damaged = VALUES(unfulfillable_distributor_damaged),
                        unfulfillable_carrier_damaged = VALUES(unfulfillable_carrier_damaged),
                        unfulfillable_defective = VALUES(unfulfillable_defective),
                        unfulfillable_expired = VALUES(unfulfillable_expired),
                        future_supply_reserved = VALUES(future_supply_reserved),
                        future_supply_buyable = VALUES(future_supply_buyable),
                        last_updated_time = VALUES(last_updated_time),
                        product_name = VALUES(product_name),
                        total_quantity = VALUES(total_quantity),
                        stores = VALUES(stores),
                        sync_time = NOW()
                """

                # 处理 lastUpdatedTime 格式（去掉 Z 后缀，转为 MySQL DATETIME 格式）
                last_updated = item.get('lastUpdatedTime')
                if last_updated == '' or last_updated is None:
                    last_updated = None
                elif isinstance(last_updated, str):
                    # 去掉 ISO 8601 的 Z 后缀，如 2026-04-28T13:13:51Z -> 2026-04-28T13:13:51
                    last_updated = last_updated.replace('Z', '')
                    # 如果有时区偏移（如 +00:00），也去掉
                    if '+' in last_updated:
                        last_updated = last_updated.split('+')[0]

                stores = item.get('stores', [])
                stores_json = json.dumps(stores) if stores else '[]'

                params = (
                    marketplace_id,
                    item.get('asin'),
                    item.get('fnSku'),
                    item.get('sellerSku'),
                    item.get('condition'),
                    details.get('fulfillableQuantity', 0),
                    details.get('inboundWorkingQuantity', 0),
                    details.get('inboundShippedQuantity', 0),
                    details.get('inboundReceivingQuantity', 0),
                    reserved.get('totalReservedQuantity', 0),
                    reserved.get('pendingCustomerOrderQuantity', 0),
                    reserved.get('pendingTransshipmentQuantity', 0),
                    reserved.get('fcProcessingQuantity', 0),
                    researching.get('totalResearchingQuantity', 0),
                    json.dumps(researching_breakdown),
                    unfulfillable.get('totalUnfulfillableQuantity', 0),
                    unfulfillable.get('customerDamagedQuantity', 0),
                    unfulfillable.get('warehouseDamagedQuantity', 0),
                    unfulfillable.get('distributorDamagedQuantity', 0),
                    unfulfillable.get('carrierDamagedQuantity', 0),
                    unfulfillable.get('defectiveQuantity', 0),
                    unfulfillable.get('expiredQuantity', 0),
                    future_supply.get('reservedFutureSupplyQuantity', 0),
                    future_supply.get('futureSupplyBuyableQuantity', 0),
                    last_updated,
                    item.get('productName'),
                    item.get('totalQuantity', 0),
                    stores_json,
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


def get_inventory_summaries_from_db(
    marketplace_id=None,
    seller_sku=None,
    asin=None,
    page=1,
    page_size=20
):
    """
    从数据库分页查询库存汇总数据
    
    Returns:
        dict: {list, total, page, page_size}
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if seller_sku:
                conditions.append("seller_sku = %s")
                params.append(seller_sku)
            if asin:
                conditions.append("asin = %s")
                params.append(asin)

            where_clause = " AND ".join(conditions)

            # 统计总数
            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_inventory WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            # 分页查询（连表 products 获取产品信息）
            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    i.*,
                    p.product_name,
                    p.declare_name_cn,
                    p.declare_name_en
                FROM amazon_inventory i
                LEFT JOIN products p ON i.seller_sku = p.seller_sku
                WHERE {where_clause}
                ORDER BY i.sync_time DESC
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


def sync_shipments(marketplace_id, shipment_data):
    """
    同步货件列表数据到数据库
    
    Args:
        marketplace_id: 市场ID
        shipment_data: get_shipments 返回的 ShipmentData 列表
    
    Returns:
        tuple: (成功数量, 错误信息)
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


def get_shipments_from_db(
    marketplace_id=None,
    shipment_status=None,
    destination_fc=None,
    page=1,
    page_size=20
):
    """
    从数据库分页查询货件列表数据
    
    Returns:
        dict: {list, total, page, page_size}
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


def sync_shipment_items(shipment_id, item_data):
    """
    同步货件商品数据到数据库
    
    Args:
        shipment_id: 货件ID
        item_data: get_shipment_items 返回的 ItemData 列表
    
    Returns:
        tuple: (成功数量, 错误信息)
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


def get_shipment_items_from_db(
    shipment_id=None,
    seller_sku=None,
    page=1,
    page_size=20
):
    """
    从数据库分页查询货件商品数据
    
    Returns:
        dict: {list, total, page, page_size}
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


def get_shipment_items_by_shipment_ids(shipment_ids):
    """
    根据货件ID列表批量查询商品数据
    
    Args:
        shipment_ids: 货件ID列表
    
    Returns:
        list: 商品数据列表
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


# ==================== FBA 仓库表操作 ====================

def sync_fba_warehouses(marketplace_id, warehouse_ids):
    """
    同步 FBA 仓库列表到数据库
    
    Args:
        marketplace_id: 市场ID
        warehouse_ids: 仓库代码列表，如 ['IND9', 'SCK4']
    
    Returns:
        tuple: (成功插入/更新数量, 错误信息)
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


def get_fba_warehouses(marketplace_id=None):
    """
    查询 FBA 仓库列表
    
    Args:
        marketplace_id: 市场ID，可选
    
    Returns:
        list: 仓库数据列表
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
