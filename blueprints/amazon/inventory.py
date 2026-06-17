"""
Amazon 库存模块（多店铺支持版）
提供库存查询与同步路由，以及底层数据库操作

注意：所有接口必须传入 shop_id，不传直接返回 400
"""
import os
import time
import json

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_sp_api_client
from services.mysql_service import get_db_connection
from services.notification_dispatcher import fire

amazon_inventory_bp = Blueprint('amazon_inventory', __name__, url_prefix='/api')


def _require_shop_id() -> int:
    """
    强制获取 shop_id，不传则抛异常
    用于路由层参数校验
    """
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


def _require_shop_id_from_body(data: dict) -> int:
    """
    从请求体中强制获取 shop_id，不传则抛异常
    """
    shop_id = data.get('shop_id')
    if shop_id is None or shop_id == '':
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except (ValueError, TypeError):
        raise ValueError("shop_id 必须是整数")


# ==================== 路由（前端调用）====================

@amazon_inventory_bp.route('/amazon/inventory', methods=['GET'])
@login_required
@permission_required('amazon_inventory:page')
def amazon_inventory():
    """
    从数据库分页查询库存汇总数据
    查询参数（必填）:
        shop_id      - 店铺ID
    查询参数（可选）:
        seller_sku   - 按卖家SKU筛选
        asin         - 按ASIN筛选
        page         - 页码，默认 1
        page_size    - 每页数量，默认 20
    """
    try:
        shop_id = _require_shop_id()
        seller_sku = request.args.get('seller_sku', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_inventory(
            shop_id=shop_id,
            seller_sku=seller_sku,
            asin=asin,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Inventory DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inventory_bp.route('/amazon/sync/inventory', methods=['POST'])
@login_required
@permission_required('amazon_inventory:sync')
def sync_amazon_inventory():
    """
    手动触发库存数据同步（从 API 写入数据库）
    请求体（必填）:
        shop_id          - 店铺ID
    请求体（可选）:
        seller_skus      - SKU列表，如 ["SKU1", "SKU2"]
        start_date_time  - 开始时间，ISO8601
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        result = _sync_inventory(
            shop_id=shop_id,
            seller_skus=data.get('seller_skus'),
            start_date_time=data.get('start_date_time'),
            details=True
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Sync] 库存同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 同步与数据库操作 ====================

def _sync_inventory(shop_id, seller_skus=None, start_date_time=None, details=True):
    """
    同步库存汇总数据（自动处理分页）
    参数:
        shop_id: 店铺ID（必填）
    """
    client = get_sp_api_client(shop_id=shop_id)
    all_items = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Inventory Sync][shop_id={shop_id}] 正在获取第 {page} 页...")

            result = client.get_inventory_summaries(
                seller_skus=seller_skus,
                details=details,
                start_date_time=start_date_time,
                next_token=next_token
            )

            payload = result.get('payload', {})
            items = payload.get('inventorySummaries', [])
            all_items.extend(items)

            next_token = payload.get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_inventory_summaries_to_db(shop_id, client.marketplace_id, all_items)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_items),
            "error": error,
            "next_token": None
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_items),
            "error": str(e),
            "next_token": next_token
        }


def _get_inventory(shop_id, seller_sku=None, asin=None, page=1, page_size=20):
    """从数据库查询库存数据（支持分页）"""
    return get_inventory_summaries_from_db(
        shop_id=shop_id,
        seller_sku=seller_sku,
        asin=asin,
        page=page,
        page_size=page_size
    )


def sync_inventory_summaries_to_db(shop_id, marketplace_id, inventory_items):
    """
    同步库存汇总数据到数据库（多店铺版）
    注意：需要确保 amazon_inventory 表有 (shop_id, seller_sku) 的唯一索引，
          否则不同店铺相同 SKU 会触发 ON DUPLICATE KEY 冲突。
    """
    if not inventory_items:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT seller_sku, fulfillable_quantity FROM amazon_inventory WHERE shop_id = %s",
                (shop_id,)
            )
            old_stock = {row['seller_sku']: (row['fulfillable_quantity'] or 0) for row in cursor.fetchall()}

            for item in inventory_items:
                details = item.get('inventoryDetails', {})
                reserved = details.get('reservedQuantity', {})
                researching = details.get('researchingQuantity', {})
                unfulfillable = details.get('unfulfillableQuantity', {})
                future_supply = details.get('futureSupplyQuantity', {})

                researching_breakdown = researching.get('researchingQuantityBreakdown', [])

                sql = """
                    INSERT INTO amazon_inventory (
                        shop_id, marketplace_id, asin, fn_sku, seller_sku, condition_status,
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
                        %s, %s, %s, %s, %s, %s,
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

                last_updated = item.get('lastUpdatedTime')
                if last_updated == '' or last_updated is None:
                    last_updated = None
                elif isinstance(last_updated, str):
                    last_updated = last_updated.replace('Z', '')
                    if '+' in last_updated:
                        last_updated = last_updated.split('+')[0]

                stores = item.get('stores', [])
                stores_json = json.dumps(stores) if stores else '[]'

                params = (
                    shop_id,
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

                sku = item.get('sellerSku')
                new_qty = details.get('fulfillableQuantity', 0) or 0
                old_qty = old_stock.get(sku, 0)
                if (old_qty == 0 and new_qty > 0) or (old_qty > 0 and new_qty == 0):
                    fire('inventory_stock_changed',
                         shop_id=shop_id,
                         sku=sku,
                         asin=item.get('asin', ''),
                         product_name=item.get('productName', ''),
                         old_qty=old_qty,
                         new_qty=new_qty)

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def get_inventory_summaries_from_db(shop_id, marketplace_id=None, seller_sku=None, asin=None, page=1, page_size=20):
    """
    从数据库分页查询库存汇总数据（多店铺版）
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s"]
            params = [shop_id]

            if marketplace_id:
                conditions.append("i.marketplace_id = %s")
                params.append(marketplace_id)
            if seller_sku:
                conditions.append("i.seller_sku = %s")
                params.append(seller_sku)
            if asin:
                conditions.append("i.asin = %s")
                params.append(asin)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_inventory i WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    i.*,
                    p.product_name as local_product_name,
                    p.declare_name_cn,
                    p.declare_name_en
                FROM amazon_inventory i
                LEFT JOIN products p ON i.seller_sku = p.seller_sku
                WHERE {where_clause}
                ORDER BY p.created_at IS NULL, p.created_at DESC
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
