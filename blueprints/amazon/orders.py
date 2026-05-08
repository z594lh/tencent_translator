"""
Amazon 订单模块
提供订单查询与同步路由，以及底层数据库操作
"""
import os
import time
import json

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient
from services.mysql_service import get_db_connection

amazon_orders_bp = Blueprint('amazon_orders', __name__, url_prefix='/api')

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


def _get_client(marketplace_id=None, region=None):
    """获取 Amazon SP-API 客户端实例"""
    return AmazonSpApiClient(
        marketplace_id=marketplace_id or MARKETPLACE_ID,
        region=region
    )


# ==================== 路由（前端调用）====================

@amazon_orders_bp.route('/amazon/orders', methods=['GET'])
@login_required
def amazon_orders():
    """
    从数据库分页查询订单列表
    查询参数:
        order_status       - 按订单状态筛选，如 Pending, Unshipped, Shipped, Canceled
        amazon_order_id    - 按订单号精确筛选
        buyer_name         - 按买家姓名模糊筛选
        purchase_date_from - 下单开始日期，格式 YYYY-MM-DD
        purchase_date_to   - 下单结束日期，格式 YYYY-MM-DD
        page               - 页码，默认 1
        page_size          - 每页数量，默认 20
    """
    try:
        order_status = request.args.get('order_status', '').strip() or None
        amazon_order_id = request.args.get('amazon_order_id', '').strip() or None
        buyer_name = request.args.get('buyer_name', '').strip() or None
        purchase_date_from = request.args.get('purchase_date_from', '').strip() or None
        purchase_date_to = request.args.get('purchase_date_to', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = _get_orders(
            order_status=order_status,
            amazon_order_id=amazon_order_id,
            buyer_name=buyer_name,
            purchase_date_from=purchase_date_from,
            purchase_date_to=purchase_date_to,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Orders DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_orders_bp.route('/amazon/orders/<order_id>', methods=['GET'])
@login_required
def amazon_order_detail(order_id):
    """
    从数据库查询单个订单详情（含商品列表）
    """
    try:
        result = _get_order_detail(order_id=order_id)

        if not result:
            return jsonify({"status": "error", "message": "订单不存在"}), 404

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Order Detail DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_orders_bp.route('/amazon/sync/orders', methods=['POST'])
@login_required
def sync_amazon_orders():
    """
    手动触发订单数据同步（从 API 写入数据库）
    请求体可选:
        created_after      - 创建开始时间，ISO8601，默认 30 天前
        created_before     - 创建结束时间，ISO8601
        order_statuses     - 状态列表，如 ["Unshipped", "Shipped"]
        marketplace_ids    - 市场ID列表
    """
    try:
        data = request.get_json() or {}

        from datetime import datetime, timedelta
        default_after = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

        result = _sync_orders(
            created_after=data.get('created_after', default_after),
            created_before=data.get('created_before'),
            order_statuses=data.get('order_statuses'),
            marketplace_ids=data.get('marketplace_ids')
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条订单",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 订单同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_orders_bp.route('/amazon/sync/orders/<order_id>/items', methods=['POST'])
@login_required
def sync_amazon_order_items(order_id):
    """
    手动触发指定订单的商品数据同步（从 API 写入数据库）
    """
    try:
        result = _sync_order_items(order_id)

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条商品",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 订单商品同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_orders_bp.route('/amazon/sync/orders-all', methods=['POST'])
@login_required
def sync_amazon_orders_all():
    """
    一键同步订单及其商品数据
    先同步订单列表，再同步所有订单的商品明细
    """
    try:
        data = request.get_json() or {}

        from datetime import datetime, timedelta
        default_after = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

        orders_result = _sync_orders(
            created_after=data.get('created_after', default_after),
            created_before=data.get('created_before'),
            order_statuses=data.get('order_statuses'),
            marketplace_ids=data.get('marketplace_ids')
        )

        order_ids = _get_order_ids(
            order_statuses=data.get('order_statuses')
        )

        items_total = 0
        items_errors = []

        for oid in order_ids:
            result = _sync_order_items(oid)
            items_total += result.get('synced_count', 0)
            if result.get('error'):
                items_errors.append({"order_id": oid, "error": result['error']})
            time.sleep(0.3)

        return jsonify({
            "status": "success",
            "message": "订单全量同步完成",
            "data": {
                "orders": orders_result,
                "items_synced": items_total,
                "items_errors": items_errors
            }
        })

    except Exception as e:
        print(f"[Amazon Sync] 订单全量同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 分割线 ====================


# ==================== 同步与数据库操作 ====================

def _sync_orders(created_after=None, created_before=None, last_updated_after=None, order_statuses=None, marketplace_ids=None):
    """同步订单列表（自动处理分页）"""
    client = _get_client()
    all_orders = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Orders Sync] 正在获取第 {page} 页...")

            result = client.get_orders(
                created_after=created_after,
                created_before=created_before,
                last_updated_after=last_updated_after,
                order_statuses=order_statuses,
                marketplace_ids=marketplace_ids,
                max_results=100,
                next_token=next_token
            )

            payload = result.get('payload', {})
            orders = payload.get('Orders', [])
            all_orders.extend(orders)

            next_token = payload.get('NextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_orders_to_db(MARKETPLACE_ID, all_orders)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_orders),
            "error": error,
            "next_token": None
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_orders),
            "error": str(e),
            "next_token": next_token
        }


def _sync_order_items(order_id):
    """同步指定订单的商品列表（自动处理分页）"""
    client = _get_client()
    all_items = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Order Items Sync] Order {order_id} 正在获取第 {page} 页...")

            result = client.get_order_items(order_id)
            payload = result.get('payload', {})
            items = payload.get('OrderItems', [])
            all_items.extend(items)

            next_token = payload.get('NextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_order_items_to_db(order_id, MARKETPLACE_ID, all_items)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_items),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_items),
            "error": str(e)
        }


def _get_orders(order_status=None, amazon_order_id=None, buyer_name=None,
                purchase_date_from=None, purchase_date_to=None, page=1, page_size=20):
    """从数据库查询订单列表（支持分页）"""
    return get_orders_from_db(
        marketplace_id=MARKETPLACE_ID,
        order_status=order_status,
        amazon_order_id=amazon_order_id,
        buyer_name=buyer_name,
        purchase_date_from=purchase_date_from,
        purchase_date_to=purchase_date_to,
        page=page,
        page_size=page_size
    )


def _get_order_detail(order_id):
    """从数据库查询订单详情（含商品）"""
    return get_order_detail_from_db(order_id=order_id)


def _get_order_ids(order_statuses=None):
    """从数据库获取所有订单ID列表"""
    return get_order_ids_from_db(marketplace_id=MARKETPLACE_ID, order_statuses=order_statuses)


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


def _parse_money(money_obj):
    """解析金额对象，返回 (currency_code, amount)"""
    if not money_obj:
        return None, None
    return money_obj.get('CurrencyCode'), money_obj.get('Amount')


def sync_orders_to_db(marketplace_id, orders):
    """
    同步订单列表到数据库
    """
    if not orders:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for order in orders:
                shipping = order.get('ShippingAddress', {}) or {}
                buyer = order.get('BuyerInfo', {}) or {}
                buyer_tax = buyer.get('BuyerTaxInfo', {}) or {}
                default_ship = order.get('DefaultShipFromLocationAddress', {}) or {}
                fulfillment = order.get('FulfillmentInstruction', {}) or {}
                automated = order.get('AutomatedShippingSettings', {}) or {}
                order_total = order.get('OrderTotal', {}) or {}

                total_currency, total_amount = _parse_money(order_total)

                sql = """
                    INSERT INTO amazon_orders (
                        amazon_order_id, marketplace_id, seller_order_id,
                        purchase_date, last_update_date, order_status,
                        fulfillment_channel, sales_channel, order_channel,
                        ship_service_level, shipment_service_level_category, order_type,
                        number_of_items_shipped, number_of_items_unshipped,
                        order_total_currency_code, order_total_amount,
                        payment_method, payment_method_details,
                        earliest_ship_date, latest_ship_date, latest_delivery_date,
                        promise_response_due_date,
                        shipping_name, shipping_address_line1, shipping_address_line2,
                        shipping_address_line3, shipping_city, shipping_state_or_region,
                        shipping_postal_code, shipping_country_code, shipping_phone,
                        shipping_address_type,
                        buyer_email, buyer_name, buyer_tax_company_legal_name,
                        buyer_tax_taxing_region, buyer_tax_tax_classifications,
                        purchase_order_number,
                        default_ship_from_name, default_ship_from_address_line1,
                        default_ship_from_city, default_ship_from_state_or_region,
                        default_ship_from_postal_code, default_ship_from_country_code,
                        default_ship_from_phone, default_ship_from_address_type,
                        is_business_order, is_prime, is_global_express_enabled,
                        is_premium_order, is_sold_by_ab, is_iba, is_ispu,
                        is_access_point_order, is_replacement_order,
                        is_estimated_ship_date_set, has_regulated_items,
                        fulfillment_supply_source_id, automated_shipping_has_settings,
                        seller_note, easy_ship_shipment_status, electronic_invoice_status,
                        cba_displayable_shipping_label, regulated_information,
                        sync_time
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        seller_order_id = VALUES(seller_order_id),
                        purchase_date = VALUES(purchase_date),
                        last_update_date = VALUES(last_update_date),
                        order_status = VALUES(order_status),
                        fulfillment_channel = VALUES(fulfillment_channel),
                        sales_channel = VALUES(sales_channel),
                        order_channel = VALUES(order_channel),
                        ship_service_level = VALUES(ship_service_level),
                        shipment_service_level_category = VALUES(shipment_service_level_category),
                        order_type = VALUES(order_type),
                        number_of_items_shipped = VALUES(number_of_items_shipped),
                        number_of_items_unshipped = VALUES(number_of_items_unshipped),
                        order_total_currency_code = VALUES(order_total_currency_code),
                        order_total_amount = VALUES(order_total_amount),
                        payment_method = VALUES(payment_method),
                        payment_method_details = VALUES(payment_method_details),
                        earliest_ship_date = VALUES(earliest_ship_date),
                        latest_ship_date = VALUES(latest_ship_date),
                        latest_delivery_date = VALUES(latest_delivery_date),
                        promise_response_due_date = VALUES(promise_response_due_date),
                        shipping_name = VALUES(shipping_name),
                        shipping_address_line1 = VALUES(shipping_address_line1),
                        shipping_address_line2 = VALUES(shipping_address_line2),
                        shipping_address_line3 = VALUES(shipping_address_line3),
                        shipping_city = VALUES(shipping_city),
                        shipping_state_or_region = VALUES(shipping_state_or_region),
                        shipping_postal_code = VALUES(shipping_postal_code),
                        shipping_country_code = VALUES(shipping_country_code),
                        shipping_phone = VALUES(shipping_phone),
                        shipping_address_type = VALUES(shipping_address_type),
                        buyer_email = VALUES(buyer_email),
                        buyer_name = VALUES(buyer_name),
                        buyer_tax_company_legal_name = VALUES(buyer_tax_company_legal_name),
                        buyer_tax_taxing_region = VALUES(buyer_tax_taxing_region),
                        buyer_tax_tax_classifications = VALUES(buyer_tax_tax_classifications),
                        purchase_order_number = VALUES(purchase_order_number),
                        default_ship_from_name = VALUES(default_ship_from_name),
                        default_ship_from_address_line1 = VALUES(default_ship_from_address_line1),
                        default_ship_from_city = VALUES(default_ship_from_city),
                        default_ship_from_state_or_region = VALUES(default_ship_from_state_or_region),
                        default_ship_from_postal_code = VALUES(default_ship_from_postal_code),
                        default_ship_from_country_code = VALUES(default_ship_from_country_code),
                        default_ship_from_phone = VALUES(default_ship_from_phone),
                        default_ship_from_address_type = VALUES(default_ship_from_address_type),
                        is_business_order = VALUES(is_business_order),
                        is_prime = VALUES(is_prime),
                        is_global_express_enabled = VALUES(is_global_express_enabled),
                        is_premium_order = VALUES(is_premium_order),
                        is_sold_by_ab = VALUES(is_sold_by_ab),
                        is_iba = VALUES(is_iba),
                        is_ispu = VALUES(is_ispu),
                        is_access_point_order = VALUES(is_access_point_order),
                        is_replacement_order = VALUES(is_replacement_order),
                        is_estimated_ship_date_set = VALUES(is_estimated_ship_date_set),
                        has_regulated_items = VALUES(has_regulated_items),
                        fulfillment_supply_source_id = VALUES(fulfillment_supply_source_id),
                        automated_shipping_has_settings = VALUES(automated_shipping_has_settings),
                        seller_note = VALUES(seller_note),
                        easy_ship_shipment_status = VALUES(easy_ship_shipment_status),
                        electronic_invoice_status = VALUES(electronic_invoice_status),
                        cba_displayable_shipping_label = VALUES(cba_displayable_shipping_label),
                        regulated_information = VALUES(regulated_information),
                        sync_time = NOW()
                """

                tax_classifications = buyer_tax.get('TaxClassifications', [])
                payment_method_details = order.get('PaymentMethodDetails', [])
                regulated_info = order.get('RegulatedInformation', {})

                params = (
                    order.get('AmazonOrderId'),
                    marketplace_id,
                    order.get('SellerOrderId'),
                    _iso_to_datetime(order.get('PurchaseDate')),
                    _iso_to_datetime(order.get('LastUpdateDate')),
                    order.get('OrderStatus'),
                    order.get('FulfillmentChannel'),
                    order.get('SalesChannel'),
                    order.get('OrderChannel'),
                    order.get('ShipServiceLevel'),
                    order.get('ShipmentServiceLevelCategory'),
                    order.get('OrderType'),
                    order.get('NumberOfItemsShipped', 0),
                    order.get('NumberOfItemsUnshipped', 0),
                    total_currency,
                    total_amount,
                    order.get('PaymentMethod'),
                    json.dumps(payment_method_details) if payment_method_details else None,
                    _iso_to_datetime(order.get('EarliestShipDate')),
                    _iso_to_datetime(order.get('LatestShipDate')),
                    _iso_to_datetime(order.get('LatestDeliveryDate')),
                    _iso_to_datetime(order.get('PromiseResponseDueDate')),
                    shipping.get('Name'),
                    shipping.get('AddressLine1'),
                    shipping.get('AddressLine2'),
                    shipping.get('AddressLine3'),
                    shipping.get('City'),
                    shipping.get('StateOrRegion'),
                    shipping.get('PostalCode'),
                    shipping.get('CountryCode'),
                    shipping.get('Phone'),
                    shipping.get('AddressType'),
                    buyer.get('BuyerEmail'),
                    buyer.get('BuyerName'),
                    buyer_tax.get('CompanyLegalName'),
                    buyer_tax.get('TaxingRegion'),
                    json.dumps(tax_classifications) if tax_classifications else None,
                    buyer.get('PurchaseOrderNumber'),
                    default_ship.get('Name'),
                    default_ship.get('AddressLine1'),
                    default_ship.get('City'),
                    default_ship.get('StateOrRegion'),
                    default_ship.get('PostalCode'),
                    default_ship.get('CountryCode'),
                    default_ship.get('Phone'),
                    default_ship.get('AddressType'),
                    1 if order.get('IsBusinessOrder') else 0,
                    1 if order.get('IsPrime') else 0,
                    1 if order.get('IsGlobalExpressEnabled') else 0,
                    1 if order.get('IsPremiumOrder') else 0,
                    1 if order.get('IsSoldByAB') else 0,
                    1 if order.get('IsIBA') else 0,
                    1 if order.get('IsISPU') else 0,
                    1 if order.get('IsAccessPointOrder') else 0,
                    1 if order.get('IsReplacementOrder') else 0,
                    1 if order.get('IsEstimatedShipDateSet') else 0,
                    1 if order.get('HasRegulatedItems') else 0,
                    fulfillment.get('FulfillmentSupplySourceId'),
                    1 if automated.get('HasAutomatedShippingSettings') else 0,
                    order.get('SellerNote'),
                    order.get('EasyShipShipmentStatus'),
                    order.get('ElectronicInvoiceStatus'),
                    order.get('CbaDisplayableShippingLabel'),
                    json.dumps(regulated_info) if regulated_info else None,
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


def sync_order_items_to_db(order_id, marketplace_id, items):
    """
    同步订单商品列表到数据库
    """
    if not items:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for item in items:
                item_price = item.get('ItemPrice', {}) or {}
                shipping_price = item.get('ShippingPrice', {}) or {}
                gift_wrap_price = item.get('GiftWrapPrice', {}) or {}
                item_tax = item.get('ItemTax', {}) or {}
                shipping_tax = item.get('ShippingTax', {}) or {}
                gift_wrap_tax = item.get('GiftWrapTax', {}) or {}
                shipping_discount = item.get('ShippingDiscount', {}) or {}
                promotion_discount = item.get('PromotionDiscount', {}) or {}
                cod_fee = item.get('CODFee', {}) or {}
                cod_fee_discount = item.get('CODFeeDiscount', {}) or {}
                points = item.get('PointsGranted', {}) or {}
                buyer_info = item.get('BuyerInfo', {}) or {}
                buyer_customized = buyer_info.get('BuyerCustomizedInfo', {}) or {}
                buyer_cancel = item.get('BuyerRequestedCancel', {}) or {}

                item_price_currency, item_price_amount = _parse_money(item_price)
                shipping_price_currency, shipping_price_amount = _parse_money(shipping_price)
                gift_wrap_price_currency, gift_wrap_price_amount = _parse_money(gift_wrap_price)
                item_tax_currency, item_tax_amount = _parse_money(item_tax)
                shipping_tax_currency, shipping_tax_amount = _parse_money(shipping_tax)
                gift_wrap_tax_currency, gift_wrap_tax_amount = _parse_money(gift_wrap_tax)
                shipping_discount_currency, shipping_discount_amount = _parse_money(shipping_discount)
                promotion_discount_currency, promotion_discount_amount = _parse_money(promotion_discount)
                cod_fee_currency, cod_fee_amount = _parse_money(cod_fee)
                cod_fee_discount_currency, cod_fee_discount_amount = _parse_money(cod_fee_discount)

                sql = """
                    INSERT INTO amazon_order_items (
                        amazon_order_id, order_item_id, marketplace_id,
                        asin, seller_sku, title,
                        quantity_ordered, quantity_shipped,
                        item_price_currency_code, item_price_amount,
                        shipping_price_currency_code, shipping_price_amount,
                        gift_wrap_price_currency_code, gift_wrap_price_amount,
                        item_tax_currency_code, item_tax_amount,
                        shipping_tax_currency_code, shipping_tax_amount,
                        gift_wrap_tax_currency_code, gift_wrap_tax_amount,
                        shipping_discount_currency_code, shipping_discount_amount,
                        promotion_discount_currency_code, promotion_discount_amount,
                        cod_fee_currency_code, cod_fee_amount,
                        cod_fee_discount_currency_code, cod_fee_discount_amount,
                        price_designation, condition_id, condition_subtype_id, condition_note,
                        is_gift, gift_message_text, gift_wrap_level,
                        buyer_customized_info_url,
                        serial_numbers, promotion_ids, points_granted,
                        buyer_requested_cancel, buyer_cancel_reason,
                        is_transparency, serial_number_required, ioss_number,
                        scheduled_delivery_start_date, scheduled_delivery_end_date,
                        sync_time
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        asin = VALUES(asin),
                        seller_sku = VALUES(seller_sku),
                        title = VALUES(title),
                        quantity_ordered = VALUES(quantity_ordered),
                        quantity_shipped = VALUES(quantity_shipped),
                        item_price_currency_code = VALUES(item_price_currency_code),
                        item_price_amount = VALUES(item_price_amount),
                        shipping_price_currency_code = VALUES(shipping_price_currency_code),
                        shipping_price_amount = VALUES(shipping_price_amount),
                        gift_wrap_price_currency_code = VALUES(gift_wrap_price_currency_code),
                        gift_wrap_price_amount = VALUES(gift_wrap_price_amount),
                        item_tax_currency_code = VALUES(item_tax_currency_code),
                        item_tax_amount = VALUES(item_tax_amount),
                        shipping_tax_currency_code = VALUES(shipping_tax_currency_code),
                        shipping_tax_amount = VALUES(shipping_tax_amount),
                        gift_wrap_tax_currency_code = VALUES(gift_wrap_tax_currency_code),
                        gift_wrap_tax_amount = VALUES(gift_wrap_tax_amount),
                        shipping_discount_currency_code = VALUES(shipping_discount_currency_code),
                        shipping_discount_amount = VALUES(shipping_discount_amount),
                        promotion_discount_currency_code = VALUES(promotion_discount_currency_code),
                        promotion_discount_amount = VALUES(promotion_discount_amount),
                        cod_fee_currency_code = VALUES(cod_fee_currency_code),
                        cod_fee_amount = VALUES(cod_fee_amount),
                        cod_fee_discount_currency_code = VALUES(cod_fee_discount_currency_code),
                        cod_fee_discount_amount = VALUES(cod_fee_discount_amount),
                        price_designation = VALUES(price_designation),
                        condition_id = VALUES(condition_id),
                        condition_subtype_id = VALUES(condition_subtype_id),
                        condition_note = VALUES(condition_note),
                        is_gift = VALUES(is_gift),
                        gift_message_text = VALUES(gift_message_text),
                        gift_wrap_level = VALUES(gift_wrap_level),
                        buyer_customized_info_url = VALUES(buyer_customized_info_url),
                        serial_numbers = VALUES(serial_numbers),
                        promotion_ids = VALUES(promotion_ids),
                        points_granted = VALUES(points_granted),
                        buyer_requested_cancel = VALUES(buyer_requested_cancel),
                        buyer_cancel_reason = VALUES(buyer_cancel_reason),
                        is_transparency = VALUES(is_transparency),
                        serial_number_required = VALUES(serial_number_required),
                        ioss_number = VALUES(ioss_number),
                        scheduled_delivery_start_date = VALUES(scheduled_delivery_start_date),
                        scheduled_delivery_end_date = VALUES(scheduled_delivery_end_date),
                        sync_time = NOW()
                """

                serial_numbers = item.get('SerialNumbers', [])
                promotion_ids = item.get('PromotionIds', [])

                params = (
                    order_id,
                    item.get('OrderItemId'),
                    marketplace_id,
                    item.get('ASIN'),
                    item.get('SellerSKU'),
                    item.get('Title'),
                    item.get('QuantityOrdered', 0),
                    item.get('QuantityShipped', 0),
                    item_price_currency, item_price_amount,
                    shipping_price_currency, shipping_price_amount,
                    gift_wrap_price_currency, gift_wrap_price_amount,
                    item_tax_currency, item_tax_amount,
                    shipping_tax_currency, shipping_tax_amount,
                    gift_wrap_tax_currency, gift_wrap_tax_amount,
                    shipping_discount_currency, shipping_discount_amount,
                    promotion_discount_currency, promotion_discount_amount,
                    cod_fee_currency, cod_fee_amount,
                    cod_fee_discount_currency, cod_fee_discount_amount,
                    item.get('PriceDesignation'),
                    item.get('ConditionId'),
                    item.get('ConditionSubtypeId'),
                    item.get('ConditionNote'),
                    1 if item.get('IsGift') else 0,
                    buyer_info.get('GiftMessageText'),
                    buyer_info.get('GiftWrapLevel'),
                    buyer_customized.get('CustomizedURL'),
                    json.dumps(serial_numbers) if serial_numbers else None,
                    json.dumps(promotion_ids) if promotion_ids else None,
                    json.dumps(points) if points else None,
                    1 if buyer_cancel.get('IsBuyerRequestedCancel') in (True, 'true') else 0,
                    buyer_cancel.get('BuyerCancelReason'),
                    1 if item.get('IsTransparency') else 0,
                    1 if item.get('SerialNumberRequired') else 0,
                    item.get('IossNumber'),
                    _iso_to_datetime(item.get('ScheduledDeliveryStartDate')),
                    _iso_to_datetime(item.get('ScheduledDeliveryEndDate')),
                )

                cursor.execute(sql, params)
                count += 1

            # 更新订单的 items_sync_time
            cursor.execute(
                "UPDATE amazon_orders SET items_sync_time = NOW() WHERE amazon_order_id = %s",
                (order_id,)
            )

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def get_orders_from_db(marketplace_id=None, order_status=None, amazon_order_id=None,
                       buyer_name=None, purchase_date_from=None, purchase_date_to=None,
                       page=1, page_size=20):
    """
    从数据库分页查询订单列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["1=1"]
            params = []

            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if order_status:
                conditions.append("order_status = %s")
                params.append(order_status)
            if amazon_order_id:
                conditions.append("amazon_order_id = %s")
                params.append(amazon_order_id)
            if buyer_name:
                conditions.append("buyer_name LIKE %s")
                params.append(f"%{buyer_name}%")
            if purchase_date_from:
                conditions.append("purchase_date >= %s")
                params.append(purchase_date_from)
            if purchase_date_to:
                conditions.append("purchase_date < DATE_ADD(%s, INTERVAL 1 DAY)")
                params.append(purchase_date_to)

            where_clause = " AND ".join(conditions)

            cursor.execute(f"SELECT COUNT(*) as total FROM amazon_orders WHERE {where_clause}", tuple(params))
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    o.*,
                    (SELECT COUNT(*) FROM amazon_order_items i WHERE i.amazon_order_id = o.amazon_order_id) as item_count
                FROM amazon_orders o
                WHERE {where_clause}
                ORDER BY o.purchase_date DESC
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


def get_order_detail_from_db(order_id):
    """
    从数据库查询单个订单详情（含商品列表）
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM amazon_orders WHERE amazon_order_id = %s
            """, (order_id,))
            order_row = cursor.fetchone()

            if not order_row:
                return None

            cursor.execute("""
                SELECT
                    i.*,
                    p.product_name as local_product_name,
                    p.declare_name_cn,
                    p.declare_name_en,
                    p.image_url as local_image_url
                FROM amazon_order_items i
                LEFT JOIN products p ON i.seller_sku = p.seller_sku
                WHERE i.amazon_order_id = %s
                ORDER BY i.id ASC
            """, (order_id,))
            items = cursor.fetchall()

            order_row['items'] = items
            return order_row
    finally:
        conn.close()


def get_order_ids_from_db(marketplace_id=None, order_statuses=None):
    """
    从数据库获取所有订单ID列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = []
            params = []
            if marketplace_id:
                conditions.append("marketplace_id = %s")
                params.append(marketplace_id)
            if order_statuses:
                placeholders = ','.join(['%s'] * len(order_statuses))
                conditions.append(f"order_status IN ({placeholders})")
                params.extend(order_statuses)
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
                sql = f"SELECT amazon_order_id FROM amazon_orders {where_clause} ORDER BY purchase_date DESC"
                cursor.execute(sql, tuple(params))
            else:
                sql = "SELECT amazon_order_id FROM amazon_orders ORDER BY purchase_date DESC"
                cursor.execute(sql)
            return [row['amazon_order_id'] for row in cursor.fetchall()]
    finally:
        conn.close()
