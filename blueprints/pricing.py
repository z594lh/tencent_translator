"""
SKU 售价计算模块
根据成本结构反推建议售价
- 佣金比例按类目查表 (category_commission_rates)
- FBA 配送费按重量+尺寸查表 (fba_tier_fees)
- 汇率从 exchange_rates 表读取
"""
import re
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from blueprints.user_auth import login_required

pricing_bp = Blueprint('pricing', __name__, url_prefix='/api')


def _get_conn():
    return get_db_connection()


def _lb_to_kg(lb):
    """磅转千克"""
    return lb * 0.45359237


def _parse_dimensions(dimensions_str):
    """
    解析 dimensions_cm 字段，如 '13.7*1.5*1.5' 或 '30x20x10'
    返回 [长, 宽, 高] 的 float 列表，解析失败返回 None
    """
    if not dimensions_str:
        return None
    parts = re.split(r'[\*xX×\s]+', str(dimensions_str).strip())
    nums = []
    for p in parts:
        try:
            nums.append(float(p.strip()))
        except (ValueError, TypeError):
            continue
    if len(nums) >= 3:
        return nums[:3]
    return None


def _get_billable_weight(weight_kg, dimensions_cm_str):
    """
    计算计费重量 = max(实际重量, 体积重)
    亚马逊体积重 ≈ 体积(cm³) / 5000
    """
    actual_weight = float(weight_kg) if weight_kg is not None else 0.0
    dims = _parse_dimensions(dimensions_cm_str)
    if dims and len(dims) == 3:
        volume = dims[0] * dims[1] * dims[2]
        volumetric_weight = volume / 5000.0
        return max(actual_weight, volumetric_weight)
    return actual_weight


def _get_exchange_rate(cursor, from_currency='CNY', to_currency='USD'):
    """从 exchange_rates 表读取最新汇率"""
    cursor.execute("""
        SELECT rate FROM exchange_rates
        WHERE from_currency = %s AND to_currency = %s
        LIMIT 1
    """, (from_currency, to_currency))
    row = cursor.fetchone()
    if row and row.get('rate') is not None:
        return float(row['rate'])
    return None


def _get_commission_rate(cursor, seller_sku):
    """根据 products.category_id 查佣金比例"""
    try:
        cursor.execute("""
            SELECT category_id FROM products WHERE seller_sku = %s LIMIT 1
        """, (seller_sku,))
        row = cursor.fetchone()
        if row and row.get('category_id'):
            cid = row['category_id']
            cursor.execute("""
                SELECT commission_rate FROM category_commission_rates
                WHERE id = %s LIMIT 1
            """, (int(cid),))
            r = cursor.fetchone()
            if r:
                return float(r['commission_rate']), f'product_category_id:{cid}'
    except Exception:
        pass
    return 0.15, 'default:0.15'


def _get_fba_fee(cursor, billable_weight_kg):
    """根据计费重量查 FBA 配送费"""
    w = float(billable_weight_kg)
    cursor.execute("""
        SELECT fee_usd, tier_name
        FROM fba_tier_fees
        WHERE weight_min_kg <= %s
          AND (weight_max_kg IS NULL OR weight_max_kg > %s)
        ORDER BY weight_min_kg DESC
        LIMIT 1
    """, (w, w))
    row = cursor.fetchone()
    if row:
        return float(row['fee_usd']), row.get('tier_name')
    return 3.22, 'Small Standard (default)'


def _get_purchase_cost(cursor, seller_sku):
    """从 products.purchase_cost 读取采购成本，默认币种 CNY"""
    cursor.execute("""
        SELECT purchase_cost
        FROM products WHERE seller_sku = %s LIMIT 1
    """, (seller_sku,))
    row = cursor.fetchone()
    if row and row.get('purchase_cost') is not None:
        return float(row['purchase_cost']), 'CNY', 'products.purchase_cost'
    return None, None, None


def _get_freight_allocation(cursor, seller_sku):
    """计算单个 SKU 的头程运费分摊（人民币）"""
    import json

    # 1. 从 amazon_inbound_plan_boxes 的 items_json 中找包含该 SKU 的最新有效货件
    like_pattern = f'%%"msku": "{seller_sku}"%%'
    cursor.execute("""
        SELECT DISTINCT b.shipment_id, s.sync_time
        FROM amazon_inbound_plan_boxes b
        INNER JOIN amazon_inbound_shipments_detail s ON b.shipment_id = s.shipment_confirmation_id
        WHERE b.items_json LIKE %s AND s.status != 'CANCELLED'
        ORDER BY s.sync_time DESC
        LIMIT 1
    """, (like_pattern,))
    row = cursor.fetchone()
    if not row:
        return 0.0, {"note": "未找到该 SKU 有效的亚马逊货件记录（已排除 CANCELLED）", "shipment_id": None}
    shipment_id = row['shipment_id']

    # 2. 查该货件下所有箱子，解析 items_json 精确汇总该 SKU 数量
    cursor.execute("""
        SELECT items_json
        FROM amazon_inbound_plan_boxes
        WHERE shipment_id = %s
    """, (shipment_id,))
    boxes = cursor.fetchall()

    qty = 0
    for box in boxes:
        items = json.loads(box.get("items_json") or "[]")
        if isinstance(items, list):
            for it in items:
                if it.get("msku") == seller_sku:
                    qty += int(it.get("quantity") or 0)

    # 3. 查这个货件绑定的运单运费
    cursor.execute("""
        SELECT total_cost_cny
        FROM logistics_waybills
        WHERE shipment_id = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (shipment_id,))
    waybill = cursor.fetchone()
    if not waybill or waybill.get('total_cost_cny') is None:
        return 0.0, {"note": "货件未绑定运单或运单无费用", "shipment_id": shipment_id}
    total_cost_cny = float(waybill['total_cost_cny'])

    # 4. 查这个货件下所有箱子的重量
    cursor.execute("""
        SELECT weight_value, weight_unit
        FROM amazon_inbound_plan_boxes
        WHERE shipment_id = %s
    """, (shipment_id,))
    boxes = cursor.fetchall()
    if not boxes:
        return 0.0, {"note": "未找到货件对应的入库计划箱子重量", "shipment_id": shipment_id}

    total_weight_kg = 0.0
    for box in boxes:
        w = float(box.get('weight_value') or 0)
        unit = (box.get('weight_unit') or '').upper()
        if unit == 'LB':
            w = _lb_to_kg(w)
        total_weight_kg += w

    if total_weight_kg <= 0:
        return 0.0, {"note": "货件汇总重量为0，无法分摊", "shipment_id": shipment_id}

    cursor.execute("""
        SELECT weight_kg FROM products WHERE seller_sku = %s LIMIT 1
    """, (seller_sku,))
    prod = cursor.fetchone()
    if not prod or prod.get('weight_kg') is None:
        return 0.0, {"note": "产品表未录入单件重量(kg)", "shipment_id": shipment_id}
    sku_weight = float(prod['weight_kg'])

    allocation = total_cost_cny * sku_weight / total_weight_kg

    return allocation, {
        "shipment_id": shipment_id,
        "quantity_shipped": qty,
        "waybill_total_cost_cny": round(total_cost_cny, 2),
        "shipment_total_weight_kg": round(total_weight_kg, 3),
        "sku_weight_kg": sku_weight,
        "allocation_formula": f"{total_cost_cny} * {sku_weight} / {total_weight_kg}"
    }


@pricing_bp.route('/pricing/calculate', methods=['POST'])
@login_required
def calculate_price():
    """
    SKU 售价反算接口
    请求体（前端只传这 4 个字段）:
      seller_sku           必填  SKU
      target_profit_rate   必填  目标利润率(小数)
      ad_rate              必填  广告费率(ACoS)
      refund_rate          必填  退货率
    """
    try:
        data = request.get_json() or {}
        seller_sku = data.get('seller_sku', '').strip()
        if not seller_sku:
            return jsonify({"status": "error", "message": "seller_sku 不能为空"}), 400

        target_profit_rate = data.get('target_profit_rate')
        if target_profit_rate is None:
            return jsonify({"status": "error", "message": "target_profit_rate 不能为空"}), 400
        try:
            target_profit_rate = float(target_profit_rate)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "target_profit_rate 必须是数字"}), 400

        ad_rate = float(data.get('ad_rate', 0.20))
        refund_rate = float(data.get('refund_rate', 0.03))

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 汇率
                exchange_rate = _get_exchange_rate(cursor, 'CNY', 'USD')
                if exchange_rate is None:
                    return jsonify({
                        "status": "error",
                        "message": "未找到 CNY->USD 汇率，请先等待自动同步"
                    }), 400

                # 产品信息
                cursor.execute("""
                    SELECT product_name, weight_kg, dimensions_cm, asin
                    FROM products WHERE seller_sku = %s LIMIT 1
                """, (seller_sku,))
                prod = cursor.fetchone()
                if not prod:
                    return jsonify({"status": "error", "message": "产品不存在"}), 404

                product_name = prod.get('product_name') or ''
                weight_kg = float(prod['weight_kg']) if prod.get('weight_kg') is not None else None
                dimensions_cm = prod.get('dimensions_cm')

                # 采购成本
                purchase_cost, purchase_cost_currency, purchase_cost_source = _get_purchase_cost(cursor, seller_sku)
                if purchase_cost is None:
                    return jsonify({"status": "error", "message": "未找到该 SKU 的采购成本"}), 400

                if purchase_cost_currency == 'CNY':
                    purchase_cost_usd = purchase_cost * exchange_rate
                else:
                    purchase_cost_usd = purchase_cost

                # 头程分摊
                freight_cost_cny, freight_detail = _get_freight_allocation(cursor, seller_sku)
                freight_cost_usd = freight_cost_cny * exchange_rate

                # 佣金比例
                commission_rate, commission_source = _get_commission_rate(cursor, seller_sku)

                # FBA 配送费
                billable_weight = _get_billable_weight(weight_kg, dimensions_cm)
                fba_fee, fba_tier = _get_fba_fee(cursor, billable_weight)

                # 售价反推
                fixed_cost_usd = purchase_cost_usd + freight_cost_usd + fba_fee
                variable_rate = commission_rate + ad_rate + refund_rate
                denominator = 1 - variable_rate - target_profit_rate

                if denominator <= 0:
                    suggested_price = None
                    commission = None
                    ad_cost = None
                    refund_cost = None
                    total_cost = None
                    profit_amount = None
                    actual_profit_rate = None
                    calc_note = (
                        f"变动费率({variable_rate*100:.0f}%) + 目标利润率({target_profit_rate*100:.0f}%) "
                        f"已超过 100%，无法计算出正数建议售价"
                    )
                else:
                    suggested_price = fixed_cost_usd / denominator
                    commission = suggested_price * commission_rate
                    ad_cost = suggested_price * ad_rate
                    refund_cost = suggested_price * refund_rate
                    total_cost = fixed_cost_usd + commission + ad_cost + refund_cost
                    profit_amount = suggested_price - total_cost
                    actual_profit_rate = profit_amount / suggested_price if suggested_price > 0 else 0
                    calc_note = None

                return jsonify({
                    "status": "success",
                    "data": {
                        "seller_sku": seller_sku,
                        "product_name": product_name,
                        "asin": prod.get('asin'),
                        "weight_kg": weight_kg,
                        "dimensions_cm": dimensions_cm,
                        "billable_weight_kg": round(billable_weight, 3) if weight_kg is not None else None,
                        "inputs": {
                            "ad_rate": ad_rate,
                            "refund_rate": refund_rate,
                            "target_profit_rate": target_profit_rate,
                            "exchange_rate": exchange_rate,
                        },
                        "commission": {
                            "rate": commission_rate,
                            "source": commission_source,
                        },
                        "fba_fee": {
                            "fee_usd": fba_fee,
                            "tier": fba_tier,
                        },
                        "purchase_cost": {
                            "value": round(purchase_cost, 2),
                            "currency": purchase_cost_currency,
                            "source": purchase_cost_source,
                            "usd": round(purchase_cost_usd, 2),
                        },
                        "freight_cost": {
                            "cny": round(freight_cost_cny, 2),
                            "usd": round(freight_cost_usd, 2),
                            "detail": freight_detail,
                        },
                        "cost_breakdown": {
                            "purchase_cost_usd": round(purchase_cost_usd, 2),
                            "freight_cost_usd": round(freight_cost_usd, 2),
                            "fba_fee_usd": fba_fee,
                            "commission_usd": round(commission, 2) if commission is not None else None,
                            "ad_cost_usd": round(ad_cost, 2) if ad_cost is not None else None,
                            "refund_cost_usd": round(refund_cost, 2) if refund_cost is not None else None,
                            "total_cost_usd": round(total_cost, 2) if total_cost is not None else None,
                        },
                        "suggested_price": round(suggested_price, 2) if suggested_price is not None else None,
                        "profit_amount": round(profit_amount, 2) if profit_amount is not None else None,
                        "actual_profit_rate": round(actual_profit_rate, 4) if actual_profit_rate is not None else None,
                        "calc_note": calc_note,
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Pricing] 计算售价异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
