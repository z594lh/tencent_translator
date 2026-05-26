"""
SKU 售价计算模块
根据成本结构反推建议售价
- 佣金比例按类目查表 (category_commission_rates)
- FBA 配送费按重量+尺寸查表 (fba_tier_fees)
- 汇率从 exchange_rates 表读取

【重构说明】
本模块所有底层成本计算（采购、头程、FBA、佣金）已下沉到 services/profit_calculator.py，
此处仅保留 HTTP 接口层和售价反推公式，确保与报表模块成本逻辑完全一致。
"""
from decimal import Decimal
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from blueprints.user_auth import login_required, permission_required
from services.profit_calculator import (
    get_unit_costs,
    get_exchange_rate,
    calculate_suggested_price,
)

pricing_bp = Blueprint('pricing', __name__, url_prefix='/api')


def _get_conn():
    return get_db_connection()


@pricing_bp.route('/pricing/calculate', methods=['POST'])
@login_required
@permission_required('pricing:calculate')
def calculate_price():
    """
    SKU 售价反算接口
    请求体:
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

        target_profit_rate_raw = data.get('target_profit_rate')
        if target_profit_rate_raw is None:
            return jsonify({"status": "error", "message": "target_profit_rate 不能为空"}), 400
        try:
            target_profit_rate = Decimal(str(target_profit_rate_raw))
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "target_profit_rate 必须是数字"}), 400

        ad_rate = Decimal(str(data.get('ad_rate', 0.20)))
        refund_rate = Decimal(str(data.get('refund_rate', 0.03)))

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 汇率
                exchange_rate = get_exchange_rate(cursor, 'CNY', 'USD')
                if exchange_rate is None:
                    return jsonify({
                        "status": "error",
                        "message": "未找到 CNY->USD 汇率，请先等待自动同步"
                    }), 400

                # 统一获取全部单位成本
                costs = get_unit_costs(cursor, seller_sku, exchange_rate)
                if costs.purchase_cost_usd <= 0:
                    return jsonify({"status": "error", "message": "未找到该 SKU 的采购成本"}), 400

                # 固定成本 = 采购 + 头程 + FBA
                fixed_cost_usd = costs.purchase_cost_usd + costs.headway_cost_usd + costs.fba_fee_usd

                # 售价反推
                price_result = calculate_suggested_price(
                    fixed_cost_usd=fixed_cost_usd,
                    commission_rate=costs.commission_rate,
                    ad_rate=ad_rate,
                    refund_rate=refund_rate,
                    target_profit_rate=target_profit_rate,
                )

                # 组装返回（保持与旧接口字段一致，全部转 float 供 JSON 序列化）
                def _f(val):
                    return float(val) if val is not None else None

                return jsonify({
                    "status": "success",
                    "data": {
                        "seller_sku": seller_sku,
                        "product_name": costs.product_name or '',
                        "asin": costs.asin,
                        "weight_kg": _f(costs.weight_kg),
                        "billable_weight_kg": _f(costs.billable_weight_kg),
                        "inputs": {
                            "ad_rate": float(ad_rate),
                            "refund_rate": float(refund_rate),
                            "target_profit_rate": float(target_profit_rate),
                            "exchange_rate": float(exchange_rate),
                        },
                        "commission": {
                            "rate": float(costs.commission_rate),
                            "source": costs.sources.get("commission", ""),
                        },
                        "fba_fee": {
                            "fee_usd": float(costs.fba_fee_usd),
                            "tier": costs.fba_tier,
                        },
                        "purchase_cost": {
                            "value": float(costs.purchase_cost_cny.quantize(Decimal("0.01"))),
                            "currency": "CNY",
                            "source": costs.sources.get("purchase_cost", ""),
                            "usd": float(costs.purchase_cost_usd.quantize(Decimal("0.01"))),
                        },
                        "freight_cost": {
                            "cny": float(costs.headway_cost_cny.quantize(Decimal("0.01"))),
                            "usd": float(costs.headway_cost_usd.quantize(Decimal("0.01"))),
                            "detail": costs.sources.get("headway", {}),
                        },
                        "cost_breakdown": {
                            "purchase_cost_usd": float(costs.purchase_cost_usd.quantize(Decimal("0.01"))),
                            "freight_cost_usd": float(costs.headway_cost_usd.quantize(Decimal("0.01"))),
                            "fba_fee_usd": float(costs.fba_fee_usd),
                            "commission_usd": _f(price_result.get("commission")),
                            "ad_cost_usd": _f(price_result.get("ad_cost")),
                            "refund_cost_usd": _f(price_result.get("refund_cost")),
                            "total_cost_usd": _f(price_result.get("total_cost")),
                        },
                        "suggested_price": _f(price_result.get("suggested_price")),
                        "profit_amount": _f(price_result.get("profit_amount")),
                        "actual_profit_rate": _f(price_result.get("actual_profit_rate")),
                        "calc_note": price_result.get("calc_note"),
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Pricing] 计算售价异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
