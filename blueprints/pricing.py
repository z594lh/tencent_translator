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
import json
from decimal import Decimal
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from blueprints.user_auth import login_required, permission_required
from services.profit_calculator import (
    get_unit_costs,
    get_unit_costs_with_api,
    get_exchange_rate,
    calculate_suggested_price,
)

pricing_bp = Blueprint('pricing', __name__, url_prefix='/api')

# 计算过程变量中文标签配置
VARIABLE_LABELS = {
    "suggested_price": "建议售价",
    "fixed_cost_usd": "固定成本(USD)",
    "purchase_cost_usd": "产品采购成本(USD)",
    "headway_cost_usd": "头程分摊运费(USD)",
    "headway_cost_cny": "头程分摊运费(CNY)",
    "waybill_total_cost_cny": "运单总费用(CNY)",
    "sku_weight_kg": "SKU单件重量(KG)",
    "shipment_total_weight_kg": "货件总重量(KG)",
    "exchange_rate": "汇率(CNY→USD)",
    "fba_fee_usd": "FBA配送费(USD)",
    "commission_rate": "销售佣金比例",
    "ad_rate": "广告费率",
    "refund_rate": "退货率",
    "target_profit_rate": "目标利润率",
    "calc_error": "计算错误",
}


def _get_conn():
    return get_db_connection()


def _build_calc_node(variable_name, variable_label, variable_value=None, formula=None,
                     source_table=None, source_field=None, source_condition=None,
                     source_value=None, is_leaf=0, children=None):
    """构建内存中的计算过程树节点"""
    return {
        "variable_name": variable_name,
        "variable_label": variable_label,
        "variable_value": variable_value,
        "formula": formula,
        "source_table": source_table,
        "source_field": source_field,
        "source_condition": source_condition,
        "source_value": source_value,
        "is_leaf": is_leaf,
        "children": children or [],
    }


def _calculate_with_tree(cursor, seller_sku, target_profit_rate, ad_rate, refund_rate,
                         shop_id=None):
    """
    执行单个 SKU 的售价计算，并返回 (result_dict, error_msg, calc_tree)。
    传 shop_id 则实时调 Amazon API 获取准确费率，不传则走缓存表。
    """
    exchange_rate = get_exchange_rate(cursor, 'CNY', 'USD')
    if shop_id is not None:
        costs = get_unit_costs_with_api(cursor, seller_sku, exchange_rate, shop_id)
    else:
        costs = get_unit_costs(cursor, seller_sku, exchange_rate)

    headway_detail = costs.sources.get("headway", {})
    shipment_id = headway_detail.get("shipment_id")

    # 补充查询运单和货代信息
    waybill_info = {}
    if shipment_id:
        cursor.execute("""
            SELECT w.waybill_no, p.name as provider_name,
                   w.freight_cost_cny, w.tax_cost_cny, w.misc_cost_cny, w.total_cost_cny
            FROM logistics_waybills w
            LEFT JOIN logistics_providers p ON w.provider_id = p.id
            WHERE w.shipment_id = %s
            ORDER BY w.created_at DESC LIMIT 1
        """, (shipment_id,))
        waybill = cursor.fetchone()
        if waybill:
            waybill_info = {
                "waybill_no": waybill.get("waybill_no"),
                "provider_name": waybill.get("provider_name"),
                "freight_cost_cny": float(waybill["freight_cost_cny"]) if waybill.get("freight_cost_cny") is not None else None,
                "tax_cost_cny": float(waybill["tax_cost_cny"]) if waybill.get("tax_cost_cny") is not None else None,
                "misc_cost_cny": float(waybill["misc_cost_cny"]) if waybill.get("misc_cost_cny") is not None else None,
            }

    fixed_cost_usd = costs.purchase_cost_usd + costs.headway_cost_usd + costs.fba_fee_usd
    variable_rate = costs.commission_rate + ad_rate + refund_rate
    denominator = Decimal("1") - variable_rate - target_profit_rate

    # 分母异常
    if denominator <= 0:
        err_msg = (f"变动费率({float(variable_rate)*100:.1f}%) + 目标利润率({float(target_profit_rate)*100:.1f}%) "
                   f"已超过 100%，无法计算出正数建议售价")
        tree = _build_calc_node(
            "suggested_price", VARIABLE_LABELS.get("suggested_price"), None,
            "fixed_cost_usd / (1 - commission_rate - ad_rate - refund_rate - target_profit_rate)",
            is_leaf=0,
            children=[_build_calc_node(
                "calc_error", VARIABLE_LABELS.get("calc_error"), err_msg,
                f"分母 = 1 - {float(costs.commission_rate):.4f} - {float(ad_rate):.4f} - {float(refund_rate):.4f} - {float(target_profit_rate):.4f} = {float(denominator):.4f} ≤ 0",
                is_leaf=1)]
        )
        return None, err_msg, tree

    price_result = calculate_suggested_price(
        fixed_cost_usd=fixed_cost_usd,
        commission_rate=costs.commission_rate,
        ad_rate=ad_rate,
        refund_rate=refund_rate,
        target_profit_rate=target_profit_rate,
    )

    suggested_price = price_result['suggested_price']

    # ========================
    # 构建计算过程树
    # ========================

    # 固定成本子树
    purchase_cost_node = _build_calc_node(
        "purchase_cost_usd", VARIABLE_LABELS.get("purchase_cost_usd"),
        f"{float(costs.purchase_cost_usd):.2f}",
        f"purchase_cost_cny({float(costs.purchase_cost_cny):.2f}) * exchange_rate({float(exchange_rate):.6f})",
        source_table="products", source_field="purchase_cost",
        source_condition=f"WHERE seller_sku = '{seller_sku}' LIMIT 1",
        source_value=f"{float(costs.purchase_cost_cny):.2f}",
        is_leaf=1,
    )

    # 头程子树
    wtc = headway_detail.get("waybill_total_cost_cny")
    stw = headway_detail.get("shipment_total_weight_kg")
    sku_w = headway_detail.get("sku_weight_kg")

    headway_cny_children = [
        _build_calc_node(
            "waybill_total_cost_cny", VARIABLE_LABELS.get("waybill_total_cost_cny"),
            f"{float(wtc):.2f}" if wtc is not None else None,
            None,
            source_table="logistics_waybills", source_field="total_cost_cny",
            source_condition=f"WHERE shipment_id = '{shipment_id}' ORDER BY created_at DESC LIMIT 1" if shipment_id else "未找到有效货件",
            source_value=f"{float(wtc):.2f}" if wtc is not None else None,
            is_leaf=1,
        ),
        _build_calc_node(
            "sku_weight_kg", VARIABLE_LABELS.get("sku_weight_kg"),
            f"{float(sku_w):.4f}" if sku_w is not None else None,
            None,
            source_table="products", source_field="weight_kg",
            source_condition=f"WHERE seller_sku = '{seller_sku}' LIMIT 1",
            source_value=f"{float(sku_w):.4f}" if sku_w is not None else None,
            is_leaf=1,
        ),
        _build_calc_node(
            "shipment_total_weight_kg", VARIABLE_LABELS.get("shipment_total_weight_kg"),
            f"{float(stw):.4f}" if stw is not None else None,
            None,
            source_table="amazon_inbound_plan_boxes", source_field="weight_value",
            source_condition=f"WHERE shipment_id = '{shipment_id}'",
            source_value=f"{float(stw):.4f}" if stw is not None else None,
            is_leaf=1,
        ),
    ]

    headway_cny_node = _build_calc_node(
        "headway_cost_cny", VARIABLE_LABELS.get("headway_cost_cny"),
        f"{float(costs.headway_cost_cny):.4f}",
        "waybill_total_cost_cny * sku_weight_kg / shipment_total_weight_kg",
        is_leaf=0, children=headway_cny_children,
    )

    exchange_rate_node = _build_calc_node(
        "exchange_rate", VARIABLE_LABELS.get("exchange_rate"),
        f"{float(exchange_rate):.6f}",
        None,
        source_table="exchange_rates", source_field="rate",
        source_condition="WHERE from_currency='CNY' AND to_currency='USD' ORDER BY updated_at DESC LIMIT 1",
        source_value=f"{float(exchange_rate):.6f}",
        is_leaf=1,
    )

    headway_usd_node = _build_calc_node(
        "headway_cost_usd", VARIABLE_LABELS.get("headway_cost_usd"),
        f"{float(costs.headway_cost_usd):.2f}",
        f"headway_cost_cny({float(costs.headway_cost_cny):.4f}) * exchange_rate({float(exchange_rate):.6f})",
        is_leaf=0,
        children=[headway_cny_node, exchange_rate_node],
    )

    fba_node = _build_calc_node(
        "fba_fee_usd", VARIABLE_LABELS.get("fba_fee_usd"),
        f"{float(costs.fba_fee_usd):.2f}",
        f"计费重量({float(costs.billable_weight_kg):.4f}kg)对应费率",
        source_table="amazon_product_fees" if costs.sources.get("fee_method", "").startswith("amazon")
        else "Amazon Product Fees API", source_field="fba_fee",
        source_condition=costs.sources.get("fba", ""),
        source_value=f"{float(costs.fba_fee_usd):.2f}",
        is_leaf=1,
    )

    fixed_cost_node = _build_calc_node(
        "fixed_cost_usd", VARIABLE_LABELS.get("fixed_cost_usd"),
        f"{float(fixed_cost_usd):.2f}",
        "purchase_cost_usd + headway_cost_usd + fba_fee_usd",
        is_leaf=0,
        children=[purchase_cost_node, headway_usd_node, fba_node],
    )

    # 直接子节点
    commission_node = _build_calc_node(
        "commission_rate", VARIABLE_LABELS.get("commission_rate"),
        f"{float(costs.commission_rate):.4f}",
        None,
        source_table="amazon_product_fees" if costs.sources.get("fee_method", "").startswith("amazon")
        else "Amazon Product Fees API", source_field="commission_rate",
        source_condition=costs.sources.get("commission", ""),
        source_value=f"{float(costs.commission_rate):.4f}" if costs.commission_rate != Decimal("0.15") else "0.15 (default)",
        is_leaf=1,
    )

    ad_rate_node = _build_calc_node("ad_rate", VARIABLE_LABELS.get("ad_rate"), f"{float(ad_rate):.4f}", "用户输入固定值", is_leaf=1)
    refund_rate_node = _build_calc_node("refund_rate", VARIABLE_LABELS.get("refund_rate"), f"{float(refund_rate):.4f}", "用户输入固定值", is_leaf=1)
    target_profit_rate_node = _build_calc_node("target_profit_rate", VARIABLE_LABELS.get("target_profit_rate"), f"{float(target_profit_rate):.4f}", "用户输入固定值", is_leaf=1)

    # 根节点
    calc_tree = _build_calc_node(
        "suggested_price", VARIABLE_LABELS.get("suggested_price"),
        f"{float(suggested_price):.2f}",
        f"fixed_cost_usd({float(fixed_cost_usd):.2f}) / (1 - commission_rate({float(costs.commission_rate):.4f}) - ad_rate({float(ad_rate):.4f}) - refund_rate({float(refund_rate):.4f}) - target_profit_rate({float(target_profit_rate):.4f}))",
        is_leaf=0,
        children=[fixed_cost_node, commission_node, ad_rate_node, refund_rate_node, target_profit_rate_node],
    )

    # 组装结果
    def _f(val):
        return float(val) if val is not None else None

    result = {
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
            "detail": {**headway_detail, **waybill_info},
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
        "calc_tree": calc_tree,
    }

    return result, None, calc_tree


@pricing_bp.route('/pricing/calculate', methods=['POST'])
@login_required
@permission_required('pricing:calculate')
def calculate_price():
    """
    SKU 售价反算接口（含计算过程树）
    请求体:
      seller_sku           必填  SKU
       target_profit_rate   必填  目标利润率(小数)
       ad_rate              必填  广告费率(ACoS)
       refund_rate          必填  退货率
       shop_id              可选  店铺ID，传则自动取当前售价调 Amazon API 获取实时费率
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

        shop_id = data.get('shop_id')
        if shop_id is not None:
            try:
                shop_id = int(shop_id)
            except (ValueError, TypeError):
                return jsonify({"status": "error", "message": "shop_id 必须是整数"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                exchange_rate = get_exchange_rate(cursor, 'CNY', 'USD')
                if exchange_rate is None:
                    return jsonify({
                        "status": "error",
                        "message": "未找到 CNY->USD 汇率，请先等待自动同步"
                    }), 400

                costs = get_unit_costs(cursor, seller_sku, exchange_rate)
                if costs.purchase_cost_usd <= 0:
                    return jsonify({"status": "error", "message": "未找到该 SKU 的采购成本"}), 400

                result, err, calc_tree = _calculate_with_tree(
                    cursor, seller_sku, target_profit_rate, ad_rate, refund_rate,
                    shop_id=shop_id
                )

                if err:
                    return jsonify({
                        "status": "error",
                        "message": err,
                        "data": {"calc_tree": calc_tree}
                    }), 400

                return jsonify({
                    "status": "success",
                    "data": result
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Pricing] 计算售价异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
