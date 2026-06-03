"""
利润计算统一入口 (Unified Profit Calculator)

设计原则：
1. 所有 SKU 维度的成本拆分（采购、头程、FBA、佣金）统一在此计算，避免多处重复逻辑。
2. 定价反推 和 报表利润 共用同一套底层成本函数，仅在「利润率公式」层面区分。
3. 金额统一使用 Decimal（财务精度），仅在接口层按需转 float。
4. 每个成本项返回 "value + source/detail"，方便排查数据问题。

主要对外接口：
- get_unit_costs(cursor, seller_sku, exchange_rate) -> UnitCostBreakdown
  获取单件 SKU 的全部单位成本（USD）。
- calculate_profit(sales_amount, qty, unit_costs, ad_cost=0, refund_amount=0) -> ProfitResult
  按实际销售额/销量计算利润（适用于日报、SKU利润表、库存估值）。
- calculate_suggested_price(fixed_cost_usd, commission_rate, ad_rate, refund_rate, target_profit_rate)
  售价反推（适用于定价模块）。
"""

import json
import re
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------
_LB_TO_KG = Decimal("0.45359237")
_DEFAULT_COMMISSION_RATE = Decimal("0.15")
_DEFAULT_FBA_FEE = Decimal("3.22")          # Small Standard fallback
_DEFAULT_EXCHANGE_RATE = Decimal("0.138")   # CNY->USD fallback


# ---------------------------------------------------------------------------
# 数据类
# ---------------------------------------------------------------------------

@dataclass
class UnitCostBreakdown:
    """单件 SKU 的单位成本拆分（全部 USD）"""
    seller_sku: str
    product_name: Optional[str] = None
    asin: Optional[str] = None

    # 固定成本（单件）
    purchase_cost_usd: Decimal = Decimal("0")
    headway_cost_usd: Decimal = Decimal("0")
    fba_fee_usd: Decimal = Decimal("0")

    # 比率（按售价/销售额乘算）
    commission_rate: Decimal = _DEFAULT_COMMISSION_RATE

    # 元信息（用于调试、前端展示）
    purchase_cost_cny: Decimal = Decimal("0")
    headway_cost_cny: Decimal = Decimal("0")
    exchange_rate: Decimal = _DEFAULT_EXCHANGE_RATE
    weight_kg: Optional[Decimal] = None
    billable_weight_kg: Optional[Decimal] = None
    fba_tier: Optional[str] = None

    # 每个字段的数据来源说明
    sources: dict = field(default_factory=dict)


@dataclass
class ProfitResult:
    """利润计算结果（全部 USD，Decimal 精度）"""
    sales_amount: Decimal = Decimal("0")
    qty: int = 0

    product_cost: Decimal = Decimal("0")
    fba_fees: Decimal = Decimal("0")
    commission: Decimal = Decimal("0")
    headway_cost: Decimal = Decimal("0")
    ad_cost: Decimal = Decimal("0")
    refund_amount: Decimal = Decimal("0")
    other_fees: Decimal = Decimal("0")

    # 利润口径
    gross_profit: Decimal = Decimal("0")      # 毛利 = 销售额 - 产品 - FBA - 佣金 - 退款 - 头程
    net_profit: Decimal = Decimal("0")        # 净利 = 毛利 - 广告 - 其他
    profit_margin: Decimal = Decimal("0")     # 净利 / 销售额

    # 经营日报口径（总成本包含广告和退款）
    total_cost: Decimal = Decimal("0")        # 总成本 = 所有项之和
    gross_profit_daily: Decimal = Decimal("0")  # 经营日报口径：销售额 - 总成本
    gross_profit_rate_daily: Decimal = Decimal("0")


# ---------------------------------------------------------------------------
# 底层 helper（原 pricing.py / report_generator.py 的重复逻辑统一到此）
# ---------------------------------------------------------------------------

def _lb_to_kg(lb: float) -> Decimal:
    return Decimal(str(lb)) * _LB_TO_KG


def parse_dimensions(dimensions_str) -> Optional[list]:
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


def get_billable_weight(weight_kg, dimensions_cm_str) -> Decimal:
    """
    计算计费重量 = max(实际重量, 体积重)
    亚马逊体积重 ≈ 体积(cm³) / 5000
    """
    actual_weight = Decimal(str(weight_kg)) if weight_kg is not None else Decimal("0")
    dims = parse_dimensions(dimensions_cm_str)
    if dims and len(dims) == 3:
        volume = Decimal(str(dims[0])) * Decimal(str(dims[1])) * Decimal(str(dims[2]))
        volumetric_weight = volume / Decimal("5000")
        return max(actual_weight, volumetric_weight)
    return actual_weight


def get_exchange_rate(cursor, from_currency: str = "CNY", to_currency: str = "USD") -> Decimal:
    """从 exchange_rates 表读取最新汇率，失败返回默认汇率"""
    try:
        cursor.execute(
            """
            SELECT rate FROM exchange_rates
            WHERE from_currency = %s AND to_currency = %s
            ORDER BY updated_at DESC LIMIT 1
            """,
            (from_currency, to_currency),
        )
        row = cursor.fetchone()
        if row and row.get("rate") is not None:
            return Decimal(str(row["rate"]))
    except Exception:
        pass
    return _DEFAULT_EXCHANGE_RATE


def get_commission_rate(cursor, seller_sku: str) -> Tuple[Decimal, str]:
    """
    根据 products.category_id 查佣金比例。
    返回 (commission_rate, source)
    """
    try:
        cursor.execute(
            "SELECT category_id FROM products WHERE seller_sku = %s LIMIT 1",
            (seller_sku,),
        )
        row = cursor.fetchone()
        if row and row.get("category_id"):
            cid = int(row["category_id"])
            cursor.execute(
                "SELECT commission_rate FROM category_commission_rates WHERE id = %s LIMIT 1",
                (cid,),
            )
            r = cursor.fetchone()
            if r and r.get("commission_rate") is not None:
                return Decimal(str(r["commission_rate"])), f"product_category_id:{cid}"
    except Exception:
        pass
    return _DEFAULT_COMMISSION_RATE, "default:0.15"


def get_fba_fee(cursor, billable_weight_kg) -> Tuple[Decimal, Optional[str]]:
    """根据计费重量查 FBA 配送费，返回 (fee_usd, tier_name)"""
    w = float(billable_weight_kg)
    cursor.execute(
        """
        SELECT fee_usd, tier_name FROM fba_tier_fees
        WHERE weight_min_kg <= %s
          AND (weight_max_kg IS NULL OR weight_max_kg > %s)
        ORDER BY weight_min_kg DESC
        LIMIT 1
        """,
        (w, w),
    )
    row = cursor.fetchone()
    if row and row.get("fee_usd") is not None:
        return Decimal(str(row["fee_usd"])), row.get("tier_name")
    return _DEFAULT_FBA_FEE, "Small Standard (default)"


def get_headway_allocation(cursor, seller_sku: str) -> Tuple[Decimal, Decimal, dict]:
    """
    计算单个 SKU 的头程运费分摊。
    返回 (headway_cny, headway_usd, detail_dict)

    计算逻辑：
      1. 找到包含该 SKU 的最新有效货件（排除 CANCELLED）。
      2. 读取该货件绑定的运单总费用(CNY)。
      3. 读取该货件下所有箱子的总重量(KG)。
      4. 读取产品表该 SKU 的单件重量(KG)。
      5. 单件头程(CNY) = 运单总费用 * SKU单件重量 / 货件总重量
      6. headway_usd = headway_cny * 汇率（汇率由调用方提供，避免在此再次查表）
    """
    like_pattern = f'%"msku": "{seller_sku}"%'
    cursor.execute(
        """
        SELECT DISTINCT b.shipment_id, s.sync_time
        FROM amazon_inbound_plan_boxes b
        INNER JOIN amazon_inbound_shipments s
            ON b.inbound_plan_id = s.inbound_plan_id AND s.shop_id = b.shop_id
        WHERE b.items_json LIKE %s AND s.status != 'CANCELLED'
        ORDER BY s.sync_time DESC
        LIMIT 1
        """,
        (like_pattern,),
    )
    row = cursor.fetchone()
    if not row:
        return (
            Decimal("0"),
            Decimal("0"),
            {"note": "未找到有效货件", "shipment_id": None},
        )
    shipment_id = row["shipment_id"]

    # 货件下该 SKU 总数量（仅用于明细展示，不参与分摊公式）
    cursor.execute(
        "SELECT items_json FROM amazon_inbound_plan_boxes WHERE shipment_id = %s",
        (shipment_id,),
    )
    boxes = cursor.fetchall()
    total_qty = 0
    for box in boxes:
        items = json.loads(box.get("items_json") or "[]")
        if isinstance(items, list):
            for it in items:
                if it.get("msku") == seller_sku:
                    total_qty += int(it.get("quantity") or 0)

    # 运单总费用
    cursor.execute(
        """
        SELECT total_cost_cny FROM logistics_waybills
        WHERE shipment_id = %s ORDER BY created_at DESC LIMIT 1
        """,
        (shipment_id,),
    )
    waybill = cursor.fetchone()
    if not waybill or waybill.get("total_cost_cny") is None:
        return (
            Decimal("0"),
            Decimal("0"),
            {"note": "货件未绑定运单", "shipment_id": shipment_id},
        )
    total_cost_cny = Decimal(str(waybill["total_cost_cny"]))

    # 货件总重量
    cursor.execute(
        "SELECT weight_value, weight_unit FROM amazon_inbound_plan_boxes WHERE shipment_id = %s",
        (shipment_id,),
    )
    boxes = cursor.fetchall()
    total_weight_kg = Decimal("0")
    for box in boxes:
        w = Decimal(str(box.get("weight_value") or 0))
        unit = (box.get("weight_unit") or "").upper()
        if unit == "LB":
            w = w * _LB_TO_KG
        total_weight_kg += w
    if total_weight_kg <= 0:
        return (
            Decimal("0"),
            Decimal("0"),
            {"note": "货件总重量为0", "shipment_id": shipment_id},
        )

    # SKU 单件重量
    cursor.execute(
        "SELECT weight_kg FROM products WHERE seller_sku = %s LIMIT 1",
        (seller_sku,),
    )
    prod = cursor.fetchone()
    if not prod or prod.get("weight_kg") is None:
        return (
            Decimal("0"),
            Decimal("0"),
            {"note": "产品表无重量", "shipment_id": shipment_id},
        )
    sku_weight_kg = Decimal(str(prod["weight_kg"]))

    # 单件头程 = 总费用 * 单件重量 / 货件总重量
    unit_headway_cny = total_cost_cny * sku_weight_kg / total_weight_kg

    detail = {
        "shipment_id": shipment_id,
        "total_qty_in_shipment": total_qty,
        "waybill_total_cost_cny": float(total_cost_cny),
        "shipment_total_weight_kg": float(total_weight_kg),
        "sku_weight_kg": float(sku_weight_kg),
        "allocation_formula": f"{float(total_cost_cny)} * {float(sku_weight_kg)} / {float(total_weight_kg)}",
    }
    return unit_headway_cny, sku_weight_kg, detail


def get_unit_headway_cost(cursor, seller_sku: str, exchange_rate: Decimal) -> Tuple[Decimal, dict]:
    """
    计算单个 SKU 的单件头程成本（USD）。
    这是报表模块最常用的快捷入口。
    """
    headway_cny, sku_weight_kg, detail = get_headway_allocation(cursor, seller_sku)
    headway_usd = headway_cny * exchange_rate
    detail["headway_cny"] = float(headway_cny)
    detail["headway_usd"] = float(headway_usd)
    detail["exchange_rate"] = float(exchange_rate)
    return headway_usd, detail


def get_purchase_cost(cursor, seller_sku: str, exchange_rate: Decimal) -> Tuple[Decimal, Decimal, str]:
    """
    读取采购成本。
    返回 (cost_cny, cost_usd, source)
    默认按 CNY 处理，如果未来支持多币种可扩展。
    """
    cursor.execute(
        "SELECT purchase_cost FROM products WHERE seller_sku = %s LIMIT 1",
        (seller_sku,),
    )
    row = cursor.fetchone()
    if row and row.get("purchase_cost") is not None:
        cny = Decimal(str(row["purchase_cost"]))
        return cny, cny * exchange_rate, "products.purchase_cost"
    return Decimal("0"), Decimal("0"), "not_found"


# ---------------------------------------------------------------------------
# 统一对外接口
# ---------------------------------------------------------------------------

def get_unit_costs(cursor, seller_sku: str, exchange_rate: Optional[Decimal] = None) -> UnitCostBreakdown:
    """
    统一获取单件 SKU 的全部单位成本。
    这是「利润计算」的唯一入口，所有需要成本拆分的地方都应调用此函数。

    Args:
        cursor: 数据库游标
        seller_sku: SKU
        exchange_rate: 汇率，传 None 则自动查询

    Returns:
        UnitCostBreakdown 数据类
    """
    if exchange_rate is None:
        exchange_rate = get_exchange_rate(cursor)

    result = UnitCostBreakdown(seller_sku=seller_sku, exchange_rate=exchange_rate)

    # 产品基本信息
    cursor.execute(
        """
        SELECT product_name, weight_kg, dimensions_cm, asin, category_id
        FROM products WHERE seller_sku = %s LIMIT 1
        """,
        (seller_sku,),
    )
    prod = cursor.fetchone()
    if prod:
        result.product_name = prod.get("product_name") or ""
        result.asin = prod.get("asin") or ""
        if prod.get("weight_kg") is not None:
            result.weight_kg = Decimal(str(prod["weight_kg"]))

    # 采购成本
    purchase_cny, purchase_usd, purchase_src = get_purchase_cost(cursor, seller_sku, exchange_rate)
    result.purchase_cost_cny = purchase_cny
    result.purchase_cost_usd = purchase_usd
    result.sources["purchase_cost"] = purchase_src

    # 头程分摊
    headway_cny, _, headway_detail = get_headway_allocation(cursor, seller_sku)
    result.headway_cost_cny = headway_cny
    result.headway_cost_usd = headway_cny * exchange_rate
    result.sources["headway"] = headway_detail

    # FBA 费
    if result.weight_kg is not None:
        bw = get_billable_weight(result.weight_kg, prod.get("dimensions_cm") if prod else None)
        result.billable_weight_kg = bw
        fba_fee, fba_tier = get_fba_fee(cursor, bw)
        result.fba_fee_usd = fba_fee
        result.fba_tier = fba_tier
        result.sources["fba"] = f"tier:{fba_tier}, weight:{float(bw):.3f}kg"
    else:
        result.fba_fee_usd, result.fba_tier = _DEFAULT_FBA_FEE, "Small Standard (default)"
        result.sources["fba"] = "default: weight not found"

    # 佣金
    comm_rate, comm_src = get_commission_rate(cursor, seller_sku)
    result.commission_rate = comm_rate
    result.sources["commission"] = comm_src

    return result


def calculate_profit(
    sales_amount: Decimal,
    qty: int,
    unit_costs: UnitCostBreakdown,
    ad_cost: Decimal = Decimal("0"),
    refund_amount: Decimal = Decimal("0"),
    other_fees: Decimal = Decimal("0"),
) -> ProfitResult:
    """
    利润计算统一公式。

    计算逻辑（与 pricing.py 成本结构保持一致）：
      - 固定成本 = (采购成本 + 头程 + FBA) * 销量
      - 佣金     = 销售额 * 佣金率
      - 毛利     = 销售额 - 固定成本 - 佣金 - 退款
      - 净利     = 毛利 - 广告费 - 其他费用
      - 利润率   = 净利 / 销售额

    经营日报口径（兼容旧数据）：
      - 总成本   = 固定成本 + 佣金 + 退款 + 广告 + 其他
      - 日报毛利 = 销售额 - 总成本
      - 日报毛利率 = 日报毛利 / 销售额
    """
    qty_d = Decimal(str(qty))

    product_cost = unit_costs.purchase_cost_usd * qty_d
    headway_cost = unit_costs.headway_cost_usd * qty_d
    fba_fees = unit_costs.fba_fee_usd * qty_d

    commission = sales_amount * unit_costs.commission_rate

    gross_profit = sales_amount - product_cost - headway_cost - fba_fees - commission - refund_amount
    net_profit = gross_profit - ad_cost - other_fees
    profit_margin = (net_profit / sales_amount) if sales_amount > 0 else Decimal("0")

    # 经营日报兼容口径（总成本包含广告和退款）
    total_cost = product_cost + headway_cost + fba_fees + commission + refund_amount + ad_cost + other_fees
    gross_profit_daily = sales_amount - total_cost
    gross_profit_rate_daily = (gross_profit_daily / sales_amount) if sales_amount > 0 else Decimal("0")

    return ProfitResult(
        sales_amount=sales_amount,
        qty=qty,
        product_cost=product_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        fba_fees=fba_fees.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        commission=commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        headway_cost=headway_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        ad_cost=ad_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        refund_amount=refund_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        other_fees=other_fees.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        gross_profit=gross_profit.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        net_profit=net_profit.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        profit_margin=profit_margin.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
        total_cost=total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        gross_profit_daily=gross_profit_daily.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        gross_profit_rate_daily=gross_profit_rate_daily.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
    )


def calculate_suggested_price(
    fixed_cost_usd: Decimal,
    commission_rate: Decimal,
    ad_rate: Decimal,
    refund_rate: Decimal,
    target_profit_rate: Decimal,
) -> dict:
    """
    售价反推公式（pricing.py 专用）。

    公式：
      suggested_price = fixed_cost / (1 - commission_rate - ad_rate - refund_rate - target_profit_rate)
    """
    variable_rate = commission_rate + ad_rate + refund_rate
    denominator = Decimal("1") - variable_rate - target_profit_rate

    if denominator <= 0:
        return {
            "suggested_price": None,
            "commission": None,
            "ad_cost": None,
            "refund_cost": None,
            "total_cost": None,
            "profit_amount": None,
            "actual_profit_rate": None,
            "calc_note": (
                f"变动费率({float(variable_rate) * 100:.0f}%) + 目标利润率({float(target_profit_rate) * 100:.0f}%) "
                f"已超过 100%，无法计算出正数建议售价"
            ),
        }

    suggested_price = fixed_cost_usd / denominator
    commission = suggested_price * commission_rate
    ad_cost = suggested_price * ad_rate
    refund_cost = suggested_price * refund_rate
    total_cost = fixed_cost_usd + commission + ad_cost + refund_cost
    profit_amount = suggested_price - total_cost
    actual_profit_rate = profit_amount / suggested_price if suggested_price > 0 else Decimal("0")

    return {
        "suggested_price": suggested_price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "commission": commission.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "ad_cost": ad_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "refund_cost": refund_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "total_cost": total_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "profit_amount": profit_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        "actual_profit_rate": actual_profit_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
        "calc_note": None,
    }
