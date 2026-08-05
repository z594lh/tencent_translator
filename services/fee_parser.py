"""
Amazon Finances items_json 费用解析公共函数

兼容 FBAPerUnitFulfillmentFee 的两种结构（2026-07-30 起 Amazon 开始叠加 Promo 抵扣）:
  - 普通:     顶层 breakdownAmount 即真实费用（负数），子项只有 Base
  - 促销:     顶层为 Base + Promo 净额（可能为 0），真实费用在子项 Base（负数）中
  规则: 子项存在时取子项负数之和（真实扣费，Promo 为正不计入）；
        子项为空时回退读顶层负数。

finances.py / report_generator.py / backfill 脚本共用，避免多处重复逻辑不一致。
"""


def _negative_children_or_top(sub):
    """取子项中负数之和; 无子项时取顶层负数绝对值。"""
    children = sub.get("breakdowns", []) or []
    if children:
        total = 0.0
        for ch in children:
            amt = float((ch.get("breakdownAmount") or {}).get("currencyAmount", 0))
            if amt < 0:
                total += abs(amt)
        return total
    amt = float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
    return abs(amt) if amt < 0 else 0.0


def extract_fba_commission(amazon_fees_breakdown):
    """从单个 AmazonFees breakdown 提取 (fba_fee, commission)，均为正数。"""
    fba = 0.0
    commission = 0.0
    for sub in (amazon_fees_breakdown.get("breakdowns", []) or []):
        st = sub.get("breakdownType", "")
        if st.startswith("FBAPer"):
            fba += _negative_children_or_top(sub)
        elif st == "Commission":
            commission += _negative_children_or_top(sub)
    return fba, commission


def extract_fees_from_items(items):
    """从 items_json 数组提取 (product_charges, fba_fees, commission)，均为正数。"""
    pc = 0.0
    fba = 0.0
    commission = 0.0
    for item in (items or []):
        for bd in (item.get("breakdowns", []) or []):
            bt = bd.get("breakdownType", "")
            if bt == "ProductCharges":
                subs = bd.get("breakdowns", []) or []
                if subs:
                    for sub in subs:
                        amt = float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
                        if amt > 0:
                            pc += amt
                else:
                    amt = float((bd.get("breakdownAmount") or {}).get("currencyAmount", 0))
                    if amt > 0:
                        pc += amt
            elif bt == "AmazonFees":
                f, c = extract_fba_commission(bd)
                fba += f
                commission += c
    return round(pc, 2), round(fba, 2), round(commission, 2)
