"""事实包时间与汇总契约；完整词组不拆分，缺失值不补零。"""
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
import hashlib
import json

ADS_FIELDS = ('impressions', 'clicks', 'cost', 'purchases_7d', 'sales_7d')
SKU_FIELDS = ('sales_qty', 'sales_amount', 'ad_cost', 'net_profit', 'refund_amount', 'ad_sales', 'ad_orders')


def encode(value):
    """金额保留十进制字符串；日期保持ISO，不把大整数ID交给浮点数。"""
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    raise TypeError('不支持的事实类型')


def dumps(value):
    return json.dumps(value, ensure_ascii=False, default=encode, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(dumps(value).encode('utf-8')).hexdigest()


def windows(end):
    """最多60个自然日；所有窗口闭区间，前期与本期不重叠。"""
    result = []
    for size in (1, 3, 7, 14, 30):
        result.append({'key': f'd{size}', 'days': size, 'start': end - timedelta(days=size - 1), 'end': end})
        if size >= 7:
            result.append({'key': f'p{size}', 'days': size, 'start': end - timedelta(days=2 * size - 1),
                           'end': end - timedelta(days=size)})
    return result


def dates(start, end):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def totals(rows, fields):
    """空集合与存在空字段均返回空；已观测小计不声称是完整周期。"""
    result = {}
    for field in fields:
        values = [row.get(field) for row in rows]
        result[field] = (sum((Decimal(str(v)) for v in values), Decimal(0))
                         if values and all(v is not None for v in values) else None)
    return result


def ratios(values, ads=False):
    result = dict(values)
    for key, numerator, denominator in (
        (('acos', 'cost', 'sales_7d'), ('ctr', 'clicks', 'impressions'), ('cpc', 'cost', 'clicks'))
        if ads else (('acos', 'ad_cost', 'ad_sales'), ('tacos', 'ad_cost', 'sales_amount'),
                     ('profit_margin', 'net_profit', 'sales_amount'))
    ):
        n, d = values.get(numerator), values.get(denominator)
        result[key] = n / d if n is not None and d is not None and d > 0 else None
    return result


def summarize(rows, periods, fields, complete_days=()):
    result = []
    covered = set(complete_days)
    for period in periods:
        selected = [r for r in rows if period['start'] <= r['report_date'] <= period['end']]
        observed = {r['report_date'] for r in selected}
        missing = set(dates(period['start'], period['end'])) - covered
        result.append({**period, 'observed_days': len(observed), 'missing_days': sorted(missing),
                       'coverage': 'complete' if not missing else 'partial' if observed else 'unknown',
                       'metrics': ratios(totals(selected, fields), fields == ADS_FIELDS)})
    return result


def group_reports(rows):
    """不跨组/匹配方式合并搜索词；ID、表达式与词原文同时保留。"""
    groups = defaultdict(list)
    dimensions = ('campaign_id', 'ad_group_id', 'advertised_sku', 'advertised_asin',
                  'keyword_id', 'keyword_type', 'keyword_match_type', 'keyword_text',
                  'targeting_id', 'targeting_expression', 'customer_search_term', 'placement', 'currency')
    for row in rows:
        groups[tuple(row.get(k) for k in dimensions)].append(row)
    return [(dict(zip(dimensions, key)), value) for key, value in groups.items()]


def select_details(items, limit=80):
    """有界分层选取，覆盖花费、转化和曝光；全量汇总由调用方独立保留。"""
    chosen = {}
    # 自动和手动至少各保留一项（如果有），避免只按高花费淹没另一种投放。
    for kind in ('AUTO', 'MANUAL'):
        if len(chosen) >= limit:
            break
        typed = [r for r in items if r.get('campaign_type') == kind]
        if typed:
            item = max(typed, key=lambda r: (r['selection_metrics'].get('cost') or Decimal(0), r['evidence_id']))
            chosen[item['evidence_id']] = item
    for field in ('cost', 'purchases_7d', 'impressions'):
        ordered = sorted(items, key=lambda r: (-(r['selection_metrics'].get(field) or Decimal(0)),
                                                r['evidence_id']))
        for item in ordered[:max(1, limit // 3)]:
            if len(chosen) >= limit:
                break
            chosen[item['evidence_id']] = item
    for item in sorted(items, key=lambda r: (-(r['selection_metrics'].get('cost') or Decimal(0)),
                                            r['evidence_id'])):
        if len(chosen) >= limit:
            break
        chosen[item['evidence_id']] = item
    return list(chosen.values())
