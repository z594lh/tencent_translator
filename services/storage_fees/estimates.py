"""亚马逊原生滚动仓储成本估算；不推导费率，不重复加回超龄费。"""
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from collections import Counter

FIELD = 'estimated-storage-cost-next-month'
NOTE = '亚马逊按报告时点及当时销售速度给出的约30天滚动估算，包含月度仓储费、利用率附加费及预计超龄附加费；不是下个自然月账单，不再叠加超龄费。'
SOURCE = 'https://sellercentral.amazon.com.mx/help/hub/reference/external/GKT9YKCHXMSDVJKB'


def native_estimate(fees, sources, names, asin='', today=None):
    """同币种同日期才汇总；缺失/重复/非法数据不补0、不当完整店铺总额。"""
    today = today or date.today()
    stock = [r for r in sources if r['kind'] == 'health' and (not asin or r.get('asin') == asin)]
    keys = Counter((r.get('sku'), r.get('fnsku'), r.get('country_code'), r['payload'].get('condition')) for r in stock)
    rows, totals = [], {}
    for r in stock:
        p = r['payload']; raw = p.get('report_fields', {}).get(FIELD)
        reason = ''
        try:
            amount = Decimal(str(raw))
            if not amount.is_finite() or amount < 0:
                raise ValueError()
        except (InvalidOperation, TypeError, ValueError):
            amount = None; reason = '报告未提供有效估算'
        snapshot = str(r.get('record_date') or '')[:10]
        try:
            day = date.fromisoformat(snapshot)
        except ValueError:
            day = None
        currency = (r.get('currency') or '').strip().upper()
        if not day or not currency:
            reason = '缺少报告日期或币种'; amount = None
        if keys[(r.get('sku'), r.get('fnsku'), r.get('country_code'), p.get('condition'))] > 1:
            reason = '报告存在重复身份，未合计'; amount = None
        stale = bool(day and not 0 <= (today-day).days <= 7)
        if stale and not reason:
            reason = '报告超过7天或日期异常，仅供参考'
        row = {'seller_sku': r.get('sku'), 'asin': r.get('asin'), 'fnsku': r.get('fnsku'), 'condition': p.get('condition'),
               'product_name': names.get(r.get('sku')) or p.get('product_name') or '',
               'country_code': r.get('country_code'), 'currency': currency, 'snapshot_date': snapshot or None,
               'reference_date': (day+timedelta(days=30)).isoformat() if day else None,
               'amount': format(amount, '.4f') if amount is not None else None, 'stale': stale, 'reason': reason}
        rows.append(row)
        if amount is not None:
            key = (snapshot, currency)
            total = totals.setdefault(key, {'amount': Decimal(0), 'row_count': 0, 'stale': stale})
            total['amount'] += amount; total['row_count'] += 1
    current = [r for r in fees if not asin or r.get('asin') == asin]
    latest = max((r['month_of_charge'] for r in current), default=None)
    known = {r.get('sku') for r in stock}
    missing = sorted({r.get('seller_sku') or r.get('fnsku') for r in current if r['month_of_charge'] == latest} - known)
    valid = sum(r['amount'] is not None for r in rows)
    unavailable = len(rows)-valid
    status = 'unavailable' if not valid else 'stale' if any(r['stale'] for r in rows) else 'partial' if unavailable or missing else 'provided'
    return {'status': status, 'rows': rows, 'fees': [{'snapshot_date': day, 'currency': c, 'amount': format(v['amount'], '.4f'),
             'row_count': v['row_count'], 'stale': v['stale']} for (day, c), v in sorted(totals.items())],
            'report_rows': len(rows), 'provided_rows': valid, 'unavailable_rows': unavailable, 'missing_skus': missing,
            'scope_note': '汇总仅覆盖当前库存健康报告中有有效估算的记录；未包含的商品不视为0。最近月费存在但健康报告未覆盖的SKU另列。',
            'note': NOTE, 'source_url': SOURCE, 'source_field': FIELD}
