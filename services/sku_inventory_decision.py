"""SKU现货回收贡献只读参考；不重算原利润、不执行广告/库存/补货操作。"""
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

MIN_UNITS = 10
RECOVERY_STEP_PERCENT = 10
MAX_AGE_DAYS = 3
FIELDS = ('gross_profit', 'product_cost', 'headway_cost', 'sales_amount',
          'fba_fees', 'platform_fees', 'ad_cost', 'refund_amount')
LIMITATIONS = [
    '回收贡献是历史经营估算，不是实际到账；现货能回收成本，不等于值得再次补货。',
    '未计未来等待售出产生的仓储/超龄费、弃置费或清算回收额，不能据此断言立即弃置更划算。',
    '退款沿用原日报分摊，近期广告数据可能补充；没有调价/投放变更快照，历史均值不保证代表调整后效果。',
    '库存为主表报表快照，消化天数仅按历史速度静态估算，不保证全部库存按同样价格售出。',
]


def dec(value):
    if value is None or isinstance(value, bool) or str(value).strip() == '':
        return None
    try:
        number = Decimal(str(value))
        return number if number.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def fmt(value):
    return format(value, '.4f') if value is not None else None


def period(facts, end, days):
    first = end - timedelta(days=days - 1)
    selected = [row for row in facts if first <= row['report_date'] <= end]
    result = {'days': days, 'date_from': str(first), 'date_to': str(end),
              'recorded_days': len(selected), 'complete': len(selected) == days,
              'sales_qty': None, 'profit': None, 'product_cost': None, 'headway_cost': None,
              'ad_cost': None, 'unit_profit': None, 'unit_cost': None, 'unit_recovery': None,
              'recovery_ratio': None, 'costs_known': False, 'reason': ''}
    if not selected:
        result['reason'] = '没有日报'
        return result
    parsed = []
    for row in selected:
        values = {field: dec(row.get(field)) for field in FIELDS}
        qty = dec(row.get('sales_qty'))
        if any(value is None for value in values.values()) or qty is None or qty < 0 or qty != qty.to_integral_value():
            result['reason'] = '日报数量或金额不完整'
            return result
        # 核对原利润口径，不修复/覆盖来源金额；超出分币舍入容差时暂停判断。
        expected = values['sales_amount'] - sum(values[field] for field in FIELDS if field not in ('sales_amount', 'gross_profit'))
        if abs(expected - values['gross_profit']) > Decimal('0.03'):
            result['reason'] = '日报成本拆分与原利润不一致'
            return result
        parsed.append((qty, values))
    qty = sum(q for q, _ in parsed)
    totals = {field: sum(values[field] for _, values in parsed) for field in FIELDS}
    result.update(sales_qty=int(qty), **{key: fmt(totals[source]) for key, source in
        (('profit', 'gross_profit'), ('product_cost', 'product_cost'), ('headway_cost', 'headway_cost'), ('ad_cost', 'ad_cost'))})
    costs_known = all(q == 0 or (values['product_cost'] > 0 and values['headway_cost'] > 0) for q, values in parsed)
    result['costs_known'] = costs_known
    if qty > 0:
        capital = totals['product_cost'] + totals['headway_cost']
        recovery = totals['gross_profit'] + capital
        result.update(unit_profit=fmt(totals['gross_profit'] / qty), unit_cost=fmt(capital / qty),
                      unit_recovery=fmt(recovery / qty), recovery_ratio=fmt(recovery / capital) if capital > 0 else None)
    if not result['complete']:
        result['reason'] = f"日报覆盖{len(selected)}/{days}天，不足部分不补零"
    elif qty == 0:
        result['reason'] = '期间没有销量，不能计算每件回收贡献'
    elif not costs_known:
        result['reason'] = '正销量日报的产品成本或头程为0/缺失，成本来源待核实'
    elif qty < MIN_UNITS:
        result['reason'] = f'销量不足{MIN_UNITS}件，样本偏少'
    return result


def unknown(reason):
    return {'code': 'insufficient', 'label': '暂不能判断', 'summary': reason,
            'currency': 'USD', 'basis_days': None, 'date_to': None, 'periods': [],
            'trend': None, 'stock': None, 'stock_cover_days': None,
            'recovery_band': None, 'limitations': LIMITATIONS,
            'rules': {'min_units': MIN_UNITS, 'recovery_step_percent': RECOVERY_STEP_PERCENT, 'max_age_days': MAX_AGE_DAYS}}


def build_decision(facts, latest, report_row, today=None):
    result = unknown('没有可用的SKU日报')
    if latest is None:
        return result
    result['date_to'] = str(latest)
    seen = set()
    for row in facts:
        if row['report_date'] in seen:
            result['summary'] = '同一SKU/日期有多条日报，无法安全合并判断'
            return result
        seen.add(row['report_date'])
    periods = {days: period(facts, latest, days) for days in (7, 14, 30)}
    result['periods'] = list(periods.values())
    result['basis_days'] = 14
    basis = periods[14]
    today = today or datetime.now(ZoneInfo('America/Los_Angeles')).date()
    if (today - latest).days > MAX_AGE_DAYS or latest > today:
        result['summary'] = '日报日期过旧或异常，先更新数据再判断'
        return result
    if basis['complete'] and basis['sales_qty'] == 0:
        result['summary'] = '近14天无销量，先控制无效消耗，不套用更早的盈利结果'
        return result
    # 只在14天样本不足时回看30天；缺成本/缺日报不得用旧数据掩盖。
    if basis['complete'] and basis['costs_known'] and 0 < (basis['sales_qty'] or 0) < MIN_UNITS:
        candidate = periods[30]
        if candidate['complete'] and candidate['costs_known'] and (candidate['sales_qty'] or 0) >= MIN_UNITS:
            result['basis_days'] = 30
            basis = candidate
            result['fallback_note'] = '14天样本偏少，当前判断参考30天；不能视为最近14天已改善'
    if basis['reason']:
        result['summary'] = basis['reason']
        return result
    profit = dec(basis['profit'])
    capital = dec(basis['product_cost']) + dec(basis['headway_cost'])
    recovery = profit + capital
    if profit > 0:
        result.update(code='profitable', label='盈利销售', summary='本期销售有利润；结合库存与近期变化评估，不据此自动加仓')
    elif profit == 0:
        result.update(code='thin', label='持平销售', summary='本期基本持平，尚需考虑后续仓储和经营波动')
    elif recovery < 0:
        result.update(code='draining', label='继续销售额外亏钱', summary='除库存成本外还在额外亏损；优先控制亏损流量，再比较调整销售与其他处置方案')
    else:
        # 按未舍入的同期总额分档，避免显示百分比取整后跨入错误档位。
        band = min(9, int(recovery * 10 / capital))
        interval = '不足10%' if band == 0 else f'{band * 10}%～{(band + 1) * 10}%'
        label = f'亏损但能回收部分成本（{interval}）' if recovery > 0 else '亏损且无成本回收（0%）'
        result.update(code='recoverable', label=label, recovery_band=band,
                      summary='回收比例＝每件回收贡献÷每件成本（产品＋头程）；每10%一档，包含下限、不含上限。可按清库存目标评估销售，暂缓补货；仍需考虑未来仓储及处置费用。' if recovery > 0 else '每件回收贡献为0，无法回收产品及头程成本；尚未计未来仓储及处置费用。')
    recent, previous = periods[7], period(facts, latest - timedelta(days=7), 7)
    if all(p['complete'] and p['costs_known'] and (p['sales_qty'] or 0) >= MIN_UNITS and p['unit_recovery'] is not None for p in (recent, previous)):
        change = dec(recent['unit_recovery']) - dec(previous['unit_recovery'])
        result['trend'] = {'change': fmt(change), 'direction': 'up' if change > 0 else 'down' if change < 0 else 'flat',
                           'text': '近7天与前7天每件回收贡献对比'}
    stock = dec(report_row.get('stock'))
    if stock is not None and stock >= 0:
        result['stock'] = fmt(stock)
        result['stock_cover_days'] = fmt(stock * basis['days'] / basis['sales_qty'])
    return result


def attach_decisions(cursor, report_rows):
    """只读取当前页身份的30天源数据；最多100组合，不扫描或写入全店业务数据。"""
    if not report_rows:
        return
    pairs = sorted({(int(row['shop_id']), row['sku']) for row in report_rows})
    if len(pairs) > 100:
        raise ValueError('经营判断超过当前页上限')
    condition = ' OR '.join(['(shop_id=%s AND sku=%s)'] * len(pairs))
    arguments = tuple(value for pair in pairs for value in pair)
    cursor.execute(f'SELECT shop_id,sku,MAX(report_date) AS latest_date FROM sku_profit WHERE {condition} GROUP BY shop_id,sku', arguments)
    latest = {(row['shop_id'], row['sku']): row['latest_date'] for row in cursor.fetchall()}
    shops = sorted({pair[0] for pair in pairs})
    cursor.execute('SELECT id,region,marketplace_id FROM amazon_shops WHERE id IN (' + ','.join(['%s'] * len(shops)) + ')', shops)
    known_shops = {row['id']: row.get('region') == 'na' and row.get('marketplace_id') == 'ATVPDKIKX0DER' for row in cursor.fetchall()}
    ranges, params = [], []
    for pair in pairs:
        if latest.get(pair) is not None and known_shops.get(pair[0]):
            ranges.append('(shop_id=%s AND sku=%s AND report_date BETWEEN %s AND %s)')
            params.extend((*pair, latest[pair] - timedelta(days=29), latest[pair]))
    facts = {}
    if ranges:
        cursor.execute('SELECT shop_id,sku,report_date,sales_qty,' + ','.join(FIELDS) +
                       ' FROM sku_profit WHERE ' + ' OR '.join(ranges) + ' ORDER BY shop_id,sku,report_date LIMIT 3001', params)
        rows = cursor.fetchall()
        if len(rows) > 3000:
            for row in report_rows:
                row['inventory_decision'] = unknown('日报行数超出安全范围，暂不判断')
            return
        for row in rows:
            facts.setdefault((row['shop_id'], row['sku']), []).append(row)
    for row in report_rows:
        pair = (int(row['shop_id']), row['sku'])
        if not known_shops.get(pair[0]):
            row['inventory_decision'] = unknown('首版仅支持已核实的美国站USD日报，当前站点币种待核实')
            row['inventory_decision']['currency'] = None
        else:
            row['inventory_decision'] = build_decision(facts.get(pair, []), latest.get(pair), row)
