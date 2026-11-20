"""月份→SKU→仓库的只读查询；月费和库龄快照分开归集。"""
from datetime import date
from decimal import Decimal
import re

from services import storage_analysis_repository as repo
from services.storage_analysis_reports import valid_month
from services.storage_cost_analysis import complete_monthly_rows, fee_group, money, dec, sum_known
from services.fba_storage_fee_report_service import month_window
from . import repository
from .sorting import sort_rows, currencies
from .estimates import native_estimate

AGE_BANDS = [('0-to-90', '0–90天', 0, 90), ('91-to-180', '91–180天', 91, 180),
             ('181-to-270', '181–270天', 181, 270), ('271-to-365', '271–365天', 271, 365),
             ('366-plus', '366天及以上', 366, 10**9)]
NOTE = '月费为报告估算，净费=月费−激励抵扣；超龄附加费独立列示。月均库存为计费月平均数量；—表示无资料。'


def band_value(buckets, left, right):
    """完整覆盖才返回；粗细重叠档只选一条覆盖路径，不重复相加。"""
    intervals = []
    for item in buckets:
        match = re.fullmatch(r'(\d+)(?:-to-(\d+)|-(plus))', item.get('tier', ''))
        value = dec(item.get('quantity'))
        if match and value is not None and value >= 0:
            intervals.append((int(match[1]), int(match[2]) if match[2] else 10**9, value))
    def cover(start):
        if start > right:
            return Decimal(0)
        for a, b, value in sorted(intervals, key=lambda v: (v[0], -v[1])):
            if a == start and b <= right:
                rest = cover(b + 1)
                if rest is not None:
                    return value + rest
        return None
    return cover(left)


def _load(shop_id, start, end):
    """完整报告优先于旧投影；业务归集和SQL职责分离。"""
    original, sources, names = repository.read(shop_id, start, end)
    return complete_monthly_rows(original, sources), sources, names


def _ages(sources, month, asin=''):
    """整份快照选择，避免每个SKU取不同报告日期及同批次重复档位。"""
    candidates = []
    for row in sources:
        snapshot = row['payload'].get('age_snapshot_date')
        if row['kind'] in ('health', 'age_history') and snapshot and snapshot[:7] == month:
            candidates.append(row)
    if not candidates:
        return [], None
    # 一个健康报告可能包含多种库龄日期，先选择该月最新报告批次，再保留各行真实日期。
    latest = max(candidates, key=lambda r: (str(r.get('record_date')), str(r.get('data_at')), r['batch_id']))['batch_id']
    return [r for r in candidates if r['batch_id'] == latest and (not asin or r.get('asin') == asin)], latest


def _age_group(rows):
    return {key: money(sum_known([band_value(r['payload'].get('age_buckets', []), a, b) for r in rows]))
            for key, _, a, b in AGE_BANDS}


def _fees(rows):
    result = []
    for currency in sorted({(r.get('currency') or '').strip().upper() for r in rows}):
        group = fee_group([r for r in rows if (r.get('currency') or '').strip().upper() == currency])
        result.append({'currency': currency, 'storage_fee': money(group['storage']), 'incentive_fee': money(group['incentive']),
                       'net_fee': money(group['net']), 'average_quantity': money(group['quantity'])})
    return result


def _month_rows(fees, sources, months, asin=''):
    result = []
    for month in reversed(months):
        current = [r for r in fees if r['month_of_charge'] == month and (not asin or r.get('asin') == asin)]
        age, _ = _ages(sources, month, asin)
        aged = [r for r in sources if r['kind'] == 'aged' and r['period_key'] == month and (not asin or r.get('asin') == asin)]
        dates = sorted({r['payload']['age_snapshot_date'] for r in age})
        result.append({'month': month, 'fees': _fees(current), 'fee_status': 'provided' if current else 'unavailable',
                       'average_quantity': money(fee_group(current)['quantity']) if current else None,
                       'sku_count': len({r.get('seller_sku') or r.get('fnsku') for r in current}),
                       'age_quantities': _age_group(age), 'age_dates': dates, 'age_sku_count': len({r['sku'] for r in age}),
                       'aged_fees': [{'currency': currency, 'amount': money(sum_known([dec(r['payload'].get('amount')) for r in aged if r['currency'] == currency]))}
                                     for currency in sorted({r['currency'] for r in aged})]})
    return result


def freeze_history(identity, today):
    """同一次事务冻结商品和店铺汇总，避免两个范围碰上报告更新而不一致。"""
    expected = (int(identity['id']), identity['marketplace_id'], identity['credential_group_id'], identity['region'])
    if tuple(str(value) for value in repo.scope(identity['id'])) != tuple(str(value) for value in expected):
        raise ValueError('店铺范围已变化，请刷新后再查询仓储费')
    end = today.strftime('%Y-%m')
    fees, sources, names = _load(identity['id'], '0001-01', end)
    # AI历史不限于页面默认一年；只补齐近期横轴，更早月份按真实已保存资料列出。
    months = set(month_window(end)) | {r['month_of_charge'] for r in fees}
    months.update(r['period_key'] for r in sources if r['kind'] == 'aged')
    months.update(r['payload']['age_snapshot_date'][:7] for r in sources if r['kind'] in ('health','age_history') and r['payload'].get('age_snapshot_date'))
    months = sorted(m for m in months if re.fullmatch(r'[1-9][0-9]{3}-(0[1-9]|1[0-2])', m) and m <= end)
    return {scope: {'rows': _month_rows(fees, sources, months, asin), 'note': NOTE,
                    'native_estimate': native_estimate(fees, sources, names, asin, today),
                    'age_bands': [{'key': k, 'label': label} for k, label, _, _ in AGE_BANDS]}
            for scope, asin in (('shop', ''), ('asin', identity['asin']))}


def history(shop_id, query=None, *, today=None):
    query = query or {}
    today = today or date.today()
    available = repo.months(shop_id)
    latest = next((m for m in available if m <= today.strftime('%Y-%m')), None)
    if not query.get('end_month') and not latest:
        return {'months': [], 'rows': [], 'latest_month': None, 'age_bands': [], 'note': NOTE}
    end = valid_month(query.get('end_month') or latest)
    months = month_window(end)
    start = valid_month(query.get('start_month') or months[0])
    if start > end or (int(end[:4])-int(start[:4]))*12+int(end[5:])-int(start[5:]) > 35:
        raise ValueError('请选择不超过36个月的有效月份范围')
    index = int(start[:4])*12+int(start[5:])-1
    last = int(end[:4])*12+int(end[5:])-1
    months = [f'{i//12:04d}-{i%12+1:02d}' for i in range(index, last+1)]
    asin = (query.get('asin') or '').strip().upper()
    if asin and not re.fullmatch('[A-Z0-9]{10}', asin):
        raise ValueError('ASIN格式无效')
    fees, sources, _ = _load(shop_id, start, end)
    return {'months': months, 'rows': _month_rows(fees, sources, months, asin), 'asin': asin, 'latest_month': latest,
            'age_bands': [{'key': k, 'label': label} for k, label, _, _ in AGE_BANDS], 'note': NOTE}


def sku_list(shop_id, query):
    month = valid_month(query.get('month'))
    fees, sources, names = _load(shop_id, month, month)
    age, _ = _ages(sources, month)
    band = query.get('age_band') or ''
    if band and band not in {r[0] for r in AGE_BANDS}:
        raise ValueError('库龄阶段无效')
    search = (query.get('search') or '').strip().lower()
    aged = [r for r in sources if r['kind'] == 'aged' and r['period_key'] == month]
    skus = {r.get('seller_sku') or r.get('fnsku') for r in fees} | {r['sku'] for r in aged}
    rows = []
    for sku in sorted(s for s in skus if s):
        current = [r for r in fees if (r.get('seller_sku') or r.get('fnsku')) == sku]
        stock = [r for r in age if r['sku'] == sku]
        charged = [r for r in aged if r['sku'] == sku]
        asins = sorted({r.get('asin') for r in current + stock + charged if r.get('asin')})
        name = names.get(sku) or next((r.get('product_name') for r in current if r.get('product_name')), '')
        quantities = _age_group(stock)
        if query.get('aged_only') == '1' and not any((dec(r['payload'].get('amount')) or Decimal(0)) > 0 for r in charged):
            continue
        if search and search not in ' '.join([sku, name or '', *asins]).lower():
            continue
        if band and not (dec(quantities.get(band)) is not None and dec(quantities[band]) > 0):
            continue
        rows.append({'seller_sku': sku, 'product_name': name, 'asins': asins, 'fees': _fees(current),
                     'average_quantity': money(fee_group(current)['quantity']) if current else None,
                     'aged_quantity': money(sum_known([dec(r['payload'].get('quantity')) for r in charged])),
                     'aged_fees': [{'currency': currency, 'amount': money(sum_known([dec(r['payload'].get('amount')) for r in charged if r['currency'] == currency]))} for currency in sorted({r['currency'] for r in charged})],
                     'age_quantities': quantities, 'age_dates': sorted({r['payload']['age_snapshot_date'] for r in stock})})
    try:
        page, size = int(query.get('page', 1)), int(query.get('page_size', 20))
        if page < 1 or not 1 <= size <= 100:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError('分页参数无效') from None
    options = currencies(rows)
    rows = sort_rows(rows, query, {'average_quantity','aged_quantity','storage_fee','incentive_fee','net_fee','aged_fee'})
    return {'month': month, 'rows': rows[(page-1)*size:page*size], 'total': len(rows), 'currencies': options,
            'age_available': bool(age), 'note': NOTE}


def current_ages(shop_id, query):
    """只查询最新健康报告库龄，不附带历史计费月份或费用。"""
    current = date.today().strftime('%Y-%m')
    _, sources, names = _load(shop_id, current, current)
    stock = [r for r in sources if r['kind'] == 'health']
    band = query.get('age_band') or ''
    if band and band not in {r[0] for r in AGE_BANDS}:
        raise ValueError('库龄阶段无效')
    search = (query.get('search') or '').strip().lower()
    rows = []
    for sku in sorted({r['sku'] for r in stock if r.get('sku')}):
        records = [r for r in stock if r['sku'] == sku]
        asins = sorted({r['asin'] for r in records if r.get('asin')})
        name = names.get(sku) or next((r['payload'].get('product_name') for r in records), '') or ''
        quantities = _age_group(records)
        if search and search not in ' '.join([sku, name, *asins]).lower():
            continue
        if band and not (dec(quantities.get(band)) is not None and dec(quantities[band]) > 0):
            continue
        rows.append({'seller_sku': sku, 'product_name': name, 'asins': asins, 'age_quantities': quantities,
                     'available': money(sum_known([dec(r['payload'].get('available')) for r in records])),
                     'age_dates': sorted({r['payload']['age_snapshot_date'] for r in records if r['payload'].get('age_snapshot_date')})})
    try:
        page, size = int(query.get('page', 1)), int(query.get('page_size', 20))
        if page < 1 or not 1 <= size <= 100:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError('分页参数无效') from None
    rows = sort_rows(rows, query, {'age_'+k for k, _, _, _ in AGE_BANDS})
    return {'rows': rows[(page-1)*size:page*size], 'total': len(rows),
            'age_dates': sorted({r['payload']['age_snapshot_date'] for r in stock if r['payload'].get('age_snapshot_date')}),
            'age_bands': [{'key': k, 'label': label} for k, label, _, _ in AGE_BANDS]}


def warehouse_detail(shop_id, query):
    month = valid_month(query.get('month'))
    sku = (query.get('seller_sku') or '').strip()
    if not sku or len(sku) > 255:
        raise ValueError('请选择SKU')
    fees, sources, _ = _load(shop_id, month, month)
    current = [r for r in fees if (r.get('seller_sku') or r.get('fnsku')) == sku]
    rows = []
    for fc, country, currency in sorted({(r.get('fulfillment_center') or '', r.get('country_code') or '', r.get('currency') or '') for r in current}):
        records = [r for r in current if (r.get('fulfillment_center') or '', r.get('country_code') or '', r.get('currency') or '') == (fc, country, currency)]
        fnskus = {r.get('fnsku') for r in records}
        raw = [s['payload'] for s in sources if s['kind'] == 'monthly' and s['fnsku'] in fnskus and (s.get('fulfillment_center') or '') == fc and (s.get('country_code') or '') == country and (s.get('currency') or '') == currency]
        rates = {p.get('base_rate') for p in raw}
        volume_units = {p.get('volume_units') for p in raw}
        rows.append({'warehouse': fc, 'country_code': country, **_fees(records)[0], 'source_rows': len(records),
                     'volume': money(sum_known([dec(p.get('total_volume')) for p in raw])) if raw else money(sum_known([dec(r.get('estimated_total_item_volume')) for r in records])),
                     'volume_units': next(iter(volume_units)) if len(volume_units) == 1 else None,
                     'base_rate': next(iter(rates)) if len(rates) == 1 else None,
                     'base_fee': money(sum_known([dec(r.get('base_fee')) for r in records])),
                     'utilization_fee': money(sum_known([dec(r.get('utilization_fee')) for r in records]))})
    aged = [r for r in sources if r['kind'] == 'aged' and r['sku'] == sku]
    age, _ = _ages(sources, month)
    age = [r for r in age if r['sku'] == sku]
    return {'month': month, 'seller_sku': sku, 'rows': rows,
            'aged_rows': [{'charge_date': str(r['record_date'])[:10], 'country_code': r['country_code'], 'currency': r['currency'], **{k: r['payload'].get(k) for k in ('tier','quantity','rate','amount')}} for r in aged],
            'age_snapshot': {'quantities': _age_group(age), 'dates': sorted({r['payload']['age_snapshot_date'] for r in age}), 'bands': [{'key': k, 'label': label} for k, label, _, _ in AGE_BANDS]},
            'note': '库龄和超龄收费报告没有仓库维度，不分摊给仓库；基础费及利用率附加费为月费内参考拆分，不重复计费。'}


def forecast(shop_id, query=None, *, today=None):
    """只展示亚马逊官方滚动估算，不提供本地情景公式。"""
    query = query or {}
    today = today or date.today()
    asin = (query.get('asin') or '').strip().upper()
    if asin and not re.fullmatch('[A-Z0-9]{10}', asin):
        raise ValueError('ASIN格式无效')
    end = today.strftime('%Y-%m')
    loaded = _load(shop_id, month_window(end)[0], end)
    fees, sources, names = loaded
    native = native_estimate(fees, sources, names, asin, today)
    return {'asin': asin, 'native': native}

