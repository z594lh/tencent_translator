"""仓储成本工作台只读编排：精确月份/国家/币种，规则不是Amazon收费标准。"""
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

from services import storage_analysis_repository as repo
from services.storage_analysis_reports import valid_month, public_batch, country, REPORT_TYPES, overage_policy

ZERO = Decimal('0')


def dec(value):
    return Decimal(str(value)) if value is not None else None


def money(value):
    return format(value, '.4f') if value is not None else None


def previous_month(month):
    year, value = map(int, valid_month(month).split('-'))
    index = year * 12 + value - 2
    return f'{index // 12:04d}-{index % 12 + 1:02d}'


def change(current, previous):
    if current is None or previous is None:
        return {'amount': None, 'percent': None, 'direction': 'unknown'}
    delta = current - previous
    return {'amount': money(delta), 'percent': money(delta / abs(previous) * 100) if previous else None,
            'direction': 'up' if delta > 0 else 'down' if delta < 0 else 'flat'}


def sum_known(values):
    return sum(values, ZERO) if values and all(v is not None for v in values) else None


def component_gap(payload):
    values = [dec(payload.get(k)) for k in ('storage_fee','base_fee','utilization_fee')]
    return values[0]-values[1]-values[2] if all(v is not None for v in values) else None


def base_rate_verified(payload):
    """参考拆分缺口时独立验证原月费；不倒填基础费、不捏造缺口原因。"""
    if component_gap(payload) in (None, ZERO) or dec(payload.get('utilization_fee')) != ZERO:
        return False
    raw = payload.get('report_fields', {})
    try:
        volume, rate = dec(raw.get('estimated_total_item_volume')), dec(raw.get('base_rate'))
        if volume is None or rate is None or not volume.is_finite() or not rate.is_finite():
            return False
        return (volume * rate).quantize(Decimal('.01'), rounding=ROUND_HALF_UP) == dec(payload.get('storage_fee'))
    except (InvalidOperation, ValueError, TypeError):
        return False


def fee_group(rows):
    storage = sum_known([dec(row['estimated_monthly_storage_fee']) for row in rows])
    incentive = sum_known([dec(row['total_incentive_fee_amount']) for row in rows])
    quantities = [dec(row['average_quantity_on_hand']) for row in rows]
    quantity = sum(quantities, ZERO) if all(value is not None for value in quantities) else None
    known = [dec(r['estimated_monthly_storage_fee'])-dec(r['total_incentive_fee_amount']) for r in rows
             if r['estimated_monthly_storage_fee'] is not None and r['total_incentive_fee_amount'] is not None]
    return {'storage': storage, 'incentive': incentive, 'net': storage - incentive if storage is not None and incentive is not None else None,
            'quantity': quantity, 'known_net':sum_known(known)}


def complete_monthly_rows(original, sources):
    """按报告每一原行归集，同仓多行不是重复导入；完整批次替换已保证重复同步不叠加。"""
    mapping = {}
    for row in original:
        if row.get('fnsku') and row.get('seller_sku'):
            mapping.setdefault((row['fnsku'], country(row['country_code'])), set()).add(row['seller_sku'])
    for row in sources:
        if row.get('fnsku') and row.get('sku'):
            mapping.setdefault((row['fnsku'], row['country_code']), set()).add(row['sku'])
    groups = {}
    for row in sources:
        if row['kind'] == 'monthly' and row['payload'].get('charge_month'):
            p = row['payload']
            identity = p.get('business_key') or (p['charge_month'], row['fnsku'], row['fulfillment_center'], row['country_code'], row['currency'])
            groups.setdefault(identity, []).append(row)
    covered = {(records[0]['payload']['charge_month'], records[0]['country_code'], records[0]['currency']) for records in groups.values()}
    result = [{**r, 'fee_source':'原月费快照'} for r in original if (r['month_of_charge'], country(r['country_code']), r['currency']) not in covered]
    for records in groups.values():
        first, p = records[0], records[0]['payload']
        multiple = len(records) > 1 or p.get('identity_rows', 1) > 1
        def value(field):
            values = [dec(r['payload'].get(field)) for r in records]
            return sum_known(values)
        candidates = mapping.get((first['fnsku'], first['country_code']), set())
        result.append({'id':None, 'seller_sku':first.get('sku') or (next(iter(candidates)) if len(candidates) == 1 else None),
            'fnsku':first['fnsku'], 'asin':first.get('asin'), 'product_name':p.get('product_name'),
            'country_code':first['country_code'], 'currency':first['currency'], 'fulfillment_center':first['fulfillment_center'],
            'month_of_charge':p['charge_month'], 'product_size_tier':p.get('size_tier'),
            'estimated_monthly_storage_fee':value('storage_fee'), 'total_incentive_fee_amount':value('incentive_fee'),
            'average_quantity_on_hand':value('average_quantity'), 'base_fee':value('base_fee'), 'utilization_fee':value('utilization_fee'),
            'component_difference':sum_known([component_gap(r['payload']) for r in records]),
            'fee_source':'完整月报', 'ambiguous_identity':False, 'merged_source_rows':len(records) if multiple else 1})
    return result


def _key(row):
    return (row.get('seller_sku') or row.get('fnsku') or '', country(row.get('country_code')), (row.get('currency') or '').strip().upper())


def _source_key(row):
    return (row.get('sku') or '', row['country_code'], row['currency'])


def age_quantity(buckets, start=181):
    """优先完整的不重叠分档；缺档仅返回已知下限，绝不把粗/细交叉档相加。"""
    intervals = []
    for bucket in buckets:
        match = re.fullmatch(r'(\d+)(?:-to-(\d+)|-(plus))', bucket['tier'])
        value = dec(bucket['quantity'])
        if match and value is not None and value >= 0 and int(match[1]) >= start:
            intervals.append((int(match[1]), int(match[2]) if match[2] else 10**9, value))
    intervals.sort(key=lambda item: (item[0], item[1]))
    def cover(cursor):
        if cursor == 10**9 + 1:
            return ZERO
        for left, right, value in intervals:
            if left == cursor and right >= left:
                rest = cover(right + 1)
                if rest is not None:
                    return value + rest
        return None
    total = cover(start)
    if total is not None:
        return money(total), True
    known, end, count = ZERO, start - 1, 0
    # 缺档时选最宽的已知档，得到保守下限，不把未知当0。
    for left, right, value in sorted(intervals, key=lambda item: (item[0], -item[1])):
        if left > end:
            known += value
            end = right
            count += 1
    return money(known) if count else None, False


def fresh_date(snapshot):
    return bool(snapshot and timedelta(0) <= datetime.utcnow().date() - snapshot <= timedelta(days=7))


def health_context(rows):
    if len(rows) != 1:
        return None
    row = rows[0]
    payload = row['payload']
    snapshot = row['record_date']
    if isinstance(snapshot, str):
        snapshot = date.fromisoformat(snapshot)
    fresh = fresh_date(snapshot)
    age_date = payload.get('age_snapshot_date')
    age_date = date.fromisoformat(age_date) if age_date else None
    old, complete = age_quantity(payload['age_buckets'])
    oldest, _ = age_quantity(payload['age_buckets'], 366)
    stock, sales = dec(payload['available']), dec(payload['sales_30d'])
    days = stock / (sales / 30) if stock is not None and sales is not None and sales > 0 else None
    return {'snapshot_date': snapshot.isoformat() if snapshot else None, 'fresh': fresh,
            'available': money(stock), 'sales_30d': money(sales), 'days_of_supply': money(days),
            'currency': row['currency'], 'age_buckets': payload['age_buckets'], 'estimated_aged': payload['estimated_aged'],
            'age_snapshot_date': age_date.isoformat() if age_date else None, 'age_fresh': fresh_date(age_date),
            'aged_quantity': old, 'aged_quantity_complete': complete, 'over365_quantity': oldest,
            **{key: payload.get(key) for key in ('excess_quantity', 'pending_removal', 'unfulfillable',
                'inbound_quantity', 'reserved_quantity', 'fc_transfer', 'report_days_of_supply', 'amazon_recommended_action')}}


def classify(net, prior, share, aged, health, utilization=None):
    tags = []
    priority = 0
    if aged is not None and aged > 0:
        tags.append({'code': 'aged', 'label': '已有超龄收费', 'tone': 'danger'})
        priority = 3
    if utilization is not None and utilization > 0:
        tags.append({'code':'utilization', 'label':'有仓储利用率附加费', 'tone':'warning'})
        priority = max(priority, 2)
    if health and health['fresh']:
        stock, sales, days = dec(health['available']), dec(health['sales_30d']), dec(health['days_of_supply'])
        if stock and stock > 0 and sales == 0:
            tags.append({'code': 'slow', 'label': '有库存·近30天无销量', 'tone': 'danger'})
            priority = 3
        elif days and days > 120:
            tags.append({'code': 'slow', 'label': '预计消化超过120天', 'tone': 'warning'})
            priority = max(priority, 2)
        if dec(health.get('excess_quantity')) and dec(health['excess_quantity']) > 0:
            tags.append({'code': 'excess', 'label': '有预计冗余库存', 'tone': 'warning'})
            priority = max(priority, 2)
        if health.get('age_fresh', True) and dec(health.get('aged_quantity')) and dec(health['aged_quantity']) > 0:
            severe = bool(dec(health.get('over365_quantity')) and dec(health['over365_quantity']) > 0)
            tags.append({'code': 'aging', 'label': '有365天以上库存' if severe else '有180天以上库存', 'tone': 'danger' if severe else 'warning'})
            priority = max(priority, 3 if severe else 2)
        estimated_fee = any((dec(item['amount']) or ZERO) > 0 for item in health['estimated_aged'])
        unpriced_units = any(item['amount'] is None and (dec(item.get('quantity')) or ZERO) > 0 for item in health['estimated_aged'])
        if health.get('age_fresh', True) and (estimated_fee or unpriced_units):
            tags.append({'code': 'estimated', 'label': '有预计超龄费用' if estimated_fee else '有预计超龄收费库存·金额未知', 'tone': 'warning'})
            priority = max(priority, 2)
    if net is not None and prior is not None and net - prior >= 5 and (prior <= 0 or (net - prior) / prior >= Decimal('.3')):
        tags.append({'code': 'rising', 'label': '费用明显上涨', 'tone': 'warning'})
        priority = max(priority, 2)
    if share is not None and share >= 10 and net and net > 0:
        tags.append({'code': 'high', 'label': '费用占比≥10%', 'tone': 'info'})
        priority = max(priority, 1)
    return tags, priority


def dataset(shop_id, requested_month=None):
    binding = repo.scope(shop_id)
    available = repo.months(shop_id, binding)
    month = valid_month(requested_month) if requested_month else (available[0] if available else datetime.utcnow().strftime('%Y-%m'))
    prior = previous_month(month)
    original, products = repo.monthly_rows(shop_id, month, prior)
    batches, sources, schema_ready = repo.read_sources(binding, month, prior)
    fee_rows = complete_monthly_rows(original, sources)
    current, previous = {}, {}
    for row in fee_rows:
        key = _key(row)
        (current if row['month_of_charge'] == month else previous).setdefault(key, []).append(row)
    aged, health = {}, {}
    for row in sources:
        if row['kind'] == 'aged':
            aged.setdefault(_source_key(row), []).append(row)
        elif row['kind'] == 'health':
            health.setdefault((row.get('sku'), row['country_code']), []).append(row)
    totals, total_parts, known_parts, unknown_groups = {}, {}, {}, {}
    for key, values in current.items():
        total_parts.setdefault(key[1:], []).append(fee_group(values)['net'])
        known_parts.setdefault(key[1:], []).append(fee_group(values)['known_net'])
        unknown_groups[key[1:]] = unknown_groups.get(key[1:],0) + sum(r['estimated_monthly_storage_fee'] is None or r['total_incentive_fee_amount'] is None for r in values)
    totals = {key: sum_known(values) for key, values in total_parts.items()}
    # 健康独有SKU也纳入，避免历史月费尚未出现就漏掉当前积压；币种未知不借用站点默认币种。
    health_keys = {_source_key(row) for row in sources if row['kind'] == 'health'}
    result = []
    for key in sorted(set(current) | set(aged) | health_keys):
        sku, country, currency = key
        values = current.get(key, [])
        fees = fee_group(values) if values else None
        before = fee_group(previous[key])['net'] if key in previous else None
        net = fees['net'] if fees else None
        denominator = totals.get(key[1:])
        share = net / denominator * 100 if net is not None and denominator is not None and denominator > 0 else None
        paid = sum((dec(row['payload']['amount']) for row in aged[key]), ZERO) if key in aged else None
        health_rows = health.get((sku, country), [])
        context = health_context(health_rows)
        base = sum_known([dec(r.get('base_fee')) for r in values])
        utilization = sum_known([dec(r.get('utilization_fee')) for r in values])
        gap = sum_known([dec(r.get('component_difference')) for r in values])
        tags, priority = classify(net, before, share, paid, context, utilization)
        if values and net is None:
            tags.append({'code':'fee_review', 'label':'月报金额资料不全', 'tone':'warning'})
            priority = max(priority, 2)
        # 拆分参考列不是新增收费，结论采用月费原金额；不因参考差额触发经营风险。
        warehouse_groups = {}
        for item in values:
            fc = item['fulfillment_center'] or '未标明仓库'
            warehouse_groups.setdefault(fc, []).append(item)
        warehouse_costs = {fc:fee_group(records)['net'] for fc, records in warehouse_groups.items()}
        top = max(warehouse_costs, key=warehouse_costs.get) if warehouse_costs and all(v is not None for v in warehouse_costs.values()) else None
        product = products.get(sku, {})
        source_name = next((r['payload'].get('product_name') for r in sources if _source_key(r) == key and r['payload'].get('product_name')), '')
        name = product.get('product_name') or product.get('declare_name_cn') or (values[0].get('product_name') if values else source_name) or ''
        health_status = 'ambiguous' if len(health_rows) > 1 else 'missing' if not context else 'fresh' if context['fresh'] else 'stale'
        result.append({'seller_sku': sku, 'product_name': name, 'country_code': country, 'currency': currency,
                       'net_fee': money(net), 'storage_fee': money(fees['storage']) if fees else None,
                       'known_net_fee':money(fees['known_net']) if fees else None,
                       'incentive_fee': money(fees['incentive']) if fees else None,
                       'average_quantity': money(fees['quantity']) if fees else None,
                       'base_fee':money(base), 'utilization_fee':money(utilization),
                       'component_difference':money(gap),
                       'fee_source':values[0]['fee_source'] if values else '该月无月费',
                       'ambiguous_fee_rows':sum(bool(r.get('ambiguous_identity')) for r in values),
                       'identifier_type':'fnsku' if values and not values[0].get('seller_sku') else 'sku',
                       'prior_net_fee': money(before), 'change': change(net, before), 'share_percent': money(share),
                       'prior_fee_source':previous[key][0]['fee_source'] if key in previous else '无上月记录',
                       'aged_fee': money(paid), 'health': context, 'health_status': health_status, 'tags': tags, 'priority': priority,
                       'top_warehouse': {'code': top, 'net_fee': money(warehouse_costs[top])} if top else None,
                       'warehouses': sorted({row['fulfillment_center'] for row in values if row['fulfillment_center']}),
                       'size_tiers': sorted({row['product_size_tier'] for row in values if row['product_size_tier']})})
    selected_batches = [b for b in batches if b['kind'] == 'health' or b['period_key'] == month]
    return {'binding':binding, 'month': month, 'prior_month': prior, 'months': available, 'rows': result, 'original': original, 'fee_rows':fee_rows,
            'sources': sources, 'batches': selected_batches, 'schema_ready': schema_ready, 'totals': totals,
            'known_totals':{key:sum_known([v for v in values if v is not None]) for key,values in known_parts.items()},
            'unknown_amount_groups':unknown_groups}


def current_age_evidence(rows):
    """当前有效完整库龄只解释当前库存，不推断历史收费，缺日期/档位仍待更新。"""
    identities = {(r['seller_sku'], r['country_code']): r.get('health') for r in rows}
    valid = [h for h in identities.values() if h and h['fresh'] and h['age_fresh'] and h['aged_quantity_complete']]
    return {'count':len(valid), 'missing':len(identities)-len(valid),
            'quantity':sum((dec(h['aged_quantity']) for h in valid), ZERO)}


def report_coverage(data):
    """按固定来源披露覆盖，不将取消报告/未返回SKU当作已确认零收费。"""
    labels = {'monthly':'月度仓储费完整报告', 'health':'库存健康报告', 'aged':'长期 / 超龄收费报告', 'overage':'仓储超量费报告'}
    result = []
    for kind in ('monthly', 'health', 'aged', 'overage'):
        batch = next((r for r in data['batches'] if r['kind'] == kind), None)
        rows = [r for r in data['sources'] if r['kind'] == kind and (kind == 'health' or r.get('period_key', r['payload'].get('charge_month', data['month'])) == data['month'])]
        policy = overage_policy(data['binding'], data['month']) if kind == 'overage' else None
        multiple = len({r['payload'].get('business_key') for r in rows if r['payload'].get('identity_rows',1)>1})
        gaps = sum(component_gap(r['payload']) not in (None,ZERO) for r in rows) if kind == 'monthly' else 0
        verified = sum(base_rate_verified(r['payload']) for r in rows) if kind == 'monthly' else 0
        note = '原始明细完整保留；金额按每行月费减抵扣汇总' if kind == 'monthly' and rows else ''
        if multiple:
            note += f'；{multiple}组同仓多行已合并，不是重复导入'
        if gaps:
            note += f'；{gaps}行参考拆分与总额不同，采用原月费金额，不额外加费'
        if verified:
            note += f'；其中{verified}行原月费已按体积×基础费率复核一致'
        display_label, context_note = '', ''
        if kind == 'aged' and (not batch or (not batch.get('data_at') and batch['attempt_status'] in ('failed', 'no_data', 'missing'))):
            display_label = '暂无收费明细'
            evidence = current_age_evidence(data['rows'])
            if evidence['count'] and evidence['quantity'] == ZERO:
                context_note = f"当前{evidence['count']}个有效库龄SKU未见180天以上库存，可能尚无符合条件的超龄收费记录。"
                if evidence['missing']:
                    context_note += f"另{evidence['missing']}个SKU库龄资料待更新。"
            context_note += '未取得所选历史月收费明细，不等于已确认0收费；真实请求状态仍保留供查看。'
        result.append({**(policy or (public_batch(batch) if batch else {'attempt_status':'missing','row_count':0})),
            'kind':kind, 'label':labels[kind], 'report_type':REPORT_TYPES[kind],
            'scope':'当前库存快照' if kind == 'health' else '店铺 / 存储类型，不分摊SKU' if kind == 'overage' else data['month'],
            'field_count':max((len(r['payload'].get('report_fields', {})) for r in rows), default=0),
            'ambiguous_groups':0, 'merged_identity_groups':multiple,
            'component_mismatch_rows':gaps, 'verified_base_rate_rows':verified, 'processing_note':note,
            'display_status_label':display_label, 'context_note':context_note})
    return result


def analysis_summary(data):
    """全店分国家/币种生成结果和处理方向；不受筛选/分页影响，不捏造收费或销量。"""
    summaries = []
    for key in sorted({(r['country_code'], r['currency']) for r in data['rows']}):
        rows = [r for r in data['rows'] if (r['country_code'], r['currency']) == key]
        fees = [r for r in data['fee_rows'] if r['month_of_charge'] == data['month'] and (country(r['country_code']), r['currency']) == key]
        before = [r for r in data['fee_rows'] if r['month_of_charge'] == data['prior_month'] and (country(r['country_code']), r['currency']) == key]
        current, prior = fee_group(fees), fee_group(before)
        top = sorted([r for r in rows if dec(r['net_fee']) is not None], key=lambda r: dec(r['net_fee']), reverse=True)[:3]
        names = '、'.join(f"{r['product_name'] or r['seller_sku']}（{r['net_fee']}）" for r in top)
        slow = [r for r in rows if any(t['code']=='slow' for t in r['tags'])]
        no_sales = [r for r in slow if (dec(r['health']['sales_30d']) == ZERO)]
        aging = [r for r in rows if any(t['code']=='aging' for t in r['tags'])]
        excess = [r for r in rows if any(t['code']=='excess' for t in r['tags'])]
        actions = []
        if names:
            actions.append('月费主要集中于：'+names+'。先查看其仓库排名与单件月费，不因总费高直接清仓。')
        if slow or excess:
            actions.append(f'当前{len(slow)}个SKU库存消化慢（其中{len(no_sales)}个近30天无销量），{len(excess)}个有Amazon预计冗余。优先检查高费用且积压的SKU，评估减缓补货、促销及去库存成本。')
        if aging:
            actions.append(f'{len(aging)}个SKU存在180天以上库存；查看库龄数量与预计收费，再比较继续持有、促销和移除成本。')
        aged_batch = next((b for b in data['batches'] if b['kind']=='aged'), None)
        if not aged_batch or not aged_batch.get('data_at'):
            latest = previous_month(datetime.utcnow().strftime('%Y-%m'))
            recent = {latest, previous_month(latest)}
            followup = '计划任务继续自动复查' if data['month'] in recent else '可用补齐按钮更新历史报告'
            evidence = current_age_evidence(rows)
            current_age = ''
            if evidence['count']:
                description = '未见180天以上库存' if evidence['quantity'] == ZERO else f"180天以上库存合计{format(evidence['quantity'].normalize(), 'f')}件"
                current_age = f"当前{evidence['count']}个有效库龄SKU{description}。"
                if evidence['missing']:
                    current_age += f"另{evidence['missing']}个SKU库龄资料待更新。"
            actions.append(f'{current_age}暂无该历史月超龄收费明细，{followup}；未返回明细不代表已确认0收费。')
        summaries.append({'country_code':key[0], 'currency':key[1], 'storage_fee':money(current['storage']),
            'incentive_fee':money(current['incentive']), 'net_fee':money(current['net']),
            'change':change(current['net'], prior['net']), 'actions':actions,
            'slow_count':len(slow), 'excess_count':len(excess), 'aging_count':len(aging)})
    return summaries


def overage_rows(data):
    return [{'country_code':r['country_code'],'currency':r['currency'],
        'charge_date':r['record_date'].isoformat() if hasattr(r['record_date'],'isoformat') else r['record_date'],
        **{k:v for k,v in r['payload'].items() if k != 'report_fields'}} for r in data['sources'] if r['kind']=='overage']


def worklist(shop_id, query):
    data = dataset(shop_id, query.get('month'))
    rows = data['rows']
    currencies = sorted({row['currency'] for row in rows})
    countries = sorted({row['country_code'] for row in rows})
    term = (query.get('search') or '').lower()[:100]
    selected = [row for row in rows if
                (not query.get('currency') or row['currency'] == query['currency']) and
                (not query.get('country_code') or row['country_code'] == query['country_code']) and
                (not query.get('fulfillment_center') or query['fulfillment_center'] in row['warehouses']) and
                (not query.get('product_size_tier') or query['product_size_tier'] in row['size_tiers']) and
                (not term or term in (row['seller_sku'] + row['product_name']).lower()) and
                (not query.get('risk') or any(tag['code'] == query['risk'] for tag in row['tags']))]
    sort = query.get('sort', 'net_fee')
    if sort not in ('priority', 'net_fee', 'aged_fee', 'change', 'days_of_supply', 'aged_quantity'):
        raise ValueError('排序字段无效')
    if query.get('order', 'desc') not in ('asc', 'desc'):
        raise ValueError('排序方向无效')
    def value(row):
        raw = ((row['health'] or {}).get(sort) if sort in ('days_of_supply', 'aged_quantity') else
               row['priority'] if sort == 'priority' else row['change']['amount'] if sort == 'change' else row[sort])
        return dec(raw)
    present, missing = [r for r in selected if value(r) is not None], [r for r in selected if value(r) is None]
    present.sort(key=lambda row: (value(row), dec(row['net_fee']) or ZERO, row['seller_sku']), reverse=query.get('order', 'desc') == 'desc')
    selected = present + missing
    page = max(1, int(query.get('page', 1)))
    size = max(1, min(100, int(query.get('page_size', 20))))
    totals = [{'country_code': key[0], 'currency': key[1], 'net_fee': money(value),
               'known_net_fee':money(data['known_totals'].get(key)), 'unknown_amount_groups':data['unknown_amount_groups'].get(key,0)} for key, value in sorted(data['totals'].items())]
    return {'month': data['month'], 'prior_month': data['prior_month'], 'months': data['months'],
            'list': selected[(page - 1) * size:page * size], 'total': len(selected), 'totals': totals,
            'currencies': currencies, 'countries': countries, 'schema_ready': data['schema_ready'],
            'sources': report_coverage(data), 'summary':analysis_summary(data),
            'report_coverage':report_coverage(data), 'overage_rows':overage_rows(data),
            'risk_count': sum(bool(row['tags']) for row in rows),
            'rules': '预警独立判断，不要求费用上涨：已有超龄收费；有库存且近30天无销量、预计消化>120天；Amazon预计冗余/超龄费用；库龄>180天（>365天重点）。费用占比≥10%、涨费≥30%且增加≥5币种单位仍作参考。库龄天数只是运营筛查线，不是当地收费标准；报告/库龄快照分别超过7天暂停对应预警，缺资料不代表无风险。'}


def detail(shop_id, query):
    sku = (query.get('seller_sku') or '').strip()
    if not sku or 'country_code' not in query or 'currency' not in query:
        raise ValueError('详情需要SKU、国家、币种和计费月')
    data = dataset(shop_id, query.get('month'))
    key = (sku, query['country_code'], query['currency'])
    row = next((item for item in data['rows'] if (item['seller_sku'], item['country_code'], item['currency']) == key), None)
    if row is None:
        raise ValueError('当前范围没有该SKU资料')
    original = [r for r in data['original'] if _key(r) == key]
    current = [r for r in data['fee_rows'] if _key(r) == key and r['month_of_charge'] == data['month']]
    groups = {}
    for r in current:
        groups.setdefault(r['fulfillment_center'] or '未标明仓库', []).append(r)
    warehouses = []
    for fc, records in groups.items():
        fees = fee_group(records)
        before_rows = [r for r in data['fee_rows'] if _key(r) == key and r['month_of_charge'] == data['prior_month'] and r['fulfillment_center'] == fc]
        before = fee_group(before_rows)['net'] if before_rows else None
        warehouse = {'warehouse': fc, 'net_fee': money(fees['net']), 'average_quantity': money(fees['quantity']),
                     'share_percent': money(fees['net'] / dec(row['net_fee']) * 100) if dec(row['net_fee']) and dec(row['net_fee']) > 0 else None,
                     'unit_monthly_fee': money(fees['net'] / fees['quantity']) if fees['net'] is not None and fees['quantity'] and fees['quantity'] > 0 else None,
                     'change': change(fees['net'], before), 'base_fee': None, 'utilization_fee': None}
        # 只有FNSKU/仓库/国家/币种和金额都与原月费快照一致，才附加拆分，绝不再加回净费。
        matches = []
        for record in records:
            found = [s for s in data['sources'] if s['kind'] == 'monthly' and (s['payload'].get('charge_month') or s.get('period_key') or data['month']) == data['month'] and s['fnsku'] == record['fnsku'] and s['fulfillment_center'] == record['fulfillment_center'] and s['country_code'] == key[1] and s['currency'] == key[2]]
            if (len(found) != 1 or dec(found[0]['payload']['storage_fee']) != dec(record['estimated_monthly_storage_fee'])
                    or dec(found[0]['payload']['incentive_fee']) != dec(record['total_incentive_fee_amount'])):
                break
            matches.append(found[0]['payload'])
        if matches and len(matches) == len(records):
            for source, target in (('base_fee', 'base_fee'), ('utilization_fee', 'utilization_fee')):
                amounts = [dec(item[source]) for item in matches]
                if all(value is not None for value in amounts):
                    warehouse[target] = money(sum(amounts, ZERO))
        if all(r.get('fee_source') == '完整月报' for r in records):
            warehouse['base_fee'] = money(sum_known([dec(r.get('base_fee')) for r in records]))
            warehouse['utilization_fee'] = money(sum_known([dec(r.get('utilization_fee')) for r in records]))
        warehouses.append(warehouse)
    warehouses.sort(key=lambda item: (item['net_fee'] is not None, dec(item['net_fee']) or ZERO), reverse=True)
    reasons = [tag['label'] for tag in row['tags']]
    overview = '；'.join(reasons) if reasons else '当前资料未触发关注规则，仍可核对费用变化。'
    if warehouses and warehouses[0]['net_fee'] is not None:
        prefix = '已知月费仓库中最高' if any(w['net_fee'] is None for w in warehouses) else '月度费用最高仓库'
        overview += f'。{prefix}：{warehouses[0]["warehouse"]}；费用高可能源于库存多，请结合单件月费核对。'
    positive_changes = [item for item in warehouses if item['change']['amount'] is not None and dec(item['change']['amount']) > 0]
    if positive_changes:
        largest = max(positive_changes, key=lambda item: dec(item['change']['amount']))
        overview += f' 较上月可比仓库中增费最多：{largest["warehouse"]}（+{largest["change"]["amount"]} {key[2]}）。'
    codes = {tag['code'] for tag in row['tags']}
    suggestions = []
    if 'aged' in codes:
        suggestions.append('优先核对超龄收费档位和数量，再结合库存快照评估去库存方案；移除或弃置须比较相关成本后人工决定。')
    if 'slow' in codes:
        suggestions.append('先核对库存是否可售及销量是否完整，再评估减缓补货、促销或清理积压；不要只因总费用高直接清库存。')
    if codes & {'aging', 'estimated', 'excess'}:
        suggestions.append('按当前库龄、Amazon预计冗余数量和预计收费档位排查待消化库存；先比较促销、持有与移除成本，再人工决定处理，不将预计费用当已收。')
    if 'rising' in codes:
        suggestions.append('查看增费最多仓库的月均库存、基础费和利用率附加费，区分存放数量增加与单位成本变化。')
    if 'fee_review' in codes:
        suggestions.insert(0, '月费必需金额资料缺失，暂不生成完整费用结论；系统后续更新报告，不把未知金额当0。')
    if 'utilization' in codes:
        suggestions.insert(0, '仓储利用率附加费来自月费报告，与按库存年龄计收的超龄费不同；先核对利用率比率及计费库存/体积，不把这项拆分再次加回月费。')
    if not row['health'] or not row['health']['fresh']:
        suggestions.append('先更新健康快照再判断当前积压；历史月均库存不能代替今天库存，也不能反推库龄。')
    if not suggestions:
        suggestions.append('结合单件月费、销售速度及历史趋势复核；费用占比高只是排查优先级，不代表收费异常。')
    aged_rows = [{**r['payload'], 'charge_date': r['record_date'].isoformat() if hasattr(r['record_date'], 'isoformat') else r['record_date']} for r in data['sources'] if r['kind'] == 'aged' and _source_key(r) == key]
    for r in original:
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = money(v)
            elif hasattr(v, 'isoformat'):
                r[k] = v.isoformat()
    return {'month': data['month'], 'prior_month': data['prior_month'], 'sku': row, 'overview': overview,
            'suggestions': suggestions[:3],
            'monthly_report_rows':[{'fnsku':s['fnsku'], 'warehouse':s['fulfillment_center'], **s['payload'], 'component_difference':money(component_gap(s['payload']))} for s in data['sources'] if s['kind']=='monthly' and s['payload'].get('charge_month') == data['month'] and any(s['fnsku']==r['fnsku'] and s['country_code']==key[1] and s['currency']==key[2] for r in current)],
            'health_report_rows':[{'fnsku':s['fnsku'], 'snapshot_date':s['record_date'].isoformat() if hasattr(s['record_date'],'isoformat') else s['record_date'],
                'report_fields':s['payload'].get('report_fields',{})} for s in data['sources'] if s['kind']=='health' and s.get('sku')==sku and s['country_code']==key[1]],
            'warehouses': warehouses, 'aged_rows': aged_rows, 'raw_rows': original,
            'sources': [public_batch(r) for r in data['batches']],
            'note': '月度费用为报告估算，超龄费用为独立收费数据，两者不混为已结算总额；超龄报告没有仓库字段，不分摊到仓库。当前库存快照不代表历史月末库存。'}
