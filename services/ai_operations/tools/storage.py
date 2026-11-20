"""仓储费工具只读本任务冻结月表；不查其他店铺、ASIN或即时报告。"""
from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

from services.storage_fees.workspace import freeze_history
from services.listing_performance.common import DomainError
from ..metrics import digest
from . import data as tool_data


def attach(dataset, now=None):
    """页面及AI采用同一个月费归集，店铺仅提供总额，不泄露其他SKU明细。"""
    identity = dataset['content']['identity']
    now = now or datetime.now(ZoneInfo('Asia/Shanghai'))
    today = now.astimezone(ZoneInfo('Asia/Shanghai')).date()
    scopes = freeze_history(identity, today)
    shop, asin = scopes['shop'], scopes['asin']
    rows = []
    for scope, result in (('asin', asin), ('shop', shop)):
        # 原生估算和月费一样纳入本任务证据；店铺只注册日期/币种小计，不返回其他SKU。
        native = result['native_estimate']
        estimates = native['fees'] if scope == 'shop' else native['rows']
        for estimate in estimates:
            ref = 'storage-estimate:' + digest([identity, scope, estimate])[:24]
            estimate['evidence_id'] = ref
            entry = {'kind': 'storage', 'label': f'{estimate.get("snapshot_date") or "日期未知"} · 亚马逊原生仓储成本估算'}
            dataset['evidence'][ref] = {**entry, 'data': estimate}
            dataset['content']['evidence_index'][ref] = entry
        for value in result['rows']:
            ref = 'storage:' + digest([identity, scope, value])[:24]
            row = {**value, 'scope': scope, 'asin': identity['asin'] if scope == 'asin' else None, 'evidence_id': ref}
            rows.append(row)
            entry = {'kind': 'storage', 'label': f'{value["month"]} · {"店铺" if scope == "shop" else identity["asin"]}仓储费'}
            dataset['evidence'][ref] = {**entry, 'data': row}
            dataset['content']['evidence_index'][ref] = entry
    dataset['content']['storage_fees'] = {'rows': rows, 'native_estimates': {scope: value['native_estimate'] for scope, value in scopes.items()}, 'as_of': now.isoformat(), 'note': shop['note']+' 附带库龄为注明日期的库存快照，不是当月计费数量。', 'age_bands': shop['age_bands']}


def read(ctx, args):
    """ref为空/当前ASIN查本商品，shop查同店汇总；只返回完整月行并披露续查位置。"""
    identity = ctx.data['content']['identity']
    if args['window'] != 'all' or args['ref'] not in ('', 'shop', identity['asin'], 'estimate', 'shop_estimate'):
        raise DomainError('仓储费仅支持window=all；ref为当前ASIN/shop，原生估算使用estimate/shop_estimate', 422)
    frozen = ctx.data['content'].get('storage_fees')
    if frozen is None:
        return {'status': 'unavailable', 'rows': [], 'reason': '旧任务未冻结仓储费资料，不用当前数据补写历史'}
    scope = 'shop' if args['ref'] in ('shop','shop_estimate') else 'asin'
    native = None
    rows = [r for r in frozen['rows'] if r['scope'] == scope]
    if args['ref'] in ('estimate','shop_estimate'):
        native = frozen.get('native_estimates', {}).get(scope)
        if native is None:
            return {'status': 'unavailable', 'rows': [], 'reason': '此旧任务未冻结原生估算，不补查当前数据'}
        rows = native['fees'] if scope == 'shop' else native['rows']
    selected = rows[args['offset']:args['offset']+args['limit']]
    returned = deepcopy(tool_data.fit(selected, max(0, getattr(ctx, 'output_budget', tool_data.MAX_DATA_BYTES)-3000)))
    end = args['offset']+len(returned)
    result = {'status': 'partial' if end < len(rows) else 'provided', 'rows': returned, 'total': len(rows),
              'offset': args['offset'], 'returned': len(returned), 'has_more': end < len(rows),
              'next_offset': end if returned and end < len(rows) else None, 'requested_limit': args['limit'],
              'byte_limited': len(returned) < len(selected), 'as_of': frozen['as_of'],
              'scope': scope, 'age_bands': frozen['age_bands'], 'note': frozen['note']}
    if native is not None:
        result.update(note=native['note']+' '+native['scope_note'], estimate_status=native['status'],
                      source_field=native['source_field'], source_url=native['source_url'], result_type='native_estimate')
    if selected and not returned:
        result.update(status='budget_exceeded', reason='完整月行超过本次剩余预算，正文未交付')
    ctx.capture(result)
    return result
