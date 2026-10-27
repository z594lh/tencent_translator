"""统一历史：页面仅读本地，平台采集按范围、可续传且不覆盖ERP原始证据。"""
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


from services import ads_history_repository as repo
from services import ads_display
from services import ads_negative_service as identities

HistoryError = repo.HistoryError
DISPLAY_TZ = timezone(timedelta(hours=8))
QUERY_TIMEZONES = ('Asia/Shanghai', 'America/Los_Angeles', 'UTC')
KINDS = ('CAMPAIGN', 'AD_GROUP', 'AD', 'KEYWORD', 'NEGATIVE_KEYWORD', 'PRODUCT_TARGETING', 'THEME')
FILTERS = {'BUDGET_AMOUNT', 'IN_BUDGET', 'STATUS', 'NAME', 'START_DATE', 'END_DATE',
           'PLACEMENT_GROUP', 'SMART_BIDDING_STRATEGY', 'DEFAULT_BID_AMOUNT', 'BID_AMOUNT', 'CREATED'}
CATEGORIES = {
    'settings': ('CAMPAIGN', 'AD_GROUP'),
    'targeting': ('KEYWORD', 'PRODUCT_TARGETING', 'THEME'),
    'negative': ('NEGATIVE_KEYWORD', 'NEGATIVE_PRODUCT_TARGETING'),
    'ad': ('AD',),
}


def context(params, allow_shop=False):
    acct = repo.account(params.get('shop_id'))
    scope = params.get('scope') or ('group' if params.get('ad_group_id') else 'campaign')
    if scope == 'shop' and allow_shop:
        if params.get('campaign_id') or params.get('ad_group_id'):
            raise HistoryError('店铺级历史不能指定活动或组')
        return {**acct, 'scope': 'shop', 'campaign_id': None, 'ad_group_id': None}
    try:
        identity = identities.context(acct['shop_id'], params.get('campaign_id'), params.get('ad_group_id'), scope)
    except identities.NegativeError as exc:
        raise HistoryError(str(exc), exc.code) from None
    return {**acct, **{k: identity[k] for k in ('scope', 'campaign_id', 'ad_group_id')}}


def _integer(value, label):
    if isinstance(value, bool) or not re.fullmatch(r'[0-9]{1,16}', str(value)):
        raise HistoryError(f'{label} 无效')
    return int(value)


def _date(value, label, display_tz=DISPLAY_TZ):
    try:
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', str(value)):
            raise ValueError()
        return datetime.strptime(value, '%Y-%m-%d').replace(tzinfo=display_tz)
    except (ValueError, TypeError):
        raise HistoryError(f'{label} 须为有效YYYY-MM-DD日期') from None


def query(params, now=None):
    ctx = context(params)
    tz_name = params.get('timezone') or 'Asia/Shanghai'
    if tz_name not in QUERY_TIMEZONES:
        raise HistoryError('历史时区无效')
    display_tz = ZoneInfo(tz_name)
    today = (now or datetime.now(timezone.utc)).astimezone(display_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    start = _date(params.get('start_date') or (today - timedelta(days=89)).strftime('%Y-%m-%d'), '开始日期', display_tz)
    end = _date(params.get('end_date') or today.strftime('%Y-%m-%d'), '结束日期', display_tz)
    if start > end or end > today:
        raise HistoryError('日期范围无效，结束日期不能晚于今天')
    page = _integer(params.get('page', 1), '页码')
    size = _integer(params.get('page_size', 50), '每页条数')
    if size not in (20, 50, 100, 200) or not 1 <= page <= 100000:
        raise HistoryError('分页范围无效')
    where = ['region=%s', 'profile_id=%s', 'campaign_id=%s', 'merged_into_id IS NULL', 'event_time_ms BETWEEN %s AND %s']
    args = [ctx['region'], ctx['profile_id'], str(ctx['campaign_id']), int(start.timestamp()*1000),
            int((end + timedelta(days=1)).timestamp()*1000)-1]
    if ctx['scope'] == 'group':
        where.append('ad_group_id=%s'); args.append(str(ctx['ad_group_id']))
    # 活动历史的后台口径不包含广告组内事件，但包含活动级否定投放。
    # 旧接口显式 include_children 的行为保持，新页面使用 view 消除歧义。
    view = params.get('view') or ('all' if params.get('include_children') == '1' else
                                  'entity' if params.get('include_children') == '0' else 'console')
    if view not in ('console', 'all', 'entity'):
        raise HistoryError('历史范围无效')
    if view == 'entity':
        where.append('entity_type=%s'); args.append('AD_GROUP' if ctx['scope'] == 'group' else 'CAMPAIGN')
    elif view == 'console' and ctx['scope'] == 'campaign':
        where.append("(entity_type='CAMPAIGN' OR (entity_type IN ('NEGATIVE_KEYWORD','NEGATIVE_PRODUCT_TARGETING') "
                     "AND (ad_group_id IS NULL OR ad_group_id='' OR ad_group_id='0')))")
    category = params.get('category')
    if category:
        if category not in CATEGORIES:
            raise HistoryError('历史类别无效')
        kinds = CATEGORIES[category]
        where.append('entity_type IN ('+','.join(['%s']*len(kinds))+')'); args.extend(kinds)
    for key, values in [('source', ('ERP', 'PLATFORM')), ('sync_status', ('pending', 'synced', 'failed'))]:
        value = params.get(key)
        if value:
            if value not in values:
                raise HistoryError('历史筛选值无效')
            where.append(key+'=%s'); args.append(value)
    change = params.get('change_type')
    if change:
        if change not in FILTERS:
            raise HistoryError('更改类型无效')
        where.append('change_type=%s'); args.append(change)
    with repo.transaction() as c:
        c.execute('SELECT COUNT(*) AS n FROM amazon_ads_history WHERE '+' AND '.join(where), args)
        total = c.fetchone()['n']
        c.execute('SELECT * FROM amazon_ads_history WHERE '+' AND '.join(where)+
                  ' ORDER BY event_time_ms DESC,id DESC LIMIT %s OFFSET %s', args+[size, (page-1)*size])
        rows = c.fetchall()
        c.execute('SELECT * FROM amazon_ads_history_sync WHERE region=%s AND profile_id=%s AND '
                  "((scope=%s AND scope_id=%s) OR scope='shop')", (ctx['region'], ctx['profile_id'], ctx['scope'], repo.scope_key(ctx)))
        states = c.fetchall()
        from services.ads_history_jobs import projection
        collection = projection(c, ctx, repo.now_ms())
    output = []
    for row in rows:
        output.append({
            'id': str(row['id']), 'entityType': row['entity_type'], 'entityId': row['entity_id'],
            'campaignId': row['campaign_id'], 'adGroupId': row['ad_group_id'],
            'entityName': row['entity_name'], 'changeType': row['change_type'],
            'previousValue': repo.loads(row['previous_value']), 'newValue': repo.loads(row['new_value']),
            'timestamp': row['event_time_ms'], 'metadata': repo.loads(row['metadata']) or {},
            'source': row['source'], 'sync_status': row['sync_status'], 'write_status': row['write_status'],
            'operator_name': row['operator_name'], 'local_started_ms': row['local_started_ms'],
            'platform_time_ms': row['platform_time_ms'], 'match_method': row['match_method'],
            'error_message': row['error_message'], 'local_before': repo.loads(row['local_before']),
            'local_after': repo.loads(row['local_after']), 'audit_context': repo.loads(row.get('audit_context_json'))})
        output[-1]['display'] = ads_display.history_display(output[-1], tz_name)
    success = max([s['last_success_ms'] or 0 for s in states] or [0])
    state = max(states, key=lambda s: max(s['last_attempt_ms'] or 0, s['dirty_since_ms'] or 0), default=None)
    status = state['status'] if state else 'never'
    if state and state['dirty_since_ms'] and state['dirty_since_ms'] > success and status == 'success':
        status = 'pending'
    return {'list': output, 'total': total, 'page': page, 'page_size': size, 'scope': ctx['scope'], 'view': view,
            'last_success_ms': success or None, 'sync_state': status,
            'sync_error': state['error_message'] if state else '', 'source': 'local', 'timezone': tz_name,
            'collection': collection}


def event_types(ctx):
    result = {}
    for kind in KINDS:
        if ctx['scope'] == 'group' and kind == 'CAMPAIGN':
            continue
        if ctx['scope'] == 'shop':
            spec = {'parents': [{'useProfileIdAdvertiser': True}]}
        elif (ctx['scope'], kind) in (('campaign', 'CAMPAIGN'), ('group', 'AD_GROUP')):
            spec = {'eventTypeIds': [repo.scope_key(ctx)], 'parents': [{'useProfileIdAdvertiser': True}]}
        else:
            parent = {'campaignId': str(ctx['campaign_id'])}
            if ctx['ad_group_id']:
                parent['adGroupId'] = str(ctx['ad_group_id'])
            spec = {'parents': [parent]}
        result[kind] = spec
    return result


def normalize_event(c, ctx, raw, lo, hi):
    if not isinstance(raw, dict) or raw.get('entityType') not in KINDS or not isinstance(raw.get('changeType'), str):
        raise HistoryError('平台事件类型或结构异常，未推进采集进度', 502)
    timestamp = raw.get('timestamp')
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or not lo <= timestamp <= hi:
        raise HistoryError('平台历史时间超出请求范围', 502)
    kind = raw['entityType']
    eid = str(identities.positive_id(raw.get('entityId'), '平台实体ID'))
    metadata = raw.get('metadata') or {}
    if not isinstance(metadata, dict):
        raise HistoryError('平台历史上下文无效', 502)
    known = repo.entity(c, ctx, kind, eid)
    if not known and kind == 'PRODUCT_TARGETING':
        known = repo.entity(c, ctx, 'NEGATIVE_PRODUCT_TARGETING', eid)
        if known:
            kind = 'NEGATIVE_PRODUCT_TARGETING'
    campaign = str(known['campaign_id']) if known else (str(metadata['campaignId']) if metadata.get('campaignId') else None)
    # 活动级否定词的历史响应可能带平台内部广告组ID；实体表已确认NULL时不能回退该元数据。
    group = (str(known['ad_group_id']) if known.get('ad_group_id') else None) if known else (
        str(metadata['adGroupId']) if metadata.get('adGroupId') else None)
    if kind == 'CAMPAIGN': campaign = eid
    if kind == 'AD_GROUP': group = eid
    if known:
        for field, expected in [('campaignId', campaign), ('adGroupId', group)]:
            if metadata.get(field) and expected and str(metadata[field]) != expected:
                raise HistoryError('平台事件与实体归属冲突，未覆盖历史', 502)
    if ctx['scope'] != 'shop':
        if campaign and campaign != str(ctx['campaign_id']):
            raise HistoryError('平台返回其他活动历史，未混入当前范围', 502)
        campaign = str(ctx['campaign_id'])
        if ctx['scope'] == 'group':
            if kind == 'CAMPAIGN' or (known and kind in ('NEGATIVE_KEYWORD', 'NEGATIVE_PRODUCT_TARGETING') and not group) or (group and group != str(ctx['ad_group_id'])):
                raise HistoryError('平台返回其他组历史，未混入当前范围', 502)
            group = str(ctx['ad_group_id'])
    name = (known or {}).get('entity_name') or metadata.get('keyword') or metadata.get('targetingExpression') or metadata.get('predefinedTarget') or metadata.get('theme') or eid
    fingerprint = repo.digest({k: raw.get(k) for k in
                               ('entityType', 'entityId', 'changeType', 'previousValue', 'newValue', 'timestamp', 'metadata')})
    return {'entity_type': kind, 'entity_id': eid, 'campaign_id': campaign, 'ad_group_id': group,
            'entity_name': str(name)[:600], 'change_type': raw['changeType'], 'timestamp': timestamp,
            'previous_value': None if raw.get('previousValue') == 'null' else raw.get('previousValue'),
            'new_value': None if raw.get('newValue') == 'null' else raw.get('newValue'),
            'metadata': metadata, 'raw': raw, 'fingerprint': fingerprint}


def _root(ctx):
    return {**ctx, 'scope': 'shop', 'campaign_id': None, 'ad_group_id': None}


def reconcile_saved(c, ctx):
    """只用本地已有证据补关联，冷却/过期窗口不应阻止已到达的历史补全。"""
    where = ''
    args = [ctx['region'], ctx['profile_id']]
    if ctx['scope'] != 'shop':
        where += ' AND p.campaign_id=%s'
        args.append(str(ctx['campaign_id']))
    if ctx['scope'] == 'group':
        where += ' AND p.ad_group_id=%s'
        args.append(str(ctx['ad_group_id']))
    # 只重读有未关联ERP候选的事件，不扫描/改写所有平台历史。
    c.execute('''SELECT p.platform_event FROM amazon_ads_history p
                 WHERE p.region=%s AND p.profile_id=%s AND p.source='PLATFORM'
                 AND p.merged_into_id IS NULL AND p.platform_event IS NOT NULL
                 AND EXISTS (SELECT 1 FROM amazon_ads_history e
                   WHERE e.region=p.region AND e.profile_id=p.profile_id AND e.entity_type=p.entity_type
                   AND e.entity_id=p.entity_id AND e.change_type=p.change_type AND e.source='ERP'
                   AND e.write_status='accepted' AND e.platform_time_ms IS NULL AND e.merged_into_id IS NULL
                   AND e.local_started_ms BETWEEN p.platform_time_ms-300000 AND p.platform_time_ms+300000)
                 '''+where+' ORDER BY p.id', args)
    events = c.fetchall()
    merged = 0
    for row in events:
        # 与采集相同的身份、父级及双向唯一校验；不绕过原匹配规则。
        raw = repo.loads(row['platform_event'])
        event = normalize_event(c, _root(ctx), raw, 0, repo.now_ms()+1000)
        merged += repo.save_event(c, ctx, event, allow_merge=True)
    return merged


def sync_scope(params, budget=25):
    """兼容原接口，只登记近期补采；网络消费由独立cron统一限速处理。"""
    from services.ads_history_jobs import request_scope
    return request_scope(context(params, allow_shop=True))


def run_due():
    """委托增量队列消费者，不再每小时7天或每日90天全店回扫。"""
    from services.ads_history_worker import run_due as consume
    return consume()
