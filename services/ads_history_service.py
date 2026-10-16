"""统一历史：页面仅读本地，平台采集按范围、可续传且不覆盖ERP原始证据。"""
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import requests

from services import ads_history_repository as repo
from services import ads_negative_service as identities
from services.shop_service import get_ads_api_client, get_all_active_shops

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
            'local_after': repo.loads(row['local_after'])})
    success = max([s['last_success_ms'] or 0 for s in states] or [0])
    state = max(states, key=lambda s: max(s['last_attempt_ms'] or 0, s['dirty_since_ms'] or 0), default=None)
    status = state['status'] if state else 'never'
    if state and state['dirty_since_ms'] and state['dirty_since_ms'] > success and status == 'success':
        status = 'pending'
    return {'list': output, 'total': total, 'page': page, 'page_size': size, 'scope': ctx['scope'], 'view': view,
            'last_success_ms': success or None, 'sync_state': status,
            'sync_error': state['error_message'] if state else '', 'source': 'local', 'timezone': tz_name}


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


def event_batches(ctx):
    """分开活动设置/否定词与其余实体，避免大扇出查询失败使所有类型都无法保存。"""
    kinds = list(event_types(ctx))
    if ctx['scope'] == 'group':
        return [kinds]
    return [['CAMPAIGN', 'NEGATIVE_KEYWORD'], [kind for kind in kinds if kind not in ('CAMPAIGN', 'NEGATIVE_KEYWORD')]]


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


def sync_scope(params, budget=25):
    ctx = context(params, allow_shop=True)
    clock = repo.now_ms()
    lock_conn = repo.get_db_connection()
    lock_name = 'ads_hist_' + repo.digest([ctx['region'], ctx['profile_id']])[:40]
    acquired = False
    try:
        lock_conn.autocommit(True)
        with lock_conn.cursor() as lock:
            lock.execute('SELECT GET_LOCK(%s,0) AS acquired', (lock_name,))
            acquired = lock.fetchone()['acquired'] == 1
        if not acquired:
            with repo.transaction() as c: repo.mark_dirty(c, ctx)
            return {'status': 'pending', 'message': '历史正在同步，已合并补采需求', 'saved': 0}
        with repo.transaction() as c:
            account_state = repo.ensure_state(c, _root(ctx))
            state = repo.ensure_state(c, ctx)
            if account_state['next_retry_ms'] > clock:
                repo.mark_dirty(c, ctx)
                return {'status': 'pending', 'message': '历史采集冷却中，已保留补采需求', 'saved': 0,
                        'next_retry_ms': account_state['next_retry_ms']}
            work = repo.loads(state['work_json'])
            if not work:
                deep = not state['deep_success_ms'] or clock-state['deep_success_ms'] >= 86400000
                # 与页面默认90个北京时间自然日一致，不能漏掉首日凌晨。
                today_ms = ((clock+28800000)//86400000)*86400000-28800000
                lo = today_ms-89*86400000 if deep else clock-7*86400000
                work = {'deep': deep, 'version': state['dirty_version'], 'started': clock,
                        'tasks': [{'lo': lo, 'hi': clock, 'page': 0, 'types': kinds} for kinds in event_batches(ctx)]}
            elif any('types' not in task for task in work['tasks']):
                # 兼容旧检查点：按相同时间窗重新分页，原事件指纹去重，不漏掉已跨页的类型。
                work['tasks'] = [part for task in work['tasks'] for part in (
                    [task] if 'types' in task else [
                        {**task, 'page': 0, 'types': kinds} for kinds in event_batches(ctx)])]
            c.execute("UPDATE amazon_ads_history_sync SET status='running',last_attempt_ms=%s,work_json=%s WHERE id=%s",
                      (clock, repo.dumps(work), state['id']))
        client = get_ads_api_client(ctx['shop_id'])
        if str(client.profile_id) != ctx['profile_id'] or client.region != ctx['region']:
            raise HistoryError('广告账户在采集中发生变化，已停止', 409)
        deadline = time.monotonic()+budget
        saved = calls = 0
        while work['tasks'] and time.monotonic() < deadline and calls < 4:
            task = work['tasks'][0]
            if task['lo'] < repo.now_ms()-90*86400000:
                raise HistoryError('未完成窗口已超过平台90天保留期，请核对后重新回填', 409)
            kinds = event_types(ctx)
            result = client.get_change_history({kind: kinds[kind] for kind in task['types']}, task['lo'], task['hi'], 200, task['page'])
            calls += 1
            total = result.get('totalRecords', result.get('totalResults')) if isinstance(result, dict) else None
            rows = result.get('events') if isinstance(result, dict) else None
            if isinstance(total, bool) or not isinstance(total, int) or total < 0 or not isinstance(rows, list) or len(rows)>200:
                raise HistoryError('平台历史分页响应异常，未推进检查点', 502)
            if total >= 10000:
                if task['lo'] >= task['hi']:
                    raise HistoryError('同毫秒事件超过分页上限，未截断；需平台支持核查', 502)
                middle = (task['lo']+task['hi'])//2
                work['tasks'][:1] = [{'lo': task['lo'], 'hi': middle, 'page': 0, 'types': task['types']},
                                     {'lo': middle+1, 'hi': task['hi'], 'page': 0, 'types': task['types']}]
            else:
                if not rows and task['page']*200 < total:
                    raise HistoryError('平台分页提前为空，未标记完整', 502)
                complete = (task['page']+1)*200 >= total
                with repo.transaction() as c:
                    normalized = [normalize_event(c, ctx, r, task['lo'], task['hi']) for r in rows]
                    fingerprints = [r['fingerprint'] for r in normalized]
                    if len(set(fingerprints)) != len(fingerprints):
                        raise HistoryError('平台页内重复事件，未推进检查点', 502)
                    for event in normalized: saved += repo.save_event(c, ctx, event)
                    if complete:
                        c.execute('''SELECT platform_event FROM amazon_ads_history WHERE region=%s AND profile_id=%s
                                     AND source='PLATFORM' AND merged_into_id IS NULL AND platform_time_ms BETWEEN %s AND %s''',
                                  (ctx['region'], ctx['profile_id'], task['lo'], task['hi']))
                        for item in c.fetchall():
                            event = normalize_event(c, _root(ctx), repo.loads(item['platform_event']), task['lo'], task['hi'])
                            repo.save_event(c, ctx, event, allow_merge=True)
                if complete: work['tasks'].pop(0)
                else: task['page'] += 1
            with repo.transaction() as c:
                c.execute('UPDATE amazon_ads_history_sync SET work_json=%s WHERE id=%s', (repo.dumps(work), state['id']))
        with repo.transaction() as c:
            finished = not work['tasks']
            c.execute('''UPDATE amazon_ads_history_sync SET status=%s,work_json=%s,error_message='',
                         last_success_ms=IF(%s,%s,last_success_ms),deep_success_ms=IF(%s,%s,deep_success_ms),
                         done_version=IF(%s,%s,done_version),dirty_since_ms=IF(%s AND dirty_version=%s,NULL,dirty_since_ms),
                         failures=0 WHERE id=%s''',
                      ('success' if finished else 'partial', None if finished else repo.dumps(work), finished, work['started'],
                       finished and work['deep'], clock, finished, work['version'], finished, work['version'], state['id']))
            c.execute('UPDATE amazon_ads_history_sync SET next_retry_ms=%s WHERE id=%s', (repo.now_ms()+180000, account_state['id']))
            if finished:
                where = ' AND campaign_id=%s' if ctx['scope'] != 'shop' else ''
                args = [ctx['region'], ctx['profile_id']]
                if where: args.append(str(ctx['campaign_id']))
                if ctx['scope'] == 'group': where += ' AND ad_group_id=%s'; args.append(str(ctx['ad_group_id']))
                c.execute("UPDATE amazon_ads_history SET sync_status='pending',error_message='' "
                          "WHERE region=%s AND profile_id=%s AND source='ERP' AND write_status='accepted' "
                          "AND platform_time_ms IS NULL"+where, args)
        return {'status': 'success' if finished else 'partial', 'saved': saved,
                'message': '历史已更新' if finished else '历史已部分保存，余下窗口待自动续采'}
    except Exception as exc:
        status = exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else None
        message = ('Amazon历史限流，保留原记录并稍后补采' if status == 429 else
                   'Amazon广告授权失效或缺少历史读取权限，请核对店铺授权' if status in (401, 403) else
                   str(exc) if isinstance(exc, HistoryError) else
                   '连接Amazon超时或网络异常，原记录及检查点保留' if isinstance(exc, requests.RequestException) else
                   '历史采集失败，原记录及检查点保留')
        with repo.transaction() as c:
            root = repo.ensure_state(c, _root(ctx))
            state = repo.ensure_state(c, ctx)
            delay = min(3600000, 300000*2**min(root['failures'], 3))
            if status == 429:
                retry = exc.response.headers.get('Retry-After', '')
                try:
                    retry_ms = int(retry)*1000 if str(retry).isdigit() else int(parsedate_to_datetime(retry).timestamp()*1000)-repo.now_ms()
                    delay = max(delay, min(86400000, retry_ms))
                except (ValueError, TypeError, OverflowError):
                    pass
            c.execute('UPDATE amazon_ads_history_sync SET next_retry_ms=%s,failures=failures+1 WHERE id=%s',
                      (repo.now_ms()+delay, root['id']))
            c.execute("UPDATE amazon_ads_history_sync SET status='failed',error_message=%s,last_attempt_ms=%s WHERE id=%s", (message, clock, state['id']))
            where = ' AND campaign_id=%s' if ctx['scope'] != 'shop' else ''
            values = [ctx['region'], ctx['profile_id']]
            if where: values.append(str(ctx['campaign_id']))
            if ctx['scope'] == 'group': where += ' AND ad_group_id=%s'; values.append(str(ctx['ad_group_id']))
            c.execute("UPDATE amazon_ads_history SET sync_status='failed',error_message=%s WHERE region=%s AND profile_id=%s "
                      "AND source='ERP' AND platform_time_ms IS NULL AND write_status='accepted'"+where, [message]+values)
        return {'status': 'failed', 'message': message, 'saved': 0}
    finally:
        if acquired:
            try:
                with lock_conn.cursor() as c: c.execute('SELECT RELEASE_LOCK(%s)', (lock_name,))
            except Exception: pass
        lock_conn.close()


def run_due():
    results = []
    for shop in get_all_active_shops():
        if not shop.get('ads_profile_id'): continue
        try:
            acct = repo.account(shop['id'])
            clock = repo.now_ms()
            with repo.transaction() as c:
                c.execute("UPDATE amazon_ads_history SET write_status='unknown',error_message=%s,updated_at=UTC_TIMESTAMP() "
                          "WHERE region=%s AND profile_id=%s AND source='ERP' AND write_status='requesting' AND local_started_ms<%s",
                          ('请求超过15分钟未记录最终响应，请核实平台状态，不会自动重发', acct['region'], acct['profile_id'], clock-900000))
                root = repo.ensure_state(c, _root(acct))
                c.execute('''SELECT * FROM amazon_ads_history_sync WHERE region=%s AND profile_id=%s AND scope<>'shop'
                             AND (work_json IS NOT NULL OR dirty_version>done_version)
                             AND COALESCE(dirty_since_ms,0)<=%s ORDER BY COALESCE(dirty_since_ms,0),id LIMIT 1''',
                          (acct['region'], acct['profile_id'], clock-120000))
                dirty = c.fetchone()
            selected = root if root['work_json'] or not root['last_success_ms'] or clock-root['last_success_ms']>=3600000 else dirty
            if selected:
                results.append(sync_scope({'shop_id': shop['id'], 'scope': selected['scope'],
                                           'campaign_id': selected['campaign_id'], 'ad_group_id': selected['ad_group_id']}, budget=90))
        except Exception:
            results.append({'status': 'failed', 'message': '店铺历史任务未完成，请核对授权和表结构'})
    return results
