"""显式启用的ERP写请求留痕；不主动调价，不在操作后请求历史接口。"""
import uuid
from functools import wraps

from flask import g, has_request_context, request, jsonify, make_response

from services import ads_history_repository as repo
from services import operation_identity


RESOURCES = {
    'campaigns': ('CAMPAIGN', 'campaigns', 'campaignId'),
    'adGroups': ('AD_GROUP', 'adGroups', 'adGroupId'),
    'productAds': ('AD', 'productAds', 'adId'),
    'keywords': ('KEYWORD', 'keywords', 'keywordId'),
    'targets': ('PRODUCT_TARGETING', 'targetingClauses', 'targetId'),
    'negativeKeywords': ('NEGATIVE_KEYWORD', 'negativeKeywords', 'keywordId'),
    'campaignNegativeKeywords': ('NEGATIVE_KEYWORD', 'campaignNegativeKeywords', 'keywordId'),
    'negativeTargets': ('NEGATIVE_PRODUCT_TARGETING', 'negativeTargetingClauses', 'targetId'),
    'campaignNegativeTargets': ('NEGATIVE_PRODUCT_TARGETING', 'campaignNegativeTargetingClauses', 'targetId'),
}
FIELDS = {'state': ('STATUS', 'state'), 'name': ('NAME', 'name'), 'bid': ('BID_AMOUNT', 'bid'),
          'defaultBid': ('DEFAULT_BID_AMOUNT', 'default_bid'), 'startDate': ('START_DATE', 'start_date'),
          'endDate': ('END_DATE', 'end_date'), 'portfolioId': ('PORTFOLIO', 'portfolio_id')}


def track_write(function):
    from blueprints.user_auth import login_required
    @wraps(function)
    def tracked(*args, **kwargs):
        provided = request.headers.get('X-Request-ID')
        try:
            request_key = str(uuid.UUID(provided)) if provided else str(uuid.uuid4())
        except (ValueError, TypeError):
            return jsonify({'status': 'error', 'message': '请求标识无效'}), 400
        g.ads_history_active = True
        g.user_id = request.current_user['id']
        g.ads_history_request_key = request_key
        g.ads_history_calls = 0
        g.ads_history_accepted = 0
        g.ads_history_new = {}
        try:
            response = make_response(function(*args, **kwargs))
            body = response.get_json(silent=True)
            warning = g.get('ads_history_warning')
            if isinstance(body, dict):
                if response.status_code >= 400 and g.ads_history_accepted:
                    body['message'] = '部分操作已被平台接受，请先同步核实，不要重复提交。' + str(body.get('message', ''))[:300]
                if warning: body['history_warning'] = warning
                response.set_data(jsonify(body).get_data())
            return response
        except repo.HistoryError as exc:
            return jsonify({'status': 'error', 'message': str(exc)}), exc.code
        finally:
            g.ads_history_active = False
    return login_required(tracked)


def _owner(c, acct, kind, eid, item, creating):
    """核对实体属于指定广告账户及父级；AI与人工使用相同隔离检查。"""
    state = operation_identity.ads_state()
    known = repo.entity(c, acct, kind, eid) if eid else None
    cached = state.ads_history_new.get((kind, str(eid))) if eid else None
    known = known or cached
    if not creating and not known:
        raise repo.HistoryError('实体不属于当前广告账户或尚未同步，未发送操作', 404)
    campaign = str((known or {}).get('campaign_id') or item.get('campaignId') or '') or None
    group = str((known or {}).get('ad_group_id') or item.get('adGroupId') or '') or None
    if kind == 'CAMPAIGN':
        campaign = str(eid) if eid else None
    for field, value in [('campaignId', campaign), ('adGroupId', group)]:
        if value and item.get(field) and str(item[field]) != value:
            raise repo.HistoryError('写入实体父级不一致，未发送操作', 409)
    if creating and kind != 'CAMPAIGN':
        parent = repo.entity(c, acct, 'CAMPAIGN', campaign) or state.ads_history_new.get(('CAMPAIGN', campaign))
        if not parent:
            raise repo.HistoryError('新建对象的活动不属于当前广告账户', 404)
        if group:
            pg = repo.entity(c, acct, 'AD_GROUP', group) or state.ads_history_new.get(('AD_GROUP', group))
            if not pg or str(pg['campaign_id']) != campaign:
                raise repo.HistoryError('新建对象的广告组不属于当前活动', 404)
    return known or {}, campaign, group


def _changes(item, known, creating):
    if creating:
        return [('CREATED', None, 'CREATED', {})]
    result = []
    for field, (change, column) in FIELDS.items():
        if field not in item: continue
        before = known.get(column)
        if hasattr(before, 'isoformat'): before = before.isoformat()
        after = item[field]
        if change == 'STATUS':
            before = str(before).upper() if before is not None else None
            after = str(after).upper()
        if repo.comparable(before, change) != repo.comparable(after, change):
            result.append((change, before, after, {}))
    budget = item.get('budget')
    if isinstance(budget, dict) and 'budget' in budget:
        result.append(('BUDGET_AMOUNT', known.get('daily_budget'), budget['budget'], {}))
    if 'dailyBudget' in item:
        result.append(('BUDGET_AMOUNT', known.get('daily_budget'), item['dailyBudget'], {}))
    dynamic = item.get('dynamicBidding') or item.get('bidding')
    if isinstance(dynamic, dict):
        old = repo.loads(known.get('bidding')) or {}
        strategies = {'LEGACY_FOR_SALES': 'LEGACY', 'AUTO_FOR_SALES': 'OPTIMIZE_FOR_SALES'}
        if 'strategy' in dynamic:
            before = strategies.get(old.get('strategy'), old.get('strategy'))
            after = strategies.get(dynamic['strategy'], dynamic['strategy'])
            if before != after: result.append(('SMART_BIDDING_STRATEGY', before, after, {}))
        old_places = {repo.placement_position(x.get('placement')): x.get('percentage') for x in old.get('placementBidding', [])}
        for placement in dynamic.get('placementBidding', []):
            position = repo.placement_position(placement.get('placement'))
            before, after = old_places.get(position), placement.get('percentage')
            if before != after: result.append(('PLACEMENT_GROUP', before, after, {'placementGroupPosition': position}))
    return result


def begin(acct, resource, method, path, payload):
    """提交前逐项写原广告历史，保留实际执行人以及可选AI任务/人工发起上下文。"""
    state = operation_identity.ads_state()
    actor = operation_identity.user()
    proof = operation_identity.metadata()
    kind, key, id_key = RESOURCES[resource]
    deleting = path.endswith('/delete')
    creating = method == 'POST' and not deleting
    if deleting:
        container = next(iter(payload.values()), {})
        items = [{id_key: eid, 'state': 'ARCHIVED'} for eid in container.get('include', [])]
    else:
        items = payload.get(key)
    if not isinstance(items, list) or not items or len(items)>1000:
        raise repo.HistoryError('无法审计无效的广告写入批次，未发送操作')
    state.ads_history_calls += 1
    call_index = state.ads_history_calls
    started = repo.now_ms()
    plans = []
    with repo.transaction() as c:
        # 即使是同值请求也先确认两张表可用，缺迁移时不能先发送再发现无法留痕。
        c.execute('SELECT id FROM amazon_ads_history LIMIT 0')
        repo.ensure_state(c, {**acct, 'scope': 'shop', 'campaign_id': None, 'ad_group_id': None})
        for index, item in enumerate(items):
            if not isinstance(item, dict): raise repo.HistoryError('广告写入项无效')
            eid = str(item[id_key]) if item.get(id_key) else None
            known, campaign, group = _owner(c, acct, kind, eid, item, creating)
            safe_payload = {k: v for k, v in item.items() if k in {
                'campaignId', 'adGroupId', 'keywordId', 'targetId', 'adId', 'name', 'state', 'bid', 'defaultBid',
                'startDate', 'endDate', 'budget', 'dailyBudget', 'dynamicBidding', 'bidding', 'portfolioId',
                'keywordText', 'matchType', 'expression', 'sku', 'asin', 'targetingType'}}
            name = known.get('entity_name') or item.get('keywordText') or item.get('name') or item.get('sku') or repo.dumps(item.get('expression') or '')
            ids = []
            for change, before, after, metadata in _changes(item, known, creating):
                row = {**acct, 'campaign_id': campaign, 'ad_group_id': group, 'entity_type': kind, 'entity_id': eid,
                       'entity_name': str(name)[:600], 'change_type': change, 'previous_value': before,
                       'new_value': after, 'metadata': metadata, 'source': 'ERP', 'sync_status': 'pending',
                       'write_status': 'requesting', 'event_time_ms': started, 'local_started_ms': started,
                       'request_key': state.ads_history_request_key, 'local_before': before, 'local_after': after,
                       'local_payload': safe_payload, 'operator_id': actor['id'], 'audit_context_json': proof,
                       'operator_name': str((actor.get('nickname') or actor.get('username')) if proof else actor.get('username') or '')[:100],
                       'local_key': repo.digest([actor['id'], state.ads_history_request_key, call_index, index, change, metadata])}
                ids.append(repo.insert_local(c, row))
            plans.append({'index': index, 'ids': ids, 'item': item, 'eid': eid, 'kind': kind, 'id_key': id_key,
                          'campaign': campaign, 'group': group, 'creating': creating})
    return plans


def results(response, key, plans):
    """返回逐项状态，兼容v3容器和旧列表；未知/缺项不能算成功。"""
    block = response.get(key) if isinstance(response, dict) else None
    output = {}
    if isinstance(block, list):
        for index, entry in enumerate(block):
            if index < len(plans) and isinstance(entry, dict):
                code = str(entry.get('code', '')).upper()
                output[index] = ('accepted' if code in ('SUCCESS', '200', '201') else 'failed' if code else 'unknown', entry)
    elif isinstance(block, dict):
        for section, status in [('success', 'accepted'), ('error', 'failed')]:
            for entry in block.get(section, []):
                index = entry.get('index') if isinstance(entry, dict) else None
                if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(plans) or index in output:
                    raise repo.HistoryError('平台写响应索引异常，操作可能已生效，请先同步，勿重复提交', 502)
                output[index] = (status, entry)
    return output


def _response_id(entry, id_key):
    aliases = {'keywordId': ('campaignNegativeKeywordId', 'negativeKeywordId'),
               'targetId': ('campaignNegativeTargetingClauseId', 'negativeTargetingClauseId')}
    for key in (id_key,)+aliases.get(id_key, ()):
        if entry.get(key): return str(entry[key])
    for value in entry.values():
        if isinstance(value, dict):
            found = _response_id(value, id_key)
            if found: return found
    return None


def finish(acct, resource, plans, response=None, error=None):
    """保存逐项平台结果并关联既有事件，不修改实际执行人或伪造平台成功。"""
    state = operation_identity.ads_state()
    key = RESOURCES[resource][1]
    outcome = results(response, key, plans) if response is not None else {}
    accepted = 0
    accepted_entities = []
    with repo.transaction() as c:
        for plan in plans:
            status, entry = outcome.get(plan['index'], ('unknown', {}))
            if error is not None:
                http_status = error.response.status_code if hasattr(error, 'response') and error.response is not None else None
                status = 'failed' if http_status in (400, 401, 403, 404, 422, 429) else 'unknown'
            returned = _response_id(entry, plan['id_key'])
            if status == 'accepted' and (not returned or (plan['eid'] and returned != plan['eid'])):
                status = 'unknown'
            for parent_key, expected in [('campaignId', plan['campaign']), ('adGroupId', plan['group'])]:
                returned_parent = _response_id(entry, parent_key)
                if expected and returned_parent and returned_parent != str(expected):
                    status = 'unknown'
            eid = returned if status == 'accepted' else plan['eid']
            campaign = eid if plan['kind']=='CAMPAIGN' else plan['campaign']
            group = eid if plan['kind']=='AD_GROUP' else plan['group']
            message = '' if status == 'accepted' else ('广告写入明确失败；历史未确认' if status=='failed' else '广告写入结果待确认，请先同步核实，勿重复提交')
            for row_id in plan['ids']:
                c.execute('''UPDATE amazon_ads_history SET write_status=%s,entity_id=%s,campaign_id=%s,ad_group_id=%s,
                             local_finished_ms=%s,sync_status=%s,error_message=%s,updated_at=UTC_TIMESTAMP() WHERE id=%s''',
                          (status, eid, campaign, group, repo.now_ms(), 'failed' if status=='failed' else 'pending', message, row_id))
            if status == 'accepted':
                accepted += 1
                accepted_entities.append((plan['kind'], eid))
                if plan['creating']:
                    state.ads_history_new[(plan['kind'], eid)] = {**plan['item'], 'campaign_id': campaign,
                                                            'ad_group_id': group, 'entity_name': plan['item'].get('name', '')}
            if campaign and status != 'failed':
                repo.mark_dirty(c, {**acct, 'scope': 'group' if group else 'campaign', 'campaign_id': campaign, 'ad_group_id': group})
                # 队列只引用刚保存的真实操作与窄时间窗，不在写入请求中调用历史API。
                from services.ads_history_jobs import enqueue_events
                enqueue_events(c, acct, plan['ids'])
        # 平台事件先被另一采集进程保存时，只读本地证据补关联，不新增API调用。
        from services.ads_history_service import normalize_event
        for kind, eid in accepted_entities:
            for raw in repo.reconcile(c, acct, kind, eid):
                event = normalize_event(c, {**acct, 'scope': 'shop', 'campaign_id': None, 'ad_group_id': None}, raw, 0, repo.now_ms()+1000)
                repo.save_event(c, acct, event, allow_merge=True)
        state.ads_history_accepted += accepted
    return accepted == len(plans)


def instrument(client, shop):
    """为认证人工或显式后台AI真实写入挂原广告审计，模拟执行不会调用本包装。"""
    if not operation_identity.explicit() and (not has_request_context() or not g.get('ads_history_active')):
        return client
    if getattr(client, '_ads_history_instrumented', False) is True:
        return client
    acct = {k: shop.get(k) for k in ('credential_group_id', 'region', 'marketplace_id')}
    acct.update(shop_id=shop['id'], profile_id=str(shop['ads_profile_id']), region=shop['region'].lower())
    original = client._request
    def audited(method, path, *args, **kwargs):
        """在原广告调用前留意图、后记回执；上下文不能随任务结束泄漏给人工。"""
        state = operation_identity.ads_state()
        parts = path.strip('/').split('/')
        resource = parts[1] if len(parts)>=2 and parts[0]=='sp' else None
        writing = resource in RESOURCES and (len(parts)==2 and method in ('PUT', 'POST') or len(parts)==3 and parts[2]=='delete' and method=='POST')
        if not writing: return original(method, path, *args, **kwargs)
        if str(client.profile_id) != acct['profile_id'] or client.region != acct['region']:
            raise repo.HistoryError('广告账户或区域不一致，未发送操作', 409)
        payload = kwargs.get('json_data') or {}
        try:
            plans = begin(acct, resource, method, path, payload)
        except repo.HistoryError: raise
        except Exception:
            raise repo.HistoryError('操作前历史记录保存失败，未向平台发送，请检查数据库迁移或请求是否重复', 503) from None
        try:
            result = original(method, path, *args, **kwargs)
        except Exception as exc:
            try: finish(acct, resource, plans, error=exc)
            except Exception: state.ads_history_warning = '写入结果及本地历史待核实，请勿重复提交'
            raise
        try:
            all_success = finish(acct, resource, plans, response=result)
        except repo.HistoryError:
            # 响应索引/结构不可信时不能放行旧路由更新缓存并显示成功。
            state.ads_history_warning = '平台返回结果无法逐项核实，请先同步，不要重复提交'
            raise
        except Exception:
            # 平台已经返回；历史补写失败不能把广告操作伪装成未发送。
            state.ads_history_warning = '平台已返回结果，但历史保存待核实；请先同步，不要重复操作'
            return result
        if not all_success and not state.get('ads_creation_partial_results', False) and 'Negative' not in resource and not resource.startswith('negative'):
            raise repo.HistoryError('平台部分操作失败或结果待确认，请先同步核实，不要整批重试', 502)
        return result
    client._request = audited
    client._ads_history_instrumented = True
    return client
