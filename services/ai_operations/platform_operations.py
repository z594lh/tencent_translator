"""AI托管正式Amazon通道：明确账户、实时前值、定向更新、只读回查；无模拟分支。"""
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import requests
from services import listing_pricing_service as pricing, listing_text_service as text
from services import listing_history_service, operation_identity
from services.listing_content_policy import ContentError
from services.shop_service import get_shop_by_id, get_ads_api_client
from services.listing_performance.common import DomainError
from . import plan_store
from .metrics import digest, dumps

MODE = 'real'
VERSION = 'amazon-live-v1'
PLACE_ALIAS = {'PLACEMENT_TOP_OF_SEARCH': 'PLACEMENT_TOP', 'PLACEMENT_PRODUCT_DETAIL': 'PLACEMENT_PRODUCT_PAGE'}


def supports(action):
    """仅开放已经具备定向提交和真实回读的五类行动。"""
    return (action.get('mode') == 'change' and (
        action['kind'] == 'listing_text_update' and action['subject'].get('field') in text.FIELDS
        or action['kind'] == 'listing_price_update' and action['subject'].get('audience') in ('ALL', 'B2B')
        or action['kind'] == 'ads_budget_update'
        or action['kind'] == 'ads_placement_update' and action['subject'].get('placement') in
        ('PLACEMENT_TOP', 'PLACEMENT_REST_OF_SEARCH', 'PLACEMENT_PRODUCT_PAGE')
        or action['kind'] == 'ads_bid_update' and action['subject'].get('entity_type') in ('group', 'keyword', 'target')))


def resource_key(identity, ref):
    """将账户边界和行动字段共同纳入持久幂等资源键。"""
    return digest([MODE, *[str(identity.get(k) or '') for k in
        ('id', 'credential_group_id', 'region', 'marketplace_id', 'ads_profile_id')], ref])


def lock_key(identity, action):
    """共享活动/同SKU整块互斥，避免不同ASIN或广告位覆盖同一设置。"""
    subject = action['subject']
    resource = ('listing', subject['sku']) if action['kind'].startswith('listing_') else ('campaign', subject['campaign_id'])
    return resource_key(identity, dumps(resource))


def equal(action, actual, expected):
    """文字按完整列表比较，金额和百分比按十进制比较，不采用浮点容差。"""
    if action['kind'] == 'listing_text_update': return actual == expected
    try:
        a, b = Decimal(str(actual)), Decimal(str(expected))
        return a.is_finite() and b.is_finite() and a == b
    except (InvalidOperation, ValueError, TypeError): return False


def account(identity):
    """每次调用前重新核对有效店铺、凭证组、区域、站点和广告Profile。"""
    shop = get_shop_by_id(int(identity['id']))
    fields = ('credential_group_id', 'region', 'marketplace_id', 'ads_profile_id')
    if not shop or any(str(shop.get(k) or '') != str(identity.get(k) or '') for k in fields):
        raise DomainError('店铺账户配置已变化，未向其他账户发出操作', 409)
    return shop


def unique(method, key, id_field, entity_id, **filters):
    """完整分页寻找唯一对象；重复游标、响应缺项和超限均拒绝猜测对象。"""
    found, seen, token = [], set(), None
    for _ in range(100):
        response = method(max_results=100, next_token=token, **filters)
        rows = response.get(key) if isinstance(response, dict) else None
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise DomainError('Amazon广告列表结构异常，未确认目标对象', 502)
        found.extend(row for row in rows if str(row.get(id_field)) == str(entity_id))
        token = response.get('nextToken')
        if not token: break
        if token in seen: raise DomainError('Amazon广告分页游标重复，未确认目标对象', 502)
        seen.add(token)
    else: raise DomainError('广告对象分页超过100页，未截断后操作', 422)
    if len(found) != 1: raise DomainError('平台没有返回唯一目标广告对象，请先同步核对', 409)
    if str(found[0].get('state', '')).upper() == 'ARCHIVED':
        raise DomainError('目标广告已归档，不修改', 409)
    return found[0]


def ads_read(identity, action):
    """实时验证活动和组父级，再读取指定广告字段及整个广告位配置。"""
    account(identity)
    client = get_ads_api_client(int(identity['id']))
    if str(client.profile_id) != str(identity['ads_profile_id']) or client.region != identity['region']:
        raise DomainError('Ads客户端账户不一致', 409)
    subject, kind = action['subject'], action['kind']
    cid = subject['campaign_id']
    campaign = unique(client.list_sp_campaigns, 'campaigns', 'campaignId', cid, campaign_id=cid)
    row, key, field = campaign, 'campaigns', 'dailyBudget'
    if kind == 'ads_budget_update':
        budget = campaign.get('budget')
        if not isinstance(budget, dict) or budget.get('budgetType') != 'DAILY':
            raise DomainError('仅支持已确认的每日预算，不覆盖其他预算类型', 409)
        value = budget.get('budget')
    elif kind == 'ads_placement_update':
        bidding = campaign.get('dynamicBidding')
        if not isinstance(bidding, dict) or not bidding.get('strategy'):
            raise DomainError('平台未返回完整竞价策略，不覆盖广告位配置', 409)
        rows = bidding.get('placementBidding', [])
        if not isinstance(rows, list) or any(not isinstance(r, dict) or 'percentage' not in r for r in rows):
            raise DomainError('广告位配置结构异常', 409)
        selected = [r for r in rows if PLACE_ALIAS.get(r.get('placement'), r.get('placement')) == subject['placement']]
        if len(selected) > 1: raise DomainError('广告位重复配置，不覆盖', 409)
        value = selected[0]['percentage'] if selected else 0
        field = 'bidding'
    else:
        gid, entity_type = subject['ad_group_id'], subject['entity_type']
        group = unique(client.list_ad_groups, 'adGroups', 'adGroupId', gid, campaign_id=cid, ad_group_id=gid)
        if str(group.get('campaignId')) != cid: raise DomainError('广告组父级不一致', 409)
        row, key, field = group, 'adGroups', 'defaultBid'
        if entity_type != 'group':
            method, key, id_field = (client.list_keywords, 'keywords', 'keywordId') if entity_type == 'keyword' else (client.list_targets, 'targetingClauses', 'targetId')
            row = unique(method, key, id_field, subject['entity_id'], campaign_id=cid, ad_group_id=gid)
            if str(row.get('campaignId')) != cid or str(row.get('adGroupId')) != gid:
                raise DomainError('广告对象父级不一致', 409)
            field = 'bid'
        elif str(subject['entity_id']) != gid: raise DomainError('广告组编号不一致', 409)
        value = row.get(field)
    if value is None: raise DomainError('平台没有返回待修改字段，不猜测前值', 409)
    return {'client': client, 'row': row, 'key': key, 'field': field,
            'value': value, 'campaign': campaign, 'source': 'Amazon Ads实时接口'}


def read(identity, action):
    """读取真实设置，Listing保留语言与受众，返回临时客户端但不将其序列化。"""
    shop = account(identity)
    if action['kind'].startswith('ads_'): return ads_read(identity, action)
    subject = action['subject']
    client, ctx = pricing.account(identity['id'], subject['sku'])
    if ctx['seller_id'] != shop['seller_id'] or ctx['marketplace_id'] != identity['marketplace_id']:
        raise DomainError('Listing客户端账户不一致', 409)
    if action['kind'] == 'listing_text_update':
        snapshot, raw = text.read(client, ctx)
        summaries = raw.get('summaries') or []
        if not any(s.get('marketplaceId') == ctx['marketplace_id'] and s.get('asin') == identity['asin'] for s in summaries):
            raise DomainError('SKU已不属于托管ASIN，不修改', 409)
        value = [r['value'] for r in snapshot['fields'][subject['field']]['rows']]
        return {'client': client, 'context': ctx, 'snapshot': snapshot, 'raw': raw, 'value': value, 'source': 'SP-API实时文字'}
    ctx = {**ctx, 'audience': subject['audience']}
    snapshot, offers, offer = pricing.read(client, ctx)
    # 价格同样检查SKU与托管ASIN关联，不只凭本地旧资料。
    raw = client.get_listings_item(subject['sku'], included_data=['summaries'])
    if not any(s.get('marketplaceId') == ctx['marketplace_id'] and s.get('asin') == identity['asin'] for s in raw.get('summaries', [])):
        raise DomainError('价格SKU不属于托管ASIN，不修改', 409)
    return {'client': client, 'context': ctx, 'snapshot': snapshot, 'offers': offers,
            'offer': offer, 'value': snapshot['fields']['standard_price']['value'], 'source': 'SP-API实时报价'}


def prepare(identity, action, current):
    """在提交前构建定向请求与持久核对依据，验证后才可登记发送阶段。"""
    subject, kind = action['subject'], action['kind']
    if not equal(action, current['value'], action['before']):
        raise DomainError('平台前值与AI资料不一致，本次未提交；请在下一轮重新分析', 409)
    if kind.startswith('ads_'):
        if kind == 'ads_placement_update':
            bidding = deepcopy(current['campaign']['dynamicBidding'])
            rows = bidding.setdefault('placementBidding', [])
            for row in rows:
                row['placement'] = PLACE_ALIAS.get(row.get('placement'), row.get('placement'))
            selected = next((r for r in rows if r.get('placement') == subject['placement']), None)
            if selected is None:
                selected = {'placement': subject['placement']}; rows.append(selected)
            selected['percentage'] = int(action['after'])
            updates = {'bidding': bidding}
        else: updates = {current['field']: float(Decimal(str(action['after'])))}
        return {'updates': updates}
    snapshot, ctx = current['snapshot'], current['context']
    if kind == 'listing_text_update':
        field = subject['field']; info = snapshot['fields'][field]
        languages = {r['language_tag'] for r in info['rows']}
        if len(languages) > 1: raise DomainError('该字段含多种语言，AI单值行动不能覆盖，请人工编辑', 409)
        language = next(iter(languages), snapshot['default_language'])
        rows = [{'value': v, 'language_tag': language, 'marketplace_id': ctx['marketplace_id']} for v in action['after']]
        patches, expected = text.build_patches(snapshot, {'version': snapshot['version'], 'changes': {field: {'mode': 'set', 'rows': rows}}})
        proof = {'expected': expected}
    else:
        if snapshot['currency'] != 'USD': raise DomainError('AI金额只支持USD，不执行其他币种', 409)
        changes = pricing.build_changes(snapshot, {'version': snapshot['version'], 'currency': 'USD',
            'actions': {'standard_price': {'mode': 'set', 'value': action['after']}}})
        patches = [{'op': 'merge', 'path': '/attributes/purchasable_offer', 'value': [
            {'marketplace_id': ctx['marketplace_id'], 'currency': 'USD', 'audience': ctx['audience'], **changes}]}]
        proof = {'changes': changes, 'preserved': pricing.preserved(current['offers'], ctx, changes)}
    return {'patches': patches, 'product_type': snapshot['product_type'],
            'proof': {**proof, 'baseline_errors': [pricing.digest(r) for r in snapshot['issues'] if r.get('severity') == 'ERROR']}}


def cache_ads(identity, action, current):
    """仅投影真实回读字段到原缓存，确保原审计前值和次日资料不是AI建议值。"""
    subject, kind = action['subject'], action['kind']
    with plan_store.transaction(True) as cur:
        if kind in ('ads_budget_update', 'ads_placement_update'):
            field = 'daily_budget' if kind == 'ads_budget_update' else 'bidding'
            value = current['value'] if field == 'daily_budget' else dumps(current['campaign']['dynamicBidding'])
            cur.execute('UPDATE amazon_ads_campaigns SET '+field+'=%s WHERE shop_id=%s AND profile_id=%s AND campaign_id=%s',
                (value, identity['id'], identity['ads_profile_id'], subject['campaign_id']))
        else:
            entity = subject['entity_type']
            table, id_field = {'group': ('amazon_ads_ad_groups', 'ad_group_id'),
                'keyword': ('amazon_ads_keywords', 'keyword_id'), 'target': ('amazon_ads_targets', 'target_id')}[entity]
            field = 'default_bid' if entity == 'group' else 'bid'
            # 子实体表没有shop_id/profile_id，必须通过活动关联限制账户，不能移除账户过滤。
            cur.execute('UPDATE '+table+' e JOIN amazon_ads_campaigns c ON c.campaign_id=e.campaign_id '
                'SET e.'+field+'=%s WHERE c.shop_id=%s AND c.profile_id=%s '
                'AND e.campaign_id=%s AND e.ad_group_id=%s AND e.'+id_field+'=%s',
                (current['value'], identity['id'], identity['ads_profile_id'], subject['campaign_id'], subject['ad_group_id'], subject['entity_id']))
        if cur.rowcount > 1: raise DomainError('广告缓存对象重复，已回滚缓存，未提交平台修改', 409)


def submit(identity, action, current, prepared):
    """唯一真实写入口；客户端自动写原日志，结果不明确时绝不循环重试。"""
    account(identity)
    client, kind, subject = current['client'], action['kind'], action['subject']
    if kind.startswith('listing_'):
        response = client.patch_listings_item(subject['sku'], prepared['patches'], product_type=prepared['product_type'])
        submission = pricing.accepted_submission(response)
        return {'runtime_mode': MODE, 'accepted': True, 'submission': submission,
                'message': 'Amazon已接受提交，等待真实属性回读；不代表商城展示'}
    updates = prepared['updates']
    if kind in ('ads_budget_update', 'ads_placement_update'):
        response = client.update_campaign(subject['campaign_id'], updates)
        id_field, entity_id = 'campaignId', subject['campaign_id']
    else:
        method = {'group': client.update_ad_group, 'keyword': client.update_keyword, 'target': client.update_target}[subject['entity_type']]
        response = method(subject['entity_id'], updates)
        id_field = {'group': 'adGroupId', 'keyword': 'keywordId', 'target': 'targetId'}[subject['entity_type']]
        entity_id = subject['entity_id']
    block = response.get(current['key']) if isinstance(response, dict) else None
    successes = block.get('success', []) if isinstance(block, dict) else []
    errors = block.get('error', []) if isinstance(block, dict) else []
    accepted = len(successes) == 1 and not errors and successes[0].get('index') == 0 and str(successes[0].get(id_field)) == str(entity_id)
    state = operation_identity.ads_state()
    return {'runtime_mode': MODE, 'accepted': True if accepted else False if errors and not successes else None,
            'history_warning': state.get('ads_history_warning'),
            'message': '广告已接受，等待真实回读' if accepted else '广告回执未确认，请只读核对，不重发'}


def failure(error):
    """只公开可控业务错误；网络异常不泄露URL/凭证，也不假定没有发送。"""
    details = getattr(error, 'details', {}) or {}
    response = getattr(error, 'response', None)
    rejected = isinstance(error, ContentError) or details.get('write_state') in ('rejected','not_sent') or isinstance(error, requests.RequestException) and response is not None and response.status_code in (400, 401, 403, 404, 413, 415, 422, 429)
    return {'runtime_mode': MODE, 'accepted': False if rejected else None,
            'not_sent': isinstance(error, ContentError) or details.get('write_state') == 'not_sent',
            'message': str(error) if isinstance(error, (DomainError, pricing.PricingError, ContentError)) else '提交阶段异常，可能已被平台处理；只读核对，不重复发送',
            **{k: details[k] for k in ('issues', 'history_id', 'submission_id', 'history_warning') if k in details}}


def link_history(identity, action, receipt):
    """按服务端AI上下文关联原业务日志，未知网络结果也保留请求历史编号。"""
    proof = operation_identity.metadata()
    if not proof: return
    listing = action['kind'].startswith('listing_')
    table = 'amazon_listing_change_history' if listing else 'amazon_ads_history'
    try:
        with plan_store.transaction() as cur:
            cur.execute('SELECT id'+(',submission_id' if listing else '')+' FROM '+table+''' WHERE shop_id=%s
                AND JSON_UNQUOTE(JSON_EXTRACT(audit_context_json,'$.run_id'))=%s
                AND JSON_UNQUOTE(JSON_EXTRACT(audit_context_json,'$.action_id'))=%s ORDER BY id''',
                (identity['id'], str(proof['run_id']), proof['action_id']))
            rows = cur.fetchall()
        receipt['history_ids'] = [str(row['id']) for row in rows]
        if listing and len(rows) == 1:
            submission = receipt.setdefault('submission', {})
            submission['history_id'] = str(rows[0]['id'])
            if rows[0].get('submission_id'): submission['submission_id'] = rows[0]['submission_id']
        if not rows: receipt['history_warning'] = '未找到原业务历史，请核对审计；不要重复提交'
    except Exception:
        receipt['history_warning'] = '原业务历史引用暂未读取，保留执行意图；不要重复提交'


def verify(identity, action, prepared, receipt):
    """从平台重新获取属性并比对持久提交依据，不依赖短期签名或本地缓存。"""
    current = read(identity, action)
    matched, issues = equal(action, current['value'], action['after']), []
    kind, state = action['kind'], 'pending'
    if kind.startswith('listing_'):
        proof, snapshot = prepared.get('proof', {}), current['snapshot']
        if kind == 'listing_text_update':
            normalized = lambda rows: sorted(rows, key=lambda row: (row['marketplace_id'], row['language_tag']))
            matched = bool(proof.get('expected')) and all(normalized(snapshot['fields'][field]['rows']) == normalized(rows)
                for field, rows in proof.get('expected', {}).items())
        else:
            matched = bool(proof.get('changes')) and all(pricing.canonical(current['offer'].get(field)) == pricing.canonical(value)
                for field, value in proof.get('changes', {}).items())
        fields = set(proof.get('expected') or ['purchasable_offer'])
        issues = [r for r in snapshot['issues'] if r.get('severity') == 'ERROR' and
            (pricing.digest(r) not in proof.get('baseline_errors', []) or set(r.get('attributeNames') or []) & fields)]
        if kind == 'listing_price_update' and pricing.preserved(current['offers'], current['context'], proof.get('changes', {})) != proof.get('preserved'):
            state = 'conflict'; matched = False
        if issues: state = 'issues'; matched = False
        if matched: state = 'attributes_confirmed'
        submission = receipt.get('submission') or receipt
        result = {'state': state, 'submission_id': submission.get('submission_id'), 'issues': issues,
                  'matched_fields': {action['subject'].get('field', 'our_price'): matched}}
        listing_history_service.confirm(current['context'], result, submission.get('history_id'))
    else:
        state = 'attributes_confirmed' if matched else 'pending'
    warnings = []
    try:
        if kind.startswith('ads_'): cache_ads(identity, action, current)
        elif kind == 'listing_text_update': text.cache_text(current['context'], current['raw'])
        else:
            pricing.cache_pricing(current['context'], current['snapshot'], current['offers'])
            if current['snapshot'].get('cache_warning'): warnings.append(current['snapshot']['cache_warning'])
    except Exception: warnings.append('平台回读已完成，本地缓存投影失败；不要重复提交')
    return {'runtime_mode': MODE, 'value': current['value'], 'matched': matched, 'state': state,
            'source': current['source'], 'issues': issues, 'warnings': warnings,
            'message': '平台属性已一致；不代表商城展示或业绩改善' if matched else '属性尚未确认或存在冲突，只核对、不重发'}
