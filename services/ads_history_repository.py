"""统一广告历史持久化；所有时间为UTC毫秒，不在事务中调用外部接口。"""
import hashlib
import json
import time
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation

from services.mysql_service import get_db_connection


class HistoryError(ValueError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def now_ms():
    return int(time.time() * 1000)


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)


def loads(value):
    return json.loads(value) if isinstance(value, str) else value


def digest(value):
    return hashlib.sha256(dumps(value).encode('utf-8')).hexdigest()


@contextmanager
def transaction():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def account(shop_id):
    from services.shop_service import get_shop_by_id
    from services.ads_negative_service import positive_id
    shop = get_shop_by_id(positive_id(shop_id, 'shop_id'))
    if not shop or not shop.get('ads_profile_id') or not shop.get('ads_refresh_token'):
        raise HistoryError('店铺未配置有效广告授权', 400)
    result = {key: shop.get(key) for key in
              ('id', 'credential_group_id', 'region', 'marketplace_id', 'ads_profile_id')}
    result.update(shop_id=shop['id'], profile_id=str(shop['ads_profile_id']), region=shop['region'].lower())
    with transaction() as c:
        c.execute('SELECT id FROM amazon_shops WHERE status=1 AND ads_profile_id=%s AND LOWER(region)=%s',
                  (result['profile_id'], result['region']))
        if len(c.fetchall()) != 1:
            raise HistoryError('同一广告账户被多个店铺绑定，请先核对配置，未混合历史', 409)
    return result


# 表名、ID列均来自固定白名单，不接收外部SQL标识符。
ENTITIES = {
    'AD_GROUP': ('amazon_ads_ad_groups', 'ad_group_id', 'name'),
    'AD': ('amazon_ads_product_ads', 'ad_id', 'sku'),
    'KEYWORD': ('amazon_ads_keywords', 'keyword_id', 'keyword_text'),
    'NEGATIVE_KEYWORD': ('amazon_ads_negative_keywords', 'keyword_id', 'keyword_text'),
    'PRODUCT_TARGETING': ('amazon_ads_targets', 'target_id', 'resolved_expression'),
    'NEGATIVE_PRODUCT_TARGETING': ('amazon_ads_negative_targets', 'target_id', 'resolved_expression'),
}


def entity(c, acct, kind, identifier):
    if not identifier:
        return None
    if kind == 'CAMPAIGN':
        c.execute('SELECT *,name AS entity_name FROM amazon_ads_campaigns WHERE campaign_id=%s AND shop_id=%s AND profile_id=%s',
                  (identifier, acct['shop_id'], acct['profile_id']))
    elif kind in ENTITIES:
        table, column, name = ENTITIES[kind]
        c.execute(f'''SELECT e.*,e.{name} AS entity_name FROM {table} e JOIN amazon_ads_campaigns a
                      ON e.campaign_id=a.campaign_id WHERE e.{column}=%s AND a.shop_id=%s AND a.profile_id=%s''',
                  (identifier, acct['shop_id'], acct['profile_id']))
    else:
        return None
    return c.fetchone()


def scope_key(ctx):
    return str(ctx.get('ad_group_id') or ctx.get('campaign_id') or '0')


def ensure_state(c, ctx):
    c.execute('''INSERT IGNORE INTO amazon_ads_history_sync
                 (shop_id,region,profile_id,scope,scope_id,campaign_id,ad_group_id)
                 VALUES (%s,%s,%s,%s,%s,%s,%s)''',
              (ctx['shop_id'], ctx['region'], ctx['profile_id'], ctx['scope'], scope_key(ctx),
               ctx.get('campaign_id'), ctx.get('ad_group_id')))
    c.execute('''SELECT * FROM amazon_ads_history_sync WHERE region=%s AND profile_id=%s
                 AND scope=%s AND scope_id=%s FOR UPDATE''',
              (ctx['region'], ctx['profile_id'], ctx['scope'], scope_key(ctx)))
    return c.fetchone()


def mark_dirty(c, ctx):
    state = ensure_state(c, ctx)
    c.execute('''UPDATE amazon_ads_history_sync SET dirty_version=dirty_version+1,
                 dirty_since_ms=COALESCE(dirty_since_ms,%s) WHERE id=%s''', (now_ms(), state['id']))


def insert_local(c, row):
    fields = ['shop_id', 'credential_group_id', 'region', 'marketplace_id', 'profile_id', 'campaign_id',
              'ad_group_id', 'entity_type', 'entity_id', 'entity_name', 'change_type', 'previous_value',
              'new_value', 'metadata', 'source', 'sync_status', 'write_status', 'event_time_ms', 'local_key',
              'request_key', 'local_before', 'local_after', 'local_payload', 'local_started_ms',
              'operator_id', 'operator_name']
    json_fields = {'previous_value', 'new_value', 'metadata', 'local_before', 'local_after', 'local_payload'}
    values = [dumps(row.get(k)) if k in json_fields else row.get(k) for k in fields]
    c.execute(f'''INSERT INTO amazon_ads_history ({','.join(fields)},created_at,updated_at)
                  VALUES ({','.join(['%s'] * len(fields))},UTC_TIMESTAMP(),UTC_TIMESTAMP())''', values)
    return c.lastrowid


def comparable(value, change):
    if value is None:
        return ('null',)
    if change in ('BUDGET_AMOUNT', 'BID_AMOUNT', 'DEFAULT_BID_AMOUNT', 'PLACEMENT_GROUP'):
        try:
            number = Decimal(str(value))
            if number.is_finite():
                return ('number', number)
        except (InvalidOperation, ValueError):
            pass
    if change == 'STATUS':
        return ('state', str(value).upper())
    return ('value', dumps(value))


def placement_position(value):
    """写入API与历史接口使用不同广告位枚举；只归一已知别名，不改原始证据。"""
    aliases = {'TOP': 'TOP_OF_SEARCH', 'PLACEMENT_TOP': 'TOP_OF_SEARCH',
               'PLACEMENT_TOP_OF_SEARCH': 'TOP_OF_SEARCH', 'PRODUCT_PAGE': 'DETAIL_PAGE',
               'PLACEMENT_PRODUCT_PAGE': 'DETAIL_PAGE', 'PLACEMENT_REST_OF_SEARCH': 'REST_OF_SEARCH'}
    if not isinstance(value, str) or not value:
        return None
    return aliases.get(value, value)


def compatible(local, event):
    """仅规则关联，不推断平台操作人；前值未知、并发歧义一律不强合。"""
    if local['change_type'] != event['change_type']:
        return False
    change = event['change_type']
    if change != 'CREATED' and (loads(local['local_before']) is None or event['previous_value'] is None):
        return False
    if comparable(loads(local['local_before']), change) != comparable(event['previous_value'], change):
        return False
    if comparable(loads(local['local_after']), change) != comparable(event['new_value'], change):
        return False
    lm = loads(local.get('metadata')) or {}
    em = event.get('metadata') or {}
    if change == 'PLACEMENT_GROUP':
        position = placement_position(lm.get('placementGroupPosition'))
        return position is not None and position == placement_position(em.get('placementGroupPosition'))
    return lm.get('placementGroupPosition') == em.get('placementGroupPosition')


def _merge(c, local, event, existing=None):
    # 平台先到也不删证据：原平台行保留并指向ERP行，解除其指纹唯一占位。
    if existing:
        c.execute('UPDATE amazon_ads_history SET merged_into_id=%s,event_fingerprint=NULL,updated_at=UTC_TIMESTAMP() WHERE id=%s',
                  (local['id'], existing['id']))
    c.execute('''UPDATE amazon_ads_history SET previous_value=%s,new_value=%s,metadata=%s,
                 platform_event=%s,event_fingerprint=%s,platform_time_ms=%s,event_time_ms=%s,
                 sync_status='synced',match_method='rule_unique',error_message='',updated_at=UTC_TIMESTAMP()
                 WHERE id=%s''',
              (dumps(event['previous_value']), dumps(event['new_value']), dumps(event['metadata']),
               dumps(event['raw']), event['fingerprint'], event['timestamp'], event['timestamp'], local['id']))


def save_event(c, acct, event, allow_merge=False):
    c.execute('''SELECT * FROM amazon_ads_history WHERE region=%s AND profile_id=%s AND event_fingerprint=%s FOR UPDATE''',
              (acct['region'], acct['profile_id'], event['fingerprint']))
    existing = c.fetchone()
    if existing and existing['source'] == 'ERP':
        return 0
    if existing and not allow_merge:
        # 实体同步后可纠正旧历史的类型/父级投影；平台原始事件、指纹、时间和金额保持不变。
        projection = ('entity_type', 'campaign_id', 'ad_group_id', 'entity_name')
        if any(str(existing.get(key) or '') != str(event.get(key) or '') for key in projection):
            c.execute('''UPDATE amazon_ads_history SET entity_type=%s,campaign_id=%s,ad_group_id=%s,
                         entity_name=%s,updated_at=UTC_TIMESTAMP() WHERE id=%s AND source='PLATFORM' ''',
                      tuple(event.get(key) for key in projection)+(existing['id'],))
        return 0
    c.execute('''SELECT * FROM amazon_ads_history WHERE region=%s AND profile_id=%s AND entity_type=%s
                 AND entity_id=%s AND change_type=%s AND source='ERP' AND platform_time_ms IS NULL
                 AND write_status='accepted' AND local_started_ms BETWEEN %s AND %s
                 AND merged_into_id IS NULL FOR UPDATE''',
              (acct['region'], acct['profile_id'], event['entity_type'], event['entity_id'], event['change_type'],
               event['timestamp'] - 300000, event['timestamp'] + 300000))
    candidates = [r for r in c.fetchall() if compatible(r, event)]
    # 还需保证该ERP行在窗口内也仅对应这一条平台事件，连续往返同价不得凭单向唯一合并。
    if allow_merge and len(candidates) == 1:
        local = candidates[0]
        c.execute('''SELECT * FROM amazon_ads_history WHERE region=%s AND profile_id=%s AND entity_type=%s
                     AND entity_id=%s AND change_type=%s AND source='PLATFORM' AND merged_into_id IS NULL
                     AND platform_time_ms BETWEEN %s AND %s FOR UPDATE''',
                  (acct['region'], acct['profile_id'], event['entity_type'], event['entity_id'], event['change_type'],
                   local['local_started_ms'] - 300000, local['local_started_ms'] + 300000))
        other = [r for r in c.fetchall() if r.get('event_fingerprint') != event['fingerprint'] and compatible(local, {
            'change_type': r['change_type'], 'previous_value': loads(r['previous_value']),
            'new_value': loads(r['new_value']), 'metadata': loads(r['metadata'])})]
        if not other:
            _merge(c, local, event, existing)
            return 1
    if existing:
        return 0
    fields = ['shop_id', 'credential_group_id', 'region', 'marketplace_id', 'profile_id', 'campaign_id',
              'ad_group_id', 'entity_type', 'entity_id', 'entity_name', 'change_type', 'previous_value',
              'new_value', 'metadata', 'source', 'sync_status', 'write_status', 'event_time_ms',
              'platform_time_ms', 'event_fingerprint', 'platform_event', 'match_method']
    row = {**acct, **event, 'source': 'PLATFORM', 'sync_status': 'synced', 'write_status': '',
           'event_time_ms': event['timestamp'], 'platform_time_ms': event['timestamp'],
           'event_fingerprint': event['fingerprint'], 'platform_event': event['raw'],
           'match_method': 'ambiguous' if candidates else ''}
    values = [dumps(row.get(k)) if k in ('previous_value', 'new_value', 'metadata', 'platform_event') else row.get(k) for k in fields]
    c.execute(f'''INSERT INTO amazon_ads_history ({','.join(fields)},created_at,updated_at)
                  VALUES ({','.join(['%s'] * len(fields))},UTC_TIMESTAMP(),UTC_TIMESTAMP())''', values)
    return 1


def reconcile(c, acct, entity_type, entity_id):
    """ERP响应晚于平台采集时再次关联；候选均已存储再判断。"""
    c.execute('''SELECT platform_event FROM amazon_ads_history WHERE region=%s AND profile_id=%s
                 AND entity_type=%s AND entity_id=%s AND source='PLATFORM' AND merged_into_id IS NULL
                 AND platform_time_ms>=%s ORDER BY id''',
              (acct['region'], acct['profile_id'], entity_type, entity_id, now_ms() - 900000))
    return [loads(r['platform_event']) for r in c.fetchall()]
