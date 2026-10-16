"""按页面范围读取广告实体；全部读取成功后再用短事务保存。"""
from datetime import datetime, timezone
import time

import requests

from services import ads_negative_service as negatives
from services.mysql_service import get_db_connection
from services.shop_service import get_shop_by_id, get_ads_api_client


FAMILIES = {
    'campaigns': ('list_sp_campaigns', 'campaigns', 'campaignId', 'amazon_ads_campaigns', 'campaign_id'),
    'ad_groups': ('list_ad_groups', 'adGroups', 'adGroupId', 'amazon_ads_ad_groups', 'ad_group_id'),
    'product_ads': ('list_product_ads', 'productAds', 'adId', 'amazon_ads_product_ads', 'ad_id'),
    'keywords': ('list_keywords', 'keywords', 'keywordId', 'amazon_ads_keywords', 'keyword_id'),
    'targets': ('list_targets', 'targetingClauses', 'targetId', 'amazon_ads_targets', 'target_id'),
}


def normalize(body):
    if not isinstance(body, dict):
        raise negatives.NegativeError('请求须为 JSON 对象')
    shop = negatives.positive_id(body.get('shop_id'), 'shop_id')
    scope = body.get('scope', 'shop')
    campaign, group = body.get('campaign_id'), body.get('ad_group_id')
    if scope not in ('shop', 'campaign', 'group'):
        raise negatives.NegativeError('同步范围必须为店铺、活动或广告组')
    if scope == 'shop':
        if campaign not in (None, '') or group not in (None, ''):
            raise negatives.NegativeError('店铺级同步不能附带活动或组 ID')
        return dict(shop_id=shop, scope=scope, campaign_id=None, ad_group_id=None)
    campaign = negatives.positive_id(campaign, 'campaign_id')
    if scope == 'group':
        group = negatives.positive_id(group, 'ad_group_id')
    elif group not in (None, ''):
        raise negatives.NegativeError('活动级同步不能附带广告组 ID')
    else:
        group = None
    return dict(shop_id=shop, scope=scope, campaign_id=campaign, ad_group_id=group)


def _read_pages(method, key, **filters):
    rows, token, seen = [], None, set()
    while True:
        for attempt in range(3):
            try:
                response = method(max_results=100, next_token=token,
                                  state_filter=['ENABLED', 'PAUSED', 'ARCHIVED'], **filters)
                break
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 429 or attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        if not isinstance(response, dict) or not isinstance(response.get(key), list):
            raise negatives.NegativeError(f'Amazon {key} 响应异常，未覆盖原数据', 502)
        rows.extend(response[key])
        token = response.get('nextToken')
        if not token:
            return rows
        if token in seen:
            raise negatives.NegativeError('Amazon 分页游标重复，已停止同步', 502)
        seen.add(token)


def _validate(rows, family, ctx):
    identifiers = set()
    for row in rows:
        if not isinstance(row, dict):
            raise negatives.NegativeError('Amazon 返回无效实体', 502)
        identifier = negatives.positive_id(row.get(FAMILIES[family][2]), '实体 ID')
        if identifier in identifiers:
            raise negatives.NegativeError('Amazon 分页返回重复实体，未保存本批数据', 502)
        identifiers.add(identifier)
        campaign = negatives.positive_id(row.get('campaignId'), 'campaignId')
        if ctx['campaign_id'] and campaign != ctx['campaign_id']:
            raise negatives.NegativeError('Amazon 返回了其他活动，未保存本批数据', 502)
        if family != 'campaigns':
            group = negatives.positive_id(row.get('adGroupId'), 'adGroupId')
            if ctx['ad_group_id'] and group != ctx['ad_group_id']:
                raise negatives.NegativeError('Amazon 返回了其他广告组，未保存本批数据', 502)


class _SnapshotClient:
    """只向原 SQL 保存函数供应内存快照，不在事务中请求 Amazon。"""
    def __init__(self, profile_id, datasets):
        self.profile_id, self.datasets = profile_id, datasets

    def __getattr__(self, method):
        for family, (name, key, *_rest) in FAMILIES.items():
            if method == name:
                return lambda **kwargs: {key: self.datasets[family]}
        if method in ('list_negative_keywords', 'list_negative_targets'):
            kind = 'keywords' if method == 'list_negative_keywords' else 'targets'
            def read(campaign_level=False, **kwargs):
                return {negatives._key(kind, campaign_level): self.datasets[(kind, campaign_level)]}
            return read
        raise AttributeError(method)


def _check_existing(cursor, datasets, ctx):
    """既有 ID 不能在一次局部同步中被重新归属给其他店铺/活动/组。"""
    for family, (_method, _key, api_id, table, db_id) in FAMILIES.items():
        expected = {int(r[api_id]): r for r in datasets[family]}
        ids = list(expected)
        for offset in range(0, len(ids), 100):
            chunk = ids[offset:offset + 100]
            owner_columns = 'shop_id,profile_id' if family == 'campaigns' else 'campaign_id'
            cursor.execute(f'SELECT {db_id},{owner_columns} FROM {table} WHERE {db_id} IN ({",".join(["%s"] * len(chunk))})', chunk)
            for old in cursor.fetchall():
                item = expected[int(old[db_id])]
                if family == 'campaigns':
                    valid = int(old['shop_id']) == ctx['shop_id'] and str(old['profile_id']) == str(ctx['profile_id'])
                else:
                    valid = int(old['campaign_id']) == int(item['campaignId'])
                if not valid:
                    raise negatives.NegativeError('实体 ID 的原归属不一致，未覆盖其他范围数据', 409)


def sync(body):
    ctx = normalize(body)
    shop = get_shop_by_id(ctx['shop_id'])
    if not shop or not shop.get('ads_profile_id') or not shop.get('ads_refresh_token'):
        raise negatives.NegativeError('店铺不存在、未授权广告或缺少广告账户，请检查店铺配置', 400)
    if ctx['scope'] != 'shop':
        negatives.context(ctx['shop_id'], ctx['campaign_id'], ctx['ad_group_id'], ctx['scope'])
    client = get_ads_api_client(ctx['shop_id'])
    if str(client.profile_id) != str(shop['ads_profile_id']) or client.region != shop['region'].lower():
        raise negatives.NegativeError('店铺广告账户或区域已变更，请重新核对配置', 409)
    ctx['profile_id'] = client.profile_id
    datasets = {}
    for family, (method, key, *_rest) in FAMILIES.items():
        filters = {}
        if ctx['campaign_id']:
            filters['campaign_id'] = ctx['campaign_id']
        if ctx['ad_group_id'] and family != 'campaigns':
            filters['ad_group_id'] = ctx['ad_group_id']
        # 组页只读取组自身及下属实体，不更新父活动或其他组。
        rows = [] if family == 'campaigns' and ctx['scope'] == 'group' else _read_pages(getattr(client, method), key, **filters)
        _validate(rows, family, ctx)
        datasets[family] = rows
    if ctx['scope'] == 'campaign' and len(datasets['campaigns']) != 1:
        raise negatives.NegativeError('Amazon 未返回当前活动，旧数据保留，请核对活动是否仍存在', 404)
    if ctx['scope'] == 'group' and len(datasets['ad_groups']) != 1:
        raise negatives.NegativeError('Amazon 未返回当前广告组，旧数据保留，请核对广告组是否仍存在', 404)
    campaign_ids = {int(r['campaignId']) for r in datasets['campaigns']} if ctx['scope'] == 'shop' else {ctx['campaign_id']}
    group_ids = {int(r['adGroupId']): int(r['campaignId']) for r in datasets['ad_groups']}
    for family in ('ad_groups', 'product_ads', 'keywords', 'targets'):
        for row in datasets[family]:
            if int(row['campaignId']) not in campaign_ids or group_ids.get(int(row['adGroupId'])) != int(row['campaignId']):
                raise negatives.NegativeError('实体所属活动或组不完整，未保存本批数据', 502)
    for kind in ('keywords', 'targets'):
        for campaign_level in (False, True):
            filters = {'campaign_id': ctx['campaign_id']} if ctx['campaign_id'] else {}
            if ctx['ad_group_id']:
                filters['ad_group_id'] = ctx['ad_group_id']
            rows = [] if campaign_level and ctx['scope'] == 'group' else negatives.fetch_all(client, kind, campaign_level, **filters)
            for row in rows:
                campaign = negatives.positive_id(row.get('campaignId'), 'campaignId')
                if campaign not in campaign_ids:
                    raise negatives.NegativeError('否定规则属于其他活动，未保存本批数据', 502)
                if not campaign_level:
                    group = negatives.positive_id(row.get('adGroupId'), 'adGroupId')
                    if group_ids.get(group) != campaign:
                        raise negatives.NegativeError('否定规则属于其他广告组，未保存本批数据', 502)
            datasets[(kind, campaign_level)] = rows
    # 暂复用旧模块稳定的实体保存 SQL；不复制表字段或更改原自动任务。
    from blueprints.amazon import advertising_manage as legacy
    snapshot = _SnapshotClient(client.profile_id, datasets)
    conn = get_db_connection()
    counts = {}
    try:
        with conn.cursor() as cursor:
            _check_existing(cursor, datasets, ctx)
            counts['campaigns'] = legacy._sync_campaigns(cursor, snapshot, ctx['shop_id'], client.profile_id)
            for family in ('ad_groups', 'product_ads', 'keywords', 'targets', 'negative_keywords', 'negative_targets'):
                name = family.replace('negative_', 'neg_', 1)
                counts[name] = getattr(legacy, '_sync_' + family)(cursor, snapshot, ctx['shop_id'])
        conn.commit()
    except negatives.NegativeError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise negatives.NegativeError('本地保存失败，已回滚，原数据保留，请稍后重试', 502)
    finally:
        conn.close()
    return {**ctx, **counts, 'error': None,
            'campaign_id': str(ctx['campaign_id']) if ctx['campaign_id'] else None,
            'ad_group_id': str(ctx['ad_group_id']) if ctx['ad_group_id'] else None,
            'counts': counts, 'total': sum(counts.values()), 'report_metrics_updated': False,
            'synced_at': datetime.now(timezone.utc).isoformat()}
