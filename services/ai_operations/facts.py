"""P1-A经营事实包：每日及7/14/30窗口、自动/手动广告、搜索词和关键词快照。"""
from .language import text as model_text

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re

from services.listing_performance.common import DomainError, parse_date, positive_id
from . import repository as repo
from services.business_dictionary import DictionaryMapping
from .metrics import ADS_FIELDS, SKU_FIELDS, dates, digest, dumps, group_reports, ratios, select_details, summarize, totals, windows

LABELS = DictionaryMapping('ads.report',builtin_only=True)
NOTES = [
    model_text('facts.e04784ad3b64'),
    model_text('facts.4e9b4f0d912c'),
    model_text('facts.bb6f5939a4ec'),
    model_text('facts.f858792db4b1'),
    model_text('facts.67c552717d27'),
    model_text('facts.2b16c44e6622'),
    model_text('facts.12ef581b0c25'),
    model_text('facts.25a2abb99e80'),
]

ADS_REPORTING_POLICY = {
    'version': 'daily_report_date_v1', 'date_field': 'report_date',
    'collection': model_text('facts.922e871038c2'),
    'backfill_enabled': False, 'require_generated_at': False, 'require_attribution_maturity': False,
    'later_attribution_revisions': 'accepted_by_user',
    'notice': model_text('facts.536ce93c80ff'),
}

PROFIT_POLICY = {
    'version': 'user_confirmed_profit_v1', 'report_profit': 'sku_profit.net_profit',
    'allocation_method': 'user_confirmed_existing_method', 'recalculate_historical_profit': False,
    'unit_profit_role': 'optional_reference_before_advertising_and_refunds',
    'notice': model_text('facts.5e094afb25b0'),
}


def identity_values(value):
    """仅身份字段转字符串，指标数量保持原类型。"""
    if isinstance(value, list):
        return [identity_values(v) for v in value]
    if isinstance(value, dict):
        return {k: str(v) if v is not None and (k == 'id' or k.endswith('_id')) else identity_values(v)
                for k, v in value.items()}
    return value


def compact_periods(rows, periods, fields):
    """对象级只返回已观测小计；日期覆盖统一由模块提供，避免重复膨胀。"""
    return [{'key': p['key'], 'metrics': ratios(totals(
        [r for r in rows if p['start'] <= r['report_date'] <= p['end']], fields), fields == ADS_FIELDS)}
        for p in periods]


def source_evidence(rows, table):
    return {'table': table, 'row_count': len(rows),
            'row_ids_digest': digest([str(r['id']) for r in rows]),
            'latest_row_update': max((r['updated_at'] for r in rows if r.get('updated_at')), default=None)}


def same_product(row, skus, asin, sku_key='sku', asin_key='asin'):
    """SKU已知时不因ASIN相同把其他SKU冒充本店目标SKU。"""
    if row.get(sku_key):
        return row[sku_key] in skus and row.get(asin_key) in (None, '', asin)
    return row.get(asin_key) == asin


def build(data, asin, end):
    periods = windows(end)
    skus = set(data['skus'])
    relevant = set()
    for row in data['products']:
        if same_product(row, skus, asin):
            relevant.add((str(row['campaign_id']), str(row['ad_group_id'])))
    for row in data['reports']:
        if row['report_type'] == 'spAdvertisedProduct' and same_product(row, skus, asin, 'advertised_sku', 'advertised_asin'):
            relevant.add((str(row['campaign_id']), str(row['ad_group_id'])))
    campaigns = {str(r['campaign_id']): r for r in data['campaigns']}
    keywords = {(str(r['campaign_id']), str(r['ad_group_id']), str(r['keyword_id'])): r for r in data['keywords']}
    targets = {(str(r['campaign_id']), str(r['ad_group_id']), str(r['target_id'])): r for r in data['targets']}
    gaps = []
    profit_ambiguous = len(skus) > 1 or len(data.get('profit_product_skus', [])) > 1
    if profit_ambiguous:
        gaps.append({'code': 'multi_sku_asin', 'message': model_text('facts.9b0bd4d5cd56')})
    if not relevant:
        gaps.append({'code': 'no_ads_relation', 'message': model_text('facts.5c20dcbf42ce')})
    if data.get('unmapped_historical_links'):
        gaps.append({'code': 'unmapped_historical_campaign',
                     'message': model_text('facts.951b6518f67b', p0=f"{len(data['unmapped_historical_links'])}")})
    # 资料中的商品描述取允许字段，禁止把完整属性中的任意内容/凭据带入。
    for listing in data['listings']:
        raw = listing.pop('attributes_json', None)
        try:
            attrs = json.loads(raw or '{}')
        except (ValueError, TypeError):
            attrs = {}
        listing['text_attributes'] = {k: attrs[k] for k in ('item_name', 'bullet_point', 'generic_keyword',
                                                          'product_description', 'title_differentiation')
                                      if isinstance(attrs, dict) and k in attrs}
        listing['pricing_attributes'] = {k: attrs[k] for k in ('purchasable_offer', 'list_price')
                                         if isinstance(attrs, dict) and k in attrs}
    generation = {}
    for row in data['generation']:
        generation[row['period']] = row
    profits = []
    for sku in sorted(skus):
        source_rows = [r for r in data['profit'] if r['sku'] == sku]
        observed = [] if profit_ambiguous else source_rows
        counts = Counter(r['report_date'] for r in observed)
        duplicates = [day for day, count in counts.items() if count > 1]
        if duplicates or any(r['asin'] != asin for r in observed):
            raise DomainError(model_text('facts.f8c52777c53d'), 409)
        complete = [r['report_date'] for r in observed
                    if generation.get(str(r['report_date']), {}).get('status') == 'success'
                    and all(r.get(f) is not None for f in SKU_FIELDS)]
        daily_map = {r['report_date']: r for r in observed}
        daily = [{'date': day, 'present': day in daily_map, 'metrics': ratios(totals(
            [daily_map[day]] if day in daily_map else [], SKU_FIELDS))}
                 for day in dates(end - timedelta(days=29), end)]
        profits.append({'sku': sku, 'evidence_id': 'sku:' + digest(sku)[:16],
                        'identity_ambiguous': profit_ambiguous,
                        'source': source_evidence(source_rows, 'sku_profit'), 'daily': daily,
                        'periods': summarize(observed, periods, SKU_FIELDS, complete)})
    modules = []
    for kind in repo.REPORT_TYPES:
        cache = [r for r in data['cache'] if r['report_type'] == kind]
        by_date = {}
        for row in cache:
            by_date.setdefault(row['report_date'], []).append(row)
        complete = [day for day, entries in by_date.items() if len(entries) == 1
                    and entries[0]['status'] == 'COMPLETED' and entries[0]['insert_flag'] == 1]
        sources = data.get('report_sources', {}).get(kind, [])
        verified_days = {r['date'] for r in sources if r['state'] == 'verified' and r['currency'] == 'USD'}
        complete = sorted(set(complete) | verified_days)
        scope_rows = [r for r in data['reports'] if r['report_type'] == kind]
        if kind == 'spAdvertisedProduct':
            scope_rows = [r for r in scope_rows if same_product(r, skus, asin, 'advertised_sku', 'advertised_asin')]
        elif kind in ('spTargeting', 'spSearchTerm'):
            scope_rows = [r for r in scope_rows if (str(r['campaign_id']), str(r['ad_group_id'])) in relevant]
        # 币种不明/异常仍在来源统计中，但不混入USD小计。
        excluded = [r for r in scope_rows if r['currency'] != 'USD']
        observed = [r for r in scope_rows if r['currency'] == 'USD']
        null_days = {r['report_date'] for r in observed if any(r.get(f) is None for f in ADS_FIELDS)}
        # 按用户报告日期口径，有合法明细的日期可纳入观察，不要求额外生成时间证据。
        complete = sorted((set(complete) | {r['report_date'] for r in observed}) - null_days)
        if excluded:
            gaps.append({'code': 'currency_excluded', 'message': model_text('facts.f68f1c8a249f', p0=f'{kind}', p1=f'{len(excluded)}')})
            complete = [day for day in complete if day not in {r['report_date'] for r in excluded}]
        items, detail_sources = [], {}
        for dimensions, group in group_reports(observed):
            cid, gid = str(dimensions['campaign_id']), str(dimensions['ad_group_id'])
            source_id = str(dimensions.get('keyword_id') or '')
            # 原始keyword_id可以代表target，但只有类型与实体同时匹配才给关联引用。
            is_keyword = dimensions.get('keyword_type') in ('EXACT', 'PHRASE', 'BROAD')
            is_target = dimensions.get('keyword_type') in ('TARGETING_EXPRESSION', 'TARGETING_EXPRESSION_PREDEFINED')
            entity = (keywords if is_keyword else targets).get((cid, gid, source_id)) if is_keyword or is_target else None
            evidence_id = kind + ':' + digest(dimensions)[:20]
            detail_sources[evidence_id] = group
            items.append({'evidence_id': evidence_id, 'dimensions': dimensions,
                          'campaign_name': campaigns.get(cid, {}).get('name'),
                          'campaign_type': campaigns.get(cid, {}).get('targeting_type', 'UNKNOWN'),
                          'entity_link': {'kind': 'keyword' if is_keyword else 'target', 'id': source_id,
                                          'current': entity} if entity else None,
                          'source': source_evidence(group, 'amazon_ads_raw_reports'),
                          'periods': compact_periods(group, periods, ADS_FIELDS),
                          'selection_metrics': totals(group, ADS_FIELDS)})
        selected = select_details(items, 1000)
        # 仅展示少量高优先级对象每日明细；其余对象保留完整窗口小计及省略说明。
        for item in selected[:6]:
            group = detail_sources[item['evidence_id']]
            item['daily'] = [{'date': day, 'metrics': ratios(totals(
                [r for r in group if r['report_date'] == day], ADS_FIELDS), True)}
                for day in dates(end - timedelta(days=29), end)]
        selected_cost = sum((r['selection_metrics']['cost'] or 0 for r in selected), 0)
        total_cost = totals(observed, ADS_FIELDS)['cost']
        modules.append({'kind': kind, 'label': LABELS[kind], 'currency': 'USD',
                        'scope': 'asin_promoted' if kind == 'spAdvertisedProduct' else 'associated_campaign' if kind in ('spCampaigns', 'spCampaignsPlacement') else 'associated_group',
                        'source': source_evidence(scope_rows, 'amazon_ads_raw_reports'),
                        'excluded_currency_rows': len(excluded), 'attribution': '7d', 'maturity': 'not_required',
                        'analysis_basis': 'daily_report_date',
                        'source_provenance': sources,
                        'coverage_evidence': cache, 'periods': summarize(observed, periods, ADS_FIELDS, complete),
                        'total_objects': len(items), 'selected_objects': len(selected),
                        'omitted_objects': len(items) - len(selected),
                        'selected_cost_share': selected_cost / total_cost if total_cost and total_cost > 0 else None,
                        'selection_basis': model_text('facts.b9ed0b341300'),
                        'details': selected})
    # 来源只作追溯；不因缺少生成时间/旧留痕而要求回补或阻断分析。
    for module in modules:
        lookup = {r['date']: r for r in module['source_provenance']}
        for period in module['periods']:
            expected = dates(period['start'], period['end'])
            verified = [lookup[d] for d in expected if d in lookup and lookup[d]['state'] == 'verified'
                        and lookup[d]['currency'] == 'USD']
            period['source_verified_days'] = len(verified)
            period['coverage_basis'] = 'report_date'
            period['maturity'] = 'not_required'
        if any(r.get('account_matches') is False for r in module['source_provenance']):
            gaps.append({'code': 'ads_account_mismatch', 'message': model_text('facts.851acb08f5ea', p0=f"{module['kind']}")})
        if any(r.get('content_matches') is False and r.get('content_check') != 'row_limit'
               for r in module['source_provenance']):
            gaps.append({'code': 'ads_content_mismatch', 'message': model_text('facts.8beaac118222', p0=f"{module['kind']}")})
    relationships = []
    for cid, gid in sorted(relevant):
        # 当前及历史共享关系都展示；不把当前只有一个SKU当成历史独占。
        members = {(r.get('sku') or '', r.get('asin') or '') for r in data['products']
                   if (str(r['campaign_id']), str(r['ad_group_id'])) == (cid, gid)}
        members |= {(r.get('advertised_sku') or '', r.get('advertised_asin') or '') for r in data['reports']
                    if r['report_type'] == 'spAdvertisedProduct'
                    and (str(r['campaign_id']), str(r['ad_group_id'])) == (cid, gid)}
        shared = any(sku not in skus or member_asin not in ('', asin) for sku, member_asin in members)
        relationships.append({'campaign_id': cid, 'ad_group_id': gid, 'shared_or_unknown': shared or not members,
                              'members': [{'sku': sku, 'asin': a} for sku, a in sorted(members)]})
    sprite_rows = data['sprite_rows']
    sprite_selected = sprite_rows[:1000]
    snapshot = data['sprite_snapshot']
    sprite = {'snapshot': snapshot, 'latest_current_snapshot': True, 'total': len(sprite_rows),
              'selected': len(sprite_selected), 'omitted': len(sprite_rows) - len(sprite_selected),
              'items': [{**r, 'evidence_id': f"sellersprite:{r['id']}"} for r in sprite_selected],
              'history': data.get('sprite_history', []),
              'note': model_text('facts.bb863521ea2a')}
    if not snapshot:
        gaps.append({'code': 'missing_sprite', 'message': model_text('facts.daffce61f324')})
    verified_zones = {r['timezone'] for m in modules for r in m['source_provenance']
                      if r['state'] == 'verified' and r['timezone']}
    result = identity_values({
        'schema_version': 'ai_facts_v1_preview', 'execution_eligible': False, 'persisted': False,
        'identity': {**data['shop'], 'asin': asin, 'skus': sorted(skus), 'currency': 'USD'},
        'time': {'report_end': end, 'read_start': end - timedelta(days=59), 'windows': periods,
                 'business_timezone': model_text('facts.ac9d059e6f76'), 'ads_timezone': next(iter(verified_zones)) if len(verified_zones) == 1 else 'unverified_or_mixed',
                 'notice': model_text('facts.ff7f5875eb8e')},
        'ads_reporting_policy': ADS_REPORTING_POLICY,
        'profit_policy': PROFIT_POLICY,
        'notes': NOTES, 'gaps': gaps, 'products': data['listings'], 'business': profits,
        'ads': modules, 'relationships': relationships,
        'unmapped_historical_links': data.get('unmapped_historical_links', []),
        'current_entities': {k: data[k] for k in ('campaigns', 'groups', 'keywords', 'targets', 'products')},
        'inventory': data['inventory'], 'offers': data['offers'], 'keywords': sprite,
        'unit_profit_references': data.get('unit_profit_references', []),
        'pricing': data.get('pricing', []),
        'recent_operations': {'listing': data['listing_history'][:30], 'ads': data['ads_history'][:30],
                              'listing_total': len(data['listing_history']), 'ads_total': len(data['ads_history']),
                              'note': model_text('facts.610b82da10b8')},
    })
    result['content_hash'] = digest(result)
    result['generated_at'] = datetime.now(timezone.utc).isoformat()
    size = len(dumps(result).encode('utf-8'))
    if size > 2 * 1024 * 1024:
        raise DomainError(model_text('facts.e03ecf96fc22'), 422)
    result['size_bytes'] = size
    return result


def preview(params):
    shop = positive_id(params.get('shop_id'), 'shop_id')
    asin = str(params.get('asin') or '').strip().upper()
    if not re.fullmatch(r'[A-Z0-9]{10}', asin):
        raise DomainError(model_text('facts.c762711baf30'))
    # 仅支持当前已核实的源报表固定UTC-7，不依赖服务器本地时区。
    today = datetime.now(timezone(timedelta(hours=-7))).date()
    end = parse_date(params.get('report_date') or str(today - timedelta(days=1)))
    if end >= today:
        raise DomainError(model_text('facts.4e7c1330efe1'))
    with repo.session() as cur:
        data = repo.read(cur, shop, asin, end - timedelta(days=59), end)
    return build(data, asin, end)
