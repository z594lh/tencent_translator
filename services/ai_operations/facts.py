"""P1-A经营事实包：每日及7/14/30窗口、自动/手动广告、搜索词和关键词快照。"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re

from services.listing_performance.common import DomainError, parse_date, positive_id
from . import repository as repo
from .metrics import ADS_FIELDS, SKU_FIELDS, dates, digest, dumps, group_reports, ratios, select_details, summarize, totals, windows

LABELS = {'spAdvertisedProduct': '商品广告（当前ASIN）', 'spCampaigns': '关联活动（含其他商品）',
          'spCampaignsPlacement': '活动广告位（含其他商品）', 'spTargeting': '自动/手动投放（关联组）',
          'spSearchTerm': '搜索词（关联组）'}
NOTES = [
    '本资料用于生成具体运营方案；读取资料本身不调用AI或执行修改，当前设置不作为历史回测资料。',
    '统计窗口7/14/30日均汇总原始7日归因指标，不是7/14/30日归因模型。重叠窗口不能当成独立证据。',
    '经营日期沿用源报表固定UTC-7；广告账户历史时区、profile及币种来源未独立核实，不能精确跨源归因。',
    '广告按report_date使用每日采集时的现有指标；用户接受后续归因修订可能带来的偏差，不要求历史重抓、生成时间或等待归因窗口结束。',
    'SKU净利润与分摊沿用用户已确认的现有报表口径；不重算历史利润，不重复扣除广告费。',
    '本地利润表唯一键为店铺/ASIN/日期，旧生成器按产品SKU写入且广告费按ASIN映射；多SKU关联时利润归属不能直接信任。',
    '商品广告按推广商品统计；搜索词/投放为组范围，活动/广告位为活动范围，不代表该ASIN独占，也不能互加。',
    '当前配置、库存及最新卖家精灵快照是读取时状态，不是报表截止日的历史状态。未采集的日期和排名不伪造。',
]

ADS_REPORTING_POLICY = {
    'version': 'daily_report_date_v1', 'date_field': 'report_date',
    'collection': '沿用现有每日15:30采集前一整天报告的任务配置，不在本模块更改调度',
    'backfill_enabled': False, 'require_generated_at': False, 'require_attribution_maturity': False,
    'later_attribution_revisions': 'accepted_by_user',
    'notice': '按已采集报告日期分析；不代表最终结算或零偏差，真实缺失、账户/币种不符仍需披露',
}

PROFIT_POLICY = {
    'version': 'user_confirmed_profit_v1', 'report_profit': 'sku_profit.net_profit',
    'allocation_method': 'user_confirmed_existing_method', 'recalculate_historical_profit': False,
    'unit_profit_role': 'optional_reference_before_advertising_and_refunds',
    'notice': '现有利润及分摊口径已由用户确认，直接使用报表；仅真实资料缺失或冲突提示异常',
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
        gaps.append({'code': 'multi_sku_asin', 'message': '该ASIN存在多个Listing或活跃产品SKU；源利润表按ASIN唯一，归属可能被覆盖，本次不展示利润及经营汇总。'})
    if not relevant:
        gaps.append({'code': 'no_ads_relation', 'message': '没有可核实的当前/窗口内商品广告关系；空结果不等于未投广告。'})
    if data.get('unmapped_historical_links'):
        gaps.append({'code': 'unmapped_historical_campaign',
                     'message': f"有{len(data['unmapped_historical_links'])}组历史商品广告无法关联当前账户活动，未强行归属；广告资料不是完整账户历史。"})
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
            raise DomainError('SKU日报存在同日重复或历史ASIN不一致，不能安全汇总，请先核对源数据', 409)
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
            gaps.append({'code': 'currency_excluded', 'message': f'{kind}有{len(excluded)}行币种非USD或未知，未汇入USD小计。'})
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
        selected = select_details(items, 80)
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
                        'selection_basis': '60日花费/归因购买/曝光分层选取；全量小计单独保留；未展示对象不能获得执行建议',
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
            gaps.append({'code': 'ads_account_mismatch', 'message': f"{module['kind']}存在已记录来源账户与当前店铺不符的日期，请核对真实归属。"})
        if any(r.get('content_matches') is False and r.get('content_check') != 'row_limit'
               for r in module['source_provenance']):
            gaps.append({'code': 'ads_content_mismatch', 'message': f"{module['kind']}存在已记录内容与当前明细不符的日期；这与缺少生成时间不同，请核对实际数据变化。"})
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
    sprite_selected = sprite_rows[:300]
    snapshot = data['sprite_snapshot']
    sprite = {'snapshot': snapshot, 'latest_current_snapshot': True, 'total': len(sprite_rows),
              'selected': len(sprite_selected), 'omitted': len(sprite_rows) - len(sprite_selected),
              'items': [{**r, 'evidence_id': f"sellersprite:{r['id']}"} for r in sprite_selected],
              'history': data.get('sprite_history', []),
              'note': '最新最多300个完整词组，历史按实际成功文件比较；近30日快照不是单日自然曝光，市场月指标不是本ASIN销量。'}
    if not snapshot:
        gaps.append({'code': 'missing_sprite', 'message': '该ASIN暂无成功卖家精灵快照。'})
    verified_zones = {r['timezone'] for m in modules for r in m['source_provenance']
                      if r['state'] == 'verified' and r['timezone']}
    result = identity_values({
        'schema_version': 'ai_facts_v1_preview', 'execution_eligible': False, 'persisted': False,
        'identity': {**data['shop'], 'asin': asin, 'skus': sorted(skus), 'currency': 'USD'},
        'time': {'report_end': end, 'read_start': end - timedelta(days=59), 'windows': periods,
                 'business_timezone': 'UTC-07:00（源报表固定偏移）', 'ads_timezone': next(iter(verified_zones)) if len(verified_zones) == 1 else 'unverified_or_mixed',
                 'notice': '当前商品/配置和最新快照独立于报表截止日；不可当作历史回测资料'},
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
                              'note': '当前向前60天内最近最多各30条本地已知记录，独立于报表截止日；非完整平台历史，缺失不代表无人修改'},
    })
    result['content_hash'] = digest(result)
    result['generated_at'] = datetime.now(timezone.utc).isoformat()
    size = len(dumps(result).encode('utf-8'))
    if size > 2 * 1024 * 1024:
        raise DomainError('事实包超过2MiB预览上限，未截断或提交AI；需要进一步拆分广告范围', 422)
    result['size_bytes'] = size
    return result


def preview(params):
    shop = positive_id(params.get('shop_id'), 'shop_id')
    asin = str(params.get('asin') or '').strip().upper()
    if not re.fullmatch(r'[A-Z0-9]{10}', asin):
        raise DomainError('请输入有效10位ASIN')
    # 仅支持当前已核实的源报表固定UTC-7，不依赖服务器本地时区。
    today = datetime.now(timezone(timedelta(hours=-7))).date()
    end = parse_date(params.get('report_date') or str(today - timedelta(days=1)))
    if end >= today:
        raise DomainError('截止日须为已结束的业务日')
    with repo.session() as cur:
        data = repo.read(cur, shop, asin, end - timedelta(days=59), end)
    return build(data, asin, end)
