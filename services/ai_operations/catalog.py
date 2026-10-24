"""把现有商品/广告设置转成明确的行动对象，中文映射复用广告展示规则。"""
import json
from decimal import Decimal, InvalidOperation
from services.ads_display import auto_label, PLACEMENTS
from services import listing_content_policy
from services.business_dictionary import DictionaryMapping
from .metrics import digest

TEXT_FIELDS = DictionaryMapping('listing.field',builtin_only=True)
PLACEMENT_CODES = {'PLACEMENT_TOP': 'TOP', 'PLACEMENT_REST_OF_SEARCH': 'REST_OF_SEARCH',
                   'PLACEMENT_PRODUCT_PAGE': 'PRODUCT_PAGE'}
REPORT_PLACEMENTS = {'Top of Search on-Amazon': 'PLACEMENT_TOP', 'Other on-Amazon': 'PLACEMENT_REST_OF_SEARCH',
                     'Detail Page on-Amazon': 'PLACEMENT_PRODUCT_PAGE'}


def number(value):
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def field_values(product, field, market):
    attrs = product.get('text_attributes', {})
    rows = attrs.get(field, [])
    if field not in attrs and field in ('item_name', 'product_description', 'title_differentiation') and isinstance(product.get(field), str):
        return [product[field]]
    if not isinstance(rows, list):
        return None
    if any(not isinstance(r, dict) or not isinstance(r.get('value'), str) for r in rows):
        return None
    return [r['value'] for r in rows if r.get('marketplace_id') in (None, market)]


def build(pack):
    entries = {}
    def add(key, kind, label, subject, before, unit, evidence_refs, **extra):
        entries[key] = {'ref': key, 'kind': kind, 'label': label, 'subject': subject,
                        'before': before, 'unit': unit, 'evidence_refs': evidence_refs, **extra}
    market = pack['identity']['marketplace_id']
    for product in pack['products']:
        sku = product['sku']; resource = 'listing:' + digest(sku)[:16]
        subject = {'sku': sku, 'asin': product['asin'], 'marketplace_id': market}
        name = product.get('product_name') or sku
        rules = listing_content_policy.policy(market, product.get('product_type'), product.get('brand'))
        for field, label in TEXT_FIELDS.items():
            before = field_values(product, field, market)
            if before is not None:
                add(resource + ':' + field, 'listing_text_update', name + ' · ' + label,
                    {**subject, 'field': field}, before, 'text_list', [resource],
                    constraints=rules['fields'][field], policy_version=rules['version'],
                    output_format={'type':'array_of_strings','min_items':rules['fields'][field]['min_items'],'max_items':rules['fields'][field]['max_items'],
                                   'note':'五点每条一个字符串，返回完整列表' if field=='bullet_point' else '整个字段仅一个完整字符串；后台搜索词的多个词组也放在同一个字符串里'})
    for pricing in pack.get('pricing', []):
        for offer in pricing['offers']:
            audience = offer.get('audience')
            value = number(offer.get('our_price'))
            if audience not in ('ALL', 'B2B') or offer.get('currency') != 'USD' or value is None or value <= 0:
                continue
            matches = [r for r in pricing['offers'] if r.get('audience') == audience and r.get('currency') == 'USD']
            if len(matches) != 1:
                continue
            key = 'price:' + digest([pricing['sku'], audience])[:16]
            add(key, 'listing_price_update', pricing['sku'] + (' · 普通价' if audience == 'ALL' else ' · 企业价'),
                {'sku': pricing['sku'], 'audience': audience, 'price_type': 'our_price'}, str(value), 'USD',
                ['pricing:' + digest(pricing['sku'])[:16]])
    for campaign in pack['current_entities']['campaigns']:
        cid = str(campaign['campaign_id']); base = 'campaign:' + cid
        affected = sorted({p['sku'] for p in pack['current_entities']['products']
                           if str(p['campaign_id']) == cid and p.get('sku')})
        subject = {'campaign_id': cid, 'campaign_name': campaign.get('name'), 'affected_skus': affected,
                   'scope': 'campaign'}
        budget = number(campaign.get('daily_budget'))
        if budget is not None and budget > 0:
            add(base + ':budget', 'ads_budget_update', (campaign.get('name') or cid) + ' · 每日预算',
                subject, str(budget), 'USD/day', [base])
        bidding = campaign.get('bidding')
        try:
            if isinstance(bidding, str):bidding = json.loads(bidding)
        except ValueError:
            bidding = None
        if isinstance(bidding, dict) and isinstance(bidding.get('placementBidding'), list):
            adjustments = {r.get('placement'): r.get('percentage') for r in bidding['placementBidding'] if isinstance(r, dict)}
            if 'PLACEMENT_TOP_OF_SEARCH' in adjustments:
                adjustments['PLACEMENT_TOP'] = adjustments['PLACEMENT_TOP_OF_SEARCH']
            for code, label_code in PLACEMENT_CODES.items():
                value = number(adjustments.get(code, 0))
                if value is not None and value == value.to_integral_value() and 0 <= value <= 900:
                    add(base + ':' + code, 'ads_placement_update', (campaign.get('name') or cid) + ' · ' + PLACEMENTS[label_code],
                        {**subject, 'placement': code}, int(value), 'percent', [base],
                        note='广告位竞价加价比例，不是曝光流量分配比例')
    for kind, key, identifier, price in (('group', 'groups', 'ad_group_id', 'default_bid'),
                                        ('keyword', 'keywords', 'keyword_id', 'bid'),
                                        ('target', 'targets', 'target_id', 'bid')):
        for entity in pack['current_entities'][key]:
            relation = next((r for r in pack['relationships'] if str(r['campaign_id']) == str(entity['campaign_id'])
                             and str(r['ad_group_id']) == str(entity['ad_group_id'])), None)
            if relation is None:
                continue
            value = number(entity.get(price))
            if value is None or value <= 0:
                continue
            cid, gid, eid = str(entity['campaign_id']), str(entity['ad_group_id']), str(entity[identifier])
            resource = kind + ':' + eid
            raw = entity.get('keyword_text') or entity.get('resolved_expression') or entity.get('name') or eid
            name = auto_label(raw) if kind == 'target' else None
            add(resource + ':bid', 'ads_bid_update', str(name or raw),
                {'entity_type': kind, 'campaign_id': cid, 'ad_group_id': gid, 'entity_id': eid,
                 'match_type': entity.get('match_type'), 'state': entity.get('state')}, str(value), 'USD', [resource])
    for business in pack['business']:
        add('observe:' + business['evidence_id'], 'observe', business['sku'] + ' · 经营指标观察',
            {'sku': business['sku']}, None, 'metrics', [business['evidence_id']])
    return entries


def object_label(row):
    dim = row['dimensions']
    if dim.get('customer_search_term'):
        return dim['customer_search_term']
    if dim.get('placement'):
        code = REPORT_PLACEMENTS.get(dim['placement'])
        return PLACEMENTS.get(PLACEMENT_CODES.get(code), dim['placement'])
    raw = dim.get('keyword_text') or dim.get('targeting_expression') or dim.get('keyword_id')
    # 只翻译自动投放对象，不替换手动关键词或自定义活动名称。
    if row.get('campaign_type') == 'AUTO':
        return auto_label(raw) or raw or row.get('campaign_name')
    return raw or row.get('campaign_name')
