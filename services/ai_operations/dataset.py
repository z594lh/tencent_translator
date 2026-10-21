"""主干模型资料：完整Listing/售价、紧凑指标矩阵、分层对象和关键词历史。"""
from . import catalog
from .metrics import ADS_FIELDS, SKU_FIELDS, digest, dumps
from services.listing_performance.common import DomainError

# 给完整文案与多项历史基线留空间；按官方模型上下文保守控制，超限明确停止而非静默删历史。
MAX_INPUT_BYTES = 800000
PERIODS = ('d1', 'd3', 'd7', 'p7', 'd14', 'p14', 'd30', 'p30')


def matrix(periods, fields):
    lookup = {r['key']: r for r in periods}
    return {'columns': ['window', *fields],
            'rows': [[key, *[lookup[key]['metrics'].get(field) for field in fields]] for key in PERIODS if key in lookup],
            'coverage': {key: lookup[key].get('coverage') for key in PERIODS if key in lookup and 'coverage' in lookup[key]}}


def build(pack):
    actions, index = catalog.build(pack), {}
    result = {key: pack[key] for key in ('identity', 'time', 'notes', 'gaps', 'relationships', 'inventory', 'offers',
                                       'ads_reporting_policy', 'profit_policy', 'pricing')}
    result.update(schema_version='ai_operations_dataset_v2', listing=[], business=[], ads=[],
                  current_settings=pack['current_entities'], recent_operations=pack['recent_operations'],
                  action_catalog=actions, keywords={})
    for product in pack['products']:
        ref = 'listing:' + digest(product['sku'])[:16]
        row = {k: product.get(k) for k in ('sku', 'asin', 'product_name', 'brand', 'product_type', 'status', 'sync_time')}
        row['fields'] = {field: catalog.field_values(product, field, pack['identity']['marketplace_id']) for field in catalog.TEXT_FIELDS}
        row['pricing_attributes'] = product.get('pricing_attributes', {})
        result['listing'].append(row)
        index[ref] = {'label': (product.get('product_name') or product['sku']) + ' · 当前Listing', 'kind': 'listing', 'data': row}
    for pricing in pack['pricing']:
        ref = 'pricing:' + digest(pricing['sku'])[:16]
        index[ref] = {'label': pricing['sku'] + ' · 计算售价', 'kind': 'pricing', 'data': pricing}
    for key, rows, field in (('campaign', pack['current_entities']['campaigns'], 'campaign_id'),
                             ('group', pack['current_entities']['groups'], 'ad_group_id'),
                             ('target', pack['current_entities']['targets'], 'target_id'),
                             ('keyword', pack['current_entities']['keywords'], 'keyword_id')):
        for row in rows:
            ref = key + ':' + str(row[field])
            name = row.get('name') or row.get('keyword_text') or row.get('resolved_expression') or str(row[field])
            label = catalog.auto_label(name) if key == 'target' else None
            index[ref] = {'label': str(label or name), 'kind': 'setting', 'data': row}
    for business in pack['business']:
        row = {'sku': business['sku'], 'evidence_id': business['evidence_id'],
               'identity_ambiguous': business['identity_ambiguous'], 'periods': matrix(business['periods'], (*SKU_FIELDS,'acos','tacos','profit_margin')),
               'daily': {'columns': ['date', 'present', *SKU_FIELDS],
                         'rows': [[r['date'], r['present'], *[r['metrics'].get(f) for f in SKU_FIELDS]] for r in business['daily']]}}
        result['business'].append(row)
        index[business['evidence_id']] = {'label': business['sku'] + ' · SKU经营', 'kind': 'business', 'data': row}
    for module in pack['ads']:
        row = {k: module[k] for k in ('kind', 'label', 'scope', 'currency', 'total_objects', 'selected_objects', 'omitted_objects')}
        row['periods'] = matrix(module['periods'], (*ADS_FIELDS,'acos','ctr','cpc'))
        row['details'] = []
        for detail in module['details']:
            selected = {'evidence_id': detail['evidence_id'], 'label': catalog.object_label(detail),
                        'dimensions': {k: v for k, v in detail['dimensions'].items() if v is not None},
                        'campaign_type': detail['campaign_type'], 'campaign_name': detail['campaign_name'],
                        'periods': matrix(detail['periods'], (*ADS_FIELDS,'acos','ctr','cpc'))}
            if detail.get('daily'):
                selected['daily'] = {'columns': ['date', *ADS_FIELDS],
                    'rows': [[day['date'], *[day['metrics'].get(f) for f in ADS_FIELDS]] for day in detail['daily']]}
            row['details'].append(selected)
            index[detail['evidence_id']] = {'label': f"{module['label']} · {detail.get('campaign_name')} · {selected['label']}",
                                           'kind': 'ads', 'data': selected}
        result['ads'].append(row)
    words = pack['keywords']
    result['keywords'] = {key: words[key] for key in ('snapshot', 'total', 'selected', 'omitted', 'note', 'items')}
    # 历史只给本轮实际提供的完整词组；新增/丢失情况由原文件保留，不能伪造0。
    provided = {r['keyword'] for r in words['items']}
    history_fields = ('keyword', 'traffic_share', 'organic_share', 'sponsored_share', 'weekly_impressions',
                      'organic_rank', 'organic_updated_text', 'sponsored_rank', 'sponsored_updated_text')
    result['keywords']['history'] = [{'snapshot': h['snapshot'], 'total': len(h['items']),
                                     'columns': list(history_fields),
                                     'rows': [[r.get(key) for key in history_fields] for r in h['items'] if r['keyword'] in provided]}
                                    for h in words.get('history', [])]
    for word in words['items']:
        index[word['evidence_id']] = {'label': '卖家精灵 · ' + word['keyword'], 'kind': 'keyword', 'data': word}
    # 索引不重复塞入data；模型资料已有原对象，HTTP详情则可展开这些同次读取的依据。
    result['evidence_index'] = {key: {k: v for k, v in row.items() if k != 'data'} for key, row in index.items()}
    raw = dumps(result)
    size = len(raw.encode('utf8'))
    if size > MAX_INPUT_BYTES:
        raise DomainError('本ASIN完整模型资料超过800KB入口上限，需按广告组拆分；未压成少量词或调用AI', 422)
    return {'content': result, 'content_hash': digest(result), 'size_bytes': size,
            'evidence': index, 'catalog': actions, 'schema_version': 'ai_operations_dataset_v2'}
