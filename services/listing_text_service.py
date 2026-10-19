"""Listing文字定向编辑：保留站点与语言，只读回查，不乐观伪造平台结果。"""
import copy
import json
import requests

from services import listing_pricing_service as pricing
from services import listing_history_service as history

FIELDS = {'item_name': '商品标题', 'title_differentiation': '商品亮点',
          'product_description': '商品描述', 'bullet_point': '卖点', 'generic_keyword': '搜索关键字'}


def current_rows(raw, marketplace, field):
    rows = (raw.get('attributes') or {}).get(field) or []
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise pricing.PricingError(f'{FIELDS[field]}的属性结构异常，不能编辑', 409)
    return [copy.deepcopy(row) for row in rows if row.get('marketplace_id') == marketplace]


def read(client, ctx):
    raw = client.get_listings_item(ctx['sku'], included_data=['summaries', 'attributes', 'issues'])
    if not isinstance(raw, dict) or raw.get('sku') != ctx['sku']:
        raise pricing.PricingError('平台返回的SKU不一致，未执行修改', 502)
    summaries = [row for row in raw.get('summaries', []) if row.get('marketplaceId') == ctx['marketplace_id']]
    if len(summaries) != 1 or not summaries[0].get('productType'):
        raise pricing.PricingError('无法确认当前站点商品类型，未执行修改', 409)
    attrs = raw.get('attributes') or {}
    fields = {}
    default_language = next((row.get('language_tag') for row in current_rows(raw, ctx['marketplace_id'], 'item_name')
                             if row.get('language_tag')), None)
    for name, label in FIELDS.items():
        rows = current_rows(raw, ctx['marketplace_id'], name)
        all_rows = attrs.get(name) or []
        editable = all(isinstance(row.get('value'), str) and row.get('language_tag') and
                       not (set(row) - {'value', 'language_tag', 'marketplace_id'}) for row in rows)
        # 缺失站点的旧结构不可猜测归属，避免把其他报价或语言当作当前值覆盖。
        editable = bool(editable and default_language and not any(not row.get('marketplace_id') for row in all_rows))
        fields[name] = {'label': label, 'rows': rows, 'editable': editable,
                        'note': '' if editable else '复杂属性或语言/站点不明确，本版只读，请在Amazon核对'}
    snapshot = {**ctx, 'product_type': summaries[0]['productType'], 'fields': fields,
                'default_language': default_language, 'issues': pricing.safe_issues(raw.get('issues')),
                'listing_status': summaries[0].get('status', [])}
    snapshot['version'] = pricing.digest([ctx, snapshot['product_type'], {key: attrs.get(key) for key in FIELDS}])
    return snapshot, raw


def get_editor(shop_id, sku):
    client, ctx = pricing.account(shop_id, sku)
    snapshot, _ = read(client, ctx)
    return snapshot


def cache_text(ctx, raw):
    """仅投影本次实时文字；不借回显重建报价、图片或产品数据。"""
    connection = pricing.get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT attributes_json FROM amazon_listings WHERE shop_id=%s AND sku=%s AND marketplace_id=%s FOR UPDATE',
                           (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            row = cursor.fetchone()
            if not row:
                return
            attrs = json.loads(row['attributes_json'] or '{}')
            for name in FIELDS:
                if name in raw['attributes']:
                    attrs[name] = raw['attributes'][name]
                else:
                    attrs.pop(name, None)
            value = lambda name: next((item['value'] for item in current_rows(raw, ctx['marketplace_id'], name)), None)
            cursor.execute('UPDATE amazon_listings SET attributes_json=%s,item_name=%s,title_differentiation=%s,product_description=%s WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                           (json.dumps(attrs, ensure_ascii=False), value('item_name'), value('title_differentiation'), value('product_description'),
                            ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            cursor.execute('DELETE FROM amazon_listing_bullets WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                           (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            for index, item in enumerate(current_rows(raw, ctx['marketplace_id'], 'bullet_point')):
                cursor.execute('INSERT INTO amazon_listing_bullets (shop_id,sku,marketplace_id,sort_order,content,language_tag) VALUES (%s,%s,%s,%s,%s,%s)',
                               (ctx['shop_id'], ctx['sku'], ctx['marketplace_id'], index, item['value'], item['language_tag']))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def build_patches(snapshot, data):
    if data.get('version') != snapshot['version']:
        raise pricing.PricingError('平台文字已变化，请重新读取后编辑，未提交本次修改', 409)
    changes = data.get('changes')
    if not isinstance(changes, dict) or not changes or set(changes) - set(FIELDS):
        raise pricing.PricingError('仅允许修改标题、亮点、描述、卖点及搜索词')
    patches, expected = [], {}
    for field, action in changes.items():
        info = snapshot['fields'][field]
        if not info['editable']:
            raise pricing.PricingError(f'{FIELDS[field]}当前为只读，未提交修改', 409)
        if not isinstance(action, dict) or action.get('mode') not in ('set', 'clear'):
            raise pricing.PricingError('修改方式无效')
        if action['mode'] == 'clear':
            if field == 'item_name':
                raise pricing.PricingError('商品标题不可清空')
            if not info['rows']:
                continue
            patches.append({'op': 'delete', 'path': '/attributes/' + field, 'value': info['rows']})
            expected[field] = []
            continue
        rows = action.get('rows')
        if not isinstance(rows, list) or not 1 <= len(rows) <= 100:
            raise pricing.PricingError(f'{FIELDS[field]}至少填写一条内容，清除须显式选择')
        for row in rows:
            if (not isinstance(row, dict) or set(row) != {'value', 'language_tag', 'marketplace_id'} or
                    row['marketplace_id'] != snapshot['marketplace_id'] or
                    not isinstance(row['value'], str) or not row['value'].strip() or len(row['value']) > 20000 or
                    not isinstance(row['language_tag'], str) or not 2 <= len(row['language_tag']) <= 20):
                raise pricing.PricingError(f'{FIELDS[field]}内容、语言或站点无效')
            allowed_languages = {item['language_tag'] for item in info['rows']} | {snapshot['default_language']}
            if row['language_tag'] not in allowed_languages:
                raise pricing.PricingError('语言标签不是翻译功能。请保留原语言或当前站点原有标题语言，避免平台新增无效语言实例')
        # 非卖点字段同语言仅能有一项；多语言实例可显式保留，不强制改成英语。
        if field != 'bullet_point' and len({row['language_tag'] for row in rows}) != len(rows):
            raise pricing.PricingError(f'{FIELDS[field]}存在重复语言')
        if rows == info['rows']:
            continue
        # replace按语言选择器替换；只换language_tag会新增实例，须显式删除已移除的旧语言。
        removed_languages = {row['language_tag'] for row in info['rows']} - {row['language_tag'] for row in rows}
        removed_rows = [row for row in info['rows'] if row['language_tag'] in removed_languages]
        if removed_rows:
            patches.append({'op': 'delete', 'path': '/attributes/' + field, 'value': removed_rows})
        patches.append({'op': 'replace', 'path': '/attributes/' + field, 'value': rows})
        expected[field] = rows
    if not patches:
        raise pricing.PricingError('没有实际修改，无需提交')
    return patches, expected


def sign(client):
    # 与价格凭据使用不同salt，不能跨接口核对或传递写请求。
    signer = pricing.signer(client)
    signer.salt = b'listing-text-v1'
    return signer


def submit_editor(shop_id, sku, data):
    client, ctx = pricing.account(shop_id, sku)
    with pricing.price_lock({**ctx, 'purpose': 'text'}):
        snapshot, raw = read(client, ctx)
        patches, expected = build_patches(snapshot, data)
        proof = {'context': ctx, 'expected': expected, 'submission_id': None,
                 'baseline_errors': [pricing.digest(row) for row in snapshot['issues'] if row.get('severity') == 'ERROR']}
        try:
            result = client.patch_listings_item(sku, patches, product_type=snapshot['product_type'])
        except requests.RequestException as exc:
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code in (400, 401, 403, 404, 413, 415, 422, 429):
                try:
                    errors = response.json().get('errors', [])
                except (ValueError, AttributeError):
                    errors = []
                issues = [{'code': str(row.get('code', ''))[:100], 'message': str(row.get('message', ''))[:1500], 'severity': 'ERROR'}
                          for row in errors if isinstance(row, dict)][:20] if isinstance(errors, list) else []
                raise pricing.PricingError('Amazon明确拒绝提交，请核对问题详情', 422,
                                           {'write_state': 'rejected', 'issues': issues}) from None
            raise pricing.PricingError('未收到可靠回执，结果待核实，请只读核对，不要重复提交', 502,
                                       {'write_state': 'unknown'}) from None
        submission = pricing.accepted_submission(result)
        proof['submission_id'] = submission['submission_id']
        proof['history_id'] = submission.get('history_id')
        return {'state': 'submitted', 'message': 'Amazon已接受提交，等待文字属性回读确认；未确认商城展示',
                'submission': submission, 'verification_token': sign(client).dumps(proof)}


def verify_editor(shop_id, sku, token):
    client, ctx = pricing.account(shop_id, sku)
    try:
        proof = sign(client).loads(token, max_age=7200)
    except (pricing.BadSignature, pricing.SignatureExpired, TypeError):
        raise pricing.PricingError('核对凭据无效或已超过2小时，请重新读取，不能据此重发修改', 409) from None
    if proof.get('context') != ctx:
        raise pricing.PricingError('核对凭据不属于当前店铺或SKU', 409)
    snapshot, raw = read(client, ctx)
    # 平台可按语言分组返回；保留每种语言内的卖点顺序，不因组间顺序误报未生效。
    normalized = lambda rows: sorted(rows, key=lambda row: (row['marketplace_id'], row['language_tag']))
    matches = {name: normalized(snapshot['fields'][name]['rows']) == normalized(rows) for name, rows in proof['expected'].items()}
    errors = [row for row in snapshot['issues'] if row.get('severity') == 'ERROR' and
              (pricing.digest(row) not in proof['baseline_errors'] or
               set(row.get('attributeNames') or []) & set(proof['expected']))]
    state = 'issues' if errors else 'attributes_confirmed' if all(matches.values()) else 'pending'
    messages = {'issues': 'Amazon返回相关处理问题，请核对详情，不要重复提交',
                'attributes_confirmed': '修改的文字属性已回读确认；商城展示仍由Amazon决定，未确认上架或恢复可售',
                'pending': 'Amazon已接受提交，文字属性尚未全部回读确认，请稍后核对，不要重复提交'}
    result = {'state': state, 'message': messages[state], 'snapshot': snapshot, 'matched_fields': matches,
              'submission_id': proof['submission_id'], 'issues': errors}
    history.confirm(ctx, result, proof.get('history_id'))
    return result, raw, ctx
