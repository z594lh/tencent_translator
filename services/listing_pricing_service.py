"""Listing价格定向编辑：实时快照、显式清除、提交回执与只读核实。"""
import copy
import hashlib
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from services.shop_service import get_shop_by_id, get_sp_api_client
from services.mysql_service import get_db_connection
from services import listing_history_service as history

FIELDS = {'standard_price': 'our_price', 'sale_price': 'discounted_price',
          'minimum_price': 'minimum_seller_allowed_price', 'maximum_price': 'maximum_seller_allowed_price',
          'map_price': 'map_price'}
LABELS = {'standard_price': '普通价', 'sale_price': '促销价', 'minimum_price': '最低允许价',
          'maximum_price': '最高允许价', 'map_price': '最低广告价MAP'}
ZERO_DIGITS = {'JPY', 'KRW', 'VND', 'CLP'}
THREE_DIGITS = {'BHD', 'KWD', 'OMR', 'TND', 'IQD', 'LYD'}


class PricingError(ValueError):
    def __init__(self, message, code=400, details=None):
        super().__init__(message)
        self.code, self.details = code, details or {}


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str).encode()).hexdigest()


def instant(value):
    try:
        if not isinstance(value, str): raise ValueError()
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None: raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        raise PricingError('促销时间须带时区，请重新选择起止时间') from None


def money(value, currency, label):
    digits = 0 if currency in ZERO_DIGITS else 3 if currency in THREE_DIGITS else 2
    try:
        if value is None or isinstance(value, bool): raise ValueError()
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0 or number > Decimal('99999999'):
            raise ValueError()
        if number != number.quantize(Decimal(1).scaleb(-digits)): raise ValueError()
        return number
    except (ValueError, InvalidOperation):
        raise PricingError(f'{label}须为大于0、最多{digits}位小数的有效金额') from None


def account(shop_id, sku):
    if isinstance(shop_id, bool) or not str(shop_id).isdigit() or not sku or len(sku) > 200:
        raise PricingError('店铺或SKU无效')
    shop = get_shop_by_id(int(shop_id))
    if not shop: raise PricingError('店铺不存在或已停用', 404)
    client = get_sp_api_client(int(shop_id))
    if (client.region, client.marketplace_id, client.seller_id) != (shop['region'].lower(), shop['marketplace_id'], shop['seller_id']):
        raise PricingError('店铺、区域或站点不一致，未执行价格操作', 409)
    return client, {'shop_id': int(shop_id), 'sku': sku, 'marketplace_id': client.marketplace_id,
                    'region': client.region, 'seller_id': client.seller_id,
                    'credential_group_id': shop['credential_group_id']}


def field_info(offer, attribute):
    entries = offer.get(attribute)
    if entries is None or entries == []:
        return {'value': None, 'start_at': None, 'end_at': None, 'editable': True, 'configured': False}
    simple = isinstance(entries, list) and len(entries) == 1 and isinstance(entries[0], dict) and set(entries[0]) == {'schedule'}
    schedules = entries[0].get('schedule') if simple else None
    simple = simple and isinstance(schedules, list) and len(schedules) == 1 and isinstance(schedules[0], dict)
    row = schedules[0] if simple else {}
    if simple and set(row)-{'value_with_tax', 'start_at', 'end_at'}: simple = False
    if simple and attribute == 'discounted_price' and (not row.get('start_at') or not row.get('end_at')): simple = False
    # 普通价带时间或附加条件时，不通过单值表单覆盖其完整时间计划。
    if simple and attribute != 'discounted_price' and (row.get('start_at') or row.get('end_at')):
        simple = False
    value = row.get('value_with_tax')
    try:
        valid = value is not None and Decimal(str(value)).is_finite()
    except InvalidOperation:
        valid = False
    return {'value': str(value) if value is not None else None, 'start_at': row.get('start_at'),
            'end_at': row.get('end_at'), 'editable': bool(simple and valid), 'configured': True,
            'note': '' if simple and valid else '多时段或复杂配置，本版只读，请在Amazon核对'}


def safe_issues(issues):
    if not isinstance(issues, list): return []
    return [{key: row[key] for key in ('code', 'severity', 'message', 'attributeNames') if key in row}
            for row in issues if isinstance(row, dict)][:100]


def read(client, ctx):
    """读取指定报价受众；人工编辑默认ALL，托管明确指定B2B时不得替换普通价。"""
    raw = client.get_listings_item(ctx['sku'], included_data=['summaries', 'attributes', 'issues', 'offers'])
    if not isinstance(raw, dict) or raw.get('sku') != ctx['sku']:
        raise PricingError('平台返回的SKU不一致，未执行操作', 502)
    summaries = [row for row in raw.get('summaries', []) if row.get('marketplaceId') == ctx['marketplace_id']]
    if len(summaries) != 1 or not summaries[0].get('productType'):
        raise PricingError('无法确认当前站点商品类型，未执行操作', 409)
    offers = (raw.get('attributes') or {}).get('purchasable_offer') or []
    if not isinstance(offers, list) or any(not isinstance(row, dict) for row in offers):
        raise PricingError('平台价格结构异常，未执行操作', 502)
    audience = ctx.get('audience', 'ALL')
    if audience not in ('ALL', 'B2B'): raise PricingError('报价受众无效')
    primary = [row for row in offers if row.get('marketplace_id') == ctx['marketplace_id'] and row.get('audience', 'ALL') == audience]
    if len(primary) != 1 or not primary[0].get('currency'):
        raise PricingError('当前站点没有唯一指定受众报价，不能使用其他报价代替，请在Amazon核对', 409)
    offer = primary[0]
    snapshot = {key: ctx[key] for key in ('shop_id', 'sku', 'marketplace_id', 'region', 'credential_group_id')}
    snapshot.update(currency=offer['currency'], product_type=summaries[0]['productType'],
                    fields={key: field_info(offer, attr) for key, attr in FIELDS.items()},
                    automatic_pricing=bool(offer.get('automated_pricing_merchandising_rule_plan')),
                    business_offers=[{'currency': row.get('currency'), 'price': field_info(row, 'our_price')}
                                     for row in offers if row.get('marketplace_id') == ctx['marketplace_id'] and row.get('audience') == 'B2B'],
                    effective_offers=[row for row in raw.get('offers', []) if row.get('marketplaceId') == ctx['marketplace_id']],
                    issues=safe_issues(raw.get('issues')), fetched_at=datetime.now(timezone.utc).isoformat())
    snapshot['listing_status'] = summaries[0].get('status', [])
    snapshot['offer_start_at'] = (offer.get('start_at') or {}).get('value')
    snapshot['offer_end_at'] = (offer.get('end_at') or {}).get('value')
    snapshot['version'] = digest([ctx, snapshot['product_type'], sorted(canonical(offers),
                                   key=lambda row: (row.get('marketplace_id', ''), row.get('audience', 'ALL'), row.get('currency', '')))])
    return snapshot, offers, offer


def get_pricing(shop_id, sku):
    client, ctx = account(shop_id, sku)
    snapshot, offers, _ = read(client, ctx)
    cache_pricing(ctx, snapshot, offers)
    return snapshot


def cache_pricing(ctx, snapshot, offers):
    """只更新当前SKU/站点价格缓存，保留标题等其他属性；失败不冒充平台失败。"""
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute('SELECT attributes_json FROM amazon_listings WHERE shop_id=%s AND sku=%s AND marketplace_id=%s FOR UPDATE',
                           (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            row = cursor.fetchone()
            if not row:
                snapshot['cache_warning'] = '本地尚无此Listing缓存；实时价格已读取，未创建其他商品数据'
                return
            attributes = json.loads(row['attributes_json'] or '{}')
            attributes['purchasable_offer'] = offers
            cursor.execute('UPDATE amazon_listings SET attributes_json=%s WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                           (json.dumps(attributes, ensure_ascii=False), ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
            # 现有缓存金额列为2位小数，三位币种不截断写入；完整精度仍保留在原始属性。
            if snapshot['currency'] in THREE_DIGITS:
                snapshot['cache_warning'] = '该币种完整精度仅保存在属性缓存，列表两位金额列未更新；以实时面板为准'
            else:
                cursor.execute('DELETE FROM amazon_listing_offers WHERE shop_id=%s AND sku=%s AND marketplace_id=%s',
                               (ctx['shop_id'], ctx['sku'], ctx['marketplace_id']))
                for offer in offers:
                    if offer.get('marketplace_id') != ctx['marketplace_id']: continue
                    start, end = offer.get('start_at') or {}, offer.get('end_at') or {}
                    start = instant(start['value']).replace(tzinfo=None) if start.get('value') else None
                    end = instant(end['value']).replace(tzinfo=None) if end.get('value') else None
                    cursor.execute('''INSERT INTO amazon_listing_offers
                                      (shop_id,sku,marketplace_id,currency,audience,our_price,discounted_price,start_at,end_at)
                                      VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                                   (ctx['shop_id'], ctx['sku'], ctx['marketplace_id'], offer.get('currency'), offer.get('audience', 'ALL'),
                                    field_info(offer, 'our_price')['value'], field_info(offer, 'discounted_price')['value'], start, end))
        connection.commit()
    except Exception:
        if connection is not None: connection.rollback()
        snapshot['cache_warning'] = '实时价格已读取，但本地价格缓存更新失败；请勿重复提交改价'
    finally:
        if connection is not None: connection.close()


def canonical(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if key == 'value_with_tax':
                try: item = str(Decimal(str(item)).normalize())
                except InvalidOperation: pass
            elif key in ('start_at', 'end_at') and isinstance(item, str):
                item = instant(item).isoformat()
            result[key] = canonical(item)
        return result
    if isinstance(value, list): return [canonical(item) for item in value]
    return value


def preserved(offers, ctx, changed):
    """对目标受众以外及未修改的报价字段计算摘要，避免ALL/B2B互相覆盖。"""
    result = copy.deepcopy(offers)
    for row in result:
        row.setdefault('audience', 'ALL')
        if row.get('marketplace_id') == ctx['marketplace_id'] and row.get('audience', 'ALL') == ctx.get('audience', 'ALL'):
            for key in changed: row.pop(key, None)
    return digest(sorted(canonical(result), key=lambda row: (row.get('marketplace_id', ''), row.get('audience', 'ALL'), row.get('currency', ''))))


def build_changes(snapshot, data):
    if data.get('version') != snapshot['version'] or data.get('currency') != snapshot['currency']:
        raise PricingError('Amazon价格配置已变化，请重新读取后确认；本次未提交', 409)
    actions = data.get('actions')
    if not isinstance(actions, dict) or not actions or set(actions)-set(FIELDS):
        raise PricingError('价格操作无效或包含不支持的字段')
    changes = {}
    values = {key: row['value'] for key, row in snapshot['fields'].items()}
    sale_end = snapshot['fields']['sale_price']['end_at']
    for key, action in actions.items():
        if not isinstance(action, dict) or action.get('mode') not in ('keep', 'set', 'clear'):
            raise PricingError('每项价格须明确选择保留、修改或清除')
        mode = action['mode']
        if mode == 'keep': continue
        if not snapshot['fields'][key]['editable']:
            raise PricingError(f'{LABELS[key]}是复杂配置，本版不覆盖，请在Amazon核对', 409)
        if mode == 'clear':
            if key == 'standard_price': raise PricingError('普通价不能清除')
            if snapshot['fields'][key]['configured']: changes[FIELDS[key]] = None
            values[key] = None
            if key == 'sale_price': sale_end = None
            continue
        amount = money(action.get('value'), snapshot['currency'], LABELS[key])
        schedule = {'value_with_tax': float(amount)}
        if key == 'sale_price':
            start, end = instant(action.get('start_at')), instant(action.get('end_at'))
            if start >= end or end <= datetime.now(timezone.utc): raise PricingError('促销结束须晚于开始且不能已过期')
            schedule.update(start_at=start.isoformat(), end_at=end.isoformat())
            sale_end = end.isoformat()
        old = snapshot['fields'][key]
        same = old['value'] is not None and Decimal(str(old['value'])) == amount
        if key == 'sale_price':
            same = same and old['start_at'] and old['end_at'] and instant(old['start_at']) == start and instant(old['end_at']) == end
        if not same: changes[FIELDS[key]] = [{'schedule': [schedule]}]
        values[key] = str(amount)
    if snapshot['automatic_pricing'] and any(key in changes for key in ('our_price', 'minimum_seller_allowed_price', 'maximum_seller_allowed_price')):
        raise PricingError('该SKU绑定自动调价，本版不修改普通价及上下限，请先到Amazon核对规则', 409)
    if values['standard_price'] is None: raise PricingError('缺少普通价，请先设置有效普通价')
    amounts = {key: Decimal(str(value)) if value is not None else None for key, value in values.items()}
    low, high, standard = amounts['minimum_price'], amounts['maximum_price'], amounts['standard_price']
    if low is not None and high is not None and low > high: raise PricingError('最低允许价不能高于最高允许价')
    for key in ('standard_price', 'sale_price'):
        amount = amounts[key]
        if key == 'sale_price' and (amount is None or (sale_end and instant(sale_end) <= datetime.now(timezone.utc))): continue
        if amount is None: continue
        if low is not None and amount < low: raise PricingError(f'{LABELS[key]}低于最低允许价，请明确调整上下限或价格')
        if high is not None and amount > high: raise PricingError(f'{LABELS[key]}高于最高允许价，请明确调整上下限或价格')
        if key == 'sale_price' and amount > standard: raise PricingError('有效或未来促销价不能高于普通价，请同时修改或清除促销')
    if not changes: raise PricingError('没有需要提交的价格修改')
    return changes


@contextmanager
def price_lock(ctx):
    connection = get_db_connection()
    name = 'listing_price_' + digest(ctx)[:40]
    acquired = False
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT GET_LOCK(%s,0) AS acquired', (name,))
            acquired = cursor.fetchone()['acquired'] == 1
        if not acquired: raise PricingError('该SKU价格正在提交，请稍后只读核对，不要重复提交', 409)
        yield
    finally:
        try:
            if acquired:
                with connection.cursor() as cursor: cursor.execute('SELECT RELEASE_LOCK(%s)', (name,))
        finally:
            connection.close()


def signer(client):
    # 使用当前授权派生用途隔离密钥；不传出凭证，无需新增配置或跨worker内存状态。
    key = hashlib.sha256(('listing-pricing-confirmation\0' + client.refresh_token).encode()).digest()
    return URLSafeTimedSerializer(key, salt='listing-pricing-v1')


def accepted_submission(result):
    issues = safe_issues(result.get('issues')) if isinstance(result, dict) else []
    details = {'submission_id': result.get('submissionId') if isinstance(result, dict) else None, 'issues': issues}
    if isinstance(result, dict) and result.get('erp_history_id'):
        details['history_id'] = result['erp_history_id']
    if isinstance(result, dict) and result.get('history_warning'):
        details['history_warning'] = result['history_warning']
    if not isinstance(result, dict) or result.get('status') not in ('ACCEPTED', 'INVALID'):
        raise PricingError('Amazon提交结果未知，请只读核对，不要重复提交', 502, {**details, 'write_state': 'unknown'})
    if result['status'] == 'INVALID' or any(row.get('severity') == 'ERROR' for row in issues):
        raise PricingError('Amazon未通过提交校验，请查看问题详情', 422, {**details, 'write_state': 'rejected'})
    if not details['submission_id']:
        raise PricingError('Amazon未提供提交标识，请只读核对，不要重复提交', 502, {**details, 'write_state': 'unknown'})
    return details


def submit_pricing(shop_id, sku, data):
    client, ctx = account(shop_id, sku)
    with price_lock(ctx):
        snapshot, offers, _ = read(client, ctx)
        changes = build_changes(snapshot, data)
        proof = {'context': ctx, 'changes': changes, 'preserved': preserved(offers, ctx, changes),
                 'baseline_errors': [digest(row) for row in snapshot['issues'] if row.get('severity') == 'ERROR']}
        try:
            result = client.patch_listings_item(sku, [{'op': 'merge', 'path': '/attributes/purchasable_offer',
                         'value': [{'marketplace_id': ctx['marketplace_id'], 'currency': snapshot['currency'],
                                    'audience': 'ALL', **changes}]}], product_type=snapshot['product_type'])
        except requests.RequestException as exc:
            response = getattr(exc, 'response', None)
            if response is not None and response.status_code in (400, 401, 403, 404, 422, 429):
                try: errors = response.json().get('errors', [])
                except (ValueError, AttributeError): errors = []
                issues = [{'code': str(row.get('code', ''))[:100], 'message': str(row.get('message', ''))[:1000], 'severity': 'ERROR'}
                          for row in errors if isinstance(row, dict)][:20] if isinstance(errors, list) else []
                raise PricingError('Amazon明确拒绝了本次提交，请查看问题详情', 422,
                                   {'write_state': 'rejected', 'issues': issues}) from None
            raise PricingError('提交请求未获可靠回执，结果待核实；请只读核对，不要重复提交', 502, {'write_state': 'unknown'}) from None
        submission = accepted_submission(result)
        proof['submission_id'] = submission['submission_id']
        proof['history_id'] = submission.get('history_id')
        return {'snapshot': snapshot, 'submission': submission, 'verification_token': signer(client).dumps(proof),
                'state': 'submitted', 'message': 'Amazon已接受提交，等待回读确认；不会自动重发改价'}


def verify_pricing(shop_id, sku, token):
    client, ctx = account(shop_id, sku)
    try:
        proof = signer(client).loads(token, max_age=7200)
    except (BadSignature, SignatureExpired, TypeError):
        raise PricingError('核对凭据无效或已超过2小时，请重新读取价格，不能据此重发修改', 409) from None
    if proof.get('context') != ctx: raise PricingError('核对凭据不属于当前店铺或SKU', 409)
    snapshot, offers, offer = read(client, ctx)
    changes = proof['changes']
    matches = {key: (offer.get(key) in (None, []) if value is None else canonical(offer.get(key)) == canonical(value))
               for key, value in changes.items()}
    errors = [row for row in snapshot['issues'] if row.get('severity') == 'ERROR' and
              (digest(row) not in proof['baseline_errors'] or
               bool(set(row.get('attributeNames') or []) & ({'purchasable_offer'} | set(changes))))]
    state, message = 'pending', 'Amazon已接受提交，价格属性尚未全部回读确认；请稍后核对，不要重复提交'
    if errors:
        state, message = 'issues', 'Amazon返回新的Listing问题，请先核对问题详情，不要重复提交'
    elif preserved(offers, ctx, changes) != proof['preserved']:
        state, message = 'conflict', '未修改的价格配置也发生变化，可能存在外部并发修改，请人工核对'
    elif all(matches.values()):
        state, message = 'attributes_confirmed', '提交的价格属性已回读确认；实际报价可能仍受促销或平台处理影响'
        expected = snapshot['fields']['standard_price']['value']
        sale = snapshot['fields']['sale_price']
        now = datetime.now(timezone.utc)
        if sale['value'] and sale['start_at'] and sale['end_at'] and instant(sale['start_at']) <= now < instant(sale['end_at']):
            expected = sale['value']
        actual = [row.get('price', {}).get('amount') for row in snapshot['effective_offers']
                  if row.get('offerType') == 'B2C' and row.get('price', {}).get('currency', row.get('price', {}).get('currencyCode')) == snapshot['currency']]
        if expected is not None and len(actual) == 1 and Decimal(str(actual[0])) == Decimal(str(expected)):
            state, message = 'effective_confirmed', '价格属性及当前实际报价已回读确认；促销和企业价仍按各自配置生效'
    result = {'state': state, 'message': message, 'snapshot': snapshot, 'matched_fields': matches,
              'submission_id': proof['submission_id'], 'issues': errors}
    history.confirm(ctx, result, proof.get('history_id'))
    cache_pricing(ctx, snapshot, offers)
    return result
