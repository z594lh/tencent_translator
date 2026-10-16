"""SP创建预检与输入规范化；不发送广告写请求。"""
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from services import ads_creation_repository as repo
from services import ads_negative_service as negatives
from services.shop_service import get_shop_by_id, get_ads_api_client

Error = repo.CreationError
AUTO_TYPES = ('QUERY_HIGH_REL_MATCHES', 'QUERY_BROAD_REL_MATCHES', 'ASIN_SUBSTITUTE_RELATED', 'ASIN_ACCESSORY_RELATED')
STRATEGIES = ('LEGACY_FOR_SALES', 'AUTO_FOR_SALES', 'MANUAL')


def identifier(value, label):
    try:
        return str(negatives.positive_id(value, label))
    except negatives.NegativeError as exc:
        raise Error(str(exc), exc.code) from None


def text(value, label, limit=128):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > limit or any(ord(x)<32 for x in value):
        raise Error(f'{label}须为1～{limit}字符，不能包含控制字符')
    return value.strip()


def money(value, label, currency, nullable=False):
    if nullable and value in (None, ''):
        return None
    try:
        if isinstance(value, bool): raise ValueError()
        number = Decimal(str(value))
        digits = 0 if currency == 'JPY' else 2
        if not number.is_finite() or not 0 < number <= Decimal('100000000') or number != number.quantize(Decimal(10)**-digits):
            raise ValueError()
        return float(number)
    except (InvalidOperation, ValueError, TypeError):
        raise Error(f'{label}须为正数，最多{0 if currency == "JPY" else 2}位小数；站点上下限由Amazon进一步校验') from None


def account(shop_id):
    shop_id = int(identifier(shop_id, '店铺ID'))
    shop = get_shop_by_id(shop_id)
    if not shop or not shop.get('ads_profile_id') or not shop.get('ads_refresh_token') or not shop.get('seller_id'):
        raise Error('所选店铺未配置卖家身份或广告授权，不能创建', 400)
    return {k: shop.get(k) for k in ('credential_group_id', 'marketplace_id', 'region', 'seller_id')} | {
        'shop_id': shop_id, 'profile_id': str(shop['ads_profile_id']), 'shop_name': shop['shop_name']}


def client_for(acct):
    client = get_ads_api_client(acct['shop_id'])
    if str(client.profile_id) != acct['profile_id'] or client.region.lower() != acct['region'].lower():
        raise Error('广告账户身份变更，未发送操作', 409)
    return client


def pages(method, key, **filters):
    result, token, seen = [], None, set()
    # 创建回读不无限翻页，也不在429后立即重试。
    for _ in range(100):
        response = method(max_results=100, next_token=token, **filters)
        if not isinstance(response, dict) or not isinstance(response.get(key), list):
            raise Error('Amazon列表结构异常，未确认创建结果', 502)
        result.extend(response[key])
        token = response.get('nextToken')
        if not token:
            return result
        if token in seen:
            raise Error('Amazon分页游标重复，未确认创建结果', 502)
        seen.add(token)
    raise Error('回读分页超过安全上限，未确认创建结果', 502)


def parent(acct, campaign_id):
    campaign_id = identifier(campaign_id, '活动ID')
    try:
        negatives.context(acct['shop_id'], campaign_id, scope='campaign')
    except negatives.NegativeError as exc:
        raise Error(str(exc), exc.code) from None
    with repo.transaction() as c:
        c.execute('SELECT * FROM amazon_ads_campaigns WHERE shop_id=%s AND profile_id=%s AND campaign_id=%s',
                  (acct['shop_id'], acct['profile_id'], campaign_id))
        row = c.fetchone()
    if not row or row['state'].upper() == 'ARCHIVED' or row['targeting_type'].upper() not in ('AUTO', 'MANUAL'):
        raise Error('父活动已归档或类型未知，请先同步核实')
    return {**row, 'campaign_id': campaign_id, 'bidding': repo.loads(row.get('bidding'))}


def metadata(shop_id, campaign_id=None):
    acct = account(shop_id)
    profiles = client_for(acct).list_profiles()
    if not isinstance(profiles, list):
        raise Error('无法读取广告账户币种和时区', 502)
    profile = next((p for p in profiles if str(p.get('profileId')) == acct['profile_id']), None)
    if not profile or not re.fullmatch('[A-Z]{3}', str(profile.get('currencyCode', ''))):
        raise Error('未找到当前广告Profile及币种，不能创建', 502)
    info = profile.get('accountInfo') or {}
    if info.get('type', '').lower() != 'seller':
        raise Error('本版创建仅支持卖家账户，不支持Vendor或Author')
    if str(info.get('marketplaceStringId') or '') != str(acct['marketplace_id']) or str(info.get('id') or '') != str(acct['seller_id']):
        raise Error('广告Profile的真实站点或卖家身份与店铺不符，未发送创建',409)
    zone = profile.get('timezone')
    try:
        ZoneInfo(zone)
    except (ValueError, TypeError, KeyError):
        raise Error('广告账户时区未知，不能猜测开始日期', 502) from None
    return {**acct, 'currency': profile['currencyCode'], 'timezone': zone,
            'country': profile.get('countryCode'), 'today': datetime.now(ZoneInfo(zone)).strftime('%Y-%m-%d'),
            'campaign': parent(acct, campaign_id) if campaign_id else None,
            'eligibility_note': '本地商品信息不等于广告资格；创建及回读结果为准，暂停不代表可投放'}


def products(acct, search='', page=1):
    try:
        page = int(page)
        if not 1 <= page <= 10000: raise ValueError()
    except (ValueError, TypeError):
        raise Error('商品页码无效') from None
    search = str(search or '')[:100].replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    where = 'shop_id=%s AND marketplace_id=%s AND seller_id=%s AND is_deleted=0'
    args = [acct['shop_id'], acct['marketplace_id'], acct['seller_id']]
    if search:
        where += ' AND (sku LIKE %s OR asin LIKE %s OR item_name LIKE %s)'
        args += ['%'+search+'%']*3
    with repo.transaction() as c:
        c.execute('SELECT COUNT(*) n FROM amazon_listings WHERE '+where, args)
        total = c.fetchone()['n']
        c.execute('''SELECT sku,asin,item_name,main_image_url,list_price,list_price_currency,status,
                     parentage_level,sync_time FROM amazon_listings WHERE '''+where+' ORDER BY sku LIMIT 20 OFFSET %s', args+[(page-1)*20])
        rows = c.fetchall()
    return {'list': rows, 'total': total, 'page': page}


def normalize(body, meta):
    if not isinstance(body, dict): raise Error('创建参数须为JSON对象')
    campaign = meta.get('campaign')
    kind = str(body.get('targeting_type') or '').upper()
    if kind not in ('AUTO', 'MANUAL') or campaign and kind != campaign['targeting_type'].upper():
        raise Error('广告组投放类型必须继承父活动，自动活动不能创建手动组')
    mode = str(body.get('mode') or '').upper()
    if mode not in (('AUTO',) if kind == 'AUTO' else ('KEYWORDS', 'PRODUCTS')):
        raise Error('投放模式与活动类型不符')
    currency = meta['currency']
    group = {'name': text(body.get('group_name'), '广告组名称'),
             'defaultBid': money(body.get('default_bid'), '默认竞价', currency)}
    selected = body.get('products')
    if not isinstance(selected, list) or not 1 <= len(selected) <= 100:
        raise Error('请选择1～100个本店商品')
    product_rows, seen = [], set()
    with repo.transaction() as c:
        for item in selected:
            if not isinstance(item, dict): raise Error('商品参数无效')
            sku = text(item.get('sku'), 'SKU（当前广告实体表上限）', 50)
            asin = str(item.get('asin') or '').upper()
            if sku in seen or not re.fullmatch('[A-Z0-9]{10}', asin): raise Error('商品重复或ASIN无效')
            seen.add(sku)
            c.execute('''SELECT sku,asin,parentage_level FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s
                         AND seller_id=%s AND sku=%s AND is_deleted=0''',
                      (meta['shop_id'], meta['marketplace_id'], meta['seller_id'], sku))
            rows = c.fetchall()
            if len(rows) != 1 or rows[0]['asin'] != asin or str(rows[0]['parentage_level'] or '').lower() == 'parent':
                raise Error('选品不属于当前店铺、是父体或资料有歧义，请重新选择')
            product_rows.append({'sku': sku, 'asin': asin, 'state': 'ENABLED'})
    output = {'targeting_type': kind, 'mode': mode, 'group': group, 'products': product_rows,
              'currency': currency, 'timezone': meta['timezone'], 'launch': body.get('launch', False)}
    if not isinstance(output['launch'], bool): raise Error('启动选择须为布尔值')
    if campaign:
        output['campaign_id'] = campaign['campaign_id']
    else:
        data = body.get('campaign')
        if not isinstance(data, dict): raise Error('缺少活动设置')
        strategy = data.get('strategy')
        if strategy not in STRATEGIES: raise Error('不支持的竞价策略')
        start, end = data.get('start_date') or meta['today'], data.get('end_date') or None
        try:
            for day in (start, end):
                if day and datetime.strptime(day, '%Y-%m-%d').strftime('%Y-%m-%d') != day: raise ValueError()
            if start < meta['today'] or end and end < start: raise ValueError()
        except (ValueError, TypeError):
            raise Error('日期无效；按账户时区，开始日期不能早于今天，结束不能早于开始') from None
        adjustments = []
        for field, placement in [('top', 'PLACEMENT_TOP'), ('rest', 'PLACEMENT_REST_OF_SEARCH'), ('product', 'PLACEMENT_PRODUCT_PAGE')]:
            val = (data.get('placements') or {}).get(field, 0)
            if isinstance(val, bool) or not isinstance(val, int) or not 0 <= val <= 900: raise Error('广告位调整须为0～900整数百分比')
            adjustments.append({'placement': placement, 'percentage': val})
        output['campaign'] = {'name': text(data.get('name'), '活动名称'), 'state': 'PAUSED', 'targetingType': kind,
                              'budget': {'budgetType': 'DAILY', 'budget': money(data.get('budget'), '日预算', currency)},
                              'startDate': start, 'dynamicBidding': {'strategy': strategy, 'placementBidding': adjustments}}
        if end: output['campaign']['endDate'] = end
        if data.get('portfolio_id'):
            portfolio = identifier(data['portfolio_id'], '广告组合ID')
            rows = client_for(meta).list_portfolios()
            if not isinstance(rows, list) or not any(str(x.get('portfolioId')) == portfolio and str(x.get('state','')).upper() != 'ARCHIVED' for x in rows):
                raise Error('广告组合不属于当前账户或已归档')
            output['campaign']['portfolioId'] = portfolio
    targets = body.get('targets')
    if not isinstance(targets, list) or not 1 <= len(targets) <= 100: raise Error('请配置1～100条投放')
    normalized, seen = [], set()
    for item in targets:
        if not isinstance(item, dict): raise Error('投放参数无效')
        if mode == 'AUTO':
            target = item.get('type')
            state = item.get('state')
            if target not in AUTO_TYPES or state not in ('ENABLED', 'PAUSED'): raise Error('自动投放类型或状态无效')
            row = {'type': target, 'state': state, 'bid': money(item.get('bid'), '分类竞价', currency, nullable=True)}
            key = target
        elif mode == 'KEYWORDS':
            phrase = text(item.get('keywordText'), '关键词', 80)
            match = item.get('matchType')
            if match not in ('BROAD', 'PHRASE', 'EXACT') or len(phrase.split()) > 10: raise Error('关键词匹配类型或词数无效')
            row = {'keywordText': phrase, 'matchType': match, 'bid': money(item.get('bid'), '关键词竞价', currency), 'state': 'ENABLED'}
            key = (phrase.casefold(), match)
        else:
            asin = str(item.get('asin') or '').strip().upper()
            if not re.fullmatch('[A-Z0-9]{10}', asin): raise Error('目标ASIN须为10位字母数字')
            row = {'expression': [{'type': 'ASIN_SAME_AS', 'value': asin}], 'expressionType': 'MANUAL',
                   'bid': money(item.get('bid'), '商品投放竞价', currency), 'state': 'ENABLED'}
            key = asin
        if key in seen: raise Error('存在重复投放，请先去重')
        seen.add(key); normalized.append(row)
    if mode == 'AUTO' and (seen != set(AUTO_TYPES) or all(x['state']=='PAUSED' for x in normalized)):
        raise Error('自动组须完整配置四类投放，且至少开启一类')
    output['targets'] = normalized
    scope = body.get('negative_scope') or 'group'
    if scope not in ('group', 'campaign') or campaign and scope != 'group':
        raise Error('已有活动新增组仅允许组级否定，不能改变整个活动的否定规则')
    if scope == 'campaign' and kind != 'AUTO' and body.get('negative_products'):
        raise Error('活动级否定商品仅支持自动活动')
    output['negative_scope'] = scope
    # 复用已有否定输入规则，使用占位父级，仅用于校验；执行时替换已核实ID。
    fake = {'campaign_id': 1, 'ad_group_id': 2 if scope=='group' else None, 'scope':scope, 'campaign': {'state':'PAUSED','targeting_type':kind}}
    for key, family in [('negative_keywords','keywords'),('negative_products','targets')]:
        rows = body.get(key) or []
        if not isinstance(rows,list): raise Error('否定规则须为列表')
        if rows:
            parents = {'campaignId':'1', **({'adGroupId':'2'} if scope=='group' else {})}
            try: prepared = negatives.prepare_items(family, fake, [{**x, **parents} for x in rows])
            except (negatives.NegativeError, TypeError) as exc: raise Error(str(exc)) from None
            output[key] = [{k:v for k,v in x.items() if k not in ('campaignId','adGroupId')} for x in prepared]
        else: output[key] = []
    return output
