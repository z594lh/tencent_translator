"""Listing文案统一规范（2026-10-22核对）：技术Schema可接受不等于符合商城内容规范。

Amazon 2026-07-27非媒体类标题75字符、Item Highlights125字符；旧200字符不能覆盖新规范。
FOOD_SLICER实测PTD仍给标题200、五点550、描述20000字节，故取官方内容规范/ERP保守线与PTD交集。
人工编辑、AI方案及最终PATCH共用本模块；不截断/改写用户文案，不追溯改变历史。
来源与例外见policy()['sources']；字数是Unicode码点（含空格），搜索词安全线为完整原文UTF8字节。
"""
import copy
import re
import threading
import time
import unicodedata
from collections import Counter
from urllib.parse import urlparse

import requests

VERSION = 'amazon-listing-copy-2026-10-22-v1'
FIELDS = {'item_name': '商品标题', 'title_differentiation': '商品亮点',
          'bullet_point': '五点描述', 'product_description': '商品描述', 'generic_keyword': '后台搜索词'}
SOURCES = [
    {'title': '2026标题及Item Highlights更新', 'url': 'https://sellercentral.amazon.com/seller-forums/discussions/t/145b6d0f-999c-4555-896c-c694bda2e470'},
    {'title': '标题及亮点官方答疑', 'url': 'https://sellercentral.amazon.com/seller-forums/discussions/t/302aaac6-6f2b-4d86-bcd2-36fe24f0e6cd'},
    {'title': '五点内容规范更新', 'url': 'https://sellercentral.amazon.com/seller-forums/discussions/t/65f8e647-977d-49ac-9036-2049b96720b2'},
    {'title': '五点长度及格式官方说明', 'url': 'https://sellercentral.amazon.com/seller-forums/discussions/t/41f1c90c-f05b-4b7f-ad7a-25f499ceeb7d'},
    {'title': '后台搜索词使用指南', 'url': 'https://sellercentral.amazon.com/help/hub/reference/external/G23501?local=en-US'},
]
STOP_WORDS = set('a an the and or but nor for so yet as at by in of on to up via with from into onto over under about after before between through during without within than per is are'.split())
TITLE_SYMBOLS = '!$?_{}^¬¦'
BULLET_SYMBOLS = '™®©€…†‡¢£¥±~'
RESTRICTED = r'\b(?:best\s*seller|best-selling|free\s+shipping|full\s+refund|money[ -]back\s+guarantee|unconditional\s+guarantee)\b'
CACHE, CACHE_LOCK = {}, threading.Lock()


class ContentError(ValueError):
    """确定的本地规范拒绝；未发送Amazon，可安全返回给用户或AI纠错。"""
    code = 422
    details = {'write_state': 'not_sent'}


def policy(marketplace='', product_type='', brand=''):
    """构造可下发前端和AI的同源基线，明确质量规范与保守上限，不冒充类目完整Schema。"""
    byte_limit = {'A21TJRUUN4KGV': 199, 'A1VC38T7YXB528': 499}.get(marketplace, 249)
    rules = {
        'item_name': {'max_chars': 75, 'min_chars': 1, 'min_items': 1, 'max_items': 1,
                      'help': '含空格最多75字符；不得堆词，同一实词最多两次；品牌内特殊符号例外'},
        'title_differentiation': {'max_chars': 125, 'min_chars': 1, 'min_items': 1, 'max_items': 1,
                                 'help': '最多125字符；用简短特点短语补充标题，不重复标题；先确保标题不超过75字符'},
        'bullet_point': {'max_chars': 254, 'min_chars': 11, 'min_items': 3, 'max_items': 5,
                         'help': 'ERP常用五点规范：每种语言3～5条，每条11～254字符；每条写独立特点，避免全大写、emoji及退款保证'},
        'product_description': {'max_chars': 2000, 'min_chars': 1, 'min_items': 1, 'max_items': 1,
                                'help': 'ERP常见类目保守线2000字符；纯文本描述，不写促销、联系方式或HTML'},
        'generic_keyword': {'max_utf8_bytes': byte_limit, 'min_chars': 1, 'min_items': 1, 'max_items': 1,
                            'help': f'完整原文UTF8最多{byte_limit}字节（ERP保守计入空格/标点）；空格分词，不重复，不填品牌/ASIN'},
    }
    return {'version': VERSION, 'checked_on': '2026-10-22', 'marketplace_id': marketplace,
            'product_type': product_type, 'brand': brand or '', 'fields': rules, 'sources': copy.deepcopy(SOURCES),
            'text_checks': {'title_symbols': TITLE_SYMBOLS, 'bullet_symbols': BULLET_SYMBOLS,
                            'title_stop_words': sorted(STOP_WORDS), 'restricted_pattern': RESTRICTED},
            'schema_status': 'baseline', 'note': '非媒体类官方标题75/亮点125；媒体类及未核实站点同样采用ERP保守75线，非宣称所有类目上限一致。技术Schema更宽不能放宽内容规范。'}


def _unconditional(schema, root, seen=None):
    """仅展开本地引用和无条件allOf；不把if/then/anyOf分支误当全局约束。"""
    if not isinstance(schema, dict):
        return []
    seen = set(seen or ())
    result = [schema]
    reference = schema.get('$ref')
    if isinstance(reference, str) and reference.startswith('#/') and reference not in seen:
        value = root
        for part in reference[2:].split('/'):
            value = value.get(part.replace('~1', '/').replace('~0', '~'), {}) if isinstance(value, dict) else {}
        result += _unconditional(value, root, seen | {reference})
    for child in schema.get('allOf', []):
        result += _unconditional(child, root, seen)
    return result


def intersect_schema(base, schema):
    """只收紧确定的PTD数值；品牌/条件/平台语义问题仍由规则和最终issues核对。"""
    result = copy.deepcopy(base)
    for block in _unconditional(schema, schema):
        for field, rule in result['fields'].items():
            for attribute in _unconditional(block.get('properties', {}).get(field, {}), schema):
                maximum = attribute.get('maxUniqueItems', attribute.get('maxItems'))
                if isinstance(maximum, int) and not isinstance(maximum, bool):
                    rule['max_items'] = min(rule['max_items'], maximum)
                for item in _unconditional(attribute.get('items', {}), schema):
                    for value in _unconditional(item.get('properties', {}).get('value', {}), schema):
                        minimum = value.get('minLength')
                        if isinstance(minimum, int) and not isinstance(minimum, bool):
                            rule['min_chars'] = max(rule['min_chars'], minimum)
                        for amazon, local in [('maxLength', 'max_chars'), ('maxUtf8ByteLength', 'max_utf8_bytes')]:
                            bound = value.get(amazon)
                            if isinstance(bound, int) and not isinstance(bound, bool):
                                rule[local] = min(rule.get(local, bound), bound)
    result['schema_status'] = 'ptd_unconditional_intersection'
    result['note'] += ' 已叠加当前商品类型PTD无条件上限；不声称覆盖全部条件Schema。'
    return result


def for_client(client, product_type, brand='', marketplace=None):
    """按凭证组/卖家/区域/站点/类型缓存6小时PTD交集；只做安全GET，失败不放宽基线。"""
    marketplace = marketplace or getattr(client, 'marketplace_id', '')
    base = policy(marketplace, product_type, brand)
    key = (getattr(client, 'credential_group_id', None), getattr(client, 'seller_id', ''),
           getattr(client, 'region', ''), marketplace, product_type)
    with CACHE_LOCK:
        saved = CACHE.get(key)
    if saved and saved[0] > time.monotonic():
        return {**copy.deepcopy(saved[1]), 'brand': brand or ''}
    try:
        definition = client.get_product_type_definition(product_type)
        if definition.get('marketplaceIds') != [marketplace] or definition.get('productType') != product_type:
            raise ContentError('商品类型定义账户/站点不一致')
        link = definition.get('schema', {}).get('link', {}).get('resource', '')
        parsed = urlparse(link)
        if parsed.scheme != 'https' or not (parsed.hostname or '').endswith('.amazonaws.com') or parsed.username or parsed.port not in (None, 443):
            raise ContentError('商品类型定义链接不是已允许的Amazon HTTPS资源')
        # 链接来自已鉴权Amazon响应，不接受浏览器传URL；签名查询参数绝不写日志或下发。
        with requests.Session() as session:
            session.trust_env = False
            with session.get(link, timeout=(3, 5), allow_redirects=False, stream=True) as response:
                if response.status_code != 200:
                    raise ContentError('商品类型Schema暂不可读')
                raw = bytearray()
                for chunk in response.iter_content(65536):
                    raw.extend(chunk)
                    if len(raw) > 4 * 1024 * 1024:
                        raise ContentError('商品类型Schema超过安全体积')
        import json
        result = intersect_schema(base, json.loads(raw))
        result['schema_version'] = definition.get('productTypeVersion', {}).get('version')
        ttl = 21600
    except Exception:
        result, ttl = base, 300
        result['schema_status'] = 'baseline_schema_unavailable'
        result['note'] += ' 当前PTD不可读，使用保守基线；最终以Amazon处理问题为准。'
    with CACHE_LOCK:
        if len(CACHE) >= 200:
            CACHE.clear()
        CACHE[key] = (time.monotonic() + ttl, copy.deepcopy(result))
    return result


def validate_field(field, values, rules=None, language='en_US'):
    """验证一语言完整文案，按码点/UTF8独立计数，不做trim或截断来掩盖超限。"""
    rules = rules or policy()
    rule = rules['fields'][field]
    label = FIELDS[field]
    if not isinstance(values, list) or not rule['min_items'] <= len(values) <= rule['max_items']:
        raise ContentError(f'{label}每种语言须为{rule["min_items"]}～{rule["max_items"]}条完整内容')
    normalized = []
    for index, value in enumerate(values, 1):
        prefix = f'{label}第{index}条'
        if not isinstance(value, str) or not value.strip():
            raise ContentError(prefix + '不能为空')
        if len(value) < rule.get('min_chars', 1) or len(value) > rule.get('max_chars', 20000):
            raise ContentError(f'{prefix}为{len(value)}字符，须为{rule.get("min_chars",1)}～{rule.get("max_chars",20000)}字符（含空格）')
        try:
            count = len(value.encode('utf-8'))
        except UnicodeError:
            raise ContentError(prefix + '包含无效Unicode字符') from None
        if count > rule.get('max_utf8_bytes', 80000):
            raise ContentError(f'{prefix}为{count}个UTF8字节，上限{rule["max_utf8_bytes"]}字节（ERP完整原文保守计数，含空格）')
        if any(unicodedata.category(c) in ('Cc', 'Cf', 'Cs') and not (field == 'product_description' and c == '\n') for c in value):
            raise ContentError(prefix + '包含换行、控制字符或不可见格式字符；仅描述允许换行')
        if re.search(r'<\s*[!/a-zA-Z][^>]*>', value):
            raise ContentError(prefix + '仅支持纯文本，不能包含HTML或脚本')
        if any(0x1F000 <= ord(c) <= 0x1FAFF or 0x2600 <= ord(c) <= 0x27BF for c in value):
            raise ContentError(prefix + '不能包含emoji或装饰符号')
        if re.search(r'https?://|www\.|[\w.+-]+@[\w.-]+\.[a-z]{2,}', value, re.I) or re.search(RESTRICTED, value, re.I):
            raise ContentError(prefix + '不能包含外链、邮箱、促销或退款保证信息')
        if field == 'item_name':
            brand = rules.get('brand')
            checked = value.replace(brand, '') if isinstance(brand, str) and brand else value
            if any(c in TITLE_SYMBOLS for c in checked):
                raise ContentError(prefix + '包含标题禁止符号 ! $ ? _ { } ^ ¬ ¦（实际品牌内字符除外）')
            if language.lower().startswith('en'):
                counts = Counter(re.findall(r"[a-z0-9]+(?:['’-][a-z0-9]+)*", value.casefold()))
                repeated = [word for word, n in counts.items() if n > 2 and word not in STOP_WORDS]
                if repeated:
                    raise ContentError('标题同一实词不得超过两次：' + ', '.join(repeated[:5]))
        if field == 'bullet_point':
            if any(c in BULLET_SYMBOLS for c in value):
                raise ContentError(prefix + '包含五点禁止的特殊装饰字符')
            letters = ''.join(c for c in value if c.isalpha())
            if len(letters) > 8 and letters.isupper():
                raise ContentError(prefix + '不能全文大写')
        if field in ('bullet_point', 'generic_keyword') and re.search(r'\bB0[A-Z0-9]{8}\b', value, re.I):
            raise ContentError(prefix + '不能填入ASIN编号')
        if field == 'generic_keyword':
            terms = re.findall(r"\w+(?:['’-]\w+)*", value.casefold())
            if len(terms) != len(set(terms)):
                raise ContentError('后台搜索词不要重复同一词；用空格分隔并保留相关词')
            brand = rules.get('brand')
            if isinstance(brand, str) and brand.strip() and brand.strip().casefold() != 'generic' and re.search(r'(?<!\w)' + re.escape(brand.strip()) + r'(?!\w)', value, re.I):
                raise ContentError('后台搜索词不能包含品牌名称')
        normalized.append(' '.join(value.casefold().split()))
    if len(normalized) != len(set(normalized)):
        raise ContentError(label + '各条内容不能完全重复')


def validate_changes(current, changes, rules=None, language='en_US'):
    """验证AI完整字段组及亮点依赖；旧未修改长文可读取，新增亮点必须配合合规最终标题。"""
    rules = rules or policy()
    for field, values in changes.items():
        if values:
            validate_field(field, values, rules, language)
        elif field == 'item_name':
            raise ContentError('商品标题不可清空')
    if changes.get('title_differentiation'):
        titles = changes.get('item_name', current.get('item_name')) or []
        if not titles or any(len(v) > rules['fields']['item_name']['max_chars'] for v in titles):
            raise ContentError('修改商品亮点前，请先将当前标题改到75字符以内，或同次提交合规标题；否则会触发Amazon 100476')


def validate_rows(current, changes, rules):
    """按语言分组校验实际属性数组，保留其他语言，不将五点条数跨语言相加。"""
    languages = set()
    for field, rows in changes.items():
        if not isinstance(rows, list) or any(not isinstance(r, dict) or not isinstance(r.get('value'), str) or not isinstance(r.get('language_tag'), str) for r in rows):
            raise ContentError(FIELDS[field] + '必须使用带语言的完整文案数组')
        languages.update(r['language_tag'] for r in rows)
        if not rows and field == 'item_name':
            raise ContentError('商品标题不可清空')
    for language in languages:
        before = {field: [r['value'] for r in rows if r.get('language_tag') == language] for field, rows in current.items()}
        changed = {field: [r['value'] for r in rows if r.get('language_tag') == language] for field, rows in changes.items() if any(r.get('language_tag') == language for r in rows)}
        validate_changes(before, changed, rules, language)


def guard_patches(client, sku, patches, product_type=None):
    """最终SP客户端写前兜底，通用PATCH不能绕过；图片/价格等非文字请求保持原行为。"""
    if not isinstance(patches, list):
        raise ContentError('Listing补丁必须为数组')
    selected = []
    for patch in patches:
        if not isinstance(patch, dict):
            raise ContentError('Listing补丁格式无效')
        path = patch.get('path', '')
        if path == '/attributes':
            raise ContentError('不允许整块替换/删除Listing属性，请使用定向字段编辑')
        field = path.split('/')[2] if isinstance(path, str) and path.startswith('/attributes/') else ''
        if field in FIELDS:
            if path != '/attributes/' + field or patch.get('op') not in ('replace', 'add', 'delete'):
                raise ContentError(FIELDS[field] + '须使用完整属性数组，不能通过嵌套路径或merge绕过校验')
            selected.append((field, patch))
    if not selected:
        return
    try:
        raw = client.get_listings_item(sku, included_data=['summaries', 'attributes', 'issues'])
        summaries = [r for r in raw.get('summaries', []) if r.get('marketplaceId') == client.marketplace_id]
        if raw.get('sku') != sku or len(summaries) != 1 or not summaries[0].get('productType'):
            raise ContentError('无法确认当前SKU/站点/商品类型，未发送文案修改')
        if product_type and product_type != summaries[0]['productType']:
            raise ContentError('请求的商品类型与当前Listing不一致，未发送文案修改')
        attrs = raw.get('attributes') or {}
        brand = next((r.get('value') for r in attrs.get('brand', []) if r.get('marketplace_id') == client.marketplace_id), '')
        rules = for_client(client, summaries[0]['productType'], brand)
        current = {field: [r for r in attrs.get(field, []) if r.get('marketplace_id') == client.marketplace_id] for field in FIELDS}
        projected, touched = copy.deepcopy(current), set()
        for field, patch in selected:
            rows = patch.get('value')
            if not isinstance(rows, list) or not rows or any(not isinstance(r, dict) or set(r) != {'value', 'language_tag', 'marketplace_id'} or
                r['marketplace_id'] != client.marketplace_id or not isinstance(r['value'], str) or not r.get('language_tag') for r in rows):
                raise ContentError(FIELDS[field] + '须显式保留当前站点及语言，不能提交复杂/无归属属性')
            languages = {r['language_tag'] for r in rows}
            projected[field] = [r for r in projected[field] if r.get('language_tag') not in languages]
            if patch['op'] != 'delete':
                projected[field] += copy.deepcopy(rows)
            touched.add(field)
        validate_rows(current, {field: projected[field] for field in touched}, rules)
    except ContentError:
        raise
    except Exception as exc:
        raise ContentError('当前Listing规范预检无法完成，未发送文案修改（' + type(exc).__name__ + '）') from None


def guard_attributes(client, attributes, product_type):
    """完整PUT同样校验已提交文案，不能从通用更新/新建绕过；不影响纯报价/图片请求。"""
    if not isinstance(attributes, dict):
        raise ContentError('Listing属性必须为对象')
    changes = {field: attributes[field] for field in FIELDS if field in attributes}
    if not changes:
        return
    for field, rows in changes.items():
        if not isinstance(rows, list) or any(not isinstance(r, dict) or r.get('marketplace_id') != client.marketplace_id for r in rows):
            raise ContentError(FIELDS[field] + '须明确当前站点和语言')
    brand = next((r.get('value') for r in attributes.get('brand', []) if isinstance(r, dict) and r.get('marketplace_id') == client.marketplace_id), '')
    validate_rows({}, changes, for_client(client, product_type, brand))
