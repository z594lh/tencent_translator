"""广告只读展示规则；不修改原事件、实体代码、金额或操作证据，不执行IO。"""
import copy
import json
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

AUTO_LABELS = {'QUERY_HIGH_REL_MATCHES': '紧密匹配', 'QUERY_BROAD_REL_MATCHES': '宽泛匹配',
               'ASIN_ACCESSORY_RELATED': '关联商品', 'ASIN_SUBSTITUTE_RELATED': '同类商品'}
AUTO_ALIASES = {'close-match': 'QUERY_HIGH_REL_MATCHES', 'loose-match': 'QUERY_BROAD_REL_MATCHES',
                'complements': 'ASIN_ACCESSORY_RELATED', 'substitutes': 'ASIN_SUBSTITUTE_RELATED'}
STATES = {'ENABLED': '启用', 'PAUSED': '暂停', 'ARCHIVED': '归档', 'CREATED': '已添加'}
STRATEGIES = {'OPTIMIZE_FOR_SALES': '动态竞价－提高和降低', 'AUTO_FOR_SALES': '动态竞价－提高和降低',
              'LEGACY': '动态竞价－仅降低', 'LEGACY_FOR_SALES': '动态竞价－仅降低', 'MANUAL': '固定竞价'}
PLACEMENTS = {'TOP': '搜索结果顶部（首页）', 'TOP_OF_SEARCH': '搜索结果顶部（首页）',
              'DETAIL_PAGE': '商品页面', 'PRODUCT_PAGE': '商品页面', 'REST_OF_SEARCH': '搜索结果的其余位置'}
CHANGES = {'BID_AMOUNT': '竞价', 'DEFAULT_BID_AMOUNT': '默认竞价', 'BUDGET_AMOUNT': '每日预算',
           'PLACEMENT_GROUP': '广告位竞价调整', 'SMART_BIDDING_STRATEGY': '竞价方案', 'STATUS': '投放状态',
           'NAME': '名称', 'START_DATE': '开始日期', 'END_DATE': '结束日期', 'CREATED': '创建/添加',
           'IN_BUDGET': '广告活动预算状态', 'RULE_BASED_BUDGET_VALUE': '规则调整预算'}
CHANGES.update({'PORTFOLIO': '广告组合', 'PORTFOLIO_ID': '广告组合', 'AUTO_TARGETED': '自动投放',
                'APPLICABLE_RULE_NAME': '适用规则名称', 'APPLICABLE_RULE_ID': '适用规则 ID',
                'IS_PREMIUM_BID_ENABLED': '加价设置', 'BUDGET_DOCUMENT_VERSION': '预算版本',
                'IS_PROCESSING': '处理状态', 'CREATION_DATE': '创建时间'})
ENTITIES = {'CAMPAIGN': '广告活动', 'AD_GROUP': '广告组', 'AD': '广告商品', 'KEYWORD': '关键词',
            'NEGATIVE_KEYWORD': '否定关键词', 'PRODUCT_TARGETING': '投放项',
            'NEGATIVE_PRODUCT_TARGETING': '否定商品', 'THEME': '主题投放'}
MATCHES = {'NEGATIVE_KEYWORD_EXACT': '精准匹配', 'NEGATIVE_KEYWORD_PHRASE': '词组匹配',
           'NEGATIVE_KEYWORD_BROAD': '广泛匹配', 'KEYWORD_EXACT': '精准匹配',
           'KEYWORD_PHRASE': '词组匹配', 'KEYWORD_BROAD': '广泛匹配'}
AMOUNTS = {'BID_AMOUNT', 'DEFAULT_BID_AMOUNT', 'BUDGET_AMOUNT', 'RULE_BASED_BUDGET_VALUE'}


def raw_text(value):
    if isinstance(value, str): return value
    if isinstance(value, Decimal): return str(value)
    return json.dumps(value, ensure_ascii=False, default=str, separators=(',', ':'))


def auto_label(value):
    if not isinstance(value, str): return None
    code = AUTO_ALIASES.get(value.lower(), value.upper())
    return AUTO_LABELS.get(code)


def number_text(value, places=0):
    if isinstance(value, bool): return raw_text(value)
    raw = raw_text(value)
    try:
        number = Decimal(raw)
        if not number.is_finite() or abs(number.adjusted()) > 20: return raw
        text = format(number, 'f')
        if '.' in text: text = text.rstrip('0').rstrip('.')
        if places:
            whole, _, fraction = text.partition('.')
            text = whole + '.' + fraction.ljust(places, '0')
        return text
    except (InvalidOperation, ValueError):
        return raw


def value_text(change, value, *, entity='', missing='—', timezone_name=None):
    if value is None or value == 'null' or value == '': return missing
    raw = raw_text(value)
    if change in ('STATUS', 'CREATED'):
        if raw == 'CREATED' and entity in ('CAMPAIGN', 'AD_GROUP'): return '已创建'
        return STATES.get(raw, raw)
    if change == 'SMART_BIDDING_STRATEGY': return STRATEGIES.get(raw, raw)
    if change == 'IN_BUDGET': return {'true': '在预算内', 'false': '超出预算'}.get(raw.lower(), raw)
    if change in AMOUNTS: return number_text(value, 2)
    if change == 'PLACEMENT_GROUP':
        text = number_text(value)
        try:
            if Decimal(text).is_finite(): return text + '%'
        except InvalidOperation: pass
        return raw
    if timezone_name and change in ('START_DATE', 'END_DATE', 'CREATION_DATE') and re.fullmatch(r'\d{10}(\d{3})?', raw):
        return datetime.fromtimestamp(int(raw) / (1000 if len(raw) == 13 else 1), timezone.utc).astimezone(ZoneInfo(timezone_name)).strftime('%Y/%m/%d %H:%M:%S')
    return raw


def object_info(entity, name, metadata):
    target = (metadata.get('keyword') or metadata.get('targetingExpression') or metadata.get('predefinedTarget') or name) if entity not in ('CAMPAIGN', 'AD_GROUP') else name
    # 仅投放对象使用自动投放翻译，不替换用户自定义活动/组名称或关键词。
    automatic = (auto_label(target) or auto_label(name)) if entity == 'PRODUCT_TARGETING' else None
    if automatic: return '自动投放', automatic, automatic
    kind = ENTITIES.get(entity, entity or '未知对象')
    match = MATCHES.get(metadata.get('negativeTargetingType') or metadata.get('keywordType'))
    if match and entity in ('KEYWORD', 'NEGATIVE_KEYWORD'): kind += '（' + match + '）'
    return kind, raw_text(target) if target is not None else '', None


def daily_display(row, before, after):
    metadata = row.get('metadata') or {}
    entity, change = row.get('entity_type', ''), row.get('change_type', '')
    kind, name, automatic = object_info(entity, row.get('entity_name'), metadata)
    path = list(dict.fromkeys(str(item) for item in (row.get('campaign_name'), row.get('ad_group_name'), name) if item))
    label = kind + '：' + (' / '.join(path) or str(row.get('entity_id') or '新建实体'))
    change_label = CHANGES.get(change, change)
    if automatic and change in ('BID_AMOUNT', 'STATUS'): change_label = automatic + change_label
    if change == 'PLACEMENT_GROUP':
        position = metadata.get('placementGroupPosition')
        change_label = PLACEMENTS.get(position, position or '广告位') + '竞价调整'
    if change == 'BUDGET_AMOUNT' and metadata.get('campaignBudgetType') == 'LIFETIME_BUDGET': change_label = '总预算'
    old = value_text(change, before, entity=entity, missing='前值未知', timezone_name='Asia/Shanghai')
    new = value_text(change, after, entity=entity, missing='未设置/清除', timezone_name='Asia/Shanghai')
    return {'object_label': label, 'summary': f'{change_label}：{old} → {new}'}


def history_display(row, timezone_name='Asia/Shanghai'):
    entity, change = row['entityType'], row['changeType']
    metadata = row.get('metadata') or {}
    kind, name, _ = object_info(entity, row.get('entityName'), metadata)
    created = change == 'CREATED' or (change == 'STATUS' and row.get('newValue') == 'CREATED')
    if change == 'PLACEMENT_GROUP':
        position = metadata.get('placementGroupPosition')
        label = PLACEMENTS.get(position, position or '广告位') + '的竞价调整'
    elif entity in ('KEYWORD', 'NEGATIVE_KEYWORD', 'PRODUCT_TARGETING', 'NEGATIVE_PRODUCT_TARGETING', 'THEME', 'AD'):
        label = f"{kind} - {name or row.get('entityId') or '未知对象'} - {'已添加' if created else CHANGES.get(change, change)}"
    elif change == 'IN_BUDGET': label = '广告活动预算状态'
    elif change == 'BUDGET_AMOUNT' and metadata.get('campaignBudgetType') == 'LIFETIME_BUDGET': label = kind + '总预算'
    else: label = kind + ('已创建' if created else CHANGES.get(change, ' · ' + change))
    group = row.get('adGroupId')
    scope = '广告组 ID：' + str(group) if group else '广告活动级' if row.get('campaignId') else ''
    return {'change': label, 'detail': ' · '.join(item for item in (name if name and name not in label else '', scope) if item), 'timezone': timezone_name,
            'previousValue': value_text(change, row.get('previousValue'), entity=entity, timezone_name=timezone_name),
            'newValue': value_text(change, row.get('newValue'), entity=entity, timezone_name=timezone_name)}


def saved_daily_display(operation):
    """旧日报只基于已存字符串做有界兼容，不读当前实体/事件，不更改原证据。"""
    evidence = operation.get('ads_evidence')
    if isinstance(evidence, dict):
        return daily_display(evidence, evidence.get('before'), evidence.get('after'))
    label, summary = operation.get('object_label', ''), operation.get('summary', '')
    automatic = None
    if label.startswith('投放项：') or label.startswith('自动投放：'):
        head, separator, tail = label.rpartition(' / ')
        token = tail if separator else label.split('：', 1)[1]
        automatic = auto_label(token)
        if automatic: label = (head + separator + automatic if separator else '自动投放：' + automatic).replace('投放项：', '自动投放：', 1)
    match = re.fullmatch(r'([^：\n]+)：([^\n]+) → ([^\n]+)', summary)
    if match:
        title, before, after = match.groups()
        change = next((key for key, text in CHANGES.items() if text == title), None)
        if '竞价调整' in title: change = 'PLACEMENT_GROUP'
        if change:
            if automatic and change in ('BID_AMOUNT', 'STATUS'): title = automatic + title
            summary = f'{title}：{value_text(change, before)} → {value_text(change, after)}'
    return {'object_label': label, 'summary': summary}


def decorate_snapshot(snapshot):
    result = copy.deepcopy(snapshot)
    for operation in result.get('reported_operations', []):
        if operation.get('kind') == 'ads': operation['display'] = saved_daily_display(operation)
    return result
