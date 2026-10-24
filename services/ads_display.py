"""广告只读展示规则；中文名称复用全系统字典，不修改事件、实体代码、金额或操作证据。"""
import copy
import json
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from services.business_dictionary import DictionaryMapping, aliases, canonical

# 兼容原公共常量接口，但名称仅有字典这一个来源；自动类型集合仍只允许程序内置四类。
AUTO_LABELS = DictionaryMapping('ads.auto_target', builtin_only=True)
AUTO_ALIASES = aliases('ads.auto_target')
STATES = DictionaryMapping('ads.state')
STRATEGIES = DictionaryMapping('ads.strategy')
PLACEMENTS = DictionaryMapping('ads.placement')
CHANGES = DictionaryMapping('ads.change')
ENTITIES = DictionaryMapping('ads.entity')
MATCHES = DictionaryMapping('ads.match')
AMOUNTS = {'BID_AMOUNT', 'DEFAULT_BID_AMOUNT', 'BUDGET_AMOUNT', 'RULE_BASED_BUDGET_VALUE'}


def raw_text(value):
    if isinstance(value, str): return value
    if isinstance(value, Decimal): return str(value)
    return json.dumps(value, ensure_ascii=False, default=str, separators=(',', ':'))


def auto_label(value):
    if not isinstance(value, str): return None
    code = canonical('ads.auto_target',value)
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
