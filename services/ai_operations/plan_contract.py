"""主干方案契约：完整内容替换/具体数值/观察项，不生成任意API命令。"""
from decimal import Decimal
import re
from services import listing_content_policy
from services.listing_performance.common import DomainError
from .catalog import number

VERSION = 'operations_plan_v6_listing_policy'
MODEL = 'deepseek-flash'
MAX_OUTPUT_TOKENS = 24000
METRICS = {'impressions', 'clicks', 'ctr', 'cpc', 'purchases_7d', 'sales_7d', 'acos',
           'sales_qty', 'sales_amount', 'net_profit', 'ad_cost', 'tacos', 'organic_rank',
           'sponsored_rank', 'traffic_share', 'organic_share', 'sponsored_share', 'weekly_impressions',
           'cost','refund_amount','ad_sales','ad_orders','profit_margin'}

# 自然语言提示已迁移为简短工具协议；字段限制由下方校验及工具返回的约束强制执行。


def text(value, label, limit=2000, empty=False):
    """检查文本长度及控制字符，拒绝空白或异常内容。"""
    if not isinstance(value, str) or len(value) > limit or (not empty and not value.strip()) or any(ord(c) < 32 and c not in '\n\t' for c in value):
        raise DomainError(label + '为空、过长或包含非法字符', 422)


def fields(value, keys, label):
    """核对JSON对象精确字段，拒绝遗漏和额外命令。"""
    if not isinstance(value, dict) or set(value) != set(keys):
        raise DomainError(label + '字段与方案契约不符', 422)


def evidence_refs(value,dataset):
    """将精确对象引用映射到本轮已登记的真实证据。"""
    if not isinstance(value,list) or not 1<=len(value)<=12 or any(not isinstance(r,str) for r in value):
        raise DomainError('行动引用依据格式无效',422)
    resolved=[]
    for ref in value:
        if ref in dataset['evidence']:resolved.append(ref)
        elif ref in dataset['catalog']:
            # 精确命中已提供的行动对象时，转为该对象已登记的当前设置证据，不猜测未知编号。
            resolved.extend(dataset['catalog'][ref]['evidence_refs'])
        else:raise DomainError('行动引用了未提供的依据：'+ref[:100],422)
    return list(dict.fromkeys(resolved))


def validate(value, dataset):
    """校验会话最终行动、完整文案、金额、对象与观察计划。"""
    fields(value, ('summary', 'actions', 'notes', 'history_reviews'), '运营方案')
    text(value['summary'], '方案摘要', 4000)
    if not isinstance(value['actions'], list) or len(value['actions']) > 12 or not isinstance(value['notes'], list) or len(value['notes']) > 12:
        raise DomainError('方案行动或说明数量超限', 422)
    for note in value['notes']:text(note, '方案说明', 1500)
    known_words = {w['keyword'] for w in dataset['content']['keywords']['items']}
    for module in dataset['content']['ads']:
        for row in module['details']:
            word = row['dimensions'].get('customer_search_term') or row['dimensions'].get('keyword_text')
            if isinstance(word, str):known_words.add(word)
    output, seen = [], set()
    for row in value['actions']:
        fields(row, ('ref', 'mode', 'after', 'reason', 'priority', 'evidence_refs', 'observation'), '行动')
        ref = row['ref']
        if not isinstance(ref, str) or ref not in dataset['catalog'] or ref in seen:
            raise DomainError('行动对象不存在或同字段重复', 422)
        seen.add(ref)
        candidate = dataset['catalog'][ref]
        if row['mode'] not in ('change', 'observe') or (candidate['kind'] == 'observe' and row['mode'] != 'observe'):
            raise DomainError('行动方式必须是明确修改或观察', 422)
        after = row['after']; kind = 'observe' if row['mode'] == 'observe' else candidate['kind']
        if kind == 'listing_text_update':
            source = next((r for r in dataset['content']['listing'] if r['sku'] == candidate['subject']['sku']), {})
            rules = source.get('content_policy') or listing_content_policy.policy(candidate['subject'].get('marketplace_id'), source.get('product_type'), source.get('brand'))
            try:
                listing_content_policy.validate_field(candidate['subject']['field'], after, rules)
            except listing_content_policy.ContentError as exc:
                raise DomainError(str(exc), 422) from None
        elif kind == 'observe':
            if after is not None:raise DomainError('观察行动不能包含修改值', 422)
        elif kind == 'ads_placement_update':
            if type(after) is not int or not 0 <= after <= 900:
                raise DomainError('广告位加价必须为0到900的整数百分比', 422)
        else:
            if not isinstance(after, str) or not re.fullmatch(r'\d{1,6}(?:\.\d{1,8})?', after):
                raise DomainError('金额必须是正数十进制字符串，精确到美分', 422)
            if number(after) is None or not Decimal('0.01') <= Decimal(after) <= Decimal('100000'):
                raise DomainError('行动金额无效', 422)
            if Decimal(after)*100 != (Decimal(after)*100).to_integral_value():
                raise DomainError('行动金额必须精确到美分，不能静默舍入AI给出的金额',422)
            # 原报表常为0.3000，等值尾零可无损规范为0.30；原回答仍完整保存在usage中。
            after=format(Decimal(after),'.2f')
        if kind != 'observe' and (after == candidate['before'] or
                (isinstance(after, str) and candidate['unit'] != 'text_list' and number(after) == number(candidate['before']))):
            raise DomainError('行动没有实际变化，不必提交', 422)
        text(row['reason'], '调整理由', 3000)
        if row['priority'] not in ('优先处理', '常规关注', '持续观察'):
            raise DomainError('建议优先级必须使用中文', 422)
        refs = evidence_refs(row['evidence_refs'],dataset)
        if not any(r in candidate['evidence_refs'] for r in refs):
            raise DomainError('行动缺少对应当前对象/字段的依据', 422)
        observation = row['observation']
        keys=('metrics', 'keywords', 'check_days', 'hypothesis', 'review_when')
        fields(observation, (*keys,'duration_days'), '观察计划')
        metrics, words, days = observation['metrics'], observation['keywords'], observation['check_days']
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= 12 or any(not isinstance(m, str) or m not in METRICS for m in metrics):
            raise DomainError('观察指标不属于现有资料', 422)
        if not isinstance(words, list) or len(words) > 20 or any(not isinstance(w, str) or w not in known_words for w in words):
            raise DomainError('观察关键词必须来自已提供的完整词组', 422)
        if not isinstance(days, list) or not days or len(days) > 4 or any(type(d) is not int or d not in (3, 7, 14, 30) for d in days):
            raise DomainError('观察时间仅支持3/7/14/30日检查点', 422)
        duration=observation['duration_days']
        if type(duration) is not int or not 1<=duration<=30 or any(d>duration for d in days):
            raise DomainError('观察周期必须为1到30天，检查点不能超过总周期',422)
        observation={**observation,'duration_days':duration}
        text(observation['hypothesis'], '观察假设', 1500)
        text(observation['review_when'], '重评条件', 1500)
        output.append({**candidate, **row, 'after':after, 'evidence_refs':refs, 'observation':observation, 'kind': kind, 'action_id': 'a' + str(len(output)+1),
                       'execution_status': 'not_implemented', 'execution_available': False})
    # 跨字段校验以本方案最终文案为准，标题/亮点不能各自合规却组合触发100476。
    changed = {}
    for action in output:
        if action['kind'] == 'listing_text_update':
            changed.setdefault(action['subject']['sku'], {})[action['subject']['field']] = action['after']
    for sku, changes in changed.items():
        source = next((r for r in dataset['content']['listing'] if r['sku'] == sku), {})
        try:
            listing_content_policy.validate_changes(source.get('fields', {}), changes, source.get('content_policy'))
        except listing_content_policy.ContentError as exc:
            raise DomainError(str(exc), 422) from None
    reviews=validate_reviews(value['history_reviews'],dataset,output)
    return {'schema_version': VERSION, 'summary': value['summary'], 'actions': output,
            'notes': value['notes'], 'history_reviews':reviews, 'execution_available': False}


def validate_reviews(reviews,dataset,actions):
    """逐项校验全部待观察或待回读执行，阻止未经说明的重复调整。"""
    memory=dataset['content'].get('management_context',{})
    observed={str(r['execution_id']):r for r in memory.get('observations',[])}
    pending={str(r['execution_id']):r for r in memory.get('pending_executions',[])}
    known={**observed,**pending};seen=set()
    if not isinstance(reviews,list) or len(reviews)>200:raise DomainError('历史复盘格式或数量无效',422)
    for row in reviews:
        fields(row,('execution_id','decision','conclusion','evidence_refs'),'历史复盘')
        eid=row['execution_id']
        if not isinstance(eid,str) or eid not in known or eid in seen:raise DomainError('历史复盘引用未知或重复的执行',422)
        seen.add(eid)
        if row['decision'] not in ('继续观察','到期复评','提前调整','等待回读'):raise DomainError('历史复盘结论类型无效',422)
        text(row['conclusion'],'历史复盘说明',2000)
        refs=row['evidence_refs']
        if not isinstance(refs,list) or not 1<=len(refs)<=12 or any(not isinstance(r,str) or r not in dataset['evidence'] for r in refs):
            raise DomainError('历史复盘缺少本次提供的依据',422)
        if eid in pending and row['decision']!='等待回读':raise DomainError('待核实执行必须先等待回读，不得认定有效',422)
        if row['decision']=='到期复评' and eid in observed and observed[eid]['phase']!='ready_review':
            raise DomainError('观察周期或资料尚未齐，不能认定已可复评',422)
    if seen!=set(known):raise DomainError('AI没有完整复盘本次提供的往期观察/待回读事项',422)
    changed={a['ref'] for a in actions if a['mode']=='change'}
    if changed & {r['ref'] for r in pending.values()}:raise DomainError('该字段的上次提交尚待核实，本轮未重新提交',422)
    decisions={r['execution_id']:r['decision'] for r in reviews}
    for eid,row in observed.items():
        if row['ref'] in changed and row['phase']!='ready_review' and decisions.get(eid)!='提前调整':
            raise DomainError('观察期内再次修改同一字段，AI未说明提前调整与中止原观察',422)
    return reviews


