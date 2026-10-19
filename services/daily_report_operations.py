"""日报引用本人ERP操作：北京时间工作日、原始操作证据、保存时逐条校验。"""
import copy
import json
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pymysql
from services.listing_performance.common import DomainError, positive_id
from services import listing_history_service as listing_history
from services import ads_display

KINDS = ('listing', 'ads')
MAX_SELECTED = 100
WRITE_NAMES = {'accepted': '已接受提交（不代表已生效）', 'rejected': '明确拒绝', 'failed': '明确拒绝',
               'unknown': '结果待核实', 'requesting': '等待回执', 'submitting': '等待回执'}


def bounds(day):
    first = datetime.combine(day, time.min, ZoneInfo('Asia/Shanghai')).astimezone(timezone.utc)
    return first.replace(tzinfo=None), (first + timedelta(days=1)).replace(tzinfo=None), int(first.timestamp() * 1000), int((first + timedelta(days=1)).timestamp() * 1000)


def decoded(value, fallback=None):
    if value is None: return fallback
    try: return json.loads(value) if isinstance(value, str) else value
    except (ValueError, TypeError): return fallback


def brief(value, kind='', currency='', known=True):
    if not known: return '前值未知'
    if value is None or value == []: return '未设置/清除'
    if kind == 'image': return f'{len(value)}张图片链接' if isinstance(value, list) else '图片链接'
    if kind == 'text' and isinstance(value, list): text = '；'.join(str(row.get('value', '')) for row in value if isinstance(row, dict))
    elif kind == 'price' and isinstance(value, list):
        values = [f"{item.get('value_with_tax')} {currency}".strip() for row in value if isinstance(row, dict) for item in row.get('schedule', [])]
        text = '；'.join(values) or json.dumps(value, ensure_ascii=False)
    elif isinstance(value, str): text = {'ENABLED': '启用', 'PAUSED': '暂停', 'ARCHIVED': '归档', 'CREATED': '已添加'}.get(value, value)
    else: text = json.dumps(value, ensure_ascii=False, default=str)
    return text[:240] + ('…（内容较长，详见修改记录）' if len(text) > 240 else '')


def normalize(kind, row):
    text_changes = []
    change_summaries = []
    if kind == 'listing':
        changes = decoded(row['changes_json'], [])
        # 新快照保留完整文字证据，先比较再折叠；过大时明确沿用摘要，不能截断后伪造差异。
        candidate = [change for change in changes if change.get('kind') == 'text']
        if len(json.dumps(candidate, ensure_ascii=False)) <= 100000:
            text_changes = candidate
        lines = []
        for change in changes:
            if change.get('kind') == 'image':
                lines.append(f"{change.get('label','图片')}：{'请求移除图片' if change.get('after') is None else '请求更新图片'}")
                change_summaries.append({'field': change.get('field'), 'text': lines[-1]})
                continue
            before = brief(change.get('before'), change.get('kind'), change.get('currency', ''), change.get('before_known', False))
            after = '本次整份提交未提供' if change.get('action') == 'omitted' else brief(change.get('after'),change.get('kind'),change.get('currency',''))
            lines.append(f"{change.get('label',change.get('field','属性'))}：{before} → {after}")
            change_summaries.append({'field': change.get('field'), 'text': lines[-1]})
        occurred = listing_history.utc_iso(row['requested_at'])
        label = 'SKU ' + row['sku']
        if row.get('source_sku'): label += '（来自 ' + row['source_sku'] + '）'
        summary = '\n'.join(lines) or row['change_summary']
        state = row['write_status']
    else:
        metadata = decoded(row.get('metadata'), {}) or {}
        before, after = decoded(row.get('local_before')), decoded(row.get('local_after'))
        evidence = {key: row.get(key) for key in ('entity_type','change_type','entity_name','entity_id','campaign_name','ad_group_name')}
        evidence.update(before=before, after=after, metadata={key:metadata[key] for key in
            ('placementGroupPosition','campaignBudgetType','keyword','targetingExpression','predefinedTarget','negativeTargetingType','keywordType') if key in metadata})
        display = ads_display.daily_display(evidence, before, after)
        label, summary = display['object_label'], display['summary']
        occurred = datetime.fromtimestamp(row['local_started_ms']/1000,timezone.utc).isoformat()
        state = row['write_status']
    result = {'kind':kind,'id':str(row['id']),'object_label':label,'summary':summary,'occurred_at':occurred,
              'write_status':state,'status_label':WRITE_NAMES.get(state,'结果待核实'),'source':'ERP'}
    if kind == 'listing':
        result.update(sku=row['sku'], product_name=row.get('product_name') or '', source_sku=row.get('source_sku'))
    else:
        result.update(display=display, ads_evidence=evidence)
    if text_changes:
        result['text_changes'] = text_changes
        result['change_summaries'] = change_summaries
    return result


def rows(cursor, kind, shop, market, actor, day, page=1, size=20, ids=None):
    if kind not in KINDS: raise DomainError('操作类型无效')
    first,last,lo,hi = bounds(day)
    if kind == 'listing':
        table = 'amazon_listing_change_history h'
        where = 'h.shop_id=%s AND h.marketplace_id=%s AND h.actor_user_id=%s AND h.requested_at >= %s AND h.requested_at < %s'
        args = [shop,market,actor,first,last]
        # 与经营SKU概况同源；标量查询不改变候选分页数量，也不依赖当前SKU负责人。
        columns = '''h.*,(SELECT p.product_name FROM products p
            WHERE BINARY p.seller_sku=BINARY h.sku LIMIT 1) AS product_name'''
    else:
        table = '''amazon_ads_history h LEFT JOIN amazon_ads_campaigns c ON c.campaign_id=h.campaign_id
            AND c.shop_id=h.shop_id AND c.profile_id=h.profile_id
            LEFT JOIN amazon_ads_ad_groups g ON g.ad_group_id=h.ad_group_id AND g.campaign_id=h.campaign_id'''
        where = "h.shop_id=%s AND h.marketplace_id=%s AND h.operator_id=%s AND h.source='ERP' AND h.merged_into_id IS NULL AND h.local_started_ms >= %s AND h.local_started_ms < %s"
        args = [shop,market,actor,lo,hi]
        columns = 'h.*,c.name AS campaign_name,g.name AS ad_group_name'
    if ids is not None:
        if not ids: return [], 0
        where += ' AND h.id IN (' + ','.join(['%s']*len(ids)) + ')'; args.extend(ids)
    try:
        cursor.execute('SELECT COUNT(*) AS n FROM ' + table + ' WHERE ' + where, args)
        total = cursor.fetchone()['n']
        cursor.execute('SELECT ' + columns + ' FROM ' + table + ' WHERE ' + where + ' ORDER BY h.id DESC LIMIT %s OFFSET %s',
                       args + ([MAX_SELECTED,0] if ids is not None else [size,(page-1)*size]))
        return [normalize(kind,row) for row in cursor.fetchall()], total
    except pymysql.err.ProgrammingError as exc:
        if exc.args and exc.args[0] == 1146: raise DomainError(('Listing' if kind=='listing' else '广告') + '操作记录表尚未部署，请先执行相应迁移SQL',503) from None
        raise


def resolve(cursor, shop, market, actor, day, params, old_snapshot, submitted):
    """只信任ID/理由；旧存档引用优先沿用快照，未引用ID重新核实归属与工作日。"""
    if 'reported_operations' not in params and 'operation_sections' not in params:
        existing = old_snapshot.get('reported_operations',[])
        return copy.deepcopy(old_snapshot.get('operation_sections',[])), copy.deepcopy(existing)
    sections = params.get('operation_sections',[])
    selected = params.get('reported_operations',[])
    if not isinstance(sections,list) or any(kind not in KINDS for kind in sections) or len(sections)!=len(set(sections)):
        raise DomainError('汇报操作类别无效')
    if not isinstance(selected,list) or len(selected)>MAX_SELECTED: raise DomainError('每份日报最多勾选100条操作')
    chosen, keys = [], set()
    for item in selected:
        if not isinstance(item,dict) or item.get('kind') not in sections: raise DomainError('勾选记录不属于启用的操作类别')
        identifier = str(positive_id(item.get('id'),'操作记录ID'))
        key = item['kind'] + ':' + identifier
        if key in keys: raise DomainError('同一操作记录不能重复汇报')
        keys.add(key)
        reason = item.get('reason','')
        if not isinstance(reason,str) or len(reason)>1000 or any(ord(c)<32 and c not in '\n\r\t' for c in reason): raise DomainError('每条操作理由须为最多1000字的文字')
        chosen.append({'kind':item['kind'],'id':identifier,'reason':reason.strip()})
    old = {item['kind']+':'+str(item['id']):item for item in old_snapshot.get('reported_operations',[]) if item.get('kind') in KINDS}
    current = {}
    for kind in KINDS:
        ids = [item['id'] for item in chosen if item['kind']==kind and kind+':'+item['id'] not in old]
        if ids:
            found,_ = rows(cursor,kind,shop,market,actor,day,ids=ids)
            current.update({kind+':'+item['id']:item for item in found})
    output = []
    for item in chosen:
        key=item['kind']+':'+item['id']
        evidence = old.get(key) or current.get(key)
        if not evidence: raise DomainError('所选操作不存在，或不属于本人、当前店铺/站点及工作日期，请刷新列表重新选择',403)
        output.append({**copy.deepcopy(evidence),'reason':item['reason']})
    return sections,output
