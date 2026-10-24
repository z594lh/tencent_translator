"""工具只读数据适配：复用本地归档、完整词组与历史来源，不包含提示词或会话首包。"""
from copy import deepcopy
from services.listing_performance.common import DomainError
from .. import plan_contract, plan_store, management_context
from ..metrics import dumps, digest

# 1000是条数上限，不承诺无限上下文；完整对象按预算分页，绝不截断单条字段。
MAX_DATA_BYTES = 96000
WINDOWS = ('all','daily','d1','d3','d7','p7','d14','p14','d30','p30')

def size(value):
    """计算实际UTF8序列化字节数，不把它冒充token用量。"""
    return len(dumps(value).encode('utf8'))


def fit(rows,budget):
    """二分选择能完整放入预算的前缀，避免1000对象逐条反复序列化。"""
    low,high=0,len(rows)
    while low<high:
        middle=(low+high+1)//2
        if size(rows[:middle])<=budget:low=middle
        else:high=middle-1
    return rows[:low]


def brief(value, ref):
    """对长字段生成带原文引用的明确预览，不截断原始归档。"""
    if size(value) <= 1400:return deepcopy(value)
    return {'preview':dumps(value)[:350], 'complete_value_ref':ref, 'truncated':True,
            'note':'仅供识别改动；完整原值/新值可用history索取，不能据此拼接新文案'}


def watch(row):
    """构造完整观察事项和紧凑基线，保留周期与缺失状态。"""
    ref = 'history:execution:'+str(row['execution_id'])
    fields = ('execution_id','plan_id','ref','label','unit','kind','changed_at','confirmed_at',
              'duration_days','started_on','due_on','elapsed_days','remaining_days','phase',
              'progress_note','report_date','observation_plan','status')
    result = {k:row[k] for k in fields if k in row}
    result.update(evidence_ref=ref,before=brief(row.get('before'),ref),after=brief(row.get('after'),ref))
    if 'reason' in row:result['original_reason']=brief(row['reason'],ref)
    baseline = row.get('baseline_snapshot',{})
    result['baseline'] = {k:baseline.get(k) for k in ('report_date','windows','keyword_snapshot','keywords') if k in baseline}
    post = row.get('post_evidence',{})
    result['post_evidence'] = {k:post[k] for k in ('source','ready','period_start','period_end','report_through',
        'metrics','business_metrics','observed_days','missing_days','not_yet_reported_days','scope','note','snapshot','items') if k in post}
    if 'business' in post:
        result['post_evidence']['business'] = {k:v for k,v in post['business'].items() if k!='note'}
    return result


def period(row, window):
    """按指定窗口提取归档指标，缺少逐日明细时明确说明。"""
    result = deepcopy(row)
    if window=='daily':
        result = {k:v for k,v in result.items() if k!='periods'}
        if 'daily' not in result:result.update(daily=None,availability_note='此对象日明细未包含于本次归档，不能当0')
        return result
    result.pop('daily',None)
    if window!='all' and 'periods' in result:
        matrix = result['periods']
        matrix['rows'] = [r for r in matrix['rows'] if r[0]==window]
        if 'coverage' in matrix:matrix['coverage'] = {window:matrix['coverage'].get(window)}
    return result


def fetch(data, request, session=None, max_bytes=MAX_DATA_BYTES):
    """仅按白名单读取本地归档或本会话历史，不运行模型给出的SQL。"""
    source, ref, window = (request[k] for k in ('source','ref','window'))
    content = data['content']; rows = []
    if source in ('listing','pricing','business'):
        rows = [period(r,window) for r in content[source] if not ref or ref in (r.get('sku'),r.get('evidence_id'))]
    elif source=='current_settings':
        rows = [r for key,r in data['catalog'].items() if not ref or ref==key]
    elif source=='erp_operations':
        operations = content['recent_operations']
        if not ref:
            rows = [{'operation_type':kind,'record':r} for kind,items in operations.items() if isinstance(items,(list,tuple)) for r in items]
    elif source=='advertising':
        candidate = data['catalog'].get(ref)
        if candidate:
            subject = candidate['subject']
            kind = {'ads_placement_update':'spCampaignsPlacement','ads_budget_update':'spCampaigns'}.get(candidate['kind'],'spTargeting')
            from ..catalog import REPORT_PLACEMENTS
            for module in content['ads']:
                if module['kind']!=kind:continue
                for row in module['details']:
                    dim = row['dimensions']
                    if str(dim.get('campaign_id'))!=str(subject.get('campaign_id')):continue
                    if subject.get('ad_group_id') and str(dim.get('ad_group_id'))!=str(subject['ad_group_id']):continue
                    if subject.get('entity_type') in ('target','keyword') and str(dim.get('keyword_id'))!=str(subject['entity_id']):continue
                    if subject.get('placement') and REPORT_PLACEMENTS.get(dim.get('placement'),dim.get('placement'))!=subject['placement']:continue
                    rows.append(period(row,window))
        else:
            for module in content['ads']:
                rows.extend(period(r,window) for r in module['details'] if not ref or ref==module['kind'] or ref==r['evidence_id'])
    elif source=='keywords':
        rows = [r for r in content['keywords']['items'] if not ref or r['keyword']==ref or r['evidence_id']==ref]
    elif source=='keyword_history':
        for version in content['keywords']['history']:
            columns = version['columns']; pos = columns.index('keyword')
            rows.extend({'snapshot':version['snapshot'],**dict(zip(columns,row))} for row in version['rows'] if not ref or row[pos]==ref)
    elif source=='history':
        if not ref and session:
            from .. import conversation_store
            result=conversation_store.history_index(session['id'],request['offset'],request['limit'])
            original=result['rows'];result['rows']=fit(original,max(0,max_bytes-2000))
            result['returned']=len(result['rows']);result['byte_limited']=len(result['rows'])<len(original)
            result['has_more']=request['offset']+len(result['rows'])<result['total_in_archive']
            result['next_offset']=request['offset']+len(result['rows']) if result['rows'] and result['has_more'] else None
            if result['byte_limited'] and not result['rows']:result.update(status='unavailable',reason='单条历史超过当前结果预算，请在页面查看原文')
            return result
        if ref.startswith('task:') and ref[5:].isdigit():
            from .. import chat_tasks
            try:task,task_session,managed=chat_tasks.checked_task(ref[5:])
            except DomainError:return {'status':'unavailable','reason':'历史问答不存在或账户范围不符'}
            if task['scope_key']!=chat_tasks.messages.scope(content['identity'])[1]:return {'status':'unavailable','reason':'历史问答不属于本账户ASIN'}
            with plan_store.transaction() as cur:
                cur.execute('SELECT * FROM ai_operation_messages WHERE id=%s',(task['user_message_id'],))
                question=chat_tasks.messages.checked(cur.fetchone(),'payload_json','payload_hash')
            rows=[{'task_id':str(task['id']),'status':task['status'],'user_question':question,
                   'result':chat_tasks.messages.checked(task,'result_json','result_hash') if task['result_json'] else None,
                   'note':'这是历史问答或未执行建议，不是平台执行事实'}]
        else:
            rows=history_rows(data,ref,content)
    offset, limit = request['offset'],request['limit']
    selected = deepcopy(rows[offset:offset+limit])
    selected=fit(selected,max(0,max_bytes-3000))
    result = {'status':'provided' if selected else 'unavailable','source':source,'ref':ref,'window':window,
        'report_end':content['time']['report_end'],'offset':offset,'total_in_archive':len(rows),
        'rows':selected,'returned':len(selected),
        'requested_limit':limit,'has_more':offset+len(selected)<len(rows),
        'next_offset':offset+len(selected) if selected and offset+len(selected)<len(rows) else None,
        'byte_limited':len(selected)<len(rows[offset:offset+limit]),
        'source_coverage':coverage(data,source),
        'note':'来自本次本地冻结资料；total_in_archive不是平台全部对象；检查source_coverage和has_more，缺项/缺日不补0'}
    if not selected:result['reason']='对象/窗口没有可提供资料，或单个完整对象超过本次字节预算；不截断全文，不把缺失当0'
    result['data_hash'] = digest(result)
    return result


def coverage(data,source):
    """披露冻结资料自身的筛选范围，避免把取数1000误认为平台无遗漏全量。"""
    content=data['content']
    if source=='advertising':return [{k:r.get(k) for k in ('kind','scope','total_objects','selected_objects','omitted_objects')} for r in content['ads']]
    if source in ('keywords','keyword_history'):
        return {k:content['keywords'].get(k) for k in ('snapshot','total','selected','omitted','note')}
    return None


def history_rows(data,ref,content):
    """读取原执行/方案历史；与人工问答索引分离但继续沿用相同账户核对。"""
    rows=[]
    entry = data['evidence'].get(ref)
    if entry and entry['kind'] in ('execution_history','analysis_history'):
        record = deepcopy(entry['data'])
        if entry['kind']=='execution_history' and 'baseline_snapshot' in record:
            baseline = record['baseline_snapshot']
            keywords = baseline.get('keywords')
            if isinstance(keywords,dict) and isinstance(keywords.get('items'),list):
                fixed = set(record.get('observation_plan',{}).get('keywords',[]))
                original = keywords['items']
                keywords['items'] = [w for w in original if w.get('keyword') in fixed]
                keywords['omitted_non_observed_count'] = len(original)-len(keywords['items'])
        rows = [{'evidence_id':ref, 'data':record}]
    elif ref.startswith('plan:') and ref.split(':')[1].isdigit() and len(ref.split(':')) in (2,4):
        parts=ref.split(':')
        try:plan = plan_store.get(parts[1])
        except DomainError as error:
            if error.status!=404:raise
            return []
        if plan.get('legacy'):return []
        before = plan['facts']['identity']; now = content['identity']
        if not management_context.same_account(before,now) or before['asin']!=now['asin']:
            return []
        if len(parts)==4:
            rows=[a for a in (plan['result'] or {}).get('actions',[]) if parts[2]=='action' and a['action_id']==parts[3]]
        else:
            rows = [{'plan_id':plan['id'],'report_date':plan['report_date'],'result':plan['result'],
                     'listing':plan['dataset']['content']['listing'],'business':plan['dataset']['content']['business']}]
    return rows
