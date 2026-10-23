"""ERP会话、不可变调用事件与有界记忆；不持有网络期间的业务行锁。"""
import json
from services.listing_performance.common import DomainError
from . import plan_store
from .metrics import digest, dumps

TABLES = ('ai_operation_conversations', 'ai_operation_messages')
ACCOUNT_FIELDS = ('id', 'credential_group_id', 'region', 'marketplace_id', 'ads_profile_id', 'asin')


def checked(row, field, hash_field):
    """解析持久化JSON并验证摘要，防止用损坏记忆继续分析。"""
    value = json.loads(row[field])
    if digest(value) != row[hash_field]:
        raise DomainError('会话资料摘要不符，请核对存储；不会重新调用AI', 503)
    return value


def scope(identity):
    """计算不随会话代次变化的账户范围，明确隔离凭证组、区域、站点及Profile。"""
    boundary = {key: str(identity.get(key) or '') for key in ACCOUNT_FIELDS}
    if not boundary['id'] or not boundary['asin'] or not boundary['marketplace_id']:
        raise DomainError('会话账户或ASIN边界不完整', 422)
    return boundary, digest([boundary, 'real'])


def create_session(cur, identity, boundary, key):
    """在已锁定托管配置的短事务内新建代次，旧会话主键和原文保持不变。"""
    cur.execute('SELECT COALESCE(MAX(generation),0)+1 generation FROM ai_operation_conversations WHERE scope_key=%s', (key,))
    generation = cur.fetchone()['generation']
    account_key = key if generation == 1 else digest([boundary, 'real', generation])
    cur.execute('''INSERT INTO ai_operation_conversations
        (account_key,scope_key,generation,shop_id,asin,identity_json,runtime_mode,title,created_at,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,'real',%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
        (account_key,key,generation,identity['id'],identity['asin'],dumps(boundary),'运营会话 '+str(generation)))
    return cur.lastrowid


def open_session(identity):
    """每日任务恢复当前共享代次；首次按旧范围找回，绝不复活已归档会话。"""
    boundary, key = scope(identity)
    with plan_store.transaction(True) as cur:
        cur.execute('''SELECT id,current_conversation_id FROM ai_operation_managed_asins
            WHERE shop_id=%s AND marketplace_id=%s AND BINARY asin=BINARY %s FOR UPDATE''',
            (identity['id'],identity['marketplace_id'],identity['asin']))
        managed = cur.fetchone()
        if not managed:raise DomainError('ASIN尚未配置托管，不创建游离会话',409)
        sid = managed['current_conversation_id']
        if not sid:
            cur.execute("SELECT id FROM ai_operation_conversations WHERE scope_key=%s AND state='active' ORDER BY generation DESC LIMIT 1",(key,))
            old = cur.fetchone()
            sid = old['id'] if old else create_session(cur,identity,boundary,key)
            cur.execute('UPDATE ai_operation_managed_asins SET current_conversation_id=%s WHERE id=%s',(sid,managed['id']))
        cur.execute('SELECT * FROM ai_operation_conversations WHERE id=%s', (sid,))
        session = cur.fetchone()
        if not session or session['state']!='active' or session['scope_key']!=key:
            raise DomainError('当前会话已归档或账户配置变化，请新建对话后再分析',409)
        restore(cur, session)
    return session


def peek_session(identity):
    """预览只读恢复；没有会话时使用明确虚拟会话，绝不创建/切换指针。"""
    boundary,key=scope(identity)
    with plan_store.transaction() as cur:
        cur.execute('SELECT current_conversation_id FROM ai_operation_managed_asins WHERE shop_id=%s AND marketplace_id=%s AND BINARY asin=BINARY %s',
                    (identity['id'],identity['marketplace_id'],identity['asin']))
        managed=cur.fetchone()
        if not managed:raise DomainError('请先添加托管ASIN再预览',404)
        sid=managed['current_conversation_id']
        cur.execute("SELECT * FROM ai_operation_conversations WHERE scope_key=%s AND state='active'"+(' AND id=%s' if sid else ' ORDER BY generation DESC LIMIT 1'),(key,sid) if sid else (key,))
        session=cur.fetchone()
        if not session:
            if sid:raise DomainError('当前会话范围已变化，请核对后新建对话',409)
            cur.execute('SELECT COALESCE(MAX(generation),0)+1 generation FROM ai_operation_conversations WHERE scope_key=%s',(key,))
            generation=cur.fetchone()['generation']
            return {'id':None,'revision':0,'generation':generation,'daily_baseline_pending':True,'memory':None,
                    'recent_feedback':[],'recent_finals':[],'preview_only':True}
        restore(cur,session)
        return session


def restore(cur, session):
    """上下文只由自然问答/摘要恢复；会话revision继续CAS，不再依赖旧策略表。"""
    session['memory']=None


def append(session_id, plan_id, round_no, event_type, payload, task_id=None, owner_token=None):
    """幂等追加不可变会话事件，已登记但内容不同则拒绝覆盖。"""
    if event_type not in ('model_request','model_response','data_response','validation_feedback','execution_feedback','execution_recovery','confirmation_feedback'):
        raise DomainError('会话事件类型无效')
    with plan_store.transaction(True) as cur:
        if task_id is not None:
            from . import chat_tasks
            task=chat_tasks.fence(cur,task_id,owner_token,allow_stop=event_type!='model_request')
            if str(task['conversation_id'])!=str(session_id):raise DomainError('任务消息会话不一致',409)
            phases={'model_request':('analyzing','AI正在分析，可能计费；关闭页面不会中断任务'),
                    'model_response':('processing_reply','已保存本轮回答，正在检查内容'),
                    'data_response':('supplementing','已保存补充资料，等待AI继续分析'),
                    'validation_feedback':('correcting','回复未通过校验，正在用剩余轮次纠错')}
            if event_type in phases:
                phase,note=phases[event_type]
                cur.execute('UPDATE ai_operation_tasks SET phase=%s,message=%s WHERE id=%s',(phase,note,task_id))
        elif plan_id is None:raise DomainError('消息必须有原方案或交互任务',422)
        field,value=('task_id',task_id) if task_id is not None else ('plan_id',plan_id)
        cur.execute('SELECT * FROM ai_operation_messages WHERE '+field+'=%s AND round_no=%s AND event_type=%s',
                    (value, round_no, event_type))
        old = cur.fetchone()
        if old:
            if old['payload_hash'] != digest(payload) or str(old['conversation_id']) != str(session_id):
                raise DomainError('会话已有不同事件，请查看已保存记录，不重新调用AI',409)
            return False
        cur.execute('''INSERT INTO ai_operation_messages
            (conversation_id,plan_id,task_id,round_no,event_type,payload_json,payload_hash,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
            (session_id,plan_id,task_id,round_no,event_type,dumps(payload),digest(payload)))
    return True


def checkpoint(cur, session, plan_id, memory):
    """与方案ready同一事务提交，避免方案可执行但记忆未保存。"""
    cur.execute('SELECT revision,state FROM ai_operation_conversations WHERE id=%s FOR UPDATE', (session['id'],))
    current = cur.fetchone()
    if not current or current['revision'] != session['revision'] or current['state']!='active':
        raise DomainError('会话版本已改变，保留本次分析，不覆盖其他记忆',409)
    from . import natural_history
    natural_history.save(cur,session,'plan:'+str(plan_id),memory['question'],memory['answer'],'daily','ERP',session.get('task_id'),plan_id)
    cur.execute('UPDATE ai_operation_conversations SET revision=revision+1,updated_at=UTC_TIMESTAMP(3) WHERE id=%s', (session['id'],))
    if session.get('purpose')=='daily':
        cur.execute('UPDATE ai_operation_conversations SET daily_baseline_pending=0,baseline_plan_id=%s WHERE id=%s',(plan_id,session['id']))


def trace(plan_id,task_id=None):
    """按实际顺序读取计划的完整会话事件及摘要。"""
    with plan_store.transaction() as cur:
        field,value=('task_id',task_id) if task_id is not None else ('plan_id',plan_id)
        cur.execute('SELECT * FROM ai_operation_messages WHERE '+field+'=%s ORDER BY id', (value,))
        rows = cur.fetchall()
    return [{'id':str(r['id']), 'conversation_id':str(r['conversation_id']), 'round_no':r['round_no'],
             'event_type':r['event_type'], 'created_at':r['created_at'], 'hash':r['payload_hash'],
             'payload':checked(r,'payload_json','payload_hash')} for r in rows]


def history_index(session_id, offset, limit):
    """仅索引当前自然问答，不依赖已经删除的策略记忆表。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ai_operation_chat_turns WHERE conversation_id=%s',(session_id,))
        total=cur.fetchone()['n']
        cur.execute('''SELECT h.*,COALESCE(p.report_date,t.report_date) report_date FROM ai_operation_chat_turns h
            LEFT JOIN ai_operation_plans p ON p.id=h.plan_id LEFT JOIN ai_operation_tasks t ON t.id=h.task_id
            WHERE h.conversation_id=%s ORDER BY h.id DESC LIMIT %s OFFSET %s''',(session_id,limit,offset))
        rows=[]
        for row in cur.fetchall():
            ref=('plan:'+str(row['plan_id'])) if row['plan_id'] is not None else 'task:'+str(row['task_id'])
            rows.append({'plan_id':str(row['plan_id']) if row['plan_id'] is not None else None,'ref':ref,
                         'report_date':row['report_date'],'question':row['question'],'summary':row['answer'],'actions':[]})
    return {'status':'provided' if rows else 'unavailable','source':'history','rows':rows,
            'total_in_archive':total,'offset':offset,'returned':len(rows),
            'next_offset':offset+len(rows) if rows and offset+len(rows)<total else None}


def execution_feedback(plan, records, status, message):
    """把已发生的正式执行与独立回读事实追加到同一会话。"""
    info = plan.get('ai_call',{}).get('prompt',{}).get('conversation')
    if not info:return
    note='ERP真实接口回读事实；属性一致不代表收益改善'
    # 执行JSON只供工具查询和折叠审计，不加入后续自然问答上下文。
    payload = {'plan_id':plan['id'],'report_date':plan['report_date'],'status':status,'message':message,
               'execution_mode':'real','note':note,
               'executions':[{'execution_id':r['id'],'action_id':r['action_id'],'status':r['status'],
                              'ref':r['action']['ref'],'label':r['action']['label'],
                              'before':r['action']['before'],'after':r['action']['after'],
                              'verified_at':r.get('verified_at')} for r in records]}
    # 终态反馈幂等；阶段中断后不覆盖已保存的原事实。
    append(info['id'],plan['id'],0,'execution_feedback',payload)
