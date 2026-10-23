"""共享AI持久队列：人工分析、每日优化、确认执行按同ASIN先后串行。"""
import json
import re
import uuid
from services.listing_performance.common import DomainError, positive_id
from . import plan_store, conversation_store as messages
from .metrics import dumps, digest

TERMINAL={'completed','failed','unknown','cancelled'}


def schema(cur):
    """仅验证新任务及消息结构，不启动消费者或调用模型。"""
    for table,fields in (('ai_operation_tasks','id,scope_key,owner_token,input_json,input_hash,user_message_id,task_type,chat_mode,run_id,confirmation_id,scheduled_at'),
                         ('ai_operation_worker_state','worker_key,owner_token,heartbeat_at,state'),
                         ('ai_operation_messages','task_id'),
                         ('ai_operation_plans','task_id'),('ai_operation_confirmations','id,plan_hash,task_id')):
        cur.execute('SELECT '+fields+' FROM '+table+' WHERE 1=0')


def health():
    """只读消费者心跳；页面打开不伪造在线，不从Web导入启动线程。"""
    with plan_store.transaction() as cur:
        cur.execute("SELECT state,heartbeat_at,TIMESTAMPDIFF(SECOND,heartbeat_at,UTC_TIMESTAMP(3)) age FROM ai_operation_worker_state WHERE worker_key='shared_chat'")
        row=cur.fetchone()
    online=bool(row and row['state']=='running' and 0<=row['age']<90)
    return {'online':online,'last_heartbeat_at':row['heartbeat_at'] if row else None,
            'message':'AI消费者在线' if online else 'AI消费者未启动、已停止或心跳过期，请管理员核对独立AI会话服务；打开对话不计费'}


def checked_task(task_id):
    """按真实会话范围回查任务，知道编号不能跨账户读取。"""
    from .conversation_queries import require_session
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_tasks WHERE id=%s',(positive_id(task_id,'任务'),))
        task=cur.fetchone()
    if not task:raise DomainError('交互任务不存在',404)
    if task['conversation_id'] is None and task['task_type']=='daily':
        from .conversation_queries import managed_identity
        managed,identity=managed_identity(task['managed_id'])
        if messages.scope(identity)[1]!=task['scope_key']:raise DomainError('任务账户范围已变化',409)
        return task,None,managed
    session,managed=require_session(task['conversation_id'])
    if session['scope_key']!=task['scope_key']:raise DomainError('任务账户范围不一致',409)
    return task,session,managed


def public(task,current=None):
    """返回阶段/用量/回复，不在轮询中重复返回全套经营档案。"""
    result={key:task.get(key) for key in ('id','conversation_id','user_message_id','actor_id','actor_name','status','phase',
        'message','error_code','stop_requested','report_date','plan_id','created_at','started_at','completed_at',
        'task_type','chat_mode','run_id','confirmation_id')}
    for key in ('id','conversation_id','user_message_id','plan_id','run_id','confirmation_id'):
        if result[key] is not None:result[key]=str(result[key])
    if current is None:
        from .protocol import native_task
        with plan_store.transaction() as cur:current=native_task(cur,task)
    result['legacy']=not current
    if not current:
        from .protocol import LEGACY_MESSAGE
        return {**result,'message':LEGACY_MESSAGE,'result':None,'usage':None,'execution_policy':'unavailable'}
    result['result']=messages.checked(task,'result_json','result_hash') if task.get('result_json') else None
    result['usage']=json.loads(task['usage_json']) if task.get('usage_json') else None
    result['execution_policy']='discussion_only' if task.get('chat_mode')=='discussion' else {'chat':'confirm_required','daily':'auto','execute':'explicit_confirmation'}.get(task.get('task_type'),'confirm_required')
    return result


def get(task_id):
    """只读任务状态；刷新不会重复收费或恢复未知请求。"""
    return public(checked_task(task_id)[0])


def enqueue(session_id,body,actor):
    """同事务保存原话与任务，返回后异步处理；原token重试先复用而非再次收费。"""
    from .conversation_queries import require_session
    from .daily_runner import authorize_actor
    if not isinstance(body,dict) or not {'text','request_token','confirm_ai'}<=set(body) or set(body)-{'text','request_token','confirm_ai','mode','retry_summary_id'} or body['confirm_ai'] is not True:
        raise DomainError('请输入问题并确认AI分析可能计费')
    mode=body.get('mode','discussion')
    if not isinstance(mode,str) or mode not in ('agent','discussion','proposal'):raise DomainError('人工对话模式无效')
    summary_retry=positive_id(body['retry_summary_id'],'摘要重试编号') if body.get('retry_summary_id') is not None else None
    text=body['text'];token=body['request_token']
    if not isinstance(text,str) or not text.strip() or len(text)>4000 or any(ord(c)<32 and c not in '\n\t' for c in text):raise DomainError('问题不能为空、超过4000字符或含控制字符')
    if not isinstance(token,str) or not re.fullmatch('[a-f0-9]{32}',token):raise DomainError('发送标识无效')
    actor=positive_id(actor,'用户');authorize_actor(actor)
    session,managed=require_session(session_id)
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT current_conversation_id FROM ai_operation_managed_asins WHERE id=%s FOR UPDATE',(managed['id'],))
        current=cur.fetchone()
        cur.execute('SELECT * FROM ai_operation_tasks WHERE request_token=%s',(token,))
        old=cur.fetchone()
        if old:
            cur.execute('SELECT * FROM ai_operation_messages WHERE id=%s',(old['user_message_id'],))
            original=cur.fetchone()
            saved=messages.checked(original,'payload_json','payload_hash') if original else {}
            # 无mode的旧客户端须按原任务模式核对，不能因新默认改动原token语义。
            original_mode=mode if 'mode' in body else old.get('chat_mode')
            if str(old['conversation_id'])!=str(session['id']) or old['actor_id']!=actor or not original or saved['text']!=text or saved.get('retry_summary_id')!=summary_retry or old.get('chat_mode')!=original_mode:
                raise DomainError('发送标识已用于其他问题，未重复发起',409)
            from .protocol import native_task
            return {**public(old,native_task(cur,old)),'reused':True}
        if session['state']!='active' or str(current['current_conversation_id'])!=str(session['id']):raise DomainError('旧对话只读，请切换到当前对话再提问',409)
        if not health()['online']:raise DomainError('AI消费者不在线，请先部署/启动独立服务；本次没有登记问题或调用AI',503)
        cur.execute("SELECT COUNT(*) n FROM ai_operation_tasks WHERE conversation_id=%s AND status IN ('queued','running')",(session['id'],))
        if cur.fetchone()['n']>=10:raise DomainError('该对话已有10个待处理问题，请等待或取消排队任务',409)
        cur.execute("SELECT COALESCE(NULLIF(nickname,''),username) name FROM users WHERE id=%s AND status=1 AND account_type='human'",(actor,))
        user=cur.fetchone()
        if not user:raise DomainError('需要有效的人工用户身份',403)
        cur.execute('''INSERT INTO ai_operation_tasks (managed_id,conversation_id,scope_key,request_token,actor_id,actor_name,chat_mode,message,created_at,available_after)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
            (managed['id'],session['id'],session['scope_key'],token,actor,user['name'][:100],mode,'已保存自然问题，等待AI按需查询；若提出计划仍须明确确认才执行'))
        tid=cur.lastrowid;payload={'engine':'native-tools-v1','text':text,'actor_id':actor,'actor_name':user['name'][:100],'source':'human','mode':mode,'retry_summary_id':summary_retry,'execution_policy':'discussion_only' if mode=='discussion' else 'confirm_required'}
        cur.execute('''INSERT INTO ai_operation_messages (conversation_id,task_id,plan_id,round_no,event_type,payload_json,payload_hash,created_at)
            VALUES (%s,%s,NULL,0,'user_message',%s,%s,UTC_TIMESTAMP(3))''',(session['id'],tid,dumps(payload),digest(payload)))
        cur.execute('UPDATE ai_operation_tasks SET user_message_id=%s WHERE id=%s',(cur.lastrowid,tid))
        cur.execute('SELECT * FROM ai_operation_tasks WHERE id=%s',(tid,))
        return {**public(cur.fetchone(),True),'reused':False}




def fence(cur,task_id,owner,allow_stop=False):
    """每次调用/落库前核对租约和代次，迟到消费者不能覆盖新结果。"""
    cur.execute('SELECT *,lease_until>UTC_TIMESTAMP(3) lease_valid FROM ai_operation_tasks WHERE id=%s FOR UPDATE',(task_id,))
    row=cur.fetchone()
    if not row or row['status']!='running' or row['owner_token']!=owner or not row['lease_valid']:raise DomainError('任务租约失效，已停止后续步骤；请核对原调用记录',409)
    if row['stop_requested'] and not allow_stop:raise DomainError('用户请求停止后续分析；已发生调用仍可能计费',409)
    return row


def phase(task_id,owner,name,note):
    """短事务更新可见阶段，不持有事务等待网络。"""
    with plan_store.transaction(True) as cur:
        fence(cur,task_id,owner)
        cur.execute('UPDATE ai_operation_tasks SET phase=%s,message=%s WHERE id=%s',(name,note[:500],task_id))


def cancel(task_id,actor):
    """排队任务立即取消；已发请求仅停止后续步骤，不承诺撤销计费。"""
    task,session,managed=checked_task(task_id)
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT * FROM ai_operation_tasks WHERE id=%s FOR UPDATE',(task['id'],));row=cur.fetchone()
        if row['status'] not in TERMINAL:
            queued=row['status']=='queued'
            cur.execute('''UPDATE ai_operation_tasks SET stop_requested=1,cancelled_by=%s,
                status=IF(%s,'cancelled',status),phase=IF(%s,'cancelled',phase),
                completed_at=IF(%s,UTC_TIMESTAMP(3),completed_at),message=%s WHERE id=%s''',
                (positive_id(actor,'取消用户'),queued,queued,queued,'排队问题已取消，未调用AI' if queued else '已请求停止后续步骤；正在进行的调用可能已计费',task['id']))
            if queued and row.get('run_id'):
                cur.execute("UPDATE ai_operation_daily_runs SET status='paused',message='每日任务已在排队阶段取消，未调用AI或修改平台',completed_at=UTC_TIMESTAMP(3),updated_at=UTC_TIMESTAMP(3) WHERE id=%s AND status='queued'",(row['run_id'],))
    return get(task['id'])


def claim():
    """回收中断任务并领取一项；只要登记过模型请求就标未知，绝不自动重放收费请求。"""
    with plan_store.transaction(True) as cur:
        cur.execute("SELECT * FROM ai_operation_tasks WHERE status='running' AND lease_until<UTC_TIMESTAMP(3) FOR UPDATE")
        for old in cur.fetchall():
            cur.execute("SELECT id FROM ai_operation_messages WHERE task_id=%s AND event_type='model_request' LIMIT 1",(old['id'],))
            sent=bool(cur.fetchone()) or old.get('plan_id') is not None or old.get('task_type')=='execute'
            if sent:
                cur.execute("UPDATE ai_operation_plans SET status='unknown',error_code='task_interrupted',message='消费者中断，请核对原请求和实际执行记录，不自动重放',completed_at=UTC_TIMESTAMP(3) WHERE (task_id=%s OR id=%s) AND status='generating'",(old['id'],old.get('plan_id')))
            cur.execute('''UPDATE ai_operation_tasks SET status=%s,phase=%s,owner_token=NULL,lease_until=NULL,
                message=%s,error_code=%s,completed_at=IF(%s,UTC_TIMESTAMP(3),NULL) WHERE id=%s''',
                ('unknown' if sent else 'cancelled' if old['stop_requested'] else 'queued','unknown' if sent else 'cancelled' if old['stop_requested'] else 'queued',
                 '消费者中断，原请求可能已计费；保留原文且不自动重试' if sent else '准备阶段中断，尚未登记模型请求，重新排队',
                 'interrupted_request' if sent else None,sent or bool(old['stop_requested']),old['id']))
            if old.get('run_id') and (sent or old['stop_requested']):
                cur.execute("UPDATE ai_operation_daily_runs SET status='failed',message='每日消费者中断，原分析或执行可能已发生；仅核对历史，不自动重放',completed_at=UTC_TIMESTAMP(3),updated_at=UTC_TIMESTAMP(3) WHERE id=%s",(old['run_id'],))
        cur.execute("""SELECT t.* FROM ai_operation_tasks t WHERE t.status='queued' AND t.available_after<=UTC_TIMESTAMP(3)
            AND NOT EXISTS (SELECT 1 FROM ai_operation_tasks prior WHERE prior.managed_id=t.managed_id
                AND prior.id<t.id AND prior.status IN ('queued','running')) ORDER BY t.id LIMIT 1 FOR UPDATE""")
        row=cur.fetchone()
        if not row:return None
        owner=uuid.uuid4().hex
        cur.execute("""UPDATE ai_operation_tasks SET status='running',phase='preparing',owner_token=%s,
            started_at=UTC_TIMESTAMP(3),lease_until=DATE_ADD(UTC_TIMESTAMP(3),INTERVAL 90 SECOND),heartbeat_at=UTC_TIMESTAMP(3),message='正在恢复共享对话与准备本地资料' WHERE id=%s""",(owner,row['id']))
        return {**row,'status':'running','owner_token':owner}


def requeue(task):
    """每日/其他同ASIN工作占锁时短暂退避，避免假失败或重复模型请求。"""
    with plan_store.transaction(True) as cur:
        fence(cur,task['id'],task['owner_token'])
        cur.execute("UPDATE ai_operation_tasks SET status='queued',phase='queued',owner_token=NULL,lease_until=NULL,available_after=DATE_ADD(UTC_TIMESTAMP(3),INTERVAL 5 SECOND),message='同ASIN正在分析或执行，等待前一项结束' WHERE id=%s",(task['id'],))
