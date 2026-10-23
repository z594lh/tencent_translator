"""每日入口只登记持久队列：18:30遇人工分析不漏跑，跨日消费不漂移任务日。"""
from datetime import timedelta, timezone
from services import operator_accounts
from services.listing_performance.common import DomainError, positive_id
from . import chat_tasks, conversation_queries, conversation_store, daily_runner, plan_store, managed_store
from .metrics import digest, dumps


def enqueue(managed_id,actor=None,source='manual',now=None):
    """与每日账本同事务登记唯一作业；cron允许离线排队，人工入口明确要求在线。"""
    if source not in ('manual','scheduled'):raise DomainError('每日触发来源无效')
    now=now or daily_runner.clock()
    if now.tzinfo is None:raise DomainError('任务时钟必须包含时区')
    managed,identity=conversation_queries.managed_identity(managed_id)
    actor=positive_id(actor if actor is not None else managed['actor_id'],'人工授权人')
    daily_runner.authorize_actor(actor)
    today=now.astimezone(daily_runner.BJ).date()
    end=min(today-timedelta(days=1),now.astimezone(daily_runner.SOURCE_ZONE).date()-timedelta(days=1))
    with plan_store.transaction(True) as cur:
        current=managed_store.target(cur,managed['id'],True)
        cur.execute("SELECT * FROM ai_operation_daily_runs WHERE managed_id=%s AND run_date=%s AND runtime_mode='conv_real'",(managed['id'],today))
        old=cur.fetchone()
        if old:
            cur.execute("SELECT * FROM ai_operation_tasks WHERE run_id=%s",(old['id'],));task=cur.fetchone()
            return {'run':managed_store.serialize_run(old),'task':chat_tasks.public(task) if task else None,'reused':True}
        if not current['enabled'] or managed['shop_status']!=1:raise DomainError('托管或店铺已停用，不登记每日任务',409)
        if source=='manual' and not chat_tasks.health()['online']:raise DomainError('AI消费者离线，请先启动服务；本次未创建任务',503)
        ai=operator_accounts.read_ai(cur,current['executor_user_id']);initiator=operator_accounts.get_user(cur,actor)
        if initiator['account_type']!='human':raise DomainError('每日任务需要人工授权人',403)
        audit={'source':'AI','executor':operator_accounts.snapshot(ai),'initiated_by':operator_accounts.snapshot(initiator)}
        cur.execute('''INSERT INTO ai_operation_daily_runs
            (managed_id,run_date,report_date,runtime_mode,trigger_source,status,actor_id,executor_user_id,audit_context_json,summary_json,message,started_at,updated_at)
            VALUES (%s,%s,%s,'conv_real',%s,'queued',%s,%s,%s,'{}','已入统一队列；实际分析和执行结果请查看后续历史',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
            (managed['id'],today,end,source,actor,ai['id'],dumps(audit)))
        rid=cur.lastrowid
        cur.execute('''INSERT INTO ai_operation_tasks
            (managed_id,conversation_id,scope_key,request_token,actor_id,actor_name,task_type,run_id,scheduled_at,report_date,message,created_at,available_after)
            VALUES (%s,NULL,%s,%s,%s,%s,'daily',%s,%s,%s,'每日任务已排队，等待同ASIN前序任务结束',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
            (managed['id'],conversation_store.scope(identity)[1],digest(['daily',managed['id'],str(today)])[:32],actor,
             (initiator.get('nickname') or initiator['username'])[:100],rid,now.astimezone(timezone.utc).replace(tzinfo=None),end))
        tid=cur.lastrowid
        marker={'engine':'native-tools-v1'}
        cur.execute('UPDATE ai_operation_tasks SET input_json=%s,input_hash=%s WHERE id=%s',(dumps(marker),digest(marker),tid))
        cur.execute('SELECT * FROM ai_operation_tasks WHERE id=%s',(tid,));task=cur.fetchone()
        cur.execute('SELECT * FROM ai_operation_daily_runs WHERE id=%s',(rid,))
        return {'run':managed_store.serialize_run(cur.fetchone()),'task':chat_tasks.public(task,True),'reused':False}


def bind(task):
    """持有同ASIN作业锁后才绑定当前共享会话，排队时不冻结旧记忆或旧数据。"""
    managed,identity=conversation_queries.managed_identity(task['managed_id'])
    if conversation_store.scope(identity)[1]!=task['scope_key']:raise DomainError('排队期间店铺账户变化，未继续原任务',409)
    session=conversation_store.open_session(identity)
    with plan_store.transaction(True) as cur:
        chat_tasks.fence(cur,task['id'],task['owner_token'])
        cur.execute('UPDATE ai_operation_tasks SET conversation_id=%s WHERE id=%s',(session['id'],task['id']))
    return session
