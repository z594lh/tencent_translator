"""领取后的人工续聊编排；纯问答没有虚假方案，具体建议保存但绝不执行Amazon。"""
from contextlib import ExitStack
from datetime import timedelta
from services.listing_performance.common import DomainError
from services import operator_accounts
from . import chat_tasks as tasks, plan_store, plans, daily_runner, conversation_store, conversation
from . import prompt_configuration, prompt_compiler, plan_contract, plan_provider
from .metrics import digest,dumps


def active(task,allow_stop=False):
    """收费/提交记忆前复核人工权限、AI账号、店铺范围、当前指针与租约；暂停仍可仅问答。"""
    daily_runner.authorize_actor(task['actor_id'])
    current,session,managed=tasks.checked_task(task['id'])
    if session['state']!='active' or str(managed['current_conversation_id'])!=str(session['id']):raise DomainError('对话已归档，未继续收费分析',409)
    if managed['shop_status']!=1:raise DomainError('店铺已停用，未继续分析',409)
    with plan_store.transaction() as cur:operator_accounts.read_ai(cur,managed['executor_user_id'])
    with plan_store.transaction(True) as cur:tasks.fence(cur,task['id'],task['owner_token'],allow_stop)
    return current,session,managed


def checkpoint(cur,task,session,memory,result,usage,plan_id=None):
    """任务终态和替换记忆同事务，人工问答不清除每日完整初始化标记。"""
    tasks.fence(cur,task['id'],task['owner_token'])
    cur.execute("SELECT MAX(id) id FROM ai_operation_messages WHERE task_id=%s AND event_type='model_response'",(task['id'],))
    result={**result,'response_message_id':str(cur.fetchone()['id'])}
    cur.execute('SELECT revision,state FROM ai_operation_conversations WHERE id=%s FOR UPDATE',(session['id'],))
    current=cur.fetchone()
    if not current or current['state']!='active' or current['revision']!=session['revision']:raise DomainError('会话记忆版本变化，保留原回答，不覆盖',409)
    cur.execute('''INSERT INTO ai_operation_memory (conversation_id,revision,plan_id,task_id,covered_message_id,memory_json,memory_hash,created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
        (session['id'],session['revision']+1,plan_id,task['id'],task['user_message_id'],dumps(memory),digest(memory)))
    cur.execute('UPDATE ai_operation_conversations SET revision=revision+1,updated_at=UTC_TIMESTAMP(3) WHERE id=%s',(session['id'],))
    # 有方案时输入档案移交原plans，保留相同input_hash和明确引用；纯回答继续保存在任务。
    cur.execute('''UPDATE ai_operation_tasks SET status='completed',phase='completed',message=%s,
        result_json=%s,result_hash=%s,usage_json=%s,plan_id=%s,input_json=IF(%s IS NULL,input_json,NULL),
        completed_at=UTC_TIMESTAMP(3),lease_until=NULL WHERE id=%s''',
        ('AI已回复；建议待人工选择确认，尚未执行' if result['type']=='propose_plan' else 'AI已回复；本次没有修改平台',
         dumps(result),digest(result),dumps(usage),plan_id,plan_id,task['id']))


def fail(task,code,note,usage=None):
    """保留失败/未知事实，不自动重放收费请求；迟到实例不能覆盖其他任务代次。"""
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT status,owner_token,stop_requested FROM ai_operation_tasks WHERE id=%s FOR UPDATE',(task['id'],))
        row=cur.fetchone()
        if not row or row['status']!='running' or row['owner_token']!=task['owner_token']:return
        state='cancelled' if row['stop_requested'] else 'unknown' if code=='unknown' else 'failed'
        cur.execute('''UPDATE ai_operation_tasks SET status=%s,phase=%s,error_code=%s,message=%s,usage_json=%s,
            completed_at=UTC_TIMESTAMP(3),lease_until=NULL WHERE id=%s''',
            (state,state,code,note[:500],dumps(usage) if usage else None,task['id']))


def process(task,invoke=None,stopping=None):
    """按原ASIN双锁串行分析，与每日复用取数、编译、模型及附件；没有平台写入口。"""
    invoke=invoke or plans.analyze
    acquired=False;waiting_lock=False;plan_id=None;usage=None
    try:
        current,raw_session,managed=active(task)
        with ExitStack() as locks:
            waiting_lock=True
            locks.enter_context(daily_runner.lock(managed));locks.enter_context(plans.lock(managed['shop_id'],managed['asin']))
            acquired=True;waiting_lock=False
            current,raw_session,managed=active(task)
            now=daily_runner.clock();today=now.astimezone(daily_runner.BJ).date()
            end=min(today-timedelta(days=1),now.astimezone(daily_runner.SOURCE_ZONE).date()-timedelta(days=1))
            pack,data=plans.build_data(managed['shop_id'],managed['asin'],str(end),
                {'managed_id':managed['id'],'run_id':0,'run_date':today,'now':now})
            if conversation_store.scope(data['content']['identity'])[1]!=task['scope_key']:raise DomainError('准备资料期间账户变化，未调用AI',409)
            session=conversation_store.peek_session(data['content']['identity'])
            if str(session['id'])!=str(task['conversation_id']):raise DomainError('当前对话已变化，未调用AI',409)
            session.update(purpose='chat',chat_mode=current.get('chat_mode'),task_id=task['id'],owner_token=task['owner_token'])
            with plan_store.transaction() as cur:
                cur.execute('SELECT * FROM ai_operation_messages WHERE id=%s AND task_id=%s',(task['user_message_id'],task['id']))
                question=conversation_store.checked(cur.fetchone(),'payload_json','payload_hash')['text']
            prepared=prompt_compiler.prepare_human(data,session,prompt_configuration.active(),question,current.get('chat_mode'))
            prepared['human_actor']={'user_id':str(task['actor_id']),'name':task['actor_name'],
                                     'question_at_utc':task['created_at'],'source':'人工续聊，非每日自动任务'}
            frozen=[pack,data,prepared]
            with plan_store.transaction(True) as cur:
                tasks.fence(cur,task['id'],task['owner_token'])
                cur.execute('UPDATE ai_operation_tasks SET report_date=%s,input_json=%s,input_hash=%s WHERE id=%s',
                            (end,dumps(frozen),digest(frozen),task['id']))
            def permitted():
                """每轮调用前重查权限/租约，并响应进程退出请求，不撤销已发生计费。"""
                active(task)
                if stopping and stopping():raise DomainError('消费者正在停止，不再发起下一轮AI请求',409)
            result,usage,memory=conversation.run(data,prepared,session,None,invoke,permitted)
            active(task)
            if result['type']=='propose_plan':
                token=digest(['human-plan',task['id']])[:32]
                plan_id=plan_store.insert(pack,data,prepared,digest(['human-plan',task['id'],digest(frozen)]),token,
                    task['actor_id'],plan_contract.MODEL,plan_contract.VERSION,task_id=task['id'])
                saved_plan={**result['plan'],'execution_available':False,'runtime_mode':'real','execution_policy':'confirm_required'}
                plan_store.finish(plan_id,'ready',saved_plan,usage,message='人工对话建议已保存，待用户选择确认；本次分析不自动执行',
                    on_ready=lambda cur:checkpoint(cur,task,session,memory,result,usage,plan_id))
            else:
                with plan_store.transaction(True) as cur:checkpoint(cur,task,session,memory,result,usage)
    except DomainError as error:
        if waiting_lock and not acquired and error.status==409:
            try:tasks.requeue(task)
            except DomainError:fail(task,'stopped',str(error),usage)
            return
        fail(task,'stopped',str(error),usage)
    except plan_provider.Failure as error:fail(task,error.code,str(error),error.usage)
    except Exception:fail(task,'unknown','人工分析未正常结束；原请求和回答保留，可能已计费，不自动重试。平台没有因本次人工问答修改',usage)
    finally:
        if plan_id:
            with plan_store.transaction(True) as cur:
                cur.execute("UPDATE ai_operation_plans SET status='failed',error_code='chat_finalize_failed',message='人工建议归档未完成，未执行',completed_at=UTC_TIMESTAMP(3) WHERE id=%s AND status='generating'",(plan_id,))
