"""人工确认执行编排：不调用模型，不造每日账本，复用正式前值预检/写入/回读。"""
import json
from services.listing_performance.common import DomainError
from services.operation_identity import ai_operation
from . import confirmations,conversation_store,chat_tasks,plan_store,plans,daily_runner,managed_store,executions
from .metrics import dumps,digest


def process(task,permitted):
    """在同ASIN双锁内执行已确认子集；依赖未确认则不提交后项，部分失败逐项保留。"""
    plan,origin,session,managed=confirmations.source(task['plan_id'])
    with daily_runner.lock(managed),plans.lock(managed['shop_id'],managed['asin']):
        frozen=conversation_store.checked(task,'input_json','input_hash')
        with plan_store.transaction() as cur:
            cur.execute('SELECT * FROM ai_operation_confirmations WHERE id=%s AND task_id=%s',(task['confirmation_id'],task['id']))
            batch=cur.fetchone()
        if not batch or batch['decision']!='execute':raise DomainError('缺少原人工确认批次',409)
        ids=json.loads(batch['action_ids_json']);actions=[a for a in plan['result']['actions'] if a['action_id'] in ids]
        if len(actions)!=len(ids):raise DomainError('确认行动与原方案不符',409)
        def active():
            """每次提交前重查权限、启停、原会话、期限、执行身份和较新同对象结论。"""
            permitted()
            current,_,latest=chat_tasks.checked_task(task['id'])
            from services import operator_accounts
            with plan_store.transaction() as cur:executor=operator_accounts.read_ai(cur,latest.get('executor_user_id'))
            latest['executor_user_id']=executor['id']
            if str(latest['executor_user_id'])!=frozen['executor_user_id']:raise DomainError('AI执行人已变化，请重新分析并确认',409)
            if latest['id']:managed_store.execution_identity(latest)
            snapshot=confirmations.view(plan['id'])
            rows={a['action_id']:a for a in snapshot['actions']}
            if any(rows[aid]['invalid_reason'] for aid in ids):raise DomainError('原建议已失效或托管暂停；未继续提交，请重新分析',409)
        active()
        chat_tasks.phase(task['id'],task['owner_token'],'executing','正在执行已确认行动，不调用AI；每项先核对实时前值')
        try:
            for action in executions.order_actions(actions):
                active()
                operation={'listing_text_update':'text','listing_price_update':'price'}.get(action['kind'],'update')
                with ai_operation(frozen['executor_user_id'],task['actor_id'],None,action['action_id'],operation,
                    task_id=task['id'],plan_id=plan['id'],confirmation_id=task['confirmation_id'],
                    conversation_id=task['conversation_id'],proposed_by=frozen['proposed_by']):
                    # 若同批标题未真实确认，亮点不根据“受理”推断依赖已经生效。
                    titles=[a for a in actions if a['kind']=='listing_text_update' and a['subject']['field']=='item_name'
                        and a['subject'].get('sku')==action['subject'].get('sku')]
                    records={r['action_id']:r for r in executions.history(plan['id'])['list']}
                    if action['kind']=='listing_text_update' and action['subject']['field']=='title_differentiation' and any(records.get(a['action_id'],{}).get('status')!='confirmed' for a in titles):
                        executions.hold(plan,action,frozen['executor_user_id'],'同批标题尚未回读确认或执行失败，本亮点未发送')
                        continue
                    result=executions.execute(plan['id'],{'action_ids':[action['action_id']]},frozen['executor_user_id'],active)['list'][0]
                    if result['status'] in ('submitting','submitted','pending','unknown'):executions.verify(result['id'])
        finally:
            records=executions.history(plan['id'])['list']
            selected=[r for r in records if r['action_id'] in ids]
            feedback(plan,task,batch,selected)
        with plan_store.transaction(True) as cur:
            chat_tasks.fence(cur,task['id'],task['owner_token'],allow_stop=True)
            result={'type':'execution_result','confirmation_id':str(batch['id']),
                    'executions':[{'id':r['id'],'action_id':r['action_id'],'status':r['status']} for r in selected]}
            note='已确认行动处理完毕；'+('全部回读一致' if len(selected)==len(ids) and all(r['status']=='confirmed' for r in selected) else '部分行动未发送或待核对，以逐项执行记录为准')
            cur.execute('''UPDATE ai_operation_tasks SET status='completed',phase='completed',message=%s,
                result_json=%s,result_hash=%s,completed_at=UTC_TIMESTAMP(3),lease_until=NULL WHERE id=%s''',
                (note,dumps(result),digest(result),task['id']))


def feedback(plan,task,batch,records):
    """每批次追加执行事实，保留未确认项，不覆盖AI原回复或替换工作记忆。"""
    payload={'confirmation_id':str(batch['id']),'task_id':str(task['id']),'plan_id':plan['id'],
        'message':'人工确认批次执行结果；未选择行动没有执行，不因用户确认就认定平台成功',
        'confirmed_by':json.loads(batch['actor_json']),
        'executions':[{'execution_id':r['id'],'action_id':r['action_id'],'label':r['action']['label'],
            'ref':r['action']['ref'],'before':r['action']['before'],'after':r['action']['after'],
            'status':r['status'],'verified_at':r['verified_at']} for r in records]}
    conversation_store.append(task['conversation_id'],plan['id'],int(batch['id']),'confirmation_feedback',payload)
