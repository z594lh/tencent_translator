"""人工行动确认：原方案不变，决策只追加；聊天文字永远不能代替明确平台授权。"""
from .language import text as model_text

import json
import re
from datetime import timedelta
from services import operator_accounts
from services.listing_performance.common import DomainError,positive_id
from . import plan_store,conversation_store,chat_tasks,daily_runner
from . import platform_operations as interfaces
from .metrics import digest,dumps

TTL_HOURS=24


def source(plan_id):
    """读取原人工建议和实际账户会话；不信任浏览器提交的身份、参数或目标值。"""
    plan=plan_store.get(positive_id(plan_id,'方案'))
    if plan.get('legacy'):raise DomainError(model_text('confirmations.99b855a91859'),410)
    if plan['status']!='ready' or plan['runtime_mode']!='real' or not plan.get('task_id'):
        raise DomainError(model_text('confirmations.06b18751ec00'),409)
    task,session,managed=chat_tasks.checked_task(plan['task_id'])
    if task['task_type']!='chat' or task.get('chat_mode')=='discussion' or task['status']!='completed' or str(task['plan_id'])!=plan['id']:
        raise DomainError('原人工建议尚未归档完成',409)
    if conversation_store.scope(plan['dataset']['content']['identity'])[1]!=task['scope_key']:
        raise DomainError(model_text('confirmations.a0c328dd74fa'),409)
    return plan,task,session,managed


def decisions(cur,plan_id):
    """只读恢复每个行动首次明确决策，保留部分确认和拒绝的不同批次。"""
    cur.execute('''SELECT c.*,t.status task_status,t.message task_message FROM ai_operation_confirmations c
        LEFT JOIN ai_operation_tasks t ON t.id=c.task_id WHERE c.plan_id=%s ORDER BY c.id''',(plan_id,))
    result={}
    for row in cur.fetchall():
        for aid in json.loads(row['action_ids_json']):result.setdefault(aid,row)
    return result


def state(cur,plan,session,managed):
    """计算动态可确认性：过期、归档、新同对象建议及后续真实写入均不能沿旧值提交。"""
    now=daily_runner.clock().replace(tzinfo=None)
    deadline=plan['completed_at']+timedelta(hours=TTL_HOURS)
    reason=None
    if now>=deadline:reason=model_text('confirmations.282996bec5a3')
    if session['state']!='active' or str(managed.get('current_conversation_id'))!=str(session['id']):reason='原对话已归档'
    if (managed['id'] and not managed['enabled']) or managed['shop_status']!=1:reason=model_text('confirmations.3839bb513cda')
    cur.execute('''SELECT p.result_json,p.result_hash FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
        WHERE t.scope_key=%s AND t.task_type='chat' AND t.status='completed' AND p.status='ready'
        AND p.id>%s AND t.conversation_id=%s ORDER BY p.id LIMIT 101''',(session['scope_key'],plan['id'],session['id']))
    newer=cur.fetchall()
    if len(newer)>100:raise DomainError(model_text('confirmations.1492cc801a4d'),422)
    superseded={a['ref'] for row in newer for a in conversation_store.checked(row,'result_json','result_hash')['actions']}
    decided=decisions(cur,plan['id'])
    cur.execute('SELECT action_id,id,status,receipt_json,verification_json,verified_at FROM ai_operation_executions WHERE plan_id=%s',(plan['id'],))
    records={r['action_id']:r for r in cur.fetchall()}
    rows=[]
    identity=plan['dataset']['content']['identity']
    for action in plan['result']['actions']:
        aid=action['action_id'];decision=decided.get(aid);record=records.get(aid);why=reason
        if action['ref'] in superseded:why=model_text('confirmations.7ac9b187a428')
        key=interfaces.resource_key(identity,action['ref'])
        cur.execute("SELECT id FROM ai_operation_executions WHERE resource_key=%s AND plan_id<>%s AND created_at>=%s AND status NOT IN ('not_sent','conflict','rejected') LIMIT 1",(key,plan['id'],plan['created_at']))
        if cur.fetchone():why=model_text('confirmations.487ec8074859')
        if action['mode']!='change' or not interfaces.supports(action):why=model_text('confirmations.44b24b138178')
        status='invalid' if why else 'pending'
        if decision:status='rejected' if decision['decision']=='reject' else decision['task_status'] or 'queued'
        if record:status=record['status']
        message=why or ('用户已拒绝' if status=='rejected' else decision['task_message'] if decision else '等待用户选择确认')
        if record:
            receipt=json.loads(record['receipt_json']) if record['receipt_json'] else {}
            verification=json.loads(record['verification_json']) if record['verification_json'] else {}
            message=(verification.get('message') if status=='confirmed' else receipt.get('message')) or '请查看该项实际执行记录'
        rows.append({'action_id':aid,'status':status,'can_confirm':not decision and not record and not why,
            'invalid_reason':why,
            'reason':message,'decided_by':json.loads(decision['actor_json']) if decision else None,
            'decided_at':decision['created_at'] if decision else None,
            'confirmation_id':str(decision['id']) if decision else None,'execution_id':str(record['id']) if record else None})
    return {'plan_id':plan['id'],'plan_hash':digest(plan['result']),'expires_at':deadline,
            'actions':rows,'source_task_id':str(plan['task_id'])}


def view(plan_id):
    """页面只读获取确认状态；查看不收费、不回读平台、不替用户执行。"""
    plan,task,session,managed=source(plan_id)
    with plan_store.transaction() as cur:return state(cur,plan,session,managed)


def records(plan_id):
    """按原人工建议范围读取完整执行回执/基线，仅点击详情加载，不轮询整份资料。"""
    from . import executions
    plan,task,session,managed=source(plan_id)
    return {'plan':{'id':plan['id'],'asin':plan['asin']},'list':executions.history(plan['id'])['list']}


def public(row,reused=False):
    """输出不可变决策及执行任务引用，不把已入队标成已修改。"""
    return {'id':str(row['id']),'plan_id':str(row['plan_id']),'decision':row['decision'],
        'action_ids':json.loads(row['action_ids_json']),'actor':json.loads(row['actor_json']),
        'task_id':str(row['task_id']) if row['task_id'] else None,'created_at':row['created_at'],'reused':reused}


def decide(plan_id,body,actor):
    """同ASIN行锁内原子确认/拒绝并入队，只接受原hash及行动编号，重复点击不重复提交。"""
    if not isinstance(body,dict) or set(body)!={'plan_hash','action_ids','decision','request_token','confirm'} or body['confirm'] is not True:
        raise DomainError('请明确确认所选原始行动')
    ids=body['action_ids'];token=body['request_token'];decision=body['decision']
    if decision not in ('execute','reject') or not isinstance(token,str) or not re.fullmatch('[a-f0-9]{32}',token):raise DomainError('决策或请求标识无效')
    if not isinstance(ids,list) or not 1<=len(ids)<=12 or any(not isinstance(a,str) or not re.fullmatch(r'a\d{1,2}',a) for a in ids) or len(ids)!=len(set(ids)):raise DomainError('请选择不重复的行动编号')
    actor=positive_id(actor,'确认人')
    from services.ai_workspace import access,products
    access.authorize(actor,'ai_chat:execute')
    plan,task,session,managed=source(plan_id)
    access.product(actor,managed,'ai_chat:execute')
    if body['plan_hash']!=digest(plan['result']):raise DomainError(model_text('confirmations.27efcb674ddc'),409)
    with plan_store.transaction(True) as cur:
        latest=products.lock_current(cur,managed)
        cur.execute('SELECT * FROM ai_operation_confirmations WHERE request_token=%s',(token,));old=cur.fetchone()
        if old:
            if old['plan_id']!=int(plan['id']) or old['actor_id']!=actor or old['decision']!=decision or set(json.loads(old['action_ids_json']))!=set(ids) or old['plan_hash']!=body['plan_hash']:
                raise DomainError(model_text('confirmations.9db22ad170c8'),409)
            return public(old,True)
        cur.execute('SELECT * FROM ai_operation_conversations WHERE id=%s',(session['id'],));session=cur.fetchone()
        managed={**managed,**latest}
        current=state(cur,plan,session,managed);lookup={a['action_id']:a for a in current['actions']}
        if any(a not in lookup for a in ids):raise DomainError('行动不属于原方案',409)
        chosen=[lookup[a] for a in ids]
        # 不同用户同时确认同一集合时返回第一次决策，而非再次登记行动。
        batches={r['confirmation_id'] for r in chosen}
        if len(batches)==1 and None not in batches:
            cur.execute('SELECT * FROM ai_operation_confirmations WHERE id=%s',(next(iter(batches)),));old=cur.fetchone()
            if old['decision']==decision and set(json.loads(old['action_ids_json']))==set(ids):return public(old,True)
        if any(not r['can_confirm'] for r in chosen):raise DomainError(model_text('confirmations.53ebfe87a247'),409)
        if decision=='execute':
            if not chat_tasks.health()['online']:raise DomainError(model_text('confirmations.dee25ee9d9d5'),503)
            validate_dependencies(plan,ids,lookup)
        person=operator_accounts.get_user(cur,actor)
        if person['account_type']!='human' or person['status']!=1:raise DomainError('需要有效人工确认人',403)
        cur.execute('''INSERT INTO ai_operation_confirmations(plan_id,plan_hash,action_ids_json,decision,request_token,actor_id,actor_json,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
            (plan['id'],body['plan_hash'],dumps(ids),decision,token,actor,dumps(operator_accounts.snapshot(person))))
        cid=cur.lastrowid
        if decision=='execute':
            managed['executor_user_id']=operator_accounts.read_ai(cur,managed.get('executor_user_id'))['id']
            frozen={'executor_user_id':str(managed['executor_user_id']),
                    'proposed_by':{'id':int(task['actor_id']),'name':task['actor_name']},'plan_hash':body['plan_hash']}
            cur.execute('''INSERT INTO ai_operation_tasks
                (managed_id,conversation_id,scope_key,request_token,actor_id,actor_name,task_type,confirmation_id,plan_id,input_json,input_hash,message,created_at,available_after)
                VALUES (%s,%s,%s,%s,%s,%s,'execute',%s,%s,%s,%s,'人工已确认，等待正式执行；不会重新调用AI',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
                (managed['id'],session['id'],task['scope_key'],digest(['confirmation',cid])[:32],actor,
                 (person.get('nickname') or person['username'])[:100],cid,plan['id'],dumps(frozen),digest(frozen)))
            cur.execute('UPDATE ai_operation_confirmations SET task_id=%s WHERE id=%s',(cur.lastrowid,cid))
        payload={'confirmation_id':str(cid),'plan_id':plan['id'],'decision':decision,'action_ids':ids,
                 'actor':operator_accounts.snapshot(person),'note':model_text('confirmations.90c2c0cd8544') if decision=='reject' else model_text('confirmations.154114bdaee1')}
        cur.execute('''INSERT INTO ai_operation_messages(conversation_id,plan_id,round_no,event_type,payload_json,payload_hash,created_at)
            VALUES (%s,%s,%s,'human_decision',%s,%s,UTC_TIMESTAMP(3))''',(session['id'],plan['id'],cid,dumps(payload),digest(payload)))
        cur.execute('SELECT * FROM ai_operation_confirmations WHERE id=%s',(cid,))
        return public(cur.fetchone())


def validate_dependencies(plan,ids,lookup):
    """亮点依赖同SKU标题时要求一起确认或标题已真实确认，不能只勾选后项。"""
    for action in plan['result']['actions']:
        if action['action_id'] not in ids or action['kind']!='listing_text_update' or action['subject']['field']!='title_differentiation':continue
        titles=[a for a in plan['result']['actions'] if a['mode']=='change' and a['kind']=='listing_text_update'
                and a['subject']['field']=='item_name' and a['subject']['sku']==action['subject']['sku']]
        if any(a['action_id'] not in ids and lookup[a['action_id']]['status']!='confirmed' for a in titles):
            raise DomainError(model_text('confirmations.588e26941d8d'),409)


def blocks_daily(plan,action,managed_id):
    """有效未处理/拒绝/排队人工建议优先，自动流程不能绕过人工决策修改同对象。"""
    sid=plan['ai_call']['prompt']['conversation']['id']
    cutoff=daily_runner.clock().replace(tzinfo=None)-timedelta(hours=TTL_HOURS)
    with plan_store.transaction() as cur:
        cur.execute('''SELECT p.id,p.prompt_json,p.result_json,p.result_hash FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
            WHERE t.conversation_id=%s AND t.task_type='chat' AND t.status='completed'
            AND p.status='ready' AND p.completed_at>%s ORDER BY p.id DESC LIMIT 101''',(sid,cutoff))
        rows=cur.fetchall()
        if len(rows)>100:raise DomainError(model_text('confirmations.1fab164df42c'),409)
        for row in rows:
            from .protocol import native_prompt
            if not native_prompt(row['prompt_json']):continue
            actions=conversation_store.checked(row,'result_json','result_hash')['actions']
            matched=next((a for a in actions if a['ref']==action['ref']),None)
            if not matched:continue
            if matched['mode']!='change':return False
            cur.execute('SELECT id FROM ai_operation_executions WHERE plan_id=%s AND action_id=%s',(row['id'],matched['action_id']))
            return not bool(cur.fetchone())
    return False


def require_execution(cur,plan,proof,action=None):
    """最终执行器再次验证服务端确认凭据及存活租约，不允许内部误调绕过显式授权。"""
    if str(proof.get('plan_id'))!=str(plan['id']):raise DomainError('确认身份与方案不一致',409)
    cur.execute('''SELECT c.*,t.owner_token,t.status task_status,t.actor_id confirmer,t.input_json,t.input_hash,
        t.lease_until>UTC_TIMESTAMP(3) lease_valid,t.stop_requested FROM ai_operation_confirmations c
        JOIN ai_operation_tasks t ON t.id=c.task_id WHERE c.id=%s AND t.id=%s AND t.task_type='execute' ''',
        (proof['confirmation_id'],proof['task_id']))
    batch=cur.fetchone()
    if not batch or batch['decision']!='execute' or batch['task_status']!='running' or not batch['lease_valid'] or batch['stop_requested']:
        raise DomainError(model_text('confirmations.9b79adb58dc4'),409)
    frozen=conversation_store.checked(batch,'input_json','input_hash')
    if batch['plan_id']!=int(plan['id']) or batch['plan_hash']!=digest(plan['result']) or str(proof['executor']['id'])!=frozen['executor_user_id'] or batch['confirmer']!=proof['initiated_by']['id']:
        raise DomainError(model_text('confirmations.50c80fac97ef'),409)
    if action and action['action_id'] not in json.loads(batch['action_ids_json']):raise DomainError(model_text('confirmations.f4f8447bdeae'),403)


def context(managed_id,scope_key=None):
    """供每日/人工首包恢复有效人工决策，逐项保留未选、拒绝、排队和真实执行状态。"""
    cutoff=daily_runner.clock().replace(tzinfo=None)-timedelta(hours=TTL_HOURS)
    with plan_store.transaction() as cur:
        cur.execute('''SELECT p.id,p.prompt_json,p.result_json,p.result_hash,t.conversation_id,t.actor_name,p.completed_at
            FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
            JOIN ai_operation_conversations c ON c.id=t.conversation_id AND c.state='active'
            WHERE t.scope_key=%s AND t.task_type='chat' AND t.status='completed' AND p.status='ready'
              AND p.completed_at>%s ORDER BY p.id DESC LIMIT 101''',(scope_key,cutoff))
        rows=cur.fetchall()
        if len(rows)>100:raise DomainError(model_text('confirmations.db3c406b7350'),422)
        result=[];seen=set()
        for row in rows:
            from .protocol import native_prompt
            if not native_prompt(row['prompt_json']):continue
            decided=decisions(cur,row['id'])
            cur.execute('SELECT action_id,id,status FROM ai_operation_executions WHERE plan_id=%s',(row['id'],))
            records={r['action_id']:r for r in cur.fetchall()}
            for action in conversation_store.checked(row,'result_json','result_hash')['actions']:
                if action['ref'] in seen:continue
                seen.add(action['ref']);aid=action['action_id'];decision=decided.get(aid);record=records.get(aid)
                result.append({'plan_ref':'plan:'+str(row['id']),'action_id':aid,'ref':action['ref'],'label':action['label'],
                    'proposed_by':row['actor_name'],'mode':action['mode'],
                    'status':record['status'] if record else 'rejected' if decision and decision['decision']=='reject' else decision['task_status'] if decision else 'pending_confirmation',
                    'execution_id':str(record['id']) if record else None,
                    'note':model_text('confirmations.87540054c24e')})
        return result
