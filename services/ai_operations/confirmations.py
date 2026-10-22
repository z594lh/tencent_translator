"""人工行动确认：原方案不变，决策只追加；聊天文字永远不能代替明确平台授权。"""
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
    if plan['status']!='ready' or plan['runtime_mode']!='real' or not plan.get('task_id'):
        raise DomainError('只能确认已完成的人工对话建议',409)
    task,session,managed=chat_tasks.checked_task(plan['task_id'])
    if task['task_type']!='chat' or task['status']!='completed' or str(task['plan_id'])!=plan['id']:
        raise DomainError('原人工建议尚未归档完成',409)
    if conversation_store.scope(plan['dataset']['content']['identity'])[1]!=task['scope_key']:
        raise DomainError('建议账户范围与当前配置不符',409)
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
    if now>=deadline:reason='建议已超过24小时，请重新分析'
    if session['state']!='active' or str(managed.get('current_conversation_id'))!=str(session['id']):reason='原对话已归档'
    if not managed['enabled'] or managed['shop_status']!=1:reason='托管或店铺已暂停，恢复并复核后才能确认'
    cur.execute('''SELECT p.result_json,p.result_hash FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
        WHERE t.managed_id=%s AND t.task_type='chat' AND t.status='completed' AND p.status='ready'
        AND p.id>%s AND t.conversation_id=%s ORDER BY p.id LIMIT 101''',(managed['id'],plan['id'],session['id']))
    newer=cur.fetchall()
    if len(newer)>100:raise DomainError('后续建议超过100份，请新建对话再分析',422)
    superseded={a['ref'] for row in newer for a in conversation_store.checked(row,'result_json','result_hash')['actions']}
    decided=decisions(cur,plan['id'])
    cur.execute('SELECT action_id,id,status,receipt_json,verification_json,verified_at FROM ai_operation_executions WHERE plan_id=%s',(plan['id'],))
    records={r['action_id']:r for r in cur.fetchall()}
    rows=[]
    identity=plan['dataset']['content']['identity']
    for action in plan['result']['actions']:
        aid=action['action_id'];decision=decided.get(aid);record=records.get(aid);why=reason
        if action['ref'] in superseded:why='已有较新的同对象人工建议，旧项不再确认'
        key=interfaces.resource_key(identity,action['ref'])
        cur.execute("SELECT id FROM ai_operation_executions WHERE resource_key=%s AND plan_id<>%s AND created_at>=%s AND status NOT IN ('not_sent','conflict','rejected') LIMIT 1",(key,plan['id'],plan['created_at']))
        if cur.fetchone():why='该对象已有后续实际提交，请重新分析'
        if action['mode']!='change' or not interfaces.supports(action):why='观察项或尚未支持的行动不执行'
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
    actor=positive_id(actor,'确认人');daily_runner.authorize_actor(actor)
    plan,task,session,managed=source(plan_id)
    if body['plan_hash']!=digest(plan['result']):raise DomainError('方案版本已变化，请刷新原建议',409)
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT * FROM ai_operation_managed_asins WHERE id=%s FOR UPDATE',(managed['id'],));latest=cur.fetchone()
        cur.execute('SELECT * FROM ai_operation_confirmations WHERE request_token=%s',(token,));old=cur.fetchone()
        if old:
            if old['plan_id']!=int(plan['id']) or old['actor_id']!=actor or old['decision']!=decision or set(json.loads(old['action_ids_json']))!=set(ids) or old['plan_hash']!=body['plan_hash']:
                raise DomainError('确认标识已用于另一项决策',409)
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
        if any(not r['can_confirm'] for r in chosen):raise DomainError('部分行动已处理或失效，请刷新后重新选择',409)
        if decision=='execute':
            if not chat_tasks.health()['online']:raise DomainError('消费者离线，本次未确认执行',503)
            validate_dependencies(plan,ids,lookup)
        person=operator_accounts.get_user(cur,actor)
        if person['account_type']!='human' or person['status']!=1:raise DomainError('需要有效人工确认人',403)
        cur.execute('''INSERT INTO ai_operation_confirmations(plan_id,plan_hash,action_ids_json,decision,request_token,actor_id,actor_json,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
            (plan['id'],body['plan_hash'],dumps(ids),decision,token,actor,dumps(operator_accounts.snapshot(person))))
        cid=cur.lastrowid
        if decision=='execute':
            frozen={'executor_user_id':str(managed['executor_user_id']),
                    'proposed_by':{'id':int(task['actor_id']),'name':task['actor_name']},'plan_hash':body['plan_hash']}
            cur.execute('''INSERT INTO ai_operation_tasks
                (managed_id,conversation_id,scope_key,request_token,actor_id,actor_name,task_type,confirmation_id,plan_id,input_json,input_hash,message,created_at,available_after)
                VALUES (%s,%s,%s,%s,%s,%s,'execute',%s,%s,%s,%s,'人工已确认，等待正式执行；不会重新调用AI',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
                (managed['id'],session['id'],task['scope_key'],digest(['confirmation',cid])[:32],actor,
                 (person.get('nickname') or person['username'])[:100],cid,plan['id'],dumps(frozen),digest(frozen)))
            cur.execute('UPDATE ai_operation_confirmations SET task_id=%s WHERE id=%s',(cur.lastrowid,cid))
        payload={'confirmation_id':str(cid),'plan_id':plan['id'],'decision':decision,'action_ids':ids,
                 'actor':operator_accounts.snapshot(person),'note':'明确拒绝，未修改平台' if decision=='reject' else '明确确认后进入执行队列，不代表平台已接受'}
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
            raise DomainError('该亮点依赖同SKU标题，请一起选择标题，或等待标题回读确认',409)


def blocks_daily(plan,action,managed_id):
    """有效未处理/拒绝/排队人工建议优先，自动流程不能绕过人工决策修改同对象。"""
    sid=plan['ai_call']['prompt']['conversation']['id']
    cutoff=daily_runner.clock().replace(tzinfo=None)-timedelta(hours=TTL_HOURS)
    with plan_store.transaction() as cur:
        cur.execute('''SELECT p.id,p.result_json,p.result_hash FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
            WHERE t.managed_id=%s AND t.conversation_id=%s AND t.task_type='chat' AND t.status='completed'
            AND p.status='ready' AND p.completed_at>%s ORDER BY p.id DESC LIMIT 101''',(managed_id,sid,cutoff))
        rows=cur.fetchall()
        if len(rows)>100:raise DomainError('待处理人工建议过多，停止自动修改并先核对对话',409)
        for row in rows:
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
        raise DomainError('人工确认执行任务不存在、已停止或租约失效',409)
    frozen=conversation_store.checked(batch,'input_json','input_hash')
    if batch['plan_id']!=int(plan['id']) or batch['plan_hash']!=digest(plan['result']) or str(proof['executor']['id'])!=frozen['executor_user_id'] or batch['confirmer']!=proof['initiated_by']['id']:
        raise DomainError('确认原文或执行身份不符，未提交',409)
    if action and action['action_id'] not in json.loads(batch['action_ids_json']):raise DomainError('该行动未被明确确认，未提交',403)


def context(managed_id):
    """供每日/人工首包恢复有效人工决策，逐项保留未选、拒绝、排队和真实执行状态。"""
    cutoff=daily_runner.clock().replace(tzinfo=None)-timedelta(hours=TTL_HOURS)
    with plan_store.transaction() as cur:
        cur.execute('''SELECT p.id,p.result_json,p.result_hash,t.conversation_id,t.actor_name,p.completed_at
            FROM ai_operation_plans p JOIN ai_operation_tasks t ON t.id=p.task_id
            JOIN ai_operation_managed_asins m ON m.id=t.managed_id AND m.current_conversation_id=t.conversation_id
            WHERE t.managed_id=%s AND t.task_type='chat' AND t.status='completed' AND p.status='ready'
              AND p.completed_at>%s ORDER BY p.id DESC LIMIT 101''',(managed_id,cutoff))
        rows=cur.fetchall()
        if len(rows)>100:raise DomainError('有效人工方案超过100份，请处理或新建对话后再分析',422)
        result=[];seen=set()
        for row in rows:
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
                    'note':'未确认/拒绝/排队不等于已执行；同对象待人工决策时每日自动修改会暂停，该事实优先于旧模型记忆'})
        return result
