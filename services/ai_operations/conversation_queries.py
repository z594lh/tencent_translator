"""共享会话的只读分页与明确新建；不调用模型或Amazon，不篡改旧会话原文。"""
from datetime import timezone
from services.listing_performance.common import DomainError, positive_id
from . import plan_store, conversation_store as store, daily_runner, plans, message_documents, chat_tasks

STATUS = {'not_sent':'未发送','confirmed':'属性已回读确认','rejected':'被拒绝','unknown':'结果待核实',
          'submitted':'已提交待回读','pending':'等待平台处理','preparing':'准备中','submitting':'提交中','issues':'平台处理有问题',
          'conflict':'前值/依赖或人工策略冲突·未发送','superseded':'已被后续操作替代'}


def managed_identity(managed_id):
    """只取当前账户范围，无凭据密钥；后续会话编号必须再核对此边界。"""
    with plan_store.transaction() as cur:
        cur.execute('''SELECT m.*,s.credential_group_id,s.region,s.ads_profile_id,s.shop_name,s.status shop_status,s.marketplace_id account_marketplace
            FROM ai_operation_managed_asins m JOIN amazon_shops s ON s.id=m.shop_id WHERE m.id=%s''',
            (positive_id(managed_id,'托管编号'),))
        managed=cur.fetchone()
    if not managed:raise DomainError('托管ASIN不存在',404)
    if managed['account_marketplace']!=managed['marketplace_id']:raise DomainError('店铺站点已变化，请先核对托管范围',409)
    identity={key:managed[key] for key in ('credential_group_id','region','marketplace_id','ads_profile_id','asin')}
    identity['id']=managed['shop_id']
    return managed,identity


def public_session(row):
    """只返回会话目录元信息，不将账户JSON或完整记忆夹在每页响应内。"""
    fields=('id','generation','state','title','revision','created_at','updated_at','archived_at',
            'daily_baseline_pending','baseline_plan_id')
    return {**{key:str(row[key]) if key in ('id','baseline_plan_id') and row[key] is not None else
            bool(row[key]) if key=='daily_baseline_pending' else row[key] for key in fields},'last_message_at':row.get('last_message_at')}


def listing(managed_id,params):
    """分页查看当前账户下的历次会话；GET不创建会话、不调用AI。"""
    managed,identity=managed_identity(managed_id);boundary,key=store.scope(identity)
    before=positive_id(params['before_id'],'会话游标') if params.get('before_id') else None
    with plan_store.transaction() as cur:
        cur.execute('''SELECT c.*,(SELECT MAX(e.created_at) FROM ai_operation_messages e WHERE e.conversation_id=c.id) last_message_at
            FROM ai_operation_conversations c WHERE scope_key=%s'''+(' AND c.id<%s' if before else '')+
                    ' ORDER BY c.id DESC LIMIT 21',(key,before) if before else (key,))
        rows=cur.fetchall()
    return {'list':[public_session(row) for row in rows[:20]],'has_more':len(rows)>20,
            'next_before_id':str(rows[19]['id']) if len(rows)>20 else None,
            'current_id':str(managed['current_conversation_id']) if managed['current_conversation_id'] else None,
            'capabilities':{'read':True,'new_conversation':True,'human_chat':True,'confirm_execution':True,
                'note':'人工分析会计费；建议须选择并确认才执行，18:30每日与人工任务共用持久队列'},
            'worker':chat_tasks.health()}


def require_session(session_id):
    """回查会话所属托管和当前真实账户，禁止跨店/已换凭证的历史被拼入新会话。"""
    with plan_store.transaction() as cur:
        cur.execute('''SELECT c.*,m.id managed_id FROM ai_operation_conversations c
            JOIN ai_operation_managed_asins m ON m.shop_id=c.shop_id AND BINARY m.asin=BINARY c.asin
              AND m.marketplace_id=JSON_UNQUOTE(JSON_EXTRACT(c.identity_json,'$.marketplace_id'))
            WHERE c.id=%s''',(positive_id(session_id,'会话编号'),))
        session=cur.fetchone()
    if not session:raise DomainError('会话不存在',404)
    managed,identity=managed_identity(session['managed_id'])
    if session['scope_key']!=store.scope(identity)[1]:raise DomainError('会话账户配置已变化，不允许跨账户读取',409)
    return session,managed


def create(managed_id,body,actor):
    """显式归档当前会话并原子切换；复用每日/方案互斥，不重置每日幂等或执行记录。"""
    if not isinstance(body,dict) or set(body)!={'expected_current_id','confirm_new'} or body['confirm_new'] is not True:
        raise DomainError('请明确确认新建会话，保留旧历史与真实执行')
    expected=positive_id(body['expected_current_id'],'当前会话') if body['expected_current_id'] is not None else None
    managed,identity=managed_identity(managed_id);boundary,key=store.scope(identity)
    if managed['shop_status']!=1:raise DomainError('店铺已停用，不新建运营会话',409)
    with daily_runner.lock(managed),plans.lock(managed['shop_id'],managed['asin']):
        with plan_store.transaction(True) as cur:
            cur.execute('SELECT current_conversation_id FROM ai_operation_managed_asins WHERE id=%s FOR UPDATE',(managed['id'],))
            old_id=cur.fetchone()['current_conversation_id']
            if old_id!=expected:raise DomainError('当前会话已被其他用户切换，请刷新后查看，不重复新建',409)
            cur.execute("SELECT id FROM ai_operation_plans WHERE shop_id=%s AND BINARY asin=BINARY %s AND status='generating' LIMIT 1",(managed['shop_id'],managed['asin']))
            if cur.fetchone():raise DomainError('该ASIN仍有生成中或结果待核实的分析，请先核对历史后再新建',409)
            cur.execute("SELECT id FROM ai_operation_tasks WHERE managed_id=%s AND status IN ('queued','running') LIMIT 1",(managed['id'],))
            if cur.fetchone():raise DomainError('还有分析、每日或确认执行任务排队/运行，请等待或取消后再新建对话',409)
            cur.execute("SELECT id FROM ai_operation_executions WHERE shop_id=%s AND BINARY asin=BINARY %s AND runtime_mode='real' AND status IN ('preparing','submitting') LIMIT 1",(managed['shop_id'],managed['asin']))
            if cur.fetchone():raise DomainError('该ASIN仍有执行中的改动，请等待本次执行结束',409)
            # 再读账户，避免等待命名锁期间店铺被切换Profile/凭证。
            cur.execute('SELECT credential_group_id,region,marketplace_id,ads_profile_id,status FROM amazon_shops WHERE id=%s FOR UPDATE',(managed['shop_id'],))
            latest=cur.fetchone()
            if not latest or latest['status']!=1 or any(str(latest[k] or '')!=str(identity[k] or '') for k in ('credential_group_id','region','marketplace_id','ads_profile_id')):
                raise DomainError('店铺账户配置已变化，请刷新后新建',409)
            if old_id:
                cur.execute('SELECT shop_id,asin,state FROM ai_operation_conversations WHERE id=%s FOR UPDATE',(old_id,))
                old=cur.fetchone()
                if not old or old['shop_id']!=managed['shop_id'] or old['asin']!=managed['asin'] or old['state']!='active':
                    raise DomainError('当前会话指针异常，请先核对配置；不会归档其他商品会话',409)
                cur.execute("UPDATE ai_operation_conversations SET state='archived',archived_at=UTC_TIMESTAMP(3),archived_by=%s WHERE id=%s AND state='active'",(positive_id(actor,'用户'),old_id))
            sid=store.create_session(cur,identity,boundary,key)
            cur.execute('UPDATE ai_operation_managed_asins SET current_conversation_id=%s WHERE id=%s',(sid,managed['id']))
            cur.execute('SELECT * FROM ai_operation_conversations WHERE id=%s',(sid,))
            return public_session(cur.fetchone())


def describe(row,plan,task=None):
    """将一条不可变原事件映射为人可读消息；不重放嵌套历史、不执行模型HTML。"""
    payload=store.checked(row,'payload_json','payload_hash');kind=row['event_type']
    result={'id':str(row['id']),'plan_id':str(row['plan_id']) if row['plan_id'] is not None else None,
            'event_type':kind,
            'task_id':str(row['task_id']) if row.get('task_id') is not None else None,'role':'erp','round':row['round_no'],
            'created_at':row['created_at'].replace(tzinfo=timezone.utc).isoformat(),'title':'ERP',
            'text':'','has_original':True,'attachment_name':'原始消息与资料.json','actor':plan.get('actor_name')}
    if kind=='user_message':
        result.update(role='user',title=payload['actor_name'],text=payload['text'],actor=None,attachment_name='用户原话与身份.json')
    elif kind=='model_request':
        actual=payload.get('messages',[])
        questions=[r.get('content','') for r in actual if r.get('role')=='user' and not r.get('content','').lstrip().startswith(('{','['))]
        result.update(title='ERP · 每日例行分析' if row['round_no']==1 else 'ERP · 继续分析',
            text=questions[0] if row['round_no']==1 and questions else payload.get('control',{}).get('instruction','继续分析已提供资料'),
            attachment_name='本轮真实问题、规则及运营资料.json')
        if task and task['task_type']=='chat':result['title']='ERP · 人工问题分析' if row['round_no']==1 else 'ERP · 补查后继续回答'
    elif kind=='model_response':
        result.update(role='assistant',title='DeepSeek')
        answer=payload.get('response') or {}
        result['response_type']=answer.get('type') if isinstance(answer,dict) else None
        result['has_error']=bool(payload.get('error_code'))
        if not isinstance(answer,dict):
            result['text']='模型输出格式无效，未形成可执行方案；可查看完整原文。'
            return result
        if payload.get('error_code'):
            usage=payload.get('usage') or {}
            readable=usage.get('readable_reply')
            if not readable and payload['error_code']=='invalid_output':
                from .plan_provider import readable_reply
                readable=readable_reply(usage.get('raw_output',''))
            result['text']=readable or payload.get('message') or '模型请求未完成，原始结果已保留'
            result['error_note']=payload.get('message') or '此回答未形成可执行方案'
        elif answer.get('type')=='request_data':
            requests=answer.get('requests')
            result['text']='需要补充以下资料后继续分析：\n'+'\n'.join(str(r.get('reason',''))+'（'+str(r.get('source',''))+' / '+str(r.get('window',''))+'）' for r in requests) if isinstance(requests,list) and all(isinstance(r,dict) for r in requests) else '模型资料请求格式无效，请查看原文'
        elif answer.get('type') in ('reply','propose_plan'):
            result['text']=answer.get('reply') if isinstance(answer.get('reply'),str) else '人工回复格式异常，请查看原文'
        else:
            value=answer.get('plan') or {}
            if not isinstance(value,dict) or not isinstance(value.get('summary',''),str) or not isinstance(value.get('actions',[]),list) or any(not isinstance(a,dict) for a in value.get('actions',[])):
                result['text']='模型方案格式无效，未执行；请查看完整原文与校验反馈。'
                return result
            parts=[value.get('summary','')]
            labels={a['ref']:a.get('label',a['ref']) for a in plan.get('actions',[])}
            for i,a in enumerate(value.get('actions',[]),1):
                ref=a.get('ref');label=str(labels.get(ref,ref)) if isinstance(ref,str) else '无法识别的行动对象'
                parts.append(str(i)+'. '+label+' · '+('建议修改（不代表已执行）' if a.get('mode')=='change' else '观察'))
                if a.get('mode')=='change':parts.append('建议值：'+ ('\n'.join(str(v) for v in a['after']) if isinstance(a.get('after'),list) else str(a.get('after'))))
                parts.append(str(a.get('reason','')))
            for review in value.get('history_reviews',[]) if isinstance(value.get('history_reviews'),list) else []:
                if isinstance(review,dict):parts.append('往期执行 #'+str(review.get('execution_id',''))+'：'+str(review.get('decision',''))+'\n'+str(review.get('conclusion','')))
            result['text']='\n\n'.join(p for p in parts if p) or '本轮回答内容请查看原文'
        result['usage']={k:payload.get('usage',{}).get(k) for k in ('prompt_tokens','completion_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens')}
    elif kind=='data_response':
        result.update(title='ERP · 补充资料',text='\n'.join(str(r.get('request',{}).get('reason',''))+'：'+str(r.get('result',r).get('status','已提供')) for r in payload.get('data_responses',[])),attachment_name='本轮补充资料.json')
    elif kind=='validation_feedback':result.update(title='ERP · 方案校验反馈',text=payload.get('validation_feedback',{}).get('message','请查看校验原文'))
    elif kind=='human_decision':
        result.update(role='user',title=payload['actor']['name'],actor=None,
            text=('已确认执行' if payload['decision']=='execute' else '已明确拒绝')+'行动：'+', '.join(payload['action_ids'])+'。'+payload['note'])
    elif kind in ('execution_feedback','execution_recovery','confirmation_feedback'):
        lines=[payload.get('message',''),payload.get('note','')]
        for r in payload.get('executions',[]):lines.append(str(r.get('label',r.get('execution_id','')))+'：'+STATUS.get(r.get('status'),r.get('status','待核实')))
        result.update(title='ERP · 人工确认执行结果' if kind=='confirmation_feedback' else 'ERP · 真实执行结果' if kind=='execution_feedback' else 'ERP · 补执行结果',text='\n'.join(x for x in lines if x))
    else:result['text']='已保存事件，请查看原文'
    return result


def messages(session_id,params):
    """按稳定事件ID游标分页，列表只传阅读版，完整messages及附件点击时单独读取。"""
    session,managed=require_session(session_id)
    before=positive_id(params['before_id'],'消息游标') if params.get('before_id') else None
    after=positive_id(params['after_id'],'消息游标') if params.get('after_id') else None
    if before and after:raise DomainError('不能同时使用向前和向后消息游标')
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_messages WHERE conversation_id=%s'+(' AND id<%s' if before else ' AND id>%s' if after else '')+
                    (' ORDER BY id ASC LIMIT 21' if after else ' ORDER BY id DESC LIMIT 21'),
                    (session['id'],before or after) if before or after else (session['id'],))
        rows=list(cur.fetchall());more=len(rows)>20;rows=rows[:20]
        if not after:rows.reverse()
        plans_by_id={}
        for pid in {r['plan_id'] for r in rows if r['plan_id'] is not None}:
            cur.execute('''SELECT p.result_json,p.result_hash,COALESCE(NULLIF(u.nickname,''),u.username) actor_name
                FROM ai_operation_plans p LEFT JOIN users u ON u.id=p.actor_id WHERE p.id=%s''',(pid,))
            p=cur.fetchone()
            if not p:raise DomainError('会话消息关联方案缺失，请核对历史',503)
            plans_by_id[pid]={'actions':store.checked(p,'result_json','result_hash').get('actions',[]) if p['result_json'] else [],'actor_name':p['actor_name']}
        items=[]
        from . import chat_tasks
        tasks_by_id={}
        for tid in {r['task_id'] for r in rows if r.get('task_id') is not None}:
            cur.execute('SELECT * FROM ai_operation_tasks WHERE id=%s AND conversation_id=%s',(tid,session['id']))
            task=cur.fetchone()
            if not task:raise DomainError('人工消息关联任务缺失',503)
            tasks_by_id[tid]=task
        for row in rows:
            task=tasks_by_id.get(row.get('task_id'))
            item=describe(row,plans_by_id.get(row['plan_id'],{'actor_name':task['actor_name'] if task else None,'actions':[]}),task)
            if task:item['task']=chat_tasks.public(task)
            item['documents']=message_documents.listing(cur,row['id'])
            if row['event_type']=='model_request':
                payload=store.checked(row,'payload_json','payload_hash')
                item['interaction']=payload.get('interaction')
                field,value=('task_id',row['task_id']) if row.get('task_id') is not None else ('plan_id',row['plan_id'])
                cur.execute("SELECT id FROM ai_operation_messages WHERE "+field+"=%s AND round_no=%s AND event_type='model_response'",(value,row['round_no']))
                item['delivery_note']='已有对应调用结果事件（不代表AI读完每个文件）' if cur.fetchone() else '请求已登记；是否收到/计费请核对结果，勿重复运行'
            items.append(item)
        cur.execute("SELECT * FROM ai_operation_tasks WHERE managed_id=%s AND status IN ('queued','running') ORDER BY id LIMIT 20",(managed['id'],))
        pending=[chat_tasks.public(r) for r in cur.fetchall()]
    return {'session':public_session(session),'managed_id':str(managed['id']),'pending_tasks':pending,
            'list':items,'has_more':more,
            'next_before_id':str(rows[0]['id']) if rows else None,'last_id':str(rows[-1]['id']) if rows else None}


def original(session_id,message_id):
    """只读查看所属会话原文和资料，使用固定主键而非文件路径，校验原hash。"""
    session,managed=require_session(session_id)
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_messages WHERE id=%s AND conversation_id=%s',
                    (positive_id(message_id,'消息编号'),session['id']))
        row=cur.fetchone()
    if not row:raise DomainError('该会话中没有此消息',404)
    return {'id':str(row['id']),'hash':row['payload_hash'],'event_type':row['event_type'],
            'payload':store.checked(row,'payload_json','payload_hash')}
