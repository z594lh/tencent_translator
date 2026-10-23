"""每日/人工方案统一查询视图；不复制历史表，不把聊天建议冒充平台已执行。"""
from datetime import timezone,timedelta
from services.listing_performance.common import DomainError,positive_id
from . import plan_store,managed_store,executions,daily_runner
from .protocol import native_prompt,native_task,object_value,LEGACY_MESSAGE

BJ=timezone(timedelta(hours=8))
INDEX_QUERY="""SELECT 'daily' source,id,started_at occurred_at FROM ai_operation_daily_runs WHERE managed_id=%s
    UNION ALL SELECT 'human' source,id,created_at occurred_at FROM ai_operation_tasks
    WHERE managed_id=%s AND task_type='chat' AND (chat_mode='proposal' OR plan_id IS NOT NULL)"""


def plan_header(cur,pid):
    """只取版本和方案摘要，不为列表加载本地完整报表快照。"""
    if not pid:return None
    cur.execute('SELECT id,status,prompt_json,result_json,report_date,completed_at FROM ai_operation_plans WHERE id=%s',(pid,))
    return cur.fetchone()


def counts(cur,pid):
    """从唯一执行意图累计状态，人工多批确认不会重复计算方案或执行。"""
    if not pid:return 0,0
    cur.execute("SELECT COUNT(*) total,COALESCE(SUM(status='confirmed'),0) confirmed FROM ai_operation_executions WHERE plan_id=%s",(pid,))
    row=cur.fetchone();return row['total'],int(row['confirmed'])


def record(cur,source,row):
    """将两种来源映射为同一列表结构；旧版仅元信息，不解释旧JSON方案。"""
    plan=plan_header(cur,row.get('plan_id'))
    current=native_prompt(plan['prompt_json']) if plan else native_task(cur,row) if source=='human' else False
    if source=='daily':
        item=managed_store.serialize_run(row)
        if not plan:
            cur.execute("SELECT * FROM ai_operation_tasks WHERE run_id=%s AND task_type='daily'",(row['id'],));task=cur.fetchone()
            current=bool(task and native_task(cur,task))
    else:
        created=row['created_at'];local=created.replace(tzinfo=timezone.utc).astimezone(BJ)
        item={'id':str(row['id']),'managed_id':str(row['managed_id']),'plan_id':str(row['plan_id']) if row['plan_id'] else None,
              'run_date':local.date(),'report_date':row['report_date'],'started_at':created,'completed_at':row['completed_at'],
              'status':row['status'],'message':row['message'],'trigger_source':'human','actor_name':row['actor_name'],
              'actor_id':str(row['actor_id']),'summary':{},'audit_context':None}
    summary=object_value(plan.get('result_json')) if current and plan else {}
    summary=summary if isinstance(summary,dict) else {}
    actions=summary.get('actions',[]);actions=actions if isinstance(actions,list) else []
    changes=sum(isinstance(a,dict) and a.get('mode')=='change' for a in actions)
    executed,confirmed=counts(cur,row.get('plan_id')) if current else (0,0)
    item['summary']={**item['summary'],'conclusion':summary.get('summary','') if current else LEGACY_MESSAGE,
                     'change_count':changes,'executed_count':executed,'confirmed_count':confirmed}
    if source=='human' and row['status']=='completed':
        item['status']='completed' if changes==0 or confirmed==changes else 'partial' if executed else 'awaiting_confirmation'
    return {**item,'source':source,'history_key':source+':'+str(row['id']),'legacy':not current,'detail_available':current,
            'message':item['message'] if current else LEGACY_MESSAGE}


def listing(managed_id,params):
    """跨来源稳定分页；只包含每日优化和人工方案，不把普通讨论/确认批次重复列入。"""
    managed=managed_store.get_target(managed_id);page=positive_id(params.get('page',1),'页码')
    if page>100000:raise DomainError('页码过大')
    union=INDEX_QUERY
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ('+union+') h',(managed['id'],managed['id']));total=cur.fetchone()['n']
        cur.execute('SELECT * FROM ('+union+') h ORDER BY occurred_at DESC,source DESC,id DESC LIMIT 20 OFFSET %s',
                    (managed['id'],managed['id'],(page-1)*20));refs=list(cur.fetchall());items=[]
        for ref in refs:
            table='ai_operation_daily_runs' if ref['source']=='daily' else 'ai_operation_tasks'
            cur.execute('SELECT * FROM '+table+' WHERE id=%s AND managed_id=%s',(ref['id'],managed['id']))
            row=cur.fetchone()
            if row:items.append(record(cur,ref['source'],row))
    return {'list':items,'total':total,'page':page}


def latest(cur,managed_id):
    """托管列表最近优化也使用统一来源，人工方案不再被最近每日记录覆盖。"""
    cur.execute('SELECT * FROM ('+INDEX_QUERY+') h ORDER BY occurred_at DESC,source DESC,id DESC LIMIT 1',(managed_id,managed_id));ref=cur.fetchone()
    if not ref:return None
    table='ai_operation_daily_runs' if ref['source']=='daily' else 'ai_operation_tasks'
    cur.execute('SELECT * FROM '+table+' WHERE id=%s AND managed_id=%s',(ref['id'],managed_id));row=cur.fetchone()
    return record(cur,ref['source'],row) if row else None


def detail(source,record_id):
    """共用详情结构；先判版本再读取快照/执行，旧版直接返回占位且无写入入口。"""
    if source not in ('daily','human'):raise DomainError('优化记录来源无效',422)
    rid=positive_id(record_id,'优化记录')
    table='ai_operation_daily_runs' if source=='daily' else 'ai_operation_tasks'
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM '+table+' WHERE id=%s',(rid,));raw=cur.fetchone()
        if not raw or source=='human' and (raw['task_type']!='chat' or raw['chat_mode']!='proposal' and not raw['plan_id']):raise DomainError('优化记录不存在',404)
        item=record(cur,source,raw)
    managed=managed_store.get_target(raw['managed_id'])
    base={'source':source,'run':item,'managed':managed,'legacy':item['legacy'],'message':item['message'],'plan':None,'executions':[],'context':{}}
    if item['legacy'] or not item.get('plan_id'):return base
    plan=plan_store.get(item['plan_id'])
    if plan.get('legacy'):return {**base,'legacy':True,'message':LEGACY_MESSAGE}
    from .conversation_queries import managed_identity
    from .conversation_store import scope
    _,identity=managed_identity(raw['managed_id'])
    if scope(plan['dataset']['content']['identity'])[1]!=scope(identity)[1]:
        raise DomainError('历史账户范围已变化，不能用当前店铺配置查看或执行原建议',409)
    return {**base,'plan':plan,'executions':executions.history(plan['id'])['list'],'context':daily_runner.retained_context(plan)}
