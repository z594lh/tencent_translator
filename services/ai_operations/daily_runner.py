"""每日ASIN托管：真实DeepSeek分析→正式执行/回读→优化账本与次日记忆。"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from services.listing_performance.common import DomainError, positive_id
from services.operation_identity import ai_operation
from . import managed_store as store, plan_store, plans, executions, conversation_store, platform_operations as interfaces
from .metrics import digest

BJ=timezone(timedelta(hours=8))
SOURCE_ZONE=timezone(timedelta(hours=-7))
TERMINAL={'completed','partial','failed','paused'}
# 每日账本使用正式会话模式；Amazon执行必须经过意图登记和实时核对。
RUN_MODE='conv_real'


def clock():
    """返回带UTC时区的任务时钟，便于计划任务与隔离验证共用。"""
    return datetime.now(timezone.utc)


@contextmanager
def lock(managed):
    """获取同ASIN互斥锁并在退出时释放，避免并发重复运行。"""
    conn=plan_store.get_db_connection();name='ai_managed:'+digest([managed['shop_id'],managed['asin']])[:40];got=False
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s,0) acquired',(name,));got=cur.fetchone()['acquired']==1
        conn.rollback()
        if not got:raise DomainError('该ASIN每日流程正在运行，请刷新优化历史，不重复启动',409)
        yield
    finally:
        try:
            if got:
                with conn.cursor() as cur:cur.execute('SELECT RELEASE_LOCK(%s)',(name,))
        finally:conn.close()


def authorize_actor(actor):
    """复核人工授权人状态和独立AI托管权限，不依赖绩效或人工分配权限。"""
    # 人工发起人必须显式获授AI托管权限，AI执行账号不需要登录或角色。
    from services.permissions_service import get_user_permission_codes
    with plan_store.transaction() as cur:
        cur.execute('SELECT status FROM users WHERE id=%s',(actor,));user=cur.fetchone()
    required={'ai_operations:manage'}
    if not user or user['status']!=1 or not required.issubset(get_user_permission_codes(actor)):
        raise DomainError('开启托管的用户已停用或AI托管权限变化，本次不执行',403)


def summarize(plan,records):
    """根据已保存方案和执行事实统计本次结果，不虚构收益。"""
    actions=(plan.get('result') or {}).get('actions',[])
    context=plan['dataset']['content'].get('management_context',{})
    watching={r['ref'] for r in context.get('observations',[]) if r['phase'] in ('observing','waiting_data')}
    watching.update(r['action']['ref'] for r in records if r['status']=='confirmed')
    return {'conclusion':(plan.get('result') or {}).get('summary',''),'action_count':len(actions),
            'analysis_provider':(plan.get('usage') or {}).get('provider','deepseek'),'execution_mode':'real',
            'conversation_id':plan.get('ai_call',{}).get('prompt',{}).get('conversation',{}).get('id'),
            'analysis_rounds':(plan.get('usage') or {}).get('round_count',1),
            'change_count':sum(a['mode']=='change' for a in actions),'observe_count':sum(a['mode']=='observe' for a in actions),
            'executed_count':len(records),'confirmed_count':sum(r['status']=='confirmed' for r in records),
            'observing_count':len(watching),
            'unsupported':[a['action_id'] for a in actions if a['mode']=='change' and not interfaces.supports(a)],
            'context_history_count':len(context.get('recent_runs',[])),
            'context_observation_count':len(context.get('observations',[]))}


def run_one(managed_id,actor=None,source='manual',now=None,authorize=None):
    """运行单个ASIN的每日会话、正式提交、回读及持久反馈。"""
    if interfaces.MODE!='real':raise DomainError('本轮Amazon修改必须使用正式接口',503)
    if source not in ('manual','scheduled'):raise DomainError('触发来源无效')
    now=now or clock()
    if now.tzinfo is None:raise DomainError('任务时钟必须包含时区')
    managed=store.get_target(managed_id)
    if not managed['enabled']:raise DomainError('该ASIN已暂停托管，历史仍可查看',409)
    actor=positive_id(actor if actor is not None else managed['actor_id'],'用户')
    check=authorize or (lambda:authorize_actor(actor))
    def active():
        """每个付费或执行阶段前重新检查权限与托管启停状态。"""
        check()
        latest=store.get_target(managed['id'])
        if not latest['enabled']:raise DomainError('托管已暂停，剩余行动未执行',409)
        if latest.get('executor_user_id')!=managed.get('executor_user_id'):
            raise DomainError('AI执行人配置已变化，本次剩余行动未执行',409)
        store.execution_identity(latest)
    with lock(managed):
        # 未授权的旧配置不登记当天失败账本，管理员授予权限后仍能正常启动。
        active()
        today=now.astimezone(BJ).date()
        end=min(today-timedelta(days=1),now.astimezone(SOURCE_ZONE).date()-timedelta(days=1))
        run=store.begin(managed,today,end,source,actor,run_mode=RUN_MODE)
        if run['status'] in TERMINAL:return {**run,'reused':True}
        try:
            if not run.get('executor_user_id') or str(run['executor_user_id'])!=str(managed.get('executor_user_id')):
                raise DomainError('本次未完成任务的AI身份不符，请核对原执行记录，不能替换操作人继续执行',409)
            active()
            # 先核实上次未知/待生效回执，绝不重新提交昨日修改。
            with plan_store.transaction() as cur:
                cur.execute('''SELECT id FROM ai_operation_executions WHERE shop_id=%s AND marketplace_id=%s
                    AND BINARY asin=BINARY %s AND runtime_mode='real' AND status IN ('preparing','submitting','submitted','pending','unknown','issues')
                    ORDER BY id LIMIT 101''',(managed['shop_id'],managed['marketplace_id'],managed['asin']))
                pending=cur.fetchall()
            if len(pending)>100:raise DomainError('待核实执行超过100条，请先核对历史',422)
            for record in pending:active();executions.verify(record['id'])
            store.update(run['id'],'analyzing','正在分析当前资料、历史调整快照与观察周期')
            plan=plans.generate({'shop_id':managed['shop_id'],'asin':managed['asin'],'report_date':str(run['report_date']),
                                 'request_token':digest(['managed-day',run['id'],interfaces.MODE])[:32],'confirm_ai':True},
                actor,active,management={'managed_id':managed['id'],'run_id':run['id'],'run_date':today,'now':now.astimezone(timezone.utc)},
                on_registered=lambda pid:store.update(run['id'],'analyzing','DeepSeek正在分析；输入已保存，可能产生费用，请勿重复发起',plan_id=pid))
            store.update(run['id'],'executing','已保存AI结论，正在正式执行并独立回读',plan_id=plan['id'])
            if plan['status']!='ready':raise DomainError(plan.get('message') or '未形成有效方案',409)
            actions=plan['result']['actions'];unsupported=[]
            for action in actions:
                if action['mode']!='change':continue
                if not interfaces.supports(action):unsupported.append(action['action_id']);continue
                active()
                operation={'listing_text_update':'text','listing_price_update':'price'}.get(action['kind'],'update')
                with ai_operation(managed['executor_user_id'],actor,run['id'],action['action_id'],operation):
                    records=executions.execute(plan['id'],{'action_ids':[action['action_id']]},
                                               managed['executor_user_id'],active)['list']
                if records[0]['status'] in ('submitting','submitted','pending','unknown'):
                    active();executions.verify(records[0]['id'])
            records=executions.history(plan['id'])['list']
            summary=summarize(plan,records)
            partial=bool(unsupported or any(r['status']!='confirmed' for r in records))
            conversation_store.execution_feedback(plan,records,'partial' if partial else 'completed',
                                                   '本次正式执行及独立回读已结束')
            store.update(run['id'],'partial' if partial else 'completed',
                '今日分析执行已结束，部分行动待核对' if partial else '今日优化已完成；观察期内的原调整未重复提交',
                summary=summary,done=True)
        except Exception as error:
            message=str(error) if isinstance(error,DomainError) else '每日流程未完成；已保存的方案/执行保留，请查看详情，不自动重复提交'
            paused=not store.get_target(managed['id'])['enabled']
            current=store.get_run(run['id']);summary=None
            if current['plan_id']:
                saved=plan_store.get(current['plan_id'])
                records=executions.history(saved['id'])['list']
                summary=summarize(saved,records)
                conversation_store.execution_feedback(saved,records,'paused' if paused else 'failed',message)
            store.update(run['id'],'paused' if paused else 'failed',message,summary=summary,done=True)
        return {**store.get_run(run['id']),'reused':False}


def run_due(now=None):
    """北京时间18点30分后遍历启用ASIN，同日已完成的任务直接复用。"""
    now=now or clock();local=now.astimezone(BJ)
    if (local.hour,local.minute)<(18,30):
        return {'status':'not_due','message':'尚未到北京时间18:30，未运行','runtime_mode':'real','processed':0,'failed':0}
    results=[]
    for managed_id in store.enabled_targets():
        try:
            row=run_one(managed_id,source='scheduled',now=now)
            results.append({'id':row['id'],'status':row['status'],'reused':row['reused']})
        except DomainError:
            results.append({'status':'skipped','reused':False})
    return {'status':'finished','runtime_mode':'real','processed':len(results),
            'failed':sum(r['status'] in ('failed','partial') for r in results),'runs':results}


def retained_context(plan):
    """仅展示仍保留的会话执行与运行；原始附件不篡改，已清理旧记录不再列为待观察。"""
    context=plan['dataset']['content'].get('management_context',{})
    rows=context.get('observations',[])+context.get('pending_executions',[])
    ids=[row['execution_id'] for row in rows]
    previous=context.get('recent_runs',[])
    with plan_store.transaction() as cur:
        if ids:
            cur.execute('SELECT id FROM ai_operation_executions WHERE id IN ('+','.join(['%s']*len(ids))+')',ids)
            kept={str(row['id']) for row in cur.fetchall()}
        else:kept=set()
        if previous:
            cur.execute("SELECT id FROM ai_operation_daily_runs WHERE runtime_mode='conv_real' AND id IN ("+','.join(['%s']*len(previous))+')',[row['run_id'] for row in previous])
            runs={str(row['id']) for row in cur.fetchall()}
        else:runs=set()
    return {**context,'observations':[row for row in context.get('observations',[]) if str(row['execution_id']) in kept],
            'pending_executions':[row for row in context.get('pending_executions',[]) if str(row['execution_id']) in kept],
            'recent_runs':[row for row in previous if str(row['run_id']) in runs]}


def detail(run_id):
    """读取弹框的当前会话详情，不重新生成AI方案或执行动作。"""
    run=store.get_run(run_id);managed=store.get_target(run['managed_id'])
    plan=plan_store.get(run['plan_id']) if run['plan_id'] else None
    return {'run':run,'managed':managed,'plan':plan,
            'executions':executions.history(plan['id'])['list'] if plan else [],
            'context':retained_context(plan) if plan else {}}


def check_ready():
    """仅验证部署结构；不代表已安装cron、数据已齐或真实API可用。"""
    with plan_store.transaction() as cur:
        for table,fields in (('ai_operation_managed_asins','id,enabled,actor_id,executor_user_id'),
                             ('ai_operation_daily_runs','id,run_date,report_date,plan_id,summary_json,executor_user_id,audit_context_json'),
                             ('ai_operation_plans','id,runtime_mode,input_hash'),
                             ('ai_operation_executions','id,baseline_json,verified_at,audit_context_json')):
            cur.execute('SELECT '+fields+' FROM '+table+' WHERE 1=0')
        for table in conversation_store.TABLES:cur.execute('SELECT id FROM '+table+' WHERE 1=0')
        cur.execute("SELECT group_key FROM cron_task_slots WHERE group_key='task_ai_operations_daily'")
        if not cur.fetchone():raise DomainError('统一任务锁槽未初始化，请执行最新create_tables.sql',503)
        from services import operator_accounts
        operator_accounts.read_ai(cur)
        cur.execute("SELECT id FROM permissions WHERE code='ai_operations:manage'")
        if not cur.fetchone(): raise DomainError('AI托管权限尚未初始化，请执行最新迁移',503)
        for table in ('amazon_listing_change_history','amazon_ads_history'):
            cur.execute('SELECT audit_context_json FROM '+table+' LIMIT 0')
    return {'status':'ready','runtime_mode':'real','analysis_provider':'deepseek','execution_mode':'real','enabled_asins':len(store.enabled_targets()),
            'schedule':'北京时间18:30；只读结构预检通过，不代表服务器cron已安装或报告已齐'}
