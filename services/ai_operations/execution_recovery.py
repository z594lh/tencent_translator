"""原方案广告补执行：只接受明确未发送项，不调用AI，不重放Listing或未知请求。"""
from datetime import datetime, timedelta, timezone
from services import operation_identity
from services.listing_performance.common import DomainError, positive_id
from . import daily_runner, managed_store, plan_store, execution_store, executions, conversation_store
from . import platform_operations as platform

MAX_AGE = timedelta(hours=24)
MAX_ATTEMPTS = 3


def parse(body):
    """仅解析记录编号与失败版本；禁止浏览器替换行动内容、出价或执行身份。"""
    if not isinstance(body, dict) or set(body) != {'executions', 'confirm_execute'} or body['confirm_execute'] is not True:
        raise DomainError('请明确确认补执行未发送广告')
    rows = body['executions']
    if not isinstance(rows, list) or not 1 <= len(rows) <= 12: raise DomainError('请选择1到12条未发送广告')
    chosen = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {'id', 'version'} or type(row['version']) is not int or not 0 <= row['version'] < MAX_ATTEMPTS:
            raise DomainError('执行编号或失败版本无效，请刷新详情')
        eid = str(positive_id(row['id'], '执行记录'))
        if eid in chosen: raise DomainError('执行编号重复')
        chosen[eid] = row['version']
    return chosen


def validate_record(cur, record, plan, run, version):
    """锁内核对原方案、确定未发送、失败版本及原广告日志不存在，不能重置请求状态。"""
    receipt = record['receipt'] or {}
    action = next((a for a in plan['result']['actions'] if a['action_id'] == record['action_id']), None)
    if (record['runtime_mode'] != 'real' or record['status'] != 'not_sent' or receipt.get('accepted') is not False
            or record['plan_id'] != plan['id'] or not action or action != record['action']
            or not action['kind'].startswith('ads_') or not platform.supports(action)):
        raise DomainError('仅能补执行原方案中确定未发送的广告，成功或未知项不能重发', 409)
    attempts = receipt.get('recovery_attempts') or []
    if len(attempts) != version or len(attempts) >= MAX_ATTEMPTS:
        raise DomainError('失败记录已经变化或补执行达到3次，请刷新详情，不重复点击', 409)
    proof = record['audit_context'] or {}
    identity = plan['dataset']['content']['identity']
    if (str(proof.get('run_id')) != run['id'] or proof.get('action_id') != record['action_id']
            or str(record['actor_id']) != str(run['executor_user_id'])
            or record['baseline'].get('identity') != identity):
        raise DomainError('原执行身份或账户与方案不一致，不能补执行', 409)
    cur.execute('''SELECT id FROM amazon_ads_history WHERE shop_id=%s
        AND JSON_UNQUOTE(JSON_EXTRACT(audit_context_json,'$.run_id'))=%s
        AND JSON_UNQUOTE(JSON_EXTRACT(audit_context_json,'$.action_id'))=%s LIMIT 1''',
        (plan['shop_id'], run['id'], record['action_id']))
    if cur.fetchone() or receipt.get('history_ids'):
        raise DomainError('已有原广告请求历史，不能认定从未发送；请先同步核对', 409)


def validate_run(run, plan, now):
    """只允许24小时内且尚未被后续每日方案替代的有效正式方案。"""
    executions.require_real(plan)
    created = plan['created_at'].replace(tzinfo=timezone.utc)
    if now < created or now-created > MAX_AGE:
        raise DomainError('原方案超过24小时或时间异常，请等待下一次分析，不补执行过期决策', 409)
    if run['runtime_mode'] != 'conv_real' or run['status'] not in daily_runner.TERMINAL:
        raise DomainError('每日任务仍在执行或模式不符，不能并行补执行', 409)
    with plan_store.transaction() as cur:
        cur.execute('SELECT id FROM ai_operation_daily_runs WHERE managed_id=%s AND id>%s LIMIT 1', (run['managed_id'], run['id']))
        if cur.fetchone(): raise DomainError('已有更新的每日任务，请查看最新方案，不补执行旧决策', 409)


def retry_one(run, plan, selected_id, version, actor, authorize):
    """持有资源锁，保留旧失败并登记新的发送前意图，再进入同一正式执行链。"""
    record = executions.read_record(selected_id)
    identity, action = plan['dataset']['content']['identity'], record['action']
    with executions.resource_lock(identity, action):
        authorize()
        with plan_store.transaction(True) as cur:
            record = execution_store.serialize(execution_store.get(cur, selected_id, True))
            validate_record(cur, record, plan, run, version)
            executions.check_pending(cur, identity, action)
            old = {k: v for k, v in (record['receipt'] or {}).items() if k not in ('recovery_attempts', 'recovery_context')}
            attempts = list((record['receipt'] or {}).get('recovery_attempts') or [])
            attempts.append({'number': version+1, 'previous_status': record['status'], 'previous_receipt': old,
                'previous_settings': record['baseline']['settings'], 'previous_updated_at': record['updated_at'],
                'requested_at': datetime.utcnow().isoformat(), 'requested_by': actor})
            recovery = {'attempts': attempts, 'context': operation_identity.metadata()}
            execution_store.submitted(cur, selected_id, 'preparing', {'accepted': False, 'runtime_mode': 'real',
                'message': '补执行已登记，尚未发送；正在重新核对平台前值',
                'recovery_attempts': attempts, 'recovery_context': recovery['context']})
        return executions.attempt(plan, action, selected_id, authorize, recovery)


def retry_ads(run_id, body, actor, authorize):
    """显式补指定未发送广告；不重开AI会话、不改行动值、不重复已成功操作。"""
    selected = parse(body)
    run = managed_store.get_run(run_id)
    managed = managed_store.get_target(run['managed_id'])
    plan = plan_store.get(run['plan_id']) if run['plan_id'] else None
    if not plan: raise DomainError('每日记录尚无有效方案', 409)
    actor = positive_id(actor, '人工操作人')
    def active():
        """每一条补执行前复核当前人员权限、AI绑定和托管负责归属。"""
        authorize()
        daily_runner.authorize_actor(actor)
        latest = managed_store.get_target(managed['id'])
        if not latest['enabled'] or latest.get('executor_user_id') != run.get('executor_user_id'):
            raise DomainError('托管已暂停或AI身份变化，不能补执行', 409)
        managed_store.execution_identity(latest)
    with daily_runner.lock(managed):
        active()
        run = managed_store.get_run(run['id'])
        validate_run(run, plan, daily_runner.clock())
        # 整批先校验，不接受夹带其他方案或已经成功的行动。
        with plan_store.transaction() as cur:
            for eid, version in selected.items():
                raw = execution_store.get(cur, eid)
                if not raw: raise DomainError('执行记录不存在', 404)
                validate_record(cur, execution_store.serialize(raw), plan, run, version)
        try:
            for eid, version in selected.items():
                action = executions.read_record(eid)['action']
                with operation_identity.ai_operation(run['executor_user_id'], actor, run['id'], action['action_id']):
                    record = retry_one(run, plan, eid, version, actor, active)
                if record['status'] in ('submitting', 'submitted', 'pending', 'unknown'):
                    active(); executions.verify(eid)
        finally:
            # 即使中途权限变化或进程内异常，已提交的事实也必须进入每日摘要及后续记忆。
            refresh_feedback(run, plan)
    return daily_runner.detail(run['id'])


def refresh_feedback(run, plan):
    """汇总最新执行，追加新反馈事件而非覆盖原AI问答与最初执行反馈。"""
    records = executions.history(plan['id'])['list']
    summary = daily_runner.summarize(plan, records)
    partial = bool(summary['unsupported'] or any(r['status'] != 'confirmed' for r in records))
    status = 'partial' if partial else 'completed'
    message = '广告补执行已结束，部分行动待核对' if partial else '原方案执行已完成；广告补执行已记录'
    managed_store.update(run['id'], status, message, summary=summary, done=True)
    revision = sum(len((r['receipt'] or {}).get('recovery_attempts') or []) for r in records)
    info = plan.get('ai_call', {}).get('prompt', {}).get('conversation')
    if info and revision:
        with plan_store.transaction() as cur:
            cur.execute("SELECT id FROM ai_operation_messages WHERE plan_id=%s AND round_no=%s AND event_type='execution_recovery' LIMIT 1", (plan['id'], revision))
            if cur.fetchone(): return
        conversation_store.append(info['id'], plan['id'], revision, 'execution_recovery',
            {'plan_id': plan['id'], 'status': status, 'message': message, 'execution_mode': 'real',
             'note': '补执行记录，仅对确定未发送广告重新实时预检；未重新调用AI，原失败和已成功Listing保留。',
             'executions': [{'execution_id': r['id'], 'action_id': r['action_id'], 'ref': r['action']['ref'],
                 'label': r['action']['label'], 'status': r['status'], 'verified_at': r['verified_at']} for r in records]})
