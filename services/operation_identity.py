"""显式 AI 操作身份与人工网页身份的共用留痕上下文，不发起外部请求。

后台执行器用 ai_operation 包住单个行动，原 Listing/广告审计读取相同身份。
ContextVar 隔离线程/任务；退出必恢复，不能把 AI 身份泄漏到后续人工请求。
浏览器不得提交执行身份；本模块只由服务端已鉴权编排调用，不授予账号权限。
"""
from contextlib import contextmanager
from contextvars import ContextVar
import re
from flask import g, has_request_context, request
from services.mysql_service import get_db_connection
from services import operator_accounts as accounts
from services.listing_performance.common import DomainError, positive_id

_current = ContextVar('erp_operation_identity', default=None)


class AuditState(dict):
    """为后台广告审计提供与 Flask g 相同的属性和 get 访问，不伪造网页请求。"""
    def __getattr__(self, name):
        """读取本行动内的审计状态，缺少状态时明确报错。"""
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        """把审计状态限定在本行动的字典中，不影响其他线程或请求。"""
        self[name] = value


def explicit():
    """读取服务端显式 AI 上下文；未启用返回 None，不从请求参数推断身份。"""
    return _current.get()


def user():
    """读取当前实际执行人，优先显式 AI，人工请求沿用已认证的 current_user。"""
    context = explicit()
    if context:
        return context['user']
    if has_request_context() and getattr(request, 'current_user', None):
        return request.current_user
    raise DomainError('缺少可记录的操作人员，未执行修改', 403)


def metadata():
    """返回 AI 来源、人员快照与任务关联；人工操作返回 None，保持原日志语义。"""
    context = explicit()
    return context['metadata'] if context else None


def ads_state():
    """返回本次广告留痕状态，后台 AI 与人工 Flask g 不交叉使用。"""
    context = explicit()
    return context['ads_state'] if context else g


@contextmanager
def ai_operation(executor_id, initiated_by, run_id, action_id, operation_type='update',*,
                 task_id=None,plan_id=None,confirmation_id=None,conversation_id=None,proposed_by=None):
    """核对 AI 与人工发起人并建立单行动身份，最后清理；不授权或调用 Amazon。"""
    if not isinstance(action_id, str) or not re.fullmatch(r'a\d{1,2}', action_id):
        raise DomainError('AI行动编号无效')
    if operation_type not in ('text', 'price', 'images', 'update'):
        raise DomainError('AI操作类型无效')
    run_id = positive_id(run_id, '每日优化记录') if run_id is not None else None
    if run_id is None and not all((task_id,plan_id,confirmation_id,conversation_id)):
        raise DomainError('人工执行必须关联确认批次、任务、会话和原方案',409)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            actor = accounts.read_ai(cur, executor_id)
            initiator = accounts.get_user(cur, initiated_by)
            if initiator['account_type'] != 'human':
                raise DomainError('托管必须保留实际人工启用人或触发人', 403)
    finally:
        conn.close()
    actor_snapshot = accounts.snapshot(actor)
    proof = {'source': 'AI', 'executor': actor_snapshot, 'initiated_by': accounts.snapshot(initiator),
             'run_id': str(run_id), 'action_id': action_id}
    if run_id is None:
        proof.update(run_id=None,task_id=str(positive_id(task_id,'执行任务')),plan_id=str(positive_id(plan_id,'原方案')),
            confirmation_id=str(positive_id(confirmation_id,'确认批次')),conversation_id=str(positive_id(conversation_id,'共享会话')),
            confirmed_by=accounts.snapshot(initiator),proposed_by=proposed_by)
    request_key=f'ai:{run_id}:{action_id}' if run_id is not None else f'ai-confirm:{confirmation_id}:{action_id}'
    state = AuditState(ads_history_active=True, user_id=actor['id'],
                       ads_history_request_key=request_key, ads_history_calls=0,
                       ads_history_accepted=0, ads_history_new={})
    token = _current.set({'user': actor, 'metadata': proof, 'operation_type': operation_type, 'ads_state': state})
    try:
        yield proof
    finally:
        _current.reset(token)
