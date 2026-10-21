"""正式执行账本：短事务保存发送意图、原ERP审计关联、回执与真实回读。"""
import json
from . import plan_store
from .metrics import dumps
from services import operation_identity
from services.listing_performance.common import DomainError


def serialize(row):
    """解析正式执行记录，不生成或推进任何平台状态。"""
    result = {k: str(row[k]) if k in ('id', 'plan_id', 'shop_id', 'actor_id') else row[k]
              for k in ('id', 'plan_id', 'action_id', 'shop_id', 'marketplace_id', 'asin', 'runtime_mode',
                        'status', 'actor_id', 'created_at', 'verified_at', 'updated_at')}
    for name in ('action', 'baseline', 'receipt', 'verification'):
        result[name] = json.loads(row[name+'_json']) if row[name+'_json'] else None
    result['audit_context'] = json.loads(row['audit_context_json']) if row.get('audit_context_json') else None
    return result


def list_for_plan(plan_id):
    """按计划读取全部正式执行记录，不触发平台请求。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_executions WHERE plan_id=%s ORDER BY id', (plan_id,))
        return [serialize(row) for row in cur.fetchall()]


def existing(cur, plan_id, action_id):
    """同计划同编号只允许一个意图，恢复时禁止再次发送。"""
    cur.execute('SELECT * FROM ai_operation_executions WHERE plan_id=%s AND action_id=%s FOR UPDATE', (plan_id, action_id))
    return cur.fetchone()


def insert(cur, plan, action, key, actor, baseline):
    """验证显式AI身份与每日方案后，先登记preparing意图。"""
    proof = operation_identity.metadata()
    if not proof or proof['executor']['id'] != int(actor) or proof['action_id'] != action['action_id']:
        raise DomainError('AI执行缺少匹配的服务端操作身份，未提交', 403)
    cur.execute('SELECT plan_id,executor_user_id FROM ai_operation_daily_runs WHERE id=%s', (proof['run_id'],))
    run = cur.fetchone()
    if not run or run['plan_id'] != int(plan['id']) or run['executor_user_id'] != int(actor):
        raise DomainError('执行身份与每日优化方案不匹配，未提交', 409)
    cur.execute('''INSERT INTO ai_operation_executions
        (plan_id,action_id,shop_id,marketplace_id,asin,runtime_mode,resource_key,status,
         action_json,baseline_json,actor_id,audit_context_json,created_at,updated_at)
        VALUES (%s,%s,%s,%s,%s,'real',%s,'preparing',%s,%s,%s,%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
        (plan['id'], action['action_id'], plan['shop_id'], plan['marketplace_id'], plan['asin'],
         key, dumps(action), dumps(baseline), actor, dumps(proof)))
    return cur.lastrowid


def get(cur, execution_id, lock=False):
    """读取执行编号，短事务更新时可加行锁。"""
    cur.execute('SELECT * FROM ai_operation_executions WHERE id=%s'+(' FOR UPDATE' if lock else ''), (execution_id,))
    return cur.fetchone()


def submitted(cur, execution_id, status, receipt):
    """保存发送阶段，未知结果不会变回可重试的未执行状态。"""
    cur.execute('UPDATE ai_operation_executions SET status=%s,receipt_json=%s,updated_at=UTC_TIMESTAMP(3) WHERE id=%s',
                (status, dumps(receipt), execution_id))


def prepared(cur, execution_id, value):
    """持久保存真实前值与核对依据，随后才允许进入submitting阶段。"""
    cur.execute('UPDATE ai_operation_executions SET baseline_json=%s,updated_at=UTC_TIMESTAMP(3) WHERE id=%s',
                (dumps(value), execution_id))


def verified(cur, execution_id, status, value):
    """保存真实回读和首次确认时间，不因反复核对重置观察起点。"""
    cur.execute('''UPDATE ai_operation_executions SET status=%s,verification_json=%s,
        verified_at=IF(%s='confirmed',COALESCE(verified_at,UTC_TIMESTAMP(3)),verified_at),
        updated_at=UTC_TIMESTAMP(3) WHERE id=%s''', (status, dumps(value), status, execution_id))
