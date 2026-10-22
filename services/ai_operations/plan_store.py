"""运营方案最小存储：短事务、生成记录、完整依据、输出与历史。"""
from contextlib import contextmanager
from datetime import datetime
import json
from services.mysql_service import get_db_connection
from services.listing_performance.common import DomainError, positive_id
from .metrics import digest, dumps


@contextmanager
def transaction(write=False):
    """统一短事务、失败回滚和连接释放，不把HTTP等待放入数据库事务。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED')
            if not write:cur.execute('SET TRANSACTION READ ONLY')
            conn.begin()
            yield cur
        conn.commit() if write else conn.rollback()
    except Exception:
        conn.rollback();raise
    finally:
        conn.close()


def serialize(row, detail=False, include_events=True):
    """校验并读取会话计划原始输入、结果及消息，不兼容旧单次计划。"""
    keys = ('id', 'shop_id', 'marketplace_id', 'asin', 'report_date', 'status', 'model', 'contract_version', 'runtime_mode',
            'message', 'error_code', 'actor_id', 'created_at', 'completed_at')
    data = {k: str(row[k]) if k in ('id', 'shop_id') else row[k] for k in keys}
    data['task_id']=str(row['task_id']) if row.get('task_id') is not None else None
    data['execution_available'] = False
    if detail:
        try:
            dataset = json.loads(row['dataset_json'])
            prompt = json.loads(row['prompt_json'])
            if not isinstance(prompt,dict) or not isinstance(prompt.get('conversation'),dict):raise ValueError()
            facts = json.loads(row['facts_json'])
            if digest([facts, dataset, prompt]) != row['input_hash']:raise ValueError()
            result = json.loads(row['result_json']) if row['result_json'] else None
            if result is not None and digest(result) != row['result_hash']:raise ValueError()
            data.update(result=result, dataset=dataset, facts=facts,
                        usage=json.loads(row['usage_json']) if row['usage_json'] else None,
                        ai_call={'input_hash':row['input_hash'],'prompt_hash':digest(prompt),
                                 'data_hash':dataset['content_hash'],'prompt':prompt,
                                 'execution_mode':row['runtime_mode']})
            data['execution_available'] = row['runtime_mode'] == 'real' and row['status'] == 'ready' and bool(result and result.get('execution_available'))
            if include_events:
                from . import conversation_store
                data['conversation_events'] = conversation_store.trace(row['id'],row.get('task_id'))
        except (ValueError, TypeError):
            raise DomainError('方案输入或结果摘要不符，请核对存储；未重新调用AI', 503) from None
    return data


def find(token=None, key=None):
    """按持久幂等标识查询计划，避免重发已登记的模型请求。"""
    field, value = ('request_token', token) if token is not None else ('request_key', key)
    with transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_plans WHERE '+field+'=%s', (value,))
        return cur.fetchone()


def get(plan_id):
    """只读取指定会话计划；历史分页统一由每日托管账本提供。"""
    with transaction() as cur:
        chosen = positive_id(plan_id, '方案编号')
        cur.execute('SELECT * FROM ai_operation_plans WHERE id=%s', (chosen,))
        row = cur.fetchone()
        if not row:raise DomainError('方案不存在', 404)
        return serialize(row, True)


def insert(pack, dataset, prompt, key, token, actor, model, version, task_id=None):
    """调用AI前保存同次真实资料、问题和提示参数，异常时保留意图记录。"""
    with transaction(True) as cur:
        cur.execute('''INSERT INTO ai_operation_plans
            (shop_id,marketplace_id,asin,report_date,request_token,request_key,status,model,contract_version,runtime_mode,
             input_hash,facts_json,dataset_json,prompt_json,actor_id,created_at,message,task_id)
            VALUES (%s,%s,%s,%s,%s,%s,'generating',%s,%s,'real',%s,%s,%s,%s,%s,%s,%s,%s)''',
            (pack['identity']['id'], pack['identity']['marketplace_id'], pack['identity']['asin'], pack['time']['report_end'],
             token, key, model, version, digest([pack, dataset, prompt]), dumps(pack), dumps(dataset), dumps(prompt),
             positive_id(actor, '用户'), datetime.utcnow(), 'AI分析请求已登记，超时可能已计费；只刷新历史核对，不重复发起',task_id))
        return cur.lastrowid


def finish(plan_id, status, result=None, usage=None, code=None, message=None, on_ready=None):
    """原子结束生成，成功时与下一版会话记忆一起提交。"""
    with transaction(True) as cur:
        cur.execute('''UPDATE ai_operation_plans SET status=%s,result_json=%s,result_hash=%s,usage_json=%s,
            error_code=%s,message=%s,completed_at=%s WHERE id=%s AND status='generating' ''',
            (status, dumps(result) if result else None, digest(result) if result else None,
             dumps(usage) if usage else None, code, message, datetime.utcnow(), plan_id))
        if cur.rowcount and status=='ready' and on_ready is not None:on_ready(cur)
    return get(plan_id)
