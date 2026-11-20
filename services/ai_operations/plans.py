"""真实DeepSeek分析→契约校验；已支持行动由正式接口执行。"""
from .language import text as model_text
from .language import freeze_build

from contextlib import contextmanager
import re
from services.listing_performance.common import DomainError, positive_id
from . import facts, dataset, plan_contract, plan_store, plan_provider, conversation, conversation_store, platform_operations as interfaces
from .metrics import digest, dumps


def analyze(data,prepared):
    """分析和摘要共用供应商入口；工具由ERP白名单分发，模型不能直连Amazon。"""
    client=plan_provider.create_client()
    try:return plan_provider.generate(client,prepared)
    finally:
        try:client.close()
        except Exception:pass


@contextmanager
def lock(shop, asin):
    """同店ASIN生成互斥，防止并发请求重复收费。"""
    conn = plan_store.get_db_connection()
    name = 'ai_plan:' + digest([shop, asin])[:40]
    got = False
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s,0) acquired', (name,));got = cur.fetchone()['acquired'] == 1
        conn.rollback()
        if not got:raise DomainError(model_text('plans.8360b3fc31a3'), 409)
        yield
    finally:
        if got:
            try:
                with conn.cursor() as cur:cur.execute('SELECT RELEASE_LOCK(%s)', (name,))
            except Exception:pass
        conn.close()


@freeze_build
def build_data(shop, asin, report_date, management=None):
    """预览和正式任务共用真实资料组包，只读本地数据，不创建会话或调用AI。"""
    pack = facts.preview({'shop_id': shop, 'asin': asin, 'report_date': report_date})
    data = dataset.build(pack)
    from .tools import competitors, storage
    competitors.attach(data,management.get('now') if management else None)
    storage.attach(data,management.get('now') if management else None)
    if management is not None:
        from . import management_context
        management_context.attach(data,management_context.build(pack,data,management))
    data['content']['analysis']={'provider':plan_provider.PROVIDER,'model':plan_contract.MODEL,
        'execution_mode':interfaces.MODE,'history_is_explicit':True,
        'output_metrics':sorted(plan_contract.METRICS),
        'note':model_text('plans.4ab6c1643ba8')}
    data['content']['reading_guide']={'windows':model_text('plans.4d512a3e1d90'),
        'matrices':model_text('plans.d453db158eae'),
        'ads':model_text('plans.55bc1260f946'),
        'keywords':model_text('plans.05b2c73b7fb0'),
        'history':model_text('plans.3d20f8addbb9')}
    data['content_hash']=digest(data['content']);data['size_bytes']=len(dumps(data['content']).encode('utf8'))
    if data['size_bytes']>dataset.MAX_INPUT_BYTES:raise DomainError(model_text('plans.3b488bbc0013'),422)
    return pack, data


def generate(body, actor, check_permissions, management=None,on_registered=None,queue_task=None):
    """整合每日资料并调用持久会话，登记精确输入、结果与记忆检查点。"""
    if not isinstance(body, dict) or set(body) != {'shop_id', 'asin', 'report_date', 'request_token', 'confirm_ai'} or body['confirm_ai'] is not True:
        raise DomainError(model_text('plans.e6fddb8ca45e'))
    shop = positive_id(body['shop_id'], '店铺')
    if not isinstance(body['report_date'], str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', body['report_date']):
        raise DomainError('必须明确选择报表截止日')
    asin = str(body['asin']).strip().upper();token = body['request_token']
    if not re.fullmatch('[A-Z0-9]{10}', asin) or not isinstance(token, str) or not re.fullmatch('[a-f0-9]{32}', token):
        raise DomainError(model_text('plans.49026f859dd9'))
    def reused(row):
        """同范围同请求直接返回已有会话计划，不再次调用模型。"""
        if str(row['shop_id']) != str(shop) or row['asin'] != asin or str(row['report_date']) != str(body['report_date']):
            raise DomainError(model_text('plans.79f92cd7c4ec'), 409)
        if row.get('runtime_mode') != interfaces.MODE:
            raise DomainError(model_text('plans.f39d8837297c'), 409)
        check_permissions()
        return {**plan_store.serialize(row, True), 'reused': True}
    check_permissions()
    previous = plan_store.find(token=token)
    if previous:return reused(previous)
    with lock(shop, asin):
        previous = plan_store.find(token=token)
        if previous:return reused(previous)
        # 同次本地读取直接生成模型资料，自动落库，不要求手工保存快照。
        pack, data = build_data(shop, asin, body['report_date'], management)
        prepared, session = conversation.prepare(data,queue_task)
        key = digest([data['content_hash'], plan_contract.VERSION, plan_contract.MODEL, plan_provider.VERSION, interfaces.VERSION, digest(prepared)])
        previous = plan_store.find(key=key)
        if previous:return reused(previous)
        check_permissions()
        plan_id = plan_store.insert(pack, data, prepared, key, token, actor, plan_contract.MODEL, plan_contract.VERSION)
        # 调用前挂接每日账本，长请求期间也能从历史详情核对本次实际输入。
        if on_registered is not None:on_registered(plan_id)
        usage, result, code = None, None, None
        try:
            check_permissions()
        except DomainError:
            plan_store.finish(plan_id, 'failed', code='not_sent', message=model_text('plans.a295aba9ccb2'))
            raise
        try:
            result, usage, next_memory = conversation.run(data,prepared,session,plan_id,analyze,check_permissions)
            for action in result['actions']:
                action.update(execution_available=interfaces.supports(action),
                              execution_status='not_started', runtime_mode=interfaces.MODE)
            result.update(runtime_mode=interfaces.MODE,
                          analysis_provider=(usage or {}).get('provider',plan_provider.PROVIDER),
                          execution_available=any(r['execution_available'] for r in result['actions']))
            state, message = 'ready', model_text('plans.dccbe58697f8')
        except plan_provider.Failure as error:
            usage=error.usage or {'provider':plan_provider.PROVIDER,'billable':error.code!='configuration'}
            state,code,message=('unknown' if error.code=='unknown' else 'invalid' if error.code=='invalid_output' else 'failed'),error.code,str(error)
        except DomainError as error:
            state, code, message = 'invalid', 'contract', str(error)
        except Exception:
            state, code, message = 'unknown', 'analysis_error', model_text('plans.c8a9b0b8c83a')
        def checkpoint(cur):
            """每日方案和记忆入库前同事务检查统一队列租约，迟到实例不得覆盖新状态。"""
            if queue_task:
                from . import chat_tasks
                chat_tasks.fence(cur,queue_task['id'],queue_task['owner_token'])
            conversation_store.checkpoint(cur,session,plan_id,next_memory)
        saved = plan_store.finish(plan_id, state, result, usage, code, message,
            on_ready=checkpoint if state=='ready' else None)
        check_permissions()
        return {**saved, 'reused': False}
