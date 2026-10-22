"""正式行动编排：先持久意图再一次发送，未知只回读，网络等待不占数据库事务。"""
from contextlib import contextmanager
from datetime import datetime
import re
from services.listing_performance.common import DomainError, positive_id
from . import plan_store, execution_store as store, platform_operations as interfaces, execution_diagnostics
from services.operation_identity import ai_operation
from services import operation_identity
from .catalog import REPORT_PLACEMENTS
from .metrics import ADS_FIELDS,totals,ratios


def ad_baseline(data,action):
    """按行动对象匹配保存的广告矩阵；组竞价聚合该组，不能只取第一条投放。"""
    if not action['kind'].startswith('ads_'):return None
    kind={'ads_bid_update':'spTargeting','ads_budget_update':'spCampaigns'}.get(action['kind'],'spCampaignsPlacement')
    module=next((m for m in data['ads'] if m['kind']==kind),None)
    if not module:return None
    subject=action['subject'];matches=[]
    for row in module['details']:
        dim=row['dimensions']
        if str(dim.get('campaign_id'))!=str(subject['campaign_id']):continue
        if kind=='spCampaignsPlacement' and REPORT_PLACEMENTS.get(dim.get('placement'))!=subject['placement']:continue
        if kind=='spTargeting':
            if str(dim.get('ad_group_id'))!=str(subject['ad_group_id']):continue
            if subject['entity_type']!='group' and str(dim.get('keyword_id'))!=str(subject['entity_id']):continue
        matches.append(row)
    if not matches:return None
    columns=['window',*ADS_FIELDS,'acos','ctr','cpc'];rows=[]
    for window in ('d3','d7','d14','d30'):
        values=[]
        for match in matches:
            matrix=match['periods'];line=next((r for r in matrix['rows'] if r[0]==window),None)
            if line:values.append(dict(zip(matrix['columns'],line)))
        metrics=ratios(totals(values,ADS_FIELDS),True)
        # 截选对象或缺矩阵时不把部分基线声称为完整组值。
        partial=len(values)!=len(matches) or (kind=='spTargeting' and subject.get('entity_type')=='group' and bool(module.get('omitted_objects')))
        rows.append([window,*[None if partial else metrics.get(k) for k in columns[1:]]])
    return {'periods':{'columns':columns,'rows':rows},'evidence_id':','.join(r['evidence_id'] for r in matches)}


def baseline(plan, action, current):
    """保存执行前设置、经营广告窗口和固定关键词作为观察基线。"""
    data = plan['dataset']['content']
    business = next((b for b in data['business'] if b['sku'] == action['subject'].get('sku')), None)
    # 广告位为活动范围；关联ASIN经营指标单列来源，不能冒充活动独占利润。
    if business is None and len(data['business']) == 1:
        business = data['business'][0]
    ad=ad_baseline(data,action)
    windows = {}
    for day in (3, 7, 14, 30):
        rows = []
        for metric in action['observation']['metrics']:
            if metric in ('organic_rank', 'sponsored_rank', 'traffic_share', 'organic_share', 'sponsored_share', 'weekly_impressions'):
                continue
            source = business if metric in ('net_profit', 'sales_qty', 'sales_amount', 'ad_cost', 'tacos','refund_amount','ad_sales','ad_orders','profit_margin') else ad
            matrix = source['periods'] if source else {'columns': [], 'rows': []}
            row = next((r for r in matrix['rows'] if r[0] == 'd'+str(day)), [])
            value = row[matrix['columns'].index(metric)] if row and metric in matrix['columns'] else None
            rows.append({'metric': metric, 'before': value,
                         'scope': ('关联ASIN经营，非活动独占' if source is business and source else '指定广告对象'),
                         'source_ref': source.get('evidence_id') if source else None})
        windows[str(day)] = rows
    words = set(action['observation']['keywords'])
    return {'captured_at': datetime.utcnow(), 'identity': data['identity'], 'report_date': plan['report_date'],
            'settings': current, 'observation_plan': action['observation'], 'windows': windows,
            'keywords': [{k: row.get(k) for k in ('keyword', 'organic_rank', 'traffic_share')}
                         for row in data['keywords']['items'] if row['keyword'] in words],
            'keyword_snapshot': data['keywords']['snapshot'],
            'note': '设置前值须经平台实时核对；经营/关键词基线来自保存资料，不冒充执行当日最新报告。'}


def require_real(plan):
    """只允许当前正式会话进入真实执行，旧样本不能转换后提交。"""
    if plan['runtime_mode'] != interfaces.MODE or plan['status'] != 'ready' or not plan.get('result'):
        raise DomainError('只能执行当前正式会话有效方案，旧样本不能提交', 409)
    if plan.get('task_id') is not None or plan.get('ai_call',{}).get('prompt',{}).get('conversation',{}).get('purpose')=='chat':
        from . import confirmations
        proof=operation_identity.metadata()
        if not proof or not proof.get('confirmation_id'):raise DomainError('人工建议必须先经用户明确确认并进入执行队列',409)
        with plan_store.transaction() as cur:confirmations.require_execution(cur,plan,proof)


@contextmanager
def resource_lock(identity, action):
    """持有独立连接的账户资源锁，不持有事务等待外部HTTP。"""
    conn = plan_store.get_db_connection()
    name = 'ai_live:' + interfaces.lock_key(identity, action)[:45]
    got = False
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s,0) acquired', (name,))
            got = cur.fetchone()['acquired'] == 1
        conn.rollback()
        if not got: raise DomainError('相同SKU或广告活动正在处理，请稍后核对历史', 409)
        yield
    finally:
        try:
            if got:
                with conn.cursor() as cur: cur.execute('SELECT RELEASE_LOCK(%s)', (name,))
        finally: conn.close()


def order_actions(selected):
    """按真实选中集合保证同SKU标题先于亮点；日常逐项执行也复用相同顺序。"""
    ordered=[]
    for action in selected:
        if action['kind']=='listing_text_update' and action['subject']['field']=='title_differentiation':
            ordered.extend(a for a in selected if a not in ordered and a['kind']=='listing_text_update'
                and a['subject'].get('sku')==action['subject'].get('sku') and a['subject']['field']=='item_name')
        if action not in ordered:ordered.append(action)
    return ordered


def execute(plan_id, body, actor, authorize):
    """按已保存行动编号执行，浏览器和模型不能临时替换目标参数。"""
    plan_id, actor = positive_id(plan_id, '方案'), positive_id(actor, '用户')
    if not isinstance(body, dict) or set(body) != {'action_ids'}:
        raise DomainError('只接受保存的行动编号，不接受场景或修改后的目标')
    ids = body['action_ids']
    if not isinstance(ids, list) or not 1 <= len(ids) <= 12 or any(not isinstance(i, str) or not re.fullmatch(r'a\d{1,2}', i) for i in ids) or len(set(ids)) != len(ids):
        raise DomainError('请选择1到12条不重复行动')
    plan = plan_store.get(plan_id); require_real(plan)
    actions = {a['action_id']: a for a in plan['result']['actions']}
    if any(i not in actions or not interfaces.supports(actions[i]) for i in ids):
        raise DomainError('该行动没有正式适配，未提交', 422)
    results = []
    # 同SKU如同时改标题/亮点，先提交标题；亮点仍实时回读，不能凭上一接口接受就假设已合规。
    for action in order_actions([actions[i] for i in ids]):
        authorize()
        results.append(_execute_one(plan, action, actor, authorize))
    return {'runtime_mode': interfaces.MODE, 'list': results}


def check_pending(cur, identity, action):
    """同账户资源存在不确定提交时禁止新写入，不以新方案绕过旧请求。"""
    cur.execute("""SELECT * FROM ai_operation_executions WHERE shop_id=%s AND marketplace_id=%s
        AND runtime_mode='real' AND status IN ('submitting','submitted','pending','unknown','issues')
        ORDER BY id LIMIT 1001""", (identity['id'], identity['marketplace_id']))
    rows = cur.fetchall()
    if len(rows) > 1000: raise DomainError('待核对执行超过1000条，请先处理历史', 409)
    for row in rows:
        saved = store.serialize(row)
        if interfaces.lock_key(saved['baseline']['identity'], saved['action']) == interfaces.lock_key(identity, action):
            raise DomainError('该SKU或活动有未确认提交，请先回读，不重复修改', 409)


def read_record(execution_id):
    """独立短事务读取执行事实，并拒绝旧模式。"""
    with plan_store.transaction() as cur: row = store.get(cur, positive_id(execution_id, '执行记录'))
    if not row: raise DomainError('执行记录不存在', 404)
    if row['runtime_mode'] != interfaces.MODE: raise DomainError('旧样本不能进入正式通道', 409)
    return store.serialize(row)


def _execute_one(plan, action, actor, authorize):
    """提交前后分别短事务落库，发送阶段崩溃只能回读不能重发。"""
    identity = plan['dataset']['content']['identity']
    with resource_lock(identity, action):
        with plan_store.transaction(True) as cur:
            previous = store.existing(cur, plan['id'], action['action_id'])
            if previous: return {**store.serialize(previous), 'reused': True}
            check_pending(cur, identity, action)
            execution_id = store.insert(cur, plan, action, interfaces.resource_key(identity, action['ref']), actor,
                baseline(plan, action, {'value': action['before'], 'source': '分析资料缓存，尚未经平台预检'}))
        return attempt(plan, action, execution_id, authorize)


def hold(plan,action,actor,message):
    """保存明确未发送的政策/依赖冲突，不把阻止的行动冒充成功或可自动补执行。"""
    identity=plan['dataset']['content']['identity']
    with resource_lock(identity,action):
        with plan_store.transaction(True) as cur:
            old=store.existing(cur,plan['id'],action['action_id'])
            if old:return store.serialize(old)
            eid=store.insert(cur,plan,action,interfaces.resource_key(identity,action['ref']),actor,
                baseline(plan,action,{'value':action['before'],'source':'原分析前值，未提交平台'}))
            store.submitted(cur,eid,'conflict',{'accepted':False,'runtime_mode':'real','message':message,'policy_hold':True})
    return read_record(eid)


def with_attempt(receipt, recovery):
    """保留原失败及补执行发起人，避免覆盖历史或递归嵌套旧回执。"""
    if recovery:
        receipt.update(recovery_attempts=recovery['attempts'], recovery_context=recovery['context'])
    receipt['submitted_at'] = datetime.utcnow().isoformat() if receipt.get('accepted') is not False else None
    return receipt


def attempt(plan, action, execution_id, authorize, recovery=None):
    """复用同一正式提交链，调用方必须持有资源锁及显式AI身份。"""
    identity = plan['dataset']['content']['identity']
    stage = 'authorize'
    try:
        authorize()
        stage = 'read_platform'
        current = interfaces.read(identity, action)
        stage = 'prepare_action'
        prepared = interfaces.prepare(identity, action, current)
        # 原广告日志从真实读取的前值留痕，不能沿用陈旧缓存。
        stage = 'cache_ads'
        if action['kind'].startswith('ads_'): interfaces.cache_ads(identity, action, current)
        saved_baseline = baseline(plan, action, {'value': current['value'], 'source': current['source']})
        saved_baseline['prepared'] = prepared
        stage = 'save_baseline'
        with plan_store.transaction(True) as cur: store.prepared(cur, execution_id, saved_baseline)
        stage = 'authorize_before_send'
        authorize()
    except Exception as error:
        receipt = interfaces.failure(error)
        receipt.update(accepted=False, message=str(error) if isinstance(error, (DomainError, interfaces.pricing.PricingError)) else '平台预检未完成，未发送修改')
        receipt['diagnostic'] = execution_diagnostics.capture(error, stage, execution_id)
        receipt = with_attempt(receipt, recovery)
        with plan_store.transaction(True) as cur:
            store.submitted(cur, execution_id, 'conflict' if getattr(error, 'status', getattr(error, 'code', None)) == 409 else 'not_sent', receipt)
        return {**read_record(execution_id), 'reused': False}
    # 此阶段提交成功落库后才能调用写接口；之后的异常一律不得自动重发。
    with plan_store.transaction(True) as cur:
        store.submitted(cur, execution_id, 'submitting', with_attempt({'accepted': None, 'runtime_mode': interfaces.MODE,
            'message': '已登记发送阶段，进程中断时只回读核对，不自动重发'}, recovery))
    try: receipt = interfaces.submit(identity, action, current, prepared)
    except Exception as error:
        receipt = interfaces.failure(error)
        receipt['diagnostic'] = execution_diagnostics.capture(error, 'submit_platform', execution_id)
    interfaces.link_history(identity, action, receipt)
    receipt = with_attempt(receipt, recovery)
    status = 'not_sent' if receipt.get('not_sent') else 'unknown' if receipt['accepted'] is None else 'submitted' if receipt['accepted'] else 'rejected'
    with plan_store.transaction(True) as cur:
        store.submitted(cur, execution_id, status, receipt)
    return {**read_record(execution_id), 'reused': False}


def history(plan_id):
    """只读查询执行历史，不触发模型、提交或平台回读。"""
    plan = plan_store.get(positive_id(plan_id, '方案'))
    return {'runtime_mode': plan['runtime_mode'], 'list': store.list_for_plan(plan['id'])}


def verify(execution_id):
    """恢复原AI身份，仅从平台回读；跨日核对不因签名过期丢失依据。"""
    record = read_record(execution_id)
    if record['status'] in ('rejected', 'conflict', 'not_sent', 'superseded', 'confirmed'): return record
    identity, action = record['baseline']['identity'], record['action']
    proof = (record['receipt'] or {}).get('recovery_context') or record['audit_context']
    if not proof: raise DomainError('执行身份缺失，不能核对其他操作', 409)
    with resource_lock(identity, action):
        record = read_record(execution_id)
        if record['status'] == 'preparing':
            with plan_store.transaction(True) as cur:
                store.submitted(cur, execution_id, 'not_sent', {**(record['receipt'] or {}), 'accepted': False, 'runtime_mode': interfaces.MODE,
                    'message': '发送前流程中断，未进入平台发送阶段；不会自动接续旧行动'})
            return read_record(execution_id)
        operation = {'listing_text_update': 'text', 'listing_price_update': 'price'}.get(action['kind'], 'update')
        context={k:proof.get(k) for k in ('task_id','plan_id','confirmation_id','conversation_id','proposed_by')} if proof.get('confirmation_id') else {}
        with ai_operation(proof['executor']['id'], proof['initiated_by']['id'], proof['run_id'], proof['action_id'], operation,**context):
            try:
                result = interfaces.verify(identity, action, record['baseline'].get('prepared') or {}, record['receipt'] or {})
                status = 'confirmed' if result['matched'] else result['state'] if result['state'] in ('issues', 'conflict') else 'pending'
            except Exception as error:
                result = {'matched': False, 'runtime_mode': interfaces.MODE, 'message': '真实回读未完成，保留原状态；不重新提交'}
                result['diagnostic'] = execution_diagnostics.capture(error, 'verify_platform', execution_id)
                status = record['status']
            with plan_store.transaction(True) as cur: store.verified(cur, execution_id, status, result)
        return read_record(execution_id)
