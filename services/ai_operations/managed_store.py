"""ASIN托管绑定AI人员、负责区间与每日账本，不修改Amazon商品或广告。

开启/恢复交接遵循固定北京时间15:00：2026-10-26 14:00→27日，15:00→28日。
暂停仅停止任务，负责人不自动退回；需要人工在分配页明确交接。
"""
import json
from datetime import timezone
from zoneinfo import ZoneInfo
from services.listing_performance.common import DomainError, positive_id, utc_now, identity
from services.listing_performance import ownership_service, ownership_repository
from services import operator_accounts
from . import plan_store, repository
from .metrics import dumps


def target(cur, managed_id, lock=False):
    """按托管编号查询配置，写入流程可加行锁。"""
    cur.execute('SELECT * FROM ai_operation_managed_asins WHERE id=%s'+(' FOR UPDATE' if lock else ''), (managed_id,))
    row = cur.fetchone()
    if not row:raise DomainError('托管ASIN不存在', 404)
    return row


def serialize_target(row):
    """转换托管配置的编号与启停状态，供接口返回。"""
    return {k: str(v) if v is not None and k in ('id','shop_id','actor_id','executor_user_id') else bool(v) if k == 'enabled' else v for k,v in row.items()}


def serialize_run(row):
    """解析每日账本摘要并保留任务日期与关联计划。"""
    result = {k: str(v) if v is not None and k in ('id','managed_id','plan_id','actor_id','executor_user_id') else v
              for k,v in row.items() if k not in ('summary_json','audit_context_json')}
    result['summary'] = json.loads(row['summary_json'])
    result['audit_context'] = json.loads(row['audit_context_json']) if row.get('audit_context_json') else None
    return result


def get_target(managed_id):
    """只读获取指定ASIN托管配置。"""
    with plan_store.transaction() as cur:return serialize_target(target(cur, positive_id(managed_id,'托管编号')))


def add(body, actor):
    """核对本店子ASIN并绑定AI，配置及负责历史同事务；重复添加不自动恢复暂停。"""
    if not isinstance(body,dict) or set(body) != {'shop_id','asin'}:raise DomainError('请选择本店ASIN')
    import re
    shop = positive_id(body['shop_id'],'店铺');asin = str(body['asin']).strip().upper()
    if not re.fullmatch('[A-Z0-9]{10}',asin):raise DomainError('ASIN无效')
    with plan_store.transaction(True) as cur:
        ai = operator_accounts.read_ai(cur)
        cur.execute('''SELECT s.id FROM amazon_shops s WHERE s.id=%s AND s.status=1 AND s.region='na'
            AND s.marketplace_id=%s AND s.ads_profile_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM amazon_listings l WHERE l.shop_id=s.id AND BINARY l.marketplace_id=BINARY s.marketplace_id
            AND BINARY l.asin=BINARY %s AND l.is_deleted=0 AND COALESCE(LOWER(l.parentage_level),'')<>'parent') FOR UPDATE''', (shop,repository.US,asin))
        if not cur.fetchone():raise DomainError('仅支持已配置Ads账户的美国站本店子ASIN',422)
        cur.execute('''INSERT INTO ai_operation_managed_asins (shop_id,marketplace_id,asin,actor_id,executor_user_id,created_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)''',
            (shop,repository.US,asin,positive_id(actor,'用户'),ai['id']))
        row = target(cur, cur.lastrowid)
        if row['enabled']:
            bind_owner(cur, row, ai, actor)
        return serialize_target(target(cur, row['id']))


def set_enabled(managed_id, body, actor=None):
    """暂停保留负责人；恢复或同步AI归属同事务预约交接，不调用商品/广告修改。"""
    if not isinstance(body,dict) or set(body) != {'enabled'} or type(body['enabled']) is not bool:
        raise DomainError('托管状态必须为启用或暂停')
    with plan_store.transaction(True) as cur:
        chosen=positive_id(managed_id,'托管编号')
        row=target(cur,chosen)
        # 与人工分配统一店铺→托管→SKU顺序，不能先锁托管再争抢店铺。
        cur.execute('SELECT id FROM amazon_shops WHERE id=%s FOR UPDATE', (row['shop_id'],))
        row=target(cur,chosen,True)
        actor=positive_id(actor if actor is not None else row['actor_id'],'启用人员')
        if body['enabled']:
            bind_owner(cur,row,operator_accounts.read_ai(cur),actor)
        cur.execute('''UPDATE ai_operation_managed_asins SET enabled=%s,
            actor_id=IF(%s,%s,actor_id),updated_at=UTC_TIMESTAMP(3) WHERE id=%s''',
                    (body['enabled'],body['enabled'],actor,chosen))
        return serialize_target(target(cur,chosen))


def bind_owner(cur, managed, ai, actor):
    """在已锁店铺的配置事务内绑定AI及全部子SKU，任何交接冲突都回滚配置。"""
    initiator=operator_accounts.get_user(cur,actor)
    if initiator['account_type']!='human':raise DomainError('开启托管需要实际人工操作人员',403)
    ownership_service.ensure_ai_owner(cur,managed['shop_id'],managed['marketplace_id'],managed['asin'],ai['id'],actor)
    cur.execute('UPDATE ai_operation_managed_asins SET executor_user_id=%s WHERE id=%s',(ai['id'],managed['id']))


def execution_identity(managed):
    """只读核对AI绑定及全部子SKU当前/预约归属；人工交接后不擅自夺回负责人。"""
    if not managed.get('executor_user_id'):
        raise DomainError('旧托管尚未绑定AI，请点击同步AI归属后运行',409)
    with plan_store.transaction() as cur:
        ai=operator_accounts.read_ai(cur,managed['executor_user_id'])
        cur.execute('''SELECT sku FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s AND BINARY asin=BINARY %s
            AND is_deleted=0 AND COALESCE(LOWER(parentage_level),'')<>'parent' LIMIT 201''',
            (managed['shop_id'],managed['marketplace_id'],managed['asin']))
        rows=cur.fetchall();now=utc_now()
        if not 1<=len(rows)<=200:raise DomainError('托管商品子SKU缺失或超过200个，请核对配置',409)
        for row in rows:
            scope=ownership_repository.scope(cur,{'identity_key':identity(int(managed['shop_id']),managed['marketplace_id'],row['sku'])})
            histories=ownership_repository.histories(cur,scope['id']) if scope else []
            current=ownership_repository.active_at(histories,now)
            future=[h for h in histories if h['effective_from_utc']>now]
            selected=future[0] if len(future)==1 else current if not future else None
            if not selected or selected['owner_user_id']!=ai['id']:
                raise DomainError(f"SKU {row['sku']} 未归属或未预约给AI；请暂停检查人工交接，确认后同步AI归属",409)
        return operator_accounts.snapshot(ai)


def listing(params):
    """分页查询托管商品、当前保留观察和最近会话运行。"""
    page=positive_id(params.get('page',1),'页码')
    if page>100000:raise DomainError('页码过大')
    keyword=str(params.get('keyword','')).strip()[:100]
    where,args='1=1',[]
    if params.get('shop_id'):
        where+=' AND m.shop_id=%s';args.append(positive_id(params['shop_id'],'店铺'))
    if params.get('enabled') in ('1','0'):
        where+=' AND m.enabled=%s';args.append(int(params['enabled']))
    if keyword:
        where+=''' AND (m.asin LIKE %s OR s.shop_name LIKE %s OR EXISTS (
            SELECT 1 FROM amazon_listings l LEFT JOIN products p ON BINARY p.seller_sku=BINARY l.sku
            WHERE l.shop_id=m.shop_id AND BINARY l.marketplace_id=BINARY m.marketplace_id AND BINARY l.asin=BINARY m.asin
            AND (l.sku LIKE %s OR p.product_name LIKE %s)))'''
        args += ['%'+keyword+'%']*4
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ai_operation_managed_asins m JOIN amazon_shops s ON s.id=m.shop_id WHERE '+where,args)
        total=cur.fetchone()['n']
        cur.execute('''SELECT m.*,s.shop_name FROM ai_operation_managed_asins m JOIN amazon_shops s ON s.id=m.shop_id
            WHERE '''+where+' ORDER BY m.id DESC LIMIT 20 OFFSET %s',(*args,(page-1)*20))
        items=[serialize_target(row) for row in cur.fetchall()]
        # 人员一次批量读取，避免逐条查名，也兼容隔离验证的MySQL临时用户表。
        people_ids={int(row[key]) for row in items for key in ('actor_id','executor_user_id') if row.get(key)}
        people={}
        if people_ids:
            cur.execute('SELECT id,nickname,username,status FROM users WHERE id IN ('+','.join(['%s']*len(people_ids))+')',tuple(people_ids))
            people={str(user['id']):user for user in cur.fetchall()}
        for row in items:
            ai,initiator=people.get(row.get('executor_user_id'),{}),people.get(row['actor_id'],{})
            row.update(executor_name=ai.get('nickname') or ai.get('username'),executor_status=ai.get('status'),
                       initiated_by_name=initiator.get('nickname') or initiator.get('username'))
            cur.execute('''SELECT l.sku,l.main_image_url,l.item_name,
                (SELECT MIN(p.product_name) FROM products p WHERE BINARY p.seller_sku=BINARY l.sku) product_name
                FROM amazon_listings l WHERE l.shop_id=%s AND l.marketplace_id=%s AND BINARY l.asin=BINARY %s
                AND l.is_deleted=0 AND COALESCE(LOWER(l.parentage_level),'')<>'parent' ORDER BY l.sku LIMIT 201''',
                (row['shop_id'],row['marketplace_id'],row['asin']))
            products=cur.fetchall();row.update(products=products,product_name=next((p['product_name'] for p in products if p['product_name']),None),
                main_image_url=next((p['main_image_url'] for p in products if p['main_image_url']),None),skus=[p['sku'] for p in products])
            cur.execute('SELECT * FROM ai_operation_daily_runs WHERE managed_id=%s ORDER BY run_date DESC,id DESC LIMIT 1',(row['id'],))
            last=cur.fetchone();row['latest_run']=serialize_run(last) if last else None
            cur.execute('''SELECT COUNT(*) n FROM ai_operation_executions e
                WHERE e.shop_id=%s AND e.marketplace_id=%s AND BINARY e.asin=BINARY %s
                AND e.runtime_mode='real' AND e.status='confirmed' AND NOT EXISTS (
                    SELECT 1 FROM ai_operation_executions n WHERE n.resource_key=e.resource_key
                    AND n.runtime_mode='real' AND n.id>e.id AND n.status IN ('confirmed','submitted','submitting','pending','unknown','issues')) ''',(row['shop_id'],row['marketplace_id'],row['asin']))
            row['active_observation_count']=cur.fetchone()['n']
            row['ownership']=ownership_summary(cur,row,products)
            row['ownership_consistent']=bool(row.get('executor_user_id') and row['ownership'] and all(
                str(owner['scheduled_owner_id'] if owner['has_scheduled'] else owner['current_owner_id'])==row['executor_user_id']
                for owner in row['ownership']))
        return {'list':items,'total':total,'page':page,'schedule':{'timezone':'Asia/Shanghai','time':'18:30','runtime_mode':'real',
                'analysis_provider':'deepseek','execution_mode':'real'}}


def ownership_summary(cur, managed, products):
    """读取托管列表的实际当前/预约负责人，避免把托管开关冒充已经生效的负责人。"""
    result=[];now=utc_now()
    for product in products:
        scope=ownership_repository.scope(cur,{'identity_key':identity(int(managed['shop_id']),managed['marketplace_id'],product['sku'])})
        histories=ownership_repository.histories(cur,scope['id']) if scope else []
        current=ownership_repository.active_at(histories,now)
        future=[h for h in histories if h['effective_from_utc']>now]
        scheduled=future[0] if future else None
        result.append({'sku':product['sku'],'current_owner':current.get('owner_name') if current else None,
                       'current_owner_id':str(current['owner_user_id']) if current and current['owner_user_id'] else None,
                       'scheduled_owner':scheduled.get('owner_name') if scheduled else None,
                       'has_scheduled':scheduled is not None,
                       'scheduled_owner_id':str(scheduled['owner_user_id']) if scheduled and scheduled['owner_user_id'] else None,
                       'effective_date':scheduled['effective_from_utc'].replace(tzinfo=timezone.utc)
                           .astimezone(ZoneInfo(scheduled['business_timezone'])).date().isoformat() if scheduled else None})
    return result


def enabled_targets():
    """读取参与每日任务的配置编号，超上限明确拒绝。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT id FROM ai_operation_managed_asins WHERE enabled=1 ORDER BY id LIMIT 1001')
        rows=cur.fetchall()
        if len(rows)>1000:raise DomainError('本轮启用托管超过1000个，请分批运行',422)
        return [str(r['id']) for r in rows]


def begin(managed, run_date, report_date, source, actor,run_mode='conv_real'):
    """只登记会话版每日运行；同ASIN同日复用已有任务，不重复收费。"""
    if run_mode!='conv_real':raise DomainError('仅支持当前会话版每日分析')
    if not managed.get('executor_user_id'):raise DomainError('旧托管尚未绑定AI，请先同步AI归属后运行',409)
    with plan_store.transaction(True) as cur:
        target(cur,managed['id'],True)
        audit={'source':'AI','executor':operator_accounts.snapshot(operator_accounts.read_ai(cur,managed['executor_user_id'])),
               'initiated_by':operator_accounts.snapshot(operator_accounts.get_user(cur,actor))}
        cur.execute('''INSERT INTO ai_operation_daily_runs
            (managed_id,run_date,report_date,runtime_mode,trigger_source,status,actor_id,executor_user_id,audit_context_json,summary_json,message,started_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,'preparing',%s,%s,%s,'{}','正在整理今日资料与历史调整',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))
            ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)''',
            (managed['id'],run_date,report_date,run_mode,source,actor,managed['executor_user_id'],dumps(audit)))
        cur.execute('SELECT * FROM ai_operation_daily_runs WHERE id=%s',(cur.lastrowid,))
        return serialize_run(cur.fetchone())


def update(run_id, status, message, plan_id=None, summary=None, done=False):
    """保存每日任务阶段、摘要和完成时间，不覆盖已有计划关联。"""
    with plan_store.transaction(True) as cur:
        cur.execute('''UPDATE ai_operation_daily_runs SET status=%s,message=%s,plan_id=COALESCE(%s,plan_id),
            summary_json=COALESCE(%s,summary_json),updated_at=UTC_TIMESTAMP(3),
            completed_at=IF(%s,UTC_TIMESTAMP(3),completed_at) WHERE id=%s''',
            (status,message[:500],plan_id,dumps(summary) if summary is not None else None,done,run_id))


def get_run(run_id):
    """只读取得指定每日运行，不存在则返回业务错误。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_daily_runs WHERE id=%s',(positive_id(run_id,'优化记录'),))
        row=cur.fetchone()
        if not row:raise DomainError('优化记录不存在',404)
        return serialize_run(row)


def history(managed_id,params):
    """只读查询会话相关历史，不触发模型或正式执行。"""
    managed=get_target(managed_id);page=positive_id(params.get('page',1),'页码')
    if page>100000:raise DomainError('页码过大')
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ai_operation_daily_runs WHERE managed_id=%s',(managed['id'],));total=cur.fetchone()['n']
        cur.execute('SELECT * FROM ai_operation_daily_runs WHERE managed_id=%s ORDER BY run_date DESC,id DESC LIMIT 20 OFFSET %s',(managed['id'],(page-1)*20))
        return {'managed':managed,'list':[serialize_run(r) for r in cur.fetchall()],'page':page,'total':total}
