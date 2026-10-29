"""与托管解耦的共享会话目录与显式新建；复用原审计和问答存储。"""
from services.ai_operations import plan_store,conversation_store as store,chat_tasks,daily_runner,plans
from services.ai_operations.conversation_queries import public_session
from services.listing_performance.common import DomainError,positive_id
from . import products


def listing(target,params):
    before=positive_id(params['before_id'],'会话游标') if params.get('before_id') else None
    with plan_store.transaction() as cur:
        cur.execute('SELECT c.*,(SELECT MAX(created_at) FROM ai_operation_messages e WHERE e.conversation_id=c.id) last_message_at FROM ai_operation_conversations c WHERE scope_key=%s'+(' AND c.id<%s' if before else '')+' ORDER BY c.id DESC LIMIT 21',
                    (target['scope_key'],before) if before else (target['scope_key'],));rows=cur.fetchall()
    return {'list':[public_session(r) for r in rows[:20]],'has_more':len(rows)>20,'next_before_id':str(rows[19]['id']) if len(rows)>20 else None,
            'current_id':str(target['current_conversation_id']) if target['current_conversation_id'] else None,
            'capabilities':{'read':True,'new_conversation':True,'human_chat':True,'confirm_execution':True},'worker':chat_tasks.health()}


def create(target,body,actor):
    if not isinstance(body,dict) or set(body)!={'expected_current_id','confirm_new'} or body['confirm_new'] is not True:raise DomainError('请明确确认新建会话',422)
    expected=positive_id(body['expected_current_id'],'当前会话') if body['expected_current_id'] is not None else None
    if target['shop_status']!=1:raise DomainError('店铺已停用',409)
    with daily_runner.lock(target),plans.lock(target['shop_id'],target['asin']):
        with plan_store.transaction(True) as cur:
            latest=products.lock_current(cur,target);old_id=latest['current_conversation_id']
            if old_id!=expected:raise DomainError('当前会话已变化，请刷新',409)
            cur.execute("SELECT id FROM ai_operation_plans WHERE shop_id=%s AND BINARY asin=BINARY %s AND status='generating' LIMIT 1",(target['shop_id'],target['asin']))
            if cur.fetchone():raise DomainError('该商品仍有生成中的方案，请先核对原任务',409)
            cur.execute("SELECT id FROM ai_operation_tasks WHERE scope_key=%s AND status IN ('queued','running') LIMIT 1",(target['scope_key'],))
            if cur.fetchone():raise DomainError('该商品仍有排队或运行任务',409)
            cur.execute("SELECT id FROM ai_operation_executions WHERE shop_id=%s AND asin=%s AND status IN ('preparing','submitting') LIMIT 1",(target['shop_id'],target['asin']))
            if cur.fetchone():raise DomainError('仍有执行中的改动',409)
            if old_id:cur.execute("UPDATE ai_operation_conversations SET state='archived',archived_at=UTC_TIMESTAMP(3),archived_by=%s WHERE id=%s AND state='active'",(actor,old_id))
            boundary,key=store.scope(latest['identity']);sid=store.create_session(cur,latest['identity'],boundary,key)
            if latest['id']:cur.execute('UPDATE ai_operation_managed_asins SET current_conversation_id=%s WHERE id=%s',(sid,latest['id']))
            cur.execute('SELECT * FROM ai_operation_conversations WHERE id=%s',(sid,));return public_session(cur.fetchone())
