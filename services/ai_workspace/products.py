"""Listing解析为AI商品范围；不新增托管、不修改负责人，不返回账户密钥。"""
from services.ai_operations import plan_store,conversation_store
from services.listing_performance.common import DomainError,positive_id
from . import access

FROM='''FROM amazon_listings l JOIN amazon_shops s ON s.id=l.shop_id AND BINARY s.marketplace_id=BINARY l.marketplace_id
    LEFT JOIN ai_operation_managed_asins m ON m.shop_id=l.shop_id AND BINARY m.marketplace_id=BINARY l.marketplace_id AND BINARY m.asin=BINARY l.asin
'''
VALID="l.is_deleted=0 AND COALESCE(LOWER(l.parentage_level),'')<>'parent'"
SELECT='''SELECT l.id product_id,l.shop_id,l.marketplace_id,l.asin,l.sku,l.item_name,
    l.main_image_url,s.shop_name,s.status shop_status,s.credential_group_id,s.region,s.ads_profile_id,
    m.id,m.executor_user_id,m.enabled,m.actor_id,
    (SELECT MIN(p.product_name) FROM products p WHERE BINARY p.seller_sku=BINARY l.sku) product_name
    '''+FROM+' WHERE '+VALID+' '


def finish(cur,row):
    if not row:raise DomainError('商品不存在或Listing已删除',404)
    identity={k:row[k] for k in ('credential_group_id','region','marketplace_id','ads_profile_id','asin')};identity['id']=row['shop_id']
    row['identity']=identity;row['scope_key']=conversation_store.scope(identity)[1]
    cur.execute("SELECT id FROM ai_operation_conversations WHERE scope_key=%s AND state='active' ORDER BY generation DESC LIMIT 1",(row['scope_key'],))
    current=cur.fetchone();row['current_conversation_id']=current['id'] if current else None
    return row


def resolve(shop,asin):
    with plan_store.transaction() as cur:
        cur.execute(SELECT+' AND l.shop_id=%s AND BINARY l.asin=BINARY %s ORDER BY l.id LIMIT 2',(positive_id(shop,'店铺'),asin))
        rows=cur.fetchall()
        if len(rows)>1:raise DomainError('该ASIN对应多个有效SKU，不能自动选择分析范围',409)
        return finish(cur,rows[0] if rows else None)


def get(identifier):
    with plan_store.transaction() as cur:
        cur.execute(SELECT+' AND l.id=%s',(positive_id(identifier,'商品'),));row=cur.fetchone()
    if not row:raise DomainError('商品不存在',404)
    return resolve(row['shop_id'],row['asin'])


def resolve_sku(shop,sku):
    with plan_store.transaction() as cur:
        cur.execute(SELECT+' AND l.shop_id=%s AND BINARY l.sku=BINARY %s LIMIT 2',(positive_id(shop,'店铺'),sku));rows=cur.fetchall()
    if len(rows)!=1:raise DomainError('SKU没有唯一有效Listing，不能打开ASIN对话',409)
    return resolve(shop,rows[0]['asin'])


def lock_current(cur,target):
    """所有入口同店铺短事务锁，锁后重查账户及当前会话。"""
    cur.execute('SELECT id FROM amazon_shops WHERE id=%s FOR UPDATE',(target['shop_id'],));cur.fetchone()
    cur.execute(SELECT+' AND l.id=%s',(target['product_id'],));latest=finish(cur,cur.fetchone())
    if latest['scope_key']!=target['scope_key']:raise DomainError('账户范围已变化，请刷新',409)
    if latest['shop_status']!=1:raise DomainError('店铺已停用，不能登记新任务或确认执行',409)
    return latest


def public(row):
    return {**{k:row.get(k) for k in ('shop_name','asin','sku','product_name','item_name','main_image_url')},
            'id':str(row['product_id']),'shop_id':str(row['shop_id']),'managed_id':str(row['id']) if row['id'] else None,
            'marketplace_id':row['marketplace_id'],'is_managed':row['id'] is not None,'enabled':bool(row['enabled']) if row['id'] else True}


def listing(params,actor,permission='ai_chat:use'):
    """对话和工具选择器共用授权商品目录。"""
    from . import catalog
    return catalog.listing(params,actor,permission)
