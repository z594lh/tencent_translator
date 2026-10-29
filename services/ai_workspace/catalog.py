"""AI商品目录：当前负责人、筛选选项及分页，始终叠加原商品访问范围。"""
from datetime import datetime,timezone
from services.ai_operations import plan_store
from services.listing_performance.common import DomainError,positive_id
from . import products,access

OWNERS='''LEFT JOIN (
    SELECT rs.shop_id,rs.marketplace_id,rs.seller_sku,COUNT(rh.id) owner_count,
        CASE WHEN COUNT(rh.id)=1 THEN MAX(rh.owner_user_id) ELSE NULL END owner_id
    FROM listing_responsibility_scopes rs JOIN listing_responsibility_history rh ON rh.scope_id=rs.id
    WHERE rh.voided_at IS NULL AND rh.effective_from_utc<=%s
        AND (rh.effective_to_utc IS NULL OR rh.effective_to_utc>%s)
    GROUP BY rs.shop_id,rs.marketplace_id,rs.seller_sku
) own ON own.shop_id=l.shop_id AND BINARY own.marketplace_id=BINARY l.marketplace_id AND BINARY own.seller_sku=BINARY l.sku
LEFT JOIN users owner ON owner.id=own.owner_id '''


def listing(params,actor,permission):
    codes=access.authorize(actor,permission);allowed,scope_args=access.predicate(actor,codes)
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    base=products.FROM+OWNERS+' WHERE '+products.VALID+" AND s.status=1 AND s.region='na' AND l.marketplace_id='ATVPDKIKX0DER' AND "+allowed
    base_args=[now,now,*scope_args];where='';args=[]
    query=str(params.get('q','')).strip()[:100];mode=params.get('managed','all')
    if mode not in ('all','yes','no','active','paused'):raise DomainError('托管状态筛选无效',422)
    if query:
        where+=' AND (l.asin LIKE %s OR l.sku LIKE %s OR l.item_name LIKE %s OR EXISTS (SELECT 1 FROM products p WHERE BINARY p.seller_sku=BINARY l.sku AND p.product_name LIKE %s))'
        args += [query+'%','%'+query+'%','%'+query+'%','%'+query+'%']
    if mode=='no':where+=' AND m.id IS NULL'
    elif mode!='all':
        where+=' AND m.id IS NOT NULL'
        if mode in ('active','paused'):where+=' AND m.enabled=%s';args.append(1 if mode=='active' else 0)
    if params.get('shop_id'):where+=' AND l.shop_id=%s';args.append(positive_id(params['shop_id'],'店铺'))
    if params.get('owner_id')=='none':where+=' AND COALESCE(own.owner_count,0)=0'
    elif params.get('owner_id'):where+=' AND own.owner_id=%s';args.append(positive_id(params['owner_id'],'负责人'))
    page=positive_id(params.get('page',1),'页码');size=positive_id(params.get('page_size',20),'每页条数')
    if size not in (20,50,100) or page>10000:raise DomainError('分页范围无效',422)
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) total '+base+where,tuple([*base_args,*args]));total=cur.fetchone()['total']
        cur.execute('''SELECT l.id product_id,l.shop_id,l.marketplace_id,l.asin,l.sku,l.item_name,l.main_image_url,s.shop_name,
            m.id,m.enabled,own.owner_id,COALESCE(own.owner_count,0) owner_count,
            COALESCE(NULLIF(owner.nickname,''),owner.username) owner_name,owner.account_type owner_type,
            (SELECT MIN(p.product_name) FROM products p WHERE BINARY p.seller_sku=BINARY l.sku) product_name
            '''+base+where+' ORDER BY l.id DESC LIMIT %s OFFSET %s',tuple([*base_args,*args,size,(page-1)*size]));rows=cur.fetchall()
        filters=None
        if str(params.get('include_filters',''))=='1':
            cur.execute('SELECT DISTINCT l.shop_id id,s.shop_name name '+base+' ORDER BY s.shop_name,l.shop_id',tuple(base_args));shops=cur.fetchall()
            cur.execute("SELECT DISTINCT own.owner_id id,COALESCE(NULLIF(owner.nickname,''),owner.username) name,owner.account_type "+base+' AND own.owner_id IS NOT NULL ORDER BY name,own.owner_id',tuple(base_args));owners=cur.fetchall()
            filters={'shops':shops,'owners':owners}
    items=[{**products.public(row),'owner_id':str(row['owner_id']) if row['owner_id'] else None,
            'owner_name':row['owner_name'],'owner_type':row['owner_type'],'owner_conflict':row['owner_count']>1} for row in rows]
    return {'list':items,'total':total,'page':page,'page_size':size,'has_more':page*size<total,'filters':filters}
