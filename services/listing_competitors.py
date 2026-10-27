"""Listing竞品本地关系；来源按店铺/站点核对，显式同意时同事务新增采集配置。

不修改亚马逊、不立即采集。来源ASIN改变时旧关系不显示；解绑不删除卖家精灵配置。
"""
import hashlib
import json
import re
from contextlib import contextmanager
import pymysql
from services.mysql_service import get_db_connection
from services import sellersprite_portal as portal

Error=portal.PortalError
MAX_COMPETITORS=50


@contextmanager
def session():
    """统一提交/回滚/关闭；竞品表或可选采集配置表缺失时明确提示迁移。"""
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:yield cur
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if isinstance(exc,(pymysql.err.ProgrammingError,pymysql.err.OperationalError)) and exc.args[0] in (1146,1054):
            raise Error('竞品绑定或采集配置表尚未初始化，请先执行上线SQL',503) from None
        raise
    finally:conn.close()


def owner(cur,shop_id,sku,lock=False):
    """限定当前启用店铺、匹配站点及未删除SKU，拒绝归属不唯一或无主ASIN的Listing。"""
    shop_id=portal.integer(shop_id,'店铺编号')
    if not isinstance(sku,str) or not sku or len(sku)>255:raise Error('SKU无效')
    cur.execute('''SELECT l.id,l.shop_id,l.sku,l.marketplace_id,l.asin FROM amazon_listings l
        JOIN amazon_shops s ON s.id=l.shop_id AND BINARY s.marketplace_id=BINARY l.marketplace_id
        WHERE l.shop_id=%s AND BINARY l.sku=BINARY %s AND l.is_deleted=0 AND s.status=1 LIMIT 2'''+(' FOR UPDATE' if lock else ''),(shop_id,sku))
    rows=cur.fetchall()
    if not rows:raise Error('当前店铺不存在可绑定竞品的Listing',404)
    if len(rows)!=1:raise Error('Listing站点不唯一，请先核对',409)
    result=rows[0];result['asin']=portal.asin_value(result['asin'])
    return result


def parse_asins(raw):
    """支持多行/空格/中英文逗号，规范为大写并保序去重；空列表表示解除全部绑定。"""
    if isinstance(raw,str):raw=re.split(r'[\s,，;；]+',raw.strip()) if raw.strip() else []
    if not isinstance(raw,list) or len(raw)>200:raise Error('请提供ASIN列表，最多50个不同竞品')
    values=list(dict.fromkeys(portal.asin_value(item) for item in raw))
    if len(values)>MAX_COMPETITORS:raise Error('每条Listing最多绑定50个不同竞品')
    return values


def bindings(cur,listing):
    """只读取当前主ASIN匹配的关系，SKU重新关联ASIN后不沿用旧竞品。"""
    cur.execute('''SELECT competitor_asin FROM amazon_listing_competitors
        WHERE listing_id=%s AND shop_id=%s AND marketplace_id=%s AND source_asin=%s ORDER BY sort_order,id''',
        (listing['id'],listing['shop_id'],listing['marketplace_id'],listing['asin']))
    return [row['competitor_asin'] for row in cur.fetchall()]


def project(listing,asins):
    """生成当前来源和有序关系的版本，阻止过期弹框覆盖他人的最新修改。"""
    content=[listing['id'],listing['shop_id'],listing['marketplace_id'],listing['asin'],asins]
    version=hashlib.sha256(json.dumps(content,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
    return {'listing_id':str(listing['id']),'shop_id':listing['shop_id'],'sku':listing['sku'],'asin':listing['asin'],
            'marketplace_id':listing['marketplace_id'],'asins':asins,'version':version,'max_competitors':MAX_COMPETITORS}


def read(shop_id,sku):
    """读取竞品编辑快照，不新增关系、采集配置或外部请求。"""
    with session() as cur:
        listing=owner(cur,shop_id,sku)
        return project(listing,bindings(cur,listing))


def save(shop_id,sku,data,actor,can_collect=False):
    """锁定来源并检查版本，关系和勾选的采集配置同事务保存，任一失败则全部回滚。"""
    asins=parse_asins(data.get('asins'))
    collect=data.get('add_to_sellersprite',False)
    if not isinstance(collect,bool):raise Error('采集选项须为布尔值')
    if collect and not can_collect:raise Error('没有卖家精灵采集配置权限，未保存任何修改',403)
    with session() as cur:
        listing=owner(cur,shop_id,sku,lock=True);current=project(listing,bindings(cur,listing))
        if data.get('version')!=current['version']:raise Error('竞品或主ASIN已变更，请重新打开弹框后保存',409)
        if listing['asin'] in asins:raise Error('不能将当前商品ASIN绑定为自己的竞品')
        if collect and listing['marketplace_id']!=portal.US:raise Error('卖家精灵采集目前仅支持美国站',422)
        cur.execute('DELETE FROM amazon_listing_competitors WHERE listing_id=%s',(listing['id'],))
        if asins:
            cur.executemany('''INSERT INTO amazon_listing_competitors
                (listing_id,shop_id,marketplace_id,source_asin,competitor_asin,sort_order,updated_by,updated_by_name,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))''',
                [(listing['id'],listing['shop_id'],listing['marketplace_id'],listing['asin'],asin,index,actor['id'],(actor.get('nickname') or actor.get('username') or '')[:255]) for index,asin in enumerate(asins)])
        collection=portal.add_config_in_session(cur,{'asins':asins,'marketplace':'US','enabled':1},actor) if collect and asins else None
        result=project(listing,asins);result['collection']=collection
        return result


def attach(cur,rows):
    """一次批量补齐当前页竞品；缺关系表时保留原Listing列表，编辑接口另行明确报错。"""
    for row in rows:row['competitor_asins']=[];row['competitors_available']=True
    if not rows:return
    try:
        cur.execute('''SELECT listing_id,shop_id,marketplace_id,source_asin,competitor_asin FROM amazon_listing_competitors
            WHERE listing_id IN ('''+','.join(['%s']*len(rows))+') ORDER BY sort_order,id',tuple(row['id'] for row in rows))
        items=cur.fetchall()
    except (pymysql.err.ProgrammingError,pymysql.err.OperationalError) as exc:
        if exc.args[0] not in (1146,1054):raise
        for row in rows:row['competitors_available']=False
        return
    by_id={row['id']:row for row in rows}
    for item in items:
        target=by_id[item['listing_id']]
        if (item['source_asin']==str(target.get('asin') or '').strip().upper()
            and item['shop_id']==target['shop_id'] and item['marketplace_id']==target['marketplace_id']):
            target['competitor_asins'].append(item['competitor_asin'])
