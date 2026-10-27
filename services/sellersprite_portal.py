"""卖家精灵Web配置和清洗快照只读查询；不调用机器接口或外部服务。"""
import json
import re
from contextlib import contextmanager
from datetime import date, datetime, timezone
import pymysql
from services.mysql_service import get_db_connection
from services.sellersprite_excel import ALIASES, INTEGER_FIELDS, DECIMAL_FIELDS, TEXT_FIELDS

US = 'ATVPDKIKX0DER'
# MySQL 5.7无REGEXP_REPLACE；关键词最长500字符，九次折叠足以合并连续空格。
WORD_TEXT_SQL = "COALESCE(keyword,'')"
for _ in ('CHAR(9)', 'CHAR(10)', 'CHAR(13)'):
    WORD_TEXT_SQL = "REPLACE("+WORD_TEXT_SQL+","+_+",' ')"
for _ in range(9):
    WORD_TEXT_SQL = "REPLACE("+WORD_TEXT_SQL+",'  ',' ')"
WORD_TEXT_SQL = 'TRIM('+WORD_TEXT_SQL+')'
WORD_COUNT_SQL = "(CASE WHEN "+WORD_TEXT_SQL+"='' THEN 0 ELSE 1+CHAR_LENGTH("+WORD_TEXT_SQL+")-CHAR_LENGTH(REPLACE("+WORD_TEXT_SQL+",' ','')) END)"
ROW_FIELDS = sorted(INTEGER_FIELDS | DECIMAL_FIELDS | TEXT_FIELDS | {'keyword','suggested_bid_min','suggested_bid_max','word_count'})
SORT_FIELDS = set(INTEGER_FIELDS | DECIMAL_FIELDS | {'keyword','source_row','suggested_bid_min','suggested_bid_max','word_count'})
RATIOS = {'traffic_share','organic_share','sponsored_share','search_growth_mom','search_growth_yoy','purchase_rate','total_click_share','total_conversion_share'}
EXTRA_LABELS={'organic_updated_text':'自然排名更新时间（来源原文）','sponsored_updated_text':'广告排名更新时间（来源原文）','suggested_bid_min':'建议竞价下限','suggested_bid_max':'建议竞价上限','word_count':'单词个数'}


def columns():
    return [{'key':key,'label':EXTRA_LABELS.get(key) or ALIASES.get(key,(key,))[0],
             'type':'ratio' if key in RATIOS else 'money' if key in ('ppc','suggested_bid_min','suggested_bid_max') else 'number' if key in INTEGER_FIELDS or key in ('demand_supply_ratio','word_count') else 'text',
             'sortable':key in SORT_FIELDS} for key in ROW_FIELDS]


class PortalError(ValueError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def integer(value, label, low=1, high=100000):
    if isinstance(value, bool) or not re.fullmatch(r'-?\d{1,12}', str(value)) or not low <= int(value) <= high:
        raise PortalError(label + '无效')
    return int(value)


def asin_value(value):
    value = str(value or '').strip().upper()
    if not re.fullmatch(r'[A-Z0-9]{10}', value): raise PortalError('ASIN须为10位英文字母或数字')
    return value


def iso(value):
    if isinstance(value, datetime): return value.replace(tzinfo=timezone.utc).isoformat()
    return value.isoformat() if isinstance(value,date) else value


@contextmanager
def session(write=False):
    conn = get_db_connection()
    try:
        if write: conn.begin()
        with conn.cursor() as cur: yield cur
        if write: conn.commit()
    except pymysql.err.ProgrammingError as exc:
        conn.rollback()
        if exc.args[0] == 1146: raise PortalError('卖家精灵数据表尚未初始化，请先执行上线SQL',503) from None
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def configured_candidates():
    """配置是唯一资格来源；Listing只补店铺/SKU，不再筛库存和销量。"""
    with session() as cur:
        cur.execute("SELECT asin FROM sellersprite_asin_config WHERE marketplace='US' AND enabled=1 ORDER BY priority DESC,id LIMIT 10001")
        items = cur.fetchall()
        if len(items)>10000: raise PortalError('启用ASIN超过10000，请缩小配置范围',422)
        result = {row['asin']: [] for row in items}
        if not result: return result
        cur.execute('''SELECT DISTINCT UPPER(TRIM(l.asin)) AS asin,l.shop_id,l.sku,l.marketplace_id
            FROM amazon_listings l JOIN amazon_shops s ON s.id=l.shop_id AND s.status=1
            WHERE l.is_deleted=0 AND l.marketplace_id=%s AND s.marketplace_id=%s
            AND UPPER(TRIM(l.asin)) IN (''' + ','.join(['%s']*len(result)) + ') ORDER BY l.shop_id,l.sku',
            (US,US,*result))
        for row in cur.fetchall():
            result[row['asin']].append({key:row[key] for key in ('shop_id','sku','marketplace_id')})
        return result


def config_identities(cur, rows):
    """按当前页批量补商品身份，保留同一ASIN的所有店铺和SKU。"""
    if not rows:
        return
    asins = [row['asin'] for row in rows]
    placeholders = ','.join(['%s'] * len(asins))
    related = {asin: [] for asin in asins}
    cur.execute('''SELECT UPPER(TRIM(l.asin)) AS asin,l.shop_id,l.sku,l.item_name,
        l.main_image_url AS image_url,s.shop_name,s.status AS shop_enabled,
        (SELECT p.product_name FROM products p WHERE BINARY p.seller_sku=BINARY l.sku ORDER BY p.id LIMIT 1) AS product_name
        FROM amazon_listings l JOIN amazon_shops s ON s.id=l.shop_id
        AND BINARY s.marketplace_id=BINARY l.marketplace_id
        WHERE l.is_deleted=0 AND l.marketplace_id=%s AND UPPER(TRIM(l.asin)) IN ('''+placeholders+''')
        ORDER BY l.shop_id,l.sku,l.id''', (US, *asins))
    for item in cur.fetchall():
        items = related[item.pop('asin')]
        if not any(existing['shop_id'] == item['shop_id'] and existing['sku'] == item['sku'] for existing in items):
            items.append(item)
    # 商品资料中的ASIN不等于已关联店铺，不擅自推定店铺归属。
    cur.execute('''SELECT UPPER(TRIM(asin)) AS asin,seller_sku AS sku,product_name,image_url
        FROM products WHERE UPPER(TRIM(asin)) IN ('''+placeholders+') ORDER BY id', asins)
    catalog = {asin: [] for asin in asins}
    for item in cur.fetchall():
        catalog[item.pop('asin')].append(item)
    for row in rows:
        items = related[row['asin']]
        row['ownership'] = 'owned' if items else 'catalog' if catalog[row['asin']] else 'external'
        row['related_products'] = items or catalog[row['asin']]
        primary = next((item for item in row['related_products'] if item.get('product_name')), {})
        row['product_name'] = primary.get('product_name') or ''
        row['item_name'] = primary.get('item_name') or next((item.get('item_name') for item in items if item.get('item_name')), '')
        row['image_url'] = next((item['image_url'] for item in row['related_products'] if item.get('image_url')), '')


def config_list(params):
    page, size = integer(params.get('page',1),'页码'), integer(params.get('page_size',20),'每页数量',1,100)
    search = str(params.get('search') or '').strip()
    if len(search)>100: raise PortalError('搜索词过长')
    where, args = "c.marketplace='US'", []
    # 与批量身份补充采用相同美国站/未删除Listing口径，店铺停用不改变商品归属。
    listing_match = '''FROM amazon_listings l JOIN amazon_shops s ON s.id=l.shop_id
        AND BINARY s.marketplace_id=BINARY l.marketplace_id
        WHERE l.is_deleted=0 AND l.marketplace_id='ATVPDKIKX0DER'
        AND BINARY UPPER(TRIM(l.asin))=BINARY c.asin'''
    catalog_match = 'FROM products p WHERE BINARY UPPER(TRIM(p.asin))=BINARY c.asin'
    ownership = params.get('ownership') or ''
    if ownership not in ('', 'owned', 'catalog', 'external'):
        raise PortalError('商品归属无效')
    if ownership == 'owned':
        where += ' AND EXISTS (SELECT 1 '+listing_match+')'
    elif ownership in ('catalog', 'external'):
        where += ' AND NOT EXISTS (SELECT 1 '+listing_match+')'
        where += (' AND EXISTS' if ownership == 'catalog' else ' AND NOT EXISTS')+' (SELECT 1 '+catalog_match+')'
    if search:
        where += ''' AND (CONVERT(c.asin USING utf8mb4) LIKE %s OR c.remark LIKE %s OR EXISTS (SELECT 1 '''+listing_match+'''
            AND (l.sku LIKE %s OR l.item_name LIKE %s OR s.shop_name LIKE %s OR EXISTS
            (SELECT 1 FROM products p WHERE BINARY p.seller_sku=BINARY l.sku AND p.product_name LIKE %s)))
            OR EXISTS (SELECT 1 '''+catalog_match+' AND (p.seller_sku LIKE %s OR p.product_name LIKE %s)))'
        args += ['%'+search+'%']*8
    enabled = params.get('enabled')
    if enabled not in (None,''):
        where += ' AND c.enabled=%s'; args.append(integer(enabled,'启用状态',0,1))
    with session() as cur:
        cur.execute('SELECT COUNT(*) AS n FROM sellersprite_asin_config c WHERE '+where,args)
        total=cur.fetchone()['n']
        cur.execute('''SELECT c.*,(SELECT t.id FROM sellersprite_keyword_tasks t
            WHERE t.asin=c.asin AND t.marketplace=c.marketplace ORDER BY t.record_date DESC,t.id DESC LIMIT 1) AS latest_task_id
            FROM sellersprite_asin_config c WHERE '''+where+' ORDER BY c.priority DESC,c.id DESC LIMIT %s OFFSET %s',args+[size,(page-1)*size])
        rows = [{key:iso(value) for key,value in row.items()} for row in cur.fetchall()]
        from services.sellersprite_recovery import diagnose, safe_message
        for row in rows:
            row['latest_status'] = None
            if row['latest_task_id']:
                cur.execute('SELECT status,record_date,error_message FROM sellersprite_keyword_tasks WHERE id=%s', (row['latest_task_id'],))
                task = cur.fetchone()
                row.update(latest_status=task['status'], latest_date=iso(task['record_date']),
                           latest_error=safe_message(task['error_message']), diagnostic=diagnose(task['error_message'], task['status']))
        config_identities(cur, rows)
        return {'list':rows, 'total':total,'page':page,'page_size':size}


def fields(data):
    if data.get('marketplace','US')!='US': raise PortalError('当前仅支持US站点',422)
    enabled=integer(data.get('enabled',1),'启用状态',0,1)
    priority=integer(data.get('priority',0),'优先级',0,9999)
    remark=data.get('remark','')
    if not isinstance(remark,str) or len(remark)>500: raise PortalError('备注最多500字')
    return enabled,priority,remark.strip()


def add_config(data, actor):
    """沿用批量添加入口并独立提交；重复ASIN保持已有启用、优先级和备注。"""
    with session(write=True) as cur:
        return add_config_in_session(cur, data, actor)


def add_config_in_session(cur, data, actor):
    """在调用方事务中复用原批量添加规则，供竞品绑定与配置原子保存。"""
    raw=data.get('asins')
    if isinstance(raw,str): raw=re.split(r'[\s,，;；]+',raw.strip())
    if not isinstance(raw,list) or not 1<=len(raw)<=200: raise PortalError('每次添加1～200个ASIN，每行一个')
    asins=list(dict.fromkeys(asin_value(value) for value in raw))
    enabled,priority,remark=fields(data)
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    created=0
    for asin in asins:
        cur.execute('''INSERT INTO sellersprite_asin_config
            (asin,marketplace,enabled,priority,remark,updated_by,updated_by_name,created_at,updated_at)
            VALUES (%s,'US',%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE id=id''',
            (asin,enabled,priority,remark,actor['id'],(actor.get('nickname') or actor['username'])[:255],now,now))
        created+=1 if cur.rowcount==1 else 0
    return {'created':created,'existing':len(asins)-created}


def update_config(identifier, data, actor):
    identifier=integer(identifier,'配置编号',1,922337203685)
    version=integer(data.get('version'),'配置版本')
    enabled,priority,remark=fields(data)
    now=datetime.now(timezone.utc).replace(tzinfo=None)
    with session(write=True) as cur:
        cur.execute('''UPDATE sellersprite_asin_config SET enabled=%s,priority=%s,remark=%s,
            updated_by=%s,updated_by_name=%s,updated_at=%s,version=version+1 WHERE id=%s AND version=%s''',
            (enabled,priority,remark,actor['id'],(actor.get('nickname') or actor['username'])[:255],now,identifier,version))
        if cur.rowcount!=1: raise PortalError('配置不存在或已被其他人修改，请刷新后重新编辑',409)
    return {'id':str(identifier),'version':version+1}


def listing_context(cur, shop, sku):
    shop=integer(shop,'店铺编号')
    if not sku or len(sku)>255: raise PortalError('SKU无效')
    cur.execute('''SELECT l.asin,l.marketplace_id FROM amazon_listings l JOIN amazon_shops s ON s.id=l.shop_id
        WHERE l.shop_id=%s AND BINARY l.sku=BINARY %s AND l.is_deleted=0
        AND BINARY l.marketplace_id=BINARY s.marketplace_id LIMIT 2''',(shop,sku))
    found=cur.fetchall()
    if not found: raise PortalError('当前店铺不存在该Listing',404)
    if len(found)!=1: raise PortalError('Listing站点不唯一，请先核对',409)
    if found[0]['marketplace_id']!=US: raise PortalError('卖家精灵采集目前仅支持美国站',422)
    return asin_value(found[0]['asin'])


def listing_data(shop, sku, params):
    with session() as cur:
        return keyword_data(cur, listing_context(cur, shop, sku), params)


def keyword_data(cur, asin, params, condition=None, facets=False):
    """共享的成功快照查询；调用者负责校验Listing或ASIN上下文。"""
    page,size=integer(params.get('page',1),'页码'),integer(params.get('page_size',20),'每页数量',1,100)
    search=str(params.get('search') or '').strip()
    if len(search)>200: raise PortalError('关键词搜索最多200字')
    sort=params.get('sort','traffic_share'); direction=params.get('direction','desc')
    if sort not in SORT_FIELDS or direction not in ('asc','desc'): raise PortalError('排序字段或方向无效')
    task_id=params.get('task_id')
    if task_id not in (None,''): task_id=integer(task_id,'快照编号',1,922337203685)
    cur.execute('''SELECT t.id,t.record_date,t.row_count,t.completed_at,t.latest_file_id
            FROM sellersprite_keyword_tasks t JOIN sellersprite_keyword_files f ON f.id=t.latest_file_id AND f.task_id=t.id AND f.status='imported'
            WHERE t.asin=%s AND t.marketplace='US' AND t.period='last30days' AND t.status='completed'
            ORDER BY t.record_date DESC,t.id DESC LIMIT 100''',(asin,))
    snapshots=[{key:(str(value) if key=='id' else iso(value)) for key,value in row.items() if key!='latest_file_id'} for row in cur.fetchall()]
    cur.execute("SELECT id,status,record_date,error_message FROM sellersprite_keyword_tasks WHERE asin=%s AND marketplace='US' ORDER BY record_date DESC,id DESC LIMIT 1",(asin,))
    latest=cur.fetchone()
    if latest:
        from services.sellersprite_recovery import diagnose, safe_message
        latest={key:iso(value) for key,value in latest.items()}
        latest['error_message'] = safe_message(latest['error_message'])
        latest['diagnostic'] = diagnose(latest['error_message'], latest['status'])
    result={'asin':asin,'marketplace':'US','period':'last30days','snapshots':snapshots,'latest_attempt':latest,'snapshot':None,'list':[],'total':0,'page':page,'page_size':size,'columns':columns()}
    selected=task_id or (snapshots[0]['id'] if snapshots else None)
    if facets:
        result.update(source_total=0, facet_options={}, word_frequency=[])
    if selected is None: return result
    cur.execute('''SELECT t.id,t.record_date,t.row_count,t.completed_at,t.latest_file_id,f.metadata_json
            FROM sellersprite_keyword_tasks t JOIN sellersprite_keyword_files f ON f.id=t.latest_file_id AND f.task_id=t.id AND f.status='imported'
            WHERE t.id=%s AND t.asin=%s AND t.marketplace='US' AND t.period='last30days' AND t.status='completed' ''',(selected,asin))
    snapshot=cur.fetchone()
    if not snapshot: raise PortalError('所选快照不属于当前Listing或尚未成功入库',404)
    file_id=snapshot.pop('latest_file_id')
    try: metadata=json.loads(snapshot.pop('metadata_json') or '{}')
    except (ValueError,TypeError): metadata={}
    result['word_frequency']=(metadata.get('word_frequency') or [])[:100]
    result['snapshot']={key:(str(value) if key=='id' else iso(value)) for key,value in snapshot.items()}
    if facets:
        cur.execute('''SELECT keyword_type,traffic_type,conversion_label,COUNT(*) AS n
            FROM sellersprite_keyword_rows WHERE file_id=%s GROUP BY keyword_type,traffic_type,conversion_label''',(file_id,))
        groups=cur.fetchall()
        result['source_total']=sum(row['n'] for row in groups)
        result['facet_options']={key:sorted({part.strip() for row in groups for part in
            (str(row[key]).split('/') if key=='traffic_type' else [str(row[key])]) if row[key] and part.strip()})
            for key in ('keyword_type','traffic_type','conversion_label')}
    where,args='file_id=%s',[file_id]
    if condition and condition[0]:
        where+=' AND '+condition[0];args+=condition[1]
    if search:
        where+=' AND (keyword LIKE %s OR translation LIKE %s)';args+=['%'+search+'%']*2
    cur.execute('SELECT COUNT(*) AS n FROM sellersprite_keyword_rows WHERE '+where,args);result['total']=cur.fetchone()['n']
    selected_fields=','.join(WORD_COUNT_SQL+' AS word_count' if field=='word_count' else field for field in ROW_FIELDS)
    cur.execute('SELECT id,source_row,'+selected_fields+',top_asins_json FROM sellersprite_keyword_rows WHERE '+where+
            f' ORDER BY {sort} IS NULL,{sort} {direction},source_row ASC LIMIT %s OFFSET %s',args+[size,(page-1)*size])
    for row in cur.fetchall():
        row['id']=str(row['id'])
        try: row['top_asins']=json.loads(row.pop('top_asins_json') or '[]')
        except (ValueError,TypeError): row['top_asins']=[]
        result['list'].append(row)
    from services.keyword_translation_service import enrich_rows
    enrich_rows(result['list'],cursor=cur)
    return result
