"""竞品工具页只读投影：查询本地商品快照，不触发采集，不返回机器令牌或文件路径。"""
import json
from services import sellersprite_portal as portal

SORT_FIELDS = {'monthly_sales', 'monthly_revenue', 'price', 'bsr', 'ratings', 'rating', 'date_available'}
VALID = """t.marketplace='US' AND t.period='last30days' AND t.status='completed'
    AND f.status='imported' AND f.task_id=t.id AND p.task_id=t.id
    AND p.asin=COALESCE(t.selected_asin,t.asin)"""
JOIN = """FROM sellersprite_competitor_tasks t
    JOIN sellersprite_competitor_files f ON f.id=t.latest_file_id
    JOIN sellersprite_competitor_products p ON p.file_id=f.id"""
NEWER = """NOT EXISTS (SELECT 1 FROM sellersprite_competitor_tasks n
    JOIN sellersprite_competitor_files nf ON nf.id=n.latest_file_id AND nf.task_id=n.id AND nf.status='imported'
    JOIN sellersprite_competitor_products np ON np.file_id=nf.id AND np.task_id=n.id
    WHERE n.marketplace=t.marketplace AND n.asin=t.asin AND n.period=t.period AND n.status='completed'
    AND np.asin=COALESCE(n.selected_asin,n.asin)
    AND (n.record_date>t.record_date OR (n.record_date=t.record_date AND n.id>t.id)))"""


def options(params):
    """分页搜索已配置或已有历史的查询ASIN，成功快照是否存在单独披露。"""
    page=portal.integer(params.get('page',1),'页码')
    size=portal.integer(params.get('page_size',50),'每页数量',1,100)
    search=str(params.get('search') or '').strip().upper()
    if len(search)>30:raise portal.PortalError('ASIN搜索最多30个字符')
    source="""FROM (SELECT asin FROM sellersprite_competitor_config WHERE marketplace='US'
        UNION SELECT asin FROM sellersprite_competitor_tasks WHERE marketplace='US' AND period='last30days') a
        WHERE LOCATE(%s,a.asin)>0"""
    with portal.session() as cur:
        cur.execute('SELECT COUNT(*) AS n '+source,(search,));total=cur.fetchone()['n']
        cur.execute('''SELECT a.asin,EXISTS(SELECT 1 '''+JOIN+' WHERE '+VALID+''' AND t.asin=a.asin) AS has_snapshot
            '''+source+' ORDER BY a.asin LIMIT %s OFFSET %s',(search,size,(page-1)*size))
        return {'list':[{'asin':r['asin'],'has_snapshot':bool(r['has_snapshot'])} for r in cur.fetchall()],
                'total':total,'page':page,'page_size':size}


def json_object(value, warnings, name):
    """坏JSON只降级相应展示，不拿其他快照或当前商品资料补值。"""
    try:
        result=json.loads(value)
        if not isinstance(result,dict):raise ValueError()
        return result
    except (ValueError,TypeError):
        warnings.append(name+'无法读取，相关字段未展示')
        return {}


def project(row):
    """保留原数据与来源编号；拆出详情供展示，不输出私有存储位置。"""
    result={k:portal.iso(v) for k,v in row.items()}
    result['warnings']=[]
    result['details']=json_object(result.pop('details_json',None),result['warnings'],'清洗详情')
    result['raw_fields']=json_object(result.pop('raw_row_json',None),result['warnings'],'原始字段')
    for key in ('id','file_id','task_id'):result[key]=str(result[key])
    return result


def products(params):
    """每查询ASIN只选最近有效快照；指定历史必须属于该ASIN，失败任务不遮住旧成功。"""
    raw=params.get('asin')
    asin=portal.asin_value(raw) if raw else None
    task=params.get('task_id')
    if task and not asin:raise portal.PortalError('查看历史快照必须指定查询ASIN')
    page=portal.integer(params.get('page',1),'页码')
    size=portal.integer(params.get('page_size',10),'每页数量',1,100)
    sort=params.get('sort','monthly_sales');direction=params.get('direction','desc')
    if sort not in SORT_FIELDS or direction not in ('asc','desc'):raise portal.PortalError('排序参数无效')
    where,args=VALID,[]
    if asin:where+=' AND t.asin=%s';args.append(asin)
    if task:where+=' AND t.id=%s';args.append(portal.integer(task,'快照编号',1,999999999999))
    else:where+=' AND '+NEWER
    with portal.session() as cur:
        cur.execute('SELECT COUNT(*) AS n '+JOIN+' WHERE '+where,args);total=cur.fetchone()['n']
        if task and not total:raise portal.PortalError('该ASIN没有对应的成功快照',404)
        cur.execute('''SELECT p.*,t.asin AS query_asin,t.record_date,t.completed_at,t.report_id,t.period,
            f.file_name,f.sha256 '''+JOIN+' WHERE '+where+
            f' ORDER BY p.{sort} IS NULL,p.{sort} {direction},t.asin,t.id DESC LIMIT %s OFFSET %s',args+[size,(page-1)*size])
        rows=[project(r) for r in cur.fetchall()]
        latest=None
        if asin:
            cur.execute("""SELECT id,record_date,status,error_message FROM sellersprite_competitor_tasks
                WHERE marketplace='US' AND period='last30days' AND asin=%s ORDER BY record_date DESC,id DESC LIMIT 1""",(asin,))
            latest=cur.fetchone()
            if latest:latest={k:str(v) if k=='id' else portal.iso(v) for k,v in latest.items()}
        return {'list':rows,'total':total,'page':page,'page_size':size,'latest_attempt':latest,
                'currency':'USD','period':'last30days','asin':asin,'historical':bool(task)}


def history(asin,params):
    """仅投影采集日期/状态与成功标记；普通查看者不取得路径、租约或配置修改能力。"""
    asin=portal.asin_value(asin);page=portal.integer(params.get('page',1),'页码')
    with portal.session() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM sellersprite_competitor_tasks WHERE marketplace='US' AND period='last30days' AND asin=%s",(asin,));total=cur.fetchone()['n']
        cur.execute('''SELECT t.id,t.asin,t.selected_asin,t.record_date,t.status,t.error_message,
            p.monthly_sales,p.price,p.bsr,p.rating,
            (t.status='completed' AND f.status='imported' AND f.task_id=t.id AND p.task_id=t.id
                AND p.asin=COALESCE(t.selected_asin,t.asin)) AS can_view
            FROM sellersprite_competitor_tasks t LEFT JOIN sellersprite_competitor_files f ON f.id=t.latest_file_id
            LEFT JOIN sellersprite_competitor_products p ON p.file_id=f.id
            WHERE t.marketplace='US' AND t.period='last30days' AND t.asin=%s
            ORDER BY t.record_date DESC,t.id DESC LIMIT 20 OFFSET %s''',(asin,(page-1)*20))
        rows=[]
        for row in cur.fetchall():
            item={k:portal.iso(v) for k,v in row.items()};item['id']=str(item['id']);item['can_view']=bool(item['can_view']);rows.append(item)
        return {'list':rows,'total':total,'page':page}
