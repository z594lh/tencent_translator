"""关键词反查只读查询：工具页ASIN选项、关键词筛选和已导入快照，不发起采集。"""
import json
import re
from decimal import Decimal, InvalidOperation
from services import sellersprite_portal as portal

NUMERIC_FIELDS = portal.INTEGER_FIELDS | portal.DECIMAL_FIELDS | {'suggested_bid_min', 'suggested_bid_max', 'word_count'}


def json_parameter(params, key, default):
    raw=params.get(key)
    if raw in (None, ''):
        return default
    if not isinstance(raw,str) or len(raw)>12000:
        raise portal.PortalError(key+'参数过长或无效')
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        raise portal.PortalError(key+'格式无效') from None


def conditions(params):
    clauses, args = [], []
    ranges=json_parameter(params,'ranges',{})
    if not isinstance(ranges,dict) or set(ranges)-NUMERIC_FIELDS:
        raise portal.PortalError('指标筛选字段无效')
    for field,bounds in ranges.items():
        if not isinstance(bounds,dict) or set(bounds)-{'min','max'}:
            raise portal.PortalError('指标区间格式无效')
        values={}
        for bound,value in bounds.items():
            if value in ('',None):
                continue
            if isinstance(value,bool) or not re.fullmatch(r'-?\d{1,18}(?:\.\d{1,8})?',str(value)):
                raise portal.PortalError('指标必须为有限数值')
            try:
                number=Decimal(str(value))
            except InvalidOperation:
                raise portal.PortalError('指标数值无效') from None
            if abs(number)>Decimal('1000000000000') or (number<0 and field not in {'search_growth_mom','search_growth_yoy'}):
                raise portal.PortalError('指标区间超出允许范围')
            if (field in portal.INTEGER_FIELDS or field=='word_count') and number!=number.to_integral_value():
                raise portal.PortalError('排名及数量须为整数')
            # 页面按百分数填写，数据库保留原始比率小数。
            values[bound]=number/100 if field in portal.RATIOS else number
        if 'min' in values and 'max' in values and values['min']>values['max']:
            raise portal.PortalError('指标最小值不能大于最大值')
        for bound,value in values.items():
            expression=portal.WORD_COUNT_SQL if field=='word_count' else field
            clauses.append(expression+(' >= %s' if bound=='min' else ' <= %s'));args.append(value)
    for field in ('keyword_type','traffic_type','conversion_label'):
        values=json_parameter(params,field,[])
        if not isinstance(values,list) or len(values)>20 or any(not isinstance(v,str) or not v or len(v)>100 for v in values):
            raise portal.PortalError('标签筛选格式无效')
        if values:
            operator='LOCATE(%s,COALESCE(traffic_type,\'\'))>0' if field=='traffic_type' else field+'=%s'
            clauses.append('('+' OR '.join([operator]*len(values))+')');args+=values
    mode=params.get('match_mode','all')
    if mode not in ('all','any'):
        raise portal.PortalError('关键词匹配方式无效')
    include_match=params.get('include_match')
    if include_match is not None and include_match not in ('broad','phrase'):
        raise portal.PortalError('包含关键词匹配方式无效')
    for key in ('include','exclude'):
        raw=params.get(key,'')
        if not isinstance(raw,str) or len(raw)>500:
            raise portal.PortalError('包含/排除关键词最多500字')
        words=list(dict.fromkeys(word.strip() for word in re.split(r'[,，;；\n]+',raw) if word.strip()))
        if len(words)>20:
            raise portal.PortalError('包含/排除关键词每组最多20个')
        if words:
            # 新页面按来源规则：逗号各项任一命中，广泛匹配允许词序变化。
            # 仅匹配原关键词；旧请求保留下方含译文及all/any的兼容逻辑。
            if include_match is not None:
                parts=[]
                for word in words:
                    terms=list(dict.fromkeys(word.split())) if key=='include' and include_match=='broad' else [word]
                    parts.append('('+' AND '.join(["LOCATE(%s,COALESCE(keyword,''))>0"]*len(terms))+')')
                    args.extend(terms)
                clauses.append(('NOT ' if key=='exclude' else '')+'('+' OR '.join(parts)+')')
                continue
            parts=['(LOCATE(%s,COALESCE(keyword,\'\'))>0 OR LOCATE(%s,COALESCE(translation,\'\'))>0)']*len(words)
            expression=(' OR ' if key=='exclude' or mode=='any' else ' AND ').join(parts)
            clauses.append(('NOT ' if key=='exclude' else '')+'('+expression+')')
            for word in words:
                args.extend([word,word])
    return ' AND '.join(clauses),args


def reverse_data(asin, params):
    asin=portal.asin_value(asin)
    condition=conditions(params)
    with portal.session() as cur:
        result=portal.keyword_data(cur,asin,params,condition=condition,facets=True)
        identities=[{'asin':asin}]
        portal.config_identities(cur,identities)
        result['product']=identities[0]
        return result


def asin_options(params):
    page=portal.integer(params.get('page',1),'页码')
    search=str(params.get('search') or '').strip()
    if len(search)>100:
        raise portal.PortalError('ASIN搜索最多100字')
    shop=params.get('shop_id')
    shop=portal.integer(shop,'店铺编号') if shop not in (None,'') else None
    where='''l.is_deleted=0 AND l.marketplace_id=%s AND s.status=1
        AND BINARY l.marketplace_id=BINARY s.marketplace_id
        AND TRIM(l.asin) REGEXP '^[A-Za-z0-9]{10}$' '''
    args=[portal.US]
    if shop:
        where+=' AND l.shop_id=%s';args.append(shop)
    if search:
        where+=''' AND (l.asin LIKE %s OR l.sku LIKE %s OR l.item_name LIKE %s OR EXISTS
            (SELECT 1 FROM products p WHERE BINARY p.seller_sku=BINARY l.sku AND p.product_name LIKE %s))'''
        args+=['%'+search+'%']*4
    with portal.session() as cur:
        cur.execute('SELECT id,shop_name FROM amazon_shops WHERE status=1 AND marketplace_id=%s ORDER BY id',(portal.US,))
        shops=cur.fetchall()
        cur.execute('''SELECT DISTINCT UPPER(TRIM(l.asin)) AS asin FROM amazon_listings l
            JOIN amazon_shops s ON s.id=l.shop_id WHERE '''+where+' ORDER BY asin LIMIT 51 OFFSET %s',args+[(page-1)*50])
        rows=list(cur.fetchall());has_more=len(rows)>50;rows=rows[:50]
        portal.config_identities(cur,rows)
        # 筛选店铺时，只返回该店铺的SKU说明，ASIN快照本身仍按站点共享。
        if shop:
            for row in rows:
                row['related_products']=[item for item in row['related_products'] if item.get('shop_id')==shop]
                row['product_name']=next((item['product_name'] for item in row['related_products'] if item.get('product_name')), '')
        return {'shops':shops,'list':rows,'page':page,'has_more':has_more}
