"""人员绩效只读查询：SKU日报金额按当天历史负责人汇总。

read_range固定读取原sku_profit，summarize只求和原金额，不重算利润/退款。
query提供人员汇总和趋势，breakdown_service复用读取/汇总处理SKU与每日排序分页。
没有生成、任务、版本发布或财务重算；REPEATABLE READ只读，权限由HTTP层限定。
"""
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from services.mysql_service import get_db_connection
from services.listing_performance import ownership_repository as ownership
from services.listing_performance import order_counts
from services.listing_performance.common import (
    DomainError, positive_id, parse_date, market_config, day_bounds, utc_now, identity, validate_shop,
)

AMOUNTS = ('sales_amount','product_cost','headway_cost','platform_fees','fba_fees',
           'other_fees','ad_cost','ad_sales','refund_loss','sku_report_profit')


def amount(value):
    """将value转换为有限Decimal，None沿用兼容规则返回0。

    非法/NaN/无穷金额报422；读取日报时另行校验必需金额，避免缺值被当作0。"""
    try:
        result = Decimal(str(value if value is not None else 0))
        if not result.is_finite():
            raise ValueError()
        return result
    except (ValueError, InvalidOperation):
        raise DomainError('来源金额不是有效数字', 422) from None


def range_input(params):
    """解析params的店铺/站点/含首尾日期范围。

    返回(shop,market,config,first,last)；范围1~93天，非法日期/倒置报业务异常。
    这里只校验参数/站点配置，店铺是否启用由validate_shop验证。"""
    shop = positive_id(params.get('shop_id'), 'shop_id')
    market = params.get('marketplace_id')
    config = market_config(market)
    start, end = parse_date(params.get('date_from')), parse_date(params.get('date_to'))
    if end < start or (end-start).days > 92:
        raise DomainError('查询范围必须为 1~93 天')
    return shop, market, config, start, end


def validate_basis(params):
    """校验旧客户端传入的计算模式，只返回sku_report。

    缺省即SKU日报，旧orders请求直接拒绝，防止旧前端把当前查询当成订单重算。"""
    basis = params.get('calculation_basis', 'sku_report')
    if basis != 'sku_report':
        raise DomainError('仅支持SKU报表利润，订单重新计算模式已停用')
    return basis


def read_range(cursor, shop, market, first, last, config):
    """只读sku_profit日报，按当日历史负责人组织可汇总记录。

    返回(记录列表,有日报日期集合,来源表名)；金额逐字段沿用，不再计算退款或利润。
    一次读取范围并缓存SKU负责历史，缺日期不造0；不查询/写入绩效任务和事实表。
    """
    if config.get('currency') != 'USD' or config.get('report_currency','USD') != 'USD':
        raise DomainError('SKU报表统计目前仅支持已核实的USD站点',422)
    cursor.execute('SELECT region FROM amazon_shops WHERE id=%s',(shop,))
    if (cursor.fetchone() or {}).get('region') != 'na':
        raise DomainError('SKU报表站点尚未核实',422)
    source = 'sku_profit'
    cursor.execute('SELECT * FROM '+source+' WHERE shop_id=%s AND report_date BETWEEN %s AND %s '
                   'ORDER BY report_date,id LIMIT 100001',(shop,first,last))
    rows = cursor.fetchall()
    if len(rows)>100000:
        raise DomainError('SKU日报超过查询上限，请缩小日期范围',422)
    facts,days,seen,cache = [],set(),set(),{}
    for row in rows:
        sku,day = row['sku'],row['report_date']
        if not sku or (day,sku) in seen:
            raise DomainError('SKU日报存在空SKU或同日重复SKU，请核查来源',422)
        seen.add((day,sku));days.add(day)
        if sku not in cache:
            scope = ownership.scope(cursor,{'identity_key':identity(shop,market,sku)})
            cache[sku] = (scope,ownership.histories(cursor,scope['id']) if scope else [])
        scope,histories = cache[sku]
        start,_ = day_bounds(day,config['timezone'])
        history = ownership.active_at(histories,start)
        fields = ('sales_amount','product_cost','headway_cost','platform_fees','fba_fees',
                  'other_fees','ad_cost','ad_sales','refund_amount','net_profit')
        if any(row.get(name) is None for name in fields):
            raise DomainError('SKU日报金额缺失，不能当作0统计',422)
        metrics = {name:amount(row[name]) for name in fields if name not in ('refund_amount','net_profit')}
        metrics.update(sales_qty=int(row['sales_qty']),refund_loss=amount(row['refund_amount']),
                       sku_report_profit=amount(row['net_profit']))
        facts.append({'id':row['id'],
            'shop_id':shop,'marketplace_id':market,'seller_sku':sku,'asin':row['asin'],
            'owner_user_id':history['owner_user_id'] if history else None,
            'owner_name':history.get('owner_name') if history else None,
            'assignment_history_id':history['id'] if history else None,
            'business_date':day,'currency':'USD','metrics':metrics,'product_name':row['product_name']})
    return facts,days,source


def filter_rows(rows, owner=None, unassigned=False, sku=None, currency=None):
    """按已鉴权人员/精确SKU/币种筛选内存来源，不扩大用户可见范围。"""
    if currency and (not isinstance(currency,str) or len(currency)!=3 or not currency.isascii() or not currency.isalpha()):
        raise DomainError('currency必须为三位币种代码')
    return [row for row in rows if (owner is None or row['owner_user_id']==owner)
            and (not unassigned or row['owner_user_id'] is None)
            and (sku is None or row['seller_sku']==sku)
            and (not currency or row['currency']==currency.upper())]


def summarize(rows):
    """按人员和币种汇总SKU日报原金额，不计算第二份利润或退款。

    净利润原样求和，ACOS/TACOS用汇总金额相除；销售额为0时比率未知。
    订单数由真实订单只读补充，SKU按店铺/站点/精确SKU去重，不跨币种相加。
    数据范围为实际计入的日期并集及分段，不用筛选起止日期代替真实覆盖。
    """
    groups = {}
    for source in rows:
        key = (source['owner_user_id'],source['currency'])
        row = groups.setdefault(key,{'owner_user_id':key[0],'owner_name':source.get('owner_name'),
            'currency':key[1],'sales_qty':0,'skus':set(),'days':set(), **{name:Decimal(0) for name in AMOUNTS}})
        metrics = source['metrics']
        for name in AMOUNTS:
            row[name] += amount(metrics.get(name))
        row['sales_qty'] += int(metrics.get('sales_qty',0))
        row['skus'].add((source['shop_id'],source['marketplace_id'],source['seller_sku']))
        if source.get('business_date') is not None:
            row['days'].add(source['business_date'])
    for row in groups.values():
        row['selling_listing_count'] = len(row.pop('skus'))
        row.update(data_range(row.pop('days')))
        row['order_count'] = None
        row['profit_margin'] = row['sku_report_profit']/row['sales_amount'] if row['sales_amount']>0 else None
        row['tacos'] = row['ad_cost']/row['sales_amount'] if row['sales_amount']>0 else None
        row['acos'] = row['ad_cost']/row['ad_sales'] if row['ad_sales']>0 else None
    return list(groups.values())


def data_range(days):
    """返回实际计入日期的起止、去重天数及连续分段，不把中间断档当成有业绩。

    同日多个SKU只计一天；这是各SKU已归属且有日报日期的并集，不是统一负责区间。
    空集合返回空日期/0天，不能表示整个筛选区间都为零业绩。
    """
    ordered = sorted(set(days))
    ranges = []
    for day in ordered:
        if ranges and day == ranges[-1]['date_to'] + timedelta(days=1):
            ranges[-1]['date_to'] = day
            ranges[-1]['days'] += 1
        else:
            ranges.append({'date_from':day,'date_to':day,'days':1})
    return {'data_from':ordered[0] if ordered else None,'data_to':ordered[-1] if ordered else None,
            'data_day_count':len(ordered),'data_ranges':ranges}


def owner_options(params, owner=None):
    """只读所选店铺的历史负责人选项；本人权限通过owner强制限定。

    不要求分配权限，不依赖当前Listing是否已删除或人员是否停用，历史业绩仍可查询。
    owner由HTTP鉴权层提供，拒绝前端通过user_id扩大选项；不创建或修改任何历史。
    """
    shop = positive_id(params.get('shop_id'),'shop_id')
    market = params.get('marketplace_id')
    market_config(market)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION TRANSACTION READ ONLY')
            validate_shop(cursor,shop,market)
            condition = ' AND h.owner_user_id=%s' if owner is not None else ''
            cursor.execute('''SELECT DISTINCT h.owner_user_id AS id,
                COALESCE(NULLIF(u.nickname,''),u.username,CONCAT('用户 ',h.owner_user_id)) AS name,
                COALESCE(u.account_type,'human') account_type
                FROM listing_responsibility_history h
                JOIN listing_responsibility_scopes s ON s.id=h.scope_id
                LEFT JOIN users u ON u.id=h.owner_user_id
                WHERE s.shop_id=%s AND s.marketplace_id=%s AND h.voided_at IS NULL
                AND h.owner_user_id IS NOT NULL'''+condition+' ORDER BY name,id',
                           (shop,market,*([owner] if owner is not None else [])))
            return {'list':cursor.fetchall()}
    finally:
        conn.close()


def query(params, owner=None, show_costs=False):
    """直接汇总sku_profit，查询即统计，不读取绩效快照或重算利润。

    金额沿用SKU日报，负责人按当天历史区间，订单数只读补充相同有来源日期。
    使用REPEATABLE READ只读事务；权限范围由路由校验，成本字段按show_costs裁剪。
    返回人员汇总和逐日趋势；SKU/日分页见breakdown_service，缺日报不造0。
    全员查询也排除未归属行；只在查询期外负责的人员不补入，无日报的期间负责人保留空值。
    """
    basis = validate_basis(params)
    shop,market,config,first,last = range_input(params)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('SET SESSION TRANSACTION READ ONLY')
            validate_shop(cursor,shop,market)
            all_rows,days,source = read_range(cursor,shop,market,first,last,config)
            # 全员也只统计已归属行；不得将未分配日期补给当前负责人。
            rows = [r for r in filter_rows(all_rows,owner=owner,
                                          currency=params.get('currency')) if r['owner_user_id'] is not None]
            metadata = {'date_from':first,'date_to':last,'timezone':config['timezone'],
                        'data_days':len(days),'expected_days':(last-first).days+1,
                        'counted_days':len({r['business_date'] for r in rows}),
                        'source_table':source,'refund_policy':'as_sku_report'}
            if not days:
                return {**metadata,'list':[],'trend':[],'total':0}
            summary = summarize(rows)
            begin,_ = day_bounds(first,config['timezone'])
            _,finish = day_bounds(last,config['timezone'])
            now = utc_now()
            scope_condition = ' AND h.owner_user_id=%s' if owner is not None else ''
            cursor.execute('''SELECT h.owner_user_id,s.id AS scope_id,h.effective_from_utc,h.effective_to_utc,
                COALESCE(NULLIF(u.nickname,''),u.username) AS owner_name,COALESCE(u.account_type,'human') account_type,
                EXISTS(SELECT 1 FROM amazon_listings l WHERE l.shop_id=s.shop_id
                    AND l.marketplace_id COLLATE utf8mb4_unicode_ci=s.marketplace_id COLLATE utf8mb4_unicode_ci
                    AND BINARY l.sku=BINARY s.seller_sku AND l.is_deleted=0
                    AND COALESCE(LOWER(l.parentage_level),'')<>'parent') AS listing_active
                FROM listing_responsibility_history h JOIN listing_responsibility_scopes s ON s.id=h.scope_id
                LEFT JOIN users u ON u.id=h.owner_user_id
                WHERE s.shop_id=%s AND s.marketplace_id=%s AND h.voided_at IS NULL AND h.owner_user_id IS NOT NULL'''+scope_condition,
                           (shop,market,*([owner] if owner is not None else [])))
            counts = {}
            for h in cursor.fetchall():
                count = counts.setdefault(h['owner_user_id'],{'current':set(),'period':set(),'owner_name':h['owner_name'],'account_type':h['account_type']})
                if h['listing_active'] and h['effective_from_utc']<=now and (h['effective_to_utc'] is None or now<h['effective_to_utc']):
                    count['current'].add(h['scope_id'])
                if h['effective_from_utc']<finish and (h['effective_to_utc'] is None or h['effective_to_utc']>begin):
                    count['period'].add(h['scope_id'])
            existing_owners={r['owner_user_id'] for r in summary}
            for user_id,count in counts.items():
                if not count['period']:
                    continue
                if user_id not in existing_owners:
                    summary.append({'owner_user_id':user_id,'owner_name':count['owner_name'],'currency':config['currency'],
                                    'profit_margin':None,'tacos':None,'acos':None,
                                    'sales_qty':None,'order_count':None,'selling_listing_count':0,
                                    **data_range([]),
                                    **{name:None for name in AMOUNTS}})
            for row in summary:
                count=counts.get(row['owner_user_id'],{})
                row['account_type']=count.get('account_type','human')
                row['current_listing_count']=len(count.get('current',[]))
                row['period_listing_count']=len(count.get('period',[]))
            order_index = order_counts.read(cursor,shop,market,first,last,config,basis,
                                           allowed_days={r['business_date'] for r in rows},
                                           allowed_sku_days={(r['seller_sku'],r['business_date']) for r in rows})
            if order_index is not None:
                metadata['order_count_note'] = '按当前本地已发货订单及下单日期去重；同单跨 SKU/人员可能重叠，不直接相加'
                metadata['order_count_missing_orders'] = order_index['missing_orders']
                for row in summary:
                    if row['data_day_count']:
                        row['order_count'] = order_counts.count(order_index,'owner',(row['owner_user_id'],row['currency']))
            trend=[]
            for day in sorted({r['business_date'] for r in rows}):
                for entry in summarize([r for r in rows if r['business_date']==day]):
                    if order_index is not None:
                        entry['order_count'] = order_counts.count(order_index,'trend',(entry['owner_user_id'],entry['currency'],day))
                    trend.append({'business_date':day,**entry})
            if not show_costs:
                for row in summary+trend:
                    for key in ('product_cost','headway_cost','platform_fees','fba_fees','other_fees'):
                        row.pop(key,None)
            return {**metadata,'list':summary,'trend':trend,'total':len(summary)}
    finally:
        conn.close()
