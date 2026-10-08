"""直接从SKU日报生成可见人员的SKU汇总和每日明细。

绩效/skus、/sku-daily调用本模块，经performance_service.read_range读取sku_profit及历史负责人。
利润只求和日报net_profit，不重算退款；订单数补充相同来源日期的已发货订单去重结果。
白名单全量排序后分页，逐日默认倒序；缺日报/负责但无数据保留None，不伪造零值。
REPEATABLE READ只读事务，不依赖绩效快照，不写任务，不调用外部服务。
"""
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from services.mysql_service import get_db_connection
from services.listing_performance import performance_service as performance
from services.listing_performance import order_counts as order_counts
from services.listing_performance.performance_service import summarize
from services.listing_performance.common import DomainError, day_bounds, positive_id


SORT_FIELDS = frozenset(('seller_sku', 'product_name', 'currency', 'sales_qty', 'ad_share',
                        'sales_amount', 'ad_cost', 'refund_loss', 'profit', 'acos', 'tacos', 'order_count'))
TEXT_FIELDS = frozenset(('seller_sku', 'product_name', 'currency', 'business_date'))


def sorted_rows(rows, field, direction):
    """对白名单字段做稳定全局排序，返回新列表。

    先以日期/SKU/币种/人员建立稳定次序，数值用Decimal，文字用字符串。
    缺值不参与比较，升序/降序都放最后；必须排序后再分页，否则跨页排名错误。"""
    stable = sorted(rows, key=lambda r: (str(r.get('business_date', '')), r['seller_sku'] or '',
                                        r['currency'], r['owner_user_id'] or 0))
    present = [r for r in stable if r.get(field) is not None]
    missing = [r for r in stable if r.get(field) is None]
    key = (lambda r: str(r[field])) if field in TEXT_FIELDS else (lambda r: Decimal(str(r[field])))
    return sorted(present, key=key, reverse=direction == 'desc') + missing


def responsibility_dates(histories, first, last, timezone):
    """将查询期内的负责历史展开为(人员,精确SKU)的业务日期集合。

    与金额归属一致，以业务日零点匹配UTC左闭右开区间；重接同SKU保留分段。
    只处理当前查询期（最多93天），不把结束当天算给原负责人，也不写历史。
    """
    starts = [(first+timedelta(days=i),day_bounds(first+timedelta(days=i),timezone)[0])
              for i in range((last-first).days+1)]
    result = defaultdict(set)
    for history in histories:
        days = result[(history['owner_user_id'],history['seller_sku'])]
        for day, start in starts:
            if history['effective_from_utc'] <= start and (history['effective_to_utc'] is None
                                                         or start < history['effective_to_utc']):
                days.add(day)
    return result


def coverage_fields(owned_days, counted_days):
    """分别返回负责/实际计入日期段及缺日报天数，查询范围不等于负责范围。

    同SKU多次交接取日期并集，缺日报只指负责日期没有该SKU来源记录，不填零。
    """
    covered = performance.data_range(counted_days)
    owned = performance.data_range(owned_days)
    missing = performance.data_range(set(owned_days)-set(counted_days))
    return {**covered,'responsibility_from':owned['data_from'],'responsibility_to':owned['data_to'],
            'responsibility_days':owned['data_day_count'],'responsibility_ranges':owned['data_ranges'],
            'missing_report_days':missing['data_day_count'],'missing_report_ranges':missing['data_ranges']}


def query(params, owner=None, daily=False, show_costs=False):
    """按params和已授权owner返回SKU汇总或单SKU每日分页指标。

    daily=False按负责人/SKU/币种汇总；daily=True必须提供seller_sku，按日拆分。
    owner=None配unassigned才是未分配，是否能查该人员由Blueprint提前校验。
    返回销量/广告占比/销售/广告费/利润/ACOS/TACOS/订单数、覆盖日期及分页排序信息。
    每页最多200；日报超10万、负责SKU超1万或同日重复SKU时报错，避免无界查询。
    返回负责/计入日期段、缺日报天数与分页前全量合计；ACOS/TACOS按总金额相除。
    daily默认只展示负责日期，daily_scope=all显示完整区间及空值原因；趋势显式使用all。
    负责但无来源的SKU保留未知，仅对已知有权SKU补缺日；show_costs=False也裁剪合计成本。
    独立只读连接关闭，无写入、计算任务或自动补源。"""
    basis = performance.validate_basis(params)
    shop,market,config,first,last = performance.range_input(params)
    sku = params.get('seller_sku')
    if daily and (not isinstance(sku,str) or not sku.strip() or len(sku)>255):
        raise DomainError('seller_sku 无效')
    page = positive_id(params.get('page',1),'page')
    size = min(positive_id(params.get('page_size',20),'page_size'),200)
    sort_by = params.get('sort_by', 'business_date' if daily else 'seller_sku')
    sort_order = params.get('sort_order', 'desc' if daily else 'asc')
    if sort_by not in (SORT_FIELDS | {'business_date'} if daily else SORT_FIELDS):
        raise DomainError('sort_by 无效')
    if sort_order not in ('asc', 'desc'):
        raise DomainError('sort_order 必须为 asc 或 desc')
    daily_scope = params.get('daily_scope','owned')
    if daily_scope not in ('owned','all'):
        raise DomainError('daily_scope 必须为 owned 或 all')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cursor.execute('SET SESSION TRANSACTION READ ONLY')
            performance.validate_shop(cursor,shop,market)
            all_rows,data_days,source = performance.read_range(cursor,shop,market,first,last,config)
            metadata = {'date_from':first, 'date_to':last, 'timezone':config['timezone'],
                        'data_days':len(data_days), 'source_table':source,
                        'expected_days':(last-first).days+1, 'order_count_missing_orders':0}
            currency = params.get('currency')
            report_currency = config.get('report_currency',config['currency'])
            rows = performance.filter_rows(all_rows,owner=owner,unassigned=params.get('unassigned')=='1',
                                          sku=sku if daily else None,currency=currency)
            begin,_ = day_bounds(first,config['timezone'])
            _,finish = day_bounds(last,config['timezone'])
            owned_condition, owned_args = '', []
            if owner is not None:
                owned_condition = ' AND h.owner_user_id=%s'; owned_args.append(owner)
            elif params.get('unassigned') == '1':
                owned_condition = ' AND h.owner_user_id IS NULL'
            if daily:
                owned_condition += ' AND BINARY s.seller_sku=BINARY %s'; owned_args.append(sku)
            cursor.execute('''SELECT s.seller_sku,h.owner_user_id,h.effective_from_utc,h.effective_to_utc
                FROM listing_responsibility_history h
                JOIN listing_responsibility_scopes s ON s.id=h.scope_id WHERE s.shop_id=%s AND s.marketplace_id=%s
                AND h.voided_at IS NULL AND h.effective_from_utc<%s
                AND (h.effective_to_utc IS NULL OR h.effective_to_utc>%s)'''+owned_condition+
                ' LIMIT 10001', (shop,market,finish,begin,*owned_args))
            responsible = cursor.fetchall()
            if len(responsible)>10000:
                raise DomainError('负责 SKU 超过查询上限，请缩小范围',422)
            if currency and currency.upper()!=report_currency:
                # 原日报只有已核实的报告币种，不能用其他币种补出虚假的负责金额行。
                responsible = []
            owned_dates = responsibility_dates(responsible,first,last,config['timezone'])
            # 来源记录本身已由read_range逐日验证归属，也证明对应的负责日期。
            for row in rows:
                owned_dates[(row['owner_user_id'],row['seller_sku'])].add(row['business_date'])
            all_skus = {r['seller_sku'] for r in rows if r['seller_sku']} | {r['seller_sku'] for r in responsible}
            names = {}
            if all_skus:
                cursor.execute('SELECT seller_sku,product_name FROM products WHERE seller_sku IN ('+
                               ','.join(['%s']*len(all_skus))+')',tuple(sorted(all_skus)))
                names = {r['seller_sku']:r['product_name'] for r in cursor.fetchall()}
            index = order_counts.read(cursor,shop,market,first,last,config,basis,
                                      allowed_days={r['business_date'] for r in rows},
                                      allowed_sku_days={(r['seller_sku'],r['business_date']) for r in rows})
            if index:
                metadata['order_count_missing_orders'] = index['missing_orders']
            buckets = defaultdict(list)
            counted_dates = defaultdict(set)
            for row in rows:
                key = (row['owner_user_id'],row['seller_sku'],row['currency'])
                counted_dates[key].add(row['business_date'])
                if daily:
                    key += (row['business_date'],)
                buckets[key].append(row)
            if daily:
                # 只有事实或负责历史证明该人员可见此SKU时才补缺日，避免用空行探测他人SKU。
                known = bool(rows or responsible)
                if known:
                    days = ({first+timedelta(days=i) for i in range((last-first).days+1)}
                            if daily_scope=='all' else owned_dates.get((owner,sku),set()))
                    for day in days:
                        buckets.setdefault((owner,sku,(currency or report_currency).upper(),day),[])
            else:
                for entry in responsible:
                    buckets.setdefault((entry['owner_user_id'],entry['seller_sku'],(currency or report_currency).upper()),[])
            result = []
            for key,facts in buckets.items():
                user_id,seller_sku,unit_currency = key[:3]
                if facts:
                    metrics = summarize(facts)[0]
                    profit = metrics['sku_report_profit']
                    metrics.update(profit=profit, ad_share=(metrics['ad_sales']/metrics['sales_amount']
                                                          if metrics['sales_amount']>0 else None))
                    product_name = names.get(seller_sku) or facts[0].get('product_name')
                else:
                    metrics = {'sales_qty':None, 'sales_amount':None, 'ad_cost':None, 'refund_loss':None,'profit':None,
                               'acos':None, 'tacos':None, 'ad_share':None, 'order_count':None}
                    product_name = names.get(seller_sku)
                owned_days = owned_dates.get((user_id,seller_sku),set())
                metrics.update(coverage_fields(owned_days,counted_dates.get(key[:3],set())))
                if index is not None and facts:
                    order_key = (user_id,seller_sku,unit_currency) + ((key[3],) if daily else ())
                    metrics['order_count'] = order_counts.count(index,'daily' if daily else 'sku',order_key)
                if daily:
                    metrics['business_date'] = key[3]
                    metrics['source_state'] = ('available' if facts else
                                               'missing_report' if key[3] in owned_days else 'not_responsible')
                    if key[3] not in data_days:
                        metrics['order_count'] = None
                metrics.update(owner_user_id=user_id, seller_sku=seller_sku,
                               product_name=product_name, currency=unit_currency)
                if not show_costs:
                    for name in ('product_cost','headway_cost','platform_fees','fba_fees','other_fees'):
                        metrics.pop(name,None)
                result.append(metrics)
            # 合计必须在分页前从同一批实际归属来源求和，不能求当前页或重复扣退款。
            summaries = summarize(rows)
            for total in summaries:
                total['profit'] = total['sku_report_profit']
                total['ad_share'] = total['ad_sales']/total['sales_amount'] if total['sales_amount']>0 else None
                if not show_costs:
                    for name in ('product_cost','headway_cost','platform_fees','fba_fees','other_fees'):
                        total.pop(name,None)
            missing_days = sum(len(days-counted_dates.get((user_id,seller_sku,report_currency),set()))
                               for (user_id,seller_sku),days in owned_dates.items())
            result = sorted_rows(result, sort_by, sort_order)
            return {**metadata,'list':result[(page-1)*size:page*size], 'total':len(result),
                    'summary':summaries,'missing_report_sku_days':missing_days,'daily_scope':daily_scope,
                    'page':page, 'page_size':size, 'sort_by':sort_by, 'sort_order':sort_order}
    finally:
        conn.close()
