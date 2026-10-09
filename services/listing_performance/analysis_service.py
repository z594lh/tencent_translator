"""负责人关注 SKU 配置及经营周期对比，不是人员绩效重算服务。

配置按店铺/站点/负责人唯一保存；当前负责 SKU 可以读取接手前的经营历史，
因此调用方必须检查独立 analysis_self/analysis_all 权限，而不是只检查绩效查看权。
金额只读 sku_profit，利润和退款原样汇总；仅 save_config 写配置表。
没有广告接口、AI、导出、任务、定时同步或订单/财务写入。
"""
from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
import json
import pymysql

from services.mysql_service import get_db_connection
from services.listing_performance.common import DomainError, market_config, parse_date, positive_id, utc_now
from services.listing_performance.performance_service import amount


METRICS = (
    {'key': 'sales_qty', 'label': '销量', 'kind': 'quantity'},
    {'key': 'sales_amount', 'label': '销售额', 'kind': 'amount'},
    {'key': 'ad_cost', 'label': '广告费', 'kind': 'amount'},
    {'key': 'sku_report_profit', 'label': 'SKU 报表利润', 'kind': 'amount'},
    {'key': 'acos', 'label': 'ACOS', 'kind': 'ratio'},
    {'key': 'tacos', 'label': 'TACOS', 'kind': 'ratio'},
    {'key': 'ad_sales', 'label': '广告归因销售额', 'kind': 'amount'},
    {'key': 'ad_orders', 'label': '广告归因订单数', 'kind': 'quantity'},
    {'key': 'refund_loss', 'label': 'SKU 报表退款', 'kind': 'amount'},
    {'key': 'profit_margin', 'label': '利润率', 'kind': 'ratio'},
)
METRIC_KEYS = tuple(item['key'] for item in METRICS)
DEFAULT_METRICS = list(METRIC_KEYS[:6])
SUM_FIELDS = {
    'sales_qty': 'sales_qty', 'sales_amount': 'sales_amount', 'ad_cost': 'ad_cost',
    'sku_report_profit': 'net_profit', 'ad_sales': 'ad_sales',
    'ad_orders': 'ad_orders', 'refund_loss': 'refund_amount',
}
PERIODS = (1, 3, 7, 14, 30)
MAX_SKUS = 200


def integer(value, name, minimum=0, maximum=2147483647):
    """严格校验范围内整数，拒绝布尔值、小数及非法表示；用于版本、页码和周期。"""
    if isinstance(value, bool):
        raise DomainError(f'{name} 必须是整数')
    try:
        number = int(str(value))
    except (TypeError, ValueError):
        raise DomainError(f'{name} 必须是整数') from None
    if number < minimum or number > maximum:
        raise DomainError(f'{name} 超出允许范围')
    return number


def text_input(value, name, maximum=100):
    """校验搜索字符串长度，不清理或改变精确 SKU 身份，不允许控制字符。"""
    if not isinstance(value, str) or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise DomainError(f'{name} 无效或过长')
    return value


@contextmanager
def session(params, owner, write=False):
    """创建并关闭本模块事务，返回(cursor, shop, market, config, owner_name)。

    只读采用一致性快照；配置写入先锁店铺，与现有负责人交接使用相同锁顺序，
    防止校验当前归属后又被并发交接。异常回滚；缺配置表返回明确上线提示。
    owner 是 HTTP 层已授权的用户 ID，本函数不代替用户权限校验。
    """
    if not hasattr(params, 'get'):
        raise DomainError('请求参数必须为对象')
    shop = positive_id(params.get('shop_id'), 'shop_id')
    market = params.get('marketplace_id')
    config = market_config(market)
    if config.get('currency') != 'USD' or config.get('report_currency', 'USD') != 'USD':
        raise DomainError('SKU 分析目前仅支持已核实的 USD 站点', 422)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL ' + ('READ COMMITTED' if write else 'REPEATABLE READ'))
            if not write:
                cursor.execute('SET SESSION TRANSACTION READ ONLY')
            conn.begin()
            cursor.execute('SELECT id,marketplace_id,region FROM amazon_shops WHERE id=%s AND status=1' +
                           (' FOR UPDATE' if write else ''), (shop,))
            row = cursor.fetchone()
            if not row or row['marketplace_id'] != market:
                raise DomainError('店铺/站点不匹配或店铺已停用')
            if row['region'] != 'na':
                raise DomainError('SKU 分析站点尚未核实', 422)
            cursor.execute("SELECT COALESCE(NULLIF(nickname,''),username) AS name FROM users WHERE id=%s", (owner,))
            user = cursor.fetchone()
            if not user:
                raise DomainError('负责人不存在', 404)
            yield cursor, shop, market, config, user['name']
        if write:
            conn.commit()
        else:
            conn.rollback()
    except pymysql.err.ProgrammingError as error:
        conn.rollback()
        if error.args and error.args[0] == 1146:
            raise DomainError('分析功能所需表尚未部署，请先执行 migrations 中的分析功能建表 SQL', 503) from None
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def current_skus(cursor, shop, market, owner):
    """只读该人员此刻负责的有效子 SKU，并取 products.product_name。

    不使用日报截止日代替当前归属，不自动创建 scopes。精确大小写匹配跨表 SKU；
    检查生效区间重叠，超过一万候选要求缩小管理范围，不静默截断。
    """
    now = utc_now()
    cursor.execute('''SELECT s.id,s.seller_sku,COALESCE(p.product_name,'') AS product_name,
        (SELECT COUNT(*) FROM listing_responsibility_history x WHERE x.scope_id=s.id
         AND x.voided_at IS NULL AND x.effective_from_utc<=%s
         AND (x.effective_to_utc IS NULL OR x.effective_to_utc>%s)) AS active_count
        FROM listing_responsibility_scopes s
        JOIN listing_responsibility_history h ON h.scope_id=s.id
        LEFT JOIN products p ON BINARY p.seller_sku=BINARY s.seller_sku
        WHERE s.shop_id=%s AND BINARY s.marketplace_id=BINARY %s
        AND h.owner_user_id=%s AND h.voided_at IS NULL AND h.effective_from_utc<=%s
        AND (h.effective_to_utc IS NULL OR h.effective_to_utc>%s)
        AND s.seller_sku<>'' AND EXISTS(
          SELECT 1 FROM amazon_listings l WHERE l.shop_id=s.shop_id
          AND BINARY l.marketplace_id=BINARY s.marketplace_id AND BINARY l.sku=BINARY s.seller_sku
          AND l.is_deleted=0 AND COALESCE(LOWER(l.parentage_level),'')<>'parent')
        ORDER BY BINARY s.seller_sku LIMIT 10001''', (now, now, shop, market, owner, now, now))
    rows = cursor.fetchall()
    if len(rows) > 10000:
        raise DomainError('当前负责 SKU 超过分析候选上限，请联系管理员', 422)
    result = {}
    for row in rows:
        if row['active_count'] != 1 or row['seller_sku'] in result:
            raise DomainError('SKU 负责人区间重叠或身份不唯一，请先核查归属', 409)
        result[row['seller_sku']] = {'seller_sku': row['seller_sku'],
                                      'product_name': row['product_name'], 'eligible': True}
    return result


def string_list(value, name, maximum):
    """校验有界字符串数组并保序去重，JSON 文本由保存/读取方法处理。"""
    if not isinstance(value, list) or len(value) > maximum:
        raise DomainError(f'{name} 必须是最多 {maximum} 项的数组')
    result = []
    for item in value:
        text_input(item, name, 255)
        if not item:
            raise DomainError(f'{name} 不能包含空值')
        if item not in result:
            result.append(item)
    return result


def load_config(cursor, shop, market, owner, lock=False):
    """读取一份负责人配置，缺记录返回空清单/推荐指标和版本 0。

    缺表不伪装为空配置，由 session 返回迁移提示；损坏 JSON 或未知指标报错。
    lock=True 只供已持有店铺写锁的保存操作使用，不自行提交或写入。
    """
    cursor.execute('''SELECT selected_skus_json,metric_keys_json,version,updated_at
        FROM performance_analysis_configs WHERE shop_id=%s AND marketplace_id=%s AND owner_user_id=%s''' +
                   (' FOR UPDATE' if lock else ''), (shop, market, owner))
    row = cursor.fetchone()
    if not row:
        return {'selected_skus': [], 'metric_keys': list(DEFAULT_METRICS), 'version': 0, 'updated_at': None}
    try:
        skus = string_list(json.loads(row['selected_skus_json']), '关注 SKU', MAX_SKUS)
        metrics = string_list(json.loads(row['metric_keys_json']), '指标', len(METRICS))
        if not metrics or any(key not in METRIC_KEYS for key in metrics) or integer(row['version'], '配置版本', 1) < 1:
            raise ValueError()
    except (TypeError, ValueError, DomainError):
        raise DomainError('保存的分析配置无效，请联系管理员核查', 422) from None
    return {'selected_skus': skus, 'metric_keys': metrics,
            'version': row['version'], 'updated_at': row['updated_at']}


def config_result(saved, candidates, owner_name):
    """组装配置响应，已转交 SKU 仅返回失效标记，不查询其产品或经营明细。"""
    return {**saved, 'owner_name': owner_name, 'metrics': list(METRICS),
            'max_selected_skus': MAX_SKUS, 'candidate_count': len(candidates),
            'selected_items': [candidates.get(sku, {'seller_sku': sku, 'product_name': '', 'eligible': False})
                               for sku in saved['selected_skus']]}


def get_config(params, owner):
    """获取已鉴权负责人的配置及当前选中项，不保存默认值，不修改归属。"""
    with session(params, owner) as (cursor, shop, market, _, name):
        saved = load_config(cursor, shop, market, owner)
        return config_result(saved, current_skus(cursor, shop, market, owner), name)


def candidates(params, owner):
    """按 SKU/中文名搜索当前负责候选并分页，最多返回 200 项，不截断已存清单。"""
    page = integer(params.get('page', 1), 'page', 1, 10000)
    size = integer(params.get('page_size', 60), 'page_size', 1, 200)
    search = text_input(params.get('search', ''), '搜索内容').casefold()
    with session(params, owner) as (cursor, shop, market, _, name):
        rows = list(current_skus(cursor, shop, market, owner).values())
        matches = [row for row in rows if not search or search in row['seller_sku'].casefold()
                   or search in row['product_name'].casefold()]
        return {'list': matches[(page - 1) * size:page * size], 'total': len(matches),
                'candidate_count': len(rows), 'page': page, 'page_size': size, 'owner_name': name}


def save_config(params, owner, actor):
    """事务保存一份配置；店铺锁与归属写入互斥，版本冲突不覆盖。

    允许保存空关注清单但至少一项指标；不允许保存已转交 SKU，不创建用户/权限。
    同内容保存不增加版本；首次保存 version=0，后续需携带返回的版本。
    """
    if not isinstance(params, dict):
        raise DomainError('请求参数必须为对象')
    skus = string_list(params.get('selected_skus'), '关注 SKU', MAX_SKUS)
    keys = string_list(params.get('metric_keys'), '指标', len(METRICS))
    expected = integer(params.get('version'), 'version')
    if not keys or any(key not in METRIC_KEYS for key in keys):
        raise DomainError('请至少选择一个支持的指标')
    with session(params, owner, write=True) as (cursor, shop, market, _, name):
        choices = current_skus(cursor, shop, market, owner)
        if any(sku not in choices for sku in skus):
            raise DomainError('所选 SKU 已转交、删除或不是该人员当前负责，请刷新配置', 409)
        old = load_config(cursor, shop, market, owner, lock=True)
        if old['version'] != expected:
            raise DomainError('配置已被其他操作更新，请重新打开配置后保存', 409)
        if old['version'] and old['selected_skus'] == skus and old['metric_keys'] == keys:
            return config_result(old, choices, name)
        now = utc_now()
        encoded = (json.dumps(skus, ensure_ascii=False), json.dumps(keys, ensure_ascii=False))
        if old['version'] == 0:
            cursor.execute('''INSERT INTO performance_analysis_configs
                (shop_id,marketplace_id,owner_user_id,selected_skus_json,metric_keys_json,version,
                 created_by,updated_by,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,1,%s,%s,%s,%s)''',
                           (shop, market, owner, *encoded, actor, actor, now, now))
        else:
            cursor.execute('''UPDATE performance_analysis_configs SET selected_skus_json=%s,
                metric_keys_json=%s,version=version+1,updated_by=%s,updated_at=%s
                WHERE shop_id=%s AND marketplace_id=%s AND owner_user_id=%s AND version=%s''',
                           (*encoded, actor, now, shop, market, owner, expected))
            if cursor.rowcount != 1:
                raise DomainError('配置并发更新，请重新加载', 409)
        return config_result({'selected_skus': skus, 'metric_keys': keys,
                              'version': expected + 1, 'updated_at': now}, choices, name)


def period_summary(rows, first, last):
    """汇总一个 SKU 的连续周期，保留缺日报日期；比率由汇总金额相除。

    无记录时全部指标为空；部分记录的值是已记录小计，不伪装为完整周期。
    源字段为空/非法、同日重复记录均拒绝；净利润、退款不重算。
    """
    selected = [row for row in rows if first <= row['report_date'] <= last]
    days = set()
    values = {key: Decimal(0) for key in SUM_FIELDS}
    for row in selected:
        if row['report_date'] in days:
            raise DomainError('SKU 日报存在同日重复 SKU，请核查来源后分析', 422)
        days.add(row['report_date'])
        for key, field in SUM_FIELDS.items():
            if row.get(field) is None:
                raise DomainError('SKU 日报必需指标缺失，不能当作零统计', 422)
            number = amount(row[field])
            if key in ('sales_qty', 'ad_orders') and number != number.to_integral_value():
                raise DomainError('SKU 日报数量不是整数', 422)
            values[key] += number
    expected = (last - first).days + 1
    missing = [first + timedelta(days=i) for i in range(expected) if first + timedelta(days=i) not in days]
    for key in ('sales_qty', 'ad_orders'):
        values[key] = int(values[key])
    values['acos'] = values['ad_cost'] / values['ad_sales'] if values['ad_sales'] > 0 else None
    values['tacos'] = values['ad_cost'] / values['sales_amount'] if values['sales_amount'] > 0 else None
    values['profit_margin'] = values['sku_report_profit'] / values['sales_amount'] if values['sales_amount'] > 0 else None
    return {'date_from': first, 'date_to': last, 'recorded_days': len(days), 'expected_days': expected,
            'missing_dates': missing, 'complete': not missing,
            'values': values if days else {key: None for key in METRIC_KEYS}}


def compare_metric(key, current, previous, complete):
    """计算方向/差额/相对变化，比率显示百分点、亏损显示差额，不把方向当经营好坏。

    缺日报或零分母不计算涨跌；零基线用新增，利润转盈/转亏不除以负数。
    point_change 为比例差乘 100，即真实百分点，不再次乘百分比。
    """
    result = {'kind': 'incomplete' if not complete else 'undefined', 'direction': 'none',
              'delta': None, 'rate': None, 'point_change': None}
    if not complete or current is None or previous is None:
        return result
    current, previous = amount(current), amount(previous)
    delta = current - previous
    result.update(kind='flat' if not delta else 'change',
                  direction='up' if delta > 0 else 'down' if delta < 0 else 'flat', delta=delta)
    if key in ('acos', 'tacos', 'profit_margin'):
        result['point_change'] = delta * 100
        result['rate'] = delta / previous if previous > 0 else None
    elif key == 'sku_report_profit' and (current < 0 or previous < 0):
        if previous < 0 <= current:
            result['kind'] = 'turn_profit' if current > 0 else 'loss_cleared'
        elif current < 0 <= previous:
            result['kind'] = 'turn_loss' if previous > 0 else 'new_loss'
        elif delta:
            result['kind'] = 'loss_narrowed' if delta > 0 else 'loss_expanded'
    elif previous > 0:
        result['rate'] = delta / previous
    elif not previous and current:
        result['kind'] = 'new' if current > 0 else 'negative_adjustment'
    elif previous < 0:
        # 非利润指标也可能有负调整，保留金额差，不制造负分母增长率。
        result['kind'] = 'negative_adjustment' if delta else 'flat'
    return result


def sort_rows(rows, field, direction):
    """按所有分析结果排序后再分页；数值用 Decimal，空值始终最后，SKU 作稳定次序。"""
    rows = sorted(rows, key=lambda row: row['seller_sku'])
    if field in ('seller_sku', 'product_name'):
        return sorted(rows, key=lambda row: row[field], reverse=direction == 'desc')
    known = [row for row in rows if row['current']['values'][field] is not None]
    unknown = [row for row in rows if row['current']['values'][field] is None]
    return sorted(known, key=lambda row: row['current']['values'][field], reverse=direction == 'desc') + unknown


def filter_rows(rows, search, metric='', direction=''):
    """在分页前同时匹配 SKU/中文名和已校验的指标方向，不修改原结果。

    方向沿用 compare_metric：亏损扩大属于 down，收窄属于 up；持平、缺日报、
    无有效比率均不属于涨跌。未选方向时不过滤，metric 由 query 白名单校验。
    """
    return [row for row in rows
            if (not search or search in row['seller_sku'].casefold() or search in row['product_name'].casefold())
            and (not direction or row['changes'][metric]['direction'] == direction)]


def query(params, owner):
    """只读已存关注配置，按最新全店 SKU 日报日期作 N 天/前 N 天对比。

    当前归属用于访问授权，接手前日报可计入经营分析，但不会改变原绩效。
    已转交 SKU 不读取、不展示经营数据，仅返回失效清单；同请求固定截止日和配置版本。
    指标涨跌筛选先匹配全量结果，再排序/分页；缺失和无有效比率不计入涨跌。
    """
    period = integer(params.get('period', 1), 'period', 1, 30)
    if period not in PERIODS:
        raise DomainError('分析周期仅支持 1/3/7/14/30 天')
    page = integer(params.get('page', 1), 'page', 1, 10000)
    size = integer(params.get('page_size', 20), 'page_size', 1, 100)
    field = params.get('sort_by', 'seller_sku')
    direction = params.get('sort_order', 'asc')
    if field not in (*METRIC_KEYS, 'seller_sku', 'product_name') or direction not in ('asc', 'desc'):
        raise DomainError('分析排序参数无效')
    search = text_input(params.get('search', ''), '搜索内容').casefold()
    change_metric = params.get('change_metric', '')
    change_direction = params.get('change_direction', '')
    if change_metric not in ('', *METRIC_KEYS) or change_direction not in ('', 'up', 'down'):
        raise DomainError('指标涨跌筛选参数无效')
    if change_direction and not change_metric:
        raise DomainError('请先选择涨跌筛选指标')
    with session(params, owner) as (cursor, shop, market, config, name):
        saved = load_config(cursor, shop, market, owner)
        if change_metric and change_metric not in saved['metric_keys']:
            raise DomainError('涨跌筛选指标未配置，请先在分析配置中添加该指标')
        if not saved['selected_skus']:
            raise DomainError('请先在分析配置中保存关注 SKU', 409)
        if params.get('config_version') is not None and integer(params['config_version'], 'config_version') != saved['version']:
            raise DomainError('分析配置已更新，请重新打开分析数据', 409)
        choices = current_skus(cursor, shop, market, owner)
        invalid = [sku for sku in saved['selected_skus'] if sku not in choices]
        skus = [sku for sku in saved['selected_skus'] if sku in choices]
        cursor.execute('SELECT MAX(report_date) AS last_date FROM sku_profit WHERE shop_id=%s', (shop,))
        last = cursor.fetchone()['last_date']
        if last is None:
            raise DomainError('该店铺尚无 SKU 日报，无法确定分析日期', 422)
        if params.get('report_date'):
            # 翻页/排序保持首次分析截止日；新打开分析不传此字段，仍取最新日报。
            anchor = parse_date(params['report_date'])
            cursor.execute('SELECT id FROM sku_profit WHERE shop_id=%s AND report_date=%s LIMIT 1', (shop, anchor))
            if anchor > last or not cursor.fetchone():
                raise DomainError('分析截止日不存在，请重新打开分析数据')
            last = anchor
        first = last - timedelta(days=period - 1)
        prior_last = first - timedelta(days=1)
        prior_first = first - timedelta(days=period)
        grouped = {sku: [] for sku in skus}
        if skus:
            cursor.execute('''SELECT sku,product_name,report_date,sales_qty,sales_amount,ad_cost,
                net_profit,ad_sales,ad_orders,refund_amount FROM sku_profit
                WHERE shop_id=%s AND report_date BETWEEN %s AND %s AND BINARY sku IN (''' +
                           ','.join(['%s'] * len(skus)) + ') ORDER BY report_date,id LIMIT 12001',
                           (shop, prior_first, last, *skus))
            source = cursor.fetchall()
            if len(source) > MAX_SKUS * 60:
                raise DomainError('SKU 日报超过分析上限或有重复记录，请核查来源', 422)
            for row in source:
                if row['sku'] in grouped:
                    grouped[row['sku']].append(row)
        result = []
        for sku in skus:
            raw = grouped[sku]
            product = choices[sku]['product_name'] or next((r['product_name'] for r in reversed(raw) if r['product_name']), '')
            current = period_summary(raw, first, last)
            previous = period_summary(raw, prior_first, prior_last)
            complete = current['complete'] and previous['complete']
            result.append({'seller_sku': sku, 'product_name': product, 'current': current, 'previous': previous,
                           'changes': {key: compare_metric(key, current['values'][key], previous['values'][key], complete)
                                       for key in saved['metric_keys']}})
        missing = sum(not row['current']['complete'] or not row['previous']['complete'] for row in result)
        matches = filter_rows(result, search, change_metric, change_direction)
        ordered = sort_rows(matches, field, direction)
        return {'owner_name': name, 'owner_user_id': owner, 'currency': config['currency'],
                'timezone': config['timezone'], 'report_date': last, 'period': period,
                'current_range': {'date_from': first, 'date_to': last},
                'previous_range': {'date_from': prior_first, 'date_to': prior_last},
                'config_version': saved['version'], 'metric_keys': saved['metric_keys'],
                'metrics': [next(m for m in METRICS if m['key'] == key) for key in saved['metric_keys']],
                'invalid_skus': invalid, 'configured_count': len(saved['selected_skus']),
                'eligible_count': len(skus), 'incomplete_count': missing,
                'change_filter': {'metric': change_metric, 'direction': change_direction},
                'list': ordered[(page - 1) * size:page * size], 'total': len(matches), 'page': page, 'page_size': size}
