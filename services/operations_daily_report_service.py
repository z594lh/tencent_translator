"""运营日报：只读原SKU日报辅助填写，保存三段式汇报和当时的经营快照。

不调用AI/Amazon，不重算利润、不扣退款、不修改负责人或广告。HTTP层校验独立日报权限；
写入只允许登录人本人。当前负责SKU可含接手前经营数据，与个人绩效历史归属无关。
日报使用短事务、店铺锁及版本检查；历史读取不依赖当前SKU是否已转交。
"""
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
import json
from zoneinfo import ZoneInfo

import pymysql
from services.mysql_service import get_db_connection
from services.listing_performance import analysis_service as sku_analysis
from services.listing_performance.common import DomainError, json_data, parse_date, positive_id, utc_now
from services import daily_report_operations as operation_records, ads_display

MAX_SKUS = 1000
MAX_FEATURED = 10
METRICS = ('sales_qty', 'sales_amount', 'ad_cost', 'sku_report_profit', 'acos', 'tacos')


def today():
    """工作日期统一按北京时间，与Amazon站点业务日/UTC保存时间严格区分。"""
    return datetime.now(ZoneInfo('Asia/Shanghai')).date()


def work_day(value):
    """校验工作日期不能在未来；允许补写过去日报，不偷偷把来源日期当工作日期。"""
    day = parse_date(value)
    if day > today():
        raise DomainError('工作日期不能晚于今天')
    return day


def scope(params):
    """验证请求店铺/站点身份；SQL仍需复核店铺记录，不能只依赖前端选择。"""
    shop = positive_id(params.get('shop_id'), 'shop_id')
    market = sku_analysis.text_input(params.get('marketplace_id'), 'marketplace_id', 32)
    if not market:
        raise DomainError('请选择站点')
    return shop, market


@contextmanager
def session(params, write=False):
    """日报自有读写事务：保存先锁店铺，与负责人交接相同锁顺序，异常回滚并关闭。

    历史允许读取停用店铺；新经营整理另由分析工具校验启用状态和已核实USD站点。
    不持有连接等待人工编辑/图片下载，不执行迁移，不吞缺表错误为零条记录。
    """
    shop, market = scope(params)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL ' + ('READ COMMITTED' if write else 'REPEATABLE READ'))
            if not write:
                cursor.execute('SET SESSION TRANSACTION READ ONLY')
            conn.begin()
            cursor.execute('SELECT id,shop_name,marketplace_id,status FROM amazon_shops WHERE id=%s' +
                           (' FOR UPDATE' if write else ''), (shop,))
            row = cursor.fetchone()
            if not row or row['marketplace_id'] != market:
                raise DomainError('店铺与站点不匹配')
            yield cursor, shop, market, row
        if write:
            conn.commit()
        else:
            conn.rollback()
    except pymysql.err.ProgrammingError as error:
        conn.rollback()
        if error.args and error.args[0] == 1146:
            raise DomainError('运营日报表尚未部署，请先执行 migrations 中的建表 SQL', 503) from None
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def text(value, label, limit):
    """校验人工多行文字长度，保留换行，不接受HTML执行；前端始终按纯文本呈现。"""
    if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 and c not in '\n\r\t' for c in value):
        raise DomainError(f'{label}必须是最多{limit}字的文字')
    return value.strip()


def decoded(value, kind):
    """解析系统保存的快照/清单，损坏时明确报错，不读取源表伪装成历史结果。"""
    try:
        data = json.loads(value)
        if not isinstance(data, kind):
            raise ValueError()
        return data
    except (TypeError, ValueError):
        raise DomainError('日报存档内容不完整，请联系管理员核查', 503) from None


def recommend(row):
    """解释性规则推荐，优先持续亏损/金额影响，不用小基数百分比夸大异常。

    规则仅是汇报提醒，不判断产品阶段、库存或自动调整广告；广告归因尚可能修正。
    7天对比缺日报时不产生涨跌结论，数据缺口只单独提醒，不当零业绩。
    """
    current, previous = row['week'], row['prior_week']
    now, old = current['values'], previous['values']
    tags, score, category, signals = [], Decimal(0), 'normal', []
    if not current['complete']:
        return {'tags': ['近7天日报有缺口，请核实来源'], 'score': Decimal(0), 'category': 'missing', 'signals': ['missing']}
    profit = now['sku_report_profit']
    if profit is not None and profit < 0:
        tags.append('近7天利润为负')
        signals.append('loss')
        category = 'attention'
        score += abs(profit)
    if previous['complete']:
        profit_drop = old['sku_report_profit'] - profit
        if profit_drop >= 5:
            tags.append('利润较前7天减少 ' + f'{profit_drop:.2f}')
            signals.append('profit_decline')
            category = 'attention'
            score += profit_drop
        if old['sales_qty'] >= 5 and now['sales_qty'] <= old['sales_qty'] * Decimal('.7'):
            tags.append('销量较前7天下降至少30%（前期≥5件）')
            signals.append('sales_decline')
            category = 'attention'
            score += max(Decimal(0), old['sales_amount'] - now['sales_amount']) * Decimal('.1')
        if now['ad_cost'] - old['ad_cost'] >= 5 and now['ad_sales'] <= old['ad_sales']:
            tags.append('广告费增加但归因销售未改善，需观察')
            signals.append('ad_efficiency')
            category = 'attention'
            score += now['ad_cost'] - old['ad_cost']
        if category == 'normal' and profit - old['sku_report_profit'] >= 5:
            tags.append('利润较前7天改善 ' + f'{profit - old["sku_report_profit"]:.2f}')
            signals.append('improved')
            category = 'improved'
            score += profit - old['sku_report_profit']
    else:
        tags.append('前7天数据不足，不判断周期涨跌')
    return {'tags': tags or ['无明显规则异常，可按实际工作选择'], 'score': score, 'category': category, 'signals': signals}


def overall(rows, key):
    """汇总全部当前负责SKU，不只汇总重点项；空来源保留空值，比例按总金额相除。"""
    periods = [row[key] for row in rows]
    values = {}
    for metric in sku_analysis.SUM_FIELDS:
        known = [p['values'][metric] for p in periods if p['values'][metric] is not None]
        values[metric] = sum(known, Decimal(0)) if known else None
    for metric, denominator in [('acos', 'ad_sales'), ('tacos', 'sales_amount'), ('profit_margin', 'sales_amount')]:
        values[metric] = (values['ad_cost' if metric != 'profit_margin' else 'sku_report_profit'] / values[denominator]
                          if values[denominator] is not None and values[denominator] > 0 else None)
    return {'values': values, 'sku_count': len(rows), 'recorded_sku_count': sum(p['recorded_days'] > 0 for p in periods),
            'incomplete_count': sum(not p['complete'] for p in periods),
            'complete': bool(periods) and all(p['complete'] for p in periods)}


def build_snapshot(cursor, shop, market, owner, day, shop_info, anchor=None):
    """在调用方事务内生成真实经营快照：最新已存且早于工作日的日报＋7天等长比较。

    查询所有当前负责SKU，不依赖关注配置；报价/成本明细、买家信息不进入日报。
    金额只读sku_profit，净利润已含原报表退款，不另扣。显式截止日用于预览保存一致。
    """
    from services.listing_performance.common import market_config
    config = market_config(market)
    if shop_info['status'] != 1 or config.get('currency') != 'USD' or config.get('report_currency', 'USD') != 'USD':
        raise DomainError('经营概况目前仅支持启用且已核实的USD店铺', 422)
    cursor.execute('SELECT region FROM amazon_shops WHERE id=%s', (shop,))
    if cursor.fetchone()['region'] != 'na':
        raise DomainError('此区域日报经营口径尚未核实', 422)
    choices = sku_analysis.current_skus(cursor, shop, market, owner)
    if len(choices) > MAX_SKUS:
        raise DomainError('当前负责SKU超过1000个，请联系管理员确认范围', 422)
    cursor.execute("SELECT COALESCE(NULLIF(nickname,''),username) AS name FROM users WHERE id=%s AND status=1", (owner,))
    user = cursor.fetchone()
    if not user:
        raise DomainError('填写人员不存在或已停用', 403)
    cursor.execute('SELECT MAX(report_date) AS last FROM sku_profit WHERE shop_id=%s AND report_date<%s', (shop, day))
    last = cursor.fetchone()['last']
    if anchor:
        anchor = parse_date(anchor)
        cursor.execute('SELECT id FROM sku_profit WHERE shop_id=%s AND report_date=%s LIMIT 1', (shop, anchor))
        if not last or anchor > last or not cursor.fetchone():
            raise DomainError('经营截止日不存在或不早于工作日，请重新整理')
        last = anchor
    grouped = {sku: [] for sku in choices}
    if last and choices:
        # 最多读取1000个SKU的14天，有界参数化IN，不拼接SKU文本。
        cursor.execute('''SELECT sku,product_name,report_date,sales_qty,sales_amount,ad_cost,net_profit,
            ad_sales,ad_orders,refund_amount FROM sku_profit WHERE shop_id=%s
            AND report_date BETWEEN %s AND %s AND BINARY sku IN (''' + ','.join(['%s'] * len(choices)) +
            ') ORDER BY report_date,id LIMIT 14001', (shop, last - timedelta(days=13), last, *choices))
        source = cursor.fetchall()
        if len(source) > 14000:
            raise DomainError('SKU来源规模异常或存在重复日报，请先核对', 422)
        for record in source:
            if record['sku'] in grouped:
                grouped[record['sku']].append(record)
    end = last or day - timedelta(days=1)
    rows = []
    for sku, item in choices.items():
        raw = grouped[sku]
        row = {**item, 'product_name': item['product_name'] or next((r['product_name'] for r in reversed(raw) if r['product_name']), '')}
        for key, start, finish in [('day', end, end), ('prior_day', end - timedelta(days=1), end - timedelta(days=1)),
                                    ('week', end - timedelta(days=6), end), ('prior_week', end - timedelta(days=13), end - timedelta(days=7))]:
            row[key] = sku_analysis.period_summary(raw, start, finish)
        row['changes'] = {key: sku_analysis.compare_metric(key, row['day']['values'][key], row['prior_day']['values'][key],
                         row['day']['complete'] and row['prior_day']['complete']) for key in METRICS}
        row['recommendation'] = recommend(row)
        rows.append(row)
    rows.sort(key=lambda r: ({'attention': 0, 'improved': 1, 'missing': 2, 'normal': 3}[r['recommendation']['category']],
                             -r['recommendation']['score'], r['seller_sku']))
    sums = {key: overall(rows, key) for key in ('day', 'prior_day', 'week', 'prior_week')}
    changes = {key: sku_analysis.compare_metric(key, sums['day']['values'][key], sums['prior_day']['values'][key],
               sums['day']['complete'] and sums['prior_day']['complete']) for key in METRICS}
    recommended = [r['seller_sku'] for r in rows if r['recommendation']['category'] in ('attention', 'improved')][:5]
    return {'schema_version': 1, 'owner_name': user['name'], 'shop_name': shop_info['shop_name'],
            'currency': config['currency'], 'report_date': last, 'work_date': day, 'generated_at': utc_now(),
            'stale_days': (day - last).days if last else None, 'summaries': sums, 'changes': changes,
            'recommended_skus': recommended, 'rows': rows,
            'notes': ['当前负责SKU经营历史（可包含接手前），不是个人绩效；全部当前SKU汇总，不只统计重点项。',
                      '利润与退款沿用SKU日报，不重复扣退款；缺来源不当零，广告归因可能回补，规则提醒不等于停广告建议。']}


def preview(params, owner):
    """只读生成编辑器候选和概况，不写草稿、不调用AI；owner只能由HTTP取登录身份。"""
    day = work_day(params.get('work_date'))
    with session(params) as (cursor, shop, market, info):
        return build_snapshot(cursor, shop, market, owner, day, info, params.get('data_date'))


def report_result(row):
    """组装完整历史日报；读取保存快照，不查询最新SKU或重新计算历史内容。"""
    return {**{k: v for k, v in row.items() if k not in ('snapshot_json', 'selected_skus_json')},
            'snapshot': ads_display.decorate_snapshot(decoded(row['snapshot_json'], dict)), 'selected_skus': decoded(row['selected_skus_json'], list)}


def operations(params, actor):
    """按本人/店铺/北京时间工作日分页，只读本地ERP证据。"""
    day = work_day(params.get('work_date'))
    page = sku_analysis.integer(params.get('page',1),'page',1,10000)
    size = sku_analysis.integer(params.get('page_size',20),'page_size',1,50)
    with session(params) as (cursor, shop, market, _):
        rows,total = operation_records.rows(cursor,params.get('kind'),shop,market,actor,day,page,size)
        return {'list':rows,'total':total,'page':page,'page_size':size,'work_date':day,'timezone':'Asia/Shanghai'}


def save(params, actor):
    """只为登录人本人保存/提交日报，唯一键和版本防重复、并发覆盖。

    新建或refresh_data=true时服务端重读真实统计；普通改文字/重点清单保留原快照。
    已提交日报保存修改仍为已提交，submitted_at保留首次提交时间；不接受浏览器金额。
    """
    if not isinstance(params, dict):
        raise DomainError('请求必须为对象')
    day = work_day(params.get('work_date'))
    version = sku_analysis.integer(params.get('version'), 'version')
    # 同一天删除重建后version可能重新为1；同时比较记录ID，避免旧窗口误覆盖新日报。
    expected_id = positive_id(params['report_id'], '日报ID') if params.get('report_id') is not None else None
    status = params.get('status')
    if status not in ('draft', 'submitted'):
        raise DomainError('日报状态无效')
    situation = text(params.get('situation_text', ''), '经营补充', 4000)
    actions = text(params.get('actions_text', ''), '调整优化', 8000)
    issues = text(params.get('issues_text', ''), '问题协助', 4000)
    no_issues = params.get('no_issues', False)
    refresh = params.get('refresh_data', False)
    if not isinstance(no_issues, bool) or not isinstance(refresh, bool):
        raise DomainError('暂无问题/刷新数据必须为布尔值')
    if no_issues and issues:
        raise DomainError('已勾选暂无问题，请清空问题文字或取消勾选')
    selected = sku_analysis.string_list(params.get('selected_skus'), '重点SKU', MAX_FEATURED)
    with session(params, write=True) as (cursor, shop, market, info):
        cursor.execute('''SELECT * FROM operations_daily_reports WHERE shop_id=%s AND marketplace_id=%s
            AND owner_user_id=%s AND work_date=%s FOR UPDATE''', (shop, market, actor, day))
        old = cursor.fetchone()
        if ((old['version'] if old else 0) != version
                or (version > 0 and (not old or old['id'] != expected_id))
                or (version == 0 and expected_id is not None)):
            raise DomainError('日报已保存或被其他窗口修改，请重新打开后编辑', 409)
        status = 'submitted' if old and old['status'] == 'submitted' else status
        old_snapshot = decoded(old['snapshot_json'],dict) if old else {}
        sections,reported = operation_records.resolve(cursor,shop,market,actor,day,params,old_snapshot,status=='submitted')
        if status == 'submitted' and ((not actions and not reported) or (not no_issues and not issues)):
            raise DomainError('提交前请勾选汇报操作，或补充实际工作；同时填写问题协助（或勾选暂无）')
        snapshot = (build_snapshot(cursor, shop, market, actor, day, info, params.get('data_date'))
                    if not old or refresh else decoded(old['snapshot_json'], dict))
        snapshot['operation_sections'], snapshot['reported_operations'] = sections,reported
        available = {r['seller_sku'] for r in snapshot['rows']}
        if any(sku not in available for sku in selected):
            raise DomainError('重点SKU不在本次经营快照中，可能已转交，请重新整理或取消选择', 409)
        now = utc_now()
        submitted = old['submitted_at'] if old else None
        if status == 'submitted' and not submitted:
            submitted = now
        values = (status, version + 1, json_data(selected), json_data(snapshot), situation, actions,
                  issues, int(no_issues), submitted, now)
        if old:
            cursor.execute('''UPDATE operations_daily_reports SET status=%s,version=%s,selected_skus_json=%s,
                snapshot_json=%s,situation_text=%s,actions_text=%s,issues_text=%s,no_issues=%s,
                submitted_at=%s,updated_at=%s WHERE id=%s AND version=%s''', (*values, old['id'], version))
            if cursor.rowcount != 1:
                raise DomainError('日报并发更新，请重新打开', 409)
            report_id = old['id']
        else:
            cursor.execute('''INSERT INTO operations_daily_reports
                (status,version,selected_skus_json,snapshot_json,situation_text,actions_text,issues_text,
                 no_issues,submitted_at,updated_at,shop_id,marketplace_id,owner_user_id,work_date,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (*values, shop, market, actor, day, now))
            report_id = cursor.lastrowid
        cursor.execute('SELECT * FROM operations_daily_reports WHERE id=%s', (report_id,))
        return report_result(cursor.fetchone())


def detail(params, actor, can_all, record_id=None):
    """读取ID详情或本人指定工作日草稿；ID同时校验店铺/站点/人员范围防越权。"""
    with session(params) as (cursor, shop, market, _):
        args = [shop, market]
        where = 'shop_id=%s AND marketplace_id=%s'
        if record_id is not None:
            where += ' AND id=%s'
            args.append(positive_id(record_id, '日报ID'))
            if not can_all:
                where += ' AND owner_user_id=%s'
                args.append(actor)
        else:
            where += ' AND owner_user_id=%s AND work_date=%s'
            args.extend([actor, work_day(params.get('work_date'))])
        cursor.execute('SELECT * FROM operations_daily_reports WHERE ' + where, args)
        row = cursor.fetchone()
        if not row:
            if record_id is None:
                return None
            raise DomainError('日报不存在或没有查看权限', 404)
        return report_result(row)


def delete_report(params, actor, can_all, record_id):
    """物理删除一个已确认的日报，不修改SKU源报表或绩效。

    独立删除权由HTTP层检查；这里再次限制店铺/站点/人员及版本。先锁店铺、再锁日报，
    与保存保持相同顺序。任何版本冲突均回滚，不自动重试；删除后可重新填写该日。
    页面确认明确告知不可恢复，管理员也不能借owner参数绕过查看范围。
    """
    if not isinstance(params, dict):
        raise DomainError('请求必须为对象')
    record_id = positive_id(record_id, '日报ID')
    version = sku_analysis.integer(params.get('version'), 'version', 1)
    if params.get('confirm_delete') is not True:
        raise DomainError('请确认删除日报')
    with session(params, write=True) as (cursor, shop, market, _):
        cursor.execute('''SELECT id,owner_user_id,version FROM operations_daily_reports
            WHERE id=%s AND shop_id=%s AND marketplace_id=%s FOR UPDATE''', (record_id, shop, market))
        row = cursor.fetchone()
        if not row or (not can_all and row['owner_user_id'] != actor):
            raise DomainError('日报不存在或没有删除权限', 404)
        if row['version'] != version:
            raise DomainError('日报已被修改，请刷新查看最新内容后再删除', 409)
        cursor.execute('''DELETE FROM operations_daily_reports
            WHERE id=%s AND shop_id=%s AND marketplace_id=%s AND version=%s''', (record_id, shop, market, version))
        if cursor.rowcount != 1:
            raise DomainError('日报并发更新，请刷新后再删除', 409)
        return {'id': record_id, 'deleted': True}


def listing(params, actor, can_all):
    """日期倒序分页查询日报摘要，不加载全部快照；管理人员可筛选人和交报状态。"""
    first, last = parse_date(params.get('date_from')), work_day(params.get('date_to'))
    if first > last or (last - first).days > 366:
        raise DomainError('查询日期范围须为不超过367天的连续范围')
    page = sku_analysis.integer(params.get('page', 1), 'page', 1, 100000)
    size = sku_analysis.integer(params.get('page_size', 20), 'page_size', 1, 100)
    chosen = positive_id(params['owner_user_id'], 'owner_user_id') if params.get('owner_user_id') else None
    if not can_all and chosen not in (None, actor):
        raise DomainError('不能查询他人日报', 403)
    status = params.get('status', '')
    if status not in ('', 'draft', 'submitted'):
        raise DomainError('日报状态筛选无效')
    with session(params) as (cursor, shop, market, _):
        where = 'r.shop_id=%s AND r.marketplace_id=%s AND r.work_date BETWEEN %s AND %s'
        args = [shop, market, first, last]
        owner = chosen if can_all else actor
        if owner:
            where += ' AND r.owner_user_id=%s'
            args.append(owner)
        if status:
            where += ' AND r.status=%s'
            args.append(status)
        cursor.execute('SELECT COUNT(*) AS total FROM operations_daily_reports r WHERE ' + where, args)
        total = cursor.fetchone()['total']
        cursor.execute('''SELECT r.id,r.owner_user_id,r.work_date,r.status,r.version,r.submitted_at,r.updated_at,
            LEFT(r.actions_text,180) AS actions_summary,LEFT(r.issues_text,180) AS issues_summary,r.no_issues,
            COALESCE(JSON_LENGTH(JSON_EXTRACT(r.snapshot_json,'$.reported_operations')),0) AS reported_operation_count,
            COALESCE(NULLIF(u.nickname,''),u.username) AS owner_name
            FROM operations_daily_reports r LEFT JOIN users u ON u.id=r.owner_user_id WHERE ''' + where +
            ' ORDER BY r.work_date DESC,r.updated_at DESC,r.id DESC LIMIT %s OFFSET %s', (*args, size, (page - 1) * size))
        reports = cursor.fetchall()
        for report in reports:
            if not report['actions_summary'] and report['reported_operation_count']:
                report['actions_summary'] = f"已汇报 {report['reported_operation_count']} 条ERP操作及调整理由"
        return {'list': reports, 'total': total, 'page': page, 'page_size': size}


def configuration(actor, can_all, can_write, can_delete=False):
    """返回页面所需公开身份/店铺/权限，不依赖绩效权限，不包含店铺凭证或隐私字段。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT id,shop_name,marketplace_id,status FROM amazon_shops ORDER BY status DESC,id')
            shops = cursor.fetchall()
            users = []
            if can_all:
                cursor.execute("SELECT id,COALESCE(NULLIF(nickname,''),username) AS name FROM users ORDER BY id")
                users = cursor.fetchall()
        return {'user_id': actor, 'can_all': can_all, 'can_write': can_write, 'can_delete': can_delete,
                'today': today(), 'shops': shops, 'users': users, 'max_featured': MAX_FEATURED}
    finally:
        conn.close()
