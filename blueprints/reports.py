"""
报表模块 — 数据报表管理

简介: 提供经营报表、SKU 利润、库存周转、广告效果等模块的查询与生成接口。

前端接口:
  经营报表:
    GET    /api/reports/business                   分页查询经营报表（日/周/月）
    GET    /api/reports/business/summary            经营报表汇总统计
    GET    /api/reports/business/trend              经营趋势（销售额/毛利走势）
    POST   /api/reports/business/generate           手动触发生成
  SKU 利润表:
    GET    /api/reports/sku-profit                  分页查询 SKU 利润列表
    GET    /api/reports/sku-profit/summary          SKU 利润汇总统计
    GET    /api/reports/sku-profit/top              SKU 利润排行
    POST   /api/reports/sku-profit/generate         手动触发生成
  库存周转:
    GET    /api/reports/inventory-turnover           分页查询库存周转列表
    GET    /api/reports/inventory-turnover/stats     库存周转统计
    POST   /api/reports/inventory-turnover/generate  手动触发生成
    POST   /api/reports/inventory-turnover/batch-update-status  批量更新库存状态
  数据导入:
    POST   /api/reports/ad-spend/import              导入广告费明细
    POST   /api/reports/refund/import                导入退款明细
  生成日志:
    GET    /api/reports/generation-logs              查询报表生成日志
  广告效果报表:
    GET    /api/reports/advertising                  分页查询广告效果报表
    GET    /api/reports/advertising/summary          广告效果汇总统计
    GET    /api/reports/advertising/trend            广告效果趋势
    POST   /api/reports/advertising/generate         手动触发生成
  一键生成:
    POST   /api/reports/generate-yesterday           一键生成昨日全部报表

数据生成: services/report_generator.py（定时任务自动跑 + 本模块支持手动触发）
"""
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from decimal import Decimal

from blueprints.user_auth import login_required, permission_required
from services.mysql_service import get_db_connection
from services.report_generator import (
    generate_business_daily,
    generate_business_weekly,
    generate_business_monthly,
    generate_sku_profit,
    generate_inventory_turnover,
    generate_yesterday_reports,
    generate_advertising_daily,
    generate_advertising_weekly,
    generate_advertising_monthly,
)

reports_bp = Blueprint('reports', __name__, url_prefix='/api')


# ============================================================
# 工具函数
# ============================================================

def _get_conn():
    """获取数据库连接"""
    return get_db_connection()


def _to_json_serializable(obj):
    """
    将数据库返回值转为 JSON 可序列化格式

    简介: 递归处理 dict/list/Decimal/datetime 类型。
    """
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_serializable(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'strftime'):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    return obj


def _parse_pagination():
    """
    解析分页参数

    简介: 从 request.args 读取 page/page_size，带边界校验。
    返回: (page, page_size)
    """
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
    except (ValueError, TypeError):
        page, page_size = 1, 20
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 20
    if page_size > 500:
        page_size = 500
    return page, page_size


def _get_shop_id_optional():
    """
    从请求参数中读取可选的 shop_id

    简介: 不存在或格式错误时返回 None，表示不过滤。
    """
    shop_id = request.args.get('shop_id', '').strip() or None
    if shop_id is None:
        return None
    try:
        return int(shop_id)
    except ValueError:
        return None


# ============================================================
# 1. 经营报表
# ============================================================

@reports_bp.route('/reports/business', methods=['GET'])
@login_required
@permission_required('reports:page')
def list_business_reports():
    """
    分页查询经营报表列表

    简介: 支持按类型（daily/weekly/monthly）、日期范围、店铺过滤。

    查询参数:
        type       (可选) 报表类型: daily / weekly / monthly
        start_date (可选) report_date >=
        end_date   (可选) report_date <=
        shop_id    (可选) 店铺ID
        page       (可选) 页码，默认 1
        page_size  (可选) 每页条数，默认 20

    返回:
        { status, data: { list, total, page, page_size } }
        每行含 data_status 字段: estimated=预估 / settled=已结算
    """
    try:
        report_type = request.args.get('type', '').strip() or None
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []

            if report_type:
                where_clauses.append("report_type = %s")
                params.append(report_type)
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM report_business {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT * FROM report_business
                    {where_sql}
                    ORDER BY report_date DESC, id DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {
                    "list": _to_json_serializable(rows),
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_business_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/summary', methods=['GET'])
@login_required
@permission_required('reports:page')
def business_summary():
    """
    经营报表汇总统计

    简介: 对筛选范围内的日报/周报/月报做 SUM 聚合，返回总量指标。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = %s"]
            params = [report_type]
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) AS record_count,
                        SUM(total_sales) AS sum_sales,
                        SUM(total_cost) AS sum_cost,
                        SUM(gross_profit) AS sum_gross_profit,
                        AVG(gross_profit_rate) AS avg_gross_profit_rate,
                        SUM(headway_cost) AS sum_headway_cost,
                        AVG(headway_ratio) AS avg_headway_ratio,
                        SUM(order_count) AS sum_orders,
                        SUM(ad_cost) AS sum_ad_cost,
                        SUM(refund_amount) AS sum_refund,
                        AVG(refund_rate) AS avg_refund_rate
                    FROM report_business
                    {where_sql}
                """, params)
                summary = cursor.fetchone()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(summary),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[business_summary] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/trend', methods=['GET'])
@login_required
@permission_required('reports:page')
def business_trend():
    """
    经营趋势

    简介: 按时间维度返回销售额、毛利、头程占比走势，用于折线图。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = %s"]
            params = [report_type]
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            if report_type == 'daily':
                time_col = 'report_date'
                order_sql = f"ORDER BY {time_col}"
            elif report_type == 'weekly':
                time_col = "report_week"
                order_sql = "ORDER BY STR_TO_DATE(SUBSTRING_INDEX(report_week, '~', 1), '%%Y.%%m.%%d')"
            else:
                time_col = 'report_month'
                order_sql = f"ORDER BY {time_col}"

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        {time_col} AS time_label,
                        SUM(total_sales) AS total_sales,
                        SUM(gross_profit) AS gross_profit,
                        AVG(gross_profit_rate) AS gross_profit_rate,
                        SUM(headway_cost) AS headway_cost,
                        AVG(headway_ratio) AS headway_ratio
                    FROM report_business
                    {where_sql}
                    GROUP BY {time_col}
                    {order_sql}
                """, params)
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(rows),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[business_trend] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/generate', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_business_report():
    """
    手动触发经营报表生成

    简介: 支持按日/周/月维度手动生成，生成结果直接写入 report_business 表。

    请求体 (JSON):
        report_type  (必填) daily / weekly / monthly
        period       (daily/monthly 必填) 日期，如 2026-05-18 或 2026-05
        period_start (weekly 必填) 周开始日期，如 2026-05-11
        period_end   (weekly 必填) 周结束日期，如 2026-05-17
        shop_id      (可选) 指定店铺，不传则所有店铺
    """
    try:
        data = request.get_json() or {}
        report_type = data.get('report_type', '').strip()
        shop_id = data.get('shop_id')

        if report_type not in ('daily', 'weekly', 'monthly'):
            return jsonify({"status": "error", "message": "report_type 必须是 daily/weekly/monthly"}), 400

        if report_type == 'daily':
            period = data.get('period', '').strip()
            if not period:
                return jsonify({"status": "error", "message": "daily 类型需要 period 参数（如 2026-05-18）"}), 400
            result = generate_business_daily(period, shop_id)
        elif report_type == 'weekly':
            period_start = data.get('period_start', '').strip()
            period_end = data.get('period_end', '').strip()
            if not period_start or not period_end:
                return jsonify({"status": "error", "message": "weekly 类型需要 period_start 和 period_end"}), 400
            result = generate_business_weekly(period_start, period_end, shop_id)
        else:
            period = data.get('period', '').strip()
            if not period:
                return jsonify({"status": "error", "message": "monthly 类型需要 period 参数（如 2026-05）"}), 400
            result = generate_business_monthly(period, shop_id)

        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_business_report] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 2. SKU 利润表
# ============================================================

@reports_bp.route('/reports/sku-profit', methods=['GET'])
@login_required
@permission_required('reports:page')
def list_sku_profit():
    """
    分页查询 SKU 利润表

    简介: 按 ASIN/SKU 查询指定周期内的利润数据，支持关键词搜索。
    """
    try:
        keyword = request.args.get('keyword', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        sku = request.args.get('sku', '').strip() or None
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []

            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if keyword:
                where_clauses.append("(asin LIKE %s OR sku LIKE %s OR product_name LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            if asin:
                where_clauses.append("asin = %s")
                params.append(asin)
            if sku:
                where_clauses.append("sku = %s")
                params.append(sku)
            if start_date:
                where_clauses.append("period_start >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("period_end <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM sku_profit {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT * FROM sku_profit
                    {where_sql}
                    ORDER BY period_end DESC, net_profit DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {
                    "list": _to_json_serializable(rows),
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_sku_profit] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-profit/summary', methods=['GET'])
@login_required
@permission_required('reports:page')
def sku_profit_summary():
    """
    SKU 利润汇总统计

    简介: 对筛选范围内的 SKU 利润做 SUM 聚合。
    """
    try:
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("period_start >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("period_end <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        COUNT(DISTINCT asin) AS asin_count,
                        SUM(sales_amount) AS total_sales,
                        SUM(net_profit) AS total_net_profit,
                        AVG(profit_margin) AS avg_profit_margin,
                        SUM(ad_cost) AS total_ad_cost,
                        SUM(headway_cost) AS total_headway_cost
                    FROM sku_profit
                    {where_sql}
                """, params)
                summary = cursor.fetchone()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(summary),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[sku_profit_summary] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-profit/top', methods=['GET'])
@login_required
@permission_required('reports:page')
def sku_profit_top():
    """
    SKU 利润排行

    简介: 按净利润/利润率/销售额等维度排序，返回 Top N。
    """
    try:
        sort_by = request.args.get('sort_by', 'net_profit').strip()
        sort_dir = request.args.get('sort_dir', 'desc').strip().lower()
        limit = int(request.args.get('limit', 10))
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        allowed_sort = {'net_profit', 'profit_margin', 'sales_amount', 'gross_profit'}
        if sort_by not in allowed_sort:
            sort_by = 'net_profit'
        if sort_dir not in ('asc', 'desc'):
            sort_dir = 'desc'
        if limit < 1 or limit > 100:
            limit = 10

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("period_start >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("period_end <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        asin,
                        sku,
                        product_name,
                        SUM(sales_amount) AS sales_amount,
                        SUM(net_profit) AS net_profit,
                        AVG(profit_margin) AS profit_margin,
                        SUM(gross_profit) AS gross_profit
                    FROM sku_profit
                    {where_sql}
                    GROUP BY asin, sku, product_name
                    ORDER BY {sort_by} {sort_dir}
                    LIMIT %s
                """, params + [limit])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(rows),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[sku_profit_top] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-profit/generate', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_sku_profit():
    """
    手动触发 SKU 利润表生成

    请求体 (JSON):
        period_start (必填) 开始日期，如 2026-05-01
        period_end   (必填) 结束日期，如 2026-05-31
        shop_id      (可选) 指定店铺
    """
    try:
        data = request.get_json() or {}
        period_start = data.get('period_start', '').strip()
        period_end = data.get('period_end', '').strip()
        shop_id = data.get('shop_id')

        if not period_start or not period_end:
            return jsonify({"status": "error", "message": "period_start 和 period_end 必填"}), 400

        result = generate_sku_profit(period_start, period_end, shop_id)
        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_sku_profit] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 3. 库存周转
# ============================================================

@reports_bp.route('/reports/inventory-turnover', methods=['GET'])
@login_required
@permission_required('reports:page')
def list_inventory_turnover():
    """
    分页查询库存周转列表

    简介: 按 SKU 查询库存现状、销售速度和周转天数，支持按滞销/缺货状态过滤。
    """
    try:
        keyword = request.args.get('keyword', '').strip() or None
        sku = request.args.get('sku', '').strip() or None
        status = request.args.get('status', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if keyword:
                where_clauses.append("(sku LIKE %s OR asin LIKE %s OR product_name LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            if sku:
                where_clauses.append("sku = %s")
                params.append(sku)
            if status:
                where_clauses.append("stock_status = %s")
                params.append(status)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM inventory_turnover {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT * FROM inventory_turnover
                    {where_sql}
                    ORDER BY
                        FIELD(stock_status, 'out_of_stock', 'slow', 'warning', 'normal'),
                        turnover_days DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {
                    "list": _to_json_serializable(rows),
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_inventory_turnover] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/inventory-turnover/stats', methods=['GET'])
@login_required
@permission_required('reports:page')
def inventory_turnover_stats():
    """
    库存周转统计

    简介: 按库存状态（正常/滞销/缺货/预警）汇总 SKU 数量、库存量、货值。
    """
    try:
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        stock_status,
                        COUNT(*) AS count,
                        SUM(current_stock) AS total_stock,
                        SUM(inventory_value) AS total_value
                    FROM inventory_turnover
                    {where_sql}
                    GROUP BY stock_status
                """, params)
                status_stats = cursor.fetchall()

                cursor.execute(f"""
                    SELECT
                        COUNT(*) AS total_sku,
                        SUM(current_stock) AS total_stock,
                        SUM(inbound_qty) AS total_inbound,
                        SUM(inventory_value) AS total_inventory_value,
                        SUM(CASE WHEN stock_status = 'out_of_stock' THEN 1 ELSE 0 END) AS out_of_stock_count,
                        SUM(CASE WHEN stock_status = 'slow' THEN 1 ELSE 0 END) AS slow_count,
                        SUM(CASE WHEN stock_status = 'warning' THEN 1 ELSE 0 END) AS warning_count
                    FROM inventory_turnover
                    {where_sql}
                """, params)
                overall = cursor.fetchone()

            return jsonify({
                "status": "success",
                "data": {
                    "overall": _to_json_serializable(overall),
                    "by_status": _to_json_serializable(status_stats),
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[inventory_turnover_stats] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/inventory-turnover/generate', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_inventory_turnover():
    """
    手动触发库存周转生成

    请求体 (JSON):
        shop_id (可选) 指定店铺
    """
    try:
        data = request.get_json() or {}
        shop_id = data.get('shop_id')
        result = generate_inventory_turnover(shop_id)
        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_inventory_turnover] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/inventory-turnover/batch-update-status', methods=['POST'])
@login_required
@permission_required('reports:edit')
def batch_update_inventory_status():
    """
    批量更新库存状态

    简介: 根据当前库存和日均销量重新计算 stock_status 和建议补货量。

    请求体 (JSON):
        shop_id (必填) 店铺ID
    """
    try:
        data = request.get_json() or {}
        shop_id = data.get('shop_id')
        if shop_id is None:
            return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE inventory_turnover
                    SET stock_status = CASE
                        WHEN current_stock = 0 AND days_without_sale >= 30 THEN 'out_of_stock'
                        WHEN turnover_days > 90 OR days_without_sale >= 30 THEN 'slow'
                        WHEN turnover_days <= 7 AND current_stock > 0 THEN 'warning'
                        ELSE 'normal'
                    END,
                    suggested_replenish = GREATEST(0, ROUND(avg_daily_sales * 60 - total_available))
                    WHERE shop_id = %s
                """, (shop_id,))
                affected = cursor.rowcount
            conn.commit()
            return jsonify({
                "status": "success",
                "message": "状态更新完成",
                "data": {"affected_rows": affected},
            })
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as e:
        print(f"[batch_update_inventory_status] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 4. 数据导入接口
# ============================================================

@reports_bp.route('/reports/ad-spend/import', methods=['POST'])
@login_required
@permission_required('reports:edit')
def import_ad_spend():
    """
    导入广告费明细

    简介: 批量上传广告花费数据到 amazon_ad_spend 表，支持单条或数组。

    请求体 (JSON):
        records: [ { shop_id, date, ad_spend, campaign_id?, asin?, clicks?, ... }, ... ]
    """
    try:
        data = request.get_json() or {}
        records = data.get('records', [])
        if not records:
            return jsonify({"status": "error", "message": "records 不能为空"}), 400
        if isinstance(records, dict):
            records = [records]

        conn = _get_conn()
        inserted = 0
        updated = 0
        errors = []
        try:
            with conn.cursor() as cursor:
                for rec in records:
                    required = ['shop_id', 'date', 'ad_spend']
                    missing = [r for r in required if r not in rec]
                    if missing:
                        errors.append({"record": rec, "error": f"缺少 {', '.join(missing)}"})
                        continue

                    orders_7d = rec.get('orders_7d')
                    if orders_7d is None:
                        orders_7d = rec.get('orders', 0)
                    orders_30d = rec.get('orders_30d', 0)
                    sales_7d = rec.get('sales_7d')
                    if sales_7d is None:
                        sales_7d = rec.get('sales', 0)
                    sales_30d = rec.get('sales_30d', 0)

                    cursor.execute("""
                        INSERT INTO amazon_ad_spend (
                            shop_id, date, campaign_id, campaign_name, ad_group_id, ad_group_name,
                            asin, sku, currency,
                            ad_spend, clicks, impressions, orders_7d, orders_30d, sales_7d, sales_30d
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            ad_spend = VALUES(ad_spend),
                            clicks = VALUES(clicks),
                            impressions = VALUES(impressions),
                            orders_7d = VALUES(orders_7d),
                            orders_30d = VALUES(orders_30d),
                            sales_7d = VALUES(sales_7d),
                            sales_30d = VALUES(sales_30d),
                            campaign_name = VALUES(campaign_name),
                            ad_group_id = VALUES(ad_group_id),
                            ad_group_name = VALUES(ad_group_name),
                            sku = VALUES(sku),
                            currency = VALUES(currency),
                            updated_at = NOW()
                    """, (
                        rec['shop_id'], rec['date'],
                        rec.get('campaign_id', ''), rec.get('campaign_name', ''),
                        rec.get('ad_group_id', ''), rec.get('ad_group_name', ''),
                        rec.get('asin', ''), rec.get('sku', ''),
                        rec.get('currency', 'USD'),
                        rec['ad_spend'], rec.get('clicks', 0), rec.get('impressions', 0),
                        orders_7d, orders_30d, sales_7d, sales_30d,
                    ))
                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        updated += 1
            conn.commit()
            return jsonify({
                "status": "success",
                "data": {"inserted": inserted, "updated": updated, "errors": errors},
            })
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as e:
        print(f"[import_ad_spend] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/refund/import', methods=['POST'])
@login_required
@permission_required('reports:edit')
def import_refund_records():
    """
    导入退款明细

    简介: 批量上传退款记录到 amazon_refund_records 表，支持单条或数组。

    请求体 (JSON):
        records: [ { shop_id, amazon_order_id, refund_date, refund_amount, ... }, ... ]
    """
    try:
        data = request.get_json() or {}
        records = data.get('records', [])
        if not records:
            return jsonify({"status": "error", "message": "records 不能为空"}), 400
        if isinstance(records, dict):
            records = [records]

        conn = _get_conn()
        inserted = 0
        updated = 0
        errors = []
        try:
            with conn.cursor() as cursor:
                for rec in records:
                    required = ['shop_id', 'amazon_order_id', 'refund_date', 'refund_amount']
                    missing = [r for r in required if r not in rec]
                    if missing:
                        errors.append({"record": rec, "error": f"缺少 {', '.join(missing)}"})
                        continue

                    cursor.execute("""
                        INSERT INTO amazon_refund_records (
                            shop_id, amazon_order_id, order_item_id, asin, sku,
                            refund_date, refund_amount, refund_quantity, refund_reason
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            refund_amount = VALUES(refund_amount),
                            refund_quantity = VALUES(refund_quantity),
                            refund_reason = VALUES(refund_reason),
                            asin = VALUES(asin),
                            sku = VALUES(sku),
                            updated_at = NOW()
                    """, (
                        rec['shop_id'], rec['amazon_order_id'],
                        rec.get('order_item_id', ''), rec.get('asin', ''), rec.get('sku', ''),
                        rec['refund_date'], rec['refund_amount'],
                        rec.get('refund_quantity', 0), rec.get('refund_reason', ''),
                    ))
                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        updated += 1
            conn.commit()
            return jsonify({
                "status": "success",
                "data": {"inserted": inserted, "updated": updated, "errors": errors},
            })
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as e:
        print(f"[import_refund_records] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 5. 报表生成日志
# ============================================================

@reports_bp.route('/reports/generation-logs', methods=['GET'])
@login_required
@permission_required('reports:page')
def list_generation_logs():
    """
    查询报表生成日志

    简介: 查看历史报表生成记录，按类型/状态过滤，用于排查生成失败原因。
    """
    try:
        report_type = request.args.get('report_type', '').strip() or None
        status = request.args.get('status', '').strip() or None
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if report_type:
                where_clauses.append("report_type = %s")
                params.append(report_type)
            if status:
                where_clauses.append("status = %s")
                params.append(status)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM report_generation_log {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT * FROM report_generation_log
                    {where_sql}
                    ORDER BY started_at DESC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {
                    "list": _to_json_serializable(rows),
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_generation_logs] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 6. 广告效果报表
# ============================================================

@reports_bp.route('/reports/advertising', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def list_advertising_reports():
    """
    分页查询广告效果报表

    简介: 按类型/维度/日期范围查询广告表现数据（曝光/点击/花费/ACOS/ROAS）。
    """
    try:
        report_type = request.args.get('type', '').strip() or None
        dimension_type = request.args.get('dimension', '').strip() or None
        campaign_id = request.args.get('campaign_id', '').strip() or None
        ad_group_id = request.args.get('ad_group_id', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if report_type:
                where_clauses.append("report_type = %s")
                params.append(report_type)
            if dimension_type:
                where_clauses.append("dimension_type = %s")
                params.append(dimension_type)
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if campaign_id:
                where_clauses.append("campaign_id = %s")
                params.append(campaign_id)
            if ad_group_id:
                where_clauses.append("ad_group_id = %s")
                params.append(ad_group_id)
            if asin:
                where_clauses.append("asin = %s")
                params.append(asin)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM report_advertising {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT * FROM report_advertising
                    {where_sql}
                    ORDER BY report_date DESC, dimension_type, ad_spend DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {
                    "list": _to_json_serializable(rows),
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_advertising_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/summary', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_summary():
    """
    广告效果汇总统计

    简介: 对筛选范围内的广告数据做 SUM 聚合，返回总量和平均指标。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        dimension_type = request.args.get('dimension', '').strip() or None
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = %s"]
            params = [report_type]
            if dimension_type:
                where_clauses.append("dimension_type = %s")
                params.append(dimension_type)
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) AS record_count,
                        SUM(impressions) AS total_impressions,
                        SUM(clicks) AS total_clicks,
                        SUM(ad_spend) AS total_ad_spend,
                        SUM(orders_7d) AS total_orders_7d,
                        SUM(orders_30d) AS total_orders_30d,
                        SUM(sales_7d) AS total_sales_7d,
                        SUM(sales_30d) AS total_sales_30d,
                        AVG(ctr) AS avg_ctr,
                        AVG(cpc) AS avg_cpc,
                        AVG(acos_7d) AS avg_acos_7d,
                        AVG(acos_30d) AS avg_acos_30d,
                        AVG(roas_7d) AS avg_roas_7d,
                        AVG(roas_30d) AS avg_roas_30d
                    FROM report_advertising
                    {where_sql}
                """, params)
                summary = cursor.fetchone()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(summary),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_summary] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/trend', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_trend():
    """
    广告效果趋势

    简介: 按时间维度返回核心广告指标走势（曝光/点击/花费/ACOS/ROAS）。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        dimension_type = request.args.get('dimension', '').strip() or 'overall'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = %s", "dimension_type = %s"]
            params = [report_type, dimension_type]
            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            if report_type == 'daily':
                time_col = 'report_date'
                order_sql = f"ORDER BY {time_col}"
            elif report_type == 'weekly':
                time_col = 'report_week'
                order_sql = "ORDER BY STR_TO_DATE(SUBSTRING_INDEX(report_week, '~', 1), '%%Y.%%m.%%d')"
            else:
                time_col = 'report_month'
                order_sql = f"ORDER BY {time_col}"

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        {time_col} AS time_label,
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks,
                        SUM(ad_spend) AS ad_spend,
                        SUM(orders_7d) AS orders_7d,
                        SUM(sales_7d) AS sales_7d,
                        AVG(ctr) AS ctr,
                        AVG(cpc) AS cpc,
                        AVG(acos_7d) AS acos_7d,
                        AVG(roas_7d) AS roas_7d
                    FROM report_advertising
                    {where_sql}
                    GROUP BY {time_col}
                    {order_sql}
                """, params)
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": _to_json_serializable(rows),
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_trend] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/generate', methods=['POST'])
@login_required
@permission_required('reports_advertising:generate')
def trigger_advertising_report():
    """
    手动触发广告效果报表生成

    请求体 (JSON):
        report_type   (必填) daily / weekly / monthly
        period        (daily/monthly 必填) 日期
        period_start  (weekly 必填) 周开始日期
        period_end    (weekly 必填) 周结束日期
        shop_id       (可选) 指定店铺
    """
    try:
        data = request.get_json() or {}
        report_type = data.get('report_type', '').strip()
        shop_id = data.get('shop_id')

        if report_type not in ('daily', 'weekly', 'monthly'):
            return jsonify({"status": "error", "message": "report_type 必须是 daily/weekly/monthly"}), 400

        if report_type == 'daily':
            period = data.get('period', '').strip()
            if not period:
                return jsonify({"status": "error", "message": "daily 类型需要 period 参数"}), 400
            result = generate_advertising_daily(period, shop_id)
        elif report_type == 'weekly':
            period_start = data.get('period_start', '').strip()
            period_end = data.get('period_end', '').strip()
            if not period_start or not period_end:
                return jsonify({"status": "error", "message": "weekly 类型需要 period_start 和 period_end"}), 400
            result = generate_advertising_weekly(period_start, period_end, shop_id)
        else:
            period = data.get('period', '').strip()
            if not period:
                return jsonify({"status": "error", "message": "monthly 类型需要 period 参数"}), 400
            result = generate_advertising_monthly(period, shop_id)

        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_advertising_report] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 7. 一键生成昨日全部报表
# ============================================================

@reports_bp.route('/reports/generate-yesterday', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_yesterday_reports():
    """
    一键生成昨日全部报表

    简介: 同时执行 T-1/T-2 预估日报 + T-3 已结算日报 + SKU利润 + 库存周转 + 广告日报，
          并检查是否需要生周报（周三）或月报（3号）。
    """
    try:
        results = generate_yesterday_reports()
        return jsonify({"status": "success", "message": "报表生成完成", "data": results})
    except Exception as e:
        print(f"[trigger_yesterday_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
