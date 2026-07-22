"""
报表模块 — 数据报表管理

简介: 提供经营报表、SKU 利润、库存周转、广告效果等模块的查询与生成接口。

前端接口:
  经营报表:
    GET    /api/reports/business                   分页查询经营报表（日/周/月，实时聚合日报）
    GET    /api/reports/business/summary            经营报表汇总统计
    GET    /api/reports/business/trend              经营趋势（销售额/毛利走势）
    POST   /api/reports/business/generate           手动生成经营日报
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
  SKU 销售数据:
    GET    /api/reports/sku-sales                    分页查询 SKU 销售数据列表
    POST   /api/reports/sku-sales/generate           手动触发全量生成
    POST   /api/reports/sku-sales/generate/<sku>     手动触发单个 SKU 生成
  数据导入:
    POST   /api/reports/ad-spend/import              导入广告费明细
    POST   /api/reports/refund/import                导入退款明细
  生成日志:
    GET    /api/reports/generation-logs              查询报表生成日志
  广告效果报表:
    GET    /api/reports/advertising                  分页查询广告效果报表（日/周/月，实时聚合日报）
    GET    /api/reports/advertising/summary          广告效果汇总统计
    GET    /api/reports/advertising/trend            广告效果趋势
    POST   /api/reports/advertising/generate         手动生成广告日报
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
    generate_sku_profit,
    generate_sku_sales,
    generate_inventory_turnover,
    generate_yesterday_reports,
    generate_advertising_daily,
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


def _normalize_ad_percentages(nodes):
    """递归将树形数据中的 ctr/acos 从小数转为百分比（×100）"""
    for node in nodes:
        for pct_field in ('ctr', 'acos_7d', 'acos_30d'):
            v = node.get(pct_field)
            if v is not None:
                try:
                    node[pct_field] = round(float(v) * 100, 4)
                except (ValueError, TypeError):
                    pass
        children = node.get('children', [])
        if children:
            _normalize_ad_percentages(children)


def _normalize_ad_percentages_flat(rows):
    """将平铺列表中的 ctr/acos 从小数转为百分比（×100）"""
    for row in rows:
        for pct_field in ('ctr', 'acos_7d', 'acos_30d'):
            v = row.get(pct_field)
            if v is not None:
                try:
                    row[pct_field] = round(float(v) * 100, 4)
                except (ValueError, TypeError):
                    pass


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
    周报/月报实时从日报数据聚合，不再预生成。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            if report_type in ('weekly', 'monthly'):
                # 实时聚合
                if report_type == 'weekly':
                    group_expr = "YEARWEEK(report_date, 1)"
                    label_expr = "CONCAT(MIN(report_date), '~', MAX(report_date)) AS report_week"
                    order_by = "ORDER BY MIN(report_date) DESC"
                else:
                    group_expr = "DATE_FORMAT(report_date, '%%Y-%%m')"
                    label_expr = "DATE_FORMAT(MIN(report_date), '%%Y-%%m') AS report_month"
                    order_by = "ORDER BY MIN(report_date) DESC"

                where_clauses = ["report_type = 'daily'"]
                params = []
                if shop_id is not None:
                    where_clauses.append("shop_id = %s"); params.append(shop_id)
                if start_date:
                    where_clauses.append("report_date >= %s"); params.append(start_date)
                if end_date:
                    where_clauses.append("report_date <= %s"); params.append(end_date)
                where_sql = "WHERE " + " AND ".join(where_clauses)

                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT {group_expr}) AS total
                        FROM report_business
                        {where_sql}
                    """, params)
                    total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT
                            {label_expr},
                            SUM(order_count) AS order_count,
                            SUM(total_sales) AS total_sales,
                            SUM(gross_profit) AS gross_profit,
                            SUM(product_cost) AS product_cost,
                            SUM(fba_fees) AS fba_fees,
                            SUM(platform_fees) AS platform_fees,
                            SUM(headway_cost) AS headway_cost,
                            SUM(ad_cost) AS ad_cost,
                            SUM(refund_amount) AS refund_amount,
                            SUM(total_cost) AS total_cost,
                            SUM(sku_count) AS sku_count,
                            CASE WHEN SUM(total_sales) > 0 THEN SUM(gross_profit) / SUM(total_sales) * 100 ELSE 0 END AS profit_margin
                        FROM report_business
                        {where_sql}
                        GROUP BY {group_expr}
                        {order_by}
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
            else:
                # daily: 直接查表
                where_clauses = []
                params = []
                if report_type:
                    where_clauses.append("report_type = %s"); params.append(report_type)
                if shop_id is not None:
                    where_clauses.append("shop_id = %s"); params.append(shop_id)
                if start_date:
                    where_clauses.append("report_date >= %s"); params.append(start_date)
                if end_date:
                    where_clauses.append("report_date <= %s"); params.append(end_date)
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) AS total FROM report_business {where_sql}", params)
                    total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT * FROM report_business
                        {where_sql}
                        ORDER BY report_date DESC, id DESC
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
        print(f"[list_business_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/summary', methods=['GET'])
@login_required
@permission_required('reports:page')
def business_summary():
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) AS record_count,
                        SUM(total_sales) AS sum_sales,
                        SUM(total_cost) AS sum_total_cost,
                        SUM(gross_profit) AS sum_gross_profit,
                        AVG(gross_profit_rate) AS avg_gross_profit_rate,
                        SUM(headway_cost) AS sum_headway_cost,
                        AVG(headway_ratio) AS avg_headway_ratio,
                        SUM(order_count) AS sum_orders,
                        SUM(ad_cost) AS sum_ad_cost,
                        SUM(refund_amount) AS sum_refund,
                        AVG(refund_rate) AS avg_refund_rate,
                        SUM(fba_fees) AS sum_fba_fees,
                        SUM(platform_fees) AS sum_platform_fees,
                        SUM(product_cost) AS sum_product_cost
                    FROM report_business
                    {where_sql}
                """, params)
                summary = cursor.fetchone()

            d = dict(summary) if summary else {}
            d["cost_breakdown"] = {
                "ad_cost": float(d.get("sum_ad_cost") or 0),
                "fba_fees": float(d.get("sum_fba_fees") or 0),
                "platform_fees": float(d.get("sum_platform_fees") or 0),
                "product_cost": float(d.get("sum_product_cost") or 0),
                "headway_cost": float(d.get("sum_headway_cost") or 0),
                "refund_amount": float(d.get("sum_refund") or 0),
            }

            return jsonify({"status": "success", "data": _to_json_serializable(d)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[business_summary] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/cost-breakdown', methods=['GET'])
@login_required
@permission_required('reports:page')
def business_cost_breakdown():
    try:
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        SUM(total_cost) AS total_cost,
                        SUM(ad_cost) AS ad_cost,
                        SUM(fba_fees) AS fba_fees,
                        SUM(platform_fees) AS platform_fees,
                        SUM(product_cost) AS product_cost,
                        SUM(headway_cost) AS headway_cost,
                        SUM(refund_amount) AS refund_amount
                    FROM report_business
                    {where_sql}
                """, params)
                row = cursor.fetchone()

            d = dict(row) if row else {}
            return jsonify({"status": "success", "data": _to_json_serializable(d)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[business_cost_breakdown] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/business/trend', methods=['GET'])
@login_required
@permission_required('reports:page')
def business_trend():
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            if report_type == 'weekly':
                time_col = "CONCAT(MIN(report_date), '~', MAX(report_date)) AS time_label"
                group_expr = "YEARWEEK(report_date, 1)"
                order_sql = "ORDER BY MIN(report_date)"
            elif report_type == 'monthly':
                time_col = "DATE_FORMAT(MIN(report_date), '%%Y-%%m') AS time_label"
                group_expr = "DATE_FORMAT(report_date, '%%Y-%%m')"
                order_sql = "ORDER BY MIN(report_date)"
            else:
                time_col = "report_date AS time_label"
                group_expr = "report_date"
                order_sql = "ORDER BY report_date"

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT {time_col},
                        SUM(total_sales) AS total_sales,
                        SUM(gross_profit) AS gross_profit,
                        AVG(gross_profit_rate) AS gross_profit_rate,
                        SUM(headway_cost) AS headway_cost,
                        AVG(headway_ratio) AS headway_ratio,
                        SUM(ad_cost) AS ad_cost,
                        SUM(order_count) AS order_count
                    FROM report_business
                    {where_sql}
                    GROUP BY {group_expr}
                    {order_sql}
                """, params)
                rows = cursor.fetchall()

            return jsonify({"status": "success", "data": _to_json_serializable(rows)})
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
    手动触发经营日报生成

    请求体 (JSON):
        period   (必填) 日期，如 2026-05-18
        shop_id  (可选) 指定店铺，不传则所有店铺
    """
    try:
        data = request.get_json() or {}
        period = data.get('period', '').strip()
        shop_id = data.get('shop_id')

        if not period:
            return jsonify({"status": "error", "message": "period 参数必填（如 2026-05-18）"}), 400

        result = generate_business_daily(period, shop_id)
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
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
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
                    ORDER BY report_date DESC, net_profit DESC
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
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
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
                        SUM(headway_cost) AS total_headway_cost,
                        SUM(ad_sales) AS total_ad_sales,
                        SUM(ad_orders) AS total_ad_orders,
                        CASE WHEN SUM(ad_sales) > 0 THEN SUM(ad_cost) / SUM(ad_sales) ELSE 0 END AS avg_ad_acos,
                        CASE WHEN SUM(sales_amount) > 0 THEN SUM(ad_cost) / SUM(sales_amount) ELSE 0 END AS tacos
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

        allowed_sort = {'net_profit', 'profit_margin', 'sales_amount', 'gross_profit', 'sales_qty'}
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
                where_clauses.append("report_date >= %s")
                params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s")
                params.append(end_date)

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        asin,
                        sku,
                        product_name,
                        SUM(sales_qty) AS sales_qty,
                        SUM(sales_amount) AS sales_amount,
                        SUM(net_profit) AS net_profit,
                        AVG(profit_margin) AS profit_margin,
                        SUM(gross_profit) AS gross_profit,
                        SUM(ad_cost) AS ad_cost,
                        SUM(ad_sales) AS ad_sales,
                        SUM(ad_orders) AS ad_orders,
                        CASE WHEN SUM(ad_sales) > 0 THEN SUM(ad_cost) / SUM(ad_sales) ELSE 0 END AS ad_acos,
                        CASE WHEN SUM(sales_amount) > 0 THEN SUM(ad_cost) / SUM(sales_amount) ELSE 0 END AS tacos
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


@reports_bp.route('/reports/sku-profit/aggregate', methods=['GET'])
@login_required
@permission_required('reports:page')
def sku_profit_aggregate():
    try:
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        keyword = request.args.get('keyword', '').strip() or None
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = []
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            if keyword:
                where_clauses.append("(asin LIKE %s OR sku LIKE %s OR product_name LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT CONCAT(asin, '|', sku)) AS total
                    FROM sku_profit
                    {where_sql}
                """, params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        sku,
                        asin,
                        product_name,
                        SUM(sales_qty) AS sales_qty,
                        SUM(sales_amount) AS sales_amount,
                        CASE WHEN SUM(sales_qty) > 0 THEN SUM(sales_amount) / SUM(sales_qty) ELSE 0 END AS avg_selling_price,
                        SUM(product_cost) AS product_cost,
                        SUM(fba_fees) AS fba_fees,
                        SUM(platform_fees) AS platform_fees,
                        SUM(ad_cost) AS ad_cost,
                        SUM(headway_cost) AS headway_cost,
                        SUM(refund_amount) AS refund_amount,
                        SUM(gross_profit) AS gross_profit,
                        SUM(net_profit) AS net_profit,
                        CASE WHEN SUM(sales_amount) > 0 THEN SUM(net_profit) / SUM(sales_amount) ELSE 0 END AS profit_margin,
                        SUM(ad_sales) AS ad_sales,
                        SUM(ad_orders) AS ad_orders,
                        CASE WHEN SUM(ad_sales) > 0 THEN SUM(ad_cost) / SUM(ad_sales) ELSE 0 END AS ad_acos,
                        CASE WHEN SUM(sales_amount) > 0 THEN SUM(ad_cost) / SUM(sales_amount) ELSE 0 END AS tacos
                    FROM sku_profit
                    {where_sql}
                    GROUP BY asin, sku, product_name
                    ORDER BY net_profit DESC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {"list": _to_json_serializable(rows), "total": total}
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[sku_profit_aggregate] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-profit/generate', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_sku_profit():
    """
    手动触发 SKU 利润表生成（按天生成，支持日期范围）
    """
    try:
        data = request.get_json() or {}
        period_start = data.get('period_start', '').strip() or data.get('report_date', '').strip()
        period_end = data.get('period_end', '').strip() or period_start
        shop_id = data.get('shop_id')

        if not period_start:
            return jsonify({"status": "error", "message": "period_start 或 report_date 必填"}), 400

        from datetime import timedelta
        start = datetime.strptime(period_start, '%Y-%m-%d')
        end = datetime.strptime(period_end, '%Y-%m-%d')
        d = start
        total = 0
        while d <= end:
            result = generate_sku_profit(d.strftime('%Y-%m-%d'), shop_id)
            total += result.get('affected_rows', 0)
            d += timedelta(days=1)

        return jsonify({"status": "success", "message": f"完成 {total} 条", "data": result})
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
                    ORDER BY sales_30d DESC, turnover_days ASC
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


@reports_bp.route('/reports/inventory-turnover/by-status', methods=['GET'])
@login_required
@permission_required('reports:page')
def inventory_turnover_by_status():
    try:
        status = request.args.get('status', '').strip()
        keyword = request.args.get('keyword', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        if not status:
            return jsonify({"status": "error", "message": "缺少 status 参数"}), 400

        conn = _get_conn()
        try:
            where_clauses = ["stock_status = %s"]
            params = [status]
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if keyword:
                where_clauses.append("(sku LIKE %s OR asin LIKE %s OR product_name LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM inventory_turnover {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT sku, asin, product_name, current_stock, turnover_days,
                           last_sale_date, days_without_sale
                    FROM inventory_turnover
                    {where_sql}
                    ORDER BY turnover_days DESC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {"list": _to_json_serializable(rows), "total": total}
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[inventory_turnover_by_status] error: {e}")
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
# 3.5 SKU 销售数据
# ============================================================

@reports_bp.route('/reports/sku-sales', methods=['GET'])
@login_required
@permission_required('reports:page')
def list_sku_sales():
    """
    分页查询 SKU 销售数据列表

    查询参数:
        keyword     (可选) 模糊搜索 ASIN/SKU/品名
        sku         (可选) 精确 SKU
        shop_id     (可选) 店铺ID
        sort_by     (可选) 排序字段，默认 sales_30d
        sort_dir    (可选) asc/desc，默认 desc
        page        (可选) 页码，默认 1
        page_size   (可选) 每页条数，默认 20，最大 100
    """
    try:
        keyword = request.args.get('keyword', '').strip() or None
        sku = request.args.get('sku', '').strip() or None
        shop_id = _get_shop_id_optional()
        sort_by = request.args.get('sort_by', 'sales_30d').strip()
        sort_dir = request.args.get('sort_dir', 'desc').strip().lower()
        page, page_size = _parse_pagination()

        allowed_sorts = {
            'stock',
            'total_revenue_1d', 'total_revenue_3d', 'total_revenue_7d', 'total_revenue_14d', 'total_revenue_30d',
            'sales_1d', 'sales_3d', 'sales_7d', 'sales_14d', 'sales_30d',
            'sales_ad_1d', 'sales_ad_3d', 'sales_ad_7d', 'sales_ad_14d', 'sales_ad_30d',
            'sales_natural_1d', 'sales_natural_3d', 'sales_natural_7d', 'sales_natural_14d', 'sales_natural_30d',
            'ad_revenue_1d', 'ad_revenue_3d', 'ad_revenue_7d', 'ad_revenue_14d', 'ad_revenue_30d',
            'natural_revenue_1d', 'natural_revenue_3d', 'natural_revenue_7d', 'natural_revenue_14d', 'natural_revenue_30d',
            'ad_cost_1d', 'ad_cost_3d', 'ad_cost_7d', 'ad_cost_14d', 'ad_cost_30d',
            'cpc', 'cvr', 'acos', 'tacos',
            'sell_price', 'promo_price',
            'profit_1d', 'profit_rate_1d', 'profit_3d', 'profit_rate_3d',
            'profit_7d', 'profit_rate_7d', 'profit_14d', 'profit_rate_14d',
            'profit_30d', 'profit_rate_30d',
            'report_date',
        }
        if sort_by not in allowed_sorts:
            sort_by = 'sales_30d'
        if sort_dir not in ('asc', 'desc'):
            sort_dir = 'desc'

        conn = _get_conn()
        try:
            where_clauses = ["report_date = (SELECT MAX(report_date) FROM report_sku_sales AS sub WHERE sub.shop_id = report_sku_sales.shop_id AND sub.sku = report_sku_sales.sku)"]
            params = []

            if shop_id is not None:
                where_clauses.append("shop_id = %s")
                params.append(shop_id)
            if keyword:
                where_clauses.append("(asin LIKE %s OR sku LIKE %s OR product_name LIKE %s)")
                params.extend([f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"])
            if sku:
                where_clauses.append("sku = %s")
                params.append(sku)

            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM report_sku_sales {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                sql = f"""
                    SELECT * FROM report_sku_sales
                    {where_sql}
                    ORDER BY {sort_by} {sort_dir}
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, params + [page_size, offset])
                rows = cursor.fetchall()

            for row in rows:
                for k in ('report_date', 'created_at', 'updated_at'):
                    if k in row and row[k] is not None:
                        row[k] = str(row[k])
                decimal_cols = (
                    'total_revenue_1d', 'total_revenue_3d', 'total_revenue_7d', 'total_revenue_14d', 'total_revenue_30d',
                    'ad_revenue_1d', 'ad_revenue_3d', 'ad_revenue_7d', 'ad_revenue_14d', 'ad_revenue_30d',
                    'natural_revenue_1d', 'natural_revenue_3d', 'natural_revenue_7d', 'natural_revenue_14d', 'natural_revenue_30d',
                    'ad_cost_1d', 'ad_cost_3d', 'ad_cost_7d', 'ad_cost_14d', 'ad_cost_30d',
                    'cpc', 'cvr', 'acos', 'tacos',
                    'sell_price', 'promo_price',
                    'profit_1d', 'profit_rate_1d', 'profit_3d', 'profit_rate_3d',
                    'profit_7d', 'profit_rate_7d', 'profit_14d', 'profit_rate_14d',
                    'profit_30d', 'profit_rate_30d',
                )
                for k in decimal_cols:
                    if k in row and row[k] is not None:
                        row[k] = float(row[k])

            return jsonify({
                "status": "success",
                "data": {
                    "list": rows,
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                }
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[list_sku_sales] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-sales/generate', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_sku_sales():
    """
    手动触发 SKU 销售数据全量生成

    请求体 (JSON):
        report_date  (可选) 报告生成日期（PDT时间），默认 PDT 昨天，格式 YYYY-MM-DD
        shop_id      (可选) 指定店铺
    """
    try:
        data = request.get_json() or {}
        report_date = data.get('report_date', '').strip() or None
        shop_id = data.get('shop_id')

        if not report_date:
            from datetime import timezone
            pdt_today = datetime.now(timezone(timedelta(hours=-7))).date()
            report_date = (pdt_today - timedelta(days=1)).strftime('%Y-%m-%d')

        result = generate_sku_sales(report_date, shop_id)
        return jsonify({"status": "success", "message": f"生成完成，{result['affected_rows']} 条", "data": result})
    except Exception as e:
        print(f"[trigger_sku_sales] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/sku-sales/generate/<sku>', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_sku_sales_single(sku):
    """
    手动触发单个 SKU 的销售数据生成

    请求体 (JSON):
        report_date  (可选) 报告生成日期（PDT时间），默认 PDT 昨天
        shop_id      (必填) 店铺ID
    """
    try:
        data = request.get_json() or {}
        report_date = data.get('report_date', '').strip() or None
        shop_id = data.get('shop_id')

        if not shop_id:
            return jsonify({"status": "error", "message": "shop_id 必填"}), 400
        if not report_date:
            from datetime import timezone
            pdt_today = datetime.now(timezone(timedelta(hours=-7))).date()
            report_date = (pdt_today - timedelta(days=1)).strftime('%Y-%m-%d')

        # 限制生成范围到指定 SKU
        result = generate_sku_sales(report_date, shop_id, sku_filter=sku)
        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_sku_sales_single] error: {e}")
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
    周报/月报实时从日报数据聚合，不再预生成。
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            if report_type in ('weekly', 'monthly'):
                if report_type == 'weekly':
                    group_expr = "YEARWEEK(report_date, 1)"
                    label_expr = "CONCAT(MIN(report_date), '~', MAX(report_date)) AS report_week"
                    order_by = "ORDER BY MIN(report_date) DESC"
                else:
                    group_expr = "DATE_FORMAT(report_date, '%%Y-%%m')"
                    label_expr = "DATE_FORMAT(MIN(report_date), '%%Y-%%m') AS report_month"
                    order_by = "ORDER BY MIN(report_date) DESC"

                where_clauses = ["report_type = 'daily'"]
                params = []
                if shop_id is not None:
                    where_clauses.append("shop_id = %s"); params.append(shop_id)
                if start_date:
                    where_clauses.append("report_date >= %s"); params.append(start_date)
                if end_date:
                    where_clauses.append("report_date <= %s"); params.append(end_date)
                where_sql = "WHERE " + " AND ".join(where_clauses)

                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(DISTINCT {group_expr}) AS total FROM report_advertising {where_sql}", params)
                    total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT
                            {label_expr},
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(ad_spend) AS ad_spend,
                            SUM(orders_7d) AS orders_7d,
                            SUM(sales_7d) AS sales_7d,
                            CASE WHEN SUM(clicks) > 0 THEN SUM(ad_spend) / SUM(clicks) ELSE 0 END AS cpc,
                            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) / SUM(impressions) * 100 ELSE 0 END AS ctr,
                            CASE WHEN SUM(sales_7d) > 0 THEN SUM(ad_spend) / SUM(sales_7d) * 100 ELSE 0 END AS acos,
                            CASE WHEN SUM(ad_spend) > 0 THEN SUM(sales_7d) / SUM(ad_spend) ELSE 0 END AS roas
                        FROM report_advertising
                        {where_sql}
                        GROUP BY {group_expr}
                        {order_by}
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
            else:
                # daily: 直接查表
                where_clauses = []
                params = []
                if report_type:
                    where_clauses.append("report_type = %s"); params.append(report_type)
                if shop_id is not None:
                    where_clauses.append("shop_id = %s"); params.append(shop_id)
                if start_date:
                    where_clauses.append("report_date >= %s"); params.append(start_date)
                if end_date:
                    where_clauses.append("report_date <= %s"); params.append(end_date)
                where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) AS total FROM report_advertising {where_sql}", params)
                    total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT * FROM report_advertising
                        {where_sql}
                        ORDER BY report_date DESC, dimension_type, ad_spend DESC
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
        print(f"[list_advertising_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/summary', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_summary():
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        campaign_id = request.args.get('campaign_id', '').strip() or None
        ad_group_id = request.args.get('ad_group_id', '').strip() or None
        advertised_asin = request.args.get('advertised_asin', '').strip() or None

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'", "dimension_type = 'overall'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            if campaign_id:
                where_clauses.append("campaign_id = %s"); params.append(campaign_id)
            if ad_group_id:
                where_clauses.append("ad_group_id = %s"); params.append(ad_group_id)
            if advertised_asin:
                where_clauses.append("asin = %s"); params.append(advertised_asin)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT
                        COUNT(*) AS record_count,
                        SUM(impressions) AS total_impressions,
                        SUM(clicks) AS total_clicks,
                        SUM(ad_spend) AS total_ad_spend,
                        SUM(orders_7d) AS total_orders_7d,
                        SUM(sales_7d) AS total_sales_7d,
                        CASE WHEN SUM(sales_7d) > 0 THEN SUM(ad_spend) / SUM(sales_7d) * 100 ELSE 0 END AS avg_acos_7d,
                        CASE WHEN SUM(ad_spend) > 0 THEN SUM(sales_7d) / SUM(ad_spend) ELSE 0 END AS avg_roas_7d,
                        CASE WHEN SUM(clicks) > 0 THEN SUM(ad_spend) / SUM(clicks) ELSE 0 END AS avg_cpc,
                        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) / SUM(impressions) * 100 ELSE 0 END AS avg_ctr
                    FROM report_advertising
                    {where_sql}
                """, params)
                summary = cursor.fetchone()

            return jsonify({"status": "success", "data": _to_json_serializable(summary)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_summary] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/trend', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_trend():
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        campaign_id = request.args.get('campaign_id', '').strip() or None
        ad_group_id = request.args.get('ad_group_id', '').strip() or None
        advertised_asin = request.args.get('advertised_asin', '').strip() or None

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            if campaign_id and not ad_group_id and not advertised_asin:
                where_clauses.append("campaign_id = %s"); params.append(campaign_id)
            elif ad_group_id and not advertised_asin:
                where_clauses.append("ad_group_id = %s"); params.append(ad_group_id)
            elif advertised_asin:
                where_clauses.append("asin = %s"); params.append(advertised_asin)
            else:
                where_clauses.append("dimension_type = 'overall'")
            where_sql = "WHERE " + " AND ".join(where_clauses)

            if report_type == 'weekly':
                time_col = "CONCAT(MIN(report_date), '~', MAX(report_date)) AS time_label"
                group_expr = "YEARWEEK(report_date, 1)"
                order_sql = "ORDER BY MIN(report_date)"
            elif report_type == 'monthly':
                time_col = "DATE_FORMAT(MIN(report_date), '%%Y-%%m') AS time_label"
                group_expr = "DATE_FORMAT(report_date, '%%Y-%%m')"
                order_sql = "ORDER BY MIN(report_date)"
            else:
                time_col = "report_date AS time_label"
                group_expr = "report_date"
                order_sql = "ORDER BY report_date"

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT {time_col},
                        SUM(impressions) AS impressions,
                        SUM(clicks) AS clicks,
                        SUM(ad_spend) AS ad_spend,
                        SUM(orders_7d) AS orders_7d,
                        SUM(sales_7d) AS sales_7d,
                        CASE WHEN SUM(clicks) > 0 THEN SUM(ad_spend) / SUM(clicks) ELSE 0 END AS cpc,
                        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) / SUM(impressions) * 100 ELSE 0 END AS ctr,
                        CASE WHEN SUM(sales_7d) > 0 THEN SUM(ad_spend) / SUM(sales_7d) * 100 ELSE 0 END AS acos,
                        CASE WHEN SUM(ad_spend) > 0 THEN SUM(sales_7d) / SUM(ad_spend) ELSE 0 END AS roas
                    FROM report_advertising
                    {where_sql}
                    GROUP BY {group_expr}
                    {order_sql}
                """, params)
                rows = cursor.fetchall()

            return jsonify({"status": "success", "data": _to_json_serializable(rows)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_trend] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/daily/tree', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_daily_tree():
    """一次性返回当前页完整四层树形数据（日报/周报/月报统一入口）：
    日期(周/月) → 活动 → 广告组 → 商品
    """
    try:
        report_type = request.args.get('type', '').strip() or 'daily'
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        # 周/月聚合参数
        if report_type == 'weekly':
            group_expr = "YEARWEEK(report_date, 1)"
            label_expr = "CONCAT(MIN(report_date), '~', MAX(report_date)) AS report_date"
            order_by = "ORDER BY MIN(report_date) DESC"
            count_expr = "COUNT(DISTINCT YEARWEEK(report_date, 1))"
        elif report_type == 'monthly':
            group_expr = "DATE_FORMAT(report_date, '%%Y-%%m')"
            label_expr = "CONCAT(DATE_FORMAT(MIN(report_date), '%%Y-%%m'), '') AS report_date"
            order_by = "ORDER BY MIN(report_date) DESC"
            count_expr = "COUNT(DISTINCT DATE_FORMAT(report_date, '%%Y-%%m'))"
        else:
            group_expr = "report_date"
            label_expr = "report_date"
            order_by = "ORDER BY report_date DESC"
            count_expr = "COUNT(DISTINCT report_date)"

        conn = _get_conn()
        try:
            # 1. 第一层（分页）
            where = ["report_type = 'daily'", "dimension_type = 'overall'"]
            params = []
            if shop_id is not None:
                where.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where.append("report_date <= %s"); params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where)

            with conn.cursor() as c:
                c.execute(f"SELECT {count_expr} AS total FROM report_advertising {where_sql}", params)
                total = c.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as c:
                c.execute(f"""
                    SELECT {label_expr},
                           {f'{group_expr} AS _group,' if report_type != 'daily' else ''}
                           SUM(impressions) impressions, SUM(clicks) clicks, SUM(ad_spend) ad_spend,
                           CASE WHEN SUM(clicks)>0 THEN SUM(ad_spend)/SUM(clicks) ELSE 0 END cpc,
                           CASE WHEN SUM(impressions)>0 THEN SUM(clicks)/SUM(impressions)*100 ELSE 0 END ctr,
                           SUM(orders_7d) orders_7d, SUM(sales_7d) sales_7d,
                           CASE WHEN SUM(sales_7d)>0 THEN SUM(ad_spend)/SUM(sales_7d)*100 ELSE 0 END acos_7d,
                           CASE WHEN SUM(ad_spend)>0 THEN SUM(sales_7d)/SUM(ad_spend) ELSE 0 END roas_7d,
                           SUM(orders_30d) orders_30d, SUM(sales_30d) sales_30d,
                           CASE WHEN SUM(sales_30d)>0 THEN SUM(ad_spend)/SUM(sales_30d)*100 ELSE 0 END acos_30d,
                           CASE WHEN SUM(ad_spend)>0 THEN SUM(sales_30d)/SUM(ad_spend) ELSE 0 END roas_30d
                    FROM report_advertising
                    {where_sql}
                    GROUP BY {group_expr}
                    {order_by}
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                dates = c.fetchall()

            if not dates:
                return jsonify({"status":"success","data":{"list":[],"total":0,"page":page,"page_size":page_size}})

            # 公共子层查询
            def _query_dimension(dimension_type, fields_extra="", group_cols="", join_extra=""):
                w = [f"report_type='daily'", f"dimension_type='{dimension_type}'"]
                p2 = []
                if shop_id is not None:
                    w.append("shop_id = %s"); p2.append(shop_id)
                if start_date:
                    w.append("report_date >= %s"); p2.append(start_date)
                if end_date:
                    w.append("report_date <= %s"); p2.append(end_date)
                ws = " AND ".join(w)
                extra = f", {fields_extra}" if fields_extra else ""
                grp = f"_group, {group_cols}" if group_cols else "_group"
                return f"""
                    SELECT {group_expr} AS _group{extra},
                           SUM(impressions) impressions, SUM(clicks) clicks, SUM(ad_spend) ad_spend,
                           CASE WHEN SUM(clicks)>0 THEN SUM(ad_spend)/SUM(clicks) ELSE 0 END cpc,
                           CASE WHEN SUM(impressions)>0 THEN SUM(clicks)/SUM(impressions)*100 ELSE 0 END ctr,
                           SUM(orders_7d) orders_7d, SUM(sales_7d) sales_7d,
                           CASE WHEN SUM(sales_7d)>0 THEN SUM(ad_spend)/SUM(sales_7d)*100 ELSE 0 END acos_7d,
                           CASE WHEN SUM(ad_spend)>0 THEN SUM(sales_7d)/SUM(ad_spend) ELSE 0 END roas_7d,
                           SUM(orders_30d) orders_30d, SUM(sales_30d) sales_30d,
                           CASE WHEN SUM(sales_30d)>0 THEN SUM(ad_spend)/SUM(sales_30d)*100 ELSE 0 END acos_30d,
                           CASE WHEN SUM(ad_spend)>0 THEN SUM(sales_30d)/SUM(ad_spend) ELSE 0 END roas_30d
                    FROM report_advertising{join_extra}
                    WHERE {ws}
                    GROUP BY {grp}
                """, p2

            # 2. 活动层
            sql_c, pc = _query_dimension('campaign', 'campaign_id, MAX(campaign_name) campaign_name', 'campaign_id')
            campaign_map = {}
            with conn.cursor() as c:
                c.execute(sql_c, pc)
                for r in c.fetchall():
                    grp = str(r.pop('_group'))
                    campaign_map.setdefault(grp, []).append(r)

            # 3. 广告组层
            sql_a, pa = _query_dimension('ad_group', 'campaign_id, ad_group_id, MAX(ad_group_name) ad_group_name', 'campaign_id, ad_group_id')
            ag_map = {}
            with conn.cursor() as c:
                c.execute(sql_a, pa)
                for r in c.fetchall():
                    grp = str(r.pop('_group'))
                    cid = r.pop('campaign_id')
                    ag_map.setdefault((grp, cid), []).append(r)

            # 4. 商品层
            prod_map = {}
            with conn.cursor() as c:
                c.execute(f"""
                    SELECT {group_expr} AS _group, r.campaign_id, r.ad_group_id,
                           r.asin advertised_asin, MAX(r.sku) advertised_sku,
                           COALESCE(MAX(p.product_name), '') product_name,
                           SUM(r.impressions) impressions, SUM(r.clicks) clicks, SUM(r.ad_spend) ad_spend,
                           CASE WHEN SUM(r.clicks)>0 THEN SUM(r.ad_spend)/SUM(r.clicks) ELSE 0 END cpc,
                           CASE WHEN SUM(r.impressions)>0 THEN SUM(r.clicks)/SUM(r.impressions)*100 ELSE 0 END ctr,
                           SUM(r.orders_7d) orders_7d, SUM(r.sales_7d) sales_7d,
                           CASE WHEN SUM(r.sales_7d)>0 THEN SUM(r.ad_spend)/SUM(r.sales_7d)*100 ELSE 0 END acos_7d,
                           CASE WHEN SUM(r.ad_spend)>0 THEN SUM(r.sales_7d)/SUM(r.ad_spend) ELSE 0 END roas_7d,
                           SUM(r.orders_30d) orders_30d, SUM(r.sales_30d) sales_30d,
                           CASE WHEN SUM(r.sales_30d)>0 THEN SUM(r.ad_spend)/SUM(r.sales_30d)*100 ELSE 0 END acos_30d,
                           CASE WHEN SUM(r.ad_spend)>0 THEN SUM(r.sales_30d)/SUM(r.ad_spend) ELSE 0 END roas_30d
                    FROM report_advertising r
                    LEFT JOIN products p ON p.seller_sku = r.sku AND p.status = 1
                    WHERE r.report_type='daily' AND r.dimension_type='asin'
                      AND r.asin != ''
                      {f"AND r.shop_id=%s" if shop_id is not None else ""}
                      {f"AND r.report_date>=%s" if start_date else ""}
                      {f"AND r.report_date<=%s" if end_date else ""}
                    GROUP BY _group, r.campaign_id, r.ad_group_id, r.asin
                    ORDER BY ad_spend DESC
                """, tuple(p for p in [shop_id, start_date, end_date] if p is not None))
                for r in c.fetchall():
                    grp = str(r.pop('_group'))
                    cid = r.pop('campaign_id')
                    agid = r.pop('ad_group_id')
                    prod_map.setdefault((grp, cid, agid), []).append(r)

            # 5. 组装树
            tree = []
            for date_row in dates:
                d = str(date_row.pop('_group', date_row['report_date']))
                date_node = dict(date_row)
                date_node['children'] = []
                for camp in campaign_map.get(d, []):
                    cid = camp['campaign_id']
                    camp_node = dict(camp)
                    camp_node['children'] = []
                    for ag in ag_map.get((d, cid), []):
                        agid = ag['ad_group_id']
                        ag_node = dict(ag)
                        ag_node['children'] = prod_map.get((d, cid, agid), [])
                        camp_node['children'].append(ag_node)
                    date_node['children'].append(camp_node)
                tree.append(date_node)

            return jsonify({
                "status": "success",
                "data": {"list": tree, "total": total, "page": page, "page_size": page_size}
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_daily_tree] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/daily', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_daily():
    """第一层：日期汇总列表（分页）"""
    try:
        start_date = request.args.get('start_date', '').strip() or None
        end_date = request.args.get('end_date', '').strip() or None
        shop_id = _get_shop_id_optional()
        page, page_size = _parse_pagination()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'", "dimension_type = 'overall'"]
            params = []
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            if start_date:
                where_clauses.append("report_date >= %s"); params.append(start_date)
            if end_date:
                where_clauses.append("report_date <= %s"); params.append(end_date)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) AS total FROM report_advertising {where_sql}", params)
                total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT report_date, impressions, clicks, ad_spend,
                           ctr, cpc, orders_7d, sales_7d, acos_7d, roas_7d,
                           orders_30d, sales_30d, acos_30d, roas_30d
                    FROM report_advertising
                    {where_sql}
                    ORDER BY report_date DESC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                rows = cursor.fetchall()

            return jsonify({
                "status": "success",
                "data": {"list": _to_json_serializable(rows), "total": total, "page": page, "page_size": page_size}
            })
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_daily] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/daily/<date>/campaigns', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_daily_campaigns(date):
    """第二层：指定日期下的广告活动列表"""
    try:
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'", "dimension_type = 'campaign'", "report_date = %s"]
            params = [date]
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT campaign_id, campaign_name,
                           impressions, clicks, ad_spend, ctr, cpc,
                           orders_7d, sales_7d, acos_7d, roas_7d,
                           orders_30d, sales_30d, acos_30d, roas_30d
                    FROM report_advertising
                    {where_sql}
                    ORDER BY ad_spend DESC
                """, params)
                rows = cursor.fetchall()

            return jsonify({"status": "success", "data": _to_json_serializable(rows)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_daily_campaigns] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/daily/<date>/campaigns/<campaign_id>/ad-groups', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_daily_ad_groups(date, campaign_id):
    """第三层：指定日期 + 广告活动下的广告组列表"""
    try:
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'", "dimension_type = 'ad_group'", "report_date = %s", "campaign_id = %s"]
            params = [date, campaign_id]
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT ad_group_id, ad_group_name,
                           impressions, clicks, ad_spend, ctr, cpc,
                           orders_7d, sales_7d, acos_7d, roas_7d,
                           orders_30d, sales_30d, acos_30d, roas_30d
                    FROM report_advertising
                    {where_sql}
                    ORDER BY ad_spend DESC
                """, params)
                rows = cursor.fetchall()

            return jsonify({"status": "success", "data": _to_json_serializable(rows)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_daily_ad_groups] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/daily/<date>/campaigns/<campaign_id>/ad-groups/<ad_group_id>/products', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def advertising_daily_products(date, campaign_id, ad_group_id):
    """第四层：指定日期 + 广告活动 + 广告组下的广告商品列表"""
    try:
        shop_id = _get_shop_id_optional()

        conn = _get_conn()
        try:
            where_clauses = ["report_type = 'daily'", "dimension_type = 'asin'", "report_date = %s", "campaign_id = %s", "ad_group_id = %s"]
            params = [date, campaign_id, ad_group_id]
            if shop_id is not None:
                where_clauses.append("shop_id = %s"); params.append(shop_id)
            where_sql = "WHERE " + " AND ".join(where_clauses)

            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT r.ad_group_id, r.ad_group_name, r.asin AS advertised_asin, r.sku AS advertised_sku,
                           COALESCE(p.product_name, '') AS product_name,
                           r.impressions, r.clicks, r.ad_spend, r.ctr, r.cpc,
                           r.orders_7d, r.sales_7d, r.acos_7d, r.roas_7d,
                           r.orders_30d, r.sales_30d, r.acos_30d, r.roas_30d
                    FROM report_advertising r
                    LEFT JOIN products p ON p.seller_sku = r.sku AND p.status = 1
                    {where_sql}
                    ORDER BY r.ad_spend DESC
                """, params)
                rows = cursor.fetchall()

            return jsonify({"status": "success", "data": _to_json_serializable(rows)})
        finally:
            conn.close()
    except Exception as e:
        print(f"[advertising_daily_products] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/generate', methods=['POST'])
@login_required
@permission_required('reports_advertising:generate')
def trigger_advertising_report():
    """
    手动触发广告效果日报生成

    请求体 (JSON):
        period   (必填) 日期，如 2026-05-18
        shop_id  (可选) 指定店铺
    """
    try:
        data = request.get_json() or {}
        period = data.get('period', '').strip()
        shop_id = data.get('shop_id')

        if not period:
            return jsonify({"status": "error", "message": "period 参数必填"}), 400

        result = generate_advertising_daily(period, shop_id)
        return jsonify({"status": "success", "message": "生成完成", "data": result})
    except Exception as e:
        print(f"[trigger_advertising_report] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 6.5 广告原始数据详情 (amazon_ads_raw_reports 宽表查询)
# ============================================================

# 通用可排序字段：数据库实列 + 前端计算列
_ALL_SORT_COLS = [
    # 数据库实列
    "impressions", "clicks", "cost",
    "purchases_7d", "purchases_14d", "purchases_30d",
    "sales_7d", "sales_14d", "sales_30d",
    "report_date",
    # 字符串列 (仅部分接口支持)
    "customer_search_term", "keyword_text", "keyword_type",
    "campaign_name", "advertised_asin", "advertised_sku",
    "placement",
    # 前端计算指标 (内存排序)
    "ctr", "cpc", "acos_7d", "acos_14d", "acos_30d",
    "roas_7d", "roas_14d", "roas_30d",
]

# 枚举值 → 中文标签 (参考 Amazon Ads API 官方文档)
_AD_ENUM_LABELS = {
    # 关键词类型 / 匹配类型
    "BROAD": "广泛匹配",
    "PHRASE": "词组匹配",
    "EXACT": "精准匹配",
    "NEGATIVE_BROAD": "否定广泛匹配",
    "NEGATIVE_PHRASE": "否定词组匹配",
    "NEGATIVE_EXACT": "否定精准匹配",
    "TARGETING_EXPRESSION": "自动定向",
    "TARGETING_EXPRESSION_PREDEFINED": "预设定向",
    # 定向表达式 (keyword_text / targeting_expression)
    "close-match": "紧密匹配",
    "loose-match": "宽泛匹配",
    "complements": "关联商品",
    "substitutes": "替代商品",
    # 活动状态
    "ENABLED": "启用",
    "PAUSED": "暂停",
    "ARCHIVED": "归档",
    # 预算类型
    "daily": "每日预算",
    "lifetime": "周期总预算",
    # 广告位置
    "Top of Search on-Amazon": "搜索顶部",
    "Detail Page on-Amazon": "商品详情页",
    "Other on-Amazon": "搜索其他位置",
    "Off Amazon": "站外",
}


def _add_labels(row):
    """给原始数据行的枚举字段附加 _label 中文解释"""
    for field in ("keyword_type", "keyword_match_type", "campaign_status",
                  "campaign_budget_type"):
        val = (row.get(field) or "").strip()
        if val:
            row[field + "_label"] = _AD_ENUM_LABELS.get(val, val)

    # keyword_text / targeting_expression 可能是 close-match 等值
    for field in ("keyword_text", "targeting_expression"):
        val = (row.get(field) or "").strip()
        if val and val in _AD_ENUM_LABELS:
            row[field + "_label"] = _AD_ENUM_LABELS[val]

    # placement 中文标签
    placement_val = (row.get("placement") or "").strip()
    if placement_val:
        row["placement_label"] = _AD_ENUM_LABELS.get(placement_val, placement_val)

    return row


def _compute_raw_metrics(row):
    """给原始数据行附加 CTR/CPC/ACOS/ROAS 计算字段"""
    imp = row.get("impressions", 0) or 0
    clk = row.get("clicks", 0) or 0
    cost = float(row.get("cost", 0) or 0)
    s7 = float(row.get("sales_7d", 0) or 0)
    s14 = float(row.get("sales_14d", 0) or 0)
    s30 = float(row.get("sales_30d", 0) or 0)

    row["ctr"] = round(clk / imp, 6) if imp > 0 else None
    row["cpc"] = round(cost / clk, 4) if clk > 0 else None
    row["acos_7d"] = round(cost / s7, 4) if s7 > 0 else None
    row["acos_14d"] = round(cost / s14, 4) if s14 > 0 else None
    row["acos_30d"] = round(cost / s30, 4) if s30 > 0 else None
    row["roas_7d"] = round(s7 / cost, 4) if cost > 0 else None
    row["roas_14d"] = round(s14 / cost, 4) if cost > 0 else None
    row["roas_30d"] = round(s30 / cost, 4) if cost > 0 else None
    return row


def _query_raw_reports(report_type, shop_id=None, start_date=None, end_date=None,
                       campaign_id=None, ad_group_id=None, asin=None, keyword=None,
                       sort_by="cost", sort_dir="desc", page=1, page_size=20):
    """查询 amazon_ads_raw_reports 通用方法，返回分页列表 + 汇总 + 中文标签"""
    conn = _get_conn()
    try:
        # 验证排序: 数据库实列 + 计算字段
        sort_db_fields = {"impressions", "clicks", "cost",
                          "purchases_7d", "purchases_14d", "purchases_30d",
                          "sales_7d", "sales_14d", "sales_30d",
                          "customer_search_term", "keyword_text", "keyword_type",
                          "campaign_name", "advertised_asin", "advertised_sku",
                          "placement",
                          "report_date"}
        if sort_by not in _ALL_SORT_COLS:
            sort_by = "cost"
        is_db_sort = sort_by in sort_db_fields
        is_desc = sort_dir.lower() == "desc"

        where = ["report_type = %s"]
        params = [report_type]

        if shop_id is not None:
            where.append("shop_id = %s")
            params.append(shop_id)
        if start_date:
            where.append("report_date >= %s")
            params.append(start_date)
        if end_date:
            where.append("report_date <= %s")
            params.append(end_date)
        if campaign_id:
            where.append("campaign_id = %s")
            params.append(campaign_id)
        if ad_group_id:
            where.append("ad_group_id = %s")
            params.append(ad_group_id)
        if asin:
            where.append("advertised_asin = %s")
            params.append(asin)
        if keyword:
            where.append("(keyword_text LIKE %s OR customer_search_term LIKE %s)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        where_sql = "WHERE " + " AND ".join(where)

        with conn.cursor() as cur:
            # 汇总
            cur.execute(f"""
                SELECT COUNT(1) AS total_rows,
                       SUM(impressions) AS total_impressions,
                       SUM(clicks) AS total_clicks,
                       SUM(cost) AS total_cost,
                       SUM(purchases_7d) AS total_purchases_7d,
                       SUM(purchases_14d) AS total_purchases_14d,
                       SUM(purchases_30d) AS total_purchases_30d,
                       SUM(sales_7d) AS total_sales_7d,
                       SUM(sales_14d) AS total_sales_14d,
                       SUM(sales_30d) AS total_sales_30d
                FROM amazon_ads_raw_reports {where_sql}
            """, params)
            summary = cur.fetchone()
            if summary:
                summary = dict(summary)
                summary = _compute_raw_metrics(summary)

            # 根据报告类型选择返回字段
            if report_type == "spSearchTerm":
                select_cols = ("campaign_name, ad_group_name, keyword_text, keyword_type, "
                               "keyword_match_type, customer_search_term, "
                               "impressions, clicks, cost, purchases_7d, sales_7d, report_date")
            elif report_type == "spTargeting":
                select_cols = ("campaign_name, ad_group_name, keyword_text, keyword_type, "
                               "keyword_match_type, targeting_expression, "
                               "impressions, clicks, cost, purchases_7d, sales_7d, report_date")
            elif report_type == "spCampaigns":
                select_cols = ("campaign_name, campaign_status, campaign_budget, campaign_budget_type, "
                               "impressions, clicks, cost, "
                               "purchases_7d, purchases_14d, purchases_30d, "
                               "sales_7d, sales_14d, sales_30d, report_date")
            elif report_type == "spCampaignsPlacement":
                select_cols = ("campaign_name, campaign_status, "
                               "placement, "
                               "impressions, clicks, cost, "
                               "purchases_7d, purchases_14d, purchases_30d, "
                               "sales_7d, sales_14d, sales_30d, report_date")
            else:  # spAdvertisedProduct
                select_cols = ("campaign_name, ad_group_name, advertised_asin, advertised_sku, "
                               "impressions, clicks, cost, "
                               "purchases_7d, purchases_30d, sales_7d, sales_30d, report_date")

            # 数据库排序 vs 内存排序 (计算字段 ctr/cpc/acos/roas 需内存排序)
            if is_db_sort:
                sort_sql = f"ORDER BY {sort_by} {'DESC' if is_desc else 'ASC'}"
                cur.execute(f"""
                    SELECT {select_cols}
                    FROM amazon_ads_raw_reports
                    {where_sql}
                    {sort_sql}
                    LIMIT %s OFFSET %s
                """, params + [page_size, (page - 1) * page_size])
                rows = cur.fetchall()
                cur.execute(f"SELECT COUNT(1) AS cnt FROM amazon_ads_raw_reports {where_sql}", params)
                total = cur.fetchone()["cnt"]

                list_data = []
                for r in rows:
                    d = dict(r)
                    d = _compute_raw_metrics(d)
                    d = _add_labels(d)
                    list_data.append(d)
            else:
                # 计算字段排序: 先取全部 → 计算 → 内存排序 → 分页
                cur.execute(f"""
                    SELECT {select_cols}
                    FROM amazon_ads_raw_reports
                    {where_sql}
                """, params)
                all_rows = cur.fetchall()

                list_data = []
                for r in all_rows:
                    d = dict(r)
                    d = _compute_raw_metrics(d)
                    d = _add_labels(d)
                    list_data.append(d)

                total = len(list_data)
                # 内存排序 (None 排最后, 不管升序还是降序)
                def _sort_key(d):
                    v = d.get(sort_by)
                    if v is None:
                        return (1, 0) if is_desc else (1, float('inf'))
                    return (0, float(v))
                list_data.sort(key=_sort_key, reverse=is_desc)

                # 内存分页
                offset = (page - 1) * page_size
                list_data = list_data[offset:offset + page_size]

        return {
            "list": _to_json_serializable(list_data),
            "total": total,
            "page": page,
            "page_size": page_size,
            "summary": _to_json_serializable(summary),
        }
    finally:
        conn.close()


@reports_bp.route('/reports/advertising/search-terms', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def list_search_terms():
    """
    客户搜索词详情 (spSearchTerm)

    简介: 查看客户实际搜索的关键词表现，对标 Amazon 后台「搜索词」页签。

    查询参数:
        start_date / end_date  日期范围
        campaign_id            按活动筛选
        ad_group_id            按广告组筛选
        keyword                按搜索词模糊搜索
        shop_id                店铺ID
        sort_by                排序字段 (impressions/clicks/cost/purchases_7d/sales_7d/customer_search_term)
        sort_dir               asc / desc
        page / page_size       分页
    """
    try:
        p = _parse_pagination()
        result = _query_raw_reports(
            report_type="spSearchTerm",
            shop_id=_get_shop_id_optional(),
            start_date=request.args.get("start_date", "").strip() or None,
            end_date=request.args.get("end_date", "").strip() or None,
            campaign_id=request.args.get("campaign_id", "").strip() or None,
            ad_group_id=request.args.get("ad_group_id", "").strip() or None,
            keyword=request.args.get("keyword", "").strip() or None,
            sort_by=request.args.get("sort_by", "cost").strip(),
            sort_dir=request.args.get("sort_dir", "desc").strip(),
            page=p[0], page_size=p[1],
        )
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"[list_search_terms] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/targeting', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def list_targeting():
    """
    关键词定向详情 (spTargeting)

    简介: 查看广告关键词/定向的表现，对标 Amazon 后台「定向」页签。

    查询参数: 同 search-terms, sort_by 多了 keyword_text/keyword_type
    """
    try:
        p = _parse_pagination()
        result = _query_raw_reports(
            report_type="spTargeting",
            shop_id=_get_shop_id_optional(),
            start_date=request.args.get("start_date", "").strip() or None,
            end_date=request.args.get("end_date", "").strip() or None,
            campaign_id=request.args.get("campaign_id", "").strip() or None,
            ad_group_id=request.args.get("ad_group_id", "").strip() or None,
            keyword=request.args.get("keyword", "").strip() or None,
            sort_by=request.args.get("sort_by", "cost").strip(),
            sort_dir=request.args.get("sort_dir", "desc").strip(),
            page=p[0], page_size=p[1],
        )
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"[list_targeting] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/campaigns', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def list_campaigns_detail():
    """
    广告活动详情 (spCampaigns)

    简介: 查看每个活动的完整指标（含14d/30d归因），对标 Amazon 后台「广告活动」页签。

    查询参数: 同 search-terms, sort_by 多了 campaign_name, purchases_14d, sales_14d 等
    """
    try:
        p = _parse_pagination()
        result = _query_raw_reports(
            report_type="spCampaigns",
            shop_id=_get_shop_id_optional(),
            start_date=request.args.get("start_date", "").strip() or None,
            end_date=request.args.get("end_date", "").strip() or None,
            campaign_id=request.args.get("campaign_id", "").strip() or None,
            keyword=request.args.get("keyword", "").strip() or None,
            sort_by=request.args.get("sort_by", "cost").strip(),
            sort_dir=request.args.get("sort_dir", "desc").strip(),
            page=p[0], page_size=p[1],
        )
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"[list_campaigns_detail] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@reports_bp.route('/reports/advertising/products', methods=['GET'])
@login_required
@permission_required('reports_advertising:page')
def list_products_detail():
    """
    推广商品详情 (spAdvertisedProduct)

    简介: 查看每个推广ASIN的表现，对标 Amazon 后台「推广的商品」页签。

    查询参数: 同 search-terms, sort_by 多了 advertised_asin/advertised_sku
    """
    try:
        p = _parse_pagination()
        result = _query_raw_reports(
            report_type="spAdvertisedProduct",
            shop_id=_get_shop_id_optional(),
            start_date=request.args.get("start_date", "").strip() or None,
            end_date=request.args.get("end_date", "").strip() or None,
            campaign_id=request.args.get("campaign_id", "").strip() or None,
            ad_group_id=request.args.get("ad_group_id", "").strip() or None,
            asin=request.args.get("asin", "").strip() or None,
            keyword=request.args.get("keyword", "").strip() or None,
            sort_by=request.args.get("sort_by", "cost").strip(),
            sort_dir=request.args.get("sort_dir", "desc").strip(),
            page=p[0], page_size=p[1],
        )
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"[list_products_detail] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 7. 一键生成昨日全部报表
# ============================================================

@reports_bp.route('/reports/generate-yesterday', methods=['POST'])
@login_required
@permission_required('reports:generate')
def trigger_yesterday_reports():
    """
    一键生成最近10天全部报表

    简介: 生成最近10天日报 + SKU利润 + 库存周转 + 最近10天广告日报，
          并检查是否需要生最近3周周报（周三）或最近2个月月报（3号）。
    """
    try:
        results = generate_yesterday_reports()
        return jsonify({"status": "success", "message": "报表生成完成", "data": results})
    except Exception as e:
        print(f"[trigger_yesterday_reports] error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
