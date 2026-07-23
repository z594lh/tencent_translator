"""
CPC 广告管理 — Flask Blueprint + 实体同步服务

前缀: /api/cpc

前端路由:
    - 广告活动 (campaigns)      - 广告组 (groups)
    - 广告位 (placements)        - 产品广告 (product-ads)
    - 关键词 (keywords)          - 投放 (targets)
    - 否定关键词 (negative-keywords) - 否定投放 (negative-targets)
    - 搜索词 (search-terms)      - 分日数据 (daily-data)
    - 广告结构 (structure)       - 创建向导 (create-campaign)

后端同步:
    - sync_shop_entities(shop_id) — 同步单个店铺全量实体到本地表
    - sync_all_shops()             — 同步所有启用店铺
"""
import json
import time
import io
import csv
from decimal import Decimal
from datetime import datetime, timedelta

import openpyxl
from flask import Blueprint, request, jsonify, g

from services.mysql_service import get_db_connection
from services.shop_service import get_ads_api_client, get_shop_by_id, get_all_active_shops

advertising_manage_bp = Blueprint('advertising_manage', __name__, url_prefix='/api/cpc')


# =================================================================
#  枚举常量
# =================================================================

SERVING_STATUS_MAP = {
    "CAMPAIGN_STATUS_ENABLED": "投放中",
    "CAMPAIGN_OUT_OF_BUDGET": "超预算",
    "CAMPAIGN_PAUSED": "已暂停",
    "CAMPAIGN_ARCHIVED": "已归档",
    "PENDING_REVIEW": "待审核",
    "REJECTED": "已拒绝",
    "PENDING_START_DATE": "待开始",
    "ENDED": "已结束",
    "CAMPAIGN_INCOMPLETE": "未完成",
    "AD_STATUS_LIVE": "投放中",
    "TARGETING_CLAUSE_STATUS_LIVE": "投放中",
}

BIDDING_STRATEGY_MAP = {
    "LEGACY_FOR_SALES": "动态竞价-仅降低",
    "AUTO_FOR_SALES": "动态竞价-提高和降低",
    "MANUAL": "固定竞价",
}

MATCH_TYPE_MAP = {
    "EXACT": "精准匹配",
    "BROAD": "广泛匹配",
    "PHRASE": "词组匹配",
    "NEGATIVE_EXACT": "否定精准",
    "NEGATIVE_PHRASE": "否定词组",
    "NEGATIVE_BROAD": "否定广泛",
}


# =================================================================
#  前端路由 — 工具函数
# =================================================================

def _get_conn():
    return get_db_connection()


def _get_shop_id_optional():
    v = request.args.get("shop_id", "").strip()
    if v:
        try:
            return int(v)
        except (ValueError, TypeError):
            return None
    return None


def _parse_pagination():
    try:
        page = int(request.args.get("page", 1))
    except (ValueError, TypeError):
        page = 1
    try:
        page_size = int(request.args.get("page_size", 20))
        page_size = max(1, min(page_size, 100))
    except (ValueError, TypeError):
        page_size = 20
    return page, page_size


def _parse_date_range():
    start = request.args.get("start_date", "").strip()
    end = request.args.get("end_date", "").strip()
    if not start:
        start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not end:
        end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return start, end


def _log_operation(shop_id, campaign_id, entity_type, entity_id, action,
                   status=1, error_message=None, key_type=None):
    try:
        user_id = g.get("user_id") if hasattr(g, "user_id") else None
    except (RuntimeError, AttributeError):
        user_id = None
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO amazon_ads_operation_logs
                    (shop_id, user_id, campaign_id, entity_type, entity_id,
                     key_type, action, status, error_message)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s)
            """, (shop_id, user_id, campaign_id, entity_type, entity_id,
                  key_type, action, status, error_message))
        conn.commit()
    finally:
        conn.close()


def _compute_metrics(d):
    impressions = int(d.get("impressions") or 0)
    clicks = int(d.get("clicks") or 0)
    cost = float(d.get("cost") or 0)
    sales = float(d.get("sales_7d") or d.get("sales") or 0)
    orders = int(d.get("purchases_7d") or d.get("orders") or 0)

    d["cpc"] = round(cost / clicks, 4) if clicks > 0 else None
    d["ctr"] = round(clicks / impressions * 100, 4) if impressions > 0 else None
    d["cvr"] = round(orders / clicks * 100, 4) if clicks > 0 else None
    d["cpa"] = round(cost / orders, 4) if orders > 0 else None
    d["acos"] = round(cost / sales * 100, 4) if sales > 0 else None
    return d


def _map_entity_row(row, entity_type="campaign"):
    d = dict(row) if row else {}
    ss = (d.get("serving_status") or "").strip()
    if ss:
        d["serving_status_label"] = SERVING_STATUS_MAP.get(ss, ss)
    bidding = d.get("bidding")
    if bidding:
        if isinstance(bidding, str):
            try:
                bidding = json.loads(bidding)
            except (json.JSONDecodeError, TypeError):
                bidding = None
        if isinstance(bidding, dict):
            strategy = bidding.get("strategy", "")
            d["bidding_strategy"] = strategy
            d["bidding_strategy_label"] = BIDDING_STRATEGY_MAP.get(strategy, strategy)
    mt = (d.get("match_type") or "").strip()
    if mt:
        d["match_type_label"] = MATCH_TYPE_MAP.get(mt, mt)
    # auto 定向 bid 为 null 时用广告组 default_bid 兜底
    if d.get("bid") is None:
        ag_bid = d.get("ad_group_default_bid")
        if ag_bid is not None:
            d["bid"] = ag_bid
    _compute_metrics(d)
    return d


def _query_entity_with_report(entity_table, entity_join_col, entity_fields,
                              report_type, report_join_col, metrics,
                              where_extra="", where_params=None,
                              order_by="cost DESC", page=None, page_size=None,
                              join_extra="", extra_left_join=""):
    """通用查询：实体表 JOIN 报表表，按日期范围聚合指标
    
    join_extra: 额外的 JOIN ON 条件，如 "AND r.campaign_id = e.campaign_id AND r.ad_group_id = e.ad_group_id"
    extra_left_join: 额外的 LEFT JOIN 子句，如 "LEFT JOIN amazon_ads_ad_groups ag ON ag.ad_group_id = e.ad_group_id"
    """
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    campaign_id = request.args.get("campaign_id", "").strip() or None
    ad_group_id = request.args.get("ad_group_id", "").strip() or None

    # ON 子句条件（报表相关）
    on_clauses = [f"r.report_type = %s",
                  f"r.report_date BETWEEN %s AND %s"]
    params = [report_type, start_date, end_date]
    join_where = join_extra

    if shop_id:
        on_clauses.append("r.shop_id = %s")
        params.append(shop_id)
        if entity_table == "amazon_ads_campaigns":
            join_where = (join_where + " " if join_where else "") + "AND e.shop_id = r.shop_id"

    # WHERE 子句条件（实体相关）
    where_clauses = []

    if campaign_id:
        where_clauses.append("e.campaign_id = %s")
        params.append(campaign_id)
    if ad_group_id:
        where_clauses.append("e.ad_group_id = %s")
        params.append(ad_group_id)

    if where_extra:
        where_clauses.append(where_extra)

    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    serving_status = request.args.get("serving_status", "").strip()

    if search:
        where_clauses.append(f"(e.name LIKE %s OR CAST(e.{entity_join_col} AS CHAR) = %s)")
        params.extend([f"%{search}%", search])
    if state:
        if state == "unarchived":
            where_clauses.append("e.state != %s")
            params.append("archived")
        else:
            where_clauses.append("e.state = %s")
            params.append(state)
    if serving_status:
        where_clauses.append("e.serving_status = %s")
        params.append(serving_status)

    if where_params:
        where_clauses.extend(where_params[0])
        params.extend(where_params[1])

    on_sql = " AND ".join(on_clauses)
    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    allowed_sort = {"cost", "impressions", "clicks", "cpc", "ctr", "cvr", "cpa", "acos",
                    "sales_7d", "purchases_7d", "name", "state", "serving_status"}
    sort_by = request.args.get("sort_by", "").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip()
    if sort_by not in allowed_sort:
        sort_by = "cost"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    safe_fields = []
    for f in entity_fields.strip().split(","):
        f = f.strip()
        if not f:
            continue
        # 提取别名: "e.name AS name" → alias="name", expr="e.name"
        parts = f.split()
        if len(parts) >= 3 and parts[-2].upper() == "AS":
            alias = parts[-1]
            expr = " ".join(parts[:-2])
        else:
            alias = parts[-1].split(".")[-1]  # e.name → name
            expr = f
        if alias == entity_join_col or f.endswith(f".{entity_join_col}"):
            safe_fields.append(f)
        else:
            safe_fields.append(f"MIN({expr}) AS {alias}")

    sql = f"""
        SELECT {', '.join(safe_fields)},
               COALESCE(SUM(r.impressions), 0) AS impressions,
               COALESCE(SUM(r.clicks), 0) AS clicks,
               COALESCE(SUM(r.cost), 0) AS cost,
               COALESCE(SUM(r.purchases_7d), 0) AS purchases_7d,
               COALESCE(SUM(r.purchases_14d), 0) AS purchases_14d,
               COALESCE(SUM(r.purchases_30d), 0) AS purchases_30d,
               COALESCE(SUM(r.sales_7d), 0) AS sales_7d,
               COALESCE(SUM(r.sales_14d), 0) AS sales_14d,
               COALESCE(SUM(r.sales_30d), 0) AS sales_30d,
               COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share
         FROM {entity_table} e
         LEFT JOIN amazon_ads_raw_reports r ON {report_join_col} = e.{entity_join_col} {join_where} AND {on_sql}
         {extra_left_join}
         {where_sql}
         GROUP BY e.{entity_join_col}
         ORDER BY {sort_by} {sort_dir}
     """
    conn = _get_conn()
    try:
        with conn.cursor() as cursor:
            if page is not None and page_size is not None:
                count_sql = f"""
                    SELECT COUNT(DISTINCT e.{entity_join_col}) AS total
                    FROM {entity_table} e
                    LEFT JOIN amazon_ads_raw_reports r ON {report_join_col} = e.{entity_join_col} {join_where} AND {on_sql}
                    {extra_left_join}
                    {where_sql}
                """
                cursor.execute(count_sql, params)
                total = cursor.fetchone()["total"]
                sql += " LIMIT %s OFFSET %s"
                cursor.execute(sql, params + [page_size, (page - 1) * page_size])
            else:
                cursor.execute(sql, params)
                total = None
            rows = cursor.fetchall()
            return [dict(r) for r in rows], total
    finally:
        conn.close()


# =================================================================
#  前端路由 — 账号列表
# =================================================================

@advertising_manage_bp.route('/campaigns/accounts', methods=['GET'])
def list_accounts():
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("""
                SELECT s.id AS shop_id, s.shop_name, s.marketplace_id, s.region,
                       cg.group_name
                FROM amazon_shops s
                LEFT JOIN amazon_credential_groups cg ON s.credential_group_id = cg.id
                WHERE s.status = 1 AND s.ads_refresh_token IS NOT NULL AND s.ads_refresh_token != ''
                ORDER BY cg.group_name, s.shop_name
            """)
            shops = c.fetchall()
        groups = {}
        for s in shops:
            gn = s["group_name"] or "默认"
            if gn not in groups:
                groups[gn] = []
            groups[gn].append({
                "shop_id": s["shop_id"],
                "shop_name": s["shop_name"],
                "marketplace_id": s["marketplace_id"],
                "region": s["region"],
            })
        return jsonify({"status": "success", "data": groups})
    finally:
        conn.close()


# =================================================================
#  前端路由 — 广告活动
# =================================================================

@advertising_manage_bp.route('/campaigns', methods=['GET'])
def list_campaigns():
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    serving_status = request.args.get("serving_status", "").strip()
    targeting_type = request.args.get("targeting_type", "").strip()
    sort_by = request.args.get("sort_by", "cost").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip()
    page, page_size = _parse_pagination()

    # 区间筛选
    _RANGE_OPS = [("_gt", ">"), ("_gte", ">="), ("_lt", "<"), ("_lte", "<=")]
    _RAW_FIELDS = {
        "impressions":   "SUM(r.impressions)",
        "clicks":        "SUM(r.clicks)",
        "cost":          "SUM(r.cost)",
        "purchases_7d":  "SUM(r.purchases_7d)",
        "sales_7d":      "SUM(r.sales_7d)",
    }
    _COMPUTED_FIELDS = {
        "ctr":  "COALESCE(SUM(r.clicks),0) / NULLIF(COALESCE(SUM(r.impressions),0), 0) * 100",
        "cpc":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0)",
        "cvr":  "COALESCE(SUM(r.purchases_7d),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0) * 100",
        "cpa":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.purchases_7d),0), 0)",
        "acos": "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.sales_7d),0), 0) * 100",
        "roas": "COALESCE(SUM(r.sales_7d),0) / NULLIF(COALESCE(SUM(r.cost),0), 0)",
    }
    _ALL_METRICS = {**_RAW_FIELDS, **_COMPUTED_FIELDS}

    having_clauses = []
    for field, expr in _ALL_METRICS.items():
        for suffix, op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try:
                    float(val)
                except ValueError:
                    continue
                having_clauses.append(f"({expr}) {op} %s")
    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

    having_params = []
    for field in _ALL_METRICS:
        for suffix, _op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try:
                    having_params.append(float(val))
                except ValueError:
                    pass

    # 排序白名单
    allowed_sort = {"cost", "impressions", "clicks", "cpc", "ctr", "cvr", "cpa", "acos",
                    "sales_7d", "purchases_7d", "name", "state", "serving_status", "daily_budget"}
    if sort_by not in allowed_sort:
        sort_by = "cost"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    conn = _get_conn()
    try:
        # 报表条件放 ON 子句（LEFT JOIN 时不会过滤掉无报表数据的实体）
        report_on = ["r.report_type = 'spCampaigns'",
                     "r.report_date BETWEEN %s AND %s"]
        params = [start_date, end_date]

        if shop_id:
            report_on.append("r.shop_id = %s"); params.append(shop_id)

        # 实体条件放 WHERE 子句
        entity_where = []
        if shop_id:
            entity_where.append("e.shop_id = %s"); params.append(shop_id)
        if search:
            entity_where.append("(e.name LIKE %s OR CAST(e.campaign_id AS CHAR) = %s)")
            params.extend([f"%{search}%", search])
        if state:
            if state == "unarchived":
                entity_where.append("e.state != %s"); params.append("archived")
            else:
                entity_where.append("e.state = %s"); params.append(state)
        if serving_status:
            entity_where.append("e.serving_status = %s"); params.append(serving_status)
        if targeting_type:
            entity_where.append("e.targeting_type = %s"); params.append(targeting_type)

        on_sql = " AND ".join(report_on)
        where_sql = ("WHERE " + " AND ".join(entity_where)) if entity_where else ""

        entity_fields = [
            "MIN(e.campaign_id) AS campaign_id",
            "MIN(e.name) AS name",
            "MIN(e.state) AS state",
            "MIN(e.targeting_type) AS targeting_type",
            "MIN(e.serving_status) AS serving_status",
            "MIN(e.daily_budget) AS daily_budget",
            "MIN(e.start_date) AS start_date",
            "MIN(e.end_date) AS end_date",
            "MIN(e.bidding) AS bidding",
            "MIN(e.last_update_datetime) AS last_update_datetime",
            "MIN(e.synced_at) AS synced_at",
            "MIN(e.re_open_day) AS re_open_day",
        ]

        base_from = f"""FROM amazon_ads_campaigns e
                 LEFT JOIN amazon_ads_raw_reports r ON r.campaign_id = e.campaign_id AND {on_sql}
                 {where_sql}"""
        base_group = "GROUP BY e.campaign_id"

        with conn.cursor() as c:
            # 总数
            if having_sql:
                count_sql = f"""
                    SELECT COUNT(*) AS total FROM (
                        SELECT 1
                        {base_from}
                        {base_group}
                        {having_sql}
                    ) t
                """
                c.execute(count_sql, params + having_params)
            else:
                count_sql = f"""
                    SELECT COUNT(DISTINCT e.campaign_id) AS total
                    FROM amazon_ads_campaigns e
                    LEFT JOIN amazon_ads_raw_reports r ON r.campaign_id = e.campaign_id AND {on_sql}
                    {where_sql}
                """
                c.execute(count_sql, params)
            total = c.fetchone()["total"]

            c.execute(f"""
                SELECT
                    {', '.join(entity_fields)},
                    COALESCE(SUM(r.impressions), 0) AS impressions,
                    COALESCE(SUM(r.clicks), 0) AS clicks,
                    COALESCE(SUM(r.cost), 0) AS cost,
                    COALESCE(SUM(r.purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(r.purchases_14d), 0) AS purchases_14d,
                    COALESCE(SUM(r.purchases_30d), 0) AS purchases_30d,
                    COALESCE(SUM(r.sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share,
                    COALESCE(SUM(r.sales_14d), 0) AS sales_14d,
                    COALESCE(SUM(r.sales_30d), 0) AS sales_30d,
                    COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share
                {base_from}
                {base_group}
                {having_sql}
                ORDER BY {sort_by} {sort_dir}
                LIMIT %s OFFSET %s
            """, params + having_params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()

        list_data = [_map_entity_row(dict(r)) for r in rows]
        for item in list_data:
            for k in ("impressions", "clicks", "cost", "purchases_7d", "sales_7d",
                       "purchases_14d", "sales_14d", "purchases_30d", "sales_30d",
                       "cpc", "ctr", "cvr", "cpa", "acos", "daily_budget"):
                v = item.get(k)
                if v is not None:
                    try:
                        item[k] = float(v) if "." in str(v) or k in ("cpc", "ctr", "cvr", "cpa", "acos", "daily_budget") else int(float(v))
                    except (ValueError, TypeError):
                        pass
        return jsonify({"status": "success", "data": {"list": list_data, "total": total}})
    finally:
        conn.close()


@advertising_manage_bp.route('/campaigns/<int:campaign_id>', methods=['GET'])
def campaign_detail(campaign_id):
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT * FROM amazon_ads_campaigns WHERE campaign_id = %s", (campaign_id,))
            row = c.fetchone()
        if not row:
            return jsonify({"status": "error", "message": "活动不存在"}), 404
        d = _map_entity_row(row)
        print(f"[CPC] GET /campaigns/{campaign_id} → daily_budget={d.get('daily_budget')}, state={d.get('state')}, strategy={d.get('bidding_strategy')}")
        return jsonify({"status": "success", "data": d})
    finally:
        conn.close()


@advertising_manage_bp.route('/campaigns/<int:campaign_id>', methods=['PUT'])
def update_campaign(campaign_id):
    body = request.get_json() or {}
    print(f"[CPC] PUT /campaigns/{campaign_id} body keys={list(body.keys())} dailyBudget={body.get('dailyBudget')!r} state={body.get('state')!r} bidding_keys={list(body.get('bidding',{}).keys())}")
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    try:
        client = get_ads_api_client(int(shop_id))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    updates = {}
    for key in ("state", "dailyBudget", "startDate", "endDate"):
        if key in body:
            v = body[key]
            if key == "state":
                print(f"[CPC] update_campaign {campaign_id} state={v!r}")
            if key == "dailyBudget" and v is not None:
                try:
                    v = float(v)
                except (ValueError, TypeError):
                    return jsonify({"status": "error", "message": "dailyBudget 必须是数字"}), 400
            updates[key] = v
    if "bidding" in body:
        bidding = body["bidding"]
        # 纠正常见错误枚举值
        for pb in bidding.get("placementBidding", []):
            wrong = pb.get("placement", "")
            if wrong in _PLACEMENT_FIX:
                pb["placement"] = _PLACEMENT_FIX[wrong]
        strategy = bidding.get("strategy", "")
        if strategy == "FIXED":
            bidding["strategy"] = "MANUAL"
        updates["bidding"] = bidding

    if not updates:
        return jsonify({"status": "error", "message": "无更新字段"}), 400

    # v3 API PUT 不支持 ARCHIVED，软归档：对 Amazon 发 PAUSED，本地存 archived
    api_updates = dict(updates)
    db_state = updates.get("state")
    if db_state and str(db_state).lower() == "archived":
        api_updates["state"] = "PAUSED"

    try:
        client.update_campaign(campaign_id, api_updates)
    except Exception as e:
        _log_operation(shop_id, campaign_id, "campaign", campaign_id,
                       f"更新失败: {updates}", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            set_parts, vals = [], []
            if "state" in updates:
                set_parts.append("state = %s"); vals.append(updates["state"] if db_state else updates["state"])
            if "dailyBudget" in updates:
                set_parts.append("daily_budget = %s"); vals.append(float(updates["dailyBudget"]))
            if "startDate" in updates:
                set_parts.append("start_date = %s"); vals.append(updates["startDate"])
            if "endDate" in updates:
                set_parts.append("end_date = %s"); vals.append(updates["endDate"])
            if "bidding" in updates:
                set_parts.append("bidding = %s"); vals.append(json.dumps(updates["bidding"]))
            if set_parts:
                vals.append(campaign_id)
                c.execute(f"UPDATE amazon_ads_campaigns SET {', '.join(set_parts)} WHERE campaign_id = %s", vals)
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, campaign_id, "campaign", campaign_id, f"更新成功: {updates}")
    return jsonify({"status": "success", "message": "更新成功"})


# =================================================================
#  前端路由 — 广告组
# =================================================================

@advertising_manage_bp.route('/groups', methods=['GET'])
def list_ad_groups():
    campaign_id = request.args.get("campaign_id", "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "message": "缺少 campaign_id"}), 400
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    sort_by = request.args.get("sort_by", "cost").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip()
    page, page_size = _parse_pagination()

    _RANGE_OPS = [("_gt", ">"), ("_gte", ">="), ("_lt", "<"), ("_lte", "<=")]
    _RAW_FIELDS = {
        "impressions":  "SUM(r.impressions)",
        "clicks":       "SUM(r.clicks)",
        "cost":         "SUM(r.cost)",
        "purchases_7d": "SUM(r.purchases_7d)",
        "sales_7d":     "SUM(r.sales_7d)",
    }
    _COMPUTED_FIELDS = {
        "ctr":  "COALESCE(SUM(r.clicks),0) / NULLIF(COALESCE(SUM(r.impressions),0), 0) * 100",
        "cpc":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0)",
        "cvr":  "COALESCE(SUM(r.purchases_7d),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0) * 100",
        "cpa":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.purchases_7d),0), 0)",
        "acos": "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.sales_7d),0), 0) * 100",
        "roas": "COALESCE(SUM(r.sales_7d),0) / NULLIF(COALESCE(SUM(r.cost),0), 0)",
    }
    _ALL_METRICS = {**_RAW_FIELDS, **_COMPUTED_FIELDS}

    having_clauses = []
    for field, expr in _ALL_METRICS.items():
        for suffix, op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try: float(val)
                except ValueError: continue
                having_clauses.append(f"({expr}) {op} %s")
    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""
    having_params = []
    for field in _ALL_METRICS:
        for suffix, _op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try: having_params.append(float(val))
                except ValueError: pass

    allowed_sort = {"cost", "impressions", "clicks", "cpc", "ctr", "cvr", "cpa", "acos",
                    "sales_7d", "purchases_7d", "name", "state", "serving_status", "default_bid"}
    if sort_by not in allowed_sort:
        sort_by = "cost"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    conn = _get_conn()
    try:
        # 报表条件放 ON 子句
        report_on = ["r.report_type = 'spAdvertisedProduct'",
                     "r.report_date BETWEEN %s AND %s"]
        params = [start_date, end_date]
        if shop_id:
            report_on.append("r.shop_id = %s"); params.append(shop_id)

        # 实体条件放 WHERE 子句
        entity_where = [f"e.campaign_id = %s"]
        params.append(campaign_id)
        if shop_id:
            entity_where.append("e.campaign_id IN (SELECT campaign_id FROM amazon_ads_campaigns WHERE shop_id = %s)")
            params.append(shop_id)
        if search:
            entity_where.append("e.name LIKE %s"); params.append(f"%{search}%")
        if state:
            if state == "unarchived":
                entity_where.append("e.state != %s"); params.append("archived")
            else:
                entity_where.append("e.state = %s"); params.append(state)

        on_sql = " AND ".join(report_on)
        where_sql = "WHERE " + " AND ".join(entity_where)

        entity_fields = [
            "MIN(e.ad_group_id) AS ad_group_id",
            "MIN(e.name) AS name",
            "MIN(e.state) AS state",
            "MIN(e.default_bid) AS default_bid",
            "MIN(e.serving_status) AS serving_status",
            "MIN(e.campaign_id) AS campaign_id",
        ]

        base_from = f"""FROM amazon_ads_ad_groups e
                 LEFT JOIN amazon_ads_raw_reports r ON r.ad_group_id = e.ad_group_id AND {on_sql}
                 {where_sql}"""
        base_group = "GROUP BY e.ad_group_id"

        with conn.cursor() as c:
            if having_sql:
                count_sql = f"SELECT COUNT(*) AS total FROM (SELECT 1 {base_from} {base_group} {having_sql}) t"
                c.execute(count_sql, params + having_params)
            else:
                count_sql = f"SELECT COUNT(DISTINCT e.ad_group_id) AS total FROM amazon_ads_ad_groups e LEFT JOIN amazon_ads_raw_reports r ON r.ad_group_id = e.ad_group_id AND {on_sql} {where_sql}"
                c.execute(count_sql, params)
            total = c.fetchone()["total"]

            c.execute(f"""
                SELECT {', '.join(entity_fields)},
                    COALESCE(SUM(r.impressions), 0) AS impressions,
                    COALESCE(SUM(r.clicks), 0) AS clicks,
                    COALESCE(SUM(r.cost), 0) AS cost,
                    COALESCE(SUM(r.purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(r.sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share
                {base_from}
                {base_group}
                {having_sql}
                ORDER BY {sort_by} {sort_dir}
                LIMIT %s OFFSET %s
            """, params + having_params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()

        list_data = [_map_entity_row(dict(r)) for r in rows]
        for item in list_data:
            for k in ("impressions", "clicks", "cost", "purchases_7d", "sales_7d",
                       "cpc", "ctr", "cvr", "cpa", "acos", "default_bid"):
                v = item.get(k)
                if v is not None:
                    try:
                        item[k] = float(v) if "." in str(v) or k in ("cpc", "ctr", "cvr", "cpa", "acos", "default_bid") else int(float(v))
                    except (ValueError, TypeError):
                        pass
        return jsonify({"status": "success", "data": {"list": list_data, "total": total}})
    finally:
        conn.close()


@advertising_manage_bp.route('/groups/<int:ad_group_id>', methods=['GET'])
def ad_group_detail(ad_group_id):
    """广告组详情"""
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT * FROM amazon_ads_ad_groups WHERE ad_group_id = %s", (ad_group_id,))
            row = c.fetchone()
        if not row:
            return jsonify({"status": "error", "message": "广告组不存在"}), 404
        return jsonify({"status": "success", "data": _map_entity_row(row)})
    finally:
        conn.close()


@advertising_manage_bp.route('/groups/<int:ad_group_id>', methods=['PUT'])
def update_ad_group(ad_group_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    try:
        client = get_ads_api_client(int(shop_id))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    updates = {}
    for key in ("state", "defaultBid"):
        if key in body:
            v = body[key]
            if key == "defaultBid":
                try:
                    v = float(v)
                except (ValueError, TypeError):
                    return jsonify({"status": "error", "message": "defaultBid 必须是数字"}), 400
            updates[key] = v
    if not updates:
        return jsonify({"status": "error", "message": "无更新字段"}), 400

    api_updates = dict(updates)
    db_state = updates.get("state")
    if db_state and str(db_state).lower() == "archived":
        api_updates["state"] = "PAUSED"

    try:
        client.update_ad_group(ad_group_id, api_updates)
    except Exception as e:
        _log_operation(shop_id, 0, "ad_group", ad_group_id,
                       f"更新失败: {updates}", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            set_parts, vals = [], []
            if "state" in updates:
                set_parts.append("state = %s"); vals.append(updates["state"])
            if "defaultBid" in updates:
                set_parts.append("default_bid = %s"); vals.append(float(updates["defaultBid"]))
            if set_parts:
                vals.append(ad_group_id)
                c.execute(f"UPDATE amazon_ads_ad_groups SET {', '.join(set_parts)} WHERE ad_group_id = %s", vals)
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "ad_group", ad_group_id, f"更新成功: {updates}")
    return jsonify({"status": "success", "message": "更新成功"})


# =================================================================
#  前端路由 — 广告位
# =================================================================

_PLACEMENT_MAP = {
    "PLACEMENT_TOP_OF_SEARCH": "Top of Search on-Amazon",
    "PLACEMENT_TOP": "Top of Search on-Amazon",
    "PLACEMENT_REST_OF_SEARCH": "Other on-Amazon",
    "PLACEMENT_PRODUCT_PAGE": "Detail Page on-Amazon",
}

# 前端可能传错的 placement → Amazon API 正确值
_PLACEMENT_FIX = {
    "PLACEMENT_TOP_OF_SEARCH": "PLACEMENT_TOP",
    "PLACEMENT_PRODUCT_DETAIL": "PLACEMENT_PRODUCT_PAGE",
}

@advertising_manage_bp.route('/placements', methods=['GET'])
def list_placements():
    """广告位报表 + 当前出价调整百分比"""
    campaign_id = request.args.get("campaign_id", "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "message": "缺少 campaign_id"}), 400

    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()

    # 查当前竞价调整百分比
    placement_fee = {}
    c0 = _get_conn()
    try:
        with c0.cursor() as c:
            c.execute("SELECT bidding FROM amazon_ads_campaigns WHERE campaign_id = %s", (campaign_id,))
            row = c.fetchone()
        if row and row.get("bidding"):
            b = row["bidding"]
            if isinstance(b, str):
                try: b = json.loads(b)
                except: pass
            for pb in b.get("placementBidding", []) if isinstance(b, dict) else []:
                nm = _PLACEMENT_MAP.get(pb.get("placement", ""))
                if nm:
                    placement_fee[nm] = pb.get("percentage", 0)
    finally:
        c0.close()

    conn = _get_conn()
    try:
        where = ["report_type = 'spCampaignsPlacement'", "campaign_id = %s",
                 "report_date BETWEEN %s AND %s"]
        params = [campaign_id, start_date, end_date]
        if shop_id:
            where.append("shop_id = %s"); params.append(shop_id)

        where_sql = "WHERE " + " AND ".join(where)
        with conn.cursor() as c:
            c.execute(f"""
                SELECT placement, COALESCE(SUM(impressions),0) impressions,
                COALESCE(SUM(clicks),0) clicks, COALESCE(SUM(cost),0) cost,
                COALESCE(SUM(purchases_7d),0) purchases_7d,
                COALESCE(SUM(purchases_14d),0) purchases_14d,
                COALESCE(SUM(purchases_30d),0) purchases_30d,
                COALESCE(SUM(sales_7d),0) sales_7d,
                COALESCE(SUM(sales_14d),0) sales_14d,
                COALESCE(SUM(sales_30d),0) sales_30d,
                COALESCE(MAX(top_of_search_impression_share),0) top_of_search_impression_share
                FROM amazon_ads_raw_reports
                {where_sql} GROUP BY placement ORDER BY cost DESC
            """, params)
            rows = list(c.fetchall())

        existing = {r["placement"] for r in rows}
        for rn in ("Top of Search on-Amazon", "Other on-Amazon", "Detail Page on-Amazon"):
            if rn not in existing:
                rows.append({"placement": rn, "impressions":0,"clicks":0,"cost":0,
                    "purchases_7d":0,"purchases_14d":0,"purchases_30d":0,
                    "sales_7d":0,"sales_14d":0,"sales_30d":0})

        list_data = []
        for r in rows:
            d = dict(r)
            d["percentage"] = placement_fee.get(d["placement"], 0)
            _compute_metrics(d)
            list_data.append(d)

        return jsonify({"status": "success", "data": {"list": list_data}})
    finally:
        conn.close()


@advertising_manage_bp.route('/campaigns/<int:campaign_id>/placement', methods=['PUT'])
def update_placement_bid(campaign_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT bidding FROM amazon_ads_campaigns WHERE campaign_id = %s", (campaign_id,))
            row = c.fetchone()
    finally:
        conn.close()

    if not row or not row.get("bidding"):
        return jsonify({"status": "error", "message": "活动无 bidding 配置"}), 400

    bidding = row["bidding"]
    if isinstance(bidding, str):
        try:
            bidding = json.loads(bidding)
        except json.JSONDecodeError:
            bidding = {}

    placement_bidding = bidding.get("placementBidding", [])
    placement_name = body.get("placement")
    # 纠正前端可能的错误 placement 值
    if placement_name in _PLACEMENT_FIX:
        placement_name = _PLACEMENT_FIX[placement_name]
    percentage = body.get("percentage")
    if not placement_name or percentage is None:
        return jsonify({"status": "error", "message": "缺少 placement 或 percentage"}), 400

    updated = False
    for pb in placement_bidding:
        if pb.get("placement") == placement_name:
            pb["percentage"] = int(percentage)
            updated = True
            break
    if not updated:
        placement_bidding.append({"placement": placement_name, "percentage": int(percentage)})
    bidding["placementBidding"] = placement_bidding

    try:
        client = get_ads_api_client(int(shop_id))
        client.update_campaign(campaign_id, {"bidding": bidding})
    except Exception as e:
        _log_operation(shop_id, campaign_id, "placement", campaign_id,
                       "更新出价失败", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    conn2 = _get_conn()
    try:
        with conn2.cursor() as c:
            c.execute("UPDATE amazon_ads_campaigns SET bidding = %s WHERE campaign_id = %s",
                      (json.dumps(bidding), campaign_id))
        conn2.commit()
    finally:
        conn2.close()

    _log_operation(shop_id, campaign_id, "placement", campaign_id,
                   f"更新广告位出价: {placement_name} -> {percentage}%")
    return jsonify({"status": "success", "message": "更新成功"})


# =================================================================
#  前端路由 — 产品广告
# =================================================================

@advertising_manage_bp.route('/product-ads', methods=['GET'])
def list_product_ads():
    campaign_id = request.args.get("campaign_id", "").strip()
    ad_group_id = request.args.get("ad_group_id", "").strip()
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    page, page_size = _parse_pagination()

    # 区间筛选
    _RANGE_OPS = [("_gt", ">"), ("_gte", ">="), ("_lt", "<"), ("_lte", "<=")]
    _RAW_FIELDS = {
        "impressions":   "SUM(r.impressions)",
        "clicks":        "SUM(r.clicks)",
        "cost":          "SUM(r.cost)",
        "purchases_7d":  "SUM(r.purchases_7d)",
        "sales_7d":      "SUM(r.sales_7d)",
    }
    _COMPUTED_FIELDS = {
        "ctr":  "COALESCE(SUM(r.clicks),0) / NULLIF(COALESCE(SUM(r.impressions),0), 0) * 100",
        "cpc":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0)",
        "cvr":  "COALESCE(SUM(r.purchases_7d),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0) * 100",
        "cpa":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.purchases_7d),0), 0)",
        "acos": "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.sales_7d),0), 0) * 100",
        "roas": "COALESCE(SUM(r.sales_7d),0) / NULLIF(COALESCE(SUM(r.cost),0), 0)",
    }
    _ALL_METRICS = {**_RAW_FIELDS, **_COMPUTED_FIELDS}

    having_clauses = []
    for field, expr in _ALL_METRICS.items():
        for suffix, op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try:
                    float(val)
                except ValueError:
                    continue
                having_clauses.append(f"({expr}) {op} %s")
    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

    having_params = []
    for field in _ALL_METRICS:
        for suffix, _op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try:
                    having_params.append(float(val))
                except ValueError:
                    pass

    conn = _get_conn()
    try:
        # 报表条件放 ON 子句
        report_on = ["r.report_type = 'spAdvertisedProduct'",
                     "r.report_date BETWEEN %s AND %s"]
        params = [start_date, end_date]
        join_where = f"e.campaign_id = r.campaign_id AND e.asin = r.advertised_asin AND {' AND '.join(report_on)}"

        if shop_id:
            join_where += " AND r.shop_id = %s"; params.append(shop_id)

        # 实体条件放 WHERE 子句
        entity_where = []
        if shop_id:
            entity_where.append("e.campaign_id IN (SELECT campaign_id FROM amazon_ads_campaigns WHERE shop_id = %s)")
            params.append(shop_id)
        if campaign_id:
            entity_where.append("e.campaign_id = %s"); params.append(campaign_id)
        if ad_group_id:
            entity_where.append("e.ad_group_id = %s"); params.append(ad_group_id)
        if search:
            entity_where.append("(e.asin LIKE %s OR e.sku LIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        if state:
            if state == "unarchived":
                entity_where.append("e.state != %s"); params.append("archived")
            else:
                entity_where.append("e.state = %s"); params.append(state)

        where_sql = ("WHERE " + " AND ".join(entity_where)) if entity_where else ""

        base_from = f"""FROM amazon_ads_product_ads e
                 LEFT JOIN amazon_ads_raw_reports r ON {join_where}
                 LEFT JOIN amazon_listings l ON l.asin = e.asin COLLATE utf8mb4_unicode_ci
                 {where_sql}"""
        base_group = "GROUP BY e.ad_id"

        with conn.cursor() as c:
            # 总数
            if having_sql:
                count_sql = f"""
                    SELECT COUNT(*) AS total FROM (
                        SELECT 1
                        {base_from}
                        {base_group}
                        {having_sql}
                    ) t
                """
                c.execute(count_sql, params + having_params)
            else:
                count_sql = f"""
                    SELECT COUNT(DISTINCT e.ad_id) AS total
                    {base_from}
                """
                c.execute(count_sql, params)
            total = c.fetchone()["total"]

            # 分页数据
            c.execute(f"""
                SELECT
                    e.ad_id,
                    MIN(e.asin) AS asin,
                    MIN(e.sku) AS sku,
                    MIN(e.state) AS state,
                    MIN(e.serving_status) AS serving_status,
                    MIN(e.campaign_id) AS campaign_id,
                    MIN(e.ad_group_id) AS ad_group_id,
                    MIN(l.main_image_url) AS main_image_url,
                    MIN(l.item_name) AS item_name,
                    COALESCE(SUM(r.impressions), 0) AS impressions,
                    COALESCE(SUM(r.clicks), 0) AS clicks,
                    COALESCE(SUM(r.cost), 0) AS cost,
                    COALESCE(SUM(r.purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(r.sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share
                {base_from}
                {base_group}
                {having_sql}
                ORDER BY cost DESC
                LIMIT %s OFFSET %s
            """, params + having_params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()

        list_data = [_compute_metrics(dict(r)) for r in rows]
        for item in list_data:
            for k in ("impressions", "clicks", "cost", "purchases_7d", "sales_7d",
                       "cpc", "ctr", "cvr", "cpa", "acos"):
                v = item.get(k)
                if v is not None:
                    try:
                        item[k] = float(v) if "." in str(v) or k in ("cpc", "ctr", "cvr", "cpa", "acos") else int(float(v))
                    except (ValueError, TypeError):
                        pass
        return jsonify({"status": "success", "data": {"list": list_data, "total": total}})
    finally:
        conn.close()


@advertising_manage_bp.route('/product-ads/<int:ad_id>', methods=['PUT'])
def update_product_ad(ad_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    state = body.get("state")
    if not shop_id or not state:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 state"}), 400
    api_state = state.upper() if state else state
    if api_state == "ARCHIVED":
        api_state = "PAUSED"
    try:
        client = get_ads_api_client(int(shop_id))
        client.update_product_ad(ad_id, {"state": api_state})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("UPDATE amazon_ads_product_ads SET state = %s WHERE ad_id = %s", (state, ad_id))
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "product_ad", ad_id, f"状态 -> {state}")
    return jsonify({"status": "success", "message": "更新成功"})


# =================================================================
#  前端路由 — 关键词
# =================================================================

@advertising_manage_bp.route('/keywords', methods=['GET'])
def list_keywords():
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    campaign_id = request.args.get("campaign_id", "").strip() or None
    ad_group_id = request.args.get("ad_group_id", "").strip() or None
    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    sort_by = request.args.get("sort_by", "cost").strip()
    sort_dir = request.args.get("sort_dir", "desc").strip()
    page, page_size = _parse_pagination()

    _RANGE_OPS = [("_gt", ">"), ("_gte", ">="), ("_lt", "<"), ("_lte", "<=")]
    _RAW_FIELDS = {
        "impressions":  "SUM(r.impressions)",
        "clicks":       "SUM(r.clicks)",
        "cost":         "SUM(r.cost)",
        "purchases_7d": "SUM(r.purchases_7d)",
        "sales_7d":     "SUM(r.sales_7d)",
    }
    _COMPUTED_FIELDS = {
        "ctr":  "COALESCE(SUM(r.clicks),0) / NULLIF(COALESCE(SUM(r.impressions),0), 0) * 100",
        "cpc":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0)",
        "cvr":  "COALESCE(SUM(r.purchases_7d),0) / NULLIF(COALESCE(SUM(r.clicks),0), 0) * 100",
        "cpa":  "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.purchases_7d),0), 0)",
        "acos": "COALESCE(SUM(r.cost),0) / NULLIF(COALESCE(SUM(r.sales_7d),0), 0) * 100",
        "roas": "COALESCE(SUM(r.sales_7d),0) / NULLIF(COALESCE(SUM(r.cost),0), 0)",
    }
    _ALL_METRICS = {**_RAW_FIELDS, **_COMPUTED_FIELDS}

    having_clauses = []
    for field, expr in _ALL_METRICS.items():
        for suffix, op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try: float(val)
                except ValueError: continue
                having_clauses.append(f"({expr}) {op} %s")
    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""
    having_params = []
    for field in _ALL_METRICS:
        for suffix, _op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try: having_params.append(float(val))
                except ValueError: pass

    allowed_sort = {"cost", "impressions", "clicks", "cpc", "ctr", "cvr", "cpa", "acos",
                    "sales_7d", "purchases_7d", "keyword_text", "match_type", "state", "serving_status", "bid"}
    if sort_by not in allowed_sort:
        sort_by = "cost"
    sort_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    conn = _get_conn()
    try:
        # 报表条件放 ON 子句
        report_on = ["r.report_type = 'spTargeting'",
                     "r.report_date BETWEEN %s AND %s"]
        params = [start_date, end_date]
        if shop_id:
            report_on.append("r.shop_id = %s"); params.append(shop_id)

        # 实体条件放 WHERE 子句
        entity_where = []
        if shop_id:
            entity_where.append("e.campaign_id IN (SELECT campaign_id FROM amazon_ads_campaigns WHERE shop_id = %s)")
            params.append(shop_id)
        if campaign_id:
            entity_where.append("e.campaign_id = %s"); params.append(campaign_id)
        if ad_group_id:
            entity_where.append("e.ad_group_id = %s"); params.append(ad_group_id)
        if search:
            entity_where.append("(e.keyword_text LIKE %s OR CAST(e.keyword_id AS CHAR) = %s)")
            params.extend([f"%{search}%", search])
        if state:
            if state == "unarchived":
                entity_where.append("e.state != %s"); params.append("archived")
            else:
                entity_where.append("e.state = %s"); params.append(state)

        on_sql = " AND ".join(report_on)
        where_sql = ("WHERE " + " AND ".join(entity_where)) if entity_where else ""

        entity_fields = [
            "MIN(e.keyword_id) AS keyword_id",
            "MIN(e.keyword_text) AS keyword_text",
            "MIN(e.match_type) AS match_type",
            "MIN(e.state) AS state",
            "MIN(e.bid) AS bid",
            "MIN(e.serving_status) AS serving_status",
            "MIN(e.campaign_id) AS campaign_id",
            "MIN(e.ad_group_id) AS ad_group_id",
        ]

        base_from = f"""FROM amazon_ads_keywords e
                 LEFT JOIN amazon_ads_raw_reports r ON r.keyword_id = e.keyword_id AND {on_sql}
                 {where_sql}"""
        base_group = "GROUP BY e.keyword_id"

        with conn.cursor() as c:
            if having_sql:
                count_sql = f"SELECT COUNT(*) AS total FROM (SELECT 1 {base_from} {base_group} {having_sql}) t"
                c.execute(count_sql, params + having_params)
            else:
                count_sql = f"SELECT COUNT(DISTINCT e.keyword_id) AS total FROM amazon_ads_keywords e LEFT JOIN amazon_ads_raw_reports r ON r.keyword_id = e.keyword_id AND {on_sql} {where_sql}"
                c.execute(count_sql, params)
            total = c.fetchone()["total"]

            c.execute(f"""
                SELECT {', '.join(entity_fields)},
                    COALESCE(SUM(r.impressions), 0) AS impressions,
                    COALESCE(SUM(r.clicks), 0) AS clicks,
                    COALESCE(SUM(r.cost), 0) AS cost,
                    COALESCE(SUM(r.purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(r.sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(r.top_of_search_impression_share), 0) AS top_of_search_impression_share
                {base_from}
                {base_group}
                {having_sql}
                ORDER BY {sort_by} {sort_dir}
                LIMIT %s OFFSET %s
            """, params + having_params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()

        list_data = [_map_entity_row(dict(r)) for r in rows]
        for item in list_data:
            for k in ("impressions", "clicks", "cost", "purchases_7d", "sales_7d",
                       "cpc", "ctr", "cvr", "cpa", "acos", "bid"):
                v = item.get(k)
                if v is not None:
                    try:
                        item[k] = float(v) if "." in str(v) or k in ("cpc", "ctr", "cvr", "cpa", "acos", "bid") else int(float(v))
                    except (ValueError, TypeError):
                        pass
        return jsonify({"status": "success", "data": {"list": list_data, "total": total}})
    finally:
        conn.close()


@advertising_manage_bp.route('/keywords/<int:keyword_id>', methods=['PUT'])
def update_keyword(keyword_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    updates = {k: v for k, v in body.items() if k in ("state", "bid")}
    if updates.get("bid") is not None:
        updates["bid"] = float(updates["bid"])
    if not updates:
        return jsonify({"status": "error", "message": "无更新字段"}), 400

    db_state = updates.get("state")
    api_updates = dict(updates)
    if db_state and str(db_state).lower() == "archived":
        api_updates["state"] = "PAUSED"

    try:
        client = get_ads_api_client(int(shop_id))
        client.update_keyword(keyword_id, api_updates)
    except Exception as e:
        _log_operation(shop_id, 0, "keyword", keyword_id,
                       f"更新失败: {updates}", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            set_parts, vals = [], []
            if "state" in updates:
                set_parts.append("state = %s"); vals.append(updates["state"] if db_state else updates["state"])
            if "bid" in updates:
                set_parts.append("bid = %s"); vals.append(float(updates["bid"]))
            if set_parts:
                vals.append(keyword_id)
                c.execute(f"UPDATE amazon_ads_keywords SET {', '.join(set_parts)} WHERE keyword_id = %s", vals)
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "keyword", keyword_id, f"更新成功: {updates}")
    return jsonify({"status": "success", "message": "更新成功"})


@advertising_manage_bp.route('/keywords/bid-recommendations', methods=['POST'])
def keyword_bid_recommendations():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    campaign_id = body.get("campaign_id")
    ad_group_id = body.get("ad_group_id")
    keywords = body.get("keywords", [])
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400
    if not campaign_id or not ad_group_id:
        return jsonify({"status": "error", "message": "缺少 campaign_id 或 ad_group_id"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        payload = {
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "recommendationType": "TARGETING_EXPRESSION",
            "targetingExpressions": [
                {
                    "expressionType": "KEYWORD",
                    "keywordText": kw.get("keywordText"),
                    "matchType": kw.get("matchType"),
                }
                for kw in keywords
            ],
        }
        resp = client._request(
            "POST", "/sp/targets/bid/recommendations",
            json_data=payload,
        )
        recommendations = resp.get("bidRecommendations", [])
        result = []
        for rec in recommendations:
            expr = rec.get("targetingExpression") or {}
            item = {
                "keywordText": expr.get("keywordText"),
                "matchType": expr.get("matchType"),
                "suggestedBid": rec.get("suggestedBid"),
                "suggestedBidRange": {
                    "low": rec.get("rangeStart"),
                    "high": rec.get("rangeEnd"),
                },
            }
            result.append(item)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@advertising_manage_bp.route('/keywords/batch', methods=['POST'])
def create_keywords():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    campaign_id = body.get("campaign_id")
    ad_group_id = body.get("ad_group_id")
    keywords = body.get("keywords", [])
    if not shop_id or not keywords:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 keywords"}), 400

    transformed = []
    null_bid_texts = []
    for kw in keywords:
        item = {
            "campaignId": campaign_id,
            "adGroupId": ad_group_id,
            "keywordText": kw.get("keywordText"),
            "matchType": kw.get("matchType"),
            "state": kw.get("state", "enabled"),
        }
        bid = kw.get("bid")
        if bid is not None:
            item["bid"] = float(bid)
        else:
            null_bid_texts.append(kw.get("keywordText", ""))
        transformed.append(item)

    if null_bid_texts:
        return jsonify({
            "status": "error",
            "message": f"以下关键词缺少 bid: {', '.join(null_bid_texts)}。请先调用 /keywords/bid-recommendations 获取建议竞价",
        }), 400

    try:
        client = get_ads_api_client(int(shop_id))
        resp = client.create_keywords(transformed)
        _sync_created_keywords(resp, int(shop_id))
        return jsonify({"status": "success", "data": resp})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@advertising_manage_bp.route('/keywords/upload', methods=['POST'])
def upload_keywords():
    shop_id = request.form.get("shop_id")
    campaign_id = request.form.get("campaign_id")
    ad_group_id = request.form.get("ad_group_id")
    if not shop_id or not campaign_id or not ad_group_id:
        return jsonify({"status": "error", "message": "缺少 shop_id / campaign_id / ad_group_id"}), 400

    file = request.files.get("file")
    if not file:
        return jsonify({"status": "error", "message": "缺少上传文件"}), 400

    filename = (file.filename or "").lower()
    try:
        if filename.endswith(".csv"):
            content = file.read().decode("utf-8-sig")
            reader = csv.reader(io.StringIO(content))
            rows = [row for row in reader]
        else:
            wb = openpyxl.load_workbook(io.BytesIO(file.read()), data_only=True)
            ws = wb.active
            rows = [[str(c.value or "").strip() for c in row] for row in ws.iter_rows()]

        if not rows or len(rows) < 2:
            return jsonify({"status": "error", "message": "文件无数据"}), 400

        headers = [h.strip().lower() for h in rows[0]]
        kw_col = next((i for i, h in enumerate(headers) if h in ("keywordtext", "keyword", "关键词")), 0)
        mt_col = next((i for i, h in enumerate(headers) if h in ("matchtype", "match_type", "匹配类型")), 1)
        bid_col = next((i for i, h in enumerate(headers) if h in ("bid", "竞价")), 2)

        VALID_MATCH_TYPES = {"BROAD", "PHRASE", "EXACT"}
        keywords = []
        errors = []
        for idx, row in enumerate(rows[1:], start=2):
            kw_text = (row[kw_col] if kw_col < len(row) else "").strip()
            mt_raw = (row[mt_col] if mt_col < len(row) else "").strip()
            bid_raw = (row[bid_col] if bid_col < len(row) else "").strip()
            mt = mt_raw.upper()

            if not kw_text:
                continue
            if mt not in VALID_MATCH_TYPES:
                errors.append(f"第{idx}行: matchType 无效 ({mt_raw})，应为 BROAD/PHRASE/EXACT")
                continue

            item = {"keywordText": kw_text, "matchType": mt}
            if bid_raw:
                try:
                    item["bid"] = float(bid_raw)
                except ValueError:
                    errors.append(f"第{idx}行: bid 格式错误 ({bid_raw})")
                    continue
            keywords.append(item)

        if errors:
            return jsonify({"status": "error", "message": "; ".join(errors)}), 400
        if not keywords:
            return jsonify({"status": "error", "message": "未解析到有效的关键词数据"}), 400

        transformed = []
        null_bid_texts = []
        for kw in keywords:
            item = {
                "campaignId": int(campaign_id),
                "adGroupId": int(ad_group_id),
                "keywordText": kw["keywordText"],
                "matchType": kw["matchType"],
                "state": "enabled",
            }
            if "bid" in kw:
                item["bid"] = kw["bid"]
            else:
                null_bid_texts.append(kw["keywordText"])
            transformed.append(item)

        if null_bid_texts:
            return jsonify({
                "status": "error",
                "message": f"以下关键词缺少 bid，请在文件中填写: {', '.join(null_bid_texts)}",
            }), 400

        try:
            client = get_ads_api_client(int(shop_id))
            resp = client.create_keywords(transformed)
            _sync_created_keywords(resp, int(shop_id))
            return jsonify({"status": "success", "data": resp})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": f"文件解析失败: {str(e)}"}), 400


# =================================================================
#  前端路由 — 投放
# =================================================================

@advertising_manage_bp.route('/targets', methods=['GET'])
def list_targets(only_auto=False):
    entity_fields = """
        e.target_id, e.resolved_expression, e.state, e.bid,
        e.serving_status, e.campaign_id, e.ad_group_id,
        ag.default_bid AS ad_group_default_bid
    """
    # 报告用 close-match/loose-match/complements/substitutes，实体用 API 枚举值，需 CASE-WHEN 映射
    targeting_expr_map = (
        "CASE r.targeting_expression "
        "WHEN 'close-match' THEN 'QUERY_HIGH_REL_MATCHES' "
        "WHEN 'loose-match' THEN 'QUERY_BROAD_REL_MATCHES' "
        "WHEN 'complements' THEN 'ASIN_ACCESSORY_RELATED' "
        "WHEN 'substitutes' THEN 'ASIN_SUBSTITUTE_RELATED' "
        "ELSE r.targeting_expression END"
    )
    where_extra = ""
    if only_auto:
        where_extra = (
            "e.campaign_id IN (SELECT campaign_id FROM amazon_ads_campaigns "
            "WHERE targeting_type = 'AUTO')"
        )
    try:
        rows, total = _query_entity_with_report(
            entity_table="amazon_ads_targets",
            entity_join_col="resolved_expression",
            entity_fields=entity_fields,
            report_type="spTargeting",
            report_join_col=targeting_expr_map,
            metrics="",
            join_extra="AND r.campaign_id = e.campaign_id AND r.ad_group_id = e.ad_group_id",
            extra_left_join="LEFT JOIN amazon_ads_ad_groups ag ON ag.ad_group_id = e.ad_group_id",
            where_extra=where_extra,
        )
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    list_data = [_map_entity_row(r) for r in rows]
    return jsonify({
        "status": "success",
        "data": {"list": list_data, "total": total or len(list_data)},
    })


@advertising_manage_bp.route('/targets/auto', methods=['GET'])
def list_auto_targets():
    return list_targets(only_auto=True)


@advertising_manage_bp.route('/targets/<int:target_id>', methods=['PUT'])
def update_target(target_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    updates = {k: v for k, v in body.items() if k in ("state", "bid")}
    if updates.get("bid") is not None:
        updates["bid"] = float(updates["bid"])
    if not updates:
        return jsonify({"status": "error", "message": "无更新字段"}), 400

    db_state = updates.get("state")
    api_updates = dict(updates)
    if db_state and str(db_state).lower() == "archived":
        api_updates["state"] = "PAUSED"

    try:
        client = get_ads_api_client(int(shop_id))
        client.update_target(target_id, api_updates)
    except Exception as e:
        _log_operation(shop_id, 0, "target", target_id,
                       f"更新失败: {updates}", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            set_parts, vals = [], []
            if "state" in updates:
                set_parts.append("state = %s"); vals.append(updates["state"] if db_state else updates["state"])
            if "bid" in updates:
                set_parts.append("bid = %s"); vals.append(float(updates["bid"]))
            if set_parts:
                vals.append(target_id)
                c.execute(f"UPDATE amazon_ads_targets SET {', '.join(set_parts)} WHERE target_id = %s", vals)
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "target", target_id, f"更新成功: {updates}")
    return jsonify({"status": "success", "message": "更新成功"})


@advertising_manage_bp.route('/targets/bid-recommendations', methods=['POST'])
def target_bid_recommendations():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        resp = client.get_target_bid_recommendations(body.get("payload", {}))
        return jsonify({"status": "success", "data": resp})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@advertising_manage_bp.route('/targets/batch', methods=['POST'])
def create_targets():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    targets = body.get("targets", [])
    if not shop_id or not targets:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 targets"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        resp = client.create_targets(targets)
        campaign_id = targets[0].get("campaignId", 0) if targets else 0
        _log_operation(shop_id, campaign_id, "target", 0, f"批量创建 {len(targets)} 个投放")
        return jsonify({"status": "success", "data": resp})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# =================================================================
#  前端路由 — 否定关键词
# =================================================================

@advertising_manage_bp.route('/negative-keywords', methods=['GET'])
def list_negative_keywords():
    campaign_id = request.args.get("campaign_id", "").strip()
    ad_group_id = request.args.get("ad_group_id", "").strip()
    state = request.args.get("state", "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "message": "缺少 campaign_id"}), 400

    conn = _get_conn()
    try:
        where = ["campaign_id = %s"]
        params = [campaign_id]
        if ad_group_id:
            where.append("ad_group_id = %s"); params.append(ad_group_id)
        if state:
            if state == "unarchived":
                where.append("state != %s"); params.append("archived")
            else:
                where.append("state = %s"); params.append(state)
        where_sql = "WHERE " + " AND ".join(where)
        with conn.cursor() as c:
            c.execute(f"""
                SELECT keyword_id, campaign_id, ad_group_id, keyword_text,
                       match_type, state, last_update_datetime
                FROM amazon_ads_negative_keywords
                {where_sql}
                ORDER BY keyword_id DESC
            """, params)
            rows = c.fetchall()
        list_data = [_map_entity_row(dict(r)) for r in rows]
        return jsonify({"status": "success", "data": {"list": list_data}})
    finally:
        conn.close()


@advertising_manage_bp.route('/negative-keywords/<int:keyword_id>', methods=['PUT'])
def update_negative_keyword(keyword_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    state = body.get("state")
    if not shop_id or not state:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 state"}), 400
    # Amazon v3 API 不支持 ARCHIVED 状态（PUT 只接受 ENABLED/PAUSED，DELETE 端点 403 不可用）
    # 降级：对 Amazon 发 PAUSED，本地 DB 仍存 archived
    api_state = state.upper()
    if api_state == "ARCHIVED":
        api_state = "PAUSED"
    try:
        client = get_ads_api_client(int(shop_id))
        client.update_negative_keyword(keyword_id, {"state": api_state})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("UPDATE amazon_ads_negative_keywords SET state = %s WHERE keyword_id = %s", (state, keyword_id))
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "negative_keyword", keyword_id, f"状态 -> {state}")
    return jsonify({"status": "success", "message": "更新成功"})


@advertising_manage_bp.route('/negative-keywords', methods=['POST'])
def create_negative_keywords():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    keywords = body.get("keywords", [])
    if not shop_id or not keywords:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 keywords"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        resp = client.create_negative_keywords(keywords)
    except Exception as e:
        campaign_id = keywords[0].get("campaignId", 0) if keywords else 0
        _log_operation(shop_id, campaign_id, "negative_keyword", 0,
                       "添加失败", status=0, error_message=str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    # 写本地表
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            success = resp.get("negativeKeywords", {}).get("success", [])
            for s in success:
                kid = int(s.get("negativeKeywordId", 0))
                if not kid:
                    continue
                idx = s.get("index", 0)
                kw = keywords[idx] if idx < len(keywords) else {}
                c.execute("""
                    INSERT INTO amazon_ads_negative_keywords
                        (campaign_id, ad_group_id, keyword_id, keyword_text, match_type, state, synced_at)
                    VALUES (%s, %s, %s, %s, %s, 'ENABLED', NOW())
                    ON DUPLICATE KEY UPDATE state='ENABLED', keyword_text=VALUES(keyword_text),
                        match_type=VALUES(match_type), synced_at=NOW()
                """, (
                    int(kw.get("campaignId", 0)),
                    int(kw["adGroupId"]) if kw.get("adGroupId") else None,
                    kid, kw.get("keywordText", ""), kw.get("matchType", ""),
                ))
        conn.commit()
    finally:
        conn.close()

    campaign_id = keywords[0].get("campaignId", 0) if keywords else 0
    _log_operation(shop_id, campaign_id, "negative_keyword", 0,
                   f"批量添加 {len(keywords)} 个否定关键词")
    return jsonify({"status": "success", "data": resp})


# =================================================================
#  前端路由 — 否定投放
# =================================================================

@advertising_manage_bp.route('/negative-targets', methods=['GET'])
def list_negative_targets():
    campaign_id = request.args.get("campaign_id", "").strip()
    ad_group_id = request.args.get("ad_group_id", "").strip()
    state = request.args.get("state", "").strip()
    if not campaign_id:
        return jsonify({"status": "error", "message": "缺少 campaign_id"}), 400

    conn = _get_conn()
    try:
        where = ["campaign_id = %s"]
        params = [campaign_id]
        if ad_group_id:
            where.append("ad_group_id = %s"); params.append(ad_group_id)
        if state:
            if state == "unarchived":
                where.append("state != %s"); params.append("archived")
            else:
                where.append("state = %s"); params.append(state)
        where_sql = "WHERE " + " AND ".join(where)
        with conn.cursor() as c:
            c.execute(f"""
                SELECT target_id, campaign_id, ad_group_id, resolved_expression,
                       state, last_update_datetime
                FROM amazon_ads_negative_targets
                {where_sql}
                ORDER BY target_id DESC
            """, params)
            rows = c.fetchall()
        list_data = [dict(r) for r in rows]
        return jsonify({"status": "success", "data": {"list": list_data}})
    finally:
        conn.close()


@advertising_manage_bp.route('/negative-targets/<int:target_id>', methods=['PUT'])
def update_negative_target(target_id):
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    state = body.get("state")
    if not shop_id or not state:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 state"}), 400
    # Amazon v3 API 不支持 ARCHIVED（PUT 只接受 ENABLED/PAUSED，DELETE 端点 403）
    api_state = state.upper()
    if api_state == "ARCHIVED":
        api_state = "PAUSED"
    try:
        client = get_ads_api_client(int(shop_id))
        client.update_negative_target(target_id, {"state": api_state})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            c.execute("UPDATE amazon_ads_negative_targets SET state = %s WHERE target_id = %s", (state, target_id))
        conn.commit()
    finally:
        conn.close()

    _log_operation(shop_id, 0, "negative_target", target_id, f"状态 -> {state}")
    return jsonify({"status": "success", "message": "更新成功"})


@advertising_manage_bp.route('/negative-targets', methods=['POST'])
def create_negative_targets():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    targets = body.get("targets", [])
    if not shop_id or not targets:
        return jsonify({"status": "error", "message": "缺少 shop_id 或 targets"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        resp = client.create_negative_targets(targets)
        campaign_id = targets[0].get("campaignId", 0) if targets else 0
        _log_operation(shop_id, campaign_id, "negative_target", 0, f"批量添加 {len(targets)} 个否定投放")
        return jsonify({"status": "success", "data": resp})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# =================================================================
#  前端路由 — 搜索词
# =================================================================

@advertising_manage_bp.route('/search-terms', methods=['GET'])
def list_search_terms():
    campaign_id = request.args.get("campaign_id", "").strip()
    ad_group_id = request.args.get("ad_group_id", "").strip()
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()
    search = request.args.get("search", "").strip()
    targeting_type_label = request.args.get("targeting_type_label", "").strip()
    keyword_match_type = request.args.get("keyword_match_type", "").strip()
    page, page_size = _parse_pagination()

    # 区间筛选：支持 _gt / _gte / _lt / _lte 后缀
    _RANGE_OPS = [
        ("_gt", ">"),
        ("_gte", ">="),
        ("_lt", "<"),
        ("_lte", "<="),
    ]
    # 原始字段 → HAVING 表达式（聚合后名称即为别名）
    _RAW_FIELDS = {
        "impressions":   "SUM(impressions)",
        "clicks":        "SUM(clicks)",
        "cost":          "SUM(cost)",
        "purchases_7d":  "SUM(purchases_7d)",
        "sales_7d":      "SUM(sales_7d)",
    }
    # 计算字段 → HAVING 表达式
    _COMPUTED_FIELDS = {
        "ctr":  "COALESCE(SUM(clicks),0) / NULLIF(COALESCE(SUM(impressions),0), 0) * 100",
        "cpc":  "COALESCE(SUM(cost),0) / NULLIF(COALESCE(SUM(clicks),0), 0)",
        "cvr":  "COALESCE(SUM(purchases_7d),0) / NULLIF(COALESCE(SUM(clicks),0), 0) * 100",
        "cpa":  "COALESCE(SUM(cost),0) / NULLIF(COALESCE(SUM(purchases_7d),0), 0)",
        "acos": "COALESCE(SUM(cost),0) / NULLIF(COALESCE(SUM(sales_7d),0), 0) * 100",
        "roas": "COALESCE(SUM(sales_7d),0) / NULLIF(COALESCE(SUM(cost),0), 0)",
    }
    _ALL_METRICS = {**_RAW_FIELDS, **_COMPUTED_FIELDS}

    having_clauses = []
    for field, expr in _ALL_METRICS.items():
        for suffix, op in _RANGE_OPS:
            val = request.args.get(f"{field}{suffix}", "").strip()
            if val:
                try:
                    float(val)
                except ValueError:
                    continue
                having_clauses.append(f"({expr}) {op} %s")

    having_sql = (" HAVING " + " AND ".join(having_clauses)) if having_clauses else ""

    conn = _get_conn()
    try:
        where = ["report_type = 'spSearchTerm'", "report_date BETWEEN %s AND %s"]
        params = [start_date, end_date]
        if shop_id:
            where.append("shop_id = %s"); params.append(shop_id)
        if campaign_id:
            where.append("campaign_id = %s"); params.append(campaign_id)
        if ad_group_id:
            where.append("ad_group_id = %s"); params.append(ad_group_id)
        if search:
            where.append("customer_search_term LIKE %s"); params.append(f"%{search}%")
        if keyword_match_type:
            where.append("keyword_match_type = %s"); params.append(keyword_match_type)

        _EXPR_TO_KEYWORD = {
            "紧密匹配": "close-match",
            "宽泛匹配": "loose-match",
            "同类商品": "substitutes",
            "关联商品": "complements",
        }
        if targeting_type_label:
            kw = _EXPR_TO_KEYWORD.get(targeting_type_label, targeting_type_label)
            where.append("keyword_text = %s"); params.append(kw)

        where_sql = "WHERE " + " AND ".join(where)

        # 提取区间筛选参数值（按顺序与 having_clauses 对应）
        having_params = []
        for field in _ALL_METRICS:
            for suffix, _op in _RANGE_OPS:
                val = request.args.get(f"{field}{suffix}", "").strip()
                if val:
                    try:
                        having_params.append(float(val))
                    except ValueError:
                        pass

        base_from = f"FROM amazon_ads_raw_reports {where_sql}"
        base_group = "GROUP BY customer_search_term, campaign_id"

        with conn.cursor() as c:
            # 总数：有 HAVING 时用子查询
            if having_sql:
                count_sql = f"""
                    SELECT COUNT(*) AS total FROM (
                        SELECT 1
                        {base_from}
                        {base_group}
                        {having_sql}
                    ) t
                """
                c.execute(count_sql, params + having_params)
            else:
                count_sql = f"""
                    SELECT COUNT(DISTINCT CONCAT(customer_search_term, '|', campaign_id)) AS total
                    {base_from}
                """
                c.execute(count_sql, params)
            total = c.fetchone()["total"]

            # 分页数据
            c.execute(f"""
                SELECT
                    customer_search_term,
                    MIN(keyword_text) AS keyword_text,
                    MIN(keyword_match_type) AS keyword_match_type,
                    MIN(campaign_id) AS campaign_id,
                    MIN(campaign_name) AS campaign_name,
                    MIN(ad_group_id) AS ad_group_id,
                    MIN(ad_group_name) AS ad_group_name,
                    CASE MIN(keyword_text)
                        WHEN 'close-match' THEN '紧密匹配'
                        WHEN 'loose-match' THEN '宽泛匹配'
                        WHEN 'substitutes' THEN '同类商品'
                        WHEN 'complements' THEN '关联商品'
                        ELSE ''
                    END AS targeting_type_label,
                    COALESCE(SUM(impressions), 0) AS impressions,
                    COALESCE(SUM(clicks), 0) AS clicks,
                    COALESCE(SUM(cost), 0) AS cost,
                    COALESCE(SUM(purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(top_of_search_impression_share), 0) AS top_of_search_impression_share
                {base_from}
                {base_group}
                {having_sql}
                ORDER BY cost DESC
                LIMIT %s OFFSET %s
            """, params + having_params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()

        list_data = [_compute_metrics(dict(r)) for r in rows]
        for item in list_data:
            for k in ("impressions", "clicks", "cost", "purchases_7d", "sales_7d",
                       "cpc", "ctr", "cvr", "cpa", "acos"):
                v = item.get(k)
                if v is not None:
                    try:
                        item[k] = float(v) if "." in str(v) or k in ("cpc", "ctr", "cvr", "cpa", "acos") else int(float(v))
                    except (ValueError, TypeError):
                        pass
        return jsonify({"status": "success", "data": {"list": list_data, "total": total}})
    finally:
        conn.close()


# =================================================================
#  前端路由 — 分日数据
# =================================================================

@advertising_manage_bp.route('/daily-data', methods=['GET'])
def get_daily_data():
    entity_type = request.args.get("type", "").strip()
    entity_id = request.args.get("id", "").strip()
    start_date, end_date = _parse_date_range()
    shop_id = _get_shop_id_optional()

    conn = _get_conn()
    try:
        # ========== 1. 广告花费数据（原有逻辑） ==========
        report_rows = []
        if entity_type and entity_id:
            # targeting 特殊处理
            if entity_type == "target":
                with conn.cursor() as c:
                    c.execute(
                        "SELECT resolved_expression, campaign_id, ad_group_id FROM amazon_ads_targets WHERE target_id = %s",
                        (entity_id,)
                    )
                    target = c.fetchone()
                if not target:
                    return jsonify({"status": "error", "message": f"target {entity_id} 不存在"}), 404
                _REPORT_EXPR_MAP = {
                    "QUERY_HIGH_REL_MATCHES": "close-match",
                    "QUERY_BROAD_REL_MATCHES": "loose-match",
                    "ASIN_ACCESSORY_RELATED": "complements",
                    "ASIN_SUBSTITUTE_RELATED": "substitutes",
                }
                report_expr = _REPORT_EXPR_MAP.get(target["resolved_expression"], target["resolved_expression"])
                where = [
                    "report_type = %s", "report_date BETWEEN %s AND %s",
                    "targeting_expression = %s", "campaign_id = %s", "ad_group_id = %s",
                ]
                params = ["spTargeting", start_date, end_date, report_expr,
                          str(target["campaign_id"]), str(target["ad_group_id"])]
            else:
                type_config = {
                    "campaign": ("spCampaigns", "campaign_id"),
                    "adgroup": ("spAdvertisedProduct", "ad_group_id"),
                    "keyword": ("spTargeting", "keyword_id"),
                    "searchterm": ("spSearchTerm", "customer_search_term"),
                    "placement": ("spCampaignsPlacement", "placement"),
                }
                cfg = type_config.get(entity_type)
                if not cfg:
                    return jsonify({"status": "error", "message": f"不支持的类型: {entity_type}"}), 400
                report_type, id_col = cfg
                where = ["report_type = %s", "report_date BETWEEN %s AND %s", f"{id_col} = %s"]
                params = [report_type, start_date, end_date, entity_id]
        else:
            # 全量聚合：所有 campaign 的 spCampaigns 数据
            where = ["report_type = 'spCampaigns'", "report_date BETWEEN %s AND %s"]
            params = [start_date, end_date]

        if shop_id:
            where.append("shop_id = %s"); params.append(shop_id)
        where_sql = "WHERE " + " AND ".join(where)

        with conn.cursor() as c:
            c.execute(f"""
                SELECT
                    report_date,
                    COALESCE(SUM(impressions), 0) AS impressions,
                    COALESCE(SUM(clicks), 0) AS clicks,
                    COALESCE(SUM(cost), 0) AS cost,
                    COALESCE(SUM(purchases_7d), 0) AS purchases_7d,
                    COALESCE(SUM(sales_7d), 0) AS sales_7d,
                    COALESCE(MAX(top_of_search_impression_share), 0) AS top_of_search_impression_share
                FROM amazon_ads_raw_reports
                {where_sql}
                GROUP BY report_date
                ORDER BY report_date ASC
            """, params)
            report_rows = c.fetchall()

        list_data = [_compute_metrics(dict(r)) for r in report_rows]
        return jsonify({"status": "success", "data": {"list": list_data}})
    finally:
        conn.close()


# =================================================================
#  前端路由 — 广告结构
# =================================================================

@advertising_manage_bp.route('/structure/tree', methods=['GET'])
def structure_tree():
    shop_id = _get_shop_id_optional()

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            where = ""
            params = []
            if shop_id:
                where = "WHERE shop_id = %s"
                params.append(shop_id)

            c.execute(f"SELECT * FROM amazon_ads_campaigns {where} ORDER BY name", params)
            campaigns = c.fetchall()

            tree = []
            for camp in campaigns:
                cid = camp["campaign_id"]
                camp_data = _map_entity_row(camp)
                node = {
                    "id": f"campaign_{cid}",
                    "label": camp["name"] or f"Campaign #{cid}",
                    "type": "campaign",
                    "data": camp_data,
                    "children": [],
                }

                c.execute("SELECT * FROM amazon_ads_ad_groups WHERE campaign_id = %s", (cid,))
                for ag in c.fetchall():
                    ag_id = ag["ad_group_id"]
                    ag_node = {
                        "id": f"adgroup_{ag_id}",
                        "label": ag["name"] or f"AdGroup #{ag_id}",
                        "type": "ad_group",
                        "data": dict(ag),
                        "children": [],
                    }

                    c.execute("SELECT keyword_id, keyword_text, match_type, state FROM amazon_ads_keywords WHERE ad_group_id = %s LIMIT 20", (ag_id,))
                    for kw in c.fetchall():
                        ag_node["children"].append({
                            "id": f"keyword_{kw['keyword_id']}",
                            "label": kw["keyword_text"],
                            "type": "keyword",
                            "data": dict(kw),
                        })

                    c.execute("SELECT target_id, resolved_expression, state FROM amazon_ads_targets WHERE ad_group_id = %s LIMIT 20", (ag_id,))
                    for tgt in c.fetchall():
                        ag_node["children"].append({
                            "id": f"target_{tgt['target_id']}",
                            "label": tgt["resolved_expression"],
                            "type": "target",
                            "data": dict(tgt),
                        })

                    c.execute("SELECT ad_id, asin, sku FROM amazon_ads_product_ads WHERE ad_group_id = %s LIMIT 20", (ag_id,))
                    for pa in c.fetchall():
                        ag_node["children"].append({
                            "id": f"productad_{pa['ad_id']}",
                            "label": pa["asin"] or pa["sku"],
                            "type": "product_ad",
                            "data": dict(pa),
                        })

                    node["children"].append(ag_node)
                tree.append(node)

        return jsonify({"status": "success", "data": tree})
    finally:
        conn.close()


# =================================================================
#  前端路由 — 创建广告活动向导
# =================================================================

@advertising_manage_bp.route('/create/portfolios', methods=['GET'])
def create_portfolios():
    shop_id = request.args.get("shop_id", "").strip()
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400
    try:
        client = get_ads_api_client(int(shop_id))
        portfolios = client.list_portfolios()
        return jsonify({"status": "success", "data": portfolios})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@advertising_manage_bp.route('/create/products', methods=['GET'])
def create_search_products():
    shop_id = request.args.get("shop_id", "").strip()
    search = request.args.get("search", "").strip()
    search_type = request.args.get("search_type", "ASIN").strip().upper()

    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    conn = _get_conn()
    try:
        with conn.cursor() as c:
            if search_type == "SELLER_SKU":
                c.execute("""
                    SELECT asin, sku, item_name, main_image_url, list_price
                    FROM amazon_listings
                    WHERE shop_id = %s AND sku LIKE %s
                    ORDER BY sku LIMIT 50
                """, (shop_id, f"%{search}%"))
            else:
                c.execute("""
                    SELECT asin, sku, item_name, main_image_url, list_price
                    FROM amazon_listings
                    WHERE shop_id = %s AND asin LIKE %s
                    ORDER BY asin LIMIT 50
                """, (shop_id, f"%{search}%"))
            rows = c.fetchall()

        return jsonify({"status": "success", "data": [dict(r) for r in rows]})
    finally:
        conn.close()


@advertising_manage_bp.route('/create-campaign', methods=['POST'])
def create_campaign_wizard():
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400

    try:
        client = get_ads_api_client(int(shop_id))
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

    data1 = body.get("data1", {})
    data2 = body.get("data2", {})
    data3 = body.get("data3", {})
    campaign_id = body.get("campaign_id") or None

    result = []
    ad_ids = []
    ad_group_id = None
    has_error = False

    if not campaign_id and data1 and data2:
        try:
            campaign_req = {
                "name": data1.get("name") or f"Auto Campaign {datetime.now().strftime('%Y%m%d%H%M')}",
                "campaignType": "sponsoredProducts",
                "targetingType": data1.get("targetingType", "AUTO"),
                "state": "enabled",
                "dynamicBidding": {
                    "strategy": data1.get("biddingStrategy", "LEGACY_FOR_SALES"),
                },
                "dailyBudget": float(data1.get("dailyBudget", 1)),
                "startDate": data1.get("startDate", datetime.now().strftime("%Y-%m-%d")),
            }
            if data1.get("endDate"):
                campaign_req["endDate"] = data1["endDate"]
            if data1.get("portfolioId"):
                campaign_req["portfolioId"] = int(data1["portfolioId"])

            placement_bidding = []
            for key, placement_name in [("topPer", "PLACEMENT_TOP_OF_SEARCH"),
                                         ("restPer", "PLACEMENT_REST_OF_SEARCH"),
                                         ("productPer", "PLACEMENT_PRODUCT_PAGE")]:
                val = data1.get(key)
                if val is not None and val != "" and int(val) != 0:
                    placement_bidding.append({"placement": placement_name, "percentage": int(val)})
            if placement_bidding:
                campaign_req["dynamicBidding"]["placementBidding"] = placement_bidding

            camp_resp = client.create_campaigns([campaign_req])
            first = (camp_resp.get("campaigns") or [{}])[0]
            campaign_id = int(first.get("campaignId", 0))
            if not campaign_id:
                raise ValueError(f"创建活动失败: {camp_resp}")
            result.append({"type": "campaign", "id": campaign_id, "status": "success"})
        except Exception as e:
            has_error = True
            result.append({"type": "campaign", "status": "error", "error": str(e)})
            _log_operation(shop_id, 0, "campaign", 0, "创建失败", status=0, error_message=str(e))
            return jsonify({"status": "error", "message": str(e), "details": result}), 500

        ad_group_id = None
        if campaign_id:
            try:
                ag_req = {
                    "name": data2.get("name") or f"AdGroup {datetime.now().strftime('%H%M%S')}",
                    "campaignId": campaign_id,
                    "state": "enabled",
                    "defaultBid": float(data2.get("defaultBid", 0.02)),
                }
                ag_resp = client.create_ad_groups([ag_req])
                first_ag = (ag_resp.get("adGroups") or [{}])[0]
                ad_group_id = int(first_ag.get("adGroupId", 0))
                result.append({"type": "ad_group", "id": ad_group_id, "status": "success"})
            except Exception as e:
                has_error = True
                result.append({"type": "ad_group", "status": "error", "error": str(e)})

        products = data2.get("products", [])
        ad_ids = []
        if ad_group_id and products:
            try:
                pa_list = []
                for p in products:
                    pa_list.append({
                        "campaignId": campaign_id,
                        "adGroupId": ad_group_id,
                        "sku": p.get("sku", ""),
                        "asin": p.get("asin", ""),
                        "state": "enabled",
                    })
                pa_resp = client.create_product_ads(pa_list)
                pa = pa_resp.get("productAds", {})
                if isinstance(pa, dict):
                    for s in pa.get("success", []):
                        ad_ids.append(int(s.get("adId", 0)))
                else:
                    for pa_item in pa:
                        ad_ids.append(int(pa_item.get("adId", 0)))
                result.append({"type": "product_ads", "count": len(pa_list), "status": "success"})
            except Exception as e:
                has_error = True
                result.append({"type": "product_ads", "status": "error", "error": str(e)})

    if data3 and campaign_id:
        targeting_type = data1.get("targetingType", "")
        if targeting_type == "AUTO":
            auto_targets = data3.get("autoTargets", [])
            for at in auto_targets:
                try:
                    client.update_target(int(at["targetId"]), {
                        "state": at.get("state", "enabled"),
                        "bid": float(at.get("bid", 0.02)),
                    })
                except Exception as e:
                    result.append({"type": "auto_target", "targetId": at.get("targetId"),
                                   "status": "error", "error": str(e)})
        else:
            keywords = data3.get("keywords", [])
            if keywords:
                try:
                    kw_list = []
                    for k in keywords:
                        kw_list.append({
                            "campaignId": campaign_id,
                            "adGroupId": ad_group_id,
                            "keywordText": k["keywordText"],
                            "matchType": k.get("matchType", "EXACT"),
                            "state": "enabled",
                            "bid": float(k.get("bid", data2.get("defaultBid", 0.02))),
                        })
                    client.create_keywords(kw_list)
                    result.append({"type": "keywords", "count": len(kw_list), "status": "success"})
                except Exception as e:
                    result.append({"type": "keywords", "status": "error", "error": str(e)})

            neg_keywords = data3.get("negativeKeywords", [])
            if neg_keywords:
                try:
                    client.create_negative_keywords(neg_keywords)
                    result.append({"type": "negative_keywords", "count": len(neg_keywords), "status": "success"})
                except Exception as e:
                    result.append({"type": "negative_keywords", "status": "error", "error": str(e)})

            targets = data3.get("targets", [])
            if targets:
                try:
                    client.create_targets(targets)
                    result.append({"type": "targets", "count": len(targets), "status": "success"})
                except Exception as e:
                    result.append({"type": "targets", "status": "error", "error": str(e)})

            neg_targets = data3.get("negativeTargets", [])
            if neg_targets:
                try:
                    client.create_negative_targets(neg_targets)
                    result.append({"type": "negative_targets", "count": len(neg_targets), "status": "success"})
                except Exception as e:
                    result.append({"type": "negative_targets", "status": "error", "error": str(e)})

    _log_operation(shop_id, campaign_id or 0, "campaign", campaign_id or 0,
                   f"创建向导完成: {json.dumps(result, ensure_ascii=False)[:500]}")
    return jsonify({
        "status": "success",
        "data": {"campaign_id": campaign_id, "ad_group_id": ad_group_id, "ad_ids": ad_ids, "results": result},
    }), (200 if not has_error else 200)


# =================================================================
#  后端同步 — 实体状态同步服务
# =================================================================

def _safe_str(val, default=""):
    if val is None:
        return default
    return str(val)


def _safe_decimal(val):
    if val is None:
        return None
    return float(val)


def _safe_date(val):
    if val is None or val == "":
        return None
    return str(val)[:10]


def _safe_json(val):
    if val is None:
        return None
    return json.dumps(val, ensure_ascii=False)


def _paginate_all(client, list_fn, **kwargs):
    all_items = []
    next_token = None
    while True:
        kwargs["next_token"] = next_token
        resp = list_fn(**kwargs)
        items = resp.get("results", resp.get("campaigns", resp.get("adGroups",
                    resp.get("productAds", resp.get("keywords",
                    resp.get("targetingClauses", resp.get("negativeKeywords",
                    resp.get("negativeTargetingClauses", []))))))))
        if items:
            all_items.extend(items)
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return all_items


def _sync_campaigns(cursor, client, shop_id, profile_id):
    campaigns = _paginate_all(client, client.list_sp_campaigns, max_results=100)
    for item in campaigns:
        bid = item.get("dynamicBidding") or item.get("bidding")
        # v3 API: budget 是嵌套对象 { budget, budgetType }
        budget_obj = item.get("budget", {}) or {}
        daily_budget = budget_obj.get("budget") if isinstance(budget_obj, dict) else None
        # v3 list 不返回 servingStatus，使用 state 作为回退
        serving_status = item.get("servingStatus") or ""
        cursor.execute("""
            INSERT INTO amazon_ads_campaigns
                (shop_id, profile_id, campaign_id, name, campaign_type, targeting_type,
                 state, serving_status, daily_budget, start_date, end_date,
                 bidding, portfolio_id, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                name=VALUES(name), campaign_type=VALUES(campaign_type),
                targeting_type=VALUES(targeting_type), state=VALUES(state),
                serving_status=VALUES(serving_status), daily_budget=VALUES(daily_budget),
                start_date=VALUES(start_date), end_date=VALUES(end_date),
                bidding=VALUES(bidding), portfolio_id=VALUES(portfolio_id),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            shop_id, profile_id,
            int(item["campaignId"]), _safe_str(item.get("name")),
            _safe_str(item.get("campaignType", "sponsoredProducts")),
            _safe_str(item.get("targetingType")),
            _safe_str(item.get("state")), _safe_str(serving_status),
            _safe_decimal(daily_budget),
            _safe_date(item.get("startDate")), _safe_date(item.get("endDate")),
            _safe_json(bid),
            int(item["portfolioId"]) if item.get("portfolioId") else None,
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(campaigns)


def _sync_ad_groups(cursor, client, shop_id):
    ad_groups = _paginate_all(client, client.list_ad_groups, max_results=100)
    for item in ad_groups:
        cursor.execute("""
            INSERT INTO amazon_ads_ad_groups
                (campaign_id, ad_group_id, name, state, default_bid,
                 serving_status, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s,%s, %s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), name=VALUES(name),
                state=VALUES(state), default_bid=VALUES(default_bid),
                serving_status=VALUES(serving_status),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]),
            int(item["adGroupId"]), _safe_str(item.get("name")),
            _safe_str(item.get("state")), _safe_decimal(item.get("defaultBid")),
            _safe_str(item.get("servingStatus")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(ad_groups)


def _sync_product_ads(cursor, client, shop_id):
    product_ads = _paginate_all(client, client.list_product_ads, max_results=100)
    for item in product_ads:
        cursor.execute("""
            INSERT INTO amazon_ads_product_ads
                (campaign_id, ad_group_id, ad_id, asin, sku,
                 state, serving_status, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), ad_group_id=VALUES(ad_group_id),
                asin=VALUES(asin), sku=VALUES(sku),
                state=VALUES(state), serving_status=VALUES(serving_status),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]) if item.get("campaignId") else 0,
            int(item["adGroupId"]) if item.get("adGroupId") else 0,
            int(item["adId"]),
            _safe_str(item.get("asin")), _safe_str(item.get("sku")),
            _safe_str(item.get("state")), _safe_str(item.get("servingStatus")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(product_ads)


def _sync_keywords(cursor, client, shop_id):
    keywords = _paginate_all(client, client.list_keywords, max_results=100)
    for item in keywords:
        cursor.execute("""
            INSERT INTO amazon_ads_keywords
                (campaign_id, ad_group_id, keyword_id, keyword_text, match_type,
                 state, bid, serving_status, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), ad_group_id=VALUES(ad_group_id),
                keyword_text=VALUES(keyword_text), match_type=VALUES(match_type),
                state=VALUES(state), bid=VALUES(bid),
                serving_status=VALUES(serving_status),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]),
            int(item["adGroupId"]),
            int(item["keywordId"]),
            _safe_str(item.get("keywordText")),
            _safe_str(item.get("matchType")),
            _safe_str(item.get("state")),
            _safe_decimal(item.get("bid")),
            _safe_str(item.get("servingStatus")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(keywords)


def _sync_created_keywords(resp, shop_id):
    """将批量创建的关键词响应写入本地 DB"""
    keywords = resp.get("keywords", {})
    success_items = keywords.get("success", []) or resp.get("success", [])
    if not success_items:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as c:
            for item in success_items:
                c.execute("""
                    INSERT INTO amazon_ads_keywords
                        (campaign_id, ad_group_id, keyword_id, keyword_text,
                         match_type, state, bid, serving_status,
                         last_update_datetime, synced_at)
                    VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        keyword_text=VALUES(keyword_text),
                        match_type=VALUES(match_type),
                        state=VALUES(state), bid=VALUES(bid),
                        serving_status=VALUES(serving_status),
                        last_update_datetime=VALUES(last_update_datetime),
                        synced_at=NOW()
                """, (
                    int(item.get("campaignId", 0)),
                    int(item.get("adGroupId", 0)),
                    int(item["keywordId"]),
                    _safe_str(item.get("keywordText")),
                    _safe_str(item.get("matchType")),
                    _safe_str(item.get("state")),
                    _safe_decimal(item.get("bid")),
                    _safe_str(item.get("servingStatus")),
                    _safe_date(item.get("lastUpdateDateTime")),
                ))
        conn.commit()
    finally:
        conn.close()


def _sync_targets(cursor, client, shop_id):
    targets = _paginate_all(client, client.list_targets, max_results=100)
    for item in targets:
        expr = item.get("expression") or item.get("expressions") or []
        resolved = item.get("resolvedExpression") or item.get(
            "resolvedExpressionsTextDelimited", "")
        if isinstance(resolved, (list, tuple)):
            parts = []
            for x in resolved:
                if isinstance(x, dict):
                    parts.append(x.get("type") or x.get("name") or json.dumps(x))
                else:
                    parts.append(str(x))
            resolved = ", ".join(parts)
        elif isinstance(resolved, dict):
            resolved = resolved.get("type") or resolved.get("name") or json.dumps(resolved)
        cursor.execute("""
            INSERT INTO amazon_ads_targets
                (campaign_id, ad_group_id, target_id, expression,
                 resolved_expression, state, bid,
                 serving_status, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), ad_group_id=VALUES(ad_group_id),
                expression=VALUES(expression),
                resolved_expression=VALUES(resolved_expression),
                state=VALUES(state), bid=VALUES(bid),
                serving_status=VALUES(serving_status),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]),
            int(item["adGroupId"]),
            int(item["targetId"]),
            _safe_json(expr),
            _safe_str(resolved),
            _safe_str(item.get("state")),
            _safe_decimal(item.get("bid")),
            _safe_str(item.get("servingStatus")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(targets)


def _sync_negative_keywords(cursor, client, shop_id):
    neg_kws = _paginate_all(client, client.list_negative_keywords, max_results=100)
    for item in neg_kws:
        cursor.execute("""
            INSERT INTO amazon_ads_negative_keywords
                (campaign_id, ad_group_id, keyword_id, keyword_text, match_type,
                 state, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s,%s, %s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), ad_group_id=VALUES(ad_group_id),
                keyword_text=VALUES(keyword_text), match_type=VALUES(match_type),
                state=VALUES(state),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]),
            int(item["adGroupId"]) if item.get("adGroupId") else None,
            int(item["keywordId"]),
            _safe_str(item.get("keywordText")),
            _safe_str(item.get("matchType")),
            _safe_str(item.get("state")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(neg_kws)


def _sync_negative_targets(cursor, client, shop_id):
    neg_targets = _paginate_all(client, client.list_negative_targets, max_results=100)
    for item in neg_targets:
        expr = item.get("expression") or []
        resolved = item.get("resolvedExpression") or ""
        if isinstance(resolved, (list, tuple)):
            resolved = ", ".join(
                (x.get("type") or x.get("name") or json.dumps(x))
                if isinstance(x, dict) else str(x) for x in resolved
            )
        elif isinstance(resolved, dict):
            resolved = resolved.get("type") or resolved.get("name") or json.dumps(resolved)
        cursor.execute("""
            INSERT INTO amazon_ads_negative_targets
                (campaign_id, ad_group_id, target_id, expression,
                 resolved_expression, state, last_update_datetime, synced_at)
            VALUES (%s,%s,%s,%s, %s,%s,%s, NOW())
            ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id), ad_group_id=VALUES(ad_group_id),
                expression=VALUES(expression),
                resolved_expression=VALUES(resolved_expression),
                state=VALUES(state),
                last_update_datetime=VALUES(last_update_datetime), synced_at=NOW()
        """, (
            int(item["campaignId"]),
            int(item["adGroupId"]) if item.get("adGroupId") else None,
            int(item["targetId"]),
            _safe_json(expr),
            _safe_str(resolved),
            _safe_str(item.get("state")),
            _safe_date(item.get("lastUpdateDateTime")),
        ))
    return len(neg_targets)


def sync_shop_entities(shop_id):
    """同步指定店铺的全部广告实体到本地表

    返回: {shop_id, campaigns, ad_groups, product_ads, keywords, targets,
            neg_keywords, neg_targets, error}
    """
    result = {
        "shop_id": shop_id,
        "campaigns": 0, "ad_groups": 0, "product_ads": 0,
        "keywords": 0, "targets": 0,
        "neg_keywords": 0, "neg_targets": 0,
        "error": None,
    }
    try:
        client = get_ads_api_client(shop_id)
    except Exception as e:
        result["error"] = str(e)
        return result

    profile_id = client.profile_id

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            result["campaigns"] = _sync_campaigns(cursor, client, shop_id, profile_id)
            result["ad_groups"] = _sync_ad_groups(cursor, client, shop_id)
            result["product_ads"] = _sync_product_ads(cursor, client, shop_id)
            result["keywords"] = _sync_keywords(cursor, client, shop_id)
            result["targets"] = _sync_targets(cursor, client, shop_id)
            result["neg_keywords"] = _sync_negative_keywords(cursor, client, shop_id)
            result["neg_targets"] = _sync_negative_targets(cursor, client, shop_id)
        conn.commit()
    finally:
        conn.close()

    return result


def sync_all_shops():
    """同步所有启用店铺的广告实体"""
    shops = get_all_active_shops()
    results = []
    for shop in shops:
        sid = shop["id"]
        name = shop.get("shop_name", f"Shop#{sid}")
        print(f"[EntitySync] 同步店铺 {name} (id={sid}) ...")
        t0 = time.time()
        r = sync_shop_entities(sid)
        elapsed = time.time() - t0
        total = (r["campaigns"] + r["ad_groups"] + r["product_ads"] +
                 r["keywords"] + r["targets"] + r["neg_keywords"] + r["neg_targets"])
        if r["error"]:
            print(f"  [EntitySync] FAIL: {r['error']}")
        else:
            print(f"  [EntitySync] OK: {total} 实体 (campaigns={r['campaigns']}, "
                  f"ad_groups={r['ad_groups']}, products={r['product_ads']}, "
                  f"keywords={r['keywords']}, targets={r['targets']}, "
                  f"neg_kw={r['neg_keywords']}, neg_tg={r['neg_targets']}) "
                   f"耗时 {elapsed:.1f}s")
        results.append(r)
    return results


# =================================================================
#  前端路由 — 实体同步
# =================================================================

@advertising_manage_bp.route('/entities/sync', methods=['POST'])
def trigger_entity_sync():
    """一键同步指定店铺的全量广告实体（campaign/group/product-ad/keyword/target/negative）"""
    body = request.get_json() or {}
    shop_id = body.get("shop_id")
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400
    try:
        shop_id = int(shop_id)
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "shop_id 必须是整数"}), 400

    t0 = time.time()
    result = sync_shop_entities(shop_id)
    elapsed = time.time() - t0
    total = (result["campaigns"] + result["ad_groups"] + result["product_ads"] +
             result["keywords"] + result["targets"] + result["neg_keywords"] + result["neg_targets"])
    print(f"[EntitySync] shop={shop_id} 完成: {total} 实体, 耗时 {elapsed:.1f}s")
    return jsonify({
        "status": "error" if result["error"] else "success",
        "data": result,
        "total": total,
        "elapsed": round(elapsed, 1),
    })


@advertising_manage_bp.route('/entities/sync-all', methods=['POST'])
def trigger_entity_sync_all():
    """一键同步所有店铺的全量广告实体"""
    t0 = time.time()
    results = sync_all_shops()
    elapsed = time.time() - t0
    total = sum(
        r["campaigns"] + r["ad_groups"] + r["product_ads"] +
        r["keywords"] + r["targets"] + r["neg_keywords"] + r["neg_targets"]
        for r in results
    )
    errors = [r for r in results if r.get("error")]
    print(f"[EntitySync] all shops 完成: {len(results)} shops, {total} 实体, {len(errors)} errors, 耗时 {elapsed:.1f}s")
    return jsonify({
        "status": "success",
        "data": {"shops": len(results), "total": total, "errors": len(errors), "results": results},
        "elapsed": round(elapsed, 1),
    })
