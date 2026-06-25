"""
Amazon 广告模块（单蓝图）

包含:
  1. OAuth 授权 (authorize-url / callback / profiles / profile)
  2. 异步报告同步 (sync / sync-all / status)
  3. 同步逻辑 (可供 cron 直接调用的 _run_ads_sync 函数)
  4. 全量字段写入新表 amazon_ads_raw_reports
"""
import os
import time
import json
import gzip
import hashlib
from urllib.parse import urlencode
from datetime import datetime, timedelta

import requests
from flask import Blueprint, request, jsonify, current_app
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from blueprints.user_auth import login_required, permission_required
from services.mysql_service import get_db_connection
from services.shop_service import get_shop_by_id
from services.credential_service import get_ads_app_credentials
advertising_bp = Blueprint('advertising', __name__, url_prefix='/api')


# ==================== 常量 ====================

ADS_REDIRECT_URI = os.getenv(
    "AMAZON_ADS_REDIRECT_URI",
    "https://api.zhongyuwen.cn/api/amazon/ads/callback",
)
ADS_SCOPE = "profile advertising::campaign_management"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
ADS_CONSENT_URLS = {
    "na": "https://www.amazon.com/ap/oa",
    "eu": "https://eu.account.amazon.com/ap/oa",
    "fe": "https://apac.account.amazon.com/ap/oa",
}
_STATE_MAX_AGE = 1800
_STATE_SALT = "amazon-ads-oauth-state"

# API 列名 → DB 列名映射（基于 Amazon Ads API v3 实际返回的列名）
_COLUMN_MAP = [
    ("campaignId", "campaign_id"),
    ("campaignName", "campaign_name"),
    ("campaignStatus", "campaign_status"),
    ("campaignBudgetAmount", "campaign_budget"),
    ("campaignBudgetType", "campaign_budget_type"),
    ("adGroupId", "ad_group_id"),
    ("adGroupName", "ad_group_name"),
    ("advertisedAsin", "advertised_asin"),
    ("advertisedSku", "advertised_sku"),
    ("purchasedAsin", "purchased_asin"),
    ("keywordId", "keyword_id"),
    ("keyword", "keyword_text"),            # API 返回 keyword 不是 keywordText
    ("keywordType", "keyword_type"),
    ("matchType", "keyword_match_type"),     # API 返回 matchType 不是 keywordMatchType
    ("targeting", "targeting_expression"),   # API 返回 targeting 不是 targetingExpression
    ("searchTerm", "customer_search_term"),  # API 返回 searchTerm 不是 customerSearchTerm
    ("impressions", "impressions"),
    ("clicks", "clicks"),
    ("cost", "cost"),
    ("purchases7d", "purchases_7d"),
    ("purchases14d", "purchases_14d"),
    ("purchases30d", "purchases_30d"),
    ("sales7d", "sales_7d"),
    ("sales14d", "sales_14d"),
    ("sales30d", "sales_30d"),
]

# Amazon Ads API v3 每种报告类型支持的列不同（根据 API 返回的 Allowed values 验证）：
#   spCampaigns:        全归因窗口 (7d/14d/30d)
#   spAdvertisedProduct: 7d/30d (无 14d)，含 adGroup 级别数据
#   spTargeting:        仅 7d，列名：keyword/matchType/targeting
#   spSearchTerm:       仅 7d，列名：keyword/matchType/searchTerm
_SYNC_REPORT_TYPES = {
    "spCampaigns": {
        "name": "SP Campaigns",
        "groupBy": ["campaign"],
        "columns": (
            "date campaignId campaignName campaignStatus "
            "impressions clicks cost "
            "purchases7d purchases14d purchases30d "
            "sales7d sales14d sales30d"
        ).split(),
    },
    "spAdvertisedProduct": {
        "name": "SP AdvertisedProduct",
        "groupBy": ["advertiser"],
        "columns": (
            "date campaignId campaignName adGroupId adGroupName "
            "advertisedAsin advertisedSku "
            "impressions clicks cost "
            "purchases7d purchases30d "
            "sales7d sales30d"
        ).split(),
    },
    "spTargeting": {
        "name": "SP Targeting",
        "groupBy": ["targeting"],
        "columns": (
            "date campaignId campaignName adGroupId adGroupName "
            "keywordId keyword keywordType matchType "
            "targeting "
            "impressions clicks cost "
            "purchases7d sales7d"
        ).split(),
    },
    "spSearchTerm": {
        "name": "SP SearchTerm",
        "groupBy": ["searchTerm"],
        "columns": (
            "date campaignId campaignName adGroupId adGroupName "
            "keywordId keyword keywordType matchType searchTerm "
            "impressions clicks cost "
            "purchases7d sales7d"
        ).split(),
    },
}

# ==================== 工具 ====================

def _serializer():
    secret = current_app.secret_key or "amazon-ads-oauth-fallback-secret"
    return URLSafeTimedSerializer(secret, salt=_STATE_SALT)


def _html(success, message):
    color = "#16a34a" if success else "#dc2626"
    title = "授权成功" if success else "授权失败"
    body = f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Amazon 广告授权</title></head>
<body style="font-family:system-ui,sans-serif;background:#f8fafc;margin:0;padding:48px 16px;text-align:center;">
<div style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;padding:32px;box-shadow:0 1px 6px rgba(0,0,0,.08);">
<h2 style="color:{color};margin:0 0 12px;">{title}</h2>
<p style="color:#334155;line-height:1.6;">{message}</p>
<p style="color:#94a3b8;font-size:13px;margin-top:24px;">可关闭本页面，返回系统继续操作。</p>
</div></body></html>"""
    return body, (200 if success else 400), {"Content-Type": "text/html; charset=utf-8"}


def _update_shop_field(shop_id, field, value):
    assert field in ("ads_refresh_token", "ads_profile_id")
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(f"UPDATE amazon_shops SET {field}=%s WHERE id=%s", (value, shop_id))
        conn.commit()
    finally:
        conn.close()


def _build_proxies(proxy_url=None):
    if proxy_url:
        return {"http": proxy_url, "https": proxy_url}
    hp = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    sp = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    proxies = {}
    if hp: proxies["http"] = hp
    if sp: proxies["https"] = sp
    return proxies if proxies else None


def _try_autofill_profile(shop_id):
    try:
        from services.shop_service import get_ads_api_client as _get_ac
        client = _get_ac(shop_id)
        profiles = client.list_profiles() or []
        shop = get_shop_by_id(shop_id)
        mp = (shop or {}).get("marketplace_id")
        matches = [p for p in profiles if isinstance(p, dict) and (p.get("accountInfo") or {}).get("marketplaceStringId") == mp]
        if len(matches) == 1:
            pid = matches[0].get("profileId")
            _update_shop_field(shop_id, "ads_profile_id", str(pid))
            return f"已自动匹配并保存 profileId={pid}。"
        if profiles:
            return f"获取到 {len(profiles)} 个广告账户，未能按当前 marketplace 唯一匹配，请手动选择。"
        return "未获取到广告账户(profile)。"
    except Exception as e:
        return f"refresh_token 已保存，但自动获取 profile 失败：{e}"


# ==================== OAuth 路由 ====================

@advertising_bp.route('/amazon/ads/authorize-url', methods=['GET'])
@login_required
@permission_required('shops:edit')
def ads_authorize_url():
    shop_id = request.args.get('shop_id', '').strip()
    if not shop_id:
        return jsonify({"status": "error", "message": "缺少 shop_id"}), 400
    try: shop_id = int(shop_id)
    except ValueError: return jsonify({"status": "error", "message": "shop_id 必须是整数"}), 400
    shop = get_shop_by_id(shop_id)
    if not shop: return jsonify({"status": "error", "message": "店铺不存在"}), 404
    creds = get_ads_app_credentials(shop["credential_group_id"])
    if not creds["client_id"]: return jsonify({"status": "error", "message": "凭证组未配置 ads_client_id"}), 400
    region = (shop.get("region") or "na").lower()
    consent_base = ADS_CONSENT_URLS.get(region, ADS_CONSENT_URLS["na"])
    state = _serializer().dumps({"shop_id": shop_id})
    url = f"{consent_base}?{urlencode({'client_id':creds['client_id'],'scope':ADS_SCOPE,'response_type':'code','redirect_uri':ADS_REDIRECT_URI,'state':state})}"
    return jsonify({"status": "success", "data": {"authorize_url": url, "redirect_uri": ADS_REDIRECT_URI, "expires_in": _STATE_MAX_AGE, "tip": "请将此链接复制到已登录 Amazon 账号的紫鸟浏览器中打开"}})


@advertising_bp.route('/amazon/ads/callback', methods=['GET'])
def ads_callback():
    error = request.args.get('error')
    if error: return _html(False, f"授权被拒绝: {error} {request.args.get('error_description','')}")
    code, state = request.args.get('code'), request.args.get('state')
    if not code or not state: return _html(False, "缺少 code 或 state 参数。")
    try: data = _serializer().loads(state, max_age=_STATE_MAX_AGE)
    except SignatureExpired: return _html(False, "授权链接已过期，请重新发起。")
    except BadSignature: return _html(False, "state 校验失败（非法请求）。")
    shop = get_shop_by_id(data.get("shop_id"))
    if not shop: return _html(False, "店铺不存在。")
    creds = get_ads_app_credentials(shop["credential_group_id"])
    if not creds["client_id"] or not creds["client_secret"]: return _html(False, "凭证组未配置广告应用凭证。")
    try:
        resp = requests.post(LWA_TOKEN_URL, data={"grant_type":"authorization_code","code":code,"redirect_uri":ADS_REDIRECT_URI,"client_id":creds["client_id"],"client_secret":creds["client_secret"]}, proxies=creds["proxies"], timeout=30)
        resp.raise_for_status()
        rt = resp.json().get("refresh_token")
    except Exception as e: return _html(False, f"换取 refresh_token 失败: {e}")
    if not rt: return _html(False, "未返回 refresh_token。")
    _update_shop_field(data["shop_id"], "ads_refresh_token", rt)
    return _html(True, f"店铺「{shop.get('shop_name')}」广告 API 授权成功。" + _try_autofill_profile(data["shop_id"]))


@advertising_bp.route('/amazon/ads/profiles', methods=['GET'])
@login_required
@permission_required('shops:edit')
def ads_list_profiles():
    shop_id = request.args.get('shop_id', '').strip()
    if not shop_id: return jsonify({"status":"error","message":"缺少 shop_id"}), 400
    try: shop_id = int(shop_id)
    except ValueError: return jsonify({"status":"error","message":"shop_id 必须是整数"}), 400
    from services.shop_service import get_ads_api_client as _get_ac
    try:
        profiles = _get_ac(shop_id).list_profiles() or []
        return jsonify({"status":"success","data":profiles})
    except ValueError as e: return jsonify({"status":"error","message":str(e)}), 400
    except Exception as e: return jsonify({"status":"error","message":f"获取 profiles 失败: {e}"}), 500


@advertising_bp.route('/amazon/ads/profile', methods=['POST'])
@login_required
@permission_required('shops:edit')
def ads_set_profile():
    body = request.get_json() or {}
    shop_id = body.get('shop_id'); pid = (str(body.get('profile_id') or '')).strip()
    if not shop_id or not pid: return jsonify({"status":"error","message":"shop_id 和 profile_id 不能为空"}), 400
    try: shop_id = int(shop_id)
    except (ValueError,TypeError): return jsonify({"status":"error","message":"shop_id 必须是整数"}), 400
    if not get_shop_by_id(shop_id): return jsonify({"status":"error","message":"店铺不存在"}), 404
    _update_shop_field(shop_id, "ads_profile_id", pid)
    return jsonify({"status":"success","message":"ads_profile_id 已更新"})


# ==================== 广告数据同步（核心逻辑，供路由和 cron 公用）====================

def _make_client(shop_id):
    """根据 shop_id 初始化 Ads API 客户端（读库凭证）"""
    from services.shop_service import get_ads_api_client as _get_ac
    return _get_ac(shop_id)


def _map_row(api_row):
    """将 Ads API 报告行映射为 DB 列字典"""
    db = {}
    for api_key, db_key in _COLUMN_MAP:
        db[db_key] = api_row.get(api_key, "")
    db["report_date"] = api_row.get("date", "")
    return db


def _write_raw_reports(cursor, shop_id, report_type, rows):
    """幂等写入 amazon_ads_raw_reports（哈希去重）"""
    inserted = 0
    updated = 0
    if not rows:
        return 0, 0
    for row in rows:
        m = _map_row(row)
        # 计算行指纹: 所有唯一维度列拼接后 SHA-256
        hash_src = "|".join([
            str(shop_id),
            str(m.get("report_date", "")),
            str(report_type),
            str(m.get("campaign_id", "") or ""),
            str(m.get("ad_group_id", "") or ""),
            "",  # ad_id (暂无, 表中默认为空)
            str(m.get("advertised_asin", "") or ""),
            str(m.get("keyword_id", "") or ""),
            str(m.get("customer_search_term", "") or ""),
            str(m.get("targeting_expression", "") or ""),
        ])
        row_hash = hashlib.sha256(hash_src.encode("utf-8")).hexdigest()

        cursor.execute("""
            INSERT INTO amazon_ads_raw_reports (
                shop_id, report_date, report_type,
                campaign_id, campaign_name, campaign_status, campaign_budget, campaign_budget_type,
                ad_group_id, ad_group_name, ad_group_status,
                advertised_asin, advertised_sku, purchased_asin,
                keyword_id, keyword_text, keyword_type, keyword_match_type,
                targeting_id, targeting_expression, targeting_type,
                customer_search_term,
                impressions, clicks, cost,
                purchases_7d, purchases_14d, purchases_30d,
                sales_7d, sales_14d, sales_30d,
                row_hash
            ) VALUES (%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s)
            ON DUPLICATE KEY UPDATE
                campaign_name = VALUES(campaign_name),
                campaign_status = VALUES(campaign_status),
                campaign_budget = VALUES(campaign_budget),
                campaign_budget_type = VALUES(campaign_budget_type),
                ad_group_name = VALUES(ad_group_name),
                ad_group_status = VALUES(ad_group_status),
                advertised_sku = VALUES(advertised_sku),
                purchased_asin = VALUES(purchased_asin),
                keyword_text = VALUES(keyword_text),
                keyword_type = VALUES(keyword_type),
                keyword_match_type = VALUES(keyword_match_type),
                targeting_expression = VALUES(targeting_expression),
                targeting_type = VALUES(targeting_type),
                customer_search_term = VALUES(customer_search_term),
                impressions = VALUES(impressions),
                clicks = VALUES(clicks),
                cost = VALUES(cost),
                purchases_7d = VALUES(purchases_7d),
                purchases_14d = VALUES(purchases_14d),
                purchases_30d = VALUES(purchases_30d),
                sales_7d = VALUES(sales_7d),
                sales_14d = VALUES(sales_14d),
                sales_30d = VALUES(sales_30d),
                updated_at = NOW()
        """, (
            shop_id, m["report_date"], report_type,
            str(m.get("campaign_id","") or ""), str(m.get("campaign_name","") or ""),
            str(m.get("campaign_status","") or ""), _safe_decimal(m.get("campaign_budget")), str(m.get("campaign_budget_type","") or ""),
            str(m.get("ad_group_id","") or ""), str(m.get("ad_group_name","") or ""), str(m.get("ad_group_status","") or ""),
            str(m.get("advertised_asin","") or ""), str(m.get("advertised_sku","") or ""), str(m.get("purchased_asin","") or ""),
            str(m.get("keyword_id","") or ""), str(m.get("keyword_text","") or ""), str(m.get("keyword_type","") or ""), str(m.get("keyword_match_type","") or ""),
            str(m.get("targeting_id","") or ""), str(m.get("targeting_expression","") or ""), str(m.get("targeting_type","") or ""),
            str(m.get("customer_search_term","") or ""),
            int(m.get("impressions",0) or 0), int(m.get("clicks",0) or 0), float(m.get("cost",0) or 0),
            int(m.get("purchases_7d",0) or 0), int(m.get("purchases_14d",0) or 0), int(m.get("purchases_30d",0) or 0),
            float(m.get("sales_7d",0) or 0), float(m.get("sales_14d",0) or 0), float(m.get("sales_30d",0) or 0),
            row_hash,
        ))
        if cursor.rowcount == 1: inserted += 1
        else: updated += 1
    return inserted, updated


def _safe_decimal(val):
    if val is None or val == "" or val == "None": return None
    try: return float(val)
    except (ValueError,TypeError): return None


def _report_cache_get(shop_id, report_type_key, report_date):
    """查询缓存的报告 ID"""
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(
                "SELECT report_id, status FROM amazon_ads_report_cache "
                "WHERE shop_id=%s AND report_type=%s AND report_date=%s LIMIT 1",
                (shop_id, report_type_key, report_date))
            return c.fetchone()
    finally:
        conn.close()


def _report_cache_set(shop_id, report_type_key, report_date, report_id, status="PENDING"):
    """写入/更新报告 ID 缓存"""
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO amazon_ads_report_cache (shop_id, report_type, report_date, report_id, status) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE report_id=VALUES(report_id), status=VALUES(status), updated_at=NOW()",
                (shop_id, report_type_key, report_date, report_id, status))
        conn.commit()
    finally:
        conn.close()


def _report_cache_clean(shop_id=None, keep_days=7):
    """清理过期的缓存报告 ID"""
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            if shop_id:
                c.execute(
                    "DELETE FROM amazon_ads_report_cache "
                    "WHERE shop_id=%s AND created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                    (shop_id, keep_days))
            else:
                c.execute(
                    "DELETE FROM amazon_ads_report_cache "
                    "WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY)",
                    (keep_days,))
            deleted = c.rowcount
        conn.commit()
        if deleted:
            print(f"[Ads Sync] 清理了 {deleted} 条过期缓存 (>{keep_days}天)")
        return deleted
    finally:
        conn.close()


def _fetch_report(client, report_type_cfg, report_type_key, start_date, end_date, shop_id=None):
    """获取报告（优先复用缓存 reportId，避免重复创建）

    流程:
      1. 查缓存 → 命中则用现有 reportId 查状态/下载
      2. 缓存未命中或已失效 → 创建新报告 → 缓存 reportId
      3. 下载前清理过期缓存
    """
    date_str = start_date  # 日报告 start=end

    # 构建请求体
    body = {
        "name": f"Daily sync {report_type_cfg['name']}",
        "startDate": start_date, "endDate": end_date,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": report_type_cfg["groupBy"],
            "columns": report_type_cfg["columns"],
            "reportTypeId": report_type_key,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        },
    }

    # 1. 检查缓存
    if shop_id:
        cached = _report_cache_get(shop_id, report_type_key, date_str)
        if cached:
            cached_id = cached["report_id"]
            print(f"[Ads Sync] → {report_type_key} date={date_str} 缓存命中 reportId={cached_id[:20]}... "
                  f"status={cached['status']}")
            try:
                status = client._get_report_status(cached_id)
                state = (status.get("status") or "").upper()
                if state == "COMPLETED":
                    url = status.get("url")
                    if url:
                        print(f"[Ads Sync] → {report_type_key} 缓存报告已完成，直接下载")
                        rows = client._download_report_content(url)
                        _report_cache_set(shop_id, report_type_key, date_str, cached_id, "COMPLETED")
                        return rows
                    else:
                        print(f"[Ads Sync] → {report_type_key} 缓存报告已完成但 URL 已过期，重新创建")
                elif state in ("PENDING", "PROCESSING"):
                    print(f"[Ads Sync] → {report_type_key} 缓存报告仍在 {state}，轮询等待...")
                    report_id = cached_id
                    url = client._poll_report_completion(report_id, max_wait=600)
                    rows = client._download_report_content(url)
                    _report_cache_set(shop_id, report_type_key, date_str, report_id, "COMPLETED")
                    return rows
                elif state in ("FAILURE", "CANCELLED"):
                    print(f"[Ads Sync] → {report_type_key} 缓存报告已失效 ({state})，重新创建")
                else:
                    print(f"[Ads Sync] → {report_type_key} 缓存报告未知状态 {state}，重新创建")
            except Exception as e:
                print(f"[Ads Sync] → {report_type_key} 缓存报告查询失败: {e}，重新创建")

    # 2. 创建新报告
    print(f"[Ads Sync] → 创建 {report_type_key} 报告, date={date_str}, "
          f"groupBy={report_type_cfg['groupBy']}, columns={len(report_type_cfg['columns'])}列")
    new_id = client._create_async_report(body)
    if shop_id:
        _report_cache_set(shop_id, report_type_key, date_str, new_id, "PENDING")
        print(f"[Ads Sync] → {report_type_key} 新报告 reportId={new_id[:20]}... 已缓存")

    # 3. 轮询 + 下载
    url = client._poll_report_completion(new_id, max_wait=600)
    rows = client._download_report_content(url)
    if shop_id:
        _report_cache_set(shop_id, report_type_key, date_str, new_id, "COMPLETED")
    return rows


def _sync_one_type(client, shop_id, report_type_key, cfg, date_str):
    """同步单个报告类型，返回结果字典"""
    result = {"report_type": report_type_key, "rows": 0, "inserted": 0, "updated": 0, "error": None}
    try:
        rows = _fetch_report(client, cfg, report_type_key, date_str, date_str, shop_id=shop_id)
        result["rows"] = len(rows)
        if rows:
            conn = get_db_connection()
            try:
                with conn.cursor() as c:
                    ins, upd = _write_raw_reports(c, shop_id, report_type_key, rows)
                conn.commit()
                result["inserted"], result["updated"] = ins, upd
            finally:
                conn.close()
        print(f"[Ads Sync] shop={shop_id} {report_type_key}: {len(rows)} rows ({result['inserted']} new)")
    except Exception as e:
        result["error"] = str(e)
        print(f"[Ads Sync] shop={shop_id} {report_type_key} FAIL: {e}")
    return result


def run_ads_sync(shop_id, date_str=None):
    """
    同步指定店铺指定日期的全量广告报告数据 → amazon_ads_raw_reports

    cron 可直接 `from blueprints.amazon.advertising import run_ads_sync` 调用。
    返回: {"shop_id":..., "date":..., "results":[...], "total_rows":...}
    """
    date_str = date_str or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    out = {"shop_id": shop_id, "date": date_str, "results": [], "total_rows": 0, "error": None}
    try:
        client = _make_client(shop_id)
    except Exception as e:
        out["error"] = str(e)
        return out

    types = list(_SYNC_REPORT_TYPES.keys())
    print(f"[Ads Sync] shop={shop_id} date={date_str} 开始同步 {len(types)} 种报告: {types}")

    # 先清理过期缓存
    _report_cache_clean(shop_id=shop_id)

    for key, cfg in _SYNC_REPORT_TYPES.items():
        r = _sync_one_type(client, shop_id, key, cfg, date_str)
        if not r["error"]:
            out["total_rows"] += r["rows"]
        out["results"].append(r)
    return out


# ==================== 同步触发路由 ====================

@advertising_bp.route('/amazon/ads/reports/sync', methods=['POST'])
@login_required
@permission_required('shops:edit')
def trigger_sync():
    data = request.get_json() or {}
    shop_id = data.get('shop_id')
    if not shop_id: return jsonify({"status":"error","message":"缺少 shop_id"}), 400
    try: shop_id = int(shop_id)
    except (ValueError,TypeError): return jsonify({"status":"error","message":"shop_id 必须是整数"}), 400
    date_str = data.get('date') or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    return jsonify({"status":"success","data":run_ads_sync(shop_id, date_str)})


@advertising_bp.route('/amazon/ads/reports/sync-all', methods=['POST'])
@login_required
@permission_required('shops:edit')
def trigger_sync_all():
    from services.shop_service import get_all_active_shops
    data = request.get_json() or {}
    date_str = data.get('date') or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    results = []
    for s in get_all_active_shops():
        results.append(run_ads_sync(s['id'], date_str))
    return jsonify({"status":"success","data":results})


@advertising_bp.route('/amazon/ads/reports/status', methods=['GET'])
@login_required
def ads_sync_status():
    shop_id = request.args.get('shop_id','').strip()
    date_str = request.args.get('date','').strip()
    if not shop_id or not date_str: return jsonify({"status":"error","message":"shop_id 和 date 不能为空"}), 400
    try: shop_id = int(shop_id)
    except ValueError: return jsonify({"status":"error","message":"shop_id 必须是整数"}), 400
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(1) AS cnt, SUM(cost) AS total FROM amazon_ads_raw_reports WHERE shop_id=%s AND report_date=%s", (shop_id, date_str))
            r = c.fetchone()
        return jsonify({"status":"success","data":{"shop_id":shop_id,"date":date_str,"row_count":r['cnt'] or 0,"total_cost":float(r['total'] or 0),"has_data":(r['cnt'] or 0)>0}})
    finally:
        conn.close()
