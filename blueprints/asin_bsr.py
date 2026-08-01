"""
ASIN BSR 监控模块
提供两个接口供家宽虚拟机上的爬虫脚本调用：
  1. GET  /api/bsr/asins   - 获取需要抓取 BSR 的 ASIN 列表
  2. POST /api/bsr/reports - 接收爬虫推送的 BSR 抓取结果并写入 asin_bsr_records

认证: 请求头 X-API-Key，值配置在 .env 的 BSR_API_KEY
"""
import os
import json
from datetime import datetime, date

from flask import Blueprint, request, jsonify
from functools import wraps

from dotenv import load_dotenv
from services.mysql_service import get_db_connection

asin_bsr_bp = Blueprint('asin_bsr', __name__, url_prefix='/api')

load_dotenv(override=True)
BSR_API_KEY = os.getenv("BSR_API_KEY", "")

# Marketplace ID -> 前台域名映射
MARKETPLACE_DOMAINS = {
    "ATVPDKIKX0DER": "www.amazon.com",
    "A1F83G8C2ARO7P": "www.amazon.co.uk",
    "A1PA6795UKMFR9": "www.amazon.de",
    "A1VC38T7YXB528": "www.amazon.co.jp",
    "A1RKKUPIHCS9HS": "www.amazon.es",
    "APJ6JRA9NG5V4": "www.amazon.it",
    "A13V1IB3VIYZZH": "www.amazon.fr",
    "A2EUQ1WTGCTBG2": "www.amazon.ca",
    "A1FZM1EM31CLB6": "www.amazon.pl",
    "A19VAU5U6O6R6C": "www.amazon.se",
}

_REGION_BY_MARKETPLACE = {
    "ATVPDKIKX0DER": "na",
    "A2EUQ1WTGCTBG2": "na",
    "A1F83G8C2ARO7P": "eu",
    "A1PA6795UKMFR9": "eu",
    "A1VC38T7YXB528": "fe",
    "A1RKKUPIHCS9HS": "eu",
    "APJ6JRA9NG5V4": "eu",
    "A13V1IB3VIYZZH": "eu",
    "A1FZM1EM31CLB6": "eu",
    "A19VAU5U6O6R6C": "eu",
}


def bsr_api_key_required(f):
    """校验 X-API-Key 请求头"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        key = request.headers.get('X-API-Key', '')
        if not BSR_API_KEY:
            return jsonify({"status": "error", "message": "服务端未配置 BSR_API_KEY"}), 500
        if key != BSR_API_KEY:
            return jsonify({"status": "error", "message": "API Key 无效"}), 401
        return f(*args, **kwargs)
    return decorated_function


def _get_sales_domain(marketplace_id):
    return MARKETPLACE_DOMAINS.get(marketplace_id, 'www.amazon.com')


def _parse_sub_category(rec):
    """从单条 BSR 记录的 bsr_categories_json 解析细分子类目（第二个节点）
    返回 (sub_rank, sub_category)，无子类目时返回 (None, None)"""
    sub_json = rec.get('bsr_categories_json')
    if sub_json:
        try:
            cats = json.loads(sub_json)
            if isinstance(cats, list) and len(cats) >= 2:
                sub = cats[1]
                return sub.get('rank'), sub.get('category')
        except (json.JSONDecodeError, TypeError):
            pass
    return None, None


def _trend_rank(rec, use_sub):
    """取涨跌对比用的排名：use_sub 为 True 时优先子类目，缺失则 fallback 大类目"""
    if use_sub:
        sub_rank, _ = _parse_sub_category(rec)
        if sub_rank is not None:
            return sub_rank
    return rec['bsr_rank']


def attach_bsr_info(rows):
    """
    为 Listing 行列表附加 BSR 摘要字段（就地修改 rows 中的每个 dict）。
    供 listing 列表/详情接口复用。

    附加字段:
        bsr_rank           - 最新日期 BSR 排名（顶层大类目）
        bsr_category       - 最新日期 BSR 类目（顶层大类目）
        bsr_sub_rank       - 最新日期细分子类目 BSR 排名（可能为 None）
        bsr_sub_category   - 最新日期细分子类目名称（可能为 None）
        bsr_prev_rank      - 上一数据日涨跌对比基准排名（细分子类目优先，fallback 大类目）
        bsr_change_pct     - 相对上一数据日的变化百分比（按细分子类目；正数=名次上升，负数=下降）
        bsr_trend          - up/down/flat/new/none（按细分子类目）
    """
    if not rows:
        return rows

    asin_set = list({str(r.get('asin') or '').strip() for r in rows if r.get('asin')})
    asin_set = [a for a in asin_set if a]
    if not asin_set:
        return rows

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(asin_set))
            cursor.execute(f"""
                SELECT asin, marketplace_id, record_date, bsr_rank, bsr_category, bsr_categories_json
                FROM asin_bsr_records
                WHERE asin IN ({placeholders})
                  AND status = 'ok' AND bsr_rank IS NOT NULL
                ORDER BY record_date ASC
            """, asin_set)
            records = cursor.fetchall()
    finally:
        conn.close()

    by_asin = {}
    for rec in records:
        by_asin.setdefault(rec['asin'], []).append(rec)

    for r in rows:
        asin = (r.get('asin') or '').strip()
        r['bsr_rank'] = None
        r['bsr_category'] = None
        r['bsr_sub_rank'] = None
        r['bsr_sub_category'] = None
        r['bsr_prev_rank'] = None
        r['bsr_change_pct'] = None
        r['bsr_trend'] = 'none'
        if not asin:
            continue

        series = by_asin.get(asin)
        if not series:
            continue

        mp = (r.get('marketplace_id') or 'ATVPDKIKX0DER').strip()
        mp_series = [s for s in series if (s['marketplace_id'] or '') == mp]
        if not mp_series:
            mp_series = series
        if not mp_series:
            continue

        latest = mp_series[-1]
        r['bsr_rank'] = latest['bsr_rank']
        r['bsr_category'] = latest['bsr_category']

        # 细分子类目（bsr_categories_json 第二个节点）
        r['bsr_sub_rank'], r['bsr_sub_category'] = _parse_sub_category(latest)

        # 涨跌趋势按细分子类目计算（缺失时 fallback 大类目）
        if len(mp_series) >= 2:
            prev = mp_series[-2]
            use_sub = r['bsr_sub_rank'] is not None
            curr_rank = _trend_rank(latest, use_sub)
            prev_rank = _trend_rank(prev, use_sub)
            if prev_rank:
                r['bsr_prev_rank'] = prev_rank
                r['bsr_change_pct'] = round((prev_rank - curr_rank) / prev_rank * 100, 1)
                if curr_rank < prev_rank:
                    r['bsr_trend'] = 'up'
                elif curr_rank > prev_rank:
                    r['bsr_trend'] = 'down'
                else:
                    r['bsr_trend'] = 'flat'
        else:
            r['bsr_trend'] = 'new'

    return rows


def get_bsr_trend(asin, marketplace_id='ATVPDKIKX0DER', days=None):
    """
    查询单个 ASIN 的 BSR 历史趋势（按日期升序）。
    返回 dict:
        {
          'dates':    [YYYY-MM-DD, ...],              // 统一日期骨架（与记录对齐）
          'parent':   [{date, rank, category}, ...],  // 顶层大类目序列
          'sub':      [{date, rank, category}, ...],  // 细分子类目序列
          'categories': {'parent': ..., 'sub': ...}   // 两个类目名称
        }
    某天缺某一类目时，该序列对应 rank 为 null（前端跳过该点不连线）。
    """
    asin = (asin or '').strip().upper()
    if not asin:
        return {}

    sql = """
        SELECT record_date, bsr_rank, bsr_category, status, bsr_categories_json
        FROM asin_bsr_records
        WHERE asin = %s AND marketplace_id = %s
    """
    params = [asin, marketplace_id]
    if days:
        sql += " AND record_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)"
        params.append(int(days))
    sql += " ORDER BY record_date ASC"

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
    finally:
        conn.close()

    dates = []
    parent_series = []
    sub_series = []
    parent_cat = None
    sub_cat = None

    for row in rows:
        if row['status'] != 'ok' or row['bsr_rank'] is None:
            continue
        d = row['record_date'].strftime('%Y-%m-%d')
        dates.append(d)

        # 顶层大类目
        parent_rank = row['bsr_rank']
        parent_cat = parent_cat or row['bsr_category']
        parent_series.append({
            'date': d,
            'rank': parent_rank,
            'category': row['bsr_category'],
        })

        # 细分子类目（bsr_categories_json 第二个节点）
        sub_rank, sub_cat_now = _parse_sub_category(row)
        sub_series.append({
            'date': d,
            'rank': sub_rank,
            'category': sub_cat_now,
        })
        if sub_rank is not None:
            sub_cat = sub_cat or sub_cat_now

    # 子类目缺某天时补 null，保持与大类目日期对齐
    full_sub = []
    for i, d in enumerate(dates):
        item = sub_series[i]
        if item['rank'] is None:
            full_sub.append({'date': d, 'rank': None, 'category': sub_cat})
        else:
            full_sub.append(item)

    return {
        'dates': dates,
        'parent': parent_series,
        'sub': full_sub,
        'categories': {'parent': parent_cat, 'sub': sub_cat},
    }


@asin_bsr_bp.route('/bsr/asins', methods=['GET'])
@bsr_api_key_required
def get_bsr_asin_list():
    """
    获取需要抓取 BSR 的 ASIN 列表
    数据来源: amazon_listings（未删除、有 ASIN）关联 amazon_shops（启用）
    查询参数（可选）:
        only_pending - 1 时只返回今天还未抓取过的 ASIN

    过滤规则:
        - 有库存: amazon_inventory.fulfillable_quantity > 0
        - 7天内有销量: report_sku_sales 最新报表日期的 sales_7d > 0
    """
    only_pending = request.args.get('only_pending', '0').strip() in ('1', 'true', 'True')
    today = date.today().strftime('%Y-%m-%d')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT l.asin, l.sku, l.shop_id, l.marketplace_id, s.region
                FROM amazon_listings l
                JOIN amazon_shops s ON s.id = l.shop_id AND s.status = 1
                WHERE l.is_deleted = 0
                  AND l.asin IS NOT NULL AND TRIM(l.asin) != ''
                  -- 有库存
                  AND EXISTS (
                      SELECT 1 FROM amazon_inventory inv
                      WHERE inv.shop_id = l.shop_id
                        AND inv.seller_sku = l.sku COLLATE utf8mb4_unicode_ci
                        AND inv.fulfillable_quantity > 0
                  )
                  -- 7天内有销量（取该 SKU 最新报表日期的 sales_7d）
                  AND EXISTS (
                      SELECT 1 FROM report_sku_sales rs
                      WHERE rs.shop_id = l.shop_id
                        AND rs.sku = l.sku COLLATE utf8mb4_unicode_ci
                        AND rs.sales_7d > 0
                        AND rs.report_date = (
                            SELECT MAX(rs2.report_date) FROM report_sku_sales rs2
                            WHERE rs2.shop_id = rs.shop_id
                              AND rs2.sku = rs.sku COLLATE utf8mb4_unicode_ci
                        )
                  )
                GROUP BY l.asin, l.marketplace_id, l.shop_id, l.sku, s.region
                ORDER BY l.asin
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

            scraped_asins = set()
            if only_pending and rows:
                asins = [r['asin'] for r in rows]
                placeholders = ','.join(['%s'] * len(asins))
                cursor.execute(
                    f"SELECT DISTINCT asin, marketplace_id FROM asin_bsr_records "
                    f"WHERE record_date = %s AND asin IN ({placeholders})",
                    [today] + asins
                )
                for r in cursor.fetchall():
                    scraped_asins.add((r['asin'], r['marketplace_id']))

        data = []
        for row in rows:
            asin = row['asin']
            marketplace_id = row['marketplace_id'] or 'ATVPDKIKX0DER'
            if only_pending and (asin, marketplace_id) in scraped_asins:
                continue
            data.append({
                "asin": asin,
                "sku": row['sku'],
                "shop_id": row['shop_id'],
                "marketplace_id": marketplace_id,
                "region": row['region'] or _REGION_BY_MARKETPLACE.get(marketplace_id, 'na'),
                "sales_domain": _get_sales_domain(marketplace_id),
                "url": f"https://{_get_sales_domain(marketplace_id)}/dp/{asin}",
            })

        return jsonify({
            "status": "success",
            "data": data,
            "count": len(data)
        })
    except Exception as e:
        print(f"[BSR] 获取 ASIN 列表异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()


@asin_bsr_bp.route('/bsr/reports', methods=['POST'])
@bsr_api_key_required
def push_bsr_reports():
    """
    接收爬虫推送的 BSR 抓取结果
    请求体:
        record_date   - 数据归属日期 YYYY-MM-DD（默认今天）
        records       - 数组，每项:
            {
              asin,                   必填
              marketplace_id          可选（默认 ATVPDKIKX0DER）
              bsr_rank                可选，整数
              bsr_category            可选，主类目名
              bsr_categories          可选，[{rank, category}, ...]
              price                   可选，数字
              currency                可选，如 USD
              status                  可选，'ok'/'failed'（默认 ok）
              error_msg               可选
              fetched_at              可选，YYYY-MM-DD HH:MM:SS（默认当前时间）
              sku / shop_id           可选，冗余记录
            }
    """
    try:
        data = request.get_json() or {}
        record_date = (data.get('record_date') or date.today().strftime('%Y-%m-%d')).strip()
        records = data.get('records')
        if not isinstance(records, list) or not records:
            return jsonify({"status": "error", "message": "缺少必填字段: records（必须为数组）"}), 400

        try:
            datetime.strptime(record_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({"status": "error", "message": "record_date 格式必须为 YYYY-MM-DD"}), 400

        default_marketplace = data.get('marketplace_id', 'ATVPDKIKX0DER')
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                saved = 0
                errors = []
                for item in records:
                    if not isinstance(item, dict):
                        errors.append({"asin": None, "error": "记录必须是对象"})
                        continue
                    asin = (item.get('asin') or '').strip().upper()
                    if not asin:
                        errors.append({"asin": None, "error": "缺少必填字段: asin"})
                        continue

                    marketplace_id = (item.get('marketplace_id') or default_marketplace or 'ATVPDKIKX0DER')
                    bsr_rank = item.get('bsr_rank')
                    if bsr_rank is not None:
                        try:
                            bsr_rank = int(bsr_rank)
                        except (ValueError, TypeError):
                            bsr_rank = None
                    price = item.get('price')
                    if price is not None:
                        try:
                            price = float(price)
                        except (ValueError, TypeError):
                            price = None

                    bsr_categories = item.get('bsr_categories') or []
                    bsr_categories_json = None
                    if bsr_categories and isinstance(bsr_categories, list):
                        bsr_categories_json = json.dumps(bsr_categories, ensure_ascii=False)

                    status = (item.get('status') or 'ok').strip()
                    if status not in ('ok', 'failed'):
                        status = 'ok'
                    error_msg = (item.get('error_msg') or '')
                    if error_msg:
                        error_msg = str(error_msg)[:500]

                    fetched_at = (item.get('fetched_at') or now_str)
                    raw_json = json.dumps(item, ensure_ascii=False)[:20000]

                    sku = (item.get('sku') or '').strip() or None
                    shop_id = item.get('shop_id')
                    if shop_id is not None:
                        try:
                            shop_id = int(shop_id)
                        except (ValueError, TypeError):
                            shop_id = None

                    cursor.execute("""
                        INSERT INTO asin_bsr_records (
                            asin, sku, shop_id, marketplace_id, region, sales_domain,
                            record_date, bsr_rank, bsr_category, bsr_categories_json,
                            price, currency, status, error_msg, raw_json, fetched_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            sku = VALUES(sku),
                            shop_id = VALUES(shop_id),
                            region = VALUES(region),
                            sales_domain = VALUES(sales_domain),
                            bsr_rank = VALUES(bsr_rank),
                            bsr_category = VALUES(bsr_category),
                            bsr_categories_json = VALUES(bsr_categories_json),
                            price = VALUES(price),
                            currency = VALUES(currency),
                            status = VALUES(status),
                            error_msg = VALUES(error_msg),
                            raw_json = VALUES(raw_json),
                            fetched_at = VALUES(fetched_at),
                            updated_at = NOW()
                    """, (
                        asin,
                        sku,
                        shop_id,
                        marketplace_id,
                        item.get('region') or _REGION_BY_MARKETPLACE.get(marketplace_id, 'na'),
                        _get_sales_domain(marketplace_id),
                        record_date,
                        bsr_rank,
                        (item.get('bsr_category') or '')[:255] or None,
                        bsr_categories_json,
                        price,
                        (item.get('currency') or '')[:8] or None,
                        status,
                        error_msg or None,
                        raw_json,
                        fetched_at,
                    ))
                    saved += 1
                conn.commit()
        finally:
            conn.close()

        msg = f"成功写入 {saved} 条 BSR 记录"
        if errors:
            msg += f"，{len(errors)} 条无效被跳过"
        return jsonify({
            "status": "success",
            "message": msg,
            "data": {"saved": saved, "skipped": len(errors), "errors": errors[:20]}
        })
    except Exception as e:
        print(f"[BSR] 推送结果异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
