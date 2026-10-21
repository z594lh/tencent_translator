"""经营事实包只读查询，使用现有表，不改源报表或隐式触发采集。"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from services.mysql_service import get_db_connection
from services.listing_performance.common import DomainError
from services import ads_report_provenance
from . import pricing

US = 'ATVPDKIKX0DER'
REPORT_TYPES = ('spAdvertisedProduct', 'spCampaigns', 'spCampaignsPlacement', 'spTargeting', 'spSearchTerm')


@contextmanager
def session():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cur.execute('SET SESSION TRANSACTION READ ONLY')
            conn.begin()
            yield cur
    finally:
        try:
            conn.rollback()
        finally:
            conn.close()


def rows(cur, sql, args=(), limit=30000):
    """硬上限拒绝超量，绝不把截断资料当完整来源。"""
    cur.execute(sql + ' LIMIT %s', (*args, limit + 1))
    result = cur.fetchall()
    if len(result) > limit:
        raise DomainError('该范围超过资料预览上限，请选择较小广告关联范围；本次没有生成截断资料', 422)
    return result


def options(shop=None):
    with session() as cur:
        shops = rows(cur, '''SELECT id,shop_name,marketplace_id FROM amazon_shops
            WHERE status=1 AND region='na' AND marketplace_id=%s ORDER BY id''', (US,), 100)
        if shop is None:
            return {'shops': shops, 'products': []}
        if shop not in {s['id'] for s in shops}:
            raise DomainError('店铺不属于当前支持的美国站范围', 422)
        products = rows(cur, '''SELECT l.asin,l.sku,l.main_image_url,l.item_name,
            (SELECT MIN(p.product_name) FROM products p WHERE BINARY p.seller_sku=BINARY l.sku) product_name
            FROM amazon_listings l WHERE l.shop_id=%s AND l.marketplace_id=%s AND l.is_deleted=0
            AND COALESCE(LOWER(l.parentage_level),'')<>'parent' ORDER BY l.asin,l.sku''', (shop, US), 3000)
        cur.execute('SELECT MAX(report_date) AS latest FROM sku_profit WHERE shop_id=%s', (shop,))
        return {'shops': shops, 'products': products, 'latest_business_date': cur.fetchone()['latest']}


def read(cur, shop_id, asin, start, end):
    operation_since = datetime.now(timezone.utc) - timedelta(days=60)
    cur.execute('''SELECT id,shop_name,credential_group_id,marketplace_id,region,ads_profile_id
        FROM amazon_shops WHERE id=%s AND status=1''', (shop_id,))
    shop = cur.fetchone()
    if not shop or shop['marketplace_id'] != US or shop['region'] != 'na' or not shop['ads_profile_id']:
        raise DomainError('首期仅支持已配置Ads账户的美国站店铺', 422)
    listing = rows(cur, '''SELECT id,sku,asin,item_name,brand,product_type,product_description,main_image_url,status,
        parent_sku,parentage_level,sync_time,attributes_json,
        (SELECT MIN(p.product_name) FROM products p WHERE BINARY p.seller_sku=BINARY l.sku) product_name
        FROM amazon_listings l WHERE shop_id=%s AND marketplace_id=%s AND BINARY asin=BINARY %s
        AND is_deleted=0 AND COALESCE(LOWER(parentage_level),'')<>'parent' ORDER BY sku''',
        (shop_id, US, asin), 50)
    if not listing:
        raise DomainError('本店没有该ASIN的有效子Listing', 404)
    skus = sorted({r['sku'] for r in listing})
    marks = ','.join(['%s'] * len(skus))
    sku_sql = f'BINARY sku IN ({marks})'
    profit = rows(cur, f'''SELECT id,sku,asin,report_date,sales_qty,sales_amount,ad_cost,net_profit,
        refund_amount,ad_sales,ad_orders,updated_at FROM sku_profit
        WHERE shop_id=%s AND {sku_sql} AND report_date BETWEEN %s AND %s ORDER BY report_date,id''',
        (shop_id, *skus, start, end), 4000)
    # 当前及窗口内历史商品广告的并集；用EXISTS避免JOIN放大报表金额。
    campaigns = rows(cur, f'''SELECT c.campaign_id,c.profile_id,c.name,c.targeting_type,c.state,
        c.daily_budget,c.bidding,c.synced_at FROM amazon_ads_campaigns c
        WHERE c.shop_id=%s AND CAST(c.profile_id AS CHAR)=%s AND c.campaign_type='sponsoredProducts'
        AND (EXISTS (SELECT 1 FROM amazon_ads_product_ads a WHERE a.campaign_id=c.campaign_id
            AND (BINARY a.sku IN ({marks}) OR BINARY a.asin=BINARY %s))
          OR EXISTS (SELECT 1 FROM amazon_ads_raw_reports r WHERE r.shop_id=c.shop_id
            AND r.report_type='spAdvertisedProduct' AND r.campaign_id=CAST(c.campaign_id AS CHAR)
            AND r.report_date BETWEEN %s AND %s
            AND (BINARY r.advertised_sku IN ({marks}) OR BINARY r.advertised_asin=BINARY %s)))
        ORDER BY c.campaign_id''', (shop_id, str(shop['ads_profile_id']), *skus, asin, start, end, *skus, asin), 100)
    ids = [str(c['campaign_id']) for c in campaigns]
    data = {'shop': shop, 'listings': listing, 'skus': skus, 'profit': profit, 'campaigns': campaigns,
            'groups': [], 'keywords': [], 'targets': [], 'products': [], 'reports': [], 'ads_history': []}
    # 旧生成器遍历全局活跃products；本地利润唯一键是shop/ASIN/day，重复产品SKU会覆盖金额。
    data['profit_product_skus'] = rows(cur, '''SELECT DISTINCT seller_sku FROM products
        WHERE status=1 AND BINARY asin=BINARY %s''', (asin,), 100)
    historical_links = rows(cur, f'''SELECT DISTINCT campaign_id,ad_group_id FROM amazon_ads_raw_reports
        WHERE shop_id=%s AND report_type='spAdvertisedProduct' AND report_date BETWEEN %s AND %s
        AND (BINARY advertised_sku IN ({marks}) OR BINARY advertised_asin=BINARY %s)''',
        (shop_id, start, end, *skus, asin), 2000)
    data['unmapped_historical_links'] = [r for r in historical_links if str(r['campaign_id']) not in ids]
    if ids:
        cm = ','.join(['%s'] * len(ids))
        # 子表无shop字段，必须只使用上面按店铺/profile核实的活动集合。
        for table, key, fields in (
            ('amazon_ads_ad_groups', 'groups', 'ad_group_id,name,state,default_bid,synced_at'),
            ('amazon_ads_keywords', 'keywords', 'ad_group_id,keyword_id,keyword_text,match_type,state,bid,synced_at'),
            ('amazon_ads_targets', 'targets', 'ad_group_id,target_id,resolved_expression,state,bid,synced_at'),
            ('amazon_ads_product_ads', 'products', 'ad_group_id,ad_id,sku,asin,state,synced_at'),
        ):
            data[key] = rows(cur, f'SELECT campaign_id,{fields} FROM {table} WHERE campaign_id IN ({cm})',
                             ids, 10000)
        data['reports'] = rows(cur, f'''SELECT id,report_type,report_date,campaign_id,ad_group_id,
            campaign_name,ad_group_name,advertised_sku,advertised_asin,keyword_id,keyword_type,
            keyword_match_type,keyword_text,targeting_id,targeting_expression,customer_search_term,
            placement,currency,impressions,clicks,cost,purchases_7d,sales_7d,updated_at
            FROM amazon_ads_raw_reports WHERE shop_id=%s AND campaign_id IN ({cm})
            AND report_date BETWEEN %s AND %s AND report_type IN (%s,%s,%s,%s,%s) ORDER BY id''',
            (shop_id, *ids, start, end, *REPORT_TYPES), 100000)
        data['ads_history'] = rows(cur, f'''SELECT id,campaign_id,ad_group_id,entity_type,entity_id,
            change_type,previous_value,new_value,event_time_ms,source,sync_status,write_status
            FROM amazon_ads_history WHERE shop_id=%s AND marketplace_id=%s AND region=%s
            AND credential_group_id=%s AND profile_id=%s AND campaign_id IN ({cm})
            AND merged_into_id IS NULL AND event_time_ms >= %s ORDER BY event_time_ms DESC''',
            (shop_id, US, shop['region'], shop['credential_group_id'], str(shop['ads_profile_id']), *ids,
             int(operation_since.timestamp() * 1000)), 10000)
    data['cache'] = rows(cur, '''SELECT id,report_type,report_date,status,insert_flag,created_at,updated_at
        FROM amazon_ads_report_cache WHERE shop_id=%s AND report_date BETWEEN %s AND %s
        AND report_type IN (%s,%s,%s,%s,%s) ORDER BY report_date''', (shop_id, start, end, *REPORT_TYPES), 1000)
    data['report_sources'] = ads_report_provenance.read_sources(cur, shop, start, end)
    data['generation'] = rows(cur, '''SELECT id,period,status,completed_at FROM report_generation_log
        WHERE shop_id=%s AND report_type='sku_profit' AND period BETWEEN %s AND %s ORDER BY id''',
        (shop_id, str(start), str(end)), 10000)
    data['inventory'] = rows(cur, f'''SELECT seller_sku,asin,fulfillable_quantity,inbound_working_quantity,
        inbound_shipped_quantity,inbound_receiving_quantity,sync_time FROM amazon_inventory
        WHERE shop_id=%s AND marketplace_id=%s AND BINARY seller_sku IN ({marks})''', (shop_id, US, *skus), 1000)
    data['offers'] = rows(cur, f'''SELECT sku,currency,audience,our_price,discounted_price,start_at,end_at,updated_at
        FROM amazon_listing_offers WHERE shop_id=%s AND marketplace_id=%s AND {sku_sql}''',
        (shop_id, US, *skus), 1000)
    # 复用现有只读售价公式，不另算成本，也不访问Amazon或写报价缓存。
    from services.listing_performance.analysis_export import unit_profit_reference
    data['unit_profit_references'] = [unit_profit_reference(cur, shop_id, US, sku) for sku in skus]
    references = {r['sku']: r for r in data['unit_profit_references']}
    data['pricing'] = [pricing.read(cur, shop_id, US, sku, references.get(sku)) for sku in skus]
    data['listing_history'] = rows(cur, f'''SELECT id,sku,operation_type,change_summary,write_status,
        verification_status,requested_at FROM amazon_listing_change_history
        WHERE shop_id=%s AND marketplace_id=%s AND {sku_sql} AND requested_at >= %s ORDER BY requested_at DESC''',
        (shop_id, US, *skus, operation_since.replace(tzinfo=None)), 10000)
    # 最新采集快照独立于报表截止日：预览不是历史回测，不能暗示当时已知。
    cur.execute('''SELECT t.id,t.record_date,t.completed_at,t.latest_file_id,f.row_count,f.imported_at
        FROM sellersprite_keyword_tasks t JOIN sellersprite_keyword_files f
        ON f.id=t.latest_file_id AND f.task_id=t.id AND f.status='imported'
        WHERE t.asin=%s AND t.marketplace='US' AND t.period='last30days' AND t.status='completed'
        ORDER BY t.record_date DESC,t.id DESC LIMIT 64''', (asin,))
    snapshots = list(cur.fetchall())
    data['sprite_snapshot'] = snapshots[0] if snapshots else None
    data['sprite_history'] = []
    data['sprite_rows'] = []
    if snapshots:
        # 最新及最接近7/14/30日前的完整快照；日期按实际采集，不伪造日趋势。
        latest_day = snapshots[0]['record_date']
        chosen = {snapshots[0]['id']: snapshots[0]}
        for offset in (7, 14, 30):
            prior = [s for s in snapshots if s['record_date'] <= latest_day - timedelta(days=offset)]
            if prior:
                chosen[prior[0]['id']] = prior[0]
        # 同日不同成功文件不重复当作多个自然日；保留最多另一个最近历史以便早期观察。
        recent = next((s for s in snapshots[1:] if s['record_date'] < latest_day), None)
        if recent:
            chosen[recent['id']] = recent
        for item in chosen.values():
            words = rows(cur, '''SELECT id,keyword,translation,traffic_share,traffic_type,
            organic_share,sponsored_share,weekly_impressions,
            organic_rank,organic_page,organic_updated_text,sponsored_rank,sponsored_page,sponsored_updated_text,
            monthly_searches,monthly_purchases,purchase_rate,ppc,suggested_bid_min,suggested_bid_max
            FROM sellersprite_keyword_rows WHERE file_id=%s ORDER BY traffic_share DESC,id''',
            (item['latest_file_id'],), 30000)
            if item['id'] == snapshots[0]['id']:
                data['sprite_rows'] = words
            else:
                data['sprite_history'].append({'snapshot': item, 'items': words})
    return data
