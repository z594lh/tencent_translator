"""人员实时SKU绩效的订单数只读补充，仅支持SKU日报日期口径。

performance_service/breakdown_service使用本模块补充SKU日报本身没有的订单数。
SKU模式按源订单存储日期对齐日报、按该业务日零点历史负责人归属，与日报金额一致。
从真实已发货订单项构建去重集合，不重算利润、不写报表或绩效快照。
跨SKU/人员可有同一订单，不能把行订单数简单相加当全店订单数。
"""
from collections import defaultdict
from datetime import datetime, time, timedelta
from services.listing_performance import ownership_repository as ownership
from services.listing_performance.common import DomainError, day_bounds, identity


def read(cursor, shop, market, first, last, config, basis, allowed_days=None, allowed_sku_days=None):
    """读取真实已发货订单并返回owner/sku/daily/trend四种去重集合索引。

    shop/market限定来源，first/last含首尾，config提供负责人业务日时区。
    allowed_days限制到有SKU日报的相同日期；sku_report按源DATE(purchase_date)对齐。
    allowed_sku_days可进一步限定实际计入的精确(SKU,日期)，防止有订单但无日报的SKU混入。
    缺商品身份记录missing_orders并complete=False；超10万行报422。
    只读；按SKU业务日零点负责历史归属，不再保留旧订单利润模式分支。
    当前用于实时日报汇总，不得当作历史快照已经冻结的订单数。"""
    if basis != 'sku_report':
        raise DomainError('仅支持SKU日报的订单数统计')
    # 日报使用存储purchase_date的日期，数量也必须沿用同一日期边界。
    start = datetime.combine(first, time.min)
    end = datetime.combine(last + timedelta(days=1), time.min)
    cursor.execute('''SELECT o.amazon_order_id,o.purchase_date,oi.seller_sku,oi.order_item_id,
        oi.item_price_currency_code FROM amazon_orders o LEFT JOIN amazon_order_items oi
        ON oi.shop_id=o.shop_id AND oi.amazon_order_id=o.amazon_order_id
        WHERE o.shop_id=%s AND o.marketplace_id=%s AND o.purchase_date>=%s AND o.purchase_date<%s
        AND o.order_status NOT IN ('Canceled','Pending','PendingAvailability')
        AND (oi.quantity_shipped>0 OR oi.order_item_id IS NULL)
        ORDER BY o.amazon_order_id,oi.order_item_id LIMIT 100001''', (shop,market,start,end))
    rows = cursor.fetchall()
    if len(rows) > 100000:
        raise DomainError('订单统计超过查询上限，请缩小日期范围', 422)
    owner_orders, sku_orders, daily_orders, trend_orders = (defaultdict(set) for _ in range(4))
    missing, cache = set(), {}
    for row in rows:
        day = row['purchase_date'].date()
        # 订单数与金额必须覆盖相同的已有SKU日报业务日。
        if allowed_days is not None and day not in allowed_days:
            continue
        sku = row['seller_sku']
        order = row['amazon_order_id']
        if not row['order_item_id'] or not sku:
            if allowed_sku_days is not None and not allowed_sku_days:
                continue
            missing.add(order)
            continue
        if allowed_sku_days is not None and (sku,day) not in allowed_sku_days:
            continue
        if sku not in cache:
            scope = ownership.scope(cursor, {'identity_key':identity(shop,market,sku)})
            cache[sku] = ownership.histories(cursor, scope['id']) if scope else []
        # 冬季订单固定UTC-7存储与站点UTC-8相差一小时，按日报业务日归属避免跨负责人。
        owner_at = day_bounds(day,config['timezone'])[0]
        owner = ownership.active_at(cache[sku], owner_at)
        owner_id = owner['owner_user_id'] if owner else None
        currency = config.get('report_currency',config['currency'])
        owner_orders[(owner_id,currency)].add(order)
        sku_orders[(owner_id,sku,currency)].add(order)
        daily_orders[(owner_id,sku,currency,day)].add(order)
        trend_orders[(owner_id,currency,day)].add(order)
    return {'owner':owner_orders, 'sku':sku_orders, 'daily':daily_orders, 'trend':trend_orders,
            'missing_orders':len(missing), 'complete':not missing}


def count(index, kind, key):
    """按kind/key返回索引中的去重订单数量。

    整批来源不完整时返回None而非零；空集合在来源完整时才是0，不写数据。"""
    return len(index[kind].get(key, ())) if index['complete'] else None
