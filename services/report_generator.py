"""
报表数据生成器
从现有 ERP 业务表自动聚合生成三类核心报表：
  1. 经营日报/周报/月报
  2. SKU 利润表
  3. 库存周转分析

数据来源：
  - 销售额/订单数：amazon_orders + amazon_order_items
  - 产品成本、FBA 费、佣金、头程：统一调用 services/profit_calculator
  - 广告费：amazon_ad_spend（有则计入，无则记0）
  - 退款：amazon_refund_records（有则计入，无则记0）

生成策略：
  - 幂等设计：INSERT ... ON DUPLICATE KEY UPDATE，重复跑不会重复数据
  - 事务安全：单店铺单周期一个事务
  - 日志追踪：report_generation_log 记录每次生成耗时和结果

【重构说明 2026-05-26】
所有 SKU 维度的成本拆分（采购、头程、FBA、佣金）已下沉到 services/profit_calculator.py，
本模块不再维护重复逻辑，直接调用：
  - profit_calculator.get_unit_costs(cursor, seller_sku, exchange_rate) -> UnitCostBreakdown
  - profit_calculator.calculate_profit(sales, qty, unit_costs, ad_cost, refund) -> ProfitResult
确保定价模块、经营日报、SKU 利润表、库存周转四处的成本计算完全一致。
"""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from services.mysql_service import get_db_connection
from services.profit_calculator import (
    get_exchange_rate,
    get_unit_costs,
    calculate_profit,
)



def _log_generation_start(report_type, period, shop_id=0):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO report_generation_log (report_type, period, shop_id, status, started_at)
                VALUES (%s, %s, %s, 'running', NOW())
            """, (report_type, period, shop_id))
            log_id = cursor.lastrowid
        conn.commit()
        return log_id
    except Exception:
        conn.rollback()
        return None
    finally:
        conn.close()


def _log_generation_end(log_id, status, affected_rows=0, error_message=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE report_generation_log
                SET status = %s, completed_at = NOW(), affected_rows = %s, error_message = %s
                WHERE id = %s
            """, (status, affected_rows, error_message, log_id))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


# ==================== 1. 经营报表生成 ====================

def generate_business_daily(report_date, shop_id=None):
    """
    生成单日的经营日报
    report_date: 'YYYY-MM-DD'
    shop_id: None 则遍历所有店铺
    """
    report_type = 'business_daily'
    period = report_date

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 获取目标店铺列表
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        exchange_rate = None
        total_affected = 0

        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    # 1. 读取汇率（缓存，同一天内复用）
                    if exchange_rate is None:
                        exchange_rate = get_exchange_rate(cursor)

                    # 2. 订单维度汇总：销售额、订单数、SKU数、销量
                    cursor.execute("""
                        SELECT
                            COUNT(DISTINCT o.amazon_order_id) AS order_count,
                            COUNT(DISTINCT oi.seller_sku) AS sku_count,
                            COALESCE(SUM(oi.item_price_amount), 0) AS total_sales,
                            COALESCE(SUM(oi.quantity_shipped), 0) AS total_qty
                        FROM amazon_orders o
                        LEFT JOIN amazon_order_items oi
                            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND DATE(o.purchase_date) = %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                    """, (sid, report_date))
                    sales_row = cursor.fetchone()
                    total_sales = Decimal(str(sales_row['total_sales'] or 0))
                    order_count = int(sales_row['order_count'] or 0)
                    sku_count = int(sales_row['sku_count'] or 0)

                    # 3. 逐 SKU 计算成本项
                    cursor.execute("""
                        SELECT
                            oi.asin,
                            oi.seller_sku,
                            SUM(oi.quantity_shipped) AS qty,
                            SUM(oi.item_price_amount) AS revenue
                        FROM amazon_orders o
                        JOIN amazon_order_items oi
                            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND DATE(o.purchase_date) = %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.asin, oi.seller_sku
                    """, (sid, report_date))
                    sku_rows = cursor.fetchall()

                    total_cost = Decimal('0')
                    total_product_cost = Decimal('0')
                    total_fba_fees = Decimal('0')
                    total_commission = Decimal('0')
                    total_headway = Decimal('0')

                    for row in sku_rows:
                        seller_sku = row['seller_sku']
                        qty = int(row['qty'] or 0)
                        revenue = Decimal(str(row['revenue'] or 0))

                        # 统一成本入口（2026-05-26 重构）
                        unit_costs = get_unit_costs(cursor, seller_sku, exchange_rate)
                        profit = calculate_profit(revenue, qty, unit_costs)

                        total_product_cost += profit.product_cost
                        total_fba_fees += profit.fba_fees
                        total_commission += profit.commission
                        total_headway += profit.headway_cost

                    # 4. 广告费（仅读取 amazon_ad_spend，无数据则记0）
                    cursor.execute("""
                        SELECT COALESCE(SUM(ad_spend), 0) AS ad_sum
                        FROM amazon_ad_spend
                        WHERE shop_id = %s AND date = %s
                    """, (sid, report_date))
                    ad_row = cursor.fetchone()
                    ad_cost = Decimal(str(ad_row['ad_sum'] or 0))

                    # 5. 退款（仅读取 amazon_refund_records，无数据则记0）
                    cursor.execute("""
                        SELECT COALESCE(SUM(refund_amount), 0) AS refund_sum
                        FROM amazon_refund_records
                        WHERE shop_id = %s AND refund_date = %s
                    """, (sid, report_date))
                    refund_row = cursor.fetchone()
                    refund_amount = Decimal(str(refund_row['refund_sum'] or 0))
                    refund_rate = (refund_amount / total_sales) if total_sales > 0 else Decimal('0')

                    # 6. 总成本 = 产品成本 + FBA + 佣金 + 头程 + 退款 + 广告费
                    total_cost = total_product_cost + total_fba_fees + total_commission + total_headway + refund_amount + ad_cost
                    gross_profit = total_sales - total_cost
                    gross_profit_rate = (gross_profit / total_sales) if total_sales > 0 else Decimal('0')
                    headway_ratio = (total_headway / total_sales) if total_sales > 0 else Decimal('0')

                    # 7. 写入/更新报表
                    # 日报：report_date 有值，report_week=''，report_month=''
                    cursor.execute("""
                        INSERT INTO report_business (
                            shop_id, report_type, report_date, report_week, report_month,
                            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
                            headway_cost, headway_ratio, order_count, sku_count,
                            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
                        ) VALUES (%s, 'daily', %s, '', '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            total_sales = VALUES(total_sales),
                            total_cost = VALUES(total_cost),
                            product_cost = VALUES(product_cost),
                            gross_profit = VALUES(gross_profit),
                            gross_profit_rate = VALUES(gross_profit_rate),
                            headway_cost = VALUES(headway_cost),
                            headway_ratio = VALUES(headway_ratio),
                            order_count = VALUES(order_count),
                            sku_count = VALUES(sku_count),
                            ad_cost = VALUES(ad_cost),
                            refund_amount = VALUES(refund_amount),
                            refund_rate = VALUES(refund_rate),
                            platform_fees = VALUES(platform_fees),
                            fba_fees = VALUES(fba_fees),
                            updated_at = NOW()
                    """, (
                        sid, report_date,
                        float(total_sales), float(total_cost), float(total_product_cost), float(gross_profit), float(gross_profit_rate),
                        float(total_headway), float(headway_ratio), order_count, sku_count,
                        float(ad_cost), float(refund_amount), float(refund_rate),
                        float(total_commission), float(total_fba_fees)
                    ))
                    total_affected += cursor.rowcount

                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', cursor.rowcount)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected, "shops_processed": len(shops)}
    finally:
        conn.close()


def generate_business_weekly(start_date, end_date, shop_id=None):
    """
    基于已生成的日报汇总周报
    start_date/end_date: 'YYYY-MM-DD'，如 '2026-05-11' / '2026-05-17'
    """
    report_type = 'business_weekly'
    period = f"{start_date}~{end_date}"
    # 对外展示用范围格式 2026.05.11~2026.05.17
    report_week_label = start_date.replace('-', '.') + '~' + end_date.replace('-', '.')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        total_affected = 0
        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    # 周报：report_date 用该周周一（方便按日期范围查询），report_week 有值，report_month=''
                    cursor.execute("""
                        INSERT INTO report_business (
                            shop_id, report_type, report_date, report_week, report_month,
                            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
                            headway_cost, headway_ratio, order_count, sku_count,
                            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
                        )
                        SELECT
                            shop_id,
                            'weekly' AS report_type,
                            %s AS report_date,
                            %s AS report_week,
                            '' AS report_month,
                            SUM(total_sales),
                            SUM(total_cost),
                            SUM(product_cost),
                            SUM(gross_profit),
                            AVG(gross_profit_rate),
                            SUM(headway_cost),
                            AVG(headway_ratio),
                            SUM(order_count),
                            MAX(sku_count),
                            SUM(ad_cost),
                            SUM(refund_amount),
                            AVG(refund_rate),
                            SUM(platform_fees),
                            SUM(fba_fees)
                        FROM report_business
                        WHERE shop_id = %s AND report_type = 'daily'
                          AND report_date BETWEEN %s AND %s
                        GROUP BY shop_id
                        ON DUPLICATE KEY UPDATE
                            total_sales = VALUES(total_sales),
                            total_cost = VALUES(total_cost),
                            product_cost = VALUES(product_cost),
                            gross_profit = VALUES(gross_profit),
                            gross_profit_rate = VALUES(gross_profit_rate),
                            headway_cost = VALUES(headway_cost),
                            headway_ratio = VALUES(headway_ratio),
                            order_count = VALUES(order_count),
                            sku_count = VALUES(sku_count),
                            ad_cost = VALUES(ad_cost),
                            refund_amount = VALUES(refund_amount),
                            refund_rate = VALUES(refund_rate),
                            platform_fees = VALUES(platform_fees),
                            fba_fees = VALUES(fba_fees),
                            updated_at = NOW()
                    """, (start_date, report_week_label, sid, start_date, end_date))
                    total_affected += cursor.rowcount
                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', cursor.rowcount)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected}
    finally:
        conn.close()


def generate_business_monthly(month_str, shop_id=None):
    """
    基于已生成的日报汇总月报
    month_str: 'YYYY-MM' 如 '2026-05'
    """
    report_type = 'business_monthly'
    period = month_str
    year, month = map(int, month_str.split('-'))
    start_date = f'{year}-{month:02d}-01'
    # 月末
    if month == 12:
        end_date = f'{year + 1}-01-01'
    else:
        end_date = f'{year}-{month + 1:02d}-01'
    # 减一天
    from datetime import datetime as dt
    end_date = (dt.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        total_affected = 0
        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    # 月报：report_date 用该月1号（方便按日期范围查询），report_week=''，report_month 有值
                    month_start = f'{year}-{month:02d}-01'
                    cursor.execute("""
                        INSERT INTO report_business (
                            shop_id, report_type, report_date, report_week, report_month,
                            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
                            headway_cost, headway_ratio, order_count, sku_count,
                            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
                        )
                        SELECT
                            shop_id,
                            'monthly' AS report_type,
                            %s AS report_date,
                            '' AS report_week,
                            %s AS report_month,
                            SUM(total_sales),
                            SUM(total_cost),
                            SUM(product_cost),
                            SUM(gross_profit),
                            AVG(gross_profit_rate),
                            SUM(headway_cost),
                            AVG(headway_ratio),
                            SUM(order_count),
                            MAX(sku_count),
                            SUM(ad_cost),
                            SUM(refund_amount),
                            AVG(refund_rate),
                            SUM(platform_fees),
                            SUM(fba_fees)
                        FROM report_business
                        WHERE shop_id = %s AND report_type = 'daily'
                          AND report_date BETWEEN %s AND %s
                        GROUP BY shop_id
                        ON DUPLICATE KEY UPDATE
                            total_sales = VALUES(total_sales),
                            total_cost = VALUES(total_cost),
                            product_cost = VALUES(product_cost),
                            gross_profit = VALUES(gross_profit),
                            gross_profit_rate = VALUES(gross_profit_rate),
                            headway_cost = VALUES(headway_cost),
                            headway_ratio = VALUES(headway_ratio),
                            order_count = VALUES(order_count),
                            sku_count = VALUES(sku_count),
                            ad_cost = VALUES(ad_cost),
                            refund_amount = VALUES(refund_amount),
                            refund_rate = VALUES(refund_rate),
                            platform_fees = VALUES(platform_fees),
                            fba_fees = VALUES(fba_fees),
                            updated_at = NOW()
                    """, (month_start, month_str, sid, start_date, end_date))
                    total_affected += cursor.rowcount
                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', cursor.rowcount)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected}
    finally:
        conn.close()


# ==================== 2. SKU 利润表生成 ====================

def generate_sku_profit(period_start, period_end, shop_id=None):
    """
    按 ASIN/SKU 汇总指定周期内的利润数据
    period_start/end: 'YYYY-MM-DD'
    """
    report_type = 'sku_profit'
    period = f"{period_start}~{period_end}"

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        exchange_rate = None
        total_affected = 0

        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    if exchange_rate is None:
                        exchange_rate = get_exchange_rate(cursor)

                    # 1. 聚合销售数据
                    cursor.execute("""
                        SELECT
                            oi.asin,
                            oi.seller_sku AS sku,
                            SUM(oi.quantity_shipped) AS sales_qty,
                            SUM(oi.item_price_amount) AS sales_amount,
                            AVG(oi.item_price_amount / NULLIF(oi.quantity_shipped, 0)) AS avg_price
                        FROM amazon_orders o
                        JOIN amazon_order_items oi
                            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND DATE(o.purchase_date) BETWEEN %s AND %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.asin, oi.seller_sku
                    """, (sid, period_start, period_end))
                    sku_rows = cursor.fetchall()

                    for row in sku_rows:
                        asin = row['asin'] or ''
                        sku = row['sku'] or ''
                        sales_qty = int(row['sales_qty'] or 0)
                        sales_amount = Decimal(str(row['sales_amount'] or 0))
                        avg_price = Decimal(str(row['avg_price'] or 0))

                        # 统一成本入口（2026-05-26 重构）
                        unit_costs = get_unit_costs(cursor, sku, exchange_rate)
                        product_name = unit_costs.product_name or ''

                        # 广告费（按 ASIN 汇总，无数据则记0）
                        cursor.execute("""
                            SELECT COALESCE(SUM(ad_spend), 0) AS ad_sum
                            FROM amazon_ad_spend
                            WHERE shop_id = %s AND asin = %s
                              AND date BETWEEN %s AND %s
                        """, (sid, asin, period_start, period_end))
                        ad_row = cursor.fetchone()
                        ad_cost = Decimal(str(ad_row['ad_sum'] or 0))

                        # 退款（按 ASIN 汇总，无数据则记0）
                        cursor.execute("""
                            SELECT COALESCE(SUM(refund_amount), 0) AS refund_sum
                            FROM amazon_refund_records
                            WHERE shop_id = %s AND asin = %s
                              AND refund_date BETWEEN %s AND %s
                        """, (sid, asin, period_start, period_end))
                        refund_row = cursor.fetchone()
                        refund_amount = Decimal(str(refund_row['refund_sum'] or 0))

                        # 统一利润公式
                        profit = calculate_profit(sales_amount, sales_qty, unit_costs, ad_cost, refund_amount)
                        product_cost = profit.product_cost
                        fba_fees = profit.fba_fees
                        platform_fees = profit.commission
                        headway_cost = profit.headway_cost
                        gross_profit = profit.gross_profit
                        net_profit = profit.net_profit
                        profit_margin = profit.profit_margin
                        other_fees = profit.other_fees

                        cursor.execute("""
                            INSERT INTO sku_profit (
                                shop_id, asin, sku, product_name, period_start, period_end,
                                sales_qty, sales_amount, avg_selling_price,
                                product_cost, fba_fees, ad_cost, headway_cost, platform_fees,
                                refund_amount, other_fees,
                                gross_profit, net_profit, profit_margin
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                sales_qty = VALUES(sales_qty),
                                sales_amount = VALUES(sales_amount),
                                avg_selling_price = VALUES(avg_selling_price),
                                product_cost = VALUES(product_cost),
                                fba_fees = VALUES(fba_fees),
                                ad_cost = VALUES(ad_cost),
                                headway_cost = VALUES(headway_cost),
                                platform_fees = VALUES(platform_fees),
                                refund_amount = VALUES(refund_amount),
                                other_fees = VALUES(other_fees),
                                gross_profit = VALUES(gross_profit),
                                net_profit = VALUES(net_profit),
                                profit_margin = VALUES(profit_margin),
                                updated_at = NOW()
                        """, (
                            sid, asin, sku, product_name, period_start, period_end,
                            sales_qty, float(sales_amount), float(avg_price),
                            float(product_cost), float(fba_fees), float(ad_cost),
                            float(headway_cost), float(platform_fees),
                            float(refund_amount), float(other_fees),
                            float(gross_profit), float(net_profit), float(profit_margin)
                        ))
                        total_affected += 1

                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', total_affected)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected, "shops_processed": len(shops)}
    finally:
        conn.close()


# ==================== 3. 库存周转生成 ====================

def generate_inventory_turnover(shop_id=None):
    """
    基于当前库存和近30天销售速度生成库存周转数据
    """
    report_type = 'inventory_turnover'
    period = datetime.now().strftime('%Y-%m-%d')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        exchange_rate = None
        total_affected = 0

        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    if exchange_rate is None:
                        exchange_rate = get_exchange_rate(cursor)

                    # 1. 获取当前库存
                    cursor.execute("""
                        SELECT
                            seller_sku AS sku,
                            asin,
                            product_name,
                            fulfillable_quantity AS current_stock,
                            inbound_working_quantity,
                            inbound_shipped_quantity,
                            inbound_receiving_quantity
                        FROM amazon_inventory
                        WHERE shop_id = %s
                    """, (sid,))
                    inv_rows = cursor.fetchall()

                    # 2. 近30天销售速度
                    cursor.execute("""
                        SELECT
                            oi.seller_sku AS sku,
                            SUM(oi.quantity_shipped) AS sales_30d,
                            MAX(DATE(o.purchase_date)) AS last_sale_date,
                            COUNT(DISTINCT DATE(o.purchase_date)) AS sale_days
                        FROM amazon_orders o
                        JOIN amazon_order_items oi
                            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND o.purchase_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.seller_sku
                    """, (sid,))
                    sales_map = {}
                    for r in cursor.fetchall():
                        sales_map[r['sku']] = r

                    # 3. 近7天销量
                    cursor.execute("""
                        SELECT
                            oi.seller_sku AS sku,
                            SUM(oi.quantity_shipped) AS sales_7d
                        FROM amazon_orders o
                        JOIN amazon_order_items oi
                            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND o.purchase_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.seller_sku
                    """, (sid,))
                    sales_7d_map = {r['sku']: r['sales_7d'] for r in cursor.fetchall()}

                    for inv in inv_rows:
                        sku = inv['sku']
                        asin = inv['asin'] or ''
                        product_name = inv['product_name'] or ''
                        current_stock = int(inv['current_stock'] or 0)
                        inbound_qty = (
                            int(inv['inbound_working_quantity'] or 0) +
                            int(inv['inbound_shipped_quantity'] or 0) +
                            int(inv['inbound_receiving_quantity'] or 0)
                        )
                        total_available = current_stock + inbound_qty

                        sales_30d = int(sales_map.get(sku, {}).get('sales_30d') or 0)
                        sales_7d = int(sales_7d_map.get(sku, 0) or 0)
                        avg_daily_sales = Decimal(str(sales_30d)) / Decimal('30') if sales_30d > 0 else Decimal('0')

                        last_sale_date = sales_map.get(sku, {}).get('last_sale_date')
                        if last_sale_date:
                            days_without_sale = (datetime.now().date() - last_sale_date).days
                        else:
                            days_without_sale = 999

                        if avg_daily_sales > 0:
                            turnover_days = int((Decimal(str(current_stock)) / avg_daily_sales).to_integral_value(rounding=ROUND_HALF_UP))
                        else:
                            turnover_days = 9999

                        # 状态判断
                        if current_stock == 0 and days_without_sale >= 30:
                            stock_status = 'out_of_stock'
                        elif turnover_days > 90 or days_without_sale >= 30:
                            stock_status = 'slow'
                        elif turnover_days <= 7 and current_stock > 0:
                            stock_status = 'warning'
                        else:
                            stock_status = 'normal'

                        # 建议补货 = max(0, 日均销量 * 60 - 总可用)
                        suggested = max(0, int((avg_daily_sales * Decimal('60') - Decimal(str(total_available))).to_integral_value(rounding=ROUND_HALF_UP)))

                        # 单位成本（统一入口，2026-05-26 重构）
                        unit_costs = get_unit_costs(cursor, sku, exchange_rate)
                        unit_cost = unit_costs.purchase_cost_usd + unit_costs.headway_cost_usd

                        inventory_value = unit_cost * Decimal(str(current_stock))

                        cursor.execute("""
                            INSERT INTO inventory_turnover (
                                shop_id, sku, asin, product_name,
                                current_stock, inbound_qty, total_available,
                                avg_daily_sales, sales_7d, sales_30d, turnover_days,
                                stock_status, last_sale_date, days_without_sale, suggested_replenish,
                                unit_cost, inventory_value
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                asin = VALUES(asin),
                                product_name = VALUES(product_name),
                                current_stock = VALUES(current_stock),
                                inbound_qty = VALUES(inbound_qty),
                                total_available = VALUES(total_available),
                                avg_daily_sales = VALUES(avg_daily_sales),
                                sales_7d = VALUES(sales_7d),
                                sales_30d = VALUES(sales_30d),
                                turnover_days = VALUES(turnover_days),
                                stock_status = VALUES(stock_status),
                                last_sale_date = VALUES(last_sale_date),
                                days_without_sale = VALUES(days_without_sale),
                                suggested_replenish = VALUES(suggested_replenish),
                                unit_cost = VALUES(unit_cost),
                                inventory_value = VALUES(inventory_value),
                                updated_at = NOW()
                        """, (
                            sid, sku, asin, product_name,
                            current_stock, inbound_qty, total_available,
                            float(avg_daily_sales), sales_7d, sales_30d, turnover_days,
                            stock_status, last_sale_date, days_without_sale, suggested,
                            float(unit_cost), float(inventory_value)
                        ))
                        total_affected += 1

                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', total_affected)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected, "shops_processed": len(shops)}
    finally:
        conn.close()


# ==================== 4. 广告效果报表生成 ====================

def _calc_ad_metrics(impressions, clicks, ad_spend, sales_7d, sales_30d):
    """
    计算广告比率指标，返回字典。
    分母为0时返回 NULL（数据库层面用 None 表示）。
    """
    ctr = (Decimal(str(clicks)) / Decimal(str(impressions))) if impressions > 0 else None
    cpc = (Decimal(str(ad_spend)) / Decimal(str(clicks))) if clicks > 0 else None
    acos_7d = (Decimal(str(ad_spend)) / Decimal(str(sales_7d))) if sales_7d > 0 else None
    acos_30d = (Decimal(str(ad_spend)) / Decimal(str(sales_30d))) if sales_30d > 0 else None
    roas_7d = (Decimal(str(sales_7d)) / Decimal(str(ad_spend))) if ad_spend > 0 else None
    roas_30d = (Decimal(str(sales_30d)) / Decimal(str(ad_spend))) if ad_spend > 0 else None
    return {
        'ctr': float(ctr) if ctr is not None else None,
        'cpc': float(cpc) if cpc is not None else None,
        'acos_7d': float(acos_7d) if acos_7d is not None else None,
        'acos_30d': float(acos_30d) if acos_30d is not None else None,
        'roas_7d': float(roas_7d) if roas_7d is not None else None,
        'roas_30d': float(roas_30d) if roas_30d is not None else None,
    }


def _insert_advertising_report(cursor, report_type, report_date, report_week, report_month,
                                shop_id, dimension_type, campaign_id, campaign_name,
                                ad_group_id, ad_group_name, asin, sku,
                                impressions, clicks, ad_spend,
                                orders_7d, orders_30d, sales_7d, sales_30d):
    """插入/更新单条广告效果报表记录"""
    metrics = _calc_ad_metrics(impressions, clicks, ad_spend, sales_7d, sales_30d)
    cursor.execute("""
        INSERT INTO report_advertising (
            shop_id, report_type, report_date, report_week, report_month,
            dimension_type, campaign_id, campaign_name, ad_group_id, ad_group_name,
            asin, sku, impressions, clicks, ad_spend,
            orders_7d, orders_30d, sales_7d, sales_30d,
            ctr, cpc, acos_7d, acos_30d, roas_7d, roas_30d
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            impressions = VALUES(impressions),
            clicks = VALUES(clicks),
            ad_spend = VALUES(ad_spend),
            orders_7d = VALUES(orders_7d),
            orders_30d = VALUES(orders_30d),
            sales_7d = VALUES(sales_7d),
            sales_30d = VALUES(sales_30d),
            ctr = VALUES(ctr),
            cpc = VALUES(cpc),
            acos_7d = VALUES(acos_7d),
            acos_30d = VALUES(acos_30d),
            roas_7d = VALUES(roas_7d),
            roas_30d = VALUES(roas_30d),
            campaign_name = VALUES(campaign_name),
            ad_group_name = VALUES(ad_group_name),
            sku = VALUES(sku),
            updated_at = NOW()
    """, (
        shop_id, report_type, report_date, report_week, report_month,
        dimension_type, campaign_id, campaign_name, ad_group_id, ad_group_name,
        asin, sku, impressions, clicks, ad_spend,
        orders_7d, orders_30d, sales_7d, sales_30d,
        metrics['ctr'], metrics['cpc'], metrics['acos_7d'], metrics['acos_30d'],
        metrics['roas_7d'], metrics['roas_30d']
    ))


def _generate_advertising_from_ads(cursor, shop_id, report_type, report_date,
                                   report_week, report_month, date_start, date_end):
    """
    从 amazon_ad_spend 聚合生成广告效果报表。
    同时生成 overall / campaign / ad_group / asin 四个维度。
    """
    total_affected = 0

    # 1. overall 维度
    cursor.execute("""
        SELECT
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(ad_spend), 0) AS ad_spend,
            COALESCE(SUM(orders_7d), 0) AS orders_7d,
            COALESCE(SUM(orders_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ad_spend
        WHERE shop_id = %s AND date BETWEEN %s AND %s
    """, (shop_id, date_start, date_end))
    row = cursor.fetchone()
    if row and row['ad_spend'] is not None:
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'overall', '', '', '', '', '', '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    # 2. campaign 维度
    cursor.execute("""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(ad_spend), 0) AS ad_spend,
            COALESCE(SUM(orders_7d), 0) AS orders_7d,
            COALESCE(SUM(orders_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ad_spend
        WHERE shop_id = %s AND date BETWEEN %s AND %s AND campaign_id != ''
        GROUP BY campaign_id
    """, (shop_id, date_start, date_end))
    for row in cursor.fetchall():
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'campaign', row['campaign_id'], row['campaign_name'] or '',
            '', '', '', '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    # 3. ad_group 维度
    cursor.execute("""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            ad_group_id,
            MAX(ad_group_name) AS ad_group_name,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(ad_spend), 0) AS ad_spend,
            COALESCE(SUM(orders_7d), 0) AS orders_7d,
            COALESCE(SUM(orders_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ad_spend
        WHERE shop_id = %s AND date BETWEEN %s AND %s AND ad_group_id != ''
        GROUP BY campaign_id, ad_group_id
    """, (shop_id, date_start, date_end))
    for row in cursor.fetchall():
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'ad_group', row['campaign_id'], row['campaign_name'] or '',
            row['ad_group_id'], row['ad_group_name'] or '', '', '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    # 4. asin 维度
    cursor.execute("""
        SELECT
            asin,
            MAX(sku) AS sku,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(ad_spend), 0) AS ad_spend,
            COALESCE(SUM(orders_7d), 0) AS orders_7d,
            COALESCE(SUM(orders_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ad_spend
        WHERE shop_id = %s AND date BETWEEN %s AND %s AND asin != ''
        GROUP BY asin
    """, (shop_id, date_start, date_end))
    for row in cursor.fetchall():
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'asin', '', '', '', '', row['asin'], row['sku'] or '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    return total_affected


def generate_advertising_daily(report_date, shop_id=None):
    """
    生成单日广告效果报表（4个维度）
    report_date: 'YYYY-MM-DD'
    """
    report_type = 'advertising_daily'
    period = report_date

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        total_affected = 0
        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    affected = _generate_advertising_from_ads(
                        cursor, sid, 'daily', report_date, '', '', report_date, report_date
                    )
                    total_affected += affected
                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', affected)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected, "shops_processed": len(shops)}
    finally:
        conn.close()


def generate_advertising_weekly(start_date, end_date, shop_id=None):
    """
    生成周报广告效果报表
    start_date/end_date: 'YYYY-MM-DD'
    """
    report_type = 'advertising_weekly'
    period = f"{start_date}~{end_date}"
    report_week_label = start_date.replace('-', '.') + '~' + end_date.replace('-', '.')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        total_affected = 0
        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    affected = _generate_advertising_from_ads(
                        cursor, sid, 'weekly', start_date, report_week_label, '', start_date, end_date
                    )
                    total_affected += affected
                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', affected)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected}
    finally:
        conn.close()


def generate_advertising_monthly(month_str, shop_id=None):
    """
    生成月报广告效果报表
    month_str: 'YYYY-MM' 如 '2026-05'
    """
    report_type = 'advertising_monthly'
    period = month_str
    year, month = map(int, month_str.split('-'))
    start_date = f'{year}-{month:02d}-01'
    if month == 12:
        end_date = f'{year + 1}-01-01'
    else:
        end_date = f'{year}-{month + 1:02d}-01'
    from datetime import datetime as _dt
    end_date = (_dt.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if shop_id is not None:
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
            else:
                cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
            shops = [r['id'] for r in cursor.fetchall()]

        total_affected = 0
        for sid in shops:
            log_id = _log_generation_start(report_type, period, sid)
            try:
                with conn.cursor() as cursor:
                    month_start = start_date
                    affected = _generate_advertising_from_ads(
                        cursor, sid, 'monthly', month_start, '', month_str, start_date, end_date
                    )
                    total_affected += affected
                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', affected)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        return {"status": "success", "affected_rows": total_affected}
    finally:
        conn.close()


def generate_yesterday_reports():
    """
    一键生成昨日全部报表（供 scheduler 调用）
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    results = {}

    # 1. 经营日报
    results['business_daily'] = generate_business_daily(yesterday)

    # 2. SKU 利润（昨日单天）
    results['sku_profit'] = generate_sku_profit(yesterday, yesterday)

    # 3. 库存周转
    results['inventory_turnover'] = generate_inventory_turnover()

    # 4. 广告效果日报
    results['advertising_daily'] = generate_advertising_daily(yesterday)

    # 5. 周报（如果昨天是周日，则生成本周周报：周一~周日）
    yesterday_dt = datetime.strptime(yesterday, '%Y-%m-%d')
    if yesterday_dt.weekday() == 6:  # Sunday=6
        week_start = (yesterday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
        week_end = yesterday
        results['business_weekly'] = generate_business_weekly(week_start, week_end)
        results['advertising_weekly'] = generate_advertising_weekly(week_start, week_end)

    # 6. 月报（如果昨天是月末，则生成本月月报）
    today = datetime.now().date()
    if today.day == 1:  # 今天1号，说明昨天是月末
        month_str = (today - timedelta(days=1)).strftime('%Y-%m')
        results['business_monthly'] = generate_business_monthly(month_str)
        results['advertising_monthly'] = generate_advertising_monthly(month_str)

    return results
