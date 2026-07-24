"""
报表数据生成器

简介: 从业务表自动聚合生成四类核心报表 + 一键调度入口。

数据来源:
  - 已结算日報 (settled):  amazon_order_finances（Amazon 实际打款净额）+ profit_calculator（内部成本）
  - 预估日報 (estimated):  amazon_orders + amazon_order_items（仅销售额/订单数）
  - 广告费:                amazon_ad_spend
  - 退款:                  amazon_refund_records / amazon_order_finances Refund 交易

生成策略:
  - 幂等: INSERT ... ON DUPLICATE KEY UPDATE
  - 事务: 单店铺单周期一个事务
  - 日志: report_generation_log 记录每次耗时和结果
  - 时区: 所有 report_date 使用 PDT 时间（UTC-7），与 Amazon 统一

模块入口:
  - generate_yesterday_reports() — Cron 每天凌晨 2 点调用（此时 PDT 前一天数据已完整）
  - generate_business_daily/weekly/monthly — 手动生成经营报表
  - generate_sku_profit — 手动生成 SKU 利润表
  - generate_inventory_turnover — 生成库存周转

成本计算 (2026-05-26 重构):
  所有 SKU 成本计算统一通过 profit_calculator:
    - get_unit_costs(cursor, seller_sku, exchange_rate) -> UnitCostBreakdown
    - calculate_profit(sales, qty, unit_costs, ad_cost, refund) -> ProfitResult
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json

from services.mysql_service import get_db_connection
from services.profit_calculator import (
    get_exchange_rate,
    get_unit_costs,
    calculate_profit,
)
from services.notification_dispatcher import fire

# 价格兜底 SQL：item_price 为 NULL 时用 listing_offers.our_price * 数量 替换
# item_price_amount 已是行级总价，非 NULL 时直接使用不乘数量
_PRICE_FALLBACK_SQL = """COALESCE(oi.item_price_amount, (
    SELECT our_price * GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1)
    FROM amazon_listing_offers lo
    WHERE lo.shop_id = oi.shop_id AND lo.sku = oi.seller_sku COLLATE utf8mb4_unicode_ci
    ORDER BY lo.updated_at DESC LIMIT 1
))"""


def _log_generation_start(report_type, period, shop_id=0):
    """
    记录生成开始

    简介: 向 report_generation_log 插入一条 running 状态记录，返回 log_id。
    """
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
    """
    记录生成结束

    简介: 更新 report_generation_log 的状态、完成时间和影响行数。
    """
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


def _check_finances_available(cursor, shop_id, report_date):
    """
    检查指定日期的 Finance 数据是否已结算

    简介: 查询 amazon_order_finances 是否存在该日期的 Shipment 交易记录。

    详细:
      - Amazon 财务结算通常延迟 2-3 天
      - 有 Shipment 记录 → 可生成 settled 报告
      - 无记录 → 仅能生成 estimated 报告
    """
    cursor.execute(
        """SELECT 1 FROM amazon_order_finances
           WHERE shop_id = %s AND DATE(posted_date) = %s
             AND transaction_type = 'Shipment'
           LIMIT 1""",
        (shop_id, report_date),
    )
    return cursor.fetchone() is not None


def _get_active_shops(cursor, shop_id=None):
    """获取活跃店铺 ID 列表"""
    if shop_id is not None:
        cursor.execute("SELECT id FROM amazon_shops WHERE id = %s AND status = 1", (shop_id,))
    else:
        cursor.execute("SELECT id FROM amazon_shops WHERE status = 1")
    return [r['id'] for r in cursor.fetchall()]


def _get_settled_shipment_data(cursor, sid, report_date):
    """获取指定日期的已结算 Shipment 数据（去重，优先 RELEASED 状态）"""
    cursor.execute("""
        SELECT dedup.items_json
        FROM (
            SELECT amazon_order_id, shop_id,
                   COALESCE(
                       MAX(CASE WHEN transaction_status = 'RELEASED' THEN items_json END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED_RELEASED' THEN items_json END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED' THEN items_json END)
                   ) AS items_json
            FROM amazon_order_finances
            WHERE transaction_type = 'Shipment'
            GROUP BY amazon_order_id, shop_id
        ) dedup
        JOIN amazon_orders o
            ON o.amazon_order_id = dedup.amazon_order_id COLLATE utf8mb4_unicode_ci
            AND o.shop_id = dedup.shop_id
        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
    """, (sid, report_date))
    return cursor.fetchall()


def _parse_finances_items(finance_rows):
    """解析 items_json，按 SKU 汇总 product_charges / fba / commission / quantityShipped"""
    sku_settled_qty = {}
    sku_sales_from_finance = {}
    sku_fees = {}

    for frow in finance_rows:
        items = frow['items_json']
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except (json.JSONDecodeError, TypeError):
                items = []
        for item in (items or []):
            it_pc = 0.0
            it_fba = 0.0
            it_com = 0.0
            for bd in (item.get('breakdowns', []) or []):
                bt = bd.get('breakdownType', '')
                if bt == 'ProductCharges':
                    subs = bd.get('breakdowns', []) or []
                    if subs:
                        for sub in subs:
                            amt = float((sub.get('breakdownAmount') or {}).get('currencyAmount', 0))
                            if amt > 0:
                                it_pc += amt
                    else:
                        amt = float((bd.get('breakdownAmount') or {}).get('currencyAmount', 0))
                        if amt > 0:
                            it_pc += amt
                elif bt == 'AmazonFees':
                    for sub in (bd.get('breakdowns', []) or []):
                        amt = float((sub.get('breakdownAmount') or {}).get('currencyAmount', 0))
                        st = sub.get('breakdownType', '')
                        if amt < 0:
                            if st.startswith('FBAPer'):
                                it_fba += abs(amt)
                            elif st == 'Commission':
                                it_com += abs(amt)
            for ctx in (item.get('contexts', []) or []):
                if ctx.get('contextType') == 'ProductContext':
                    sku = ctx.get('sku', '') or ''
                    qty = int(ctx.get('quantityShipped', 0) or 0)
                    if sku:
                        sku_settled_qty[sku] = sku_settled_qty.get(sku, 0) + qty
                        if it_pc > 0:
                            se = sku_sales_from_finance.setdefault(sku, 0.0)
                            sku_sales_from_finance[sku] = se + it_pc
                        if it_fba > 0 or it_com > 0:
                            entry = sku_fees.setdefault(sku, {'fba': 0.0, 'commission': 0.0})
                            entry['fba'] += it_fba
                            entry['commission'] += it_com

    return sku_settled_qty, sku_sales_from_finance, sku_fees


def _get_unsettled_sales(cursor, sid, report_date):
    """获取未结算订单的预估销售数据（PRICE_FALLBACK），按 ASIN 汇总"""
    cursor.execute(f"""
        SELECT oi.asin,
               COALESCE(SUM(GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1)), 0) AS unsettled_qty,
               COALESCE(SUM({_PRICE_FALLBACK_SQL}), 0) AS unsettled_sales
        FROM amazon_orders o
        JOIN amazon_order_items oi ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
          AND NOT EXISTS (
            SELECT 1 FROM amazon_order_finances f2
            WHERE f2.shop_id = %s AND f2.transaction_type = 'Shipment'
              AND f2.amazon_order_id = o.amazon_order_id COLLATE utf8mb4_unicode_ci
          )
        GROUP BY oi.asin
    """, (sid, report_date, sid))
    result = {}
    for r in cursor.fetchall():
        result[r['asin']] = {
            'qty': int(r['unsettled_qty'] or 0),
            'sales': float(r['unsettled_sales'] or 0),
        }
    return result


def _get_daily_refund(cursor, sid, report_date):
    """获取当日退款总额（Finances Refund RELEASED）"""
    cursor.execute("""
        SELECT COALESCE(SUM(ABS(f.total_amount)), 0) AS refund_sum
        FROM amazon_order_finances f
        JOIN amazon_orders o
            ON o.amazon_order_id = f.amazon_order_id COLLATE utf8mb4_unicode_ci AND o.shop_id = f.shop_id
        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
          AND f.transaction_type = 'Refund'
          AND f.transaction_status = 'RELEASED'
    """, (sid, report_date))
    return Decimal(str(cursor.fetchone()['refund_sum'] or 0))


def _generate_estimated_daily(cursor, sid, report_date, exchange_rate):
    """
    生成 estimated（预估）日报数据

    简介: 从 amazon_orders + amazon_order_items 拉取销售额/订单数/SKU数，
          从 amazon_product_fees 估算 FBA 费 + 佣金（finance 数据未到达时用）。

    详细:
      - total_sales = 前台售价汇总（gross），无 item_price 时取 listing_offers.our_price
      - fba_fees = SUM(real_fba_fee or fba_fee × qty) 按 SKU 估算
      - commission = SUM(price × (real_commission_rate or commission_rate or 0.15))
      - 采购成本/头程 = 从 profit_calculator 按 SKU 计算
      - 毛利/毛利率 = 销售额 - 全部成本
    """
    # 1. 订单汇总 + 按 SKU 统计销量 (Pending 订单用 quantity_ordered)
    cursor.execute(f"""
        SELECT
            oi.seller_sku AS sku,
            SUM(GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1)) AS qty,
            COALESCE(SUM({_PRICE_FALLBACK_SQL}), 0) AS sales
        FROM amazon_orders o
        JOIN amazon_order_items oi
            ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
        WHERE o.shop_id = %s
          AND DATE(o.purchase_date) = %s
          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
        GROUP BY oi.seller_sku
    """, (sid, report_date))
    sku_rows = cursor.fetchall()

    total_sales = Decimal('0')
    order_count = 0
    sku_count = 0
    total_fba_fees = Decimal('0')
    total_commission = Decimal('0')

    for sr in sku_rows:
        sku = sr['sku'] or ''
        qty = int(sr['qty'] or 0)
        sales = Decimal(str(sr['sales'] or 0))
        if not sku or qty <= 0:
            continue

        if sales == 0:
            print(f"[Report] {report_date} SKU {sku} qty={qty}: item_price 和 listing_offers 均无价格数据，销售额为 0")

        total_sales += sales
        sku_count += 1

        # 查 amazon_product_fees 估算费率
        cursor.execute("""
            SELECT fba_fee, commission_rate, real_fba_fee, real_commission_rate
            FROM amazon_product_fees
            WHERE shop_id = %s AND sku = %s LIMIT 1
        """, (sid, sku))
        fee_row = cursor.fetchone()
        if fee_row:
            fba = Decimal(str(fee_row['real_fba_fee'] or fee_row['fba_fee'] or 0))
            rate = Decimal(str(fee_row['real_commission_rate'] or fee_row['commission_rate'] or '0.15'))
        else:
            fba = Decimal('0')
            rate = Decimal('0.15')

        total_fba_fees += fba * qty
        total_commission += (sales * rate).quantize(Decimal('0.01'))

    # 2. 订单数
    if total_sales > 0 or sku_count > 0:
        cursor.execute("""
            SELECT COUNT(DISTINCT amazon_order_id) AS order_count
            FROM amazon_orders
            WHERE shop_id = %s
              AND DATE(purchase_date) = %s
              AND order_status NOT IN ('Canceled', 'PendingAvailability')
        """, (sid, report_date))
        order_count = int(cursor.fetchone()['order_count'] or 0)

    # 3. 内部成本（按 SKU 计算采购成本+头程）
    total_product_cost = Decimal('0')
    total_headway = Decimal('0')
    for sr in sku_rows:
        sku = sr['sku'] or ''
        qty = int(sr['qty'] or 0)
        if not sku or qty <= 0:
            continue
        try:
            unit_costs = get_unit_costs(cursor, sku, exchange_rate, shop_id=sid)
            total_product_cost += unit_costs.purchase_cost_usd * qty
            total_headway += unit_costs.headway_cost_usd * qty
        except Exception:
            continue

    # 4. 广告费
    cursor.execute("""
        SELECT COALESCE(SUM(cost), 0) AS ad_sum
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date = %s
          AND report_type = 'spCampaigns'
    """, (sid, report_date))
    ad_cost = Decimal(str(cursor.fetchone()['ad_sum'] or 0))

    # 5. 汇总
    total_cost = total_product_cost + total_headway + total_fba_fees + total_commission + ad_cost
    gross_profit = total_sales - total_cost
    gross_profit_rate = (gross_profit / total_sales) if total_sales > 0 else Decimal('0')
    headway_ratio = (total_headway / total_sales) if total_sales > 0 else Decimal('0')

    return {
        'total_sales': total_sales,
        'order_count': order_count,
        'sku_count': sku_count,
        'total_product_cost': total_product_cost,
        'total_headway': total_headway,
        'total_fba_fees': total_fba_fees,
        'total_commission': total_commission,
        'ad_cost': ad_cost,
        'refund_amount': Decimal('0'),
        'refund_rate': Decimal('0'),
        'total_cost': total_cost,
        'gross_profit': gross_profit,
        'gross_profit_rate': gross_profit_rate,
        'headway_ratio': headway_ratio,
        'data_status': 'estimated',
    }


def _generate_settled_daily(cursor, sid, report_date, exchange_rate):
    """
    生成 settled（完整已结算）日报数据，支持部分结算(partial)状态

    简介: 按下单日期 (DATE(o.purchase_date)) 聚合已结算 Shipment 订单的费用和利润。
          当日有订单但部分未结算时返回 data_status='partial'，包含未结算订单的预估销售额。

    详细:
      - order_count = 当天全部有效订单数（含未结算），而非仅已结算订单数
      - total_sales: 已结算部分用 product_charges，未结算部分用 item_price_amount 估算（缺失时取 listing_offers.our_price 兜底）
      - fba_fees / commission: 从 items_json breakdowns 解析的真实费用（仅已结算部分）
      - 内部成本: 从 profit_calculator 按 SKU 逐项计算（含已结算+未结算 SKU）
      - 退款: 从 finances 按订单的 purchase_date 汇总 Refund 交易
      - 广告费: 从 amazon_ads_raw_reports 汇总
      - 无已结算订单时返回 None 让调用方回退到 estimated
      - data_status: 'settled' 全部已结算 / 'partial' 部分已结算
    """
    # 0. 获取当天全部有效订单数
    cursor.execute("""
        SELECT COUNT(DISTINCT amazon_order_id) AS total_order_count
        FROM amazon_orders
        WHERE shop_id = %s
          AND DATE(purchase_date) = %s
          AND order_status NOT IN ('Canceled', 'PendingAvailability')
    """, (sid, report_date))
    total_order_count = int(cursor.fetchone()['total_order_count'] or 0)
    if total_order_count == 0:
        return None

    # 1. 按 purchase_date 汇总 Shipment 收入 + 费用（仅已结算部分）
    #    使用子查询去重：同一订单在 finances 中可能同时存在
    #    DEFERRED / DEFERRED_RELEASED / RELEASED 多条记录，
    #    按 RELEASED > DEFERRED_RELEASED > DEFERRED 优先级取唯一一条
    cursor.execute("""
        SELECT
            COUNT(DISTINCT o.amazon_order_id) AS settled_order_count,
            COALESCE(SUM(dedup.product_charges), 0) AS total_product_charges,
            COALESCE(SUM(dedup.total_amount), 0) AS total_net_sales,
            COALESCE(SUM(dedup.fba_fees), 0) AS total_fba_fees,
            COALESCE(SUM(dedup.commission), 0) AS total_commission
        FROM amazon_orders o
        JOIN (
            SELECT amazon_order_id, shop_id,
                   COALESCE(
                       MAX(CASE WHEN transaction_status = 'RELEASED' THEN product_charges END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED_RELEASED' THEN product_charges END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED' THEN product_charges END)
                   ) AS product_charges,
                   COALESCE(
                       MAX(CASE WHEN transaction_status = 'RELEASED' THEN total_amount END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED_RELEASED' THEN total_amount END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED' THEN total_amount END)
                   ) AS total_amount,
                   COALESCE(
                       MAX(CASE WHEN transaction_status = 'RELEASED' THEN fba_fees END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED_RELEASED' THEN fba_fees END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED' THEN fba_fees END)
                   ) AS fba_fees,
                   COALESCE(
                       MAX(CASE WHEN transaction_status = 'RELEASED' THEN commission END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED_RELEASED' THEN commission END),
                       MAX(CASE WHEN transaction_status = 'DEFERRED' THEN commission END)
                   ) AS commission
            FROM amazon_order_finances
            WHERE transaction_type = 'Shipment'
            GROUP BY amazon_order_id, shop_id
        ) dedup ON o.amazon_order_id = dedup.amazon_order_id COLLATE utf8mb4_unicode_ci
                AND o.shop_id = dedup.shop_id
        WHERE o.shop_id = %s
          AND DATE(o.purchase_date) = %s
          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
    """, (sid, report_date))
    ship_row = cursor.fetchone()
    settled_order_count = int(ship_row['settled_order_count'] or 0)
    if settled_order_count == 0:
        return None

    data_status = 'partial' if settled_order_count < total_order_count else 'settled'
    order_count = total_order_count

    print(f"[Report] {report_date} shop={sid}: total_orders={total_order_count}, settled_orders={settled_order_count}, status={data_status}")

    if float(ship_row['total_product_charges'] or 0) > 0:
        total_sales = Decimal(str(ship_row['total_product_charges'] or 0))
        total_fba_fees = Decimal(str(ship_row['total_fba_fees'] or 0))
        total_commission = Decimal(str(ship_row['total_commission'] or 0))
    else:
        total_sales = Decimal(str(ship_row['total_net_sales'] or 0))
        total_fba_fees = Decimal('0')
        total_commission = Decimal('0')

    # 2. 获取已结算订单的 SKU 销量（去重 + items_json 解析）
    finance_rows = _get_settled_shipment_data(cursor, sid, report_date)
    sku_qty, _, _ = _parse_finances_items(finance_rows)

    # 3. 补充未结算订单的 SKU 销量 + 预估销售额
    if data_status == 'partial':
        cursor.execute(f"""
            SELECT
                oi.seller_sku AS sku,
                SUM(GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1)) AS qty,
                COALESCE(SUM({_PRICE_FALLBACK_SQL}), 0) AS sales
            FROM amazon_orders o
            JOIN amazon_order_items oi
                ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
            WHERE o.shop_id = %s
              AND DATE(o.purchase_date) = %s
              AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
              AND NOT EXISTS (
                SELECT 1 FROM amazon_order_finances f2
                WHERE f2.shop_id = %s AND f2.transaction_type = 'Shipment'
                  AND f2.amazon_order_id = o.amazon_order_id COLLATE utf8mb4_unicode_ci
              )
            GROUP BY oi.seller_sku
        """, (sid, report_date, sid))
        for nsr in cursor.fetchall():
            sku = nsr['sku'] or ''
            qty = int(nsr['qty'] or 0)
            if not sku or qty <= 0:
                continue
            sku_qty[sku] = sku_qty.get(sku, 0) + qty

            unsettled_sales = Decimal(str(nsr['sales'] or 0))
            if unsettled_sales == 0:
                print(f"[Report] {report_date} SKU {sku} qty={qty}: (partial) item_price 和 listing_offers 均无价格数据，销售额为 0")
            total_sales += unsettled_sales

            cursor.execute("""
                SELECT fba_fee, commission_rate, real_fba_fee, real_commission_rate
                FROM amazon_product_fees
                WHERE shop_id = %s AND sku = %s LIMIT 1
            """, (sid, sku))
            fee_row = cursor.fetchone()
            if fee_row:
                fba = Decimal(str(fee_row['real_fba_fee'] or fee_row['fba_fee'] or 0))
                rate = Decimal(str(fee_row['real_commission_rate'] or fee_row['commission_rate'] or '0.15'))
            else:
                fba = Decimal('0')
                rate = Decimal('0.15')
            total_fba_fees += fba * qty
            total_commission += (unsettled_sales * rate).quantize(Decimal('0.01'))

    sku_count = len(sku_qty)

    # 4. 按 SKU 计算内部成本（含已结算+未结算 SKU）
    total_product_cost = Decimal('0')
    total_headway = Decimal('0')
    for sku, qty in sku_qty.items():
        try:
            unit_costs = get_unit_costs(cursor, sku, exchange_rate, shop_id=sid)
            total_product_cost += unit_costs.purchase_cost_usd * qty
            total_headway += unit_costs.headway_cost_usd * qty
        except Exception as e:
            print(f"[Report] SKU {sku} 成本计算异常: {e}")
            continue

    # 5. 退款（公共函数）
    refund_amount = _get_daily_refund(cursor, sid, report_date)

    # 6. 广告费（从 amazon_ads_raw_reports 汇总）
    cursor.execute("""
        SELECT COALESCE(SUM(cost), 0) AS ad_sum
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date = %s
          AND report_type = 'spCampaigns'
    """, (sid, report_date))
    ad_row = cursor.fetchone()
    ad_cost = Decimal(str(ad_row['ad_sum'] or 0))

    # 7. 汇总计算
    total_cost = total_product_cost + total_headway + total_fba_fees + total_commission + refund_amount + ad_cost
    gross_profit = total_sales - total_cost
    gross_profit_rate = (gross_profit / total_sales) if total_sales > 0 else Decimal('0')
    headway_ratio = (total_headway / total_sales) if total_sales > 0 else Decimal('0')
    refund_rate = (refund_amount / total_sales) if total_sales > 0 else Decimal('0')

    return {
        'total_sales': total_sales,
        'order_count': order_count,
        'sku_count': sku_count,
        'total_product_cost': total_product_cost,
        'total_headway': total_headway,
        'total_fba_fees': total_fba_fees,
        'total_commission': total_commission,
        'ad_cost': ad_cost,
        'refund_amount': refund_amount,
        'refund_rate': refund_rate,
        'total_cost': total_cost,
        'gross_profit': gross_profit,
        'gross_profit_rate': gross_profit_rate,
        'headway_ratio': headway_ratio,
        'data_status': data_status,
    }


def _upsert_daily_report(cursor, sid, report_date, d):
    """
    写入/更新日报行

    简介: 将预处理好的日报数据 INSERT 到 report_business，已存在则 UPDATE。
    参数 d 由 _generate_estimated_daily 或 _generate_settled_daily 返回。
    """
    cursor.execute("""
        INSERT INTO report_business (
            shop_id, report_type, report_date, report_week, report_month, data_status,
            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
            headway_cost, headway_ratio, order_count, sku_count,
            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
        ) VALUES (%s, 'daily', %s, '', '', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            data_status = VALUES(data_status),
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
        sid, report_date, d['data_status'],
        float(d['total_sales']), float(d['total_cost']), float(d['total_product_cost']),
        float(d['gross_profit']), float(d['gross_profit_rate']),
        float(d['total_headway']), float(d['headway_ratio']),
        d['order_count'], d['sku_count'],
        float(d['ad_cost']), float(d['refund_amount']), float(d['refund_rate']),
        float(d['total_commission']), float(d['total_fba_fees']),
    ))


def generate_business_daily(report_date, shop_id=None):
    """
    生成单日经营日报

    简介: 根据 finance 数据可用性自动选择 estimated 或 settled 模式。

    详细:
      - 有 finance 数据: settled 模式，拉取真实流水 + 内部成本计算完整利润
      - 无 finance 数据: estimated 模式，仅统计销售额/订单数/SKU数，费用字段为 0
      - 幂等: ON DUPLICATE KEY UPDATE，重复调用安全
      - settled 模式会覆盖同日的 estimated 数据

    参数:
        report_date: 'YYYY-MM-DD'
        shop_id: None=所有店铺, int=指定店铺
    """
    report_type = 'business_daily'
    period = report_date

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

                    settled_data = _generate_settled_daily(cursor, sid, report_date, exchange_rate)

                    if settled_data is not None:
                        d = settled_data
                        print(f"[Report] {report_date} shop={sid} {d['data_status']}: "
                              f"sales={d['total_sales']}, orders={d['order_count']}, profit={d['gross_profit']}")
                    else:
                        d = _generate_estimated_daily(cursor, sid, report_date, exchange_rate)
                        if d['total_sales'] == 0 and d['order_count'] == 0:
                            # 真无数据: 删除残留旧记录
                            cursor.execute(
                                "DELETE FROM report_business WHERE shop_id=%s AND report_type='daily' AND report_date=%s",
                                (sid, report_date))
                            if cursor.rowcount:
                                print(f"[Report] {report_date} shop={sid}: 无销售数据，清除旧记录 ({cursor.rowcount} 行)")
                            else:
                                print(f"[Report] {report_date} shop={sid}: 无销售数据，跳过")
                            continue
                        print(f"[Report] {report_date} shop={sid} estimated: sales={d['total_sales']}, "
                              f"orders={d['order_count']} (finance 未到达)")

                    _upsert_daily_report(cursor, sid, report_date, d)
                    total_affected += cursor.rowcount

                conn.commit()
                if log_id:
                    _log_generation_end(log_id, 'success', cursor.rowcount)
            except Exception as e:
                conn.rollback()
                if log_id:
                    _log_generation_end(log_id, 'failed', 0, str(e)[:500])
                raise

        # 日报完成后同步生成 SKU 利润
        try:
            generate_sku_profit(report_date, shop_id)
        except Exception as e:
            print(f"[Report] SKU利润 {report_date} 失败: {e}")

        return {"status": "success", "affected_rows": total_affected, "shops_processed": len(shops)}
    finally:
        conn.close()


# ==================== 2. SKU 利润表生成 ====================

def generate_sku_profit(report_date, shop_id=None):
    """
    按 ASIN/SKU 生成单日利润数据

    简介: 从 amazon_orders + profit_calculator 计算每个 SKU 的单日营收/成本/利润。

    详细:
      - 逐 SKU 计算：销售额、销量、采购成本、FBA费、佣金、头程、广告费、退款
      - 统一成本入口: profit_calculator.get_unit_costs + calculate_profit
      - 幂等: ON DUPLICATE KEY UPDATE

    参数:
        report_date: 'YYYY-MM-DD'
        shop_id:     None=所有店铺
    """
    report_type = 'sku_profit'
    period = report_date

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

                    # 0. 获取已结算订单数据（去重 + 按 SKU 解析 pc/fba/comm/qty）
                    finance_rows = _get_settled_shipment_data(cursor, sid, report_date)
                    sku_settled_qty, sku_sales_from_finance, sku_fees = _parse_finances_items(finance_rows)

                    # 1. 获取该店铺所有活跃产品（SKU 全集）
                    cursor.execute("SELECT asin, seller_sku AS sku, product_name FROM products WHERE status=1 AND asin IS NOT NULL AND asin != ''")
                    all_products = cursor.fetchall()

                    # 2. 预聚合销售数据（按 ASIN，价格缺失时用 listing_offers 兜底）
                    #    分两步：1) 总销量 qty（全部订单）2) 未结算订单的预估销售额
                    cursor.execute(f"""
                        SELECT oi.asin,
                               COALESCE(SUM(oi.quantity_shipped), 0) AS sales_qty
                        FROM amazon_orders o
                        JOIN amazon_order_items oi ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.asin
                    """, (sid, report_date))
                    sales_map = {}
                    for r in cursor.fetchall():
                        sales_map[r['asin']] = r

                    # 2b. 未结算订单的预估销售额 + 销量（公共函数）
                    unsettled_sales_map = _get_unsettled_sales(cursor, sid, report_date)

                    # 3. 预聚合广告数据（按 advertised_asin，含花费/归因销售额/归因订单数）
                    cursor.execute("""
                        SELECT advertised_asin AS asin,
                               COALESCE(SUM(cost), 0) AS ad_cost,
                               COALESCE(SUM(sales_7d), 0) AS ad_sales,
                               COALESCE(SUM(purchases_7d), 0) AS ad_orders
                        FROM amazon_ads_raw_reports
                        WHERE shop_id = %s AND report_date = %s AND report_type = 'spAdvertisedProduct'
                        GROUP BY advertised_asin
                    """, (sid, report_date))
                    ad_map = {}
                    for r in cursor.fetchall():
                        ad_map[r['asin']] = {
                            'ad_cost': float(r['ad_cost'] or 0),
                            'ad_sales': float(r['ad_sales'] or 0),
                            'ad_orders': int(r['ad_orders'] or 0),
                        }

                    # 3b. 日退款总额（公共函数）
                    refund_total = _get_daily_refund(cursor, sid, report_date)

                    # 3c. 日费用总计（与 report_business 完全同源），用于按销售额比例分配到 SKU
                    daily_total_sales = Decimal(str(sum(sku_sales_from_finance.values())))
                    daily_total_sales += Decimal(str(sum(u['sales'] for u in unsettled_sales_map.values())))

                    # 4. 遍历所有产品，写入 sku_profit（每天每个 SKU 一条）
                    for prod in all_products:
                        asin = prod['asin'] or ''
                        sku = prod['sku'] or ''
                        product_name = prod.get('product_name') or ''
                        if not asin or not sku:
                            continue

                        # 销售数据 — Finances product_charges（已结算）+ PRICE_FALLBACK（未结算）
                        sales = sales_map.get(asin, {})
                        sales_qty = int(sales.get('sales_qty', 0) or 0)
                        unsettled = unsettled_sales_map.get(asin, {'qty': 0, 'sales': 0.0})
                        unsettled_qty = unsettled['qty']
                        unsettled_sales_amount = Decimal(str(unsettled['sales']))

                        fin_sales = sku_sales_from_finance.get(sku, 0)
                        sales_amount = Decimal(str(fin_sales)) + unsettled_sales_amount

                        # 总销量用 SKU 维度（与 fba_fees/platform_fees/sales_amount 口径一致）
                        total_qty = sku_settled_qty.get(sku, 0) + unsettled_qty

                        avg_price = Decimal('0')
                        if total_qty > 0 and sales_amount > 0:
                            avg_price = sales_amount / total_qty

                        # 广告数据
                        ad_data = ad_map.get(asin, {})
                        ad_cost = Decimal(str(ad_data.get('ad_cost', 0)))
                        ad_sales = Decimal(str(ad_data.get('ad_sales', 0)))
                        ad_orders = int(ad_data.get('ad_orders', 0))
                        ad_acos = (ad_cost / ad_sales) if ad_sales > 0 else Decimal('0')

                        # 成本
                        unit_costs = get_unit_costs(cursor, sku, exchange_rate, shop_id=sid)
                        if not product_name:
                            product_name = unit_costs.product_name or ''

                        # FBA + 佣金 = Finances 已结算真实费用 + amazon_product_fees 未结算估算
                        #   与 report_business 口径完全一致
                        fin_fba = Decimal(str(sku_fees.get(sku, {}).get('fba', 0)))
                        fin_comm = Decimal(str(sku_fees.get(sku, {}).get('commission', 0)))
                        if unsettled_qty > 0:
                            cursor.execute("""
                                SELECT fba_fee, commission_rate, real_fba_fee, real_commission_rate
                                FROM amazon_product_fees
                                WHERE shop_id = %s AND sku = %s LIMIT 1
                            """, (sid, sku))
                            fee_row = cursor.fetchone()
                            if fee_row:
                                u_fba = Decimal(str(fee_row['real_fba_fee'] or fee_row['fba_fee'] or 0))
                                u_rate = Decimal(str(fee_row['real_commission_rate'] or fee_row['commission_rate'] or '0.15'))
                            else:
                                u_fba = Decimal('0')
                                u_rate = Decimal('0.15')
                            fba_fees = fin_fba + u_fba * unsettled_qty
                            platform_fees = fin_comm + (unsettled_sales_amount * u_rate).quantize(Decimal('0.01'))
                        elif fin_fba > 0 or fin_comm > 0:
                            fba_fees = fin_fba
                            platform_fees = fin_comm
                        elif total_qty > 0:
                            profit_all = calculate_profit(sales_amount, total_qty, unit_costs, Decimal('0'), Decimal('0'))
                            fba_fees = profit_all.fba_fees
                            platform_fees = profit_all.commission
                        else:
                            fba_fees = Decimal('0')
                            platform_fees = Decimal('0')

                        # 退款 — 每日总额按销售额比例分配（与 report_business 口径一致）
                        if daily_total_sales > 0 and refund_total > 0:
                            refund_amount = (refund_total * sales_amount / daily_total_sales).quantize(Decimal('0.01'))
                        else:
                            refund_amount = Decimal('0')

                        # 产品成本 + 头程 — 使用精确单件成本 × 销量（SKU维度）
                        product_cost = (unit_costs.purchase_cost_usd * total_qty).quantize(Decimal('0.01'))
                        headway_cost = (unit_costs.headway_cost_usd * total_qty).quantize(Decimal('0.01'))
                        total_cost = product_cost + headway_cost + fba_fees + platform_fees + ad_cost + refund_amount
                        gross_profit = sales_amount - total_cost
                        net_profit = gross_profit
                        profit_margin = (net_profit / sales_amount) if sales_amount > 0 else Decimal('0')
                        other_fees = Decimal('0')

                        cursor.execute("""
                            INSERT INTO sku_profit (
                                shop_id, asin, sku, product_name, report_date,
                                sales_qty, sales_amount, avg_selling_price,
                                product_cost, fba_fees, ad_cost, headway_cost, platform_fees,
                                refund_amount, other_fees,
                                gross_profit, net_profit, profit_margin,
                                ad_sales, ad_orders, ad_acos
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                                ad_sales = VALUES(ad_sales),
                                ad_orders = VALUES(ad_orders),
                                ad_acos = VALUES(ad_acos),
                                updated_at = NOW()
                        """, (
                            sid, asin, sku, product_name, report_date,
                            total_qty, float(sales_amount), float(avg_price),
                            float(product_cost), float(fba_fees), float(ad_cost),
                            float(headway_cost), float(platform_fees),
                            float(refund_amount), float(other_fees),
                            float(gross_profit), float(net_profit), float(profit_margin),
                            float(ad_sales), ad_orders, float(ad_acos)
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


# ==================== 2.5 SKU 销售数据汇总 ====================


def generate_sku_sales(report_date, shop_id=None, sku_filter=None):
    """
    生成 SKU 销售数据汇总报表 (v2)

    简介: 从各数据源汇总每个 SKU 的库存、多窗口销量（总/广告/自然）、
          广告销售额/自然销售额、多窗口广告花费/CPC/CVR/ACOS/TACOS、
          售价/促销价、多窗口利润及利润率。

    参数:
        report_date: 'YYYY-MM-DD'  报告生成日期（PDT 时间，通常是 PDT 昨天）
        shop_id:     None=所有店铺, int=指定店铺
        sku_filter:  None=所有SKU, 'XXX'=仅生成指定SKU
    """
    report_type = 'sku_sales'
    period = report_date

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

                    # 数据窗口终点: report_business 最新已生成日期（确保数据完整）
                    cursor.execute(
                        "SELECT MAX(report_date) AS latest FROM report_business WHERE report_type='daily' AND shop_id=%s AND total_sales > 0",
                        (sid,))
                    row = cursor.fetchone()
                    data_end = row['latest'] if row and row['latest'] else datetime.strptime(report_date, '%Y-%m-%d').date()

                    windows = {
                        '1d':  (data_end, data_end),
                        '3d':  (data_end - timedelta(days=2), data_end),
                        '7d':  (data_end - timedelta(days=6), data_end),
                        '14d': (data_end - timedelta(days=13), data_end),
                        '30d': (data_end - timedelta(days=29), data_end),
                    }
                    window_keys = ['1d', '3d', '7d', '14d', '30d']

                    # ---- 1. 活跃 SKU 列表 ----
                    if sku_filter:
                        cursor.execute(
                            "SELECT asin, seller_sku AS sku, product_name FROM products WHERE status=1 AND seller_sku = %s", (sku_filter,))
                    else:
                        cursor.execute(
                            "SELECT asin, seller_sku AS sku, product_name FROM products WHERE status=1 AND asin IS NOT NULL AND asin != ''")
                    products = cursor.fetchall()

                    # ---- 2. 库存 ----
                    cursor.execute("SELECT seller_sku, fulfillable_quantity FROM amazon_inventory WHERE shop_id = %s", (sid,))
                    inventory_map = {r['seller_sku']: int(r['fulfillable_quantity'] or 0) for r in cursor.fetchall()}

                    # ---- 3. 售价 & 促销价 ----
                    cursor.execute("""
                        SELECT t.sku, t.our_price, t.discounted_price
                        FROM amazon_listing_offers t
                        JOIN (
                            SELECT sku, MAX(updated_at) AS max_ts FROM amazon_listing_offers
                            WHERE shop_id = %s GROUP BY sku
                        ) latest ON t.sku = latest.sku AND t.updated_at = latest.max_ts
                        WHERE t.shop_id = %s
                    """, (sid, sid))
                    pricing_map = {}
                    for r in cursor.fetchall():
                        pricing_map[r['sku']] = {
                            'sell_price': Decimal(str(r['our_price'] or 0)),
                            'promo_price': Decimal(str(r['discounted_price'] or 0)),
                        }

                    # ---- 4. 总销量 qty (5窗口) ----
                    cursor.execute("""
                        SELECT oi.seller_sku AS sku,
                               SUM(CASE WHEN DATE(o.purchase_date) = %s THEN GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1) ELSE 0 END) AS s1,
                               SUM(CASE WHEN DATE(o.purchase_date) BETWEEN %s AND %s THEN GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1) ELSE 0 END) AS s3,
                               SUM(CASE WHEN DATE(o.purchase_date) BETWEEN %s AND %s THEN GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1) ELSE 0 END) AS s7,
                               SUM(CASE WHEN DATE(o.purchase_date) BETWEEN %s AND %s THEN GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1) ELSE 0 END) AS s14,
                               SUM(CASE WHEN DATE(o.purchase_date) BETWEEN %s AND %s THEN GREATEST(oi.quantity_shipped, oi.quantity_ordered, 1) ELSE 0 END) AS s30
                        FROM amazon_orders o
                        JOIN amazon_order_items oi ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s
                          AND DATE(o.purchase_date) BETWEEN %s AND %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.seller_sku
                    """, (
                        str(data_end),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        sid, str(windows['30d'][0]), str(windows['30d'][1]),
                    ))
                    sales_map = {}
                    for r in cursor.fetchall():
                        sales_map[r['sku']] = {
                            's1': int(r['s1'] or 0), 's3': int(r['s3'] or 0),
                            's7': int(r['s7'] or 0), 's14': int(r['s14'] or 0),
                            's30': int(r['s30'] or 0),
                        }

                    # ---- 5. 广告数据 (5窗口: cost, purchases_7d, sales_7d, clicks) ----
                    cursor.execute("""
                        SELECT advertised_sku AS sku,
                               SUM(CASE WHEN report_date = %s THEN cost ELSE 0 END) AS c1,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN cost ELSE 0 END) AS c3,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN cost ELSE 0 END) AS c7,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN cost ELSE 0 END) AS c14,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN cost ELSE 0 END) AS c30,
                               SUM(CASE WHEN report_date = %s THEN purchases_7d ELSE 0 END) AS pur1,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN purchases_7d ELSE 0 END) AS pur3,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN purchases_7d ELSE 0 END) AS pur7,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN purchases_7d ELSE 0 END) AS pur14,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN purchases_7d ELSE 0 END) AS pur30,
                               SUM(CASE WHEN report_date = %s THEN sales_7d ELSE 0 END) AS rev1,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_7d ELSE 0 END) AS rev3,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_7d ELSE 0 END) AS rev7,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_7d ELSE 0 END) AS rev14,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_7d ELSE 0 END) AS rev30,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN clicks ELSE 0 END) AS clk7
                        FROM amazon_ads_raw_reports
                        WHERE shop_id = %s AND report_type = 'spAdvertisedProduct'
                          AND report_date BETWEEN %s AND %s
                        GROUP BY advertised_sku
                    """, (
                        str(data_end),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        str(data_end),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        str(data_end),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        sid, str(windows['30d'][0]), str(data_end),
                    ))
                    ad_map = {}
                    for r in cursor.fetchall():
                        clk7 = int(r['clk7'] or 0)
                        ad_map[r['sku']] = {
                            'c1': r['c1'], 'c3': r['c3'], 'c7': r['c7'], 'c14': r['c14'], 'c30': r['c30'],
                            'pur1': int(r['pur1'] or 0), 'pur3': int(r['pur3'] or 0),
                            'pur7': int(r['pur7'] or 0), 'pur14': int(r['pur14'] or 0),
                            'pur30': int(r['pur30'] or 0),
                            'rev1': r['rev1'], 'rev3': r['rev3'], 'rev7': r['rev7'],
                            'rev14': r['rev14'], 'rev30': r['rev30'],
                            'clk7': clk7,
                        }

                    # ---- 6. 利润 + 总销售额 (从 sku_profit, 5窗口) ----
                    cursor.execute("""
                        SELECT sku,
                               SUM(CASE WHEN report_date = %s THEN gross_profit ELSE 0 END) AS p1,
                               SUM(CASE WHEN report_date = %s THEN sales_amount ELSE 0 END) AS a1,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN gross_profit ELSE 0 END) AS p3,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_amount ELSE 0 END) AS a3,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN gross_profit ELSE 0 END) AS p7,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_amount ELSE 0 END) AS a7,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN gross_profit ELSE 0 END) AS p14,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_amount ELSE 0 END) AS a14,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN gross_profit ELSE 0 END) AS p30,
                               SUM(CASE WHEN report_date BETWEEN %s AND %s THEN sales_amount ELSE 0 END) AS a30
                        FROM sku_profit
                        WHERE shop_id = %s AND report_date BETWEEN %s AND %s
                        GROUP BY sku
                    """, (
                        str(data_end), str(data_end),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['3d'][0]), str(windows['3d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['7d'][0]), str(windows['7d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['14d'][0]), str(windows['14d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        str(windows['30d'][0]), str(windows['30d'][1]),
                        sid, str(windows['30d'][0]), str(data_end),
                    ))
                    profit_map = {}
                    for r in cursor.fetchall():
                        profit_map[r['sku']] = {k: r[k] for k in ['p1','a1','p3','a3','p7','a7','p14','a14','p30','a30']}

                    # ---- 7. 逐 SKU 写入 ----
                    cols = (
                        "shop_id, sku, asin, product_name, report_date, stock, "
                        "total_revenue_1d, total_revenue_3d, total_revenue_7d, total_revenue_14d, total_revenue_30d, "
                        "sales_1d, sales_3d, sales_7d, sales_14d, sales_30d, "
                        "sales_ad_1d, sales_ad_3d, sales_ad_7d, sales_ad_14d, sales_ad_30d, "
                        "sales_natural_1d, sales_natural_3d, sales_natural_7d, sales_natural_14d, sales_natural_30d, "
                        "ad_revenue_1d, ad_revenue_3d, ad_revenue_7d, ad_revenue_14d, ad_revenue_30d, "
                        "natural_revenue_1d, natural_revenue_3d, natural_revenue_7d, natural_revenue_14d, natural_revenue_30d, "
                        "ad_cost_1d, ad_cost_3d, ad_cost_7d, ad_cost_14d, ad_cost_30d, "
                        "cpc, cvr, acos, tacos, "
                        "sell_price, promo_price, "
                        "profit_1d, profit_rate_1d, profit_3d, profit_rate_3d, "
                        "profit_7d, profit_rate_7d, profit_14d, profit_rate_14d, "
                        "profit_30d, profit_rate_30d"
                    )
                    update_clause = ", ".join(
                        f"{c.split()[0]} = VALUES({c.split()[0]})" for c in cols.split(", ") if c.strip() not in (
                            "shop_id", "sku", "asin", "product_name", "report_date", "created_at")
                    )

                    base = Decimal('0')
                    for prod in products:
                        sku = prod['sku'] or ''
                        asin = prod['asin'] or ''
                        pname = prod.get('product_name') or ''
                        if not sku:
                            continue

                        s = sales_map.get(sku, {'s1':0,'s3':0,'s7':0,'s14':0,'s30':0})
                        a = ad_map.get(sku, {'c1':base,'c3':base,'c7':base,'c14':base,'c30':base,
                                              'pur1':0,'pur3':0,'pur7':0,'pur14':0,'pur30':0,
                                              'rev1':base,'rev3':base,'rev7':base,'rev14':base,'rev30':base,
                                              'clk7':0})
                        p = profit_map.get(sku, {'p1':base,'a1':base,'p3':base,'a3':base,'p7':base,'a7':base,
                                                  'p14':base,'a14':base,'p30':base,'a30':base})
                        pr = pricing_map.get(sku, {'sell_price': base, 'promo_price': base})

                        # 计算值
                        d = lambda v: Decimal(str(v or 0))
                        clk7 = int(a['clk7'] or 0)
                        cpc   = (d(a['c7']) / clk7).quantize(Decimal('0.0001')) if clk7 > 0 else base
                        cvr   = (d(a['pur7']) / clk7).quantize(Decimal('0.0001')) if clk7 > 0 else base
                        acos  = (d(a['c7']) / d(a['rev7'])).quantize(Decimal('0.0001')) if d(a['rev7']) > 0 else base
                        tacos = (d(a['c7']) / d(p['a7'])).quantize(Decimal('0.0001')) if d(p['a7']) > 0 else base

                        def adv(w, k): return d(a[k + w])
                        def pval(w, k): return d(p[k + w])

                        vals = (
                            sid, sku, asin, pname, str(data_end), inventory_map.get(sku, 0),
                            float(d(p['a1'])), float(d(p['a3'])), float(d(p['a7'])), float(d(p['a14'])), float(d(p['a30'])),
                            s['s1'], s['s3'], s['s7'], s['s14'], s['s30'],
                            int(a['pur1'] or 0), int(a['pur3'] or 0), int(a['pur7'] or 0), int(a['pur14'] or 0), int(a['pur30'] or 0),
                            s['s1'] - int(a['pur1'] or 0), s['s3'] - int(a['pur3'] or 0),
                            s['s7'] - int(a['pur7'] or 0), s['s14'] - int(a['pur14'] or 0), s['s30'] - int(a['pur30'] or 0),
                            float(d(a['rev1'])), float(d(a['rev3'])), float(d(a['rev7'])), float(d(a['rev14'])), float(d(a['rev30'])),
                            float(d(p['a1']) - d(a['rev1'])), float(d(p['a3']) - d(a['rev3'])),
                            float(d(p['a7']) - d(a['rev7'])), float(d(p['a14']) - d(a['rev14'])), float(d(p['a30']) - d(a['rev30'])),
                            float(d(a['c1'])), float(d(a['c3'])), float(d(a['c7'])), float(d(a['c14'])), float(d(a['c30'])),
                            float(cpc), float(cvr), float(acos), float(tacos),
                            float(d(pr['sell_price'])), float(d(pr['promo_price'])),
                            float(pval('1','p')), float(pval('1','p') / d(p['a1']) if d(p['a1']) > 0 else base),
                            float(pval('3','p')), float(pval('3','p') / d(p['a3']) if d(p['a3']) > 0 else base),
                            float(pval('7','p')), float(pval('7','p') / d(p['a7']) if d(p['a7']) > 0 else base),
                            float(pval('14','p')), float(pval('14','p') / d(p['a14']) if d(p['a14']) > 0 else base),
                            float(pval('30','p')), float(pval('30','p') / d(p['a30']) if d(p['a30']) > 0 else base),
                        )

                        # 构建 SQL (55个值)
                        placeholders = ', '.join(['%s'] * len(vals))
                        cursor.execute(
                            f"INSERT INTO report_sku_sales ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_clause}, updated_at = NOW()",
                            vals
                        )
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
    生成库存周转数据

    简介: 基于当前 FBA 库存 + 近 7 天销售速度，计算每个 SKU 的周转天数、状态和建议补货量。
    周转天数不足45天时通过企业微信发送预警通知。

    详细:
      - 数据源: amazon_inventory（库存）+ amazon_orders（销售速度）
      - 周转天数 = 当前库存 / (近7天销量 / 7)
      - 状态: normal / slow 滞销 / out_of_stock 缺货 / warning 预警
      - 幂等: ON DUPLICATE KEY UPDATE
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

                    # 1. 获取当前库存（product_name 优先取 products 表）
                    cursor.execute("""
                        SELECT
                            ai.seller_sku AS sku,
                            ai.asin,
                            COALESCE(p.product_name, ai.product_name, '') AS product_name,
                            ai.fulfillable_quantity AS current_stock,
                            ai.inbound_working_quantity,
                            ai.inbound_shipped_quantity,
                            ai.inbound_receiving_quantity
                        FROM amazon_inventory ai
                        LEFT JOIN products p ON p.seller_sku = ai.seller_sku AND p.status = 1
                        WHERE ai.shop_id = %s
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

                    alert_warnings = []

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
                        avg_daily_sales = Decimal(str(sales_7d)) / Decimal('7') if sales_7d > 0 else Decimal('0')

                        last_sale_date = sales_map.get(sku, {}).get('last_sale_date')
                        if last_sale_date:
                            if isinstance(last_sale_date, str):
                                last_sale_date = datetime.strptime(last_sale_date, '%Y-%m-%d').date()
                            days_without_sale = (datetime.now().date() - last_sale_date).days
                        else:
                            days_without_sale = 999

                        if avg_daily_sales > 0:
                            turnover_days = int((Decimal(str(current_stock)) / avg_daily_sales).to_integral_value(rounding=ROUND_HALF_UP))
                        else:
                            turnover_days = 9999

                        if turnover_days < 45 and current_stock > 0:
                            alert_warnings.append({
                                'sku': sku,
                                'product_name': product_name or sku,
                                'turnover_days': turnover_days,
                                'sales_7d': sales_7d,
                                'current_stock': current_stock
                            })

                        # 状态判断
                        if current_stock == 0 and days_without_sale >= 30:
                            stock_status = 'out_of_stock'
                        elif turnover_days > 90 or days_without_sale >= 30:
                            stock_status = 'slow'
                        elif turnover_days < 40 and current_stock > 0:
                            stock_status = 'warning'
                        else:
                            stock_status = 'normal'

                        # 建议补货 = max(0, 日均销量 * 60 - 总可用)
                        suggested = max(0, int((avg_daily_sales * Decimal('60') - Decimal(str(total_available))).to_integral_value(rounding=ROUND_HALF_UP)))

                        # 单位成本（统一入口，2026-05-26 重构）
                        unit_costs = get_unit_costs(cursor, sku, exchange_rate, shop_id=sid)
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

                if alert_warnings:
                    fire('inventory_turnover_warning',
                         shop_id=sid,
                         period=period,
                         warnings=alert_warnings)

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


# ==================== 4. 广告效果报表生成（新数据源: amazon_ads_raw_reports）====================


def _generate_advertising_from_raw(cursor, shop_id, report_type, report_date,
                                   report_week, report_month, date_start, date_end):
    """
    从 amazon_ads_raw_reports 聚合生成广告效果报表

    简介: 从 Ads API 全量宽表聚合 overall / campaign / ad_group / asin 四个维度。
          相比旧版 _generate_advertising_from_ads（依赖 amazon_ad_spend）：
            - 数据源更全：收录 spCampaigns/spAdGroups/spAdvertisedProduct/spTargeting/spSearchTerm 全部字段
            - 归因窗口完整：purchases_7d/14d/30d + sales_7d/14d/30d
            - 自动按 report_type 去重：每个维度只取对应报告类型，避免跨类型重复累加
    """
    total_affected = 0

    # 1. overall 维度 — 汇总 spCampaigns 报告（不含 spAdGroups 避免 double-count）
    cursor.execute("""
        SELECT
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(cost), 0) AS ad_spend,
            COALESCE(SUM(purchases_7d), 0) AS orders_7d,
            COALESCE(SUM(purchases_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date BETWEEN %s AND %s
          AND report_type = 'spCampaigns'
    """, (shop_id, date_start, date_end))
    row = cursor.fetchone()
    if row and row['ad_spend'] is not None and float(row['ad_spend']) > 0:
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'overall', '', '', '', '', '', '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    # 2. campaign 维度 — 从 spCampaigns 取
    cursor.execute("""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(cost), 0) AS ad_spend,
            COALESCE(SUM(purchases_7d), 0) AS orders_7d,
            COALESCE(SUM(purchases_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date BETWEEN %s AND %s
          AND report_type = 'spCampaigns' AND campaign_id != ''
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

    # 3. ad_group 维度 — 从 spAdvertisedProduct 聚合 (含 adGroupId/adGroupName)
    cursor.execute("""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            ad_group_id,
            MAX(ad_group_name) AS ad_group_name,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(cost), 0) AS ad_spend,
            COALESCE(SUM(purchases_7d), 0) AS orders_7d,
            COALESCE(SUM(purchases_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date BETWEEN %s AND %s
          AND report_type = 'spAdvertisedProduct' AND ad_group_id != ''
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

    # 4. asin 维度 — 从 spAdvertisedProduct 取（含 campaign_id + ad_group_id）
    cursor.execute("""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            ad_group_id,
            MAX(ad_group_name) AS ad_group_name,
            advertised_asin AS asin,
            MAX(advertised_sku) AS sku,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(cost), 0) AS ad_spend,
            COALESCE(SUM(purchases_7d), 0) AS orders_7d,
            COALESCE(SUM(purchases_30d), 0) AS orders_30d,
            COALESCE(SUM(sales_7d), 0) AS sales_7d,
            COALESCE(SUM(sales_30d), 0) AS sales_30d
        FROM amazon_ads_raw_reports
        WHERE shop_id = %s AND report_date BETWEEN %s AND %s
          AND report_type = 'spAdvertisedProduct' AND advertised_asin != ''
        GROUP BY campaign_id, ad_group_id, advertised_asin
    """, (shop_id, date_start, date_end))
    for row in cursor.fetchall():
        _insert_advertising_report(
            cursor, report_type, report_date, report_week, report_month,
            shop_id, 'asin', row['campaign_id'] or '', row['campaign_name'] or '',
            row['ad_group_id'] or '', row['ad_group_name'] or '', row['asin'], row['sku'] or '',
            int(row['impressions'] or 0), int(row['clicks'] or 0), Decimal(str(row['ad_spend'] or 0)),
            int(row['orders_7d'] or 0), int(row['orders_30d'] or 0),
            Decimal(str(row['sales_7d'] or 0)), Decimal(str(row['sales_30d'] or 0))
        )
        total_affected += cursor.rowcount

    return total_affected


def _calc_ad_metrics(impressions, clicks, ad_spend, sales_7d, sales_30d):
    """
    计算广告比率指标

    简介: 返回 CT/CPC/ACOS/ROAS（分母为 0 时返回 None）。
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
    """
    插入/更新单条广告效果报表

    简介: 自动计算 CT/CPC/ACOS/ROAS 后写入 report_advertising，幂等。
    """
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
    从 amazon_ad_spend 聚合生成广告效果报表

    简介: 同步生成 overall / campaign / ad_group / asin 四个维度的广告数据。
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
    生成单日广告效果报表

    简介: 从 amazon_ads_raw_reports 拉取单日数据，生成 4 个维度的广告报表。
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
                    affected = _generate_advertising_from_raw(
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


def generate_yesterday_reports():
    """
    一键生成全部定时报表（Cron 入口）

    简介: 生成最近10天日报 + SKU利润 + SKU销售汇总 + 库存周转 + 广告日报。

    详细:
      - 日报: 生成最近10天的经营日报 + 广告日报
      - SKU 利润: 生成 PDT T-1 的 SKU 利润表
      - SKU 销售汇总: 生成 PDT T-1 的 SKU 销售数据 (v2)
      - 库存周转: 生成当前库存周转数据
      - 时区: 所有 report_date 使用 PDT 时间 (UTC-7)，与 Amazon 数据时间一致
    """
    pdt_today = datetime.now(timezone(timedelta(hours=-7))).date()
    results = {}

    for i in range(1, 11):
        report_date = (pdt_today - timedelta(days=i)).strftime('%Y-%m-%d')
        print(f"[Report] generate daily: {report_date}")
        results[f'business_daily_{report_date}'] = generate_business_daily(report_date)

    yesterday = (pdt_today - timedelta(days=1)).strftime('%Y-%m-%d')
    results['sku_profit'] = generate_sku_profit(yesterday)
    results['sku_sales'] = generate_sku_sales(yesterday)
    results['inventory_turnover'] = generate_inventory_turnover()

    for i in range(1, 11):
        report_date = (pdt_today - timedelta(days=i)).strftime('%Y-%m-%d')
        results[f'advertising_daily_{report_date}'] = generate_advertising_daily(report_date)

    return results
