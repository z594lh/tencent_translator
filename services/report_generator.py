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

模块入口:
  - generate_yesterday_reports() — Cron 每天凌晨 2 点调用
  - generate_business_daily/w.eekly/monthly — 手动生成经营报表
  - generate_sku_profit — 手动生成 SKU 利润表
  - generate_inventory_turnover — 生成库存周转

成本计算 (2026-05-26 重构):
  所有 SKU 成本计算统一通过 profit_calculator:
    - get_unit_costs(cursor, seller_sku, exchange_rate) -> UnitCostBreakdown
    - calculate_profit(sales, qty, unit_costs, ad_cost, refund) -> ProfitResult
"""
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import json

from services.mysql_service import get_db_connection
from services.profit_calculator import (
    get_exchange_rate,
    get_unit_costs,
    calculate_profit,
)

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
    cursor.execute("""
        SELECT
            COUNT(DISTINCT o.amazon_order_id) AS settled_order_count,
            COALESCE(SUM(f.product_charges), 0) AS total_product_charges,
            COALESCE(SUM(f.total_amount), 0) AS total_net_sales,
            COALESCE(SUM(f.fba_fees), 0) AS total_fba_fees,
            COALESCE(SUM(f.commission), 0) AS total_commission
        FROM amazon_orders o
        JOIN amazon_order_finances f
            ON o.amazon_order_id = f.amazon_order_id COLLATE utf8mb4_unicode_ci
            AND o.shop_id = f.shop_id
        WHERE o.shop_id = %s
          AND DATE(o.purchase_date) = %s
          AND f.transaction_type = 'Shipment'
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

    # 2. 获取该日期已结算订单的 items，解析 SKU 级别数据
    cursor.execute("""
        SELECT f.items_json FROM amazon_order_finances f
        JOIN amazon_orders o
            ON o.amazon_order_id = f.amazon_order_id COLLATE utf8mb4_unicode_ci AND o.shop_id = f.shop_id
        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
          AND f.transaction_type = 'Shipment'
          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
    """, (sid, report_date))
    txn_rows = cursor.fetchall()

    sku_qty = {}
    for txn_row in txn_rows:
        items = txn_row['items_json']
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except (json.JSONDecodeError, TypeError):
                items = []
        for item in (items or []):
            for ctx in item.get('contexts', []):
                if ctx.get('contextType') == 'ProductContext':
                    sku = ctx.get('sku', '') or ''
                    qty = int(ctx.get('quantityShipped', 0) or 0)
                    if sku and qty > 0:
                        sku_qty[sku] = sku_qty.get(sku, 0) + qty

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

    # 5. 退款（按订单 purchase_date 汇总 Refund）
    cursor.execute("""
        SELECT COALESCE(SUM(ABS(f.total_amount)), 0) AS refund_sum
        FROM amazon_order_finances f
        JOIN amazon_orders o
            ON o.amazon_order_id = f.amazon_order_id COLLATE utf8mb4_unicode_ci AND o.shop_id = f.shop_id
        WHERE o.shop_id = %s
          AND DATE(o.purchase_date) = %s
          AND f.transaction_type = 'Refund'
    """, (sid, report_date))
    refund_row = cursor.fetchone()
    refund_amount = Decimal(str(refund_row['refund_sum'] or 0))

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


def generate_business_weekly(start_date, end_date, shop_id=None):
    """
    基于已生成的日报汇总周报

    简介: 对指定周的日报进行 SUM 聚合，写入一条 weekly 记录。

    详细:
      - 周三生成时，上周日报应全部为 settled（>= T-3）
      - 若仍有 estimated 日报，输出警告日志但不阻塞写入
      - 周报 data_status = 'settled'

    参数:
        start_date/end_date: 'YYYY-MM-DD'，如 '2026-05-25' / '2026-05-31'
    """
    report_type = 'business_weekly'
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
                    cursor.execute(
                        """SELECT report_date, data_status FROM report_business
                           WHERE shop_id = %s AND report_type = 'daily'
                             AND report_date BETWEEN %s AND %s
                             AND data_status IN ('estimated', 'partial')""",
                        (sid, start_date, end_date),
                    )
                    est_rows = cursor.fetchall()
                    if est_rows:
                        est_dates = [r['report_date'] for r in est_rows]
                        print(f"[Report] 周报 {report_week_label} shop={sid}: "
                              f"{len(est_dates)} 天未完全结算: {est_dates}")

                    cursor.execute("""
                        INSERT INTO report_business (
                            shop_id, report_type, report_date, report_week, report_month, data_status,
                            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
                            headway_cost, headway_ratio, order_count, sku_count,
                            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
                        )
                        SELECT
                            shop_id,
                            'weekly',
                            %s,
                            %s,
                            '',
                            'settled',
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
                            data_status = 'settled',
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

    简介: 对上月日报进行 SUM 聚合，写入一条 monthly 记录。

    详细:
      - 每月 3 号生成时，上月日报应全部为 settled（>= T-3）
      - 若仍有 estimated 日报，输出警告日志但不阻塞写入
      - 月报 data_status = 'settled'

    参数:
        month_str: 'YYYY-MM' 如 '2026-05'
    """
    report_type = 'business_monthly'
    period = month_str
    year, month = map(int, month_str.split('-'))
    start_date = f'{year}-{month:02d}-01'
    if month == 12:
        end_date = f'{year + 1}-01-01'
    else:
        end_date = f'{year}-{month + 1:02d}-01'
    end_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

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
                    cursor.execute(
                        """SELECT report_date, data_status FROM report_business
                           WHERE shop_id = %s AND report_type = 'daily'
                             AND report_date BETWEEN %s AND %s
                             AND data_status IN ('estimated', 'partial')""",
                        (sid, start_date, end_date),
                    )
                    est_rows = cursor.fetchall()
                    if est_rows:
                        est_dates = [r['report_date'] for r in est_rows]
                        print(f"[Report] 月报 {month_str} shop={sid}: "
                              f"{len(est_dates)} 天未完全结算: {est_dates}")

                    month_start = f'{year}-{month:02d}-01'
                    cursor.execute("""
                        INSERT INTO report_business (
                            shop_id, report_type, report_date, report_week, report_month, data_status,
                            total_sales, total_cost, product_cost, gross_profit, gross_profit_rate,
                            headway_cost, headway_ratio, order_count, sku_count,
                            ad_cost, refund_amount, refund_rate, platform_fees, fba_fees
                        )
                        SELECT
                            shop_id,
                            'monthly',
                            %s,
                            '',
                            %s,
                            'settled',
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
                            data_status = 'settled',
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

                    # 0. 预聚合: 从 finances items_json 解析真实 FBA 费 + 佣金 (按 SKU/按 item)
                    cursor.execute("""
                        SELECT items_json
                        FROM amazon_order_finances
                        WHERE shop_id = %s
                          AND transaction_type = 'Shipment'
                          AND posted_date = %s
                    """, (sid, report_date))
                    finance_rows = cursor.fetchall()

                    sku_fees = {}
                    for frow in finance_rows:
                        items = frow['items_json']
                        if isinstance(items, str):
                            try:
                                items = json.loads(items)
                            except (json.JSONDecodeError, TypeError):
                                items = []
                        for item in (items or []):
                            # 每 item 有独立的 breakdowns → 解析 item-level 费用
                            it_fba = 0.0
                            it_com = 0.0
                            for bd in (item.get('breakdowns', []) or []):
                                bt = bd.get('breakdownType', '')
                                if bt == 'AmazonFees':
                                    for sub in (bd.get('breakdowns', []) or []):
                                        amt = float((sub.get('breakdownAmount') or {}).get('currencyAmount', 0))
                                        st = sub.get('breakdownType', '')
                                        if amt < 0:
                                            if st.startswith('FBAPer'):
                                                it_fba += abs(amt)
                                            elif st == 'Commission':
                                                it_com += abs(amt)
                            # 找 SKU
                            for ctx in (item.get('contexts', []) or []):
                                if ctx.get('contextType') == 'ProductContext':
                                    sku = ctx.get('sku', '') or ''
                                    if sku and (it_fba > 0 or it_com > 0):
                                        entry = sku_fees.setdefault(sku, {'fba': 0.0, 'commission': 0.0})
                                        entry['fba'] += it_fba
                                        entry['commission'] += it_com

                    # 1. 获取该店铺所有活跃产品（SKU 全集）
                    cursor.execute("SELECT asin, seller_sku AS sku, product_name FROM products WHERE status=1 AND asin IS NOT NULL AND asin != ''")
                    all_products = cursor.fetchall()

                    # 2. 预聚合销售数据（按 ASIN）
                    cursor.execute("""
                        SELECT oi.asin,
                               COALESCE(SUM(oi.quantity_shipped), 0) AS sales_qty,
                               COALESCE(SUM(oi.item_price_amount), 0) AS sales_amount
                        FROM amazon_orders o
                        JOIN amazon_order_items oi ON o.amazon_order_id = oi.amazon_order_id AND o.shop_id = oi.shop_id
                        WHERE o.shop_id = %s AND DATE(o.purchase_date) = %s
                          AND o.order_status NOT IN ('Canceled', 'PendingAvailability')
                        GROUP BY oi.asin
                    """, (sid, report_date))
                    sales_map = {}
                    for r in cursor.fetchall():
                        sales_map[r['asin']] = r

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

                    # 4. 遍历所有产品，写入 sku_profit（每天每个 SKU 一条）
                    for prod in all_products:
                        asin = prod['asin'] or ''
                        sku = prod['sku'] or ''
                        product_name = prod.get('product_name') or ''
                        if not asin or not sku:
                            continue

                        # 销售数据
                        sales = sales_map.get(asin, {})
                        sales_qty = int(sales.get('sales_qty', 0) or 0)
                        sales_amount = Decimal(str(sales.get('sales_amount', 0) or 0))
                        avg_price = Decimal('0')
                        if sales_qty > 0 and sales_amount > 0:
                            avg_price = sales_amount / sales_qty

                        # 广告数据
                        ad_data = ad_map.get(asin, {})
                        ad_cost = Decimal(str(ad_data.get('ad_cost', 0)))
                        ad_sales = Decimal(str(ad_data.get('ad_sales', 0)))
                        ad_orders = int(ad_data.get('ad_orders', 0))
                        ad_acos = (ad_cost / ad_sales) if ad_sales > 0 else Decimal('0')

                        # 成本（仅对有销售的 SKU 详细计算）
                        unit_costs = get_unit_costs(cursor, sku, exchange_rate, shop_id=sid)
                        if not product_name:
                            product_name = unit_costs.product_name or ''

                        # 真实 FBA 费 + 佣金
                        real_fba = Decimal(str(sku_fees.get(sku, {}).get('fba', 0)))
                        real_commission = Decimal(str(sku_fees.get(sku, {}).get('commission', 0)))
                        if real_fba > 0 or real_commission > 0:
                            fba_fees = real_fba
                            platform_fees = real_commission
                        elif sales_qty > 0:
                            profit = calculate_profit(sales_amount, sales_qty, unit_costs, Decimal('0'), Decimal('0'))
                            fba_fees = profit.fba_fees
                            platform_fees = profit.commission
                        else:
                            fba_fees = Decimal('0')
                            platform_fees = Decimal('0')

                        # 退款
                        cursor.execute("""
                            SELECT COALESCE(SUM(refund_amount), 0) AS refund_sum
                            FROM amazon_refund_records
                            WHERE shop_id = %s AND asin = %s AND refund_date = %s
                        """, (sid, asin, report_date))
                        refund_amount = Decimal(str(cursor.fetchone()['refund_sum'] or 0))

                        # 产品成本
                        product_cost = unit_costs.purchase_cost_usd * sales_qty
                        headway_cost = unit_costs.headway_cost_usd * sales_qty
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
                            sales_qty, float(sales_amount), float(avg_price),
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


# ==================== 3. 库存周转生成 ====================

def generate_inventory_turnover(shop_id=None):
    """
    生成库存周转数据

    简介: 基于当前 FBA 库存 + 近 30 天销售速度，计算每个 SKU 的周转天数、状态和建议补货量。

    详细:
      - 数据源: amazon_inventory（库存）+ amazon_orders（销售速度）
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
                            if isinstance(last_sale_date, str):
                                last_sale_date = datetime.strptime(last_sale_date, '%Y-%m-%d').date()
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


def generate_advertising_weekly(start_date, end_date, shop_id=None):
    """
    生成周度广告效果报表

    简介: 对指定周内的 amazon_ads_raw_reports 数据聚合，生成 4 维度广告周报。
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
                    affected = _generate_advertising_from_raw(
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
    生成月度广告效果报表

    简介: 对指定月的 amazon_ads_raw_reports 数据聚合，生成 4 维度广告月报。
    """
    report_type = 'advertising_monthly'
    period = month_str
    year, month = map(int, month_str.split('-'))
    start_date = f'{year}-{month:02d}-01'
    if month == 12:
        end_date = f'{year + 1}-01-01'
    else:
        end_date = f'{year}-{month + 1:02d}-01'
    end_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

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
                    affected = _generate_advertising_from_raw(
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
    一键生成全部定时报表（Cron 入口）

    简介: 生成最近10天日报 + 检查周报/月报触发条件。

    详细:
      - 日报: 生成最近10天的日报（确保未结算订单后续也能被覆盖）
      - 周报: 每周三生成上周 Mon-Sun
      - 月报: 每月 3 号生成上月
    """
    today = datetime.now().date()
    results = {}

    # 日报: 最近10天（T-1 到 T-10），确保延迟结算的订单最终被收录
    for i in range(1, 11):
        report_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        print(f"[Report] generate daily: {report_date}")
        results[f'business_daily_{report_date}'] = generate_business_daily(report_date)

    # SKU 利润（T-1，基于 orders 数据，不依赖 finances）
    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    results['sku_profit'] = generate_sku_profit(yesterday)

    # 库存周转
    results['inventory_turnover'] = generate_inventory_turnover()

    # 广告效果日报（最近10天）
    for i in range(1, 11):
        report_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        results[f'advertising_daily_{report_date}'] = generate_advertising_daily(report_date)

    # 周报: 每周三生成最近3周
    if today.weekday() == 2:
        for week_offset in range(3):
            sunday_dt = today - timedelta(days=today.weekday() + 1 + week_offset * 7)
            week_start = (sunday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
            week_end = sunday_dt.strftime('%Y-%m-%d')
            print(f"[Report] 周三触发周报: {week_start}~{week_end}")
            results[f'business_weekly_{week_start}'] = generate_business_weekly(week_start, week_end)
            results[f'advertising_weekly_{week_start}'] = generate_advertising_weekly(week_start, week_end)

    # 月报: 每月 3 号生成最近2个月
    if today.day == 3:
        for month_offset in range(2):
            month_dt = today.replace(day=1) - timedelta(days=1 + month_offset * 31)
            month_str = month_dt.strftime('%Y-%m')
            print(f"[Report] 3号触发月报: {month_str}")
            results[f'business_monthly_{month_str}'] = generate_business_monthly(month_str)
            results[f'advertising_monthly_{month_str}'] = generate_advertising_monthly(month_str)

    return results
