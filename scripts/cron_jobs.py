#!/usr/bin/env python3
"""
Crontab 定时任务入口脚本
原 APScheduler 的所有定时任务迁移到此，通过命令行参数指定执行哪个任务

用法：
    python scripts/cron_jobs.py <task_name>

可用任务：
    inventory        库存同步（每小时）
    inbound-30min    入库计划 + 货件列表同步（每30分钟）
    inbound-6h       箱子 + 货件详情同步（每6小时）
    listing          Listing同步（每3小时）
    orders-recent    近期订单同步 24h（每15分钟）
    orders-week      本周订单同步 7d（每3小时）
    orders-month     本月订单同步 30d（每6小时，仅列表）
    finances-recent  近期订单财务明细同步 2d（每30分钟）
    exchange-rate    汇率同步（每天9点）
     reports-daily    经营日报+SKU利润+库存周转+广告日报（每天凌晨2点，T-1/T-2 estimated + T-3 settled）
     reports-weekly   经营周报+广告周报（每周三凌晨3点，汇总上周）
     reports-monthly  经营月报+广告月报（每月3号凌晨4点，汇总上月）
     auto-complete    自动完结超期订单：进货单/货代运单创建10天后仍为初始状态，自动变更为已完成并入账（每天凌晨1点）

crontab 示例：
    0 * * * * cd /项目路径 && python scripts/cron_jobs.py inventory >> /var/log/cron_inventory.log 2>&1
"""
import os
import sys
import time
import requests

# 把项目根目录加入 PYTHONPATH，确保能导入 services/ blueprints/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 加载 .env 配置（crontab 不会自动加载环境变量）
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from datetime import datetime, timedelta
from services.mysql_service import get_db_connection
from services.shop_service import get_all_active_shops
def fetch_and_save_exchange_rate(from_currency='CNY', to_currency='USD'):
    try:
        url = f"https://open.er-api.com/v6/latest/{from_currency}"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get('result') != 'success':
            print(f"[ExchangeRate] API 返回非 success: {data}")
            return None

        rate = data.get('rates', {}).get(to_currency)
        if rate is None:
            print(f"[ExchangeRate] 未找到 {from_currency}->{to_currency} 汇率")
            return None

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO exchange_rates (from_currency, to_currency, rate, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        rate = VALUES(rate),
                        updated_at = VALUES(updated_at)
                """, (from_currency, to_currency, rate))
                conn.commit()
        finally:
            conn.close()

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now}] [ExchangeRate] 汇率更新成功: {from_currency}->{to_currency} = {rate}")
        return {'rate': rate, 'updated_at': now}
    except Exception as e:
        print(f"[ExchangeRate] 获取汇率异常: {e}")
        return None


def _now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


# ==================== 1. 库存同步 ====================

def task_inventory():
    from blueprints.amazon.inventory import _sync_inventory
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过库存同步")
        return

    print(f"[{_now_str()}] [Cron] 开始库存同步，共 {len(shops)} 个店铺...")
    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        try:
            result = _sync_inventory(shop_id=shop['id'])
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 库存同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 库存同步异常: {e}")


# ==================== 2. 入库计划同步（30分钟粒度：计划+货件列表）====================

def task_inbound_30min():
    from blueprints.amazon.inbound_plans import (
        _sync_inbound_plans,
        _sync_all_inbound_plan_shipments,
    )
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过入库计划同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']

        # 1. 同步入库计划列表
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划同步...")
        try:
            plans_result = _sync_inbound_plans(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划同步完成: {plans_result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划同步异常: {e}")
            continue

        # 2. 同步货件列表
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件同步...")
        try:
            result = _sync_all_inbound_plan_shipments(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步异常: {e}")


# ==================== 3. 入库计划同步（6小时粒度：箱子+货件详情）====================

def task_inbound_6h():
    from blueprints.amazon.inbound_plans import (
        _sync_all_inbound_plan_boxes,
        _sync_all_inbound_shipment_details,
    )
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过入库计划深度同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']

        # 1. 同步所有箱子
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划箱子同步...")
        try:
            result = _sync_all_inbound_plan_boxes(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步异常: {e}")

        # 2. 同步所有货件详情
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件详情同步...")
        try:
            result = _sync_all_inbound_shipment_details(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件详情同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件详情同步异常: {e}")


# ==================== 3. Listing 同步 ====================

def task_listing():
    from blueprints.amazon.listing import _sync_listings
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过 Listing 同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始 Listing 同步...")
        try:
            result = _sync_listings(
                shop_id=shop_id,
                included_data=["summaries", "attributes", "issues"],
                page_size=20
            )
            err_msg = f", error={result['error']}" if result.get('error') else ''
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] Listing 同步完成: synced={result.get('synced_count', 0)}, fetched={result.get('total_fetched', 0)}{err_msg}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] Listing 同步异常: {e}")


# ==================== 4. 订单同步 ====================

def _get_recent_order_ids(shop_id, hours):
    """查询最近 N 小时内有更新的订单ID"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            since = (datetime.now() - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                SELECT amazon_order_id FROM amazon_orders
                WHERE shop_id = %s AND last_update_date >= %s
                ORDER BY last_update_date DESC
            """, (shop_id, since))
            return [row['amazon_order_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def _sync_order_items_batch(shop_id, order_ids, label=""):
    """批量同步订单商品，返回统计信息"""
    from blueprints.amazon.orders import _sync_order_items
    items_total = 0
    items_errors = []
    for oid in order_ids:
        try:
            result = _sync_order_items(shop_id=shop_id, order_id=oid)
            items_total += result.get('synced_count', 0)
            if result.get('error'):
                items_errors.append({"order_id": oid, "error": result['error']})
        except Exception as e:
            items_errors.append({"order_id": oid, "error": str(e)})
        time.sleep(0.3)
    return items_total, items_errors


def task_orders_recent():
    """每15分钟：同步最近24小时内有更新的订单（列表+商品）"""
    from blueprints.amazon.orders import _sync_orders
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    shops = get_all_active_shops()
    if not shops:
        print(f"[{now_str}] [Cron] 没有启用的店铺，跳过订单同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']
        print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始近期订单同步(24h)...")
        try:
            last_updated_after = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
            result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")

            order_ids = _get_recent_order_ids(shop_id=shop_id, hours=24)
            if order_ids:
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始同步近期订单商品，共 {len(order_ids)} 单...")
                items_total, items_errors = _sync_order_items_batch(shop_id=shop_id, order_ids=order_ids, label="近期")
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单商品同步完成: {items_total} 条"
                      f"{f', 错误: {len(items_errors)}个' if items_errors else ''}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单同步异常: {e}")


def task_orders_week():
    """每3小时：同步最近7天内有更新的订单（列表+商品，商品只同步24h~7d区间）"""
    from blueprints.amazon.orders import _sync_orders
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    shops = get_all_active_shops()
    if not shops:
        print(f"[{now_str}] [Cron] 没有启用的店铺，跳过订单同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']
        print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始本周订单同步(7d)...")
        try:
            last_updated_after = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
            result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本周订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")

            # 只同步 24h ~ 7d 这个区间的订单商品
            since_24h = (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
            since_7d = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT amazon_order_id FROM amazon_orders
                        WHERE shop_id = %s AND last_update_date >= %s AND last_update_date < %s
                        ORDER BY last_update_date DESC
                    """, (shop_id, since_7d, since_24h))
                    order_ids = [row['amazon_order_id'] for row in cursor.fetchall()]
            finally:
                conn.close()

            if order_ids:
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始同步本周订单商品(24h~7d)，共 {len(order_ids)} 单...")
                items_total, items_errors = _sync_order_items_batch(shop_id=shop_id, order_ids=order_ids, label="本周")
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本周订单商品同步完成: {items_total} 条"
                      f"{f', 错误: {len(items_errors)}个' if items_errors else ''}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本周订单同步异常: {e}")


def task_orders_month():
    """每6小时：同步最近30天内有更新的订单（仅列表，不抓商品）"""
    from blueprints.amazon.orders import _sync_orders
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    shops = get_all_active_shops()
    if not shops:
        print(f"[{now_str}] [Cron] 没有启用的店铺，跳过订单同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']
        print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始本月订单同步(30d,仅列表)...")
        try:
            last_updated_after = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本月订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本月订单同步异常: {e}")


# ==================== 4. 财务明细同步 ====================

def task_finances_recent():
    """每30分钟：同步3~7天前订单的财务明细（跳过T+2未结算窗口）"""
    from blueprints.amazon.finances import sync_finances_recent
    now_str = _now_str()
    print(f"[{now_str}] [Cron] 开始财务明细同步(3~7d ago)...")
    try:
        results = sync_finances_recent(days_to=3, days_from=7)
        total_orders = sum(r.get('total_orders', 0) for r in results.values())
        total_success = sum(r.get('success', 0) for r in results.values())
        total_failed = sum(r.get('failed', 0) for r in results.values())
        print(f"[{now_str}] [Cron] 财务明细同步完成: 店铺={len(results)}, 订单={total_orders}, 成功={total_success}, 失败={total_failed}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 财务明细同步异常: {e}")


# ==================== 5. 汇率同步 ====================

def task_exchange_rate():
    print(f"[{_now_str()}] [Cron] 开始汇率同步...")
    try:
        result = fetch_and_save_exchange_rate('CNY', 'USD')
        if result:
            print(f"[{_now_str()}] [Cron] 汇率同步完成: {result}")
        else:
            print(f"[{_now_str()}] [Cron] 汇率同步失败，请检查日志")
    except Exception as e:
        print(f"[{_now_str()}] [Cron] 汇率同步异常: {e}")


# ==================== 6. 报表生成 ====================

def task_reports_daily():
    """
    每天凌晨生成日报（T-1/T-2 预估 + T-3 已结算）
    顺带检查今天是否是周三（生周报）或 3 号（生月报）
    """
    from services.report_generator import generate_yesterday_reports
    print(f"[{_now_str()}] [Cron] 开始生成报表（T-1/T-2 estimated + T-3 settled）...")
    try:
        results = generate_yesterday_reports()
        print(f"[{_now_str()}] [Cron] 报表生成完成: {list(results.keys())}")
    except Exception as e:
        import traceback
        print(f"[{_now_str()}] [Cron] 报表生成异常: {e}")
        traceback.print_exc()


def task_reports_weekly():
    """
    每周三凌晨生成周报（上周 Mon-Sun）

    注: 主入口是 task_reports_daily（每天跑），周三自动出周报。
    此函数是备用手动入口。
    """
    from services.report_generator import generate_business_weekly, generate_advertising_weekly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    yesterday_dt = now - timedelta(days=1)
    week_start = (yesterday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    week_end = yesterday_dt.strftime('%Y-%m-%d')
    week_label = f"{week_start}~{week_end}"
    print(f"[{now_str}] [Cron] 开始生成周报 {week_label}...")
    try:
        result_biz = generate_business_weekly(week_start, week_end)
        result_ad = generate_advertising_weekly(week_start, week_end)
        print(f"[{now_str}] [Cron] 周报生成完成: biz={result_biz}, ad={result_ad}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 周报生成异常: {e}")


def task_reports_monthly():
    """
    每月3号生成本月月报

    注: 主入口是 task_reports_daily（每天跑），3号自动出月报。
    此函数是备用手动入口。
    """
    from services.report_generator import generate_business_monthly, generate_advertising_monthly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    month_str = (now - timedelta(days=1)).strftime('%Y-%m')
    print(f"[{now_str}] [Cron] 开始生成月报 {month_str}...")
    try:
        result_biz = generate_business_monthly(month_str)
        result_ad = generate_advertising_monthly(month_str)
        print(f"[{now_str}] [Cron] 月报生成完成: biz={result_biz}, ad={result_ad}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 月报生成异常: {e}")


# ==================== 7. 自动完结超期订单 ====================

def task_auto_complete():
    """
    每天凌晨执行：将创建超过 10 天仍为初始状态（status=0）的进货单和货代运单
    自动变更为最终状态（已完成），并创建对应的支出记录。
    """
    from blueprints.supplier import PURCHASE_ORDER_STATUS_INITIAL, PURCHASE_ORDER_STATUS_COMPLETED
    from blueprints.logistics import WAYBILL_STATUS_INITIAL, WAYBILL_STATUS_COMPLETED
    from blueprints.expenses import create_expense_for_source

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # ---------- 进货单 ----------
            cursor.execute("""
                SELECT id, order_no, total_amount
                FROM purchase_orders
                WHERE status = %s AND created_at <= DATE_SUB(NOW(), INTERVAL 10 DAY)
            """, (PURCHASE_ORDER_STATUS_INITIAL,))
            stale_orders = cursor.fetchall()

            if stale_orders:
                print(f"[{_now_str()}] [Cron] 发现 {len(stale_orders)} 条超期进货单，开始自动完结...")
                for order in stale_orders:
                    try:
                        cursor.execute(
                            "UPDATE purchase_orders SET status = %s WHERE id = %s",
                            (PURCHASE_ORDER_STATUS_COMPLETED, order['id'])
                        )
                        # 避免重复创建支出记录
                        cursor.execute(
                            "SELECT id FROM expenses WHERE source_type = %s AND source_no = %s LIMIT 1",
                            ('purchase_order', order['order_no'])
                        )
                        if not cursor.fetchone() and order['order_no']:
                            create_expense_for_source(
                                conn, '采购/货值',
                                float(order['total_amount'] or 0),
                                datetime.now().strftime('%Y-%m-%d'),
                                f"进货单 {order['order_no']}（自动完结）",
                                'purchase_order', order['order_no'], 'company'
                            )
                        print(f"[{_now_str()}] [Cron]   进货单 {order['order_no']} 已自动完结并入账")
                    except Exception as e:
                        print(f"[{_now_str()}] [Cron]   进货单 {order.get('order_no')} 自动完结异常: {e}")
            else:
                print(f"[{_now_str()}] [Cron] 无超期进货单")

            # ---------- 货代运单 ----------
            cursor.execute("""
                SELECT id, waybill_no, total_cost_cny
                FROM logistics_waybills
                WHERE status = %s AND created_at <= DATE_SUB(NOW(), INTERVAL 10 DAY)
            """, (WAYBILL_STATUS_INITIAL,))
            stale_waybills = cursor.fetchall()

            if stale_waybills:
                print(f"[{_now_str()}] [Cron] 发现 {len(stale_waybills)} 条超期货代运单，开始自动完结...")
                for wb in stale_waybills:
                    try:
                        cursor.execute(
                            "UPDATE logistics_waybills SET status = %s WHERE id = %s",
                            (WAYBILL_STATUS_COMPLETED, wb['id'])
                        )
                        if wb['waybill_no']:
                            cursor.execute(
                                "SELECT id FROM expenses WHERE source_type = %s AND source_no = %s LIMIT 1",
                                ('logistics_waybill', wb['waybill_no'])
                            )
                            if not cursor.fetchone():
                                create_expense_for_source(
                                    conn, '物流/头程',
                                    float(wb['total_cost_cny'] or 0),
                                    datetime.now().strftime('%Y-%m-%d'),
                                    f"运单 {wb['waybill_no']}（自动完结）",
                                    'logistics_waybill', wb['waybill_no'], 'company'
                                )
                        print(f"[{_now_str()}] [Cron]   运单 {wb['waybill_no']} 已自动完结并入账")
                    except Exception as e:
                        print(f"[{_now_str()}] [Cron]   运单 {wb.get('waybill_no')} 自动完结异常: {e}")
            else:
                print(f"[{_now_str()}] [Cron] 无超期货代运单")

            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[{_now_str()}] [Cron] 自动完结超期订单异常: {e}")
    finally:
        conn.close()


# ==================== 入口 ====================

TASK_MAP = {
    'inventory': task_inventory,
    'inbound-30min': task_inbound_30min,
    'inbound-6h': task_inbound_6h,
    'listing': task_listing,
    'orders-recent': task_orders_recent,
    'orders-week': task_orders_week,
    'orders-month': task_orders_month,
    'finances-recent': task_finances_recent,
    'exchange-rate': task_exchange_rate,
    'reports-daily': task_reports_daily,
    'reports-weekly': task_reports_weekly,
    'reports-monthly': task_reports_monthly,
    'auto-complete': task_auto_complete,
}


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python scripts/cron_jobs.py <task_name>")
        print(f"可用任务: {', '.join(TASK_MAP.keys())}")
        sys.exit(1)

    task_name = sys.argv[1]
    task_func = TASK_MAP.get(task_name)
    if not task_func:
        print(f"未知任务: {task_name}")
        print(f"可用任务: {', '.join(TASK_MAP.keys())}")
        sys.exit(1)

    print(f"[{_now_str()}] [Cron] 开始执行任务: {task_name}")
    task_func()
    print(f"[{_now_str()}] [Cron] 任务执行结束: {task_name}")
