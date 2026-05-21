#!/usr/bin/env python3
"""
Crontab 定时任务入口脚本
原 APScheduler 的所有定时任务迁移到此，通过命令行参数指定执行哪个任务

用法：
    python scripts/cron_jobs.py <task_name>

可用任务：
    inventory        库存同步（每小时）
    inbound          入库计划同步（每3小时）
    listing          Listing同步（每3小时）
    orders-recent    近期订单同步 24h（每15分钟）
    orders-week      本周订单同步 7d（每3小时）
    orders-month     本月订单同步 30d（每6小时，仅列表）
    exchange-rate    汇率同步（每天9点）
    reports-daily    经营日报+SKU利润+库存周转+广告日报（每天凌晨2点）
    reports-weekly   经营周报+广告周报（每周一凌晨3点）
    reports-monthly  经营月报+广告月报（每月1号凌晨4点）

crontab 示例：
    0 * * * * cd /项目路径 && python scripts/cron_jobs.py inventory >> /var/log/cron_inventory.log 2>&1
"""
import os
import sys
import time

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
from services.scheduler import fetch_and_save_exchange_rate


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


# ==================== 2. 入库计划同步 ====================

def task_inbound():
    from blueprints.amazon.inbound_plans import (
        _sync_inbound_plans,
        _sync_all_inbound_plan_boxes,
        _sync_all_inbound_plan_shipments,
        _sync_inbound_plan_shipments_all,
        _get_inbound_plan_ids,
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

        if plans_result.get('synced_count', 0) <= 0:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 没有入库计划，跳过后续同步")
            continue

        # 2. 同步箱子
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划箱子同步...")
        try:
            result = _sync_all_inbound_plan_boxes(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步异常: {e}")

        # 3. 同步货件列表
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件同步...")
        try:
            result = _sync_all_inbound_plan_shipments(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步异常: {e}")
            continue

        # 4. 同步货件详情
        plan_ids = _get_inbound_plan_ids(shop_id=shop_id, status='ACTIVE')
        if not plan_ids:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 没有 ACTIVE 入库计划，跳过货件详情同步")
            continue

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件详情同步，共 {len(plan_ids)} 个计划...")
        total_detail_synced = 0
        detail_errors = []
        for plan_id in plan_ids:
            try:
                result = _sync_inbound_plan_shipments_all(shop_id=shop_id, plan_id=plan_id)
                total_detail_synced += result.get('details_synced_count', 0)
                if result.get('errors'):
                    detail_errors.extend(result['errors'])
            except Exception as e:
                detail_errors.append({"plan_id": plan_id, "error": str(e)})

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件详情同步完成: {total_detail_synced} 条"
              f"{f', 错误: {detail_errors}' if detail_errors else ''}")


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


# ==================== 4. 汇率同步 ====================

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


# ==================== 5. 报表生成 ====================

def task_reports_daily():
    """每天凌晨生成昨日报表（经营日报+SKU利润+库存周转+广告日报）"""
    from services.report_generator import generate_yesterday_reports
    print(f"[{_now_str()}] [Cron] 开始生成昨日报表...")
    try:
        results = generate_yesterday_reports()
        print(f"[{_now_str()}] [Cron] 昨日报表生成完成: {results}")
    except Exception as e:
        print(f"[{_now_str()}] [Cron] 昨日报表生成异常: {e}")


def task_reports_weekly():
    """每周一生成周报（周一~周日）"""
    from services.report_generator import generate_business_weekly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    yesterday = now - timedelta(days=1)
    week_start = (yesterday - timedelta(days=6)).strftime('%Y-%m-%d')
    week_end = yesterday.strftime('%Y-%m-%d')
    week_label = f"{week_start}~{week_end}"
    print(f"[{now_str}] [Cron] 开始生成周报 {week_label}...")
    try:
        result = generate_business_weekly(week_start, week_end)
        print(f"[{now_str}] [Cron] 周报生成完成: {result}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 周报生成异常: {e}")


def task_reports_monthly():
    """每月1号生成本月月报"""
    from services.report_generator import generate_business_monthly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    month_str = (now - timedelta(days=1)).strftime('%Y-%m')
    print(f"[{now_str}] [Cron] 开始生成月报 {month_str}...")
    try:
        result = generate_business_monthly(month_str)
        print(f"[{now_str}] [Cron] 月报生成完成: {result}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 月报生成异常: {e}")


# ==================== 入口 ====================

TASK_MAP = {
    'inventory': task_inventory,
    'inbound': task_inbound,
    'listing': task_listing,
    'orders-recent': task_orders_recent,
    'orders-week': task_orders_week,
    'orders-month': task_orders_month,
    'exchange-rate': task_exchange_rate,
    'reports-daily': task_reports_daily,
    'reports-weekly': task_reports_weekly,
    'reports-monthly': task_reports_monthly,
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
