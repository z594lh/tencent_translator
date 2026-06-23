#!/usr/bin/env python3
"""
订单同步
  用法: python scripts/cron/orders.py --recent    近期24h（每15分钟）
        python scripts/cron/orders.py --week      本周7d（每3小时）
        python scripts/cron/orders.py --month     本月30d（每6小时，仅列表）
"""
import os
import sys
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from datetime import datetime, timedelta
from scripts.cron import _now_str
from services.mysql_service import get_db_connection
from services.shop_service import get_all_active_shops
import services.notification_handlers


def _get_recent_order_ids(shop_id, hours):
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


def run_recent():
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
            if result.get('error'):
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单列表同步错误: {result['error']}")

            order_ids = _get_recent_order_ids(shop_id=shop_id, hours=24)
            if order_ids:
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 开始同步近期订单商品，共 {len(order_ids)} 单...")
                items_total, items_errors = _sync_order_items_batch(shop_id=shop_id, order_ids=order_ids, label="近期")
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单商品同步完成: {items_total} 条"
                      f"{f', 错误: {len(items_errors)}个' if items_errors else ''}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 近期订单同步异常: {e}")


def run_week():
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
            if result.get('error'):
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本周订单列表同步错误: {result['error']}")

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


def run_month():
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
            if result.get('error'):
                print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本月订单列表同步错误: {result['error']}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 店铺[{shop_name}] 本月订单同步异常: {e}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python scripts/cron/orders.py --recent|--week|--month")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == '--recent':
        run_recent()
    elif mode == '--week':
        run_week()
    elif mode == '--month':
        run_month()
    else:
        print(f"未知模式: {mode}，请使用 --recent / --week / --month")
        sys.exit(1)
