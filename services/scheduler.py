"""
APScheduler 定时任务调度器（多店铺支持版）
统一存放所有后台定时同步任务，避免 app.py 臃肿
"""
import os
import time
import requests
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from services.mysql_service import get_db_connection
from services.shop_service import get_all_active_shops

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


# ==================== 汇率同步 ====================

def fetch_and_save_exchange_rate(from_currency='CNY', to_currency='USD'):
    """
    从 open.er-api.com 获取最新汇率并写入数据库
    返回: {'rate': float, 'updated_at': str} 或 None
    """
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


def start_scheduler():
    """启动所有定时任务并返回 scheduler 实例"""
    from blueprints.amazon.inventory import _sync_inventory
    from blueprints.amazon.inbound_plans import (
        _sync_inbound_plans,
        _sync_all_inbound_plan_boxes,
        _sync_all_inbound_plan_shipments,
        _sync_inbound_plan_shipments_all,
        _get_inbound_plan_ids,
    )

    scheduler = BackgroundScheduler()

    # ==================== 1. 库存同步：每小时（遍历所有店铺）====================
    def job_inventory():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        shops = get_all_active_shops()
        if not shops:
            print(f"[{now}] [Scheduler] 没有启用的店铺，跳过库存同步")
            return

        print(f"[{now}] [Scheduler] 开始库存同步，共 {len(shops)} 个店铺...")
        for shop in shops:
            shop_name = shop.get('shop_name', f"shop_{shop['id']}")
            try:
                result = _sync_inventory(shop_id=shop['id'])
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 库存同步完成: {result}")
            except Exception as e:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 库存同步异常: {e}")

    # ==================== 2. 入库计划 + 箱子 + 货件 + 货件详情：每3小时（遍历所有店铺）====================
    def job_inbound():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        shops = get_all_active_shops()
        if not shops:
            print(f"[{now}] [Scheduler] 没有启用的店铺，跳过入库计划同步")
            return

        for shop in shops:
            shop_name = shop.get('shop_name', f"shop_{shop['id']}")
            shop_id = shop['id']

            # 1. 同步入库计划列表
            print(f"[{now}] [Scheduler] 店铺[{shop_name}] 开始入库计划同步...")
            try:
                plans_result = _sync_inbound_plans(shop_id=shop_id, status='ACTIVE')
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划同步完成: {plans_result}")
            except Exception as e:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划同步异常: {e}")
                continue

            if plans_result.get('synced_count', 0) <= 0:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 没有入库计划，跳过后续同步")
                continue

            # 2. 同步箱子
            print(f"[{now}] [Scheduler] 店铺[{shop_name}] 开始入库计划箱子同步...")
            try:
                result = _sync_all_inbound_plan_boxes(shop_id=shop_id, status='ACTIVE')
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划箱子同步完成: {result}")
            except Exception as e:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划箱子同步异常: {e}")

            # 3. 同步货件列表
            print(f"[{now}] [Scheduler] 店铺[{shop_name}] 开始入库计划货件同步...")
            try:
                result = _sync_all_inbound_plan_shipments(shop_id=shop_id, status='ACTIVE')
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划货件同步完成: {result}")
            except Exception as e:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划货件同步异常: {e}")
                continue

            # 4. 同步货件详情
            plan_ids = _get_inbound_plan_ids(shop_id=shop_id, status='ACTIVE')
            if not plan_ids:
                print(f"[{now}] [Scheduler] 店铺[{shop_name}] 没有 ACTIVE 入库计划，跳过货件详情同步")
                continue

            print(f"[{now}] [Scheduler] 店铺[{shop_name}] 开始入库计划货件详情同步，共 {len(plan_ids)} 个计划...")
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

            print(f"[{now}] [Scheduler] 店铺[{shop_name}] 入库计划货件详情同步完成: {total_detail_synced} 条"
                  f"{f', 错误: {detail_errors}' if detail_errors else ''}")

    # ==================== 4. 订单同步（三层梯度，遍历所有店铺）====================
    from blueprints.amazon.orders import _sync_orders, _sync_order_items

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

    # 任务4a：每15分钟，同步最近24小时内有更新的订单（列表+商品）
    def job_orders_recent():
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        shops = get_all_active_shops()
        if not shops:
            print(f"[{now_str}] [Scheduler] 没有启用的店铺，跳过订单同步")
            return

        for shop in shops:
            shop_name = shop.get('shop_name', f"shop_{shop['id']}")
            shop_id = shop['id']
            print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 开始近期订单同步(24h)...")
            try:
                last_updated_after = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
                result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 近期订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")

                order_ids = _get_recent_order_ids(shop_id=shop_id, hours=24)
                if order_ids:
                    print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 开始同步近期订单商品，共 {len(order_ids)} 单...")
                    items_total, items_errors = _sync_order_items_batch(shop_id=shop_id, order_ids=order_ids, label="近期")
                    print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 近期订单商品同步完成: {items_total} 条"
                          f"{f', 错误: {len(items_errors)}个' if items_errors else ''}")
            except Exception as e:
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 近期订单同步异常: {e}")

    # 任务4b：每3小时，同步最近7天内有更新的订单（列表+商品）
    def job_orders_week():
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        shops = get_all_active_shops()
        if not shops:
            print(f"[{now_str}] [Scheduler] 没有启用的店铺，跳过订单同步")
            return

        for shop in shops:
            shop_name = shop.get('shop_name', f"shop_{shop['id']}")
            shop_id = shop['id']
            print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 开始本周订单同步(7d)...")
            try:
                last_updated_after = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
                result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 本周订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")

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
                    print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 开始同步本周订单商品(24h~7d)，共 {len(order_ids)} 单...")
                    items_total, items_errors = _sync_order_items_batch(shop_id=shop_id, order_ids=order_ids, label="本周")
                    print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 本周订单商品同步完成: {items_total} 条"
                          f"{f', 错误: {len(items_errors)}个' if items_errors else ''}")
            except Exception as e:
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 本周订单同步异常: {e}")

    # 任务4c：每6小时，同步最近30天内有更新的订单（仅列表，不抓商品）
    def job_orders_month():
        now = datetime.now()
        now_str = now.strftime('%Y-%m-%d %H:%M:%S')
        shops = get_all_active_shops()
        if not shops:
            print(f"[{now_str}] [Scheduler] 没有启用的店铺，跳过订单同步")
            return

        for shop in shops:
            shop_name = shop.get('shop_name', f"shop_{shop['id']}")
            shop_id = shop['id']
            print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 开始本月订单同步(30d,仅列表)...")
            try:
                last_updated_after = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
                result = _sync_orders(shop_id=shop_id, last_updated_after=last_updated_after)
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 本月订单列表同步完成: fetched={result.get('total_fetched', 0)}, synced={result.get('synced_count', 0)}")
            except Exception as e:
                print(f"[{now_str}] [Scheduler] 店铺[{shop_name}] 本月订单同步异常: {e}")

    # ==================== 5. 汇率同步：每天上午 9 点 ====================
    def job_exchange_rate():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now}] [Scheduler] 开始汇率同步...")
        try:
            result = fetch_and_save_exchange_rate('CNY', 'USD')
            if result:
                print(f"[{now}] [Scheduler] 汇率同步完成: {result}")
            else:
                print(f"[{now}] [Scheduler] 汇率同步失败，请检查日志")
        except Exception as e:
            print(f"[{now}] [Scheduler] 汇率同步异常: {e}")

    # 注册任务
    scheduler.add_job(job_inventory, 'cron', minute=0, id='inventory_hourly', replace_existing=True)
    scheduler.add_job(job_inbound, 'cron', hour='0,3,6,9,12,15,18,21', minute=15, id='inbound_3h', replace_existing=True)
    scheduler.add_job(job_orders_recent, 'cron', minute='0,15,30,45', id='orders_recent_15m', replace_existing=True)
    scheduler.add_job(job_orders_week, 'cron', hour='1,4,7,10,13,16,19,22', minute=10, id='orders_week_3h', replace_existing=True)
    scheduler.add_job(job_orders_month, 'cron', hour='2,8,14,20', minute=20, id='orders_month_6h', replace_existing=True)
    scheduler.add_job(job_exchange_rate, 'cron', hour=9, minute=0, id='exchange_rate_daily', replace_existing=True)

    scheduler.start()
    return scheduler
