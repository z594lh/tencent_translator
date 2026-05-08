"""
APScheduler 定时任务调度器
统一存放所有后台定时同步任务，避免 app.py 臃肿
"""
import requests
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from services.mysql_service import get_db_connection


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

    # ==================== 1. 库存同步：每小时 ====================
    def job_inventory():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now}] [Scheduler] 开始库存同步...")
        try:
            result = _sync_inventory()
            print(f"[{now}] [Scheduler] 库存同步完成: {result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 库存同步异常: {e}")

    # ==================== 2. 入库计划 + 箱子 + 货件 + 货件详情：每3小时（ACTIVE）====================
    def job_inbound():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 1. 同步入库计划列表
        print(f"[{now}] [Scheduler] 开始入库计划同步...")
        try:
            plans_result = _sync_inbound_plans(status='ACTIVE')
            print(f"[{now}] [Scheduler] 入库计划同步完成: {plans_result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 入库计划同步异常: {e}")
            return

        if plans_result.get('synced_count', 0) <= 0:
            print(f"[{now}] [Scheduler] 没有入库计划，跳过后续同步")
            return

        # 2. 同步箱子
        print(f"[{now}] [Scheduler] 开始入库计划箱子同步...")
        try:
            result = _sync_all_inbound_plan_boxes(status='ACTIVE')
            print(f"[{now}] [Scheduler] 入库计划箱子同步完成: {result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 入库计划箱子同步异常: {e}")

        # 3. 同步货件列表
        print(f"[{now}] [Scheduler] 开始入库计划货件同步...")
        try:
            result = _sync_all_inbound_plan_shipments(status='ACTIVE')
            print(f"[{now}] [Scheduler] 入库计划货件同步完成: {result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 入库计划货件同步异常: {e}")
            return

        # 4. 同步货件详情
        plan_ids = _get_inbound_plan_ids(status='ACTIVE')
        if not plan_ids:
            print(f"[{now}] [Scheduler] 没有 ACTIVE 入库计划，跳过货件详情同步")
            return

        print(f"[{now}] [Scheduler] 开始入库计划货件详情同步，共 {len(plan_ids)} 个计划...")
        total_detail_synced = 0
        detail_errors = []
        for plan_id in plan_ids:
            try:
                result = _sync_inbound_plan_shipments_all(plan_id)
                total_detail_synced += result.get('details_synced_count', 0)
                if result.get('errors'):
                    detail_errors.extend(result['errors'])
            except Exception as e:
                detail_errors.append({"plan_id": plan_id, "error": str(e)})

        print(f"[{now}] [Scheduler] 入库计划货件详情同步完成: {total_detail_synced} 条"
              f"{f', 错误: {detail_errors}' if detail_errors else ''}")

    # ==================== 4. 汇率同步：每天上午 9 点 ====================
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
    scheduler.add_job(job_exchange_rate, 'cron', hour=9, minute=0, id='exchange_rate_daily', replace_existing=True)

    scheduler.start()
    return scheduler
