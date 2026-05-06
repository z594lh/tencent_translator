"""
APScheduler 定时任务调度器
统一存放所有后台定时同步任务，避免 app.py 臃肿
"""
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


def start_scheduler():
    """启动所有定时任务并返回 scheduler 实例"""
    from blueprints.amazon.inventory import _sync_inventory
    from blueprints.amazon.shipments import _sync_all_shipments, _sync_all_shipment_items
    from blueprints.amazon.inbound_plans import _sync_inbound_plans, _sync_all_inbound_plan_boxes

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

    # ==================== 2. 货件 + 货件商品：每3小时（15天内）====================
    def job_shipments():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        last_update_after = (datetime.utcnow() - timedelta(days=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
        default_statuses = ['WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED', 'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN']

        print(f"[{now}] [Scheduler] 开始货件同步...")
        try:
            shipments_result = _sync_all_shipments(
                shipment_status_list=default_statuses,
                last_update_after=last_update_after
            )
            print(f"[{now}] [Scheduler] 货件同步完成: {shipments_result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 货件同步异常: {e}")
            return

        shipment_ids = shipments_result.get('shipment_ids', [])
        if not shipment_ids:
            print(f"[{now}] [Scheduler] 没有货件ID，跳过货件商品同步")
            return

        print(f"[{now}] [Scheduler] 开始货件商品同步...")
        try:
            result = _sync_all_shipment_items(shipment_ids)
            print(f"[{now}] [Scheduler] 货件商品同步完成: {result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 货件商品同步异常: {e}")

    # ==================== 3. 入库计划 + 箱子：每3小时（ACTIVE）====================
    def job_inbound():
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print(f"[{now}] [Scheduler] 开始入库计划同步...")
        try:
            plans_result = _sync_inbound_plans(status='ACTIVE')
            print(f"[{now}] [Scheduler] 入库计划同步完成: {plans_result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 入库计划同步异常: {e}")
            return

        if plans_result.get('synced_count', 0) <= 0:
            print(f"[{now}] [Scheduler] 没有入库计划，跳过箱子同步")
            return

        print(f"[{now}] [Scheduler] 开始入库计划箱子同步...")
        try:
            result = _sync_all_inbound_plan_boxes(status='ACTIVE')
            print(f"[{now}] [Scheduler] 入库计划箱子同步完成: {result}")
        except Exception as e:
            print(f"[{now}] [Scheduler] 入库计划箱子同步异常: {e}")

    # 注册任务
    scheduler.add_job(job_inventory, 'cron', minute=0, id='inventory_hourly', replace_existing=True)
    scheduler.add_job(job_shipments, 'cron', hour='0,3,6,9,12,15,18,21', minute=0, id='shipments_3h', replace_existing=True)
    scheduler.add_job(job_inbound, 'cron', hour='0,3,6,9,12,15,18,21', minute=15, id='inbound_3h', replace_existing=True)

    scheduler.start()
    return scheduler
