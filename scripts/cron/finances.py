#!/usr/bin/env python3
"""财务明细同步（每30分钟：3~7天前订单）"""
from scripts.cron import _now_str


def run():
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


if __name__ == '__main__':
    run()
