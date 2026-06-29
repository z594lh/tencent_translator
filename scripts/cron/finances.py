#!/usr/bin/env python3
"""财务明细同步（每天：通过日期范围拉取最近30天财务信息）"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str


def run():
    from blueprints.amazon.finances import sync_finances_date_range
    now_str = _now_str()
    print(f"[{now_str}] [Cron] 开始财务明细同步(最近30天，日期范围)...")
    try:
        results = sync_finances_date_range(days=30)
        total_fetched = sum(r.get('transactions_fetched', 0) for r in results.values())
        total_saved = sum(r.get('saved', 0) for r in results.values())
        print(f"[{now_str}] [Cron] 财务明细同步完成: 店铺={len(results)}, 拉取={total_fetched}, 入库={total_saved}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 财务明细同步异常: {e}")


if __name__ == '__main__':
    run()
