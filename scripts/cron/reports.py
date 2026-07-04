#!/usr/bin/env python3
"""
经营日报 + 广告日报 + SKU利润生成
周报/月报改为前端实时聚合日报数据，不再预生成。

用法: python scripts/cron/reports.py --daily
定时: 建议每天下午 16:30
      crontab: 30 16 * * * python scripts/cron/reports.py --daily >> log/cron_reports.log 2>&1
"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from datetime import datetime, timedelta
from scripts.cron import _now_str


def run_daily():
    from services.report_generator import generate_business_daily, generate_inventory_turnover
    now_str = _now_str()
    today = datetime.now().date()

    print(f"[{now_str}] [Cron] 开始生成最近10天日报...")

    for i in range(1, 11):
        report_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        try:
            generate_business_daily(report_date)
        except Exception as e:
            print(f"[{now_str}] [Cron] 经营日报 {report_date} 异常: {e}")

    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        generate_inventory_turnover()
    except Exception as e:
        print(f"[{now_str}] [Cron] 库存周转异常: {e}")

    print(f"[{now_str}] [Cron] 日报生成完成")


if __name__ == '__main__':
    if len(sys.argv) < 2 or sys.argv[1] != '--daily':
        print("用法: python scripts/cron/reports.py --daily")
        sys.exit(1)
    run_daily()
