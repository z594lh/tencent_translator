#!/usr/bin/env python3
"""
经营日报 + SKU利润 + 库存周转生成

用法:
    python scripts/cron/reports.py --daily                         最近10天
    python scripts/cron/reports.py --daily --start 2026-06-22 --end 2026-07-02  指定日期范围

定时: 建议每天下午 16:30
      crontab: 30 16 * * * python scripts/cron/reports.py --daily >> log/cron_reports.log 2>&1
"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

import argparse
from datetime import datetime, timedelta
from scripts.cron import _now_str


def run_daily(start=None, end=None):
    from services.report_generator import generate_business_daily, generate_sku_sales, generate_inventory_turnover
    now_str = _now_str()
    today = datetime.now().date()

    if start and end:
        dates = []
        d = datetime.strptime(start, '%Y-%m-%d').date()
        e = datetime.strptime(end, '%Y-%m-%d').date()
        while d <= e:
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        print(f"[{now_str}] [Cron] 生成 {start} ~ {end} 日报, 共 {len(dates)} 天...")
    else:
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 11)]
        print(f"[{now_str}] [Cron] 开始生成最近10天日报...")

    for report_date in dates:
        try:
            generate_business_daily(report_date)
        except Exception as e:
            print(f"[{now_str}] [Cron] 经营日报 {report_date} 异常: {e}")

    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        generate_sku_sales(today.strftime('%Y-%m-%d'))
        print(f"[{now_str}] [Cron] SKU销售汇总 {today} 完成")
    except Exception as e:
        print(f"[{now_str}] [Cron] SKU销售汇总异常: {e}")

    try:
        generate_inventory_turnover()
    except Exception as e:
        print(f"[{now_str}] [Cron] 库存周转异常: {e}")

    print(f"[{now_str}] [Cron] 日报生成完成")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='经营日报+SKU利润生成')
    parser.add_argument('--daily', action='store_true')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    args = parser.parse_args()
    if not args.daily:
        print("用法: python scripts/cron/reports.py --daily [--start YYYY-MM-DD --end YYYY-MM-DD]")
        sys.exit(1)
    run_daily(start=args.start, end=args.end)
