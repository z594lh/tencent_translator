#!/usr/bin/env python3
"""
报表生成
  用法: python scripts/cron/reports.py --daily       日报（每天凌晨2点，最近10天）
        python scripts/cron/reports.py --weekly      周报（每周三凌晨3点，最近3周）
        python scripts/cron/reports.py --monthly     月报（每月3号凌晨4点，最近2个月）
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
    from services.report_generator import generate_business_daily, generate_advertising_daily, generate_sku_profit, generate_inventory_turnover
    now_str = _now_str()
    today = datetime.now().date()

    print(f"[{now_str}] [Cron] 开始生成最近10天日报...")
    results = {}

    for i in range(1, 11):
        report_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        try:
            results[f'business_daily_{report_date}'] = generate_business_daily(report_date)
        except Exception as e:
            print(f"[{now_str}] [Cron] 日报 {report_date} 异常: {e}")

    yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        results['sku_profit'] = generate_sku_profit(yesterday, yesterday)
        results['inventory_turnover'] = generate_inventory_turnover()
    except Exception as e:
        print(f"[{now_str}] [Cron] SKU利润/库存周转异常: {e}")

    for i in range(1, 11):
        report_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        try:
            results[f'advertising_daily_{report_date}'] = generate_advertising_daily(report_date)
        except Exception as e:
            print(f"[{now_str}] [Cron] 广告日报 {report_date} 异常: {e}")

    print(f"[{now_str}] [Cron] 日报生成完成: {list(results.keys())}")


def run_weekly():
    from services.report_generator import generate_business_weekly, generate_advertising_weekly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    today = now.date()

    print(f"[{now_str}] [Cron] 开始生成最近3周周报...")

    for week_offset in range(3):
        sunday_dt = today - timedelta(days=today.weekday() + 1 + week_offset * 7)
        week_start = (sunday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
        week_end = sunday_dt.strftime('%Y-%m-%d')
        week_label = f"{week_start}~{week_end}"
        print(f"[{now_str}] [Cron] 生成周报 {week_label}...")
        try:
            result_biz = generate_business_weekly(week_start, week_end)
            result_ad = generate_advertising_weekly(week_start, week_end)
            print(f"[{now_str}] [Cron] 周报 {week_label} 完成: biz={result_biz}, ad={result_ad}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 周报 {week_label} 异常: {e}")


def run_monthly():
    from services.report_generator import generate_business_monthly, generate_advertising_monthly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')

    print(f"[{now_str}] [Cron] 开始生成最近2个月月报...")

    for month_offset in range(2):
        month_dt = now.replace(day=1) - timedelta(days=1 + month_offset * 31)
        month_str = month_dt.strftime('%Y-%m')
        print(f"[{now_str}] [Cron] 生成月报 {month_str}...")
        try:
            result_biz = generate_business_monthly(month_str)
            result_ad = generate_advertising_monthly(month_str)
            print(f"[{now_str}] [Cron] 月报 {month_str} 完成: biz={result_biz}, ad={result_ad}")
        except Exception as e:
            print(f"[{now_str}] [Cron] 月报 {month_str} 异常: {e}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python scripts/cron/reports.py --daily|--weekly|--monthly")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == '--daily':
        run_daily()
    elif mode == '--weekly':
        run_weekly()
    elif mode == '--monthly':
        run_monthly()
    else:
        print(f"未知模式: {mode}，请使用 --daily / --weekly / --monthly")
        sys.exit(1)
