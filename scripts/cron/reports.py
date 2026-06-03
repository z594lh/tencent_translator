#!/usr/bin/env python3
"""
报表生成
  用法: python scripts/cron/reports.py --daily      日报（每天凌晨2点）
        python scripts/cron/reports.py --weekly     周报（每周三凌晨3点）
        python scripts/cron/reports.py --monthly    月报（每月3号凌晨4点）
"""
import sys
from datetime import datetime, timedelta
from scripts.cron import _now_str


def run_daily():
    from services.report_generator import generate_yesterday_reports
    print(f"[{_now_str()}] [Cron] 开始生成报表（T-1/T-2 estimated + T-3 settled）...")
    try:
        results = generate_yesterday_reports()
        print(f"[{_now_str()}] [Cron] 报表生成完成: {list(results.keys())}")
    except Exception as e:
        import traceback
        print(f"[{_now_str()}] [Cron] 报表生成异常: {e}")
        traceback.print_exc()


def run_weekly():
    from services.report_generator import generate_business_weekly, generate_advertising_weekly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    yesterday_dt = now - timedelta(days=1)
    week_start = (yesterday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    week_end = yesterday_dt.strftime('%Y-%m-%d')
    week_label = f"{week_start}~{week_end}"
    print(f"[{now_str}] [Cron] 开始生成周报 {week_label}...")
    try:
        result_biz = generate_business_weekly(week_start, week_end)
        result_ad = generate_advertising_weekly(week_start, week_end)
        print(f"[{now_str}] [Cron] 周报生成完成: biz={result_biz}, ad={result_ad}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 周报生成异常: {e}")


def run_monthly():
    from services.report_generator import generate_business_monthly, generate_advertising_monthly
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    month_str = (now - timedelta(days=1)).strftime('%Y-%m')
    print(f"[{now_str}] [Cron] 开始生成月报 {month_str}...")
    try:
        result_biz = generate_business_monthly(month_str)
        result_ad = generate_advertising_monthly(month_str)
        print(f"[{now_str}] [Cron] 月报生成完成: biz={result_biz}, ad={result_ad}")
    except Exception as e:
        print(f"[{now_str}] [Cron] 月报生成异常: {e}")


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
