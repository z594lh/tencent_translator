#!/usr/bin/env python3
"""
广告报表生成 — 从 amazon_ads_raw_reports 聚合生成 report_advertising

用法:
    python scripts/cron/ads_report.py --daily        生成昨日广告日报
    python scripts/cron/ads_report.py --weekly        生成上周广告周报
    python scripts/cron/ads_report.py --monthly       生成上月广告月报
    python scripts/cron/ads_report.py --date 2026-06-20  生成指定日期日报

定时建议:
    # 广告日报: 每天早上 7:00 (在 ads_sync 之后)
    crontab: 0 7 * * * cd /path/to/project && python scripts/cron/ads_report.py --daily >> log/ads_report.log 2>&1
    # 广告周报: 每周三早上 8:00
    crontab: 0 8 * * 3 cd /path/to/project && python scripts/cron/ads_report.py --weekly >> log/ads_report.log 2>&1
    # 广告月报: 每月 3 号早上 9:00
    crontab: 0 9 3 * * cd /path/to/project && python scripts/cron/ads_report.py --monthly >> log/ads_report.log 2>&1
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


def run_ads_daily(date_str=None):
    """生成单日广告效果报表"""
    from services.report_generator import generate_advertising_daily

    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"[{_now_str()}] [AdsReport] 开始生成广告日报 {date_str} ...")
    try:
        result = generate_advertising_daily(date_str)
        print(f"[{_now_str()}] [AdsReport] 广告日报完成: {result}")
    except Exception as e:
        import traceback
        print(f"[{_now_str()}] [AdsReport] 广告日报异常: {e}")
        traceback.print_exc()


def run_ads_weekly():
    """生成上周广告周报 (Mon-Sun)"""
    from services.report_generator import generate_advertising_weekly

    now = datetime.now()
    yesterday_dt = now - timedelta(days=1)
    week_start = (yesterday_dt - timedelta(days=6)).strftime('%Y-%m-%d')
    week_end = yesterday_dt.strftime('%Y-%m-%d')
    week_label = f"{week_start}~{week_end}"
    print(f"[{_now_str()}] [AdsReport] 开始生成广告周报 {week_label} ...")
    try:
        result = generate_advertising_weekly(week_start, week_end)
        print(f"[{_now_str()}] [AdsReport] 广告周报完成: {result}")
    except Exception as e:
        import traceback
        print(f"[{_now_str()}] [AdsReport] 广告周报异常: {e}")
        traceback.print_exc()


def run_ads_monthly():
    """生成上月广告月报"""
    from services.report_generator import generate_advertising_monthly

    now = datetime.now()
    month_str = (now - timedelta(days=1)).strftime('%Y-%m')
    print(f"[{_now_str()}] [AdsReport] 开始生成广告月报 {month_str} ...")
    try:
        result = generate_advertising_monthly(month_str)
        print(f"[{_now_str()}] [AdsReport] 广告月报完成: {result}")
    except Exception as e:
        import traceback
        print(f"[{_now_str()}] [AdsReport] 广告月报异常: {e}")
        traceback.print_exc()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon 广告效果报表生成')
    parser.add_argument('--daily', action='store_true', help='生成昨日广告日报')
    parser.add_argument('--weekly', action='store_true', help='生成上周广告周报')
    parser.add_argument('--monthly', action='store_true', help='生成上月广告月报')
    parser.add_argument('--date', type=str, default=None, help='指定日期 YYYY-MM-DD (仅 --daily 有效)')
    args = parser.parse_args()

    if args.daily:
        run_ads_daily(date_str=args.date)
    elif args.weekly:
        run_ads_weekly()
    elif args.monthly:
        run_ads_monthly()
    else:
        print("用法: python scripts/cron/ads_report.py --daily|--weekly|--monthly [--date YYYY-MM-DD]")
        sys.exit(1)
