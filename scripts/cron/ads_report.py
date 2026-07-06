#!/usr/bin/env python3
"""
广告日报生成 — 从 amazon_ads_raw_reports 聚合生成 report_advertising

用法:
    python scripts/cron/ads_report.py --daily                        生成昨日
    python scripts/cron/ads_report.py --daily --date 2026-06-20     指定日期
    python scripts/cron/ads_report.py --daily --start 2026-06-22 --end 2026-07-02  日期范围

定时: 建议每天下午 16:10
      crontab: 10 16 * * * python scripts/cron/ads_report.py --daily >> log/ads_report.log 2>&1
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


def run_ads_daily(date_str=None, start=None, end=None):
    from services.report_generator import generate_advertising_daily

    if start and end:
        dates = []
        d = datetime.strptime(start, '%Y-%m-%d')
        e = datetime.strptime(end, '%Y-%m-%d')
        while d <= e:
            dates.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)
        print(f"[{_now_str()}] [AdsReport] 生成 {start} ~ {end} 广告日报, 共 {len(dates)} 天...")
    else:
        if date_str is None:
            date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        dates = [date_str]

    for report_date in dates:
        print(f"[{_now_str()}] [AdsReport] 广告日报 {report_date} ...")
        try:
            result = generate_advertising_daily(report_date)
            print(f"[{_now_str()}] [AdsReport] 广告日报 {report_date} 完成: {result}")
        except Exception as e:
            import traceback
            print(f"[{_now_str()}] [AdsReport] 广告日报 {report_date} 异常: {e}")
            traceback.print_exc()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon 广告日报生成')
    parser.add_argument('--daily', action='store_true', help='生成广告日报')
    parser.add_argument('--date', type=str, default=None, help='指定日期 YYYY-MM-DD')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    args = parser.parse_args()

    if args.daily:
        run_ads_daily(date_str=args.date, start=args.start, end=args.end)
    else:
        print("用法: python scripts/cron/ads_report.py --daily [--date YYYY-MM-DD | --start YYYY-MM-DD --end YYYY-MM-DD]")
        sys.exit(1)
