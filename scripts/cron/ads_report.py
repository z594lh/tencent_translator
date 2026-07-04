#!/usr/bin/env python3
"""
广告日报生成 — 从 amazon_ads_raw_reports 聚合生成 report_advertising
周报/月报改为前端实时聚合日报数据，不再预生成。

用法:
    python scripts/cron/ads_report.py --daily                     生成昨日广告日报
    python scripts/cron/ads_report.py --daily --date 2026-06-20  指定日期

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


def run_ads_daily(date_str=None):
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon 广告日报生成')
    parser.add_argument('--daily', action='store_true', help='生成广告日报')
    parser.add_argument('--date', type=str, default=None, help='指定日期 YYYY-MM-DD')
    args = parser.parse_args()

    if args.daily:
        run_ads_daily(date_str=args.date)
    else:
        print("用法: python scripts/cron/ads_report.py --daily [--date YYYY-MM-DD]")
        sys.exit(1)
