#!/usr/bin/env python3
"""
同步 FBA 月度仓储费 (GET_FBA_STORAGE_FEE_CHARGES_DATA) 到 amazon_fba_storage_fees

用法:
    python scripts/cron/storage_fee_sync.py            同步所有启用店铺
    python scripts/cron/storage_fee_sync.py --shop 1   仅同步指定店铺
    python scripts/cron/storage_fee_sync.py --start 2026-05-01 --end 2026-08-01  指定数据范围

定时: 仓储费每月约 7 号出上月的账，建议每月 8 号执行一次
      crontab: 0 8 8 * * cd /path && python scripts/cron/storage_fee_sync.py >> log/storage_fee_sync.log 2>&1
"""
import os
import sys
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)
from scripts.cron import _now_str


def run(shop_id=None, start_date=None, end_date=None):
    from services.fba_storage_fee_service import sync_storage_fees, sync_all_storage_fees

    if shop_id:
        result = sync_storage_fees(shop_id, start_date=start_date, end_date=end_date)
        print(f"[{_now_str()}] [StorageFee] shop={shop_id} 完成: {result}")
    else:
        results = sync_all_storage_fees(start_date=start_date, end_date=end_date)
        for sid, r in results.items():
            status = "OK" if "error" not in r else f"FAIL {r['error']}"
            print(f"[{_now_str()}] [StorageFee] shop={sid} {status}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FBA 月度仓储费同步')
    parser.add_argument('--shop', type=int, default=None)
    parser.add_argument('--start', type=str, default=None, help='YYYY-MM-DD')
    parser.add_argument('--end', type=str, default=None, help='YYYY-MM-DD')
    args = parser.parse_args()
    run(shop_id=args.shop, start_date=args.start, end_date=args.end)
