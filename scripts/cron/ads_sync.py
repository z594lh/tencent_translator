#!/usr/bin/env python3
"""
广告数据同步 — 从 Amazon Ads API 拉取全量报告字段写入 amazon_ads_raw_reports

用法:
    python scripts/cron/ads_sync.py                     拉取所有店铺昨天数据
    python scripts/cron/ads_sync.py --shop 1             仅拉取店铺1昨天数据
    python scripts/cron/ads_sync.py --date 2026-06-20    拉取所有店铺指定日期
    python scripts/cron/ads_sync.py --shop 1 --date 2026-06-20  拉取店铺1指定日期

定时: 建议每天早上 6:00 (UTC+8) 执行 (Ads API 报告通常 T+0 可用, 但偶有延迟)
      crontab: 0 6 * * * cd /path/to/project && python scripts/cron/ads_sync.py >> log/ads_sync.log 2>&1
"""
import os
import sys
import importlib.util

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

import argparse
from datetime import datetime, timedelta
from scripts.cron import _now_str


def run_ads_sync(shop_id=None, date_str=None):
    """调用 advertising 蓝图的 run_ads_sync，按店铺拉取全量广告报告"""
    path = os.path.join(PROJECT_ROOT, "blueprints", "amazon", "advertising.py")
    spec = importlib.util.spec_from_file_location("advertising", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _sync = mod.run_ads_sync

    from services.shop_service import get_all_active_shops

    if date_str is None:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    if shop_id is not None:
        shops = [{'id': shop_id, 'shop_name': f'Shop #{shop_id}'}]
    else:
        shops = get_all_active_shops()
        if not shops:
            print(f"[{_now_str()}] [AdsSync] 没有启用的店铺，退出")
            return

    total_rows = 0
    errors = []

    for shop in shops:
        sid = shop['id']
        name = shop.get('shop_name', f'Shop#{sid}')
        print(f"[{_now_str()}] [AdsSync] 开始同步店铺 {name} (id={sid}) date={date_str} ...")
        try:
            result = _sync(sid, date_str)
            rows = result.get('total_rows', 0)
            total_rows += rows
            if result.get('error'):
                errors.append({'shop_id': sid, 'error': result['error']})
            for r in result.get('results', []):
                if r.get('error'):
                    print(f"  [{r['report_type']}] FAIL: {r['error']}")
                else:
                    print(f"  [{r['report_type']}] OK: {r['rows']} rows ({r['inserted']} new, {r['updated']} updated)")
        except Exception as e:
            import traceback
            msg = f"{e}"
            print(f"[{_now_str()}] [AdsSync] 店铺 {sid} 同步异常: {msg}")
            traceback.print_exc()
            errors.append({'shop_id': sid, 'error': msg})

    print(f"[{_now_str()}] [AdsSync] 完成: {len(shops)} 店铺, {total_rows} 行, {len(errors)} 异常")
    return {'shops': len(shops), 'total_rows': total_rows, 'errors': errors}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon Ads 报告数据同步')
    parser.add_argument('--shop', type=int, default=None, help='指定店铺 ID (不传则全部)')
    parser.add_argument('--date', type=str, default=None, help='指定日期 YYYY-MM-DD (默认昨天)')
    args = parser.parse_args()
    run_ads_sync(shop_id=args.shop, date_str=args.date)
