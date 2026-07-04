#!/usr/bin/env python3
"""
下载已完成广告报告 + 写入 amazon_ads_raw_reports

用法:
    python scripts/cron/ads_download.py                      下载昨日待处理报告
    python scripts/cron/ads_download.py --date 2026-06-20   指定日期
    python scripts/cron/ads_download.py --start ... --end ...  日期范围
    python scripts/cron/ads_download.py --force              强制重建（先删后插）

定时: 建议每 10 分钟执行
      crontab: */10 * * * * cd /path && python scripts/cron/ads_download.py >> log/ads_download.log 2>&1
"""
import os, sys, importlib.util, argparse, time
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)
from scripts.cron import _now_str


def _load_advertising():
    path = os.path.join(PROJECT_ROOT, "blueprints", "amazon", "advertising.py")
    spec = importlib.util.spec_from_file_location("advertising", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(shop_id=None, date_str=None, force_refresh=False):
    mod = _load_advertising()
    from services.shop_service import get_all_active_shops

    shops = [{'id': shop_id}] if shop_id else get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [AdsDownload] no shops")
        return

    for shop in shops:
        sid = shop['id']
        name = shop.get('shop_name', f'Shop#{sid}')
        print(f"[{_now_str()}] [AdsDownload] shop={name} date={date_str} force={force_refresh}")

        # 1. 超时 PENDING (>2h) → 标记 EXPIRED + 立即重建
        client = mod._make_client(sid)
        expired = mod._cache_get_expired(sid)
        for e in expired:
            print(f"  [{e['report_type']}] {e['report_date']} PENDING >2h -> EXPIRED, recreating...")
            cfg = mod._SYNC_REPORT_TYPES.get(e['report_type'])
            if cfg:
                mod._cache_delete(sid, e['report_type'], e['report_date'])
                body = mod._build_report_body(cfg, e['report_type'], e['report_date'], e['report_date'])
                try:
                    new_id = client._create_async_report(body)
                    mod._report_cache_set(sid, e['report_type'], e['report_date'], new_id, "PENDING")
                    print(f"    -> recreated {new_id[:20]}...")
                except Exception as ex:
                    print(f"    -> FAIL: {ex}")

        # 2. 下载已完成的报告
        result = mod.download_ads_reports(sid, date_str, force_refresh=force_refresh)
        print(f"  downloaded={len(result['downloaded'])} pending={len(result['pending'])} errors={len(result['errors'])}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='广告报告下载')
    parser.add_argument('--shop', type=int, default=None)
    parser.add_argument('--date', type=str, default=None, help='YYYY-MM-DD (默认昨天)')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--force', action='store_true', help='强制重建（先删旧数据再写入）')

    args = parser.parse_args()

    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d")
        end = datetime.strptime(args.end, "%Y-%m-%d")
        d = start
        while d <= end:
            run(shop_id=args.shop, date_str=d.strftime("%Y-%m-%d"), force_refresh=args.force)
            d += timedelta(days=1)
    else:
        date_str = args.date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        run(shop_id=args.shop, date_str=date_str, force_refresh=args.force)
