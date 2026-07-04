#!/usr/bin/env python3
"""
创建广告报告（仅创建+缓存，不等待不下载）

用法:
    python scripts/cron/ads_create.py                      创建昨日报告
    python scripts/cron/ads_create.py --date 2026-06-20    指定日期
    python scripts/cron/ads_create.py --start 2026-06-20 --end 2026-06-25  日期范围
    python scripts/cron/ads_create.py --force              强制重建（清除旧缓存）

定时: 建议每天早上 6:00 执行
      crontab: 0 6 * * * cd /path && python scripts/cron/ads_create.py >> log/ads_create.log 2>&1
"""
import os, sys, importlib.util, argparse
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
        print(f"[{_now_str()}] [AdsCreate] no shops")
        return

    for shop in shops:
        sid = shop['id']
        name = shop.get('shop_name', f'Shop#{sid}')
        print(f"[{_now_str()}] [AdsCreate] shop={name} date={date_str} force={force_refresh}")

        # 1. 保底：处理过期报告（download 已即时重建，这里只是兜底）
        expired = mod._cache_get_expired(sid)
        for e in expired:
            rt = e['report_type']
            rd = str(e['report_date'])
            print(f"  [{rt}] {rd} EXPIRED (safety net), recreating...")
            cfg = mod._SYNC_REPORT_TYPES.get(rt)
            if cfg:
                mod._cache_delete(sid, rt, rd)
                body = mod._build_report_body(cfg, rt, rd, rd)
                try:
                    new_id = mod._make_client(sid)._create_async_report(body)
                    mod._report_cache_set(sid, rt, rd, new_id, "PENDING")
                    print(f"    -> recreated {new_id[:20]}...")
                except Exception as ex:
                    print(f"    -> FAIL: {ex}")

        # 2. 创建目标日期的报告
        result = mod.create_ads_reports(sid, date_str, force_refresh=force_refresh)
        print(f"  created={len(result['created'])} skipped={len(result['skipped'])} errors={len(result['errors'])}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='广告报告创建')
    parser.add_argument('--shop', type=int, default=None)
    parser.add_argument('--date', type=str, default=None, help='YYYY-MM-DD (默认昨天)')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--force', action='store_true', help='强制重建')

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
