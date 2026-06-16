#!/usr/bin/env python3
"""Listing 同步（每3小时）"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str
from services.shop_service import get_all_active_shops


def run():
    from blueprints.amazon.listing import _sync_listings
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过 Listing 同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']
        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始 Listing 同步...")
        try:
            result = _sync_listings(
                shop_id=shop_id,
                included_data=["summaries", "attributes", "issues"],
                page_size=20,
                sync_products_async=False
            )
            err_msg = f", error={result['error']}" if result.get('error') else ''
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] Listing 同步完成: synced={result.get('synced_count', 0)}, fetched={result.get('total_fetched', 0)}{err_msg}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] Listing 同步异常: {e}")


if __name__ == '__main__':
    run()
