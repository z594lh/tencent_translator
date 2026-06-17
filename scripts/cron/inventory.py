#!/usr/bin/env python3
"""库存同步（每小时）"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str
from services.shop_service import get_all_active_shops
import services.notification_handlers


def run():
    from blueprints.amazon.inventory import _sync_inventory
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过库存同步")
        return

    print(f"[{_now_str()}] [Cron] 开始库存同步，共 {len(shops)} 个店铺...")
    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        try:
            result = _sync_inventory(shop_id=shop['id'])
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 库存同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 库存同步异常: {e}")


if __name__ == '__main__':
    run()
