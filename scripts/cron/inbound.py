#!/usr/bin/env python3
"""
入库计划同步
  用法: python scripts/cron/inbound.py --30min    每30分钟
        python scripts/cron/inbound.py --6h       每6小时
"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str
from services.shop_service import get_all_active_shops


def run_30min():
    from blueprints.amazon.inbound_plans import (
        _sync_inbound_plans,
        _sync_all_inbound_plan_shipments,
    )
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过入库计划同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划同步...")
        try:
            plans_result = _sync_inbound_plans(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划同步完成: {plans_result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划同步异常: {e}")
            continue

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件同步...")
        try:
            result = _sync_all_inbound_plan_shipments(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件同步异常: {e}")


def run_6h():
    from blueprints.amazon.inbound_plans import (
        _sync_all_inbound_plan_boxes,
        _sync_all_inbound_shipment_details,
    )
    shops = get_all_active_shops()
    if not shops:
        print(f"[{_now_str()}] [Cron] 没有启用的店铺，跳过入库计划深度同步")
        return

    for shop in shops:
        shop_name = shop.get('shop_name', f"shop_{shop['id']}")
        shop_id = shop['id']

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划箱子同步...")
        try:
            result = _sync_all_inbound_plan_boxes(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划箱子同步异常: {e}")

        print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 开始入库计划货件详情同步...")
        try:
            result = _sync_all_inbound_shipment_details(shop_id=shop_id, status='ACTIVE')
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件详情同步完成: {result}")
        except Exception as e:
            print(f"[{_now_str()}] [Cron] 店铺[{shop_name}] 入库计划货件详情同步异常: {e}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python scripts/cron/inbound.py --30min|--6h")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == '--30min':
        run_30min()
    elif mode == '--6h':
        run_6h()
    else:
        print(f"未知模式: {mode}，请使用 --30min 或 --6h")
        sys.exit(1)
