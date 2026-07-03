#!/usr/bin/env python3
"""
广告实体状态同步 — 从 Amazon Ads API 同步 Campaign/AdGroup/Keyword/Target 等到本地表

用法:
    python scripts/cron/ads_entity_sync.py                      同步所有店铺
    python scripts/cron/ads_entity_sync.py --shop 1             仅同步店铺1

定时: 建议每 30-60 分钟执行一次（实体变更频率低）
      crontab: */30 * * * * cd /path && python scripts/cron/ads_entity_sync.py >> log/ads_entity.log 2>&1
"""
import os
import sys
import importlib.util

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

import argparse
from datetime import datetime
from scripts.cron import _now_str


def _load_sync_module():
    """直接加载 advertising_manage.py 文件，绕过 __init__.py 连锁导入"""
    path = os.path.join(PROJECT_ROOT, "blueprints", "amazon", "advertising_manage.py")
    spec = importlib.util.spec_from_file_location("advertising_manage", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(shop_id=None):
    mod = _load_sync_module()

    if shop_id is not None:
        r = mod.sync_shop_entities(shop_id)
        print(f"[{_now_str()}] [EntitySync] 完成")
        return [r]
    else:
        results = mod.sync_all_shops()
        successes = sum(1 for r in results if not r["error"])
        failures = sum(1 for r in results if r["error"])
        print(f"[{_now_str()}] [EntitySync] 完成: {successes} 成功, {failures} 失败")
        return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Amazon Ads 实体状态同步')
    parser.add_argument('--shop', type=int, default=None, help='指定店铺 ID')
    args = parser.parse_args()
    run(shop_id=args.shop)
