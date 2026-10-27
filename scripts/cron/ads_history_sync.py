#!/usr/bin/env python3
"""广告历史独立任务：统一入口持有本任务锁，服务内部按账户退避及断点续传。"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / '.env', override=True)
from services.ads_history_service import run_due


def run():
    results = run_due()
    if not results:
        print('没有到期的广告历史采集需求，本轮不调用Amazon。', flush=True)
    for item in results:
        print(f"历史采集：{item['status']}；{item.get('message', '')}；外部请求{item.get('calls', 0)}次；保存/关联{item.get('saved', 0)}条；"
              f"本地已有记录补关联{item.get('reconciled', 0)}条", flush=True)
        if item.get('next_retry_ms'):
            from datetime import datetime, timezone
            print('下次最早请求（UTC）：'+datetime.fromtimestamp(item['next_retry_ms']/1000,timezone.utc).isoformat(),flush=True)
    return 1 if any(item['status'] == 'failed' for item in results) else 0


if __name__ == '__main__':
    sys.exit(run())
