#!/usr/bin/env python3
"""仓储完整报告采集；通过run_task.py --task storage_analysis_sync统一加锁与审计。"""
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env', override=True)


def main():
    from services.storage_analysis_scheduler import run_all
    results = run_all()
    for row in results:
        print(f"[StorageAnalysis] shop={row['shop_id']} {row['kind']} {row['period']} "
              f"{row['status']} {row.get('error_type', '')}", flush=True)
    print(f'[StorageAnalysis] 本轮推进{len(results)}项；未完成报告下一轮续查，费用未知不记为0', flush=True)
    return 1 if any(row['status'] in ('failed', 'error') for row in results) else 0


if __name__ == '__main__':
    sys.exit(main())
