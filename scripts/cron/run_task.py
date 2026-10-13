#!/usr/bin/env python3
"""固定任务公共入口；crontab只使用--task，内部请求由独立worker传递。"""
import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env', override=True)

from services.cron_tasks.catalog import TASKS
from services.cron_tasks.execution import execute, ignore_termination_while_running


def main():
    parser = argparse.ArgumentParser(description='统一计划任务执行入口，不接受自定义脚本/命令')
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--task', choices=sorted(TASKS))
    source.add_argument('--run-id', type=int)
    parser.add_argument('--instance')
    args = parser.parse_args()
    if (args.run_id is not None) != bool(args.instance):
        parser.error('内部请求必须同时提供run-id和instance')
    ignore_termination_while_running()
    try:
        return execute(args.task, args.run_id, args.instance)
    except Exception as exc:
        print(f'任务未正常结束（{type(exc).__name__}），请检查执行记录/服务配置；不会自动重试', flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
