#!/usr/bin/env python3
"""每日18:30（北京时间）仅登记统一AI队列；ai_worker.py消费后才分析和正式执行。"""
import argparse
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT/'.env',override=False)
from services.ai_operations import daily_runner


def run(check_only=False):
    try:
        if check_only:
            result=daily_runner.check_ready()
        else:result=daily_runner.run_due()
        # 只输出任务状态/数量，不把SKU、完整输入或凭证写入普通计划任务日志。
        print('ASIN每日托管：'+json.dumps(result,ensure_ascii=False),flush=True)
        return 1 if result.get('failed',0) else 0
    except Exception as error:
        print('ASIN每日托管未完成：'+type(error).__name__+'；请检查优化历史与表结构，不自动重发真实请求',flush=True)
        return 1


if __name__=='__main__':
    parser=argparse.ArgumentParser(description='每日ASIN托管；--check只读预检')
    parser.add_argument('--check',action='store_true')
    sys.exit(run(parser.parse_args().check))
