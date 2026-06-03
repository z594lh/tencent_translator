#!/usr/bin/env python3
"""汇率同步（每天9点）"""
import os
import sys
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, '.env'), override=True)

from scripts.cron import _now_str, fetch_and_save_exchange_rate


def run():
    print(f"[{_now_str()}] [Cron] 开始汇率同步...")
    try:
        result = fetch_and_save_exchange_rate('CNY', 'USD')
        if result:
            print(f"[{_now_str()}] [Cron] 汇率同步完成: {result}")
        else:
            print(f"[{_now_str()}] [Cron] 汇率同步失败，请检查日志")
    except Exception as e:
        print(f"[{_now_str()}] [Cron] 汇率同步异常: {e}")


if __name__ == '__main__':
    run()
