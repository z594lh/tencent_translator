#!/usr/bin/env python3
"""关键词与广告搜索词独立补译入口，复用统一任务审计，不新增常驻服务。"""
import json
import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / '.env', override=False)
from services.keyword_translation_service import run_due, check_readiness, failure_details


def run(check_only=False):
    try:
        result = check_readiness() if check_only else run_due()
        # 只输出数量、状态和脱敏说明，不输出搜索词、APPID/密钥或原始响应。
        print('完整词组翻译：'+json.dumps(result,ensure_ascii=False),flush=True)
        return 1 if result['status'] in ('deferred','paused') else 0
    except Exception as error:
        print('翻译任务未完成；可能已有部分请求或译文，不会在本轮自动重发。',flush=True)
        print('脱敏诊断：'+json.dumps(failure_details(error),ensure_ascii=False),flush=True)
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='关键词翻译任务；--check仅检查配置和表字段，不请求百度')
    parser.add_argument('--check',action='store_true',help='只读部署预检，不建任务、不请求翻译')
    sys.exit(run(check_only=parser.parse_args().check))
