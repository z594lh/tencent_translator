#!/usr/bin/env python3
"""关键词与广告搜索词独立补译入口，复用统一任务审计，不新增常驻服务。"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / '.env', override=False)
from services.keyword_translation_service import run_due


def run():
    try:
        result = run_due()
        # 只输出数量、状态和脱敏说明，不输出搜索词、APPID/密钥或原始响应。
        print('完整词组翻译：'+json.dumps(result,ensure_ascii=False),flush=True)
        return 1 if result['status'] in ('deferred','paused') else 0
    except Exception:
        print('翻译任务未完成，请检查数据库/新表/翻译配置；不会在本轮自动重发。',flush=True)
        return 1


if __name__ == '__main__':
    sys.exit(run())
