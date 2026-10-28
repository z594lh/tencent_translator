#!/usr/bin/env python3
"""服务端Excel清理入口；默认预览，--apply才删除已成功入库且超过保护期的原件。

在ERP服务器运行，不在采集虚拟机运行；任务/快照/文件元数据均保留，不修改数据库。
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / '.env', override=False)
from services.sellersprite_cleanup import cleanup


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='实际清理；不提供则只预览数量')
    parser.add_argument('--min-age-hours', type=int, default=1, help='成功上传后最小保留小时数，默认1')
    args = parser.parse_args()
    try:
        result = cleanup(apply=args.apply, min_age_hours=args.min_age_hours)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return int(bool(result['errors'] or result['invalid_records']))
    except Exception as exc:
        # 避免数据库连接异常对象带出连接参数；详细排查由受控服务端日志处理。
        print('Excel清理未完成：' + type(exc).__name__, flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
