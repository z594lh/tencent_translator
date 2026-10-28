"""关键词与竞品共用的任务错误、日期范围、文件大小和私有存储根目录。

不查询业务任务表；两类任务的领取、进度和入库分别由各自Service处理。
"""
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

MAX_FILE_SIZE = 20 * 1024 * 1024


class PipelineError(ValueError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record_date(value=None, allow_history=False):
    today = datetime.now(ZoneInfo('Asia/Shanghai')).date()
    try:
        result = date.fromisoformat(str(value)) if value else today
    except (ValueError, TypeError) as exc:
        raise PipelineError('record_date必须为YYYY-MM-DD') from exc
    if result > today or (not allow_history and result < today - timedelta(days=31)):
        raise PipelineError('只允许领取或重传最近31天任务')
    return result


def storage_root():
    return Path(os.environ.get('SELLERSPRITE_UPLOAD_DIR') or
                Path(__file__).resolve().parents[1] / 'data' / 'sellersprite').resolve()
