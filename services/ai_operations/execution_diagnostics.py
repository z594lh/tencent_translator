"""执行异常脱敏诊断：保存阶段、异常类型和代码位置，不记录SQL参数、凭证或响应原文。"""
import logging
from pathlib import Path
import traceback

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


def capture(error, stage, execution_id):
    """生成可展示和定位的诊断；服务端日志同样只写脱敏结构，避免丢失原因。"""
    locations = []
    for frame in traceback.extract_tb(error.__traceback__):
        try: path = Path(frame.filename).resolve().relative_to(ROOT).as_posix()
        except ValueError: continue
        if not path.startswith(('services/', 'blueprints/', 'scripts/cron/')): continue
        locations.append(f'{path}:{frame.lineno}:{frame.name}')
    code = error.args[0] if error.args and type(error.args[0]) is int else None
    hints = {1054: '数据库字段与查询不一致，请核对当前Schema和查询关联',
             1146: '数据库缺少所需表，请核对部署结构', 1205: '数据库锁等待超时', 1213: '数据库并发锁冲突'}
    result = {'stage': stage, 'exception_type': type(error).__name__,
              'hint': hints.get(code, '请根据执行阶段、异常类型及代码位置排查；异常原文已隐藏'),
              'locations': locations[-6:]}
    if code is not None: result['error_code'] = code
    LOGGER.error('AI execution %s failed: %s', execution_id, result)
    return result
