"""关键词/竞品Excel共用错误、空值与数值转换；不包含业务列或表结构。

保留原转换精度和缺失值规则，不猜测来源单位。
"""
import re
from decimal import Decimal, InvalidOperation


class ExcelValidationError(ValueError):
    pass


MISSING = {'', '-', '--', 'none', 'null', 'n/a', 'na', '前3页无排名', '暂无数据'}


def text_value(value):
    if value is None or str(value).strip().casefold() in MISSING:
        return None
    return str(value).strip()


def number_value(value, integer=False, signed=False):
    raw = text_value(value)
    if raw is None:
        return None
    percent = raw.endswith('%')
    raw = re.sub(r'[$,\s]', '', raw.rstrip('%'))
    try:
        number = Decimal(raw)
    except InvalidOperation as exc:
        raise ExcelValidationError('无法识别数字字段') from exc
    if not number.is_finite() or (not signed and number < 0):
        raise ExcelValidationError('数字字段超出允许范围')
    if percent:
        number /= 100
    if integer:
        if number != number.to_integral_value() or number > 9223372036854775807:
            raise ExcelValidationError('计数或排名不是有效整数')
        return int(number)
    if abs(number) >= Decimal('1000000000000'):
        raise ExcelValidationError('数值超出数据库精度')
    return number.quantize(Decimal('0.00000001'))
