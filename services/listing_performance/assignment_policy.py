"""负责人交接日期政策，人工分配与 AI 托管必须共用。

用户确定的固定北京时间 15:00 分界（不随美国夏令时移动）：
2026-10-26 14:00 变更，默认从 2026-10-27 业务日生效；
2026-10-26 15:00 变更，默认从 2026-10-28 业务日生效。
即严格早于 15:00 为北京时间日期 +1，15:00 整起为 +2。
日期是绩效报表归属的业务日标签；具体零点 UTC 仍沿用站点已配置时区。
不重写已经保存的历史区间和报表。签名预览跨越分界后须重新预览。
"""
from datetime import datetime, time, timedelta, timezone

BJ = timezone(timedelta(hours=8))
DESCRIPTION = '北京时间15:00前变更，次日生效；15:00起变更，后天生效。历史业绩不变。'


def beijing_now(now=None):
    """把注入的 UTC 时钟或当前时钟转换为北京时间，无时区输入按 UTC 解释。"""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(BJ)


def default_date(now=None):
    """返回当前允许的最早交接业务日，15:00 整计入后天档。"""
    current = beijing_now(now)
    return current.date() + timedelta(days=1 if current.time() < time(15) else 2)
