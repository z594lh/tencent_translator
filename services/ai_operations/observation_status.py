"""观察日历到期判断；资料覆盖由独立的改后证据披露。"""


def review_due(observation):
    """用冻结记录的日龄判断到期，兼容旧阶段，不读取当前时钟。"""
    elapsed = observation.get('elapsed_days')
    duration = observation.get('duration_days')
    if type(elapsed) is int and type(duration) is int and duration > 0:
        return elapsed >= duration
    # 老冻结资料未提供日龄时，waiting_data本就意味着日历已到期。
    return observation.get('phase') in ('ready_review', 'waiting_data')
