"""仓储完整报告定时编排；单轮推进，不驻留、不等待Amazon生成。"""
from datetime import datetime, timezone

from services import storage_analysis_reports as reports
from services import storage_analysis_repository as repo
from services.shop_service import get_all_active_shops


def jobs(now, pending):
    """先续查历史在途，再补最近两个月；UTC整月，跨年也不请求当月。"""
    result = [(row['kind'], row['period_key']) for row in pending if row['kind'] in reports.REPORT_TYPES]
    result.append(('health', 'latest'))
    current = now.astimezone(timezone.utc) if now.tzinfo else now
    index = current.year * 12 + current.month - 1
    for offset in (1, 2):
        year, month = divmod(index - offset, 12)
        period = f'{year:04d}-{month + 1:02d}'
        result.extend((kind, period) for kind in ('monthly', 'aged', 'overage'))
    return list(dict.fromkeys(result))


def run_shop(shop_id):
    binding = repo.scope(shop_id)
    pending = repo.pending_batches(binding)
    results = []
    for kind, period in jobs(datetime.now(timezone.utc), pending):
        try:
            # 自动刷新资格在advance的店铺报告锁内重查；不信任本轮初始快照。
            if repo.scope(shop_id) != binding:
                raise ValueError('店铺绑定已变化')
            row = reports.advance(shop_id, kind, None if kind == 'health' else period,
                                  'start', automatic=True, expected_binding=binding)
            results.append({'shop_id': shop_id, 'kind': kind, 'period': period,
                            'status': row['attempt_status']})
        except Exception as error:
            # 只输出错误类别，不记录凭证、带签名URL或原始接口响应。
            results.append({'shop_id': shop_id, 'kind': kind, 'period': period,
                            'status': 'error', 'error_type': type(error).__name__})
    return results


def run_all():
    results = []
    for shop in get_all_active_shops():
        try:
            results.extend(run_shop(shop['id']))
        except Exception as error:
            results.append({'shop_id': shop['id'], 'kind': 'scope', 'period': '-',
                            'status': 'error', 'error_type': type(error).__name__})
    return results
