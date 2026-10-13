"""FBA仓储费只读年度趋势；按月、币种聚合，不修改同步和费用计算。"""
from decimal import Decimal

from services.mysql_service import get_db_connection


_VALID_MONTH_SQL = "month_of_charge REGEXP '^[1-9][0-9]{3}-(0[1-9]|1[0-2])$'"


def month_window(end_month):
    """截至最新计费月的连续12个月，跨年和缺月也保留完整横轴。"""
    year, month = map(int, end_month.split('-'))
    end = year * 12 + month - 1
    return [f'{index // 12:04d}-{index % 12 + 1:02d}' for index in range(end - 11, end + 1)]


def _amount(value):
    return Decimal(str(value)) if value is not None else Decimal('0')


def monthly_trend(shop_id):
    """读取全店最近一年；不受SKU列表月份、搜索、仓库或分页影响。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'''SELECT MAX(month_of_charge) AS latest_month
                FROM amazon_fba_storage_fees WHERE shop_id=%s AND {_VALID_MONTH_SQL}''', (shop_id,))
            latest = cursor.fetchone()['latest_month']
            if not latest:
                return {'months': [], 'latest_month': None, 'series': []}
            months = month_window(latest)
            cursor.execute(f'''SELECT month_of_charge,
                UPPER(TRIM(COALESCE(currency, ''))) AS currency, COUNT(*) AS record_count,
                COALESCE(SUM(estimated_monthly_storage_fee), 0) AS storage_fee,
                COALESCE(SUM(total_incentive_fee_amount), 0) AS incentive_fee
                FROM amazon_fba_storage_fees
                WHERE shop_id=%s AND month_of_charge BETWEEN %s AND %s AND {_VALID_MONTH_SQL}
                GROUP BY month_of_charge, UPPER(TRIM(COALESCE(currency, '')))
                ORDER BY currency, month_of_charge''', (shop_id, months[0], months[-1]))
            rows = cursor.fetchall()
        by_currency = {}
        for row in rows:
            storage = _amount(row['storage_fee'])
            incentive = _amount(row['incentive_fee'])
            by_currency.setdefault(row['currency'], {})[row['month_of_charge']] = {
                'month': row['month_of_charge'], 'record_count': row['record_count'],
                'storage_fee': format(storage, '.4f'), 'incentive_fee': format(incentive, '.4f'),
                'net_fee': format(storage - incentive, '.4f'),
            }
        series = []
        for currency, values in sorted(by_currency.items()):
            points = [values.get(month, {
                'month': month, 'record_count': 0,
                'storage_fee': None, 'incentive_fee': None, 'net_fee': None,
            }) for month in months]
            series.append({'currency': currency, 'points': points})
        return {'months': months, 'latest_month': latest, 'series': series}
    finally:
        conn.close()
