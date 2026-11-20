"""仓储查询数字排序：全量结果先排序再分页，缺值始终末尾。"""
from decimal import Decimal, InvalidOperation

MONEY = {'storage_fee', 'incentive_fee', 'net_fee', 'aged_fee'}


def currencies(rows):
    return sorted({fee.get('currency') or '' for row in rows for key in ('fees', 'aged_fees') for fee in row.get(key, [])})


def sort_rows(rows, query, allowed):
    field, order = query.get('sort_by') or '', query.get('sort_order') or ''
    if not field and not order:
        return rows
    if field not in allowed or order not in ('ascending', 'descending'):
        raise ValueError('排序列或方向无效')
    currency = query.get('sort_currency') or ''
    if field in MONEY:
        options = currencies(rows)
        if not currency and len(options) > 1:
            raise ValueError('多币种金额排序需要选择币种')
        currency = currency or (options[0] if options else '')
    def value(row):
        if field in MONEY:
            fees = row.get('aged_fees' if field == 'aged_fee' else 'fees', [])
            raw = next((f.get('amount' if field == 'aged_fee' else field) for f in fees if (f.get('currency') or '') == currency), None)
        elif field.startswith('age_'):
            raw = row.get('age_quantities', {}).get(field[4:])
        else:
            raw = row.get(field)
        try:
            number = Decimal(str(raw))
            return number if number.is_finite() else None
        except (InvalidOperation, ValueError, TypeError):
            return None
    known, unknown = [], []
    for row in rows:
        number = value(row)
        if number is None:
            unknown.append(row)
        else:
            known.append((number, row))
    return [row for _, row in sorted(known, key=lambda item: item[0], reverse=order == 'descending')] + unknown
