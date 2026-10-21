"""AI运营售价资料，复用统一成本/售价公式，只读，不刷新API或重算历史利润。"""
from decimal import Decimal
from services import profit_calculator as calculator


def read(cur, shop, market, sku, reference=None):
    costs = calculator.get_unit_costs(cur, sku, shop_id=shop)
    cur.execute('''SELECT audience,currency,our_price,discounted_price,start_at,end_at,updated_at
        FROM amazon_listing_offers WHERE shop_id=%s AND marketplace_id=%s AND BINARY sku=BINARY %s''', (shop, market, sku))
    offers = list(cur.fetchall())
    ordinary = [r for r in offers if r['audience'] == 'ALL' and r['currency'] == 'USD']
    cur.execute('SELECT currency FROM amazon_product_fees WHERE shop_id=%s AND sku=%s ORDER BY updated_at DESC LIMIT 1', (shop, sku))
    fee = cur.fetchone()
    currency_ok = not fee or fee['currency'] == 'USD'
    warnings = list((reference or {}).get('warnings', []))
    if not currency_ok and not any('费用币种' in item for item in warnings):
        warnings.append('费用币种不是USD，不混入USD售价场景')
    fixed = costs.purchase_cost_usd + costs.headway_cost_usd + costs.fba_fee_usd
    scenarios = []
    if currency_ok and len(ordinary) == 1 and ordinary[0]['our_price'] is not None:
        price = Decimal(str(ordinary[0]['our_price']))
        for rate in (Decimal(0), Decimal('.10'), Decimal('.20'), Decimal('.30')):
            scenarios.append({'price': price, 'assumed_ad_rate': rate, 'assumed_refund_rate': Decimal(0),
                **calculator.calculate_profit_rate(fixed, price, costs.commission_rate, rate, Decimal(0))})
    return {'sku': sku, 'currency': 'USD', 'offers': offers,
            'unit_costs': {'purchase': costs.purchase_cost_usd, 'headway': costs.headway_cost_usd,
                'fba': costs.fba_fee_usd if currency_ok else None, 'fixed_total': fixed if currency_ok else None, 'commission_rate': costs.commission_rate,
                'exchange_rate': costs.exchange_rate, 'sources': costs.sources},
            'scenarios': scenarios, 'warnings': warnings,
            'target_price_examples': [{'assumed_ad_rate': Decimal('.20'), 'assumed_refund_rate': Decimal(0),
                'target_profit_rate': margin, **calculator.calculate_suggested_price(
                    fixed, costs.commission_rate, Decimal('.20'), Decimal(0), margin)}
                for margin in (Decimal('.10'), Decimal('.20'))] if currency_ok else [],
            'note': '当前售价模型，假设费率明确列出；不是历史实际收益或销量预测。普通/促销/企业价不得混用。'}
