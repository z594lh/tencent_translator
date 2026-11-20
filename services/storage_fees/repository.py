"""仓储费页面及AI工具的只读数据访问，不在查询时触发报告同步。"""
import json
import pymysql
from services import storage_analysis_repository as reports


def read(shop_id, start, end):
    """同一事务读取月费、报告原行及名称；缺报告表兼容旧月费投影。"""
    binding = reports.scope(shop_id)
    with reports.transaction() as cur:
        cur.execute('SELECT * FROM amazon_fba_storage_fees WHERE shop_id=%s AND month_of_charge BETWEEN %s AND %s', (shop_id, start, end))
        original = cur.fetchall()
        sources = []
        try:
            cur.execute('''SELECT r.*,b.kind,b.period_key,b.data_at FROM fba_storage_analysis_rows r
                JOIN fba_storage_analysis_batches b ON b.id=r.batch_id
                WHERE b.shop_id=%s AND b.marketplace_id=%s AND b.credential_group_id=%s AND b.region=%s
                AND b.data_at IS NOT NULL AND ((b.kind IN ('monthly','aged','age_history') AND b.period_key BETWEEN %s AND %s)
                OR (b.kind='health' AND b.period_key='latest'))''', (*binding, start, end))
            sources = cur.fetchall()
        except pymysql.err.ProgrammingError as error:
            if error.args[0] != 1146:
                raise
        for row in sources:
            row['payload'] = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
        cur.execute('SELECT seller_sku,MAX(COALESCE(NULLIF(declare_name_cn,\'\'),product_name)) product_name FROM products GROUP BY seller_sku')
        names = {r['seller_sku']: r['product_name'] for r in cur.fetchall()}
    return original, sources, names
