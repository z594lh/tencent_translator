"""仓储分析独立资料的短事务和只读查询，不修改原业务表。"""
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json

import pymysql
from services.mysql_service import get_db_connection
from services.shop_service import get_shop_by_id


def scope(shop_id):
    shop = get_shop_by_id(shop_id)
    if not shop or not shop.get('marketplace_id') or not shop.get('credential_group_id') or shop.get('region') not in ('na', 'eu', 'fe'):
        raise ValueError('店铺站点、区域或凭证组配置不完整')
    return (shop_id, shop['marketplace_id'], shop['credential_group_id'], shop['region'])


@contextmanager
def transaction():
    conn = get_db_connection()
    try:
        conn.begin()
        with conn.cursor() as cursor:
            yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def report_lock(binding):
    """跨Web进程阻止同店重复创建报告；不在事务内等待Amazon。"""
    name = 'storage_analysis_' + hashlib.sha256(str(binding).encode()).hexdigest()[:40]
    conn = get_db_connection()
    acquired = False
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT GET_LOCK(%s,0) AS acquired', (name,))
            acquired = cursor.fetchone()['acquired'] == 1
        conn.commit()
        if not acquired:
            raise ValueError('该店铺资料正在更新，请稍后查看')
        yield
    finally:
        try:
            if acquired:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT RELEASE_LOCK(%s)', (name,))
        finally:
            conn.close()


def batch(binding, kind, period):
    with transaction() as cursor:
        cursor.execute('''SELECT * FROM fba_storage_analysis_batches
            WHERE shop_id=%s AND marketplace_id=%s AND credential_group_id=%s AND region=%s
            AND kind=%s AND period_key=%s''', (*binding, kind, period))
        return cursor.fetchone()


def pending_batches(binding):
    """自动任务续查同绑定所有在途报告，避免跨月后遗漏上月人工请求。"""
    with transaction() as cursor:
        cursor.execute('''SELECT kind,period_key FROM fba_storage_analysis_batches
            WHERE shop_id=%s AND marketplace_id=%s AND credential_group_id=%s AND region=%s
            AND attempt_status IN ('creating','pending') ORDER BY requested_at,id''', binding)
        return cursor.fetchall()


def reserve(binding, kind, period):
    with transaction() as cursor:
        cursor.execute('''INSERT INTO fba_storage_analysis_batches
            (shop_id,marketplace_id,credential_group_id,region,kind,period_key,attempt_status,requested_at,checked_at)
            VALUES (%s,%s,%s,%s,%s,%s,'creating',%s,%s)
            ON DUPLICATE KEY UPDATE report_id=NULL,attempt_status='creating',attempt_message='',
            requested_at=VALUES(requested_at),checked_at=VALUES(checked_at)''', (*binding, kind, period, datetime.utcnow(), datetime.utcnow()))
    return batch(binding, kind, period)


def state(batch_id, status, message='', report_id=None):
    with transaction() as cursor:
        cursor.execute('''UPDATE fba_storage_analysis_batches SET attempt_status=%s,
            attempt_message=%s,checked_at=%s,report_id=COALESCE(%s,report_id) WHERE id=%s''',
            (status, message[:255], datetime.utcnow(), report_id, batch_id))


def save(batch_row, rows):
    """已解析整份报告才替换；保存失败回滚，旧成功资料保持可用。"""
    snapshots = {row['record_date'] for row in rows if row.get('record_date')}
    snapshot = max(snapshots) if snapshots else None
    with transaction() as cursor:
        cursor.execute('SELECT * FROM fba_storage_analysis_batches WHERE id=%s FOR UPDATE', (batch_row['id'],))
        current = cursor.fetchone()
        if current['report_id'] != batch_row['report_id']:
            raise ValueError('报告请求已变化，本次结果不保存')
        if batch_row['kind'] == 'health' and current['snapshot_date'] and snapshot and snapshot < current['snapshot_date'].isoformat():
            raise ValueError('报告快照早于已保存数据，未覆盖原资料')
        _replace(cursor, batch_row['id'], rows, snapshot)


def _replace(cursor, batch_id, rows, snapshot):
    """调用方已锁定批次，同一短事务内整体替换，不独立提交。"""
    cursor.execute('DELETE FROM fba_storage_analysis_rows WHERE batch_id=%s', (batch_id,))
    for row in rows:
        cursor.execute('''INSERT INTO fba_storage_analysis_rows
            (batch_id,row_key,sku,fnsku,asin,country_code,currency,fulfillment_center,record_date,payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (batch_id, row['row_key'], row.get('sku'), row.get('fnsku'), row.get('asin'),
             row['country'], row['currency'], row.get('fc'), row.get('record_date'), json.dumps(row['payload'], ensure_ascii=False)))
    cursor.execute('''UPDATE fba_storage_analysis_batches SET attempt_status='ready',attempt_message='',
            data_at=%s,checked_at=%s,snapshot_date=%s,row_count=%s WHERE id=%s''',
            (datetime.utcnow(), datetime.utcnow(), snapshot, len(rows), batch_id))


def save_monthly(batch_row, rows):
    """Amazon实测返回多个月：按实际账期原子保存，不丢掉非请求月，也不覆盖其他在途请求。"""
    groups = {}
    for row in rows:
        groups.setdefault(row['payload']['charge_month'], []).append(row)
    binding = tuple(batch_row[key] for key in ('shop_id', 'marketplace_id', 'credential_group_id', 'region'))
    with transaction() as cursor:
        cursor.execute('SELECT * FROM fba_storage_analysis_batches WHERE id=%s FOR UPDATE', (batch_row['id'],))
        current = cursor.fetchone()
        if current['report_id'] != batch_row['report_id']:
            raise ValueError('月报请求已变化，未保存')
        for month, values in sorted(groups.items()):
            cursor.execute('''SELECT * FROM fba_storage_analysis_batches WHERE shop_id=%s AND marketplace_id=%s
                AND credential_group_id=%s AND region=%s AND kind='monthly' AND period_key=%s FOR UPDATE''', (*binding, month))
            target = cursor.fetchone()
            if target and target['id'] != batch_row['id']:
                if target['attempt_status'] in ('creating', 'pending') or (target['report_id'] != batch_row['report_id'] and target['data_at'] and target['data_at'] >= batch_row['requested_at']):
                    continue
            if not target:
                cursor.execute('''INSERT INTO fba_storage_analysis_batches
                    (shop_id,marketplace_id,credential_group_id,region,kind,period_key,report_id,attempt_status,requested_at,checked_at)
                    VALUES (%s,%s,%s,%s,'monthly',%s,%s,'pending',%s,%s)''',
                    (*binding, month, batch_row['report_id'], batch_row['requested_at'], datetime.utcnow()))
                target_id = cursor.lastrowid
            else:
                target_id = target['id']
                cursor.execute('''UPDATE fba_storage_analysis_batches SET report_id=%s,requested_at=%s WHERE id=%s''',
                               (batch_row['report_id'], batch_row['requested_at'], target_id))
            _replace(cursor, target_id, values, month + '-01')
        if batch_row['period_key'] not in groups:
            cursor.execute('''UPDATE fba_storage_analysis_batches SET attempt_status='no_data',attempt_message=%s,checked_at=%s WHERE id=%s''',
                ('Amazon未返回请求月；已按实际计费月份保存其他月，原成功资料保留', datetime.utcnow(), batch_row['id']))


def read_sources(binding, month, prior=None):
    """未迁移只标缺资料，不误报为空库存或零收费。"""
    try:
        with transaction() as cursor:
            cursor.execute('''SELECT * FROM fba_storage_analysis_batches
                WHERE shop_id=%s AND marketplace_id=%s AND credential_group_id=%s AND region=%s
                AND ((kind='health' AND period_key='latest') OR period_key=%s
                     OR (kind='monthly' AND period_key=%s))''', (*binding, month, prior or month))
            batches = cursor.fetchall()
            ids = [row['id'] for row in batches if row['data_at']]
            rows = []
            if ids:
                cursor.execute('SELECT * FROM fba_storage_analysis_rows WHERE batch_id IN (' + ','.join(['%s'] * len(ids)) + ')', ids)
                rows = cursor.fetchall()
            kinds = {row['id']: (row['kind'], row['period_key']) for row in batches}
            for row in rows:
                row['kind'], row['period_key'] = kinds[row['batch_id']]
                row['payload'] = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            return batches, rows, True
    except pymysql.err.ProgrammingError as error:
        if error.args[0] != 1146:
            raise
        return [], [], False


def monthly_rows(shop_id, month, prior):
    with transaction() as cursor:
        cursor.execute('''SELECT * FROM amazon_fba_storage_fees
            WHERE shop_id=%s AND month_of_charge IN (%s,%s)''', (shop_id, month, prior))
        rows = cursor.fetchall()
        cursor.execute('''SELECT seller_sku,MAX(product_name) AS product_name,MAX(declare_name_cn) AS declare_name_cn
            FROM products WHERE status=1 GROUP BY seller_sku''')
        products = {row['seller_sku']: row for row in cursor.fetchall()}
    return rows, products


def months(shop_id, binding=None):
    binding = binding or scope(shop_id)
    with transaction() as cursor:
        cursor.execute('''SELECT DISTINCT month_of_charge AS month FROM amazon_fba_storage_fees
            WHERE shop_id=%s AND month_of_charge REGEXP '^[1-9][0-9]{3}-(0[1-9]|1[0-2])$'
            ORDER BY month_of_charge DESC''', (shop_id,))
        result = {row['month'] for row in cursor.fetchall()}
        try:
            cursor.execute('''SELECT DISTINCT period_key AS month FROM fba_storage_analysis_batches
                WHERE shop_id=%s AND marketplace_id=%s AND credential_group_id=%s AND region=%s
                AND kind='monthly' AND data_at IS NOT NULL
                AND period_key REGEXP '^[1-9][0-9]{3}-(0[1-9]|1[0-2])$' ''', binding)
            result.update(row['month'] for row in cursor.fetchall())
        except pymysql.err.ProgrammingError as error:
            if error.args[0] != 1146:
                raise
        return sorted(result, reverse=True)
