"""广告创建进度持久化：短事务保存，不在数据库事务中调用Amazon。"""
import json
from contextlib import contextmanager

from services.mysql_service import get_db_connection


class CreationError(ValueError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)


def loads(value):
    return json.loads(value) if isinstance(value, str) else value


@contextmanager
def transaction():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get(job_id, shop_id, operator_id):
    with transaction() as c:
        c.execute('SELECT * FROM amazon_ads_creation_jobs WHERE id=%s AND shop_id=%s AND operator_id=%s',
                  (job_id, shop_id, operator_id))
        row = c.fetchone()
    if not row:
        raise CreationError('创建记录不存在或不属于当前操作人及店铺', 404)
    row['request_json'] = loads(row['request_json'])
    row['progress_json'] = loads(row['progress_json'])
    return row


def save(job):
    with transaction() as c:
        c.execute('''UPDATE amazon_ads_creation_jobs SET status=%s,progress_json=%s,
                     campaign_id=%s,ad_group_id=%s,error_message=%s,updated_at=UTC_TIMESTAMP()
                     WHERE id=%s''',
                  (job['status'], dumps(job['progress_json']), job.get('campaign_id'), job.get('ad_group_id'),
                   job.get('error_message', ''), job['id']))
