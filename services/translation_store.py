"""共享翻译表的连接与短事务；不执行外部翻译。"""
import hashlib
from contextlib import contextmanager
from services.mysql_service import get_db_connection


def account_key(appid):
    return hashlib.sha256(appid.encode('utf-8')).hexdigest()


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
