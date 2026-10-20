"""同APPID的数据库共享防并发、预算及退避，覆盖网页与计划任务。"""
import math
import os
import random
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
import pymysql
from services import translation_store as store


def option(name, default, low, high, integer=False):
    from services.baidu_translator import TranslationError
    try:
        value = float(os.getenv(name, str(default)))
        if not low <= value <= high or (integer and value != int(value)):
            raise ValueError()
        return int(value) if integer else value
    except (ValueError, OverflowError):
        raise TranslationError('百度翻译节流或预算配置无效，请联系管理员', 503) from None


@contextmanager
def account_request(appid, characters):
    """持有连接命名锁但不持长事务；请求前预留，超时仍计入保守预算。"""
    from services.baidu_translator import TranslationError
    interval = option('BAIDU_TRANSLATE_INTERVAL_SECONDS', 2, .2, 10)
    char_limit = option('BAIDU_TRANSLATE_DAILY_CHAR_LIMIT', 30000, 1, 10000000, True)
    request_limit = option('BAIDU_TRANSLATE_DAILY_REQUEST_LIMIT', 1000, 1, 100000, True)
    key = store.account_key(appid)
    lock_name = 'baidu_translation:' + key[:40]
    conn, locked, sent = None, False, False
    try:
        conn = store.get_db_connection()
        with conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s,0) AS acquired', (lock_name,))
            locked = cur.fetchone()['acquired'] == 1
            if not locked:
                raise TranslationError('百度翻译账户正忙，请稍后重试', 429, retry_after=2)
            conn.begin()
            cur.execute('INSERT IGNORE INTO translation_provider_state (account_key) VALUES (%s)', (key,))
            cur.execute('SELECT * FROM translation_provider_state WHERE account_key=%s FOR UPDATE', (key,))
            state = cur.fetchone()
            now = datetime.utcnow()
            if state['paused']:
                raise TranslationError('百度翻译因额度或授权异常暂停，请管理员核实后恢复', 503,
                                       state['pause_code'])
            cooldown = state['cooldown_until']
            if cooldown and cooldown > now:
                raise TranslationError('百度翻译账户处于冷却期，本次未请求平台', 429,
                                       retry_after=math.ceil((cooldown-now).total_seconds()))
            today = now.date()
            reserved = state['reserved_chars'] if state['budget_date'] == today else 0
            requests = state['reserved_requests'] if state['budget_date'] == today else 0
            if reserved+characters > char_limit or requests >= request_limit:
                midnight = datetime.combine(today+timedelta(days=1), datetime.min.time())
                raise TranslationError('百度翻译达到本地每日安全预算，等待次日或管理员调整', 429,
                                       retry_after=math.ceil((midnight-now).total_seconds()))
            wait = max(0, (state['next_allowed_at']-now).total_seconds()) if state['next_allowed_at'] else 0
            if wait > max(3, interval):
                raise TranslationError('百度翻译账户尚未到下次请求时间，本次未请求平台', 429,
                                       retry_after=math.ceil(wait))
            # 进程若在发送中崩溃，至少留下30秒保护；正常完成后改为响应结束+间隔。
            cur.execute('''UPDATE translation_provider_state SET budget_date=%s,reserved_chars=%s,
                reserved_requests=%s,next_allowed_at=%s,updated_at=%s WHERE account_key=%s''',
                (today,reserved+characters,requests+1,now+timedelta(seconds=wait+30),now,key))
            conn.commit()
            if wait:
                time.sleep(wait)
            try:
                sent = True
                yield
            except Exception as exc:
                now = datetime.utcnow()
                code = exc.provider_code if isinstance(exc, TranslationError) else None
                paused = code in ('52003','52010','54001','54004','58000','58002','58003','90107')
                failures = state['failure_count']+1
                delay = min(3600, (300 if getattr(exc,'status',None)==429 else 60)*2**min(failures-1,6))
                delay += random.randint(0,15)
                cur.execute('''UPDATE translation_provider_state SET paused=%s,pause_code=%s,
                    failure_count=%s,cooldown_until=%s,next_allowed_at=%s,updated_at=%s WHERE account_key=%s''',
                    (int(paused),code if paused else None,failures,now+timedelta(seconds=delay),
                     now+timedelta(seconds=interval),now,key))
                conn.commit()
                if isinstance(exc, TranslationError):
                    exc.request_sent = True
                    exc.retry_after = delay
                raise
            else:
                now = datetime.utcnow()
                cur.execute('''UPDATE translation_provider_state SET failure_count=0,cooldown_until=NULL,
                    next_allowed_at=%s,updated_at=%s WHERE account_key=%s''',
                    (now+timedelta(seconds=interval),now,key))
                conn.commit()
    except pymysql.MySQLError:
        error = TranslationError('翻译共享状态不可用，请核对新表或数据库；本次不绕过账户节流',503)
        error.request_sent = sent
        raise error from None
    finally:
        if conn:
            try:
                try:
                    conn.rollback()
                    if locked:
                        with conn.cursor() as cur:
                            cur.execute('SELECT RELEASE_LOCK(%s)', (lock_name,))
                except pymysql.MySQLError:
                    # 连接断开后服务端自动释放命名锁；关闭连接，不暴露底层异常。
                    pass
            finally:
                conn.close()
