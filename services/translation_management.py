"""计划任务页的翻译状态与人工恢复；不调用外部翻译、不重置安全预算。"""
import hashlib
import json
from datetime import datetime

from services import translation_store as store
from services.baidu_translator import TranslationError, get_client
from services.baidu_rate_limit import option


def version(key, state):
    # 预算和发现游标变化不使确认失效；账户再度报错或切换凭据必须重新确认。
    values = [key] + [str(state.get(name)) for name in
                     ('paused','pause_code','failure_count','cooldown_until')]
    return hashlib.sha256(json.dumps(values).encode('utf-8')).hexdigest()


def snapshot():
    key = store.account_key(get_client().appid)
    now = datetime.utcnow()
    with store.transaction() as cur:
        cur.execute('''SELECT paused,pause_code,failure_count,cooldown_until,budget_date,
            reserved_chars,reserved_requests FROM translation_provider_state WHERE account_key=%s''',(key,))
        state = cur.fetchone() or {}
        cur.execute("SELECT status,COUNT(*) n FROM text_translation_tasks WHERE provider='baidu' GROUP BY status")
        queue = {row['status']:row['n'] for row in cur.fetchall()}
    today = state.get('budget_date') == now.date()
    cooldown = state.get('cooldown_until')
    return {
        'queue':queue,'paused':bool(state.get('paused')),'pause_code':state.get('pause_code'),
        'cooldown_until':cooldown.isoformat()+'Z' if cooldown else None,
        'cooling_down':bool(cooldown and cooldown>now),'version':version(key,state),
        'budget_date':now.date().isoformat(),
        'reserved_chars':state.get('reserved_chars',0) if today else 0,
        'reserved_requests':state.get('reserved_requests',0) if today else 0,
        'char_limit':option('BAIDU_TRANSLATE_DAILY_CHAR_LIMIT',30000,1,10000000,True),
        'request_limit':option('BAIDU_TRANSLATE_DAILY_REQUEST_LIMIT',1000,1,100000,True),
    }


def resume(payload):
    if not isinstance(payload,dict) or payload.get('confirm') is not True or not isinstance(payload.get('version'),str):
        raise TranslationError('请先查看当前翻译状态并确认已修复余额或授权问题',400)
    key = store.account_key(get_client().appid)
    conn = store.get_db_connection()
    locks = []
    try:
        with conn.cursor() as cur:
            # 与任务使用相同顺序，拒绝与在途批次/网页请求同时恢复。
            for name in ('task_keyword_translation_worker','baidu_translation:'+key[:40]):
                cur.execute('SELECT GET_LOCK(%s,0) acquired',(name,))
                if cur.fetchone()['acquired'] != 1:
                    raise TranslationError('翻译请求或任务仍在执行，请稍后刷新状态再恢复',409)
                locks.append(name)
            conn.begin()
            cur.execute('''SELECT paused,pause_code,failure_count,cooldown_until FROM
                translation_provider_state WHERE account_key=%s FOR UPDATE''',(key,))
            state = cur.fetchone() or {}
            if payload['version'] != version(key,state):
                raise TranslationError('翻译账户状态已变化，请刷新后重新确认',409)
            cur.execute("SELECT COUNT(*) n FROM text_translation_tasks WHERE provider='baidu' AND source_lang='auto' AND target_lang='zh' AND status='blocked'")
            blocked = cur.fetchone()['n']
            if not state.get('paused') and not blocked:
                conn.rollback()
                return {'restored':0,'message':'当前没有需恢复的暂停或阻塞任务'}
            now = datetime.utcnow()
            due = max(now,state.get('cooldown_until') or now)
            cur.execute('''UPDATE translation_provider_state SET paused=0,pause_code=NULL,
                failure_count=0,updated_at=%s WHERE account_key=%s''',(now,key))
            cur.execute('''UPDATE text_translation_tasks SET status='retry',attempts=0,next_retry_at=%s,
                lease_token=NULL,lease_until=NULL,updated_at=%s,
                last_error_code=CASE WHEN last_error_code LIKE 'batch_%%|%%'
                THEN SUBSTRING_INDEX(last_error_code,'|',1) ELSE NULL END
                WHERE provider='baidu' AND source_lang='auto' AND target_lang='zh' AND status='blocked' ''',(due,now))
            restored = cur.rowcount
            conn.commit()
            return {'restored':restored,'message':'已恢复阻塞任务，等待原定时计划；现存冷却和日预算保持，不会立即调用翻译'}
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            for name in reversed(locks):
                with conn.cursor() as cur:
                    cur.execute('SELECT RELEASE_LOCK(%s)',(name,))
        finally:
            conn.close()
