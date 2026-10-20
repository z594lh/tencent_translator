"""完整词组后台待译队列、增量发现和页面只读批量补译文。"""
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timedelta
import pymysql
from services import translation_store as store


def phrase_identity(value):
    if not isinstance(value, str):
        return None
    phrase = re.sub(r'\s+', ' ', value).strip()
    if not phrase or len(phrase) > 500 or not any(char.isalpha() for char in phrase):
        return None
    if re.fullmatch(r'B0[A-Z0-9]{8}', phrase.upper()):
        return None
    try:
        digest = hashlib.sha256(phrase.encode('utf-8')).hexdigest()
    except UnicodeError:
        return None
    return digest, phrase


def enqueue(cursor, phrases, source_flag, priority):
    now = datetime.utcnow()
    unique = dict(identity for value in phrases if (identity := phrase_identity(value)))
    if unique:
        cursor.executemany('''INSERT INTO text_translation_tasks
            (phrase_hash,phrase,source_flags,priority,next_retry_at,created_at,updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE
            source_flags=source_flags | VALUES(source_flags),priority=GREATEST(priority,VALUES(priority))''',
            [(digest,text,source_flag,priority,now,now,now) for digest,text in unique.items()])
    return len(unique)


def _read_enrichment(cursor, hashes):
    cursor.execute('''SELECT phrase_hash,status,translated_text,last_error_code FROM text_translation_tasks
        WHERE provider='baidu' AND source_lang='auto' AND target_lang='zh' AND phrase_hash IN ('''+
        ','.join(['%s']*len(hashes))+')', list(hashes))
    return {row['phrase_hash']:row for row in cursor.fetchall()}


def enrich_rows(rows, field='keyword', cursor=None):
    """只读缓存：无建任务/配置读取/外部翻译，源译文优先，缺表安全降级。"""
    identities = {id(row):phrase_identity(row.get(field)) for row in rows}
    hashes = {item[0] for item in identities.values() if item}
    data, unavailable = {}, False
    if hashes:
        try:
            if cursor is not None:
                data = _read_enrichment(cursor, hashes)
            else:
                conn = store.get_db_connection()
                try:
                    with conn.cursor() as cur:
                        data = _read_enrichment(cur, hashes)
                finally:
                    conn.close()
        except pymysql.err.ProgrammingError as exc:
            if exc.args[0] != 1146:
                raise
            unavailable = True
    for row in rows:
        original = row.get('translation')
        identity = identities[id(row)]
        cached = data.get(identity[0]) if identity else None
        row['translation_source'] = 'source' if isinstance(original,str) and original.strip() else ''
        row['translation_status'] = 'source' if row['translation_source'] else 'skipped' if not identity else 'unavailable' if unavailable else 'pending'
        if row['translation_source']:
            continue
        row['translation'] = ''
        if cached:
            row['translation_status'] = cached['status']
            if cached['status'] == 'succeeded' and cached['translated_text']:
                row['translation'] = cached['translated_text']
                row['translation_source'] = 'baidu'
    return rows


def discover(account, scan_limit=1000):
    """新ID优先，历史倒序续扫；每日重开有界兜底，修复迟提交/旧行更新。"""
    discovered = {}
    with store.transaction() as cur:
        cur.execute('INSERT IGNORE INTO translation_provider_state (account_key) VALUES (%s)', (account,))
        cur.execute('SELECT discovery_json FROM translation_provider_state WHERE account_key=%s FOR UPDATE', (account,))
        try:
            state = json.loads(cur.fetchone()['discovery_json'] or '{}')
        except (ValueError,TypeError):
            state = {}
        if not isinstance(state,dict):
            state = {}
        today = datetime.utcnow().date().isoformat()
        for name,table,columns,flag,priority in [
            ('sprite','sellersprite_keyword_rows','id,keyword AS phrase,translation',1,100),
            ('ads','amazon_ads_raw_reports','id,customer_search_term AS phrase,report_type',2,50)]:
            cur.execute('SELECT COALESCE(MAX(id),0) AS mid FROM '+table)
            maximum = cur.fetchone()['mid']
            mark = state.get(name)
            if not isinstance(mark,dict):
                mark = {'forward':maximum,'before':maximum+1,'day':today}
            if mark.get('before',0) == 0 and mark.get('day') != today:
                mark['before'],mark['day'] = maximum+1,today
            candidates = []
            cur.execute('SELECT '+columns+' FROM '+table+' WHERE id>%s ORDER BY id LIMIT %s',
                        (int(mark.get('forward',0)),scan_limit))
            forward = list(cur.fetchall())
            if forward:
                mark['forward'] = forward[-1]['id']
                candidates.extend(forward)
            if mark.get('before',0):
                cur.execute('SELECT '+columns+' FROM '+table+' WHERE id<%s ORDER BY id DESC LIMIT %s',
                            (int(mark['before']),scan_limit))
                backfill = list(cur.fetchall())
                candidates.extend(backfill)
                mark['before'] = backfill[-1]['id'] if len(backfill) == scan_limit else 0
            phrases = [row['phrase'] for row in candidates if
                       (not (row.get('translation') or '').strip() if name=='sprite' else row['report_type']=='spSearchTerm')]
            discovered[name] = enqueue(cur,phrases,flag,priority)
            state[name] = mark
        cur.execute('UPDATE translation_provider_state SET discovery_json=%s,updated_at=%s WHERE account_key=%s',
                    (json.dumps(state),datetime.utcnow(),account))
    return discovered


def claim(limit, characters):
    now,token = datetime.utcnow(),uuid.uuid4().hex
    with store.transaction() as cur:
        cur.execute('''UPDATE text_translation_tasks SET status=IF(attempts>=5,'failed','retry'),
            next_retry_at=%s,last_error_code='lease_expired',lease_token=NULL,lease_until=NULL,updated_at=%s
            WHERE status='running' AND lease_until<%s''', (now+timedelta(minutes=15),now,now))
        cur.execute('''SELECT id,phrase,attempts FROM text_translation_tasks WHERE provider='baidu'
            AND source_lang='auto' AND target_lang='zh' AND status IN ('pending','retry')
            AND next_retry_at<=%s AND attempts<5 ORDER BY priority DESC,id LIMIT 200 FOR UPDATE''',(now,))
        rows, byte_size, char_size = [],0,0
        for row in cur.fetchall():
            text = row['phrase']
            new_bytes = len(text.encode('utf-8'))+(1 if rows else 0)
            new_chars = len(text)+(1 if rows else 0)
            if byte_size+new_bytes>5800 or char_size+new_chars>characters:
                break
            rows.append(row)
            byte_size += new_bytes
            char_size += new_chars
            if len(rows)>=min(50,limit):
                break
        if rows:
            cur.execute('''UPDATE text_translation_tasks SET status='running',attempts=attempts+1,
                lease_token=%s,lease_until=%s,updated_at=%s WHERE id IN ('''+','.join(['%s']*len(rows))+')',
                (token,now+timedelta(minutes=2),now,*[row['id'] for row in rows]))
    return rows,token,char_size


def complete(rows, token, translations):
    if len(rows)!=len(translations) or any(not isinstance(text,str) or not text.strip() or len(text)>4000 for text in translations):
        raise ValueError('翻译批次结果不完整或过长')
    now = datetime.utcnow()
    with store.transaction() as cur:
        cur.executemany('''UPDATE text_translation_tasks SET status='succeeded',translated_text=%s,
            translated_at=%s,updated_at=%s,lease_token=NULL,lease_until=NULL,last_error_code=NULL
            WHERE id=%s AND status='running' AND lease_token=%s''',
            [(text,now,now,row['id'],token) for row,text in zip(rows,translations)])
        return cur.rowcount


def defer(rows, token, error):
    now = datetime.utcnow()
    code = getattr(error,'provider_code',None)
    status_code = getattr(error,'status',503)
    sent = getattr(error,'request_sent',True)
    pause = code in ('52003','52010','54001','54004','58000','58002','58003','90107')
    delay = max(60,int(getattr(error,'retry_after',None) or 300))
    with store.transaction() as cur:
        for row in rows:
            attempts = row['attempts']+int(sent)
            status = 'blocked' if pause else 'failed' if code=='58001' or attempts>=5 else 'retry'
            # 429是账户节流，不应因限流把正常词组永久判错。
            if status_code==429:
                status,attempts = 'retry',row['attempts']
            cur.execute('''UPDATE text_translation_tasks SET status=%s,attempts=%s,next_retry_at=%s,
                lease_token=NULL,lease_until=NULL,last_error_code=%s,updated_at=%s
                WHERE id=%s AND status='running' AND lease_token=%s''',
                (status,attempts,now+timedelta(seconds=delay),code or 'HTTP_'+str(status_code),now,row['id'],token))


def queue_summary():
    with store.transaction() as cur:
        cur.execute("SELECT status,COUNT(*) n FROM text_translation_tasks WHERE provider='baidu' GROUP BY status")
        return {row['status']:row['n'] for row in cur.fetchall()}


def run_due(max_phrases=500, max_batches=10, max_seconds=60, max_characters=10000, scan_limit=1000):
    from services.baidu_translator import get_client, translate_batch, TranslationError
    if not (1<=max_phrases<=500 and 1<=max_batches<=10 and 1<=max_seconds<=120 and
            1<=max_characters<=10000 and 1<=scan_limit<=5000):
        raise ValueError('翻译任务预算无效')
    client = get_client()
    key = store.account_key(client.appid)
    lock_conn = store.get_db_connection()
    locked = False
    started = time.monotonic()
    result = {'status':'finished','saved':0,'batches':0,'characters':0}
    try:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK('task_keyword_translation_worker',0) acquired")
            locked = cur.fetchone()['acquired']==1
        if not locked:
            return {**result,'status':'busy','message':'已有翻译任务，本轮不调用百度'}
        result['discovered'] = discover(key,scan_limit)
        with store.transaction() as cur:
            cur.execute('SELECT paused,pause_code,cooldown_until FROM translation_provider_state WHERE account_key=%s',(key,))
            state = cur.fetchone()
        if state['paused'] or (state['cooldown_until'] and state['cooldown_until']>datetime.utcnow()):
            return {**result,'status':'paused' if state['paused'] else 'cooldown','queue':queue_summary(),
                    'error_code':state['pause_code'],
                    'cooldown_until_utc':state['cooldown_until'].isoformat() if state['cooldown_until'] else None,
                    'message':'账户暂停或冷却，本轮未调用百度'}
        while result['saved']<max_phrases and result['batches']<max_batches and time.monotonic()-started<max_seconds:
            budget = max_characters-result['characters']
            if budget<=0:
                break
            rows,token,characters = claim(max_phrases-result['saved'],budget)
            if not rows:
                break
            result['batches'] += 1
            result['characters'] += characters
            try:
                translations = translate_batch([row['phrase'] for row in rows],client=client)
            except TranslationError as error:
                defer(rows,token,error)
                result.update(status='deferred',message=str(error),error_code=error.provider_code)
                break
            except Exception:
                # 发送结果无法确定或本地保存异常：保留租约，不在本轮重发。
                raise
            result['saved'] += complete(rows,token,translations)
        result['queue'] = queue_summary()
        if result['status']=='finished' and any(result['queue'].get(status,0) for status in ('pending','retry','running','blocked','failed')):
            result['status'] = 'partial'
        return result
    finally:
        try:
            if locked:
                with lock_conn.cursor() as cur:
                    cur.execute("SELECT RELEASE_LOCK('task_keyword_translation_worker')")
        finally:
            lock_conn.close()
