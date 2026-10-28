"""关键词反查失败恢复：补采/补传队列、诊断和重试状态；不处理竞品任务。"""
import re
from datetime import timedelta

from services.mysql_service import get_db_connection
from services.sellersprite_common import PipelineError, record_date, utcnow

STAGES = {'query', 'export', 'download', 'upload', 'parse', 'worker', 'queue'}
BLOCKED = {'LOGIN_REQUIRED', 'VERIFICATION_REQUIRED', 'PERMISSION_DENIED',
           'EXPORT_NODE_MISSING', 'SCHEMA_CHANGED', 'ASIN_MISMATCH', 'UNKNOWN_ERROR'}
LABELS = {'query': '查询/登录', 'export': '导出', 'download': '下载Excel',
          'upload': '上传ERP', 'parse': '清洗入库', 'worker': '采集进程', 'queue': '补救队列'}
ACCOUNT_BLOCKS = {'LOGIN_REQUIRED', 'VERIFICATION_REQUIRED', 'PERMISSION_DENIED'}


def safe_message(value):
    """限制诊断长度并移除常见凭证、URL和本地路径，防止页面泄露机器配置。"""
    text = str(value or '')[:4000]
    text = re.sub(r'https?://\S+|(?:[A-Za-z]:[\\/]|/home/|/var/|/tmp/)\S+', '[位置已隐藏]', text)
    text = re.sub(r'(?i)(token|cookie|secret|password|api[_-]?key|authorization)\s*[=:]\s*\S+', r'\1=[已隐藏]', text)
    return text[:1000]


def diagnose(message='', status='', diagnostic=None):
    """把旧日志和新结构化错误归一；不将推测的登录或页面变化冒充已核实原因。"""
    raw = str(message or '')
    pairs = [
        (('WAIT_VERIFICATION', 'captcha', '验证码'), 'query', 'VERIFICATION_REQUIRED', '需要在虚拟机浏览器完成人机验证'),
        (('WAIT_PERMISSION', 'access denied', '无权访问'), 'query', 'PERMISSION_DENIED', '网站拒绝访问，请检查账号权限/套餐'),
        (('WAIT_LOGIN', 'LOGIN_REQUIRED'), 'query', 'LOGIN_REQUIRED', '网站登录未完成，请在虚拟机浏览器重新登录'),
        (('EMPTY_EXPORT', '空关键词表'), 'download', 'EMPTY_EXPORT', '下载的关键词主表为空，需要重新查询并导出'),
        (('EXPORT_NODE_MISSING',), 'export', 'EXPORT_NODE_MISSING', '未找到唯一可用的导出按钮，需要核对网站页面'),
        (('EXPORT_FAILED',), 'export', 'EXPORT_FAILED', '点击导出按钮失败，请核对页面遮挡或网站变化'),
        (('No Excel downloaded', 'DOWNLOAD_TIMEOUT'), 'download', 'DOWNLOAD_TIMEOUT', '等待超时，未下载到Excel；无法仅凭此确定登录或网络原因'),
        (('QUERY_TIMEOUT',), 'query', 'QUERY_TIMEOUT', '查询超时，未出现实际关键词记录'),
        (('NAVIGATION_FAILED',), 'query', 'NAVIGATION_FAILED', '打开卖家精灵页面失败，请核对虚拟机网络'),
        (('BROWSER_ERROR',), 'worker', 'BROWSER_ERROR', '浏览器进程发生异常，请核对虚拟机浏览器/Xvfb'),
        (('ASIN_MISMATCH', 'ASIN不一致', 'ASIN不符'), 'parse', 'ASIN_MISMATCH', '导出文件ASIN与任务不一致，需人工核对'),
        (('UPLOAD_FAILED', 'ERP网络', 'ERP返回非JSON'), 'upload', 'UPLOAD_FAILED', '上传ERP失败，优先恢复本地已有文件'),
        (('文件丢失', '原文件丢失', 'FILE_MISSING'), 'download', 'FILE_MISSING', '待补传文件丢失或无效，需要重新采集当天快照'),
        (('WORKER_INTERRUPTED', '进程中断', '租约过期'), 'worker', 'WORKER_INTERRUPTED', '执行进程中断或租约过期，可补救'),
        (('INVALID_EXCEL',), 'download', 'INVALID_EXCEL', '下载文件损坏或并非有效Excel'),
        (('SCHEMA_CHANGED',), 'parse', 'SCHEMA_CHANGED', '导出结构不符，需要核对网站格式或ERP清洗规则'),
    ]
    stage, code, summary = 'worker', 'UNKNOWN_ERROR', '原因尚未确定，请查看诊断并核对虚拟机日志'
    for needles, stage_value, code_value, summary_value in pairs:
        if any(needle.lower() in raw.lower() for needle in needles):
            stage, code, summary = stage_value, code_value, summary_value
            break
    else:
        if status == 'parse_failed':
            stage, code, summary = 'parse', 'SCHEMA_CHANGED', 'Excel清洗校验失败，需要核对原文件或清洗规则'
        elif status == 'downloaded':
            stage, code, summary = 'upload', 'UPLOAD_PENDING', 'Excel已下载，但尚未确认ERP入库'
        elif status in ('running', 'pending'):
            stage, code, summary = 'worker', 'IN_PROGRESS', '任务尚未结束'
        elif status == 'completed':
            stage, code, summary = 'parse', 'IMPORTED', '关键词已清洗入库'
    # 机器只允许已知错误码，不接收任意URL、SQL或可执行内容。
    if isinstance(diagnostic, dict) and diagnostic.get('stage') in STAGES:
        supplied_code = diagnostic.get('code')
        allowed = BLOCKED | {'EMPTY_EXPORT', 'DOWNLOAD_TIMEOUT', 'UPLOAD_FAILED', 'FILE_MISSING',
                             'WORKER_INTERRUPTED', 'INVALID_EXCEL', 'UPLOADED', 'IMPORTED',
                             'RESULTS_READY', 'DOWNLOADED', 'STARTED', 'UPLOAD_PENDING'}
        allowed |= {'QUERY_TIMEOUT', 'NAVIGATION_FAILED', 'BROWSER_ERROR', 'EXPORT_FAILED'}
        if supplied_code in allowed:
            stage, code = diagnostic['stage'], supplied_code
            summary = safe_message(diagnostic.get('message')) or summary
    if status == 'parse_failed':
        stage = 'parse'
    return {'stage': stage, 'stage_label': LABELS[stage], 'code': code,
            'message': summary, 'detail': safe_message(raw), 'requires_attention': code in BLOCKED,
            'suggestion': '处理虚拟机登录/验证/权限或导出页面后，确认加入补救队列' if code in BLOCKED
            else '优先补传已有文件；空表或缺失文件重新采集当天快照'}


def append_event(cur, task, stage, code, message):
    """在调用方事务内追加安全事件，重试不覆盖之前错误。"""
    cur.execute('''INSERT INTO sellersprite_task_events(task_id,attempt,stage,error_code,message,created_at)
        VALUES (%s,%s,%s,%s,%s,%s)''',
                (task['id'], task.get('attempts', 0), stage, code, safe_message(message), utcnow()))


def settle(cur, task, status, message='', diagnostic=None):
    """原任务状态落库时同步补救结果；只处理同ASIN/日期队列，不增加领取次数。"""
    info = diagnose(message, status, diagnostic)
    append_event(cur, task, info['stage'], info['code'], info['detail'] or info['message'])
    if info['code'] in ACCOUNT_BLOCKS:
        cur.execute('''UPDATE sellersprite_worker_state SET blocked=1,error_code=%s,message=%s,updated_at=%s WHERE id=1''',
                    (info['code'], info['message'], utcnow()))
    if status not in ('completed', 'failed', 'parse_failed', 'action_required'):
        return
    cur.execute("SELECT * FROM sellersprite_recovery_queue WHERE asin=%s AND record_date=%s AND status IN ('queued','running') FOR UPDATE",
                (task['asin'], task['record_date']))
    queue = cur.fetchone()
    if not queue:
        return
    state = 'completed' if status == 'completed' else 'blocked' if info['requires_attention'] or status == 'action_required' else 'exhausted' if queue['auto_attempts'] >= queue['max_attempts'] else 'queued'
    if info['code'] == 'FILE_MISSING' and queue['mode'] == 'upload':
        if queue['record_date'] != record_date():
            state = 'cancelled'  # 下一轮根据原任务错误生成今天重采，不补造旧日数据。
        else:
            cur.execute("UPDATE sellersprite_recovery_queue SET mode='collect' WHERE id=%s", (queue['id'],))
    elif info['code'] == 'UPLOAD_FAILED' and task.get('local_path'):
        cur.execute("UPDATE sellersprite_recovery_queue SET mode='upload' WHERE id=%s", (queue['id'],))
    # 一次手动请求只消费一次；后续失败只按尚未使用的自动预算续排，不产生无限自动循环。
    cur.execute('''UPDATE sellersprite_recovery_queue SET status=%s,last_error=%s,next_attempt_at=%s,
        request_source=%s,updated_at=%s WHERE id=%s''',
                (state, None if state == 'completed' else info['code'] + ': ' + info['message'], utcnow() + timedelta(hours=1),
                 queue['request_source'] if state == 'completed' else 'automatic', utcnow(), queue['id']))


def enqueue(task_id, actor='系统自动补救', confirm=False, automatic=False):
    """自动最多3次，手动每次排一次但总次数不限；已等待/执行的队列绝不改变来源或增加预算。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sellersprite_keyword_tasks WHERE id=%s FOR UPDATE', (task_id,))
            task = cur.fetchone()
            if not task:
                raise PipelineError('任务不存在', 404)
            cur.execute("SELECT enabled FROM sellersprite_asin_config WHERE asin=%s AND marketplace='US'", (task['asin'],))
            config = cur.fetchone()
            if not config or not config['enabled']:
                raise PipelineError('请先启用该ASIN采集配置', 409)
            if task['status'] == 'completed':
                raise PipelineError('该任务已经成功，无需补救', 409)
            if task.get('lease_until') and task['lease_until'] > utcnow():
                raise PipelineError('任务仍在执行，请等待租约结束', 409)
            info = diagnose(task.get('error_message'), task['status'])
            if (info['requires_attention'] or task['status'] == 'action_required') and not confirm:
                raise PipelineError('需要先处理登录、验证或文件问题，再明确确认入队', 409)
            mode = 'upload' if task.get('local_path') and task['record_date'] >= record_date() - timedelta(days=31) and (task['status'] == 'downloaded' or info['code'] in ('UPLOAD_FAILED', 'UPLOAD_PENDING')) else 'collect'
            day = task['record_date'] if mode == 'upload' else record_date()
            record_date(day)
            cur.execute("SELECT status FROM sellersprite_keyword_tasks WHERE asin=%s AND marketplace='US' AND record_date=%s AND period='last30days'", (task['asin'], day))
            target = cur.fetchone()
            if target and target['status'] == 'completed':
                raise PipelineError('目标日期已有成功快照，无需重复采集', 409)
            cur.execute('''INSERT INTO sellersprite_recovery_queue
                (source_task_id,asin,record_date,mode,status,request_source,next_attempt_at,requested_by,created_at,updated_at)
                VALUES (%s,%s,%s,%s,'queued',%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)''',
                        (task_id, task['asin'], day, mode, 'automatic' if automatic else 'manual',
                         utcnow().replace(microsecond=0), str(actor)[:255], utcnow(), utcnow()))
            identifier, changed = cur.lastrowid, cur.rowcount == 1
            cur.execute('SELECT * FROM sellersprite_recovery_queue WHERE id=%s FOR UPDATE', (identifier,))
            queue = cur.fetchone()
            if not automatic and queue['status'] in ('blocked', 'exhausted', 'cancelled'):
                if not confirm:
                    raise PipelineError('请确认问题已处理并追加一次补救', 409)
                cur.execute("UPDATE sellersprite_recovery_queue SET status='queued',request_source='manual',next_attempt_at=%s,requested_by=%s,updated_at=%s WHERE id=%s",
                            (utcnow().replace(microsecond=0), str(actor)[:255], utcnow(), identifier))
                changed = True
            if changed:
                append_event(cur, task, 'queue', 'ENQUEUED', f'补救队列#{identifier}；方式{mode}；目标日期{day}；请求人{actor}')
        conn.commit()
        return {'id': identifier, 'mode': queue['mode'], 'record_date': day.isoformat(), 'duplicate': not changed}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def prepare(limit=20, worker_id=''):
    """小时任务发现近期最新失败并返回到期队列；GET页面永不调用本方法。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('UPDATE sellersprite_worker_state SET last_seen_at=%s,worker_id=%s WHERE id=1', (utcnow(), str(worker_id)[:100]))
            conn.commit()  # 心跳单独提交，避免持账号锁再等待任务锁，与失败回写形成反向锁序。
            # 先回收明确过期的执行；事件与队列状态同事务提交。
            cur.execute("""SELECT * FROM sellersprite_keyword_tasks WHERE
                (status IN ('running','downloaded') AND (lease_until IS NULL OR lease_until<%s))
                OR (status='pending' AND updated_at<%s) ORDER BY id LIMIT 200 FOR UPDATE""", (utcnow(), utcnow() - timedelta(hours=2)))
            for task in cur.fetchall():
                message = 'UPLOAD_FAILED：下载完成后进程中断，等待补传' if task['status'] == 'downloaded' else 'WORKER_INTERRUPTED：执行租约过期'
                cur.execute("UPDATE sellersprite_keyword_tasks SET status='failed',error_message=%s,lease_until=NULL,updated_at=%s WHERE id=%s", (message, utcnow(), task['id']))
                settle(cur, task, 'failed', message)
            cur.execute('''SELECT t.id FROM sellersprite_keyword_tasks t JOIN sellersprite_asin_config c
                ON c.asin=t.asin AND c.marketplace=t.marketplace AND c.enabled=1
                WHERE t.marketplace='US' AND t.record_date>=%s AND t.status IN ('failed','parse_failed','action_required')
                AND NOT EXISTS (SELECT 1 FROM sellersprite_keyword_tasks newer WHERE newer.asin=t.asin
                    AND newer.marketplace=t.marketplace AND (newer.record_date>t.record_date OR (newer.record_date=t.record_date AND newer.id>t.id)))
                ORDER BY c.priority DESC,t.id LIMIT 200''', (record_date() - timedelta(days=7),))
            identifiers = [row['id'] for row in cur.fetchall()]
        conn.commit()
        for identifier in identifiers:
            try:
                enqueue(identifier, automatic=True)
            except PipelineError:
                pass  # 人工阻塞、成功和并发占用不自动解锁。
        with conn.cursor() as cur:
            # 旧日未消费的重采不伪装历史日期；取消后由最新失败重新生成今天队列。
            cur.execute("UPDATE sellersprite_recovery_queue SET status='cancelled',last_error='已跨日，请按最新失败创建当天补采',updated_at=%s WHERE mode='collect' AND record_date<%s AND status='queued'", (utcnow(), record_date()))
            cur.execute("UPDATE sellersprite_recovery_queue SET status='cancelled',last_error='原文件补传已超过31天，请重新采集今天快照',updated_at=%s WHERE record_date<%s AND status='queued'", (utcnow(), record_date() - timedelta(days=31)))
            cur.execute('''SELECT q.id AS recovery_id,q.asin,q.record_date,q.mode,q.source_task_id,
                COALESCE(target.local_path,t.local_path) AS local_path,COALESCE(target.file_name,t.file_name) AS file_name
                FROM sellersprite_recovery_queue q JOIN sellersprite_keyword_tasks t ON t.id=q.source_task_id
                LEFT JOIN sellersprite_keyword_tasks target ON target.asin=q.asin AND target.record_date=q.record_date
                    AND target.marketplace='US' AND target.period='last30days'
                JOIN sellersprite_asin_config c ON c.asin=q.asin AND c.marketplace='US' AND c.enabled=1
                WHERE q.status='queued' AND (q.request_source='manual' OR q.auto_attempts<q.max_attempts) AND q.next_attempt_at<=%s
                AND (q.mode='upload' OR NOT EXISTS (SELECT 1 FROM sellersprite_worker_state WHERE id=1 AND blocked=1))
                ORDER BY c.priority DESC,q.id LIMIT %s''', (utcnow(), limit))
            items = cur.fetchall()
        conn.commit()
        return [{**row, 'record_date': row['record_date'].isoformat(), 'marketplace': 'US'} for row in items]
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def claim_queue(cur, task, identifier):
    """在任务领取事务中占用同一队列；失败回滚时两者都不消耗次数。"""
    cur.execute('SELECT * FROM sellersprite_recovery_queue WHERE id=%s FOR UPDATE', (identifier,))
    queue = cur.fetchone()
    if not queue or queue['asin'] != task['asin'] or queue['record_date'] != task['record_date']:
        raise PipelineError('补救队列与目标任务不一致', 409)
    cur.execute("SELECT enabled FROM sellersprite_asin_config WHERE asin=%s AND marketplace='US'", (task['asin'],))
    config = cur.fetchone()
    if not config or not config['enabled']:
        raise PipelineError('ASIN已停用，停止新的补救领取', 409)
    if queue['status'] != 'queued' or (queue['request_source'] != 'manual' and queue['auto_attempts'] >= queue['max_attempts']) or queue['next_attempt_at'] > utcnow():
        raise PipelineError('队列未到执行时间、已占用或需人工处理', 409)
    if queue['mode'] == 'collect' and queue['record_date'] != record_date():
        raise PipelineError('跨日重采必须创建今天的快照任务', 409)
    if queue['mode'] == 'collect':
        cur.execute('SELECT blocked FROM sellersprite_worker_state WHERE id=1')
        state = cur.fetchone()
        if state and state['blocked']:
            raise PipelineError('采集账号已暂停，请处理登录/验证后在页面恢复', 409)
    cur.execute("UPDATE sellersprite_recovery_queue SET status='running',attempts=attempts+1,auto_attempts=auto_attempts+%s,updated_at=%s WHERE id=%s",
                (0 if queue['request_source'] == 'manual' else 1, utcnow(), identifier))
    append_event(cur, task, 'queue', 'MANUAL_CLAIMED' if queue['request_source'] == 'manual' else 'AUTO_CLAIMED',
                 '领取一次手动补救，不占自动次数' if queue['request_source'] == 'manual' else '领取一次自动补救，最多3次')


def resume_account(actor):
    """人工确认浏览器问题已处理后恢复账号；其他格式错误及次数耗尽不会解除。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sellersprite_worker_state SET blocked=0,error_code=NULL,message=%s,updated_at=%s WHERE id=1", (safe_message('人工确认恢复：' + str(actor)), utcnow()))
            cur.execute("""UPDATE sellersprite_recovery_queue SET status=IF(request_source='manual' OR auto_attempts<max_attempts,'queued','exhausted'),
                next_attempt_at=%s,updated_at=%s WHERE status='blocked' AND
                (last_error LIKE 'LOGIN_REQUIRED:%%' OR last_error LIKE 'VERIFICATION_REQUIRED:%%' OR last_error LIKE 'PERMISSION_DENIED:%%')""", (utcnow().replace(microsecond=0), utcnow()))
        conn.commit()
        return {'resumed': True}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def cancel(identifier, actor):
    """取消尚未执行的当天队列，自动发现不会在同一天重新打开；执行中不可强制取消。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sellersprite_recovery_queue SET status='cancelled',last_error=%s,updated_at=%s WHERE id=%s AND status IN ('queued','blocked','exhausted')",
                        (safe_message('人工取消：' + str(actor)), utcnow(), identifier))
            if cur.rowcount != 1:
                raise PipelineError('队列已执行、正在执行或已被取消，请刷新', 409)
        conn.commit()
        return {'id': identifier, 'status': 'cancelled'}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def history(asin, page=1, size=20):
    """返回浏览器可见任务/阶段/队列，显式排除领取令牌、虚拟机路径和文件私有路径。"""
    from services.sellersprite_portal import asin_value, integer, iso
    asin, page, size = asin_value(asin), integer(page, '页码'), integer(size, '每页数量', 1, 50)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM sellersprite_keyword_tasks WHERE asin=%s AND marketplace='US'", (asin,))
            total = cur.fetchone()['n']
            cur.execute('''SELECT id,asin,record_date,status,attempts,file_name,latest_file_id,row_count,
                error_message,started_at,completed_at,updated_at,lease_until FROM sellersprite_keyword_tasks
                WHERE asin=%s AND marketplace='US' ORDER BY record_date DESC,id DESC LIMIT %s OFFSET %s''', (asin, size, (page - 1) * size))
            rows = cur.fetchall()
            for row in rows:
                row['diagnostic'] = diagnose(row.get('error_message'), row['status'])
                row['error_message'] = safe_message(row['error_message'])
                cur.execute('SELECT stage,error_code,message,created_at,attempt FROM sellersprite_task_events WHERE task_id=%s ORDER BY id DESC LIMIT 50', (row['id'],))
                row['events'] = [{key: iso(value) for key, value in event.items()} for event in cur.fetchall()]
                if row['events']:
                    event = row['events'][0]
                    if event['stage'] != 'queue':
                        row['diagnostic'] = diagnose(row['error_message'], row['status'], {'stage': event['stage'], 'code': event['error_code'], 'message': event['message']})
                row['active'] = bool(row['lease_until'] and row['lease_until'] > utcnow())
                row.pop('lease_until')
                for key, value in list(row.items()):
                    if key not in ('events', 'diagnostic'):
                        row[key] = iso(value)
            cur.execute('''SELECT id,source_task_id,record_date,mode,status,attempts,auto_attempts,request_source,max_attempts,next_attempt_at,
                last_error,requested_by,updated_at FROM sellersprite_recovery_queue WHERE asin=%s ORDER BY id DESC LIMIT 20''', (asin,))
            queues = [{key: iso(value) for key, value in row.items()} for row in cur.fetchall()]
            cur.execute('SELECT blocked,error_code,message,worker_id,last_seen_at,updated_at FROM sellersprite_worker_state WHERE id=1')
            state = {key: iso(value) for key, value in (cur.fetchone() or {}).items()}
        return {'list': rows, 'queues': queues, 'total': total, 'page': page, 'worker': state}
    finally:
        conn.close()
