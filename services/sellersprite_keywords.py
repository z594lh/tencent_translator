"""关键词反查任务Service：领取续租、原件归档、关键词清洗入库和进度。

只处理关键词业务表；Excel解析由sellersprite_keyword_excel.py负责，
竞品任务在sellersprite_competitors.py，共用错误/日期/存储配置在sellersprite_common.py。"""
import hashlib
import hmac
import json
import os
import re
import secrets
import tempfile
from datetime import timedelta
from pathlib import Path

from services.mysql_service import get_db_connection
from services.sellersprite_common import PipelineError, utcnow, record_date, storage_root, MAX_FILE_SIZE
from services.sellersprite_keyword_excel import parse_excel, ExcelValidationError

US_MARKETPLACE = 'ATVPDKIKX0DER'
PARSER_VERSION = '1'


def candidates():
    """人工启用配置决定待抓范围；店铺/SKU仅补来源，不按库存销量过滤。"""
    from services.sellersprite_portal import configured_candidates, PortalError
    try:
        return configured_candidates()
    except PortalError as exc:
        raise PipelineError(str(exc),exc.code) from None


def pending_asins(day=None, limit=50):
    """列出正常日采候选；补救队列和账号阻塞由专用小时入口处理。"""
    day = record_date(day)
    eligible = candidates()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT blocked FROM sellersprite_worker_state WHERE id=1')
            state = cur.fetchone()
            if state and state['blocked']:
                return [], 0
            cur.execute("SELECT asin,status,lease_until FROM sellersprite_keyword_tasks WHERE marketplace='US' AND record_date=%s AND period='last30days'", (day,))
            existing = {row['asin']: row for row in cur.fetchall()}
            cur.execute("SELECT asin FROM sellersprite_recovery_queue WHERE record_date=%s AND status IN ('queued','running','blocked','exhausted')", (day,))
            recovering = {row['asin'] for row in cur.fetchall()}
        result = []
        for asin, sources in eligible.items():
            if asin in recovering:
                continue
            task = existing.get(asin)
            if task and (task['status'] == 'completed' or (task['lease_until'] and task['lease_until'] > utcnow()
                                                           and task['status'] in ('running', 'downloaded'))):
                continue
            result.append({'asin': asin, 'marketplace': 'US', 'record_date': day.isoformat(), 'sources': sources})
        return result[:limit], len(result)
    finally:
        conn.close()


def claim_task(asin, day, worker_id, recovery_id=None):
    """同事务领取任务与补救队列；执行日志保留旧失败而不是清掉唯一证据。"""
    from services import sellersprite_keyword_recovery as recovery
    if recovery_id is not None and (isinstance(recovery_id, bool) or not str(recovery_id).isdigit()):
        raise PipelineError('补救编号无效')
    asin = str(asin or '').strip().upper()
    if not re.fullmatch(r'[A-Z0-9]{10}', asin):
        raise PipelineError('ASIN格式错误')
    worker_id = str(worker_id or '').strip()
    if not worker_id or len(worker_id) > 100:
        raise PipelineError('worker_id必填且不超过100字符')
    day = record_date(day)
    eligible = candidates()
    sources = eligible.get(asin)
    # 已领取的历史任务允许恢复上传，即使SKU此时已不再符合筛选条件。
    conn = get_db_connection()
    now = utcnow()
    token = secrets.token_hex(32)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM sellersprite_keyword_tasks WHERE marketplace='US' AND asin=%s AND record_date=%s AND period='last30days'", (asin, day))
            existing = cur.fetchone()
            if not existing and asin not in eligible:
                raise PipelineError('该ASIN不在美国站待抓取范围', 404)
            cur.execute("""INSERT INTO sellersprite_keyword_tasks
                (asin,marketplace,record_date,period,sources_json,status,created_at,updated_at)
                VALUES (%s,'US',%s,'last30days',%s,'pending',%s,%s)
                ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
                        (asin, day, json.dumps(sources or [], ensure_ascii=False), now, now))
            task_id = cur.lastrowid
            cur.execute('SELECT * FROM sellersprite_keyword_tasks WHERE id=%s FOR UPDATE', (task_id,))
            task = cur.fetchone()
            if task['status'] == 'completed':
                recovery.settle(cur, task, 'completed')
                conn.commit()
                return {'id': task_id, 'status': 'completed', 'asin': asin, 'row_count': task['row_count']}
            if task['lease_until'] and task['lease_until'] > now and task['status'] in ('running', 'downloaded'):
                raise PipelineError('任务已被领取，等待租约结束后重试', 409)
            if recovery_id is not None:
                recovery.claim_queue(cur, task, int(recovery_id))
            else:
                cur.execute("SELECT id FROM sellersprite_recovery_queue WHERE asin=%s AND record_date=%s AND status IN ('queued','running','blocked','exhausted')", (asin, day))
                if cur.fetchone():
                    raise PipelineError('该任务由补救队列管理，请等待小时任务', 409)
            if task.get('error_message'):
                info = recovery.diagnose(task['error_message'], task['status'])
                recovery.append_event(cur, task, info['stage'], info['code'], task['error_message'])
            lease = now + timedelta(hours=2)
            cur.execute("""UPDATE sellersprite_keyword_tasks SET status='running',worker_id=%s,claim_token=%s,
                lease_until=%s,attempts=attempts+1,started_at=%s,updated_at=%s,error_message=NULL
                WHERE id=%s""", (worker_id, token, lease, now, now, task_id))
            recovery.append_event(cur, {**task, 'attempts': task['attempts'] + 1}, 'worker', 'STARTED', '已领取任务，准备查询或恢复上传')
        conn.commit()
        return {'id': task_id, 'asin': asin, 'marketplace': 'US', 'record_date': day.isoformat(),
                'claim_token': token, 'lease_until': lease.isoformat() + 'Z', 'status': 'running'}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _locked_task(cur, task_id, token, allow_completed=False):
    cur.execute('SELECT * FROM sellersprite_keyword_tasks WHERE id=%s FOR UPDATE', (task_id,))
    task = cur.fetchone()
    if not task:
        raise PipelineError('任务不存在', 404)
    if not token or not hmac.compare_digest(str(task['claim_token'] or ''), str(token)):
        raise PipelineError('领取令牌不匹配', 409)
    if task['status'] == 'completed' and allow_completed:
        return task
    if task['status'] not in ('running', 'downloaded') or not task['lease_until'] or task['lease_until'] < utcnow():
        raise PipelineError('任务租约已失效，需要重新领取', 409)
    return task


def update_progress(task_id, token, data):
    """接收令牌绑定的阶段或失败，心跳不重复写事件；失败同步限次队列。"""
    from services import sellersprite_keyword_recovery as recovery
    status = data.get('status')
    if status not in ('running', 'downloaded', 'failed', 'action_required'):
        raise PipelineError('不允许的进度状态')
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            task = _locked_task(cur, task_id, token)
            local_path = str(data.get('local_path') or task['local_path'] or '')[:1000] or None
            file_name = str(data.get('file_name') or task['file_name'] or '')[:255] or None
            if status == 'downloaded' and (not local_path or not file_name):
                raise PipelineError('下载完成需要文件名和虚拟机路径')
            lease = utcnow() + timedelta(hours=2) if status in ('running', 'downloaded') else None
            error = recovery.safe_message(data.get('error_message')) or None
            cur.execute("""UPDATE sellersprite_keyword_tasks SET status=%s,local_path=%s,file_name=%s,
                lease_until=%s,error_message=%s,updated_at=%s WHERE id=%s""",
                        (status, local_path, file_name, lease, error, utcnow(), task_id))
            if data.get('diagnostic') or status in ('downloaded', 'failed', 'action_required'):
                recovery.settle(cur, task, status, error, data.get('diagnostic'))
        conn.commit()
        return {'id': task_id, 'status': status}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def receive_file(task_id, token, upload, expected_hash):
    """原文件不可变保存；文件记录、明细和任务完成在同一数据库事务内提交。"""
    from services import sellersprite_keyword_recovery as recovery
    if not upload.filename or not upload.filename.lower().endswith('.xlsx'):
        raise PipelineError('仅接受卖家精灵原始xlsx文件')
    if not re.fullmatch(r'[a-f0-9]{64}', str(expected_hash or '')):
        raise PipelineError('必须提供sha256校验值')
    root = storage_root()
    root.mkdir(parents=True, exist_ok=True)
    digest, size = hashlib.sha256(), 0
    fd, temp_name = tempfile.mkstemp(prefix='.upload-', suffix='.xlsx', dir=root)
    temporary = Path(temp_name)
    conn = None
    try:
        with os.fdopen(fd, 'wb') as target:
            while True:
                chunk = upload.stream.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_FILE_SIZE:
                    raise PipelineError('Excel超过20MB限制', 413)
                digest.update(chunk)
                target.write(chunk)
        sha = digest.hexdigest()
        if not hmac.compare_digest(sha, expected_hash):
            raise PipelineError('文件SHA256校验不匹配')
        conn = get_db_connection()
        with conn.cursor() as cur:
            task = _locked_task(cur, task_id, token, allow_completed=True)
            cur.execute('SELECT id,status,row_count,error_message FROM sellersprite_keyword_files WHERE task_id=%s AND sha256=%s', (task_id, sha))
            previous = cur.fetchone()
            if previous:
                if previous['status'] == 'imported':
                    cur.execute("UPDATE sellersprite_keyword_tasks SET status='completed',latest_file_id=%s,row_count=%s,error_message=NULL,lease_until=NULL,completed_at=%s,updated_at=%s WHERE id=%s", (previous['id'], previous['row_count'], utcnow(), utcnow(), task_id))
                else:
                    cur.execute("UPDATE sellersprite_keyword_tasks SET status='parse_failed',latest_file_id=%s,error_message=%s,lease_until=NULL,updated_at=%s WHERE id=%s", (previous['id'], previous['error_message'], utcnow(), task_id))
                recovery.settle(cur, task, 'completed' if previous['status'] == 'imported' else 'parse_failed', previous.get('error_message'))
                conn.commit()
                return {**previous, 'file_id': previous['id'], 'task_id': task_id, 'duplicate': True}
            if task['status'] == 'completed':
                raise PipelineError('已完成任务不能上传另一份文件', 409)
            try:
                rows, metadata = parse_excel(temporary, task['asin'], task['marketplace'])
                error = None
            except Exception as exc:
                error = str(exc)[:1000] if isinstance(exc, ExcelValidationError) else f'Excel读取失败（{type(exc).__name__}）'
                rows, metadata = [], {}
            status = 'parse_failed' if error else 'imported'
            relative = Path(task['record_date'].isoformat()) / f'{task_id}_{sha}.xlsx'
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists():
                temporary.replace(destination)
            file_name = f"US_{task['asin']}_{task['record_date'].isoformat()}.xlsx"
            now = utcnow()
            cur.execute("""INSERT INTO sellersprite_keyword_files
                (task_id,sha256,file_name,storage_path,byte_size,status,parser_version,row_count,
                 metadata_json,error_message,uploaded_at,imported_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (task_id, sha, file_name, relative.as_posix(), size, status, PARSER_VERSION,
                         len(rows), json.dumps(metadata, ensure_ascii=False, default=str), error, now, now if not error else None))
            file_id = cur.lastrowid
            if rows:
                columns = [key for key in rows[0] if key != 'keyword_normalized']
                names = ['file_id', 'task_id', 'keyword_hash'] + columns
                sql = 'INSERT INTO sellersprite_keyword_rows (' + ','.join(names) + ') VALUES (' + ','.join(['%s'] * len(names)) + ')'
                for start in range(0, len(rows), 200):
                    values = [(file_id, task_id, hashlib.sha256(row['keyword_normalized'].encode()).hexdigest(),
                               *(row[column] for column in columns)) for row in rows[start:start + 200]]
                    cur.executemany(sql, values)
            cur.execute("""UPDATE sellersprite_keyword_tasks SET status=%s,latest_file_id=%s,file_name=%s,
                row_count=%s,error_message=%s,lease_until=NULL,completed_at=%s,updated_at=%s WHERE id=%s""",
                        ('parse_failed' if error else 'completed', file_id, file_name, len(rows), error,
                         now if not error else None, now, task_id))
            recovery.settle(cur, task, 'parse_failed' if error else 'completed', error)
        conn.commit()
        return {'task_id': task_id, 'file_id': file_id, 'status': status, 'row_count': len(rows), 'error_message': error}
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        temporary.unlink(missing_ok=True)
        if conn:
            conn.close()


def list_tasks(day=None, status=None, page=1, page_size=50):
    day = record_date(day, allow_history=True)
    where, params = 't.record_date=%s', [day]
    if status:
        where += ' AND t.status=%s'
        params.append(status)
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS total FROM sellersprite_keyword_tasks t WHERE {where}', params)
            total = cur.fetchone()['total']
            cur.execute(f"""SELECT t.id,t.asin,t.marketplace,t.record_date,t.period,t.status,t.sources_json,
                t.worker_id,t.attempts,t.local_path,t.file_name,t.latest_file_id,t.row_count,t.error_message,
                t.lease_until,t.started_at,t.completed_at,t.updated_at,f.storage_path,f.sha256,f.byte_size
                FROM sellersprite_keyword_tasks t LEFT JOIN sellersprite_keyword_files f ON f.id=t.latest_file_id
                WHERE {where} ORDER BY t.id DESC LIMIT %s OFFSET %s""", params + [page_size, (page - 1) * page_size])
            return {'items': cur.fetchall(), 'total': total, 'page': page, 'page_size': page_size}
    finally:
        conn.close()


def file_details(file_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM sellersprite_keyword_files WHERE id=%s', (file_id,))
            row = cur.fetchone()
            if not row:
                raise PipelineError('文件不存在', 404)
            path = (storage_root() / row['storage_path']).resolve()
            if not path.is_relative_to(storage_root()) or not path.is_file():
                raise PipelineError('归档文件不存在', 404)
            return path, row['file_name']
    finally:
        conn.close()


def keyword_rows(task_id, page=1, page_size=100):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT latest_file_id,row_count FROM sellersprite_keyword_tasks WHERE id=%s AND status='completed'", (task_id,))
            task = cur.fetchone()
            if not task:
                raise PipelineError('任务尚未成功入库', 404)
            cur.execute('SELECT * FROM sellersprite_keyword_rows WHERE file_id=%s ORDER BY source_row LIMIT %s OFFSET %s',
                        (task['latest_file_id'], page_size, (page - 1) * page_size))
            return {'items': cur.fetchall(), 'total': task['row_count'], 'page': page, 'page_size': page_size}
    finally:
        conn.close()
