"""计划任务短事务与审计记录；连接不跨业务执行持有。"""
from contextlib import contextmanager
from datetime import datetime, timedelta
import uuid

from services.mysql_service import get_db_connection
from services.cron_tasks.locking import host_identity

ACTIVE = ('pending', 'running', 'unknown')

# 只允许明确结束的状态；未知状态及占用引用永不按年龄自动删除。
_EXPIRED_RUN_WHERE = """r.status IN ('finished','failed','skipped','expired','interrupted')
    AND r.finished_at < %s
    AND NOT EXISTS (SELECT 1 FROM cron_task_slots s WHERE s.active_run_id=r.id)
    AND NOT EXISTS (SELECT 1 FROM cron_task_runtime t WHERE t.active_manual_id=r.id)"""


class TaskError(Exception):
    def __init__(self, message, status=409):
        super().__init__(message)
        self.status = status


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


def _runtime(cursor, lock=False):
    cursor.execute('SELECT * FROM cron_task_runtime WHERE id=1' + (' FOR UPDATE' if lock else ''))
    row = cursor.fetchone()
    if not row:
        raise TaskError('计划任务执行服务尚未初始化', 503)
    return row


def _slot(cursor, group):
    cursor.execute('SELECT active_run_id FROM cron_task_slots WHERE group_key=%s FOR UPDATE', (group,))
    row = cursor.fetchone()
    if row is None:
        raise TaskError('计划任务占用表尚未初始化', 503)
    return row['active_run_id']


def _release(cursor, run):
    cursor.execute('UPDATE cron_task_slots SET active_run_id=NULL WHERE group_key=%s AND active_run_id=%s',
                   (run['group_key'], run['id']))
    cursor.execute('UPDATE cron_task_runtime SET active_manual_id=NULL WHERE id=1 AND active_manual_id=%s', (run['id'],))


def snapshot():
    with transaction() as cursor:
        runtime = _runtime(cursor)
        cursor.execute('SELECT s.group_key,r.* FROM cron_task_slots s LEFT JOIN cron_task_runs r ON r.id=s.active_run_id')
        active = {r['group_key']: r for r in cursor.fetchall() if r.get('id')}
        # 每个固定任务只取最新记录，不扫描/返回历史正文。
        cursor.execute('''SELECT r.* FROM cron_task_runs r JOIN
            (SELECT task_id,MAX(id) AS id FROM cron_task_runs GROUP BY task_id) t ON t.id=r.id''')
        latest = {r['task_id']: r for r in cursor.fetchall()}
    return runtime, active, latest


def worker_online(runtime, server_id, uid):
    stamp = runtime.get('heartbeat_at')
    age = (datetime.utcnow() - stamp).total_seconds() if stamp else 9999
    # MySQL DATETIME会将微秒四舍五入到整秒，允许2秒精度差，不能将正常心跳误判成未来时间。
    return bool(runtime.get('accepting') and -2 <= age <= 15 and runtime['server_id'] == server_id and runtime['worker_uid'] == uid)


def duplicate(actor_id, request_id, task_id):
    with transaction() as cursor:
        cursor.execute('SELECT * FROM cron_task_runs WHERE actor_id=%s AND request_id=%s', (actor_id, request_id))
        row = cursor.fetchone()
    if row and row['task_id'] != task_id:
        raise TaskError('请求编号已用于其他任务，请刷新后重试')
    return row


def submit(task, actor, request_id, server_id, uid):
    now = datetime.utcnow()
    with transaction() as cursor:
        runtime = _runtime(cursor, True)
        cursor.execute('SELECT * FROM cron_task_runs WHERE actor_id=%s AND request_id=%s', (actor['id'], request_id))
        existing = cursor.fetchone()
        if existing:
            if existing['task_id'] != task.id:
                raise TaskError('请求编号已用于其他任务')
            return existing
        if not worker_online(runtime, server_id, uid):
            raise TaskError('执行服务未就绪，请稍后刷新或联系管理员', 503)
        if _slot(cursor, task.group):
            raise TaskError('此任务正在执行或状态待核实，请先查看执行记录')
        cursor.execute('SELECT created_at FROM cron_task_runs WHERE task_id=%s AND source=%s ORDER BY id DESC LIMIT 1', (task.id, 'manual'))
        last = cursor.fetchone()
        if last and now - last['created_at'] < timedelta(seconds=60):
            raise TaskError('此任务刚刚触发过，请间隔60秒后再执行', 429)
        cursor.execute('SELECT created_at FROM cron_task_runs WHERE actor_id=%s ORDER BY id DESC LIMIT 1', (actor['id'],))
        last = cursor.fetchone()
        if last and now - last['created_at'] < timedelta(seconds=10):
            raise TaskError('操作较频繁，请稍后再试', 429)
        cursor.execute('''INSERT INTO cron_task_runs
            (run_key,task_id,group_key,source,actor_id,actor_name,request_id,status,summary,
             worker_instance,server_id,created_at) VALUES (%s,%s,%s,'manual',%s,%s,%s,'pending',%s,%s,%s,%s)''',
            (uuid.uuid4().hex, task.id, task.group, actor['id'], (actor.get('nickname') or actor.get('username') or str(actor['id']))[:80],
             request_id, '请求已接收，等待执行服务启动；30秒内未启动将失效', runtime['worker_instance'], server_id, now))
        run_id = cursor.lastrowid
        cursor.execute('UPDATE cron_task_slots SET active_run_id=%s WHERE group_key=%s', (run_id, task.group))
        cursor.execute('SELECT * FROM cron_task_runs WHERE id=%s', (run_id,))
        return cursor.fetchone()


def begin_run(task, server_id, run_id=None, instance=None):
    now = datetime.utcnow()
    with transaction() as cursor:
        runtime = _runtime(cursor, True)  # 与提交/回收统一锁顺序：服务行→资源占用→记录。
        if runtime.get('host_key') and runtime['host_key'] != host_identity():
            raise TaskError('当前任务表已绑定其他服务器，禁止跨主机执行', 503)
        occupied = _slot(cursor, task.group)
        if run_id is None:
            status = 'skipped' if occupied else 'running'
            cursor.execute('''INSERT INTO cron_task_runs
                (run_key,task_id,group_key,source,status,summary,server_id,created_at,started_at,heartbeat_at,finished_at)
                VALUES (%s,%s,%s,'scheduled',%s,%s,%s,%s,%s,%s,%s)''',
                (uuid.uuid4().hex, task.id, task.group, status, '此任务已占用，本轮跳过' if occupied else '正在执行原任务脚本',
                 server_id, now, None if occupied else now, now, now if occupied else None))
            run_id = cursor.lastrowid
            if not occupied:
                cursor.execute('UPDATE cron_task_slots SET active_run_id=%s WHERE group_key=%s', (run_id, task.group))
        else:
            import os
            if runtime['worker_instance'] != instance or not worker_online(runtime, server_id, os.getuid()):
                raise TaskError('执行服务已停止或实例已变化，当前请求不执行')
            cursor.execute('SELECT * FROM cron_task_runs WHERE id=%s FOR UPDATE', (run_id,))
            row = cursor.fetchone()
            if not row or row['task_id'] != task.id or row['group_key'] != task.group or row['status'] != 'pending' or occupied != run_id:
                raise TaskError('请求已失效或不属于当前任务')
            if row['worker_instance'] != instance or row['server_id'] != server_id or now - row['created_at'] > timedelta(seconds=30):
                raise TaskError('请求已过期，不会延迟执行')
            cursor.execute("UPDATE cron_task_runs SET status='running',started_at=%s,heartbeat_at=%s,summary=%s WHERE id=%s",
                           (now, now, '正在执行原任务脚本', run_id))
        cursor.execute('SELECT * FROM cron_task_runs WHERE id=%s', (run_id,))
        return cursor.fetchone()


def scheduled_skip(task, server_id):
    with transaction() as cursor:
        now = datetime.utcnow()
        cursor.execute('''INSERT INTO cron_task_runs
            (run_key,task_id,group_key,source,status,summary,server_id,created_at,finished_at)
            VALUES (%s,%s,%s,'scheduled','skipped',%s,%s,%s,%s)''',
            (uuid.uuid4().hex, task.id, task.group, '此任务持有执行锁，本轮跳过', server_id, now, now))


def finish(run_id, status, summary, exit_code=None):
    with transaction() as cursor:
        _runtime(cursor, True)
        cursor.execute('SELECT group_key FROM cron_task_runs WHERE id=%s', (run_id,))
        group = cursor.fetchone()
        if not group:
            return
        _slot(cursor, group['group_key'])
        cursor.execute('SELECT * FROM cron_task_runs WHERE id=%s FOR UPDATE', (run_id,))
        row = cursor.fetchone()
        if row['status'] not in ACTIVE:
            return
        cursor.execute('UPDATE cron_task_runs SET status=%s,summary=%s,exit_code=%s,finished_at=%s WHERE id=%s',
                       (status, summary[:500], exit_code, datetime.utcnow(), run_id))
        _release(cursor, row)


def run_heartbeat(run_id, pid):
    with transaction() as cursor:
        cursor.execute("UPDATE cron_task_runs SET heartbeat_at=%s,process_id=%s WHERE id=%s AND status='running'",
                       (datetime.utcnow(), pid, run_id))


def worker_heartbeat(instance, server_id, uid, accepting=True):
    with transaction() as cursor:
        runtime = _runtime(cursor, True)
        host_key = host_identity()
        if runtime.get('host_key') and runtime['host_key'] != host_key:
            raise TaskError('执行表已绑定其他服务器，不能自动接管或清理其任务', 503)
        if not runtime.get('host_key'):
            cursor.execute('SELECT r.server_id FROM cron_task_slots s JOIN cron_task_runs r ON r.id=s.active_run_id')
            if any(r['server_id'] != server_id for r in cursor.fetchall()):
                raise TaskError('首次绑定前发现其他服务器遗留占用，需管理员核对', 503)
        cursor.execute('''UPDATE cron_task_runtime SET worker_instance=%s,server_id=%s,worker_uid=%s,
            heartbeat_at=%s,accepting=%s,host_key=%s WHERE id=1''', (instance, server_id, uid, datetime.utcnow(), int(accepting), host_key))


def pending(instance):
    with transaction() as cursor:
        cursor.execute("SELECT * FROM cron_task_runs WHERE status='pending' AND worker_instance=%s ORDER BY id LIMIT 1", (instance,))
        return cursor.fetchone()


def active_runs():
    with transaction() as cursor:
        cursor.execute('SELECT r.* FROM cron_task_slots s JOIN cron_task_runs r ON r.id=s.active_run_id')
        return list(cursor.fetchall())


def history(task_id, page, per_page):
    with transaction() as cursor:
        cursor.execute('SELECT COUNT(*) AS total FROM cron_task_runs WHERE task_id=%s', (task_id,))
        total = cursor.fetchone()['total']
        cursor.execute('SELECT * FROM cron_task_runs WHERE task_id=%s ORDER BY id DESC LIMIT %s OFFSET %s',
                       (task_id, per_page, (page - 1) * per_page))
        return list(cursor.fetchall()), total


def get_run(run_id):
    with transaction() as cursor:
        cursor.execute('SELECT * FROM cron_task_runs WHERE id=%s', (run_id,))
        return cursor.fetchone()


def _retention_host(cursor, lock=False):
    runtime = _runtime(cursor, lock)
    host = host_identity()
    if not host or runtime.get('host_key') != host:
        raise TaskError('执行记录清理只能在已绑定的同一服务器执行', 503)


def expired_runs(cutoff):
    """候选只读；按真实结束时间，单批最多200条，不回收任何任务占用。"""
    with transaction() as cursor:
        _retention_host(cursor)
        cursor.execute('SELECT r.id,r.run_key FROM cron_task_runs r WHERE ' + _EXPIRED_RUN_WHERE +
                       ' ORDER BY r.id LIMIT 200', (cutoff,))
        return list(cursor.fetchall())


def delete_expired_runs(run_ids, cutoff):
    """文件处理后再短事务删除，并在统一服务锁内重新核对结束状态/占用。"""
    if not run_ids:
        return 0
    if len(run_ids) > 200:
        raise ValueError('执行记录清理超过单批上限')
    with transaction() as cursor:
        _retention_host(cursor, True)
        placeholders = ','.join(['%s'] * len(run_ids))
        cursor.execute('DELETE r FROM cron_task_runs r WHERE ' + _EXPIRED_RUN_WHERE +
                       f' AND r.id IN ({placeholders})', (cutoff, *run_ids))
        return cursor.rowcount
