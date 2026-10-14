"""任务查询、权限之后的业务门禁及有限日志读取。"""
from datetime import datetime, timezone
import os
import re
import uuid

from services.cron_tasks import catalog, repository as repo
from services.cron_tasks.locking import identity, is_busy, safe_directory

STATUS_LABELS = {
    'pending': '启动中', 'running': '执行中', 'unknown': '状态待核实',
    'finished': '执行结束', 'failed': '执行异常', 'skipped': '本轮跳过',
    'expired': '请求已失效', 'interrupted': '执行中断', 'idle': '空闲',
    'unmanaged': '未接入防重', 'external': '相关操作执行中', 'blocked': '关联任务占用',
}


def public_run(row):
    if not row:
        return None
    keys = ('id', 'task_id', 'source', 'actor_name', 'status', 'summary', 'created_at',
            'started_at', 'finished_at', 'heartbeat_at', 'exit_code')
    result = {key: row.get(key) for key in keys}
    for key in ('created_at', 'started_at', 'finished_at', 'heartbeat_at'):
        if result[key]:
            result[key] = result[key].replace(tzinfo=timezone.utc).isoformat()
    start = row.get('started_at')
    result['duration_seconds'] = max(0, int(((row.get('finished_at') or datetime.utcnow()) - start).total_seconds())) if start else None
    result['status_label'] = STATUS_LABELS.get(row['status'], '状态待核实')
    return result


def manual_gate(discovery=None):
    discovery = discovery or catalog.discover()
    if not discovery['supported'] or not discovery['ready']:
        raise repo.TaskError(discovery['message'], 503)
    if os.environ.get('CRON_MANUAL_ENABLED') != '1':
        raise repo.TaskError('服务器尚未开放手动执行，请由管理员完成上线核对后开启', 503)
    return discovery


def list_tasks(keyword, page, per_page, can_execute):
    discovery = catalog.discover()
    rows = discovery['tasks']
    message = discovery['message']
    ready = False
    runtime, active, latest = None, {}, {}
    if discovery['supported']:
        try:
            runtime, active, latest = repo.snapshot()
            server, uid = identity()
            manual_gate(discovery)
            ready = repo.worker_online(runtime, server, uid)
            if not ready:
                message = '执行服务未就绪；任务仅可查看，请联系管理员。'
        except repo.TaskError as exc:
            message = str(exc)
        except Exception:
            message = '执行记录或锁服务未就绪，请先完成数据库迁移和执行服务配置。'
    for task in rows:
        run = active.get(task['group'])
        last = latest.get(task['task_id'])
        status = ('idle' if runtime is not None else 'unknown') if task['managed'] else 'unmanaged'
        if run:
            stamp = run.get('heartbeat_at') or run['created_at']
            status = run['status'] if (datetime.utcnow() - stamp).total_seconds() <= 30 else 'unknown'
            if status != 'unknown' and run['task_id'] != task['task_id']:
                status = 'blocked'
        elif task['managed']:
            try:
                if is_busy(task['group']):
                    status = 'external'
            except Exception:
                status = 'unknown'
        task.update(status=status, status_label=STATUS_LABELS.get(status, '状态待核实'),
                    active_run=public_run(run), last_result=public_run(last))
        reason = ''
        if not task['manual_allowed']:
            reason = '此任务涉及财务或自动入账，本版不开放手动执行'
        elif not can_execute:
            reason = '需要“计划任务立即执行”权限'
        elif not ready or not task['managed']:
            reason = message or '任务尚未接入统一防重入口'
        elif status != 'idle':
            reason = '此任务正在执行或状态待核实'
        task['can_execute'] = not bool(reason)
        task['disabled_reason'] = reason
    stats = {'total': len(rows), 'running': sum(t['status'] == 'running' for t in rows),
             'upcoming': sum(bool(t['next_run']) and 0 <= (datetime.fromisoformat(t['next_run']) - datetime.now(timezone.utc)).total_seconds() <= 3600 for t in rows)}
    if keyword:
        rows = [t for t in rows if keyword in (t['task_name'] + t['description'] + (t['task_id'] or '')).lower()]
    total = len(rows)
    return {'success': True, 'tasks': rows[(page - 1) * per_page:page * per_page],
            'pagination': {'page': page, 'per_page': per_page, 'total': total},
            'capabilities': {'supported': discovery['supported'], 'ready': ready, 'can_execute': can_execute, 'message': message}, 'stats': stats}


def submit(task_id, data, actor):
    if not isinstance(data, dict) or set(data) != {'request_id', 'confirm'} or data['confirm'] is not True:
        raise repo.TaskError('请确认任务后提交；仅支持固定任务，不接受额外命令或参数', 400)
    request_id = data['request_id']
    try:
        if not isinstance(request_id, str) or str(uuid.UUID(request_id)) != request_id:
            raise ValueError()
    except (ValueError, AttributeError):
        raise repo.TaskError('请求编号格式错误', 400) from None
    task = catalog.get_task(task_id)
    if not task.manual:
        raise repo.TaskError('此任务不开放手动执行', 403)
    # 原请求响应丢失后，返回已有记录；不会因服务离线或锁占用而重复执行。
    existing = repo.duplicate(actor['id'], request_id, task_id) if catalog.supported() else None
    if existing:
        return public_run(existing), False
    discovery = manual_gate()
    if not any(t['task_id'] == task_id and t['managed'] for t in discovery['tasks']):
        raise repo.TaskError('该任务未配置为受保护的定时任务', 409)
    if is_busy(task.group):
        raise repo.TaskError('此任务正在执行，请稍后再试')
    server, uid = identity()
    return public_run(repo.submit(task, actor, request_id, server, uid)), True


def history(task_id, page, per_page):
    catalog.get_task(task_id)
    rows, total = repo.history(task_id, page, per_page)
    return {'success': True, 'records': [public_run(r) for r in rows], 'pagination': {'total': total, 'page': page, 'per_page': per_page}}


def detail(run_id):
    row = repo.get_run(run_id)
    if not row:
        raise repo.TaskError('执行记录不存在', 404)
    # 只读服务端生成的随机文件名；不接受路径，不读取旧crontab日志。
    if not re.fullmatch('[0-9a-f]{32}', row['run_key']):
        raise repo.TaskError('执行记录日志标识异常', 503)
    path = safe_directory('runs') / (row['run_key'] + '.log')
    log = ''
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - 65536))
            log = stream.read(65536).decode('utf-8', errors='replace')
            if size > 65536:
                log = '（仅展示末尾64KiB日志）\n' + log
    except FileNotFoundError:
        log = '暂无日志；请求可能尚未启动、已跳过，或日志已按保留期清理。'
    return {'success': True, 'record': public_run(row), 'log': log}
