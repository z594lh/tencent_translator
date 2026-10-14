"""公共执行器：固定argv、实际子进程持锁、限量脱敏日志和保守恢复。"""
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from functools import wraps

from services.cron_tasks import catalog, repository as repo
from services.cron_tasks.locking import TaskLock, BusyError, identity, safe_directory


def resource_guard(group):
    """现有HTTP同步入口与cron互斥，Windows保持原接口能力。"""
    def decorate(function):
        @wraps(function)
        def guarded(*args, **kwargs):
            if not catalog.supported():
                return function(*args, **kwargs)
            from flask import jsonify
            try:
                with TaskLock(group):
                    return function(*args, **kwargs)
            except BusyError:
                return jsonify({'status': 'error', 'message': '相关同步或计划任务正在执行，请稍后再试'}), 409
        return guarded
    return decorate


class SafeLog:
    """每条日志有界，单次最多1MiB；达到上限仍持续消费stdout，不阻塞子进程。"""
    def __init__(self, run_key):
        self.path = safe_directory('runs') / (run_key + '.log')
        fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        self.stream = os.fdopen(fd, 'wb', buffering=0)
        self.size = 0
        self.truncated = False
        self.secrets = [v for k, v in os.environ.items() if re.search('SECRET|TOKEN|PASSWORD|API_KEY|ACCESS_KEY|WEBHOOK', k, re.I) and len(v) >= 6]

    def write(self, value):
        value = value.decode('utf-8', errors='replace') if isinstance(value, bytes) else value
        # 凭证/买家字段整行隐藏；未知原始API响应不直接暴露。
        if re.search(r'authorization|refresh.?token|access.?token|secret|password|api.?key|access.?key|webhook|cookie|credential|buyer|address|recipient|phone|买家|地址|收件|姓名|电话|手机', value, re.I):
            value = '[包含凭证或个人信息的日志已隐藏]\n'
        for secret in self.secrets:
            value = value.replace(secret, '[已隐藏]')
        value = re.sub(r'https?://[^\s\"\x27<>]+', '[链接已隐藏]', value)
        value = re.sub(r'\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b', '[邮箱已隐藏]', value, flags=re.I)
        value = re.sub(r'\b(?:Atza|Atzr)\|[^\s]+', '[凭证已隐藏]', value)
        value = re.sub(r'\beyJ[A-Za-z0-9_.-]{40,}', '[凭证已隐藏]', value)
        value = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)
        data = value.encode('utf-8')[:32768]
        if self.size + len(data) <= 1024 * 1024:
            self.stream.write(data)
            self.size += len(data)
        elif not self.truncated:
            self.stream.write('\n[日志达到1MiB，后续输出省略；任务仍继续执行]\n'.encode())
            self.truncated = True

    def close(self):
        self.stream.close()


def _validate_manual(row, instance):
    from services.cron_tasks.service import manual_gate
    from services.permissions_service import get_user_permission_codes
    discovery = manual_gate()
    task = catalog.get_task(row['task_id'])
    if not task.manual or not any(t['task_id'] == task.id and t['managed'] for t in discovery['tasks']):
        raise repo.TaskError('任务调度已变化，当前请求不执行')
    with repo.transaction() as cursor:
        cursor.execute('SELECT status FROM users WHERE id=%s', (row['actor_id'],))
        actor = cursor.fetchone()
    codes = get_user_permission_codes(row['actor_id'])
    if not actor or actor['status'] != 1 or not {'tasks:page', 'tasks:execute'}.issubset(codes):
        raise repo.TaskError('操作人权限已变化，当前请求不执行', 403)
    if row['worker_instance'] != instance or row['group_key'] != task.group:
        raise repo.TaskError('请求所属执行服务或任务锁已变化')
    return task


def execute(task_id=None, run_id=None, instance=None):
    if not catalog.supported():
        raise repo.TaskError('当前环境不支持Linux计划任务执行', 503)
    server, _uid = identity()
    if run_id is not None:
        row = repo.get_run(run_id)
        if not row or row['status'] != 'pending':
            return 1
        try:
            task = _validate_manual(row, instance)
        except Exception:
            repo.finish(run_id, 'expired', '启动前配置或权限核对未通过，请刷新查看；不会自动重试')
            return 1
    else:
        task = catalog.get_task(task_id)
    try:
        lock = TaskLock(task.group)
        lock.__enter__()
    except BusyError:
        if run_id is None:
            repo.scheduled_skip(task, server)
        else:
            repo.finish(run_id, 'skipped', '此任务已持有执行锁，本次未启动，不会排队重试')
        return 0
    log = None
    child = None
    try:
        row = repo.begin_run(task, server, run_id, instance)
        if row['status'] == 'skipped':
            print(f'[{task.id}] 此任务已占用，本轮跳过', flush=True)
            return 0
        run_id = row['id']
        print(f'[{task.id}] 开始执行，记录#{run_id}', flush=True)
        log = SafeLog(row['run_key'])
        log.write(f"任务：{task.title}\n范围：{task.scope}\n提示：退出码不代表全部业务成功，请核对同步结果。\n")
        argv = [sys.executable, '-u', str(catalog.ROOT / 'scripts' / 'cron' / (task.script + '.py')), *task.args]
        # 不使用Shell，不继承worker单实例锁；资源锁必须由实际脚本继承。
        child = subprocess.Popen(argv, cwd=str(catalog.ROOT), stdin=subprocess.DEVNULL,
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False,
                                 pass_fds=(lock.fd,), start_new_session=True)
        repo.run_heartbeat(run_id, child.pid)
        selector = selectors.DefaultSelector()
        selector.register(child.stdout, selectors.EVENT_READ)
        buffer = b''
        dropping_line = False
        last_heartbeat = time.monotonic()
        while selector.get_map() or child.poll() is None:
            for key, _mask in selector.select(timeout=1):
                chunk = os.read(key.fd, 4096)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                buffer += chunk
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if not dropping_line:
                        log.write(line + b'\n')
                    dropping_line = False
                if len(buffer) > 16384:
                    # 超长未换行输出整段丢弃，避免拆开凭证后泄漏。
                    log.write('[超长原始输出已隐藏]\n')
                    buffer = b''
                    dropping_line = True
            if time.monotonic() - last_heartbeat >= 5:
                try:
                    repo.run_heartbeat(run_id, child.pid)
                except Exception:
                    # DB暂不可达时仍持锁等待业务结束，不杀任务、不放行第二份。
                    pass
                last_heartbeat = time.monotonic()
        if buffer and not dropping_line:
            log.write(buffer)
        selector.close()
        child.stdout.close()
        code = child.wait()
        status = 'finished' if code == 0 else 'failed'
        summary = '脚本执行结束；退出码0不代表全部业务成功，请核对日志' if code == 0 else f'脚本异常退出（退出码{code}）；部分数据可能已写入，不自动重试'
        log.write('\n' + summary + '\n')
        repo.finish(run_id, status, summary, code)
        print(f'[{task.id}] 执行结束，记录#{run_id}，退出码{code}；业务结果请核对执行日志', flush=True)
        return 0 if code == 0 else 1
    except Exception:
        if child is not None and child.poll() is None:
            # 启动后状态写入或日志出错不能放弃管控；持续等待，不重发任务。
            while child.stdout.read(65536):
                pass
            child.wait()
        if run_id:
            repo.finish(run_id, 'interrupted', '执行器异常；业务结果需要核实，不会自动重试')
        raise
    finally:
        try:
            if log:
                log.close()
        finally:
            lock.__exit__(None, None, None)


def reconcile(instance):
    """只有获取实际进程锁后才能回收；心跳超时本身不是释放依据。"""
    for row in repo.active_runs():
        if row['status'] == 'pending' and row['worker_instance'] == instance and datetime.utcnow() - row['created_at'] <= timedelta(seconds=30):
            continue
        try:
            with TaskLock(row['group_key']):
                if row['status'] == 'pending':
                    repo.finish(row['id'], 'expired', '执行服务变化或请求过期，本次未启动，不会自动重试')
                else:
                    repo.finish(row['id'], 'interrupted', '实际执行锁已释放但缺少完整结束记录，业务结果待核实，不自动重试')
        except BusyError:
            continue


def ignore_termination_while_running():
    # systemd正常停止由worker等待；执行器不将停止信号转发给正在写业务的子进程。
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: None)
