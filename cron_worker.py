"""单服务器计划任务执行服务；与箱唛/发票worker完全独立。"""
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / '.env', override=True)

from services.cron_tasks import catalog, repository as repo
from services.cron_tasks.execution import reconcile
from services.cron_tasks.locking import TaskLock, BusyError, identity, safe_directory


def prune_logs():
    """仅清理本执行器已结束且超过30天的随机日志，保留数据库审计记录。"""
    active = {row['run_key'] + '.log' for row in repo.active_runs()}
    directory = safe_directory('runs')
    import re
    for path in directory.iterdir():
        if re.fullmatch(r'[0-9a-f]{32}\.log', path.name) and path.name not in active and not path.is_symlink():
            if path.is_file() and path.stat().st_mtime < time.time() - 30 * 86400:
                path.unlink()


def main():
    if not catalog.supported():
        print('当前环境不支持Linux计划任务执行服务', flush=True)
        return 1
    stop = False

    def request_stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        with TaskLock('_worker'):
            instance = uuid.uuid4().hex
            server, uid = identity()
            children = {}
            last_heartbeat = last_reconcile = last_prune = 0
            while not stop or children:
                # 不同任务的手动执行互不阻塞；同任务排重由公共入口的独立锁/占用槽负责。
                children = {run_id: child for run_id, child in children.items() if child.poll() is None}
                now = time.monotonic()
                try:
                    if now - last_heartbeat >= 3:
                        repo.worker_heartbeat(instance, server, uid, accepting=not stop)
                        last_heartbeat = now
                    if now - last_reconcile >= 10:
                        reconcile(instance)
                        last_reconcile = now
                    if now - last_prune >= 86400:
                        prune_logs()
                        last_prune = now
                    if not stop:
                        pending = repo.pending(instance)
                        if pending and pending['id'] not in children:
                            children[pending['id']] = subprocess.Popen([sys.executable, '-u', str(catalog.RUNNER),
                                '--run-id', str(pending['id']), '--instance', instance],
                                cwd=str(catalog.ROOT), stdin=subprocess.DEVNULL, close_fds=True, shell=False)
                except Exception as exc:
                    print(f'计划任务服务暂不可用（{type(exc).__name__}），不新启动任务', flush=True)
                time.sleep(1)
            repo.worker_heartbeat(instance, server, uid, accepting=False)
    except BusyError:
        print('已有计划任务执行服务实例，本实例退出', flush=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
