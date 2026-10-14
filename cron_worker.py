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
from services.cron_tasks.locking import TaskLock, BusyError, identity
from services.cron_tasks.retention import prune_history


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
            prune_interval = 3600
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
                    if last_prune == 0 or now - last_prune >= prune_interval:
                        last_prune = now
                        try:
                            result = prune_history()
                            # 历史较多时分批推进，不在一次循环长时间持有数据库锁。
                            prune_interval = 60 if result['more'] else 3600
                            if result['records'] or result['files'] or result['errors']:
                                print(f"计划任务七天清理：记录{result['records']}条，日志{result['files']}个，未处理{result['errors']}个", flush=True)
                        except Exception as exc:
                            prune_interval = 60
                            print(f'执行记录清理暂不可用（{type(exc).__name__}），稍后重试，不影响任务领取', flush=True)
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
