"""箱唛与发票后台任务的独立进程入口。

生产环境应只启动一个本进程。进程通过 MySQL advisory lock 防止多个
实例同时重置和消费任务；Web/Gunicorn 进程不再启动后台线程。
"""
import signal
import sys
import threading
import time

from blueprints.amazon.invoice_export import _start_invoice_workers
from blueprints.fba_tools import _start_label_workers
from services.mysql_service import get_db_connection


WORKER_LOCK_NAME = "tencent_translator_background_workers"
LOCK_HEALTHCHECK_SECONDS = 30


def _acquire_worker_lock():
    """获取跨进程 MySQL advisory lock，并返回持有锁的连接。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (WORKER_LOCK_NAME,))
            row = cursor.fetchone() or {}
        if row.get("acquired") != 1:
            conn.close()
            return None
        return conn
    except Exception:
        conn.close()
        raise


def _release_worker_lock(conn):
    if not conn:
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (WORKER_LOCK_NAME,))
    except Exception as exc:
        print(f"[Background Worker] 释放进程锁失败: {exc}", flush=True)
    finally:
        conn.close()


def main():
    lock_conn = _acquire_worker_lock()
    if lock_conn is None:
        print("[Background Worker] 已有实例持有任务锁，本实例退出", flush=True)
        return 1

    stop_event = threading.Event()

    def request_stop(signum, _frame):
        print(f"[Background Worker] 收到信号 {signum}，等待当前任务结束", flush=True)
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    threads = []
    exit_code = 0
    try:
        # 只有持有全局任务锁的进程可以重置中断任务并启动消费者。
        threads.append(_start_label_workers(stop_event=stop_event, daemon=False, reset_running=True))
        threads.append(_start_invoice_workers(stop_event=stop_event, daemon=False, reset_running=True))
        print("[Background Worker] 箱唛与发票消费者已启动", flush=True)

        last_lock_check = time.monotonic()
        while not stop_event.wait(1):
            dead_threads = [thread.name for thread in threads if not thread.is_alive()]
            if dead_threads:
                print(
                    f"[Background Worker] 消费线程异常退出: {', '.join(dead_threads)}",
                    flush=True,
                )
                exit_code = 1
                stop_event.set()
                break

            if time.monotonic() - last_lock_check >= LOCK_HEALTHCHECK_SECONDS:
                try:
                    # 不允许自动重连：连接丢失意味着 advisory lock 已丢失，
                    # 必须停止消费者，由 systemd 重启整个进程重新获取锁。
                    lock_conn.ping(reconnect=False)
                except Exception as exc:
                    print(f"[Background Worker] 任务锁连接已断开: {exc}", flush=True)
                    exit_code = 1
                    stop_event.set()
                    break
                last_lock_check = time.monotonic()

        # 保持 advisory lock，直到正在处理的任务正常结束。systemd 如需强制
        # 终止，会在 TimeoutStopSec 到期后发送 SIGKILL。
        for thread in threads:
            thread.join()
    except Exception as exc:
        print(f"[Background Worker] 启动或运行失败: {exc}", flush=True)
        stop_event.set()
        exit_code = 1
        for thread in threads:
            thread.join()
    finally:
        _release_worker_lock(lock_conn)

    print("[Background Worker] 进程已退出", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
