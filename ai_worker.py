"""AI续聊消费者的根目录独立入口。

与worker.py（箱唛/发票）、cron_worker.py（计划任务）分别运行；
不得从Gunicorn/Web import启动。只读预检使用python ai_worker.py --check。
"""
import argparse
import signal
import threading
import uuid
from services.ai_operations import plan_store, chat_tasks, task_runner, prompt_configuration

LOCK_NAME='ai_operations_shared_chat_worker'


def check():
    """只读部署预检；不发心跳、不领取任务、不调用AI或Amazon。"""
    with plan_store.transaction() as cur:
        chat_tasks.schema(cur);prompt_configuration.check_schema(cur)
    return {'status':'ready','worker':chat_tasks.health(),'note':'只读结构检查通过，不代表服务已启动或AI授权已验证'}


def serve():
    """持有跨进程锁单实例消费，心跳线程仅属于独立进程，长模型调用期间持续续租。"""
    lock_connection=plan_store.get_db_connection();acquired=False
    shutdown=threading.Event();heartbeat_stop=threading.Event();failed=threading.Event();state_lock=threading.Lock()
    state={'task':None};instance=uuid.uuid4().hex;thread=None
    def request_stop(_signum,_frame):
        """停止领取新任务和后续模型轮次，等待已发请求落库；心跳持续到当前处理结束。"""
        shutdown.set()
    def heartbeat():
        """同一线程检查进程锁连接并续心跳/任务租约，丢锁不自动重连。"""
        while not heartbeat_stop.wait(15):
            try:
                lock_connection.ping(reconnect=False)
                with plan_store.transaction(True) as cur:
                    cur.execute("UPDATE ai_operation_worker_state SET heartbeat_at=UTC_TIMESTAMP(3) WHERE worker_key='shared_chat' AND owner_token=%s",(instance,))
                    if not cur.rowcount:raise RuntimeError('worker instance changed')
                    with state_lock:task=state['task']
                    if task:
                        cur.execute("UPDATE ai_operation_tasks SET heartbeat_at=UTC_TIMESTAMP(3),lease_until=DATE_ADD(UTC_TIMESTAMP(3),INTERVAL 90 SECOND) WHERE id=%s AND status='running' AND owner_token=%s",(task['id'],task['owner_token']))
            except Exception:
                failed.set()
                shutdown.set()
                print('AI消费者心跳或进程锁异常，停止后续请求；请核对原任务，不自动重复收费',flush=True)
                return
    try:
        check()
        with lock_connection.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s,0) acquired',(LOCK_NAME,));acquired=cur.fetchone()['acquired']==1
        lock_connection.rollback()
        if not acquired:
            print('已有AI会话消费者，当前实例退出',flush=True);return 1
        with plan_store.transaction(True) as cur:
            cur.execute("""INSERT INTO ai_operation_worker_state(worker_key,owner_token,started_at,heartbeat_at)
                VALUES ('shared_chat',%s,UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))
                ON DUPLICATE KEY UPDATE owner_token=VALUES(owner_token),state='running',started_at=VALUES(started_at),heartbeat_at=VALUES(heartbeat_at)""",(instance,))
        for name in (signal.SIGTERM,signal.SIGINT):signal.signal(name,request_stop)
        thread=threading.Thread(target=heartbeat,name='ai-chat-heartbeat',daemon=True);thread.start()
        print('AI统一消费者已启动：人工分析、每日优化及显式确认执行；人工建议不自动提交',flush=True)
        while not shutdown.is_set():
            task=chat_tasks.claim()
            if not task:
                shutdown.wait(2);continue
            with state_lock:state['task']=task
            try:task_runner.process(task,stopping=shutdown.is_set)
            finally:
                with state_lock:state['task']=None
        return 1 if failed.is_set() else 0
    finally:
        heartbeat_stop.set()
        if thread:thread.join(timeout=30)
        if acquired:
            try:
                with plan_store.transaction(True) as cur:
                    cur.execute("UPDATE ai_operation_worker_state SET state='stopped' WHERE worker_key='shared_chat' AND owner_token=%s",(instance,))
                with lock_connection.cursor() as cur:cur.execute('SELECT RELEASE_LOCK(%s)',(LOCK_NAME,))
            except Exception:pass
        lock_connection.close()


def main():
    """显式命令行入口，默认启动消费者；开发只能使用--check或隔离模型检查。"""
    parser=argparse.ArgumentParser(description='AI共享对话独立消费者')
    parser.add_argument('--check',action='store_true',help='只读结构与心跳预检，不消费任务')
    args=parser.parse_args()
    if args.check:
        from services.ai_operations.metrics import dumps
        print(dumps(check()),flush=True);return 0
    try:return serve()
    except Exception:
        print('AI消费者启动或存储异常，请核对迁移与服务日志；不会自动重放已登记模型请求',flush=True)
        return 1


if __name__=='__main__':raise SystemExit(main())
