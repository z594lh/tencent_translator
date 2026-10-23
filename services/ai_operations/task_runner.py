"""共享队列唯一消费者分发；每日、人工分析和确认执行共用租约及ASIN顺序。"""
from datetime import timezone
from services.listing_performance.common import DomainError
from . import chat_runner,chat_tasks,plan_store,daily_runner,managed_store


def process(task,stopping=None):
    """执行一项已领取作业；锁忙退避，已开始外部请求中断不自动重发。"""
    from .protocol import native_task,LEGACY_MESSAGE
    with plan_store.transaction() as cur:current_protocol=native_task(cur,task)
    if not current_protocol:
        chat_runner.fail(task,'legacy_protocol',LEGACY_MESSAGE)
        if task.get('run_id'):managed_store.update(task['run_id'],'failed',LEGACY_MESSAGE,done=True)
        return
    if task['task_type']=='chat':return chat_runner.process(task,stopping=stopping)
    try:
        def permitted():
            """每轮收费和每项提交前复核租约、权限及退出信号。"""
            daily_runner.authorize_actor(task['actor_id'])
            with plan_store.transaction(True) as cur:chat_tasks.fence(cur,task['id'],task['owner_token'])
            if stopping and stopping():raise DomainError('消费者正在停止，未启动后续操作',409)
        permitted()
        if task['task_type']=='execute':
            from . import confirmation_runner
            return confirmation_runner.process(task,permitted)
        if task['task_type']!='daily':raise DomainError('任务类型无效',422)
        run=managed_store.get_run(task['run_id'])
        result=daily_runner.run_one(task['managed_id'],task['actor_id'],run['trigger_source'],
            now=task['scheduled_at'].replace(tzinfo=timezone.utc),authorize=permitted,task=task)
        with plan_store.transaction(True) as cur:
            chat_tasks.fence(cur,task['id'],task['owner_token'],allow_stop=True)
            status='completed' if result['status'] in ('completed','partial') else 'failed'
            cur.execute('''UPDATE ai_operation_tasks SET status=%s,phase=%s,message=%s,plan_id=%s,
                completed_at=UTC_TIMESTAMP(3),lease_until=NULL WHERE id=%s''',
                (status,status,result['message'],result['plan_id'],task['id']))
    except DomainError as error:
        # 只在尚未登记方案/执行且确实为ASIN锁忙时退避，不把业务失效当无限重试。
        if error.status==409 and ('流程正在运行' in str(error) or '正在生成方案' in str(error)):
            try:chat_tasks.requeue(task)
            except DomainError:chat_runner.fail(task,'stopped',str(error))
            return
        chat_runner.fail(task,'stopped',str(error))
        if task.get('run_id'):managed_store.update(task['run_id'],'failed',str(error),done=True)
    except Exception:
        chat_runner.fail(task,'unknown','作业未正常结束；仅核对原分析和执行记录，不自动重放')
        if task.get('run_id'):managed_store.update(task['run_id'],'failed','消费者异常，核对原请求与执行记录；不自动重试',done=True)
