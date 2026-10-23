"""当前原生协议的唯一识别边界；旧记录只占位，不尝试恢复旧大资料包。"""
import json

VERSION='native-tools-v1'
LEGACY_MESSAGE='旧数据，无法查看；请使用当前版本重新分析，不会重放旧操作'


def object_value(value):
    """安全读取协议元信息；历史文本无效也只视为旧版，不使列表中断。"""
    if isinstance(value,(dict,list)):return value
    try:return json.loads(value) if isinstance(value,str) else None
    except (ValueError,TypeError,RecursionError):return None


def native_prompt(value):
    """只认明确原生版本，不根据字段相似或时间猜测可执行性。"""
    value=object_value(value)
    return isinstance(value,dict) and isinstance(value.get('conversation'),dict) and value['conversation'].get('protocol')==VERSION


def native_task(cur,row):
    """新排队任务有明确标记；已有原生任务按其输入或原模型事件识别。"""
    value=object_value(row.get('input_json'))
    if isinstance(value,dict) and value.get('engine')==VERSION:return True
    if isinstance(value,list) and value and native_prompt(value[-1]):return True
    if row.get('plan_id'):
        cur.execute('SELECT prompt_json FROM ai_operation_plans WHERE id=%s',(row['plan_id'],));plan=cur.fetchone()
        if plan and native_prompt(plan['prompt_json']):return True
    if row.get('user_message_id'):
        cur.execute('SELECT payload_json FROM ai_operation_messages WHERE id=%s',(row['user_message_id'],));message=cur.fetchone()
        payload=object_value(message['payload_json']) if message else None
        if isinstance(payload,dict) and payload.get('engine')==VERSION:return True
    cur.execute("SELECT id FROM ai_operation_messages WHERE task_id=%s AND event_type='model_request' AND JSON_UNQUOTE(JSON_EXTRACT(CASE WHEN JSON_VALID(payload_json) THEN payload_json ELSE '{}' END,'$.engine'))=%s LIMIT 1",(row['id'],VERSION))
    return bool(cur.fetchone())
