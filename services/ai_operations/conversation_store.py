"""ERP会话、不可变调用事件与有界记忆；不持有网络期间的业务行锁。"""
import json
from services.listing_performance.common import DomainError
from . import plan_store
from .metrics import digest, dumps

TABLES = ('ai_operation_conversations', 'ai_operation_messages', 'ai_operation_memory')
ACCOUNT_FIELDS = ('id', 'credential_group_id', 'region', 'marketplace_id', 'ads_profile_id', 'asin')


def checked(row, field, hash_field):
    """解析持久化JSON并验证摘要，防止用损坏记忆继续分析。"""
    value = json.loads(row[field])
    if digest(value) != row[hash_field]:
        raise DomainError('会话资料摘要不符，请核对存储；不会重新调用AI', 503)
    return value


def open_session(identity):
    """按店铺账户与ASIN恢复会话及最近记忆，不跨账户继承。"""
    boundary = {key: str(identity.get(key) or '') for key in ACCOUNT_FIELDS}
    if not boundary['id'] or not boundary['asin'] or not boundary['marketplace_id']:
        raise DomainError('会话账户或ASIN边界不完整', 422)
    key = digest([boundary, 'real'])
    with plan_store.transaction(True) as cur:
        cur.execute('''INSERT INTO ai_operation_conversations
            (account_key,shop_id,asin,identity_json,runtime_mode,created_at,updated_at)
            VALUES (%s,%s,%s,%s,'real',UTC_TIMESTAMP(3),UTC_TIMESTAMP(3))
            ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)''',
            (key, identity['id'], identity['asin'], dumps(boundary)))
        sid = cur.lastrowid
        cur.execute('SELECT * FROM ai_operation_conversations WHERE id=%s', (sid,))
        session = cur.fetchone()
        cur.execute('SELECT * FROM ai_operation_memory WHERE conversation_id=%s ORDER BY revision DESC LIMIT 1', (sid,))
        memory = cur.fetchone()
        if (memory['revision'] if memory else 0) != session['revision']:
            raise DomainError('会话记忆版本不完整，未调用AI', 503)
        session['memory'] = checked(memory, 'memory_json', 'memory_hash') if memory else None
        # 只恢复最近两份终稿/执行反馈；所有旧原文仍按方案编号归档。
        cur.execute('''SELECT * FROM ai_operation_messages WHERE conversation_id=%s
            AND event_type='execution_feedback' ORDER BY id DESC LIMIT 2''', (sid,))
        session['recent_feedback'] = [checked(r, 'payload_json', 'payload_hash') for r in reversed(cur.fetchall())]
        cur.execute('''SELECT * FROM ai_operation_memory WHERE conversation_id=%s
            ORDER BY revision DESC LIMIT 2''', (sid,))
        session['recent_finals'] = []
        for checkpoint in reversed(cur.fetchall()):
            cur.execute('SELECT result_json,result_hash FROM ai_operation_plans WHERE id=%s AND status=\'ready\'', (checkpoint['plan_id'],))
            plan = cur.fetchone()
            if plan and plan['result_json']:
                result = checked(plan, 'result_json', 'result_hash')
                final={'plan_id':str(checkpoint['plan_id']), 'summary':result['summary'],
                       'history_reviews':result.get('history_reviews',[]),
                       'complete_message_ref':'plan:'+str(checkpoint['plan_id'])}
                cur.execute("SELECT * FROM ai_operation_messages WHERE plan_id=%s AND event_type='model_response' ORDER BY round_no DESC LIMIT 1",(checkpoint['plan_id'],))
                event=cur.fetchone()
                if event:
                    payload=checked(event,'payload_json','payload_hash')
                    original=(payload.get('usage') or {}).get('raw_output') or dumps(payload.get('response'))
                    if len(original.encode('utf8'))<=16000:
                        final['assistant_message']={'role':'assistant','content':original}
                    else:final['original_omitted_reason']='单份原回答超过16KB，恢复结论和记忆；完整行动可按方案编号取回'
                session['recent_finals'].append(final)
    return session


def append(session_id, plan_id, round_no, event_type, payload):
    """幂等追加不可变会话事件，已登记但内容不同则拒绝覆盖。"""
    if event_type not in ('model_request','model_response','data_response','validation_feedback','execution_feedback'):
        raise DomainError('会话事件类型无效')
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT * FROM ai_operation_messages WHERE plan_id=%s AND round_no=%s AND event_type=%s',
                    (plan_id, round_no, event_type))
        old = cur.fetchone()
        if old:
            if old['payload_hash'] != digest(payload) or str(old['conversation_id']) != str(session_id):
                raise DomainError('会话已有不同事件，请查看已保存记录，不重新调用AI',409)
            return False
        cur.execute('''INSERT INTO ai_operation_messages
            (conversation_id,plan_id,round_no,event_type,payload_json,payload_hash,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
            (session_id,plan_id,round_no,event_type,dumps(payload),digest(payload)))
    return True


def checkpoint(cur, session, plan_id, memory):
    """与方案ready同一事务提交，避免方案可执行但记忆未保存。"""
    cur.execute('SELECT revision FROM ai_operation_conversations WHERE id=%s FOR UPDATE', (session['id'],))
    current = cur.fetchone()
    if not current or current['revision'] != session['revision']:
        raise DomainError('会话版本已改变，保留本次分析，不覆盖其他记忆',409)
    cur.execute('''INSERT INTO ai_operation_memory
        (conversation_id,revision,plan_id,memory_json,memory_hash,created_at)
        VALUES (%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
        (session['id'],session['revision']+1,plan_id,dumps(memory),digest(memory)))
    cur.execute('UPDATE ai_operation_conversations SET revision=revision+1,updated_at=UTC_TIMESTAMP(3) WHERE id=%s', (session['id'],))


def trace(plan_id):
    """按实际顺序读取计划的完整会话事件及摘要。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_messages WHERE plan_id=%s ORDER BY id', (plan_id,))
        rows = cur.fetchall()
    return [{'id':str(r['id']), 'conversation_id':str(r['conversation_id']), 'round_no':r['round_no'],
             'event_type':r['event_type'], 'created_at':r['created_at'], 'hash':r['payload_hash'],
             'payload':checked(r,'payload_json','payload_hash')} for r in rows]


def history_index(session_id, offset, limit):
    """会话内历史索引，支持模型不知道旧方案编号时分页查找。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ai_operation_memory WHERE conversation_id=%s',(session_id,))
        total=cur.fetchone()['n']
        cur.execute('''SELECT m.plan_id,m.revision,p.report_date,p.result_json,p.result_hash
            FROM ai_operation_memory m JOIN ai_operation_plans p ON p.id=m.plan_id
            WHERE m.conversation_id=%s ORDER BY m.revision DESC LIMIT %s OFFSET %s''',(session_id,limit,offset))
        rows=[]
        for row in cur.fetchall():
            result=checked(row,'result_json','result_hash')
            rows.append({'plan_id':str(row['plan_id']),'ref':'plan:'+str(row['plan_id']),
                         'context_version':row['revision'],'report_date':row['report_date'],
                         'summary':result['summary'],
                         'actions':[{'ref':'plan:'+str(row['plan_id'])+':action:'+a['action_id'],
                                     'label':a['label'],'mode':a['mode'],'kind':a['kind']} for a in result['actions']]})
    return {'status':'provided' if rows else 'unavailable','source':'history','rows':rows,
            'total_in_archive':total,'offset':offset,'returned':len(rows),
            'next_offset':offset+len(rows) if rows and offset+len(rows)<total else None}


def execution_feedback(plan, records, status, message):
    """把已发生的正式执行与独立回读事实追加到同一会话。"""
    info = plan.get('ai_call',{}).get('prompt',{}).get('conversation')
    if not info:return
    payload = {'plan_id':plan['id'],'report_date':plan['report_date'],'status':status,'message':message,
               'execution_mode':'real','note':'ERP真实接口回读事实；属性一致不代表收益改善',
               'executions':[{'execution_id':r['id'],'action_id':r['action_id'],'status':r['status'],
                              'ref':r['action']['ref'],'label':r['action']['label'],
                              'before':r['action']['before'],'after':r['action']['after'],
                              'verified_at':r.get('verified_at')} for r in records]}
    # 终态反馈幂等；阶段中断后不覆盖已保存的原事实。
    append(info['id'],plan['id'],0,'execution_feedback',payload)
