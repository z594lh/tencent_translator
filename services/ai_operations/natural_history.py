"""纯问答持久化和增量摘要；工具返回、附件、规则及旧memory不进入续聊记忆。"""
import json
from . import plan_store,plan_provider,prompt_settings
from .metrics import digest,dumps
from services.listing_performance.common import DomainError

MAX_HISTORY_BYTES=60000
KEEP_TURNS=10
MAX_SOURCE_BYTES=90000
MAX_SUMMARY_BYTES=16000
MAX_SUMMARY_CALLS=3


def save(cur,session,source_key,question,answer,source,actor_name,task_id=None,plan_id=None,created_at=None):
    """在原任务完成事务保存准确自然问答；同来源不同内容拒绝覆盖。"""
    if not isinstance(question,str) or not question.strip() or not isinstance(answer,str) or not answer.strip():raise DomainError('自然问答不能为空',422)
    hashed=digest([question,answer]);sid=session['id'] if isinstance(session,dict) else session
    cur.execute('SELECT id,content_hash FROM ai_operation_chat_turns WHERE source_key=%s',(source_key,));old=cur.fetchone()
    if old:
        if old['content_hash']!=hashed:raise DomainError('原问答已有不同内容，未覆盖历史',409)
        return old['id']
    cur.execute('''INSERT INTO ai_operation_chat_turns(conversation_id,source_key,task_id,plan_id,question,answer,content_hash,source,actor_name,created_at,completed_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(%s,UTC_TIMESTAMP(3)),UTC_TIMESTAMP(3))''',
        (sid,source_key,task_id,plan_id,question,answer,hashed,source,(actor_name or 'ERP')[:100],created_at))
    return cur.lastrowid


def latest(cur,sid):
    """读取最后成功摘要并校验，失败版本不会替代旧记忆。"""
    cur.execute("SELECT * FROM ai_operation_chat_summaries WHERE conversation_id=%s AND status='ready' ORDER BY id DESC LIMIT 1",(sid,));row=cur.fetchone()
    if row and digest(row['summary_text'])!=row['summary_hash']:raise DomainError('会话摘要校验失败，未继续调用AI',503)
    return row


def turns(cur,sid,after=0,limit=201):
    """只读自然问答并逐对校验，不读取审计messages。"""
    cur.execute('SELECT * FROM ai_operation_chat_turns WHERE conversation_id=%s AND id>%s ORDER BY id LIMIT %s',(sid,after,limit));rows=list(cur.fetchall())
    for row in rows:
        if digest([row['question'],row['answer']])!=row['content_hash']:raise DomainError('问答原文校验失败，未继续调用AI',503)
    return rows


def dialogue(rows):
    """恢复准确user/assistant消息，不夹带计划JSON或数据库附件。"""
    return [message for row in rows for message in ({'role':'user','content':row['question']},{'role':'assistant','content':row['answer']})]


def context(session,invoke,permitted,prompt_snapshot=None):
    """超限时分批压缩旧问答；近期原文不静默删除，每次摘要独立登记付费意图。"""
    usage=[];frozen=prompt_snapshot or prompt_settings.snapshot()
    for _ in range(MAX_SUMMARY_CALLS+1):
        with plan_store.transaction() as cur:
            previous=latest(cur,session['id']);rows=turns(cur,session['id'],previous['covered_turn_id'] if previous else 0)
        prefix=[{'role':'user','content':frozen['config']['prompts']['summary_prefix']+'\n'+previous['summary_text']}] if previous else []
        messages=[*prefix,*dialogue(rows)]
        if len(rows)<201 and len(rows)<=30 and len(dumps(messages).encode('utf8'))<=MAX_HISTORY_BYTES:
            return messages,{'summary_id':str(previous['id']) if previous else None,'covered_turn_id':str(previous['covered_turn_id']) if previous else None,
                'recent_turns':len(rows),'summary_calls':usage,'history_bytes':len(dumps(messages).encode('utf8'))}
        if len(usage)>=MAX_SUMMARY_CALLS:raise plan_provider.Failure('context_limit','本次问答压缩已达3批，成功摘要保留；请明确继续或新建会话',{'summary_calls':usage})
        keep=KEEP_TURNS if len(rows)>KEEP_TURNS else 2
        if len(rows)<=keep:raise plan_provider.Failure('context_limit','最近完整问答本身过长，未静默删除；请查看原文后新建会话',{'summary_calls':usage})
        batch=[]
        for row in rows[:-keep]:
            if len(dumps([*prefix,*dialogue([*batch,row])]).encode('utf8'))>MAX_SOURCE_BYTES:break
            batch.append(row)
        if not batch:raise plan_provider.Failure('context_limit','单个原问答超过摘要输入限制，未截断原文；请新建会话',{'summary_calls':usage})
        try:usage.append(summarize(session,previous,batch,invoke,permitted,frozen))
        except plan_provider.Failure as error:
            info=error.usage or {}
            raise plan_provider.Failure(error.code,str(error),{**info,'summary_calls':[*usage,*info.get('summary_calls',[])]}) from None
    raise DomainError('会话压缩未完成',422)


def summarize(session,previous,rows,invoke,permitted,prompt_snapshot=None):
    """先登记唯一摘要意图再收费；游标CAS，失败保留旧摘要，显式重试才允许再次请求。"""
    source={'previous_summary':previous['summary_text'] if previous else None,'dialogue':dialogue(rows)}
    hashed=digest(source);base=digest([session['id'],previous['id'] if previous else None,rows[-1]['id'],hashed])
    frozen=prompt_snapshot or prompt_settings.snapshot()
    messages=[{'role':'system','content':frozen['config']['prompts']['summary_system']},
              {'role':'user','content':dumps(source)}]
    permitted()
    with plan_store.transaction(True) as cur:
        from . import chat_tasks
        if session.get('task_id'):chat_tasks.fence(cur,session['task_id'],session.get('owner_token'))
        cur.execute('SELECT id,status FROM ai_operation_chat_summaries WHERE request_key=%s',(base,));old=cur.fetchone();key=base
        if old:
            if old['status']=='ready':raise DomainError('摘要覆盖已变化，请重新读取会话',409)
            retry=session.get('retry_summary_id');allowed=False
            if retry:
                cur.execute('SELECT status,source_hash,previous_id FROM ai_operation_chat_summaries WHERE id=%s AND conversation_id=%s',(retry,session['id']));attempt=cur.fetchone()
                allowed=bool(attempt and attempt['status']!='ready' and attempt['source_hash']==hashed and attempt['previous_id']==(previous['id'] if previous else None))
            if not allowed:raise plan_provider.Failure('summary_blocked','摘要请求 #'+str(old['id'])+' 已登记未成功，可能已计费；请在会话记忆中明确重试或新建会话')
            key=digest([base,'explicit-retry',session.get('task_id')])
            cur.execute('SELECT id FROM ai_operation_chat_summaries WHERE request_key=%s',(key,))
            if cur.fetchone():raise plan_provider.Failure('summary_blocked','本次显式摘要重试已登记，不重复收费')
        cur.execute('''INSERT INTO ai_operation_chat_summaries(conversation_id,task_id,request_key,previous_id,covered_turn_id,source_hash,status,input_json,created_at)
            VALUES(%s,%s,%s,%s,%s,%s,'registered',%s,UTC_TIMESTAMP(3))''',
            (session['id'],session.get('task_id'),key,previous['id'] if previous else None,rows[-1]['id'],hashed,dumps(messages)))
        sid=cur.lastrowid
    usage=None
    try:
        reply,usage=invoke(None,{'messages':messages,'summary':True});permitted()
        value=reply.get('content') if isinstance(reply,dict) else None
        if not isinstance(value,str) or not value.strip() or len(value.encode('utf8'))>MAX_SUMMARY_BYTES or reply.get('tool_calls'):raise plan_provider.Failure('invalid_output','摘要文本无效或超限，旧摘要和原文保留',usage)
        with plan_store.transaction(True) as cur:
            if session.get('task_id'):chat_tasks.fence(cur,session['task_id'],session.get('owner_token'))
            cur.execute("SELECT id FROM ai_operation_conversations WHERE id=%s AND state='active' FOR UPDATE",(session['id'],))
            if not cur.fetchone():raise DomainError('会话已归档，摘要未应用',409)
            current=latest(cur,session['id'])
            if (current['id'] if current else None)!=(previous['id'] if previous else None):raise DomainError('摘要版本变化，未覆盖其他结果',409)
            cur.execute('SELECT id,content_hash FROM ai_operation_chat_turns WHERE conversation_id=%s AND id IN ('+','.join(['%s']*len(rows))+') ORDER BY id',tuple([session['id'],*[r['id'] for r in rows]]))
            if [(r['id'],r['content_hash']) for r in cur.fetchall()]!=[(r['id'],r['content_hash']) for r in rows]:raise DomainError('摘要原文在生成期间变化，未应用旧源结果',409)
            cur.execute("UPDATE ai_operation_chat_summaries SET status='ready',summary_text=%s,summary_hash=%s,usage_json=%s,message='历史问答压缩完成',completed_at=UTC_TIMESTAMP(3) WHERE id=%s AND status='registered'",(value,digest(value),dumps(usage),sid))
        return {'summary_id':str(sid),**usage}
    except Exception as error:
        code=error.code if isinstance(error,plan_provider.Failure) else 'unknown'
        usage=(error.usage or usage) if isinstance(error,plan_provider.Failure) else usage
        message=str(error) if isinstance(error,(DomainError,plan_provider.Failure)) else '摘要回执未知，保留旧摘要及原文，不自动重复收费'
        with plan_store.transaction(True) as cur:
            cur.execute("UPDATE ai_operation_chat_summaries SET status=%s,usage_json=%s,message=%s,completed_at=UTC_TIMESTAMP(3) WHERE id=%s AND status='registered'",('unknown' if code=='unknown' else 'failed',dumps(usage),message[:500],sid))
        raise plan_provider.Failure('summary_failed',message,{'summary_id':str(sid),'summary_calls':[usage] if usage else [],'billable':True}) from None


def describe(sid):
    """页面只读摘要覆盖与独立用量，不启动压缩。"""
    with plan_store.transaction() as cur:
        row=latest(cur,sid)
        cur.execute('SELECT COUNT(*) n FROM ai_operation_chat_turns WHERE conversation_id=%s AND id>%s',(sid,row['covered_turn_id'] if row else 0));count=cur.fetchone()['n']
        cur.execute('SELECT id,status,message,created_at,usage_json FROM ai_operation_chat_summaries WHERE conversation_id=%s ORDER BY id DESC LIMIT 1',(sid,));last=cur.fetchone()
    if last:
        last['id']=str(last['id']);raw=last.pop('usage_json');last['usage']=json.loads(raw) if raw else None
    return {'summary_id':str(row['id']) if row else None,'summary':row['summary_text'] if row else '',
            'covered_turn_id':str(row['covered_turn_id']) if row else None,'recent_turns':count,'last_attempt':last,
            'note':'长期上下文只有原问题、最终回答及其摘要；工具过程和资料不进入下一次续聊。超限压缩可能额外计费。'}


def search(sid,query,offset,limit):
    """本会话检索完整问答，不返回原资料/工具协议。"""
    where='conversation_id=%s';params=[sid]
    if query:where+=' AND (LOCATE(%s,question)>0 OR LOCATE(%s,answer)>0)';params.extend([query,query])
    with plan_store.transaction() as cur:
        cur.execute('SELECT COUNT(*) n FROM ai_operation_chat_turns WHERE '+where,tuple(params));count=cur.fetchone()['n']
        cur.execute('SELECT id,question,answer,created_at,completed_at FROM ai_operation_chat_turns WHERE '+where+' ORDER BY id DESC LIMIT %s OFFSET %s',tuple([*params,limit,offset]));rows=list(cur.fetchall())
    while rows and len(dumps(rows).encode('utf8'))>28000:rows.pop()
    return {'rows':rows,'total':count,'offset':offset,'next_offset':offset+len(rows) if rows and offset+len(rows)<count else None,
            'note':'原问题与最终回答不是执行事实；单条超限时请在页面查看原文'}
