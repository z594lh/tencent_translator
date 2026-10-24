"""原生工具调用任务；长期上下文仅问答，工具数据只在本任务循环内回传。"""
import time
from copy import deepcopy
from services.listing_performance.common import DomainError
from . import conversation_store as store,plan_provider,natural_history,tools
from .metrics import dumps,digest
from . import tool_activity,prompt_settings

VERSION='native-tools-v1'
MAX_CALLS=8
MAX_TOOL_CALLS=32
MAX_CONTEXT_BYTES=220000
MAX_TOTAL_INPUT_BYTES=1000000
MAX_ELAPSED_SECONDS=540


class ToolContext:
    """本任务的可核实证据、候选对象及唯一方案；不会写入后续聊天记忆。"""
    def __init__(self,data,session,purpose):
        """固定同一次本地来源，防止工具之间日期/账户漂移。"""
        self.data=data;self.session=session;self.purpose=purpose
        self.pack={'products':data['content']['listing']};self.settings=set();self.evidence=set()
        self.observations=set();self.observe_checked=False;self.plan=None;self.plan_input_hash=None

    def capture(self,value):
        """仅将实际返回给模型的证据引用计入已读集合，不接受模型自造引用。"""
        if isinstance(value,list):
            for row in value:self.capture(row)
        elif isinstance(value,dict):
            for key,row in value.items():
                if key in ('evidence_id','evidence_ref','ref') and isinstance(row,str) and row in self.data['evidence']:self.evidence.add(row)
                elif key=='evidence_refs' and isinstance(row,list):self.evidence.update(r for r in row if isinstance(r,str) and r in self.data['evidence'])
                self.capture(row)


def prepare_human(data,session,question,mode='discussion',prompt_snapshot=None):
    """人工自然问题仅绑定工具、身份和会话；不预加载报表/Listing/关键词资料。"""
    enabled=tools.snapshot(allow_plan=mode!='discussion')
    frozen=prompt_snapshot or prompt_settings.snapshot()
    return {'system':prompt_settings.system(frozen,mode),'question':question,'prompt_snapshot':frozen,'tools_snapshot':enabled,'request':plan_provider.request_spec(),
            'scope':{key:data['content'][key] for key in ('identity','time')},
            'conversation':{'id':str(session['id']),'purpose':'chat','mode':mode,'protocol':VERSION,
                'generation':session.get('generation',1),'context_version':session['revision'],'max_calls':MAX_CALLS}}


def prepare(data,queue_task=None):
    """每日同样从一句自然任务开始，不再使用多场景话术或全量资料首包。"""
    session=store.open_session(data['content']['identity']);session['purpose']='daily'
    if queue_task:session.update(task_id=queue_task['id'],owner_token=queue_task['owner_token'])
    frozen=prompt_settings.snapshot()
    prepared=prepare_human(data,session,frozen['config']['prompts']['daily_question'],'proposal',frozen)
    if 'read_observations' not in prepared['tools_snapshot']['names']:
        raise DomainError('每日优化必须复盘已有执行和观察，请先在AI工具管理开放“执行事实与观察”；尚未调用模型',422)
    prepared['conversation']['purpose']='daily'
    return prepared,session


def audit_messages(messages):
    """审计只保存自然内容和工具协议；思考字段只留hash，不保存隐式思维链原文。"""
    result=[]
    for message in messages:
        row={k:v for k,v in message.items() if k!='reasoning_content'}
        if message.get('reasoning_content') is not None:row['reasoning_hash']=digest(message['reasoning_content'])
        result.append(row)
    return result


def usage_summary(calls,started,input_bytes,memory=None):
    """模型分析与独立摘要用量分开列示；未知用量不伪造为0。"""
    result={'provider':'deepseek','billable':any(r.get('billable',True) for r in calls),'rounds':calls,'round_count':len(calls),
            'elapsed_ms':round((time.monotonic()-started)*1000),'total_input_bytes':input_bytes,
            'summary_calls':(memory or {}).get('summary_calls',[]),'memory':{k:v for k,v in (memory or {}).items() if k!='summary_calls'}}
    for key in ('prompt_tokens','completion_tokens','total_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens'):
        result[key]=sum(r[key] for r in calls) if calls and all(type(r.get(key)) is int for r in calls) else None
    return result


def run(data,prepared,session,plan_id,invoke,check_permissions):
    """原生工具循环；模型自然回答与已校验计划独立，任何失败都不会进入执行。"""
    started=time.monotonic();calls=[];input_bytes=0;tool_count=0;ids=set();ctx=ToolContext(data,session,prepared['conversation']['purpose'])
    frozen=prepared['prompt_snapshot'];texts=frozen['config']['prompts']
    history,memory=natural_history.context(session,invoke,check_permissions,frozen)
    # 跨任务仅自然问答作为引用资料；不伪造旧assistant工具消息/思考字段，避免供应商协议不完整。
    messages=[{'role':'system','content':prepared['system']},
              {'role':'user','content':dumps({'current_scope':prepared['scope'],'past_question_answers':history})},
              {'role':'user','content':prepared['question']}]
    def permitted():
        """中途停止仍带回已发生用量，不因权限/租约变化丢失之前的计费回执。"""
        try:check_permissions()
        except DomainError as error:raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes,memory)) from None
    def emit(round_no,kind,payload):
        """复用不可变事件与租约，同一轮不重复收费；工具过程仅在审计层保存。"""
        try:
            return store.append(session['id'],plan_id,round_no,kind,{'engine':VERSION,**payload},
                task_id=session.get('task_id'),owner_token=session.get('owner_token'))
        except DomainError as error:raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes,memory)) from None
    last_tool_error=None
    for round_no in range(1,MAX_CALLS+1):
        permitted()
        active_tools=prepared['tools_snapshot']['tools']
        if ctx.plan is not None:
            # 已接受唯一计划后只请求自然总结，避免再调工具改稿而耗尽剩余轮数。
            active_tools=[]
            messages.append({'role':'user','content':texts['plan_accepted']})
        length=len(dumps(messages).encode('utf8'))+len(dumps(active_tools).encode('utf8'))
        if length>MAX_CONTEXT_BYTES or input_bytes+length>MAX_TOTAL_INPUT_BYTES or time.monotonic()-started>MAX_ELAPSED_SECONDS-plan_provider.TIMEOUT:
            raise plan_provider.Failure('context_limit','本次工具分析达到长度/费用或耗时上限，原记录保留，未执行计划',usage_summary(calls,started,input_bytes,memory))
        if not emit(round_no,'model_request',{'messages':audit_messages(messages),'wire_hash':digest(messages),
                'question':prepared['question'],'request':{**prepared['request'],'tools':active_tools},
                'memory':memory,'prompt_version':{'id':frozen['version_id'],'hash':frozen['hash']},
                'tools_hash':digest(active_tools),'tools_snapshot_hash':prepared['tools_snapshot']['hash']}):
            raise plan_provider.Failure('unknown','本轮请求已登记，不重复收费',usage_summary(calls,started,input_bytes,memory))
        input_bytes+=length
        try:wire,usage=invoke(data,{'messages':messages,'tools':active_tools})
        except plan_provider.Failure as error:
            calls.append(error.usage or {'billable':True})
            emit(round_no,'model_response',{'error_code':error.code,'message':str(error),'usage':error.usage})
            raise plan_provider.Failure(error.code,str(error),usage_summary(calls,started,input_bytes,memory)) from None
        calls.append({**usage,'round_no':round_no});wire=deepcopy(wire)
        if not isinstance(wire,dict) or wire.get('role')!='assistant':raise plan_provider.Failure('invalid_output','模型没有返回原生assistant消息，未执行计划',usage_summary(calls,started,input_bytes,memory))
        pending=wire.get('tool_calls') or []
        if ctx.plan is not None and pending:
            note='计划已校验，但模型在收尾阶段仍返回工具调用，未形成最终回答；本次未执行，不自动重发'
            total=usage_summary(calls,started,input_bytes,memory)
            emit(round_no,'model_response',{'error_code':'invalid_output','message':note,'usage':usage})
            raise plan_provider.Failure('invalid_output',note,total)
        if not pending:
            answer=wire.get('content')
            if not isinstance(answer,str) or not answer.strip():raise plan_provider.Failure('invalid_output','模型没有给出最终自然回答，未执行计划',usage_summary(calls,started,input_bytes,memory))
            if (ctx.purpose=='daily' or prepared['conversation'].get('mode')=='proposal') and ctx.plan is None:
                emit(round_no,'model_response',{'response':{'type':'tool_progress','reply':answer},'usage':usage})
                messages.append(wire);messages.append({'role':'user','content':texts['plan_required']})
                continue
            emit(round_no,'model_response',{'final':True,'response':{'type':'reply','reply':answer},'usage':usage})
            state={'engine':VERSION,'question':prepared['question'],'answer':answer,'memory':{k:v for k,v in memory.items() if k!='summary_calls'}}
            result={'type':'propose_plan' if ctx.plan is not None else 'reply','reply':answer,'execution_status':'尚未执行','evidence_refs':[]}
            if ctx.plan is not None:result['plan']=ctx.plan
            return (ctx.plan if ctx.purpose=='daily' else result),usage_summary(calls,started,input_bytes,memory),state
        emit(round_no,'model_response',{'response':{'type':'tool_calls','reply':wire.get('content',''),'calls':pending},'usage':usage})
        messages.append(wire);results=[];batch_bytes=0
        for call in pending:
            permitted();tool_count+=1;tool_started=time.monotonic()
            if tool_count>MAX_TOOL_CALLS or call.get('id') in ids:raise plan_provider.Failure('tool_limit','工具次数超限或调用编号重复，未执行计划',usage_summary(calls,started,input_bytes,memory))
            ids.add(call['id']);name=call['function']['name'];args=call['function']['arguments']
            before=(set(ctx.settings),set(ctx.evidence),set(ctx.observations),ctx.observe_checked)
            # 默认1000只改变条数；给下一次请求与收尾留余量，不扩大220KB/1MB费用保护。
            ctx.output_budget=min(96000,max(0,MAX_CONTEXT_BYTES-len(dumps(messages).encode('utf8'))-len(dumps(active_tools).encode('utf8'))-12000))
            arguments=None
            try:
                arguments=plan_provider.json_object(args);result=tools.dispatch(ctx,prepared['tools_snapshot'],name,arguments)
                size=len(dumps(result).encode('utf8'))
                if size>ctx.output_budget or batch_bytes+size>160000:
                    ctx.settings,ctx.evidence,ctx.observations,ctx.observe_checked=before
                    result={'status':'too_large','message':'本批查询结果过大，未提供全文；请缩小对象、limit或分轮查询，不能把省略当0'}
                batch_bytes+=len(dumps(result).encode('utf8'))
            except DomainError as error:
                result={'status':'error','message':str(error)}
                if name=='submit_optimization_plan':
                    from .tools.planning import repair_hint
                    result['repair']=repair_hint(ctx,arguments)
            except (ValueError,TypeError,RecursionError):result={'status':'error','message':'工具参数不是有效JSON对象或字段类型无效，请按工具Schema修正'}
            except Exception:result={'status':'error','message':'ERP工具未完成，可能是本地资料/结构异常；缺失不是0，本工具没有写入Amazon'}
            if result.get('status')=='error':last_tool_error=result.get('message')
            results.append({'tool_call_id':call['id'],'name':name,'arguments':args,'result':result,
                'elapsed_ms':round((time.monotonic()-tool_started)*1000),'result_bytes':len(dumps(result).encode('utf8')),
                'activity':tool_activity.call_summary(name,arguments,result)})
            messages.append({'role':'tool','tool_call_id':call['id'],'content':dumps(result)})
        emit(round_no,'data_response',{'tool_results':results})
    note='本次AI分析已达'+str(MAX_CALLS)+'轮上限，未形成可提交结果，未执行任何改动，不自动重发'
    if last_tool_error:note+='；最后校验原因：'+last_tool_error[:200]
    total=usage_summary(calls,started,input_bytes,memory)
    # 这只是本地结束事件，不登记model_request、不再收费；让主对话直接看到失败原因。
    emit(MAX_CALLS+1,'model_response',{'error_code':'tool_limit','message':note,'usage':{},'aggregate_usage':total})
    raise plan_provider.Failure('tool_limit',note,total)
