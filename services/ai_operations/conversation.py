"""原生工具调用任务；长期上下文仅问答，工具数据只在本任务循环内回传。"""
import time
from copy import deepcopy
from services.listing_performance.common import DomainError
from . import conversation_store as store,plan_provider,natural_history,tools
from .metrics import dumps,digest
from . import tool_activity,prompt_settings,task_budget

VERSION='native-tools-v1'
MAX_CALLS=64
MAX_TOOL_CALLS=256
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


def usage_summary(calls,started,input_bytes,memory=None,stats=None):
    """模型分析与独立摘要用量分开列示；未知用量不伪造为0。"""
    result={'provider':'deepseek','billable':any(r.get('billable',True) for r in [*calls,*((memory or {}).get('summary_calls') or [])]),'rounds':calls,'round_count':len(calls),
            'elapsed_ms':round((time.monotonic()-started)*1000),'total_input_bytes':input_bytes,
            'summary_calls':(memory or {}).get('summary_calls',[]),'memory':{k:v for k,v in (memory or {}).items() if k!='summary_calls'}}
    for key in ('prompt_tokens','completion_tokens','total_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens'):
        result[key]=sum(r[key] for r in calls) if calls and all(type(r.get(key)) is int for r in calls) else None
    result.update(stats or {})
    return result


def run(data,prepared,session,plan_id,invoke,check_permissions):
    """原生工具循环；模型自然回答与已校验计划独立，任何失败都不会进入执行。"""
    started=time.monotonic();calls=[];input_bytes=0;tool_count=0;ids=set();ctx=ToolContext(data,session,prepared['conversation']['purpose'])
    frozen=prepared['prompt_snapshot'];config=prompt_settings.validate(frozen['config']);texts=config['prompts']
    stats={'input_limit_bytes':config['limits']['total_input_bytes'],'max_request_bytes':0,'tool_result_bytes':0,'request_sizes':[]}
    def summary_invoke(source,request):
        """独立历史摘要也计入任务输入，但用量仍保留在原summary_calls中。"""
        nonlocal input_bytes
        length=task_budget.size(request['messages'])+task_budget.size(request.get('tools',[]))
        if input_bytes+length>config['limits']['total_input_bytes']:
            raise plan_provider.Failure('task_input_limit','历史摘要所需输入超过任务剩余额度',{'billable':False,'input_bytes':0})
        input_bytes+=length;stats['summary_input_bytes']=stats.get('summary_input_bytes',0)+length
        stats['max_request_bytes']=max(stats['max_request_bytes'],length)
        try:answer,usage=invoke(source,request)
        except plan_provider.Failure as error:
            raise plan_provider.Failure(error.code,str(error),{**(error.usage or {}),'input_bytes':length}) from None
        return answer,{**usage,'input_bytes':length}
    try:history,memory=natural_history.context(session,summary_invoke,check_permissions,frozen)
    except plan_provider.Failure as error:
        prior=error.usage or {};total=usage_summary(calls,started,input_bytes,{'summary_calls':prior.get('summary_calls',[])},stats)
        total['billable']=prior.get('billable',False)
        raise plan_provider.Failure(error.code,str(error),total) from None
    # 跨任务仅自然问答作为引用资料；不伪造旧assistant工具消息/思考字段，避免供应商协议不完整。
    messages=[{'role':'system','content':prepared['system']},
              {'role':'user','content':dumps({'current_scope':prepared['scope'],'past_question_answers':history})},
              {'role':'user','content':prepared['question']}]
    def permitted():
        """中途停止仍带回已发生用量，不因权限/租约变化丢失之前的计费回执。"""
        try:check_permissions()
        except DomainError as error:raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes,memory,stats)) from None
    def emit(round_no,kind,payload):
        """复用不可变事件与租约，同一轮不重复收费；工具过程仅在审计层保存。"""
        try:
            return store.append(session['id'],plan_id,round_no,kind,{'engine':VERSION,**payload},
                task_id=session.get('task_id'),owner_token=session.get('owner_token'))
        except DomainError as error:raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes,memory,stats)) from None
    last_tool_error=None
    for round_no in range(1,MAX_CALLS+1):
        permitted()
        active_tools=prepared['tools_snapshot']['tools']
        outgoing,budget,length=task_budget.request(messages,active_tools,input_bytes,config)
        budget['max_request_bytes']=max(stats['max_request_bytes'],length)
        code=None
        if input_bytes+length>config['limits']['total_input_bytes']:code='task_input_limit'
        elif length>task_budget.TECH_CONTEXT_BYTES:code='request_safety_limit'
        elif time.monotonic()-started>MAX_ELAPSED_SECONDS-plan_provider.TIMEOUT:code='elapsed_limit'
        if code:
            stats['stop_reason']=code;stats['blocked_request_bytes']=length
            note={'task_input_limit':f"任务累计输入额度不足：已发送{input_bytes}字节，下一轮需{length}字节，上限{config['limits']['total_input_bytes']}字节",
                  'request_safety_limit':f'单次请求体超过技术保护：{length}/{task_budget.TECH_CONTEXT_BYTES}字节',
                  'elapsed_limit':'任务达到耗时保护，未发送下一轮请求'}[code]
            if last_tool_error:note+='；最后校验原因：'+last_tool_error[:200]
            emit(round_no,'model_response',{'error_code':code,'message':note,'usage':{},'budget':budget})
            raise plan_provider.Failure(code,note,usage_summary(calls,started,input_bytes,memory,stats))
        if not emit(round_no,'model_request',{'messages':audit_messages(outgoing),'wire_hash':digest(outgoing),'budget':budget,
                'question':prepared['question'],'request':{**prepared['request'],'tools':active_tools},
                'memory':memory,'prompt_version':{'id':frozen['version_id'],'hash':frozen['hash']},
                'tools_hash':digest(active_tools),'tools_snapshot_hash':prepared['tools_snapshot']['hash']}):
            raise plan_provider.Failure('unknown','本轮请求已登记，不重复收费',usage_summary(calls,started,input_bytes,memory,stats))
        input_bytes+=length
        stats['max_request_bytes']=max(stats['max_request_bytes'],length)
        stats['request_sizes'].append({'round_no':round_no,'input_bytes':length,'cumulative_input_bytes':input_bytes,'stage':budget['stage']})
        try:wire,usage=invoke(data,{'messages':outgoing,'tools':active_tools})
        except plan_provider.Failure as error:
            calls.append(error.usage or {'billable':True})
            emit(round_no,'model_response',{'error_code':error.code,'message':str(error),'usage':error.usage})
            raise plan_provider.Failure(error.code,str(error),usage_summary(calls,started,input_bytes,memory,stats)) from None
        calls.append({**usage,'round_no':round_no,'input_bytes':length,'cumulative_input_bytes':input_bytes});wire=deepcopy(wire)
        if not isinstance(wire,dict) or wire.get('role')!='assistant':raise plan_provider.Failure('invalid_output','模型没有返回原生assistant消息，未执行计划',usage_summary(calls,started,input_bytes,memory,stats))
        pending=wire.get('tool_calls') or []
        if ctx.plan is not None and pending:
            note='计划已校验，但模型在收尾阶段仍返回工具调用，未形成最终回答；本次未执行，不自动重发'
            total=usage_summary(calls,started,input_bytes,memory,stats)
            emit(round_no,'model_response',{'error_code':'invalid_output','message':note,'usage':usage})
            raise plan_provider.Failure('invalid_output',note,total)
        if not pending:
            answer=wire.get('content')
            if not isinstance(answer,str) or not answer.strip():raise plan_provider.Failure('invalid_output','模型没有给出最终自然回答，未执行计划',usage_summary(calls,started,input_bytes,memory,stats))
            if (ctx.purpose=='daily' or prepared['conversation'].get('mode')=='proposal') and ctx.plan is None:
                emit(round_no,'model_response',{'response':{'type':'tool_progress','reply':answer},'usage':usage})
                messages.append(wire);messages.append({'role':'user','content':texts['plan_required']})
                continue
            emit(round_no,'model_response',{'final':True,'response':{'type':'reply','reply':answer},'usage':usage})
            state={'engine':VERSION,'question':prepared['question'],'answer':answer,'memory':{k:v for k,v in memory.items() if k!='summary_calls'}}
            result={'type':'propose_plan' if ctx.plan is not None else 'reply','reply':answer,'execution_status':'尚未执行','evidence_refs':[]}
            if ctx.plan is not None:result['plan']=ctx.plan
            return (ctx.plan if ctx.purpose=='daily' else result),usage_summary(calls,started,input_bytes,memory,stats),state
        emit(round_no,'model_response',{'response':{'type':'tool_calls','reply':wire.get('content',''),'calls':pending},'usage':usage})
        messages.append(wire);results=[];batch_bytes=0
        for call in pending:
            permitted();tool_count+=1;tool_started=time.monotonic()
            if tool_count>MAX_TOOL_CALLS or call.get('id') in ids:raise plan_provider.Failure('tool_limit','工具次数超限或调用编号重复，未执行计划',usage_summary(calls,started,input_bytes,memory,stats))
            ids.add(call['id']);name=call['function']['name'];args=call['function']['arguments']
            before=(set(ctx.settings),set(ctx.evidence),set(ctx.observations),ctx.observe_checked)
            # 剩余额度必须能再次携带已有协议；资料分页边界不再绑定220KB单轮业务上限。
            ctx.output_budget=min(96000,max(0,config['limits']['total_input_bytes']-input_bytes-task_budget.size(messages)-task_budget.size(active_tools)-16000))
            arguments=None
            try:
                arguments=plan_provider.json_object(args);result=tools.dispatch(ctx,prepared['tools_snapshot'],name,arguments)
                size=len(dumps(result).encode('utf8'))
                if name!='submit_optimization_plan' and (size>ctx.output_budget or batch_bytes+size>160000):
                    ctx.settings,ctx.evidence,ctx.observations,ctx.observe_checked=before
                    result={'status':'budget_exceeded','reason_code':'delivery_budget','message':'查询正文未交付：本批剩余返回预算不足','returned':0}
                batch_bytes+=len(dumps(result).encode('utf8'))
            except DomainError as error:
                result={'status':'error','message':str(error)}
                if name=='submit_optimization_plan':
                    from .tools.planning import repair_hint
                    result['repair']=repair_hint(ctx,arguments)
                    result['instruction']=texts['plan_repair']
            except (ValueError,TypeError,RecursionError):result={'status':'error','message':'工具参数JSON或字段类型无效','instruction':texts['plan_repair']}
            except Exception:result={'status':'error','message':'ERP工具未完成，可能是本地资料/结构异常；缺失不是0，本工具没有写入Amazon'}
            if result.get('status')=='error':last_tool_error=result.get('message')
            result=task_budget.delivery(result,name,arguments,texts['tool_delivery'])
            stats['tool_result_bytes']+=task_budget.size(result)
            results.append({'tool_call_id':call['id'],'name':name,'arguments':args,'result':result,
                'elapsed_ms':round((time.monotonic()-tool_started)*1000),'result_bytes':len(dumps(result).encode('utf8')),
                'activity':tool_activity.call_summary(name,arguments,result)})
            messages.append({'role':'tool','tool_call_id':call['id'],'content':dumps(result)})
        emit(round_no,'data_response',{'tool_results':results})
        if ctx.plan is not None:
            # 校验方案已包含中文summary；直接形成成果，不再为收尾额外调用模型。
            answer=ctx.plan['summary']
            emit(round_no+1,'model_response',{'final':True,'response':{'type':'reply','reply':answer},'usage':{},'completion_source':'validated_plan_summary'})
            state={'engine':VERSION,'question':prepared['question'],'answer':answer,'memory':{k:v for k,v in memory.items() if k!='summary_calls'}}
            result={'type':'propose_plan','reply':answer,'execution_status':'尚未执行','evidence_refs':[],'plan':ctx.plan}
            return (ctx.plan if ctx.purpose=='daily' else result),usage_summary(calls,started,input_bytes,memory,stats),state
    note='本次AI分析已达'+str(MAX_CALLS)+'轮上限，未形成可提交结果，未执行任何改动，不自动重发'
    if last_tool_error:note+='；最后校验原因：'+last_tool_error[:200]
    total=usage_summary(calls,started,input_bytes,memory,stats)
    # 这只是本地结束事件，不登记model_request、不再收费；让主对话直接看到失败原因。
    emit(MAX_CALLS+1,'model_response',{'error_code':'tool_limit','message':note,'usage':{},'aggregate_usage':total})
    raise plan_provider.Failure('tool_limit',note,total)
