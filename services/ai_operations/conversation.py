"""一次每日任务中的持久会话恢复、按需补资料、最终方案；此处不提交Amazon。"""
import time
from services.listing_performance.common import DomainError
from . import conversation_store as store, conversation_context as context, plan_contract, plan_provider, prompt_configuration, prompt_compiler
from .metrics import dumps, digest

MAX_CALLS = 3
MAX_TOTAL_INPUT_BYTES = 300000
MAX_ELAPSED_SECONDS = 540


def prepare(data,queue_task=None):
    """恢复ASIN会话并保存可读任务文字和真实资料，二者都进入实际模型请求。"""
    snapshot = prompt_configuration.active()
    session = store.open_session(data['content']['identity'])
    session['purpose'] = 'daily'
    if queue_task:session.update(task_id=queue_task['id'],owner_token=queue_task['owner_token'])
    return prompt_compiler.prepare(data,session,snapshot), session


def memory(value):
    """校验下一版结构化记忆的字段与长度，不用默认值掩盖模型遗漏。"""
    plan_contract.fields(value,('strategy','focus','open_questions','lessons'),'运营记忆')
    plan_contract.text(value['strategy'],'下一版运营策略',2000)
    for field in ('focus','open_questions','lessons'):
        rows = value[field]
        if not isinstance(rows,list) or len(rows)>10:raise DomainError('运营记忆条数超限',422)
        for item in rows:plan_contract.text(item,'运营记忆项',500)
    return value


def usage_summary(calls, started, input_bytes):
    """汇总各轮实际用量；任一轮未知时不伪造累计token。"""
    result = {'provider':'deepseek','billable':any(c.get('billable',True) for c in calls),
              'execution_mode':'real','round_count':len(calls),'rounds':calls,
              'elapsed_ms':round((time.monotonic()-started)*1000),'total_input_bytes':input_bytes,
              'token_note':'累计实际供应商用量；任一轮缺失则总量未知。字节限额不是token估算或价格承诺。'}
    for field in ('prompt_tokens','completion_tokens','total_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens'):
        result[field] = sum(c[field] for c in calls) if calls and all(type(c.get(field)) is int for c in calls) else None
    if calls:
        for key in ('request_id','returned_model','finish_reason','raw_output'):result[key] = calls[-1].get(key)
    return result


def run(data, prepared, session, plan_id, invoke, check_permissions):
    """在同一任务内取数或纠错，最终方案校验通过才交给每日执行流程。"""
    history = []; calls = []; requested = {}; input_bytes = 0; started = time.monotonic()
    human=session.get('purpose')=='chat'
    def emit(round_no,event_type,payload,archives=None):
        """同一编排按每日方案或人工任务绑定消息，均先登记实际请求和附件。"""
        return store.append(session['id'],plan_id,round_no,event_type,payload,archives=archives,
                            task_id=session.get('task_id'),owner_token=session.get('owner_token'))
    for round_no in range(1,MAX_CALLS+1):
        try:check_permissions()
        except DomainError as error:
            raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes)) from None
        remaining = MAX_CALLS-round_no+1
        allow_data=prompt_compiler.allow_data(prepared,history,remaining,input_bytes,time.monotonic()-started)
        messages,control=prompt_compiler.messages(prepared,history,remaining,allow_data)
        byte_count = context.size(messages)
        if byte_count>context.MAX_CONTEXT_BYTES or input_bytes+byte_count>MAX_TOTAL_INPUT_BYTES or time.monotonic()-started>MAX_ELAPSED_SECONDS-plan_provider.TIMEOUT:
            raise plan_provider.Failure('context_limit','会话达到本次长度或耗时上限，已保存各轮回答，未执行修改，不自动重发',
                                        usage_summary(calls,started,input_bytes))
        call = {**prepared,'messages':messages}
        fresh = emit(round_no,'model_request',
                             {'messages':messages,'request':prepared['request'],'input_bytes':byte_count,'control':control,
                              'interaction':{'version_id':prepared['interaction']['version_id'],'hash':prepared['interaction']['hash'],
                                  'published_hash':prepared['interaction'].get('published_hash'),
                                  'schema_migration':prepared['interaction'].get('schema_migration',False),
                                  'matched_scenes':prepared['interaction']['matched_scenes']},
                              'attachment_manifest':prompt_compiler.manifest(prepared,data)},
                             archives={field:data['content'][field] for _,fields in prompt_configuration.DATA.values() for field in fields if field in data['content']} if round_no==1 else {})
        if not fresh:raise plan_provider.Failure('unknown','此轮模型请求已登记，不重复收费；请查看会话记录',usage_summary(calls,started,input_bytes))
        input_bytes += byte_count
        try:
            raw, usage = invoke(data,call)
        except plan_provider.Failure as error:
            calls.append({**(error.usage or {'provider':'deepseek','billable':error.code!='configuration'}),'round_no':round_no})
            emit(round_no,'model_response',{'error_code':error.code,'message':str(error),'usage':calls[-1]})
            raise plan_provider.Failure(error.code,str(error),usage_summary(calls,started,input_bytes)) from None
        calls.append({**usage,'round_no':round_no})
        emit(round_no,'model_response',{'response':raw,'usage':calls[-1]})
        try:
            if human and isinstance(raw,dict) and raw.get('type') in ('reply','propose_plan'):
                from . import human_contract
                return human_contract.validate(raw,data),usage_summary(calls,started,input_bytes),memory(raw['memory'])
            if not isinstance(raw,dict) or raw.get('type') not in (('request_data',) if human else ('request_data','final_plan')):
                raise DomainError('AI回答必须为request_data或final_plan会话协议',422)
            if raw['type']=='final_plan':
                plan_contract.fields(raw,('type','plan','memory'),'最终会话方案')
                state = memory(raw['memory'])
                validated = plan_contract.validate(raw['plan'],data)
                return validated,usage_summary(calls,started,input_bytes),state
            if not allow_data:raise DomainError('最后可用一轮仍索取资料，未形成最终方案，不执行修改',422)
            results = []; new_keys = []
            for request in context.requests(raw):
                key = digest({k:v for k,v in request.items() if k!='reason'})
                if key in requested:
                    result = {'status':'already_provided','original_round':requested[key]['round_no'],
                              'data_hash':requested[key]['hash'],'note':prompt_compiler.scene(prepared,'duplicate_data')}
                else:
                    result = context.fetch(data,request,session=session)
                    if result.get('status')=='unavailable':result['guidance']=prompt_compiler.scene(prepared,'data_unavailable')
                    requested[key] = {'round_no':round_no,'hash':digest(result)}
                    new_keys.append(key)
                results.append({'request':request,'result':result})
            payload = {'data_responses':results,'remaining_model_calls':remaining-1,
                       'note':prompt_compiler.scene(prepared,'data_response')}
            # 多个28KB对象不能挤掉收尾空间；整个补充包过大则显式要求更小分页。
            if context.size(payload)>context.MAX_DATA_BYTES:
                payload = {'data_responses':[{'status':'unavailable','reason':prompt_compiler.scene(prepared,'data_too_large')}],
                           'remaining_model_calls':remaining-1}
                for key in new_keys:requested.pop(key,None)
            emit(round_no,'data_response',payload)
            history.extend([{'role':'assistant','content':dumps(raw)},{'role':'user','content':dumps(payload)}])
        except DomainError as error:
            feedback = {'validation_feedback':{'message':str(error),
                'required_final_keys':['type','plan','memory'],
                'required_plan_keys':['summary','actions','notes','history_reviews'],
                'required_memory_keys':['strategy','focus','open_questions','lessons'],
                'instruction':prompt_compiler.scene(prepared,'human_validation_feedback' if human else 'validation_feedback')},
                'remaining_model_calls':remaining-1}
            if human:
                feedback['validation_feedback']['required_final_keys']=['type','reply','memory','plan（仅propose_plan）或evidence_refs（仅reply）']
            candidate = [*history,{'role':'assistant','content':dumps(raw)},
                         {'role':'user','content':dumps(feedback),'validation_feedback':True}]
            future_bytes = context.size(prompt_compiler.messages(prepared,candidate,remaining-1,False)[0])+1000
            if (remaining>1 and isinstance(raw,dict) and raw.get('type') in (('reply','propose_plan') if human else ('final_plan',))
                and future_bytes<=context.MAX_CONTEXT_BYTES and input_bytes+future_bytes<=MAX_TOTAL_INPUT_BYTES
                and time.monotonic()-started+plan_provider.TIMEOUT<=MAX_ELAPSED_SECONDS):
                emit(round_no,'validation_feedback',feedback)
                history=candidate
                continue
            raise plan_provider.Failure('invalid_output',str(error),usage_summary(calls,started,input_bytes)) from None
    raise plan_provider.Failure('invalid_output','未形成最终方案，未执行修改',usage_summary(calls,started,input_bytes))
