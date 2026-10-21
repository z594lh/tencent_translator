"""一次每日任务中的持久会话恢复、按需补资料、最终方案；此处不提交Amazon。"""
import time
from services.listing_performance.common import DomainError
from . import conversation_store as store, conversation_context as context, plan_contract, plan_provider
from .metrics import dumps, digest

MAX_CALLS = 3
MAX_TOTAL_INPUT_BYTES = 300000
MAX_ELAPSED_SECONDS = 540


def prepare(data):
    """恢复ASIN会话并保存可读任务文字和真实资料，二者都进入实际模型请求。"""
    session = store.open_session(data['content']['identity'])
    seed = context.seed(data,session)
    question = ('请继续运营ASIN '+data['content']['identity']['asin']+'，报表截至'+str(data['content']['time']['report_end'])+'。'
                '请先复盘往期改动和观察周期，再综合Listing、售价成本、经营、广告与关键词资料，给出必要的具体修改和后续观察方案。'
                '如需明细请提出资料请求，资料足够后返回最终方案及下一版运营记忆。最终行动通过ERP正式接口提交，接受与生效必须分开核对。')
    prompt = {'system':context.SYSTEM,'question':question,'user':dumps(seed),'request':plan_provider.request_spec(),
              'conversation':{'id':str(session['id']),'context_version':session['revision'],
                 'protocol':context.VERSION,'max_calls':MAX_CALLS,'max_context_bytes':context.MAX_CONTEXT_BYTES,
                 'max_total_input_bytes':MAX_TOTAL_INPUT_BYTES,'max_elapsed_seconds':MAX_ELAPSED_SECONDS,
                 'archived_bytes':data['size_bytes'],'seed_bytes':context.size(seed)}}
    return prompt, session


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
    for field in ('prompt_tokens','completion_tokens','total_tokens'):
        result[field] = sum(c[field] for c in calls) if calls and all(type(c.get(field)) is int for c in calls) else None
    if calls:
        for key in ('request_id','returned_model','finish_reason','raw_output'):result[key] = calls[-1].get(key)
    return result


def run(data, prepared, session, plan_id, invoke, check_permissions):
    """在同一任务内取数或纠错，最终方案校验通过才交给每日执行流程。"""
    history = []; calls = []; requested = {}; input_bytes = 0; started = time.monotonic()
    for round_no in range(1,MAX_CALLS+1):
        try:check_permissions()
        except DomainError as error:
            raise plan_provider.Failure('stopped',str(error),usage_summary(calls,started,input_bytes)) from None
        remaining = MAX_CALLS-round_no+1
        messages = [{'role':'system','content':prepared['system']},
                    {'role':'user','content':prepared['question']},
                    {'role':'user','content':prepared['user']}, *history]
        base_bytes = context.size(messages)+1000
        # 提前要求收尾，不能明知下一轮无长度/耗时预算仍鼓励索取资料。
        allow_data = (remaining>1 and input_bytes+2*base_bytes+context.MAX_DATA_BYTES<=MAX_TOTAL_INPUT_BYTES
                      and base_bytes+context.MAX_DATA_BYTES+2000<=context.MAX_CONTEXT_BYTES
                      and time.monotonic()-started+2*plan_provider.TIMEOUT<=MAX_ELAPSED_SECONDS)
        repairing = bool(history and history[-1].get('validation_feedback'))
        # 发送的messages只能包含供应商支持的role/content，内部标志不发往API。
        messages = [{k:v for k,v in message.items() if k!='validation_feedback'} for message in messages]
        if repairing:allow_data=False
        control = {'remaining_model_calls':remaining if allow_data else 1,'data_requests_allowed':allow_data,
                   'instruction':'如需补充资料请按request_data索取；资料足够则给final_plan。' if allow_data else '本轮必须给完整final_plan，顶层type/plan/memory，plan的notes/history_reviews即使为空也给[]；缺资料的对象保持观察，不再索取资料。'}
        messages.append({'role':'user','content':dumps({'task_control':control})})
        byte_count = context.size(messages)
        if byte_count>context.MAX_CONTEXT_BYTES or input_bytes+byte_count>MAX_TOTAL_INPUT_BYTES or time.monotonic()-started>MAX_ELAPSED_SECONDS-plan_provider.TIMEOUT:
            raise plan_provider.Failure('context_limit','会话达到本次长度或耗时上限，已保存各轮回答，未执行修改，不自动重发',
                                        usage_summary(calls,started,input_bytes))
        call = {**prepared,'messages':messages}
        fresh = store.append(session['id'],plan_id,round_no,'model_request',
                             {'messages':messages,'request':prepared['request'],'input_bytes':byte_count,'control':control})
        if not fresh:raise plan_provider.Failure('unknown','此轮模型请求已登记，不重复收费；请查看会话记录',usage_summary(calls,started,input_bytes))
        input_bytes += byte_count
        try:
            raw, usage = invoke(data,call)
        except plan_provider.Failure as error:
            calls.append({**(error.usage or {'provider':'deepseek','billable':error.code!='configuration'}),'round_no':round_no})
            store.append(session['id'],plan_id,round_no,'model_response',{'error_code':error.code,'message':str(error),'usage':calls[-1]})
            raise plan_provider.Failure(error.code,str(error),usage_summary(calls,started,input_bytes)) from None
        calls.append({**usage,'round_no':round_no})
        store.append(session['id'],plan_id,round_no,'model_response',{'response':raw,'usage':calls[-1]})
        try:
            if not isinstance(raw,dict) or raw.get('type') not in ('request_data','final_plan'):
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
                              'data_hash':requested[key]['hash'],'note':'本次任务没有更新版本，请使用前文已提供资料，不重复索取'}
                else:
                    result = context.fetch(data,request,session=session)
                    requested[key] = {'round_no':round_no,'hash':digest(result)}
                    new_keys.append(key)
                results.append({'request':request,'result':result})
            payload = {'data_responses':results,'remaining_model_calls':remaining-1,
                       'note':'ERP只读本次本地归档；缺失不补0，无真实Amazon写入或报表重抓'}
            # 多个28KB对象不能挤掉收尾空间；整个补充包过大则显式要求更小分页。
            if context.size(payload)>context.MAX_DATA_BYTES:
                payload = {'data_responses':[{'status':'unavailable','reason':'本轮补充资料合计过大，请按具体对象/更小limit索取；没有截断全文'}],
                           'remaining_model_calls':remaining-1}
                for key in new_keys:requested.pop(key,None)
            store.append(session['id'],plan_id,round_no,'data_response',payload)
            history.extend([{'role':'assistant','content':dumps(raw)},{'role':'user','content':dumps(payload)}])
        except DomainError as error:
            feedback = {'validation_feedback':{'message':str(error),
                'required_final_keys':['type','plan','memory'],
                'required_plan_keys':['summary','actions','notes','history_reviews'],
                'required_memory_keys':['strategy','focus','open_questions','lessons'],
                'instruction':'请修正并给完整final_plan；同一ref只保留一项，最多12项，不输出占位动作；必要时只保留最重要的改动。'},
                'remaining_model_calls':remaining-1}
            candidate = [*history,{'role':'assistant','content':dumps(raw)},
                         {'role':'user','content':dumps(feedback),'validation_feedback':True}]
            future_bytes = context.size([{'role':'system','content':prepared['system']},
                {'role':'user','content':prepared['question']},
                {'role':'user','content':prepared['user']},*candidate])+1000
            if (remaining>1 and isinstance(raw,dict) and raw.get('type')=='final_plan'
                and future_bytes<=context.MAX_CONTEXT_BYTES and input_bytes+future_bytes<=MAX_TOTAL_INPUT_BYTES
                and time.monotonic()-started+plan_provider.TIMEOUT<=MAX_ELAPSED_SECONDS):
                store.append(session['id'],plan_id,round_no,'validation_feedback',feedback)
                history=candidate
                continue
            raise plan_provider.Failure('invalid_output',str(error),usage_summary(calls,started,input_bytes)) from None
    raise plan_provider.Failure('invalid_output','未形成最终方案，未执行修改',usage_summary(calls,started,input_bytes))
