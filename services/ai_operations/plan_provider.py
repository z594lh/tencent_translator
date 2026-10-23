"""DeepSeek原生工具/自然文本传输；思考字段仅在本任务内回传，不持久化或展示。"""
import json
import time
from openai import APIStatusError
from services.deepseekAI import get_deepseek_client
from . import plan_contract as contract

PROVIDER='deepseek'
VERSION='deepseek-native-tools-v1'
TIMEOUT=120.0


class Failure(Exception):
    """可公开的请求类别及实际用量；不把网络未知误判为未收费。"""
    def __init__(self,code,message,usage=None):
        """保存脱敏消息和已知回执，不保存请求凭据。"""
        super().__init__(message);self.code=code;self.usage=usage


def request_spec(summary=False):
    """描述真实参数；不再启用整段JSON回答或自定义final_plan协议。"""
    return {'provider':PROVIDER,'model':contract.MODEL,'timeout_seconds':TIMEOUT,'max_retries':0,
            'thinking':{'type':'disabled' if summary else 'enabled'},'max_tokens':4000 if summary else contract.MAX_OUTPUT_TOKENS,
            'stream':False,'response_format':'text','protocol':VERSION}


def create_client():
    """复用现有凭据，关闭SDK自动重试，防止未知回执重复收费。"""
    try:return get_deepseek_client().with_options(timeout=TIMEOUT,max_retries=0)
    except Exception:raise Failure('configuration','DeepSeek配置不可用，尚未调用模型') from None


def json_object(text):
    """仅解析工具参数；拒绝重复键、非标准数字和对象外多余内容。"""
    def unique(rows):
        """同名参数不能被后面的键覆盖。"""
        value={}
        for key,item in rows:
            if key in value:raise ValueError('重复参数')
            value[key]=item
        return value
    def invalid(_):
        """拒绝NaN/Infinity，金额仍使用业务校验器。"""
        raise ValueError('非标准数值')
    value=json.loads(text,object_pairs_hook=unique,parse_constant=invalid)
    if not isinstance(value,dict):raise ValueError('参数不是对象')
    return value


def generate(client,prepared):
    """一次原生调用；当前任务可带工具，摘要调用只有自然问答，不记录隐式思维链。"""
    summary=bool(prepared.get('summary'));usage={'provider':PROVIDER,'billable':True,'requested_model':contract.MODEL}
    started=time.monotonic()
    try:
        params={'model':contract.MODEL,'messages':prepared['messages'],'stream':False,
                'max_tokens':4000 if summary else contract.MAX_OUTPUT_TOKENS,
                'extra_body':{'thinking':{'type':'disabled' if summary else 'enabled'}}}
        if prepared.get('tools'):params.update(tools=prepared['tools'],tool_choice='auto')
        if not summary:params['extra_body']['reasoning_effort']='high'
        reply=client.chat.completions.create(**params)
        usage.update(request_id=str(getattr(reply,'id','') or '')[:200],returned_model=str(getattr(reply,'model','') or '')[:100],elapsed_ms=round((time.monotonic()-started)*1000))
        counts=getattr(reply,'usage',None)
        for key in ('prompt_tokens','completion_tokens','total_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens'):
            value=getattr(counts,key,None)
            if type(value) is int and value>=0:usage[key]=value
        choice=reply.choices[0];message=choice.message;content=message.content;calls=getattr(message,'tool_calls',None) or []
        usage['finish_reason']=choice.finish_reason
        if isinstance(content,str) and len(content.encode('utf8'))<=160000:usage['raw_output']=content
        if choice.finish_reason not in ('stop','tool_calls') or len(calls)>8 or (content is not None and (not isinstance(content,str) or len(content.encode('utf8'))>160000)):
            raise Failure('incomplete_output','模型输出被截断、超限或未正常结束；保留原回执，不自动重发',usage)
        wire={'role':'assistant','content':content or ''}
        if calls:
            wire['tool_calls']=[{'id':r.id,'type':'function','function':{'name':r.function.name,'arguments':r.function.arguments}} for r in calls]
            for row in wire['tool_calls']:
                if not isinstance(row['id'],str) or not 1<=len(row['id'])<=200 or not isinstance(row['function']['name'],str) or not isinstance(row['function']['arguments'],str) or len(row['function']['arguments'].encode('utf8'))>64000:raise Failure('invalid_output','模型工具调用结构无效，未执行工具',usage)
        elif not wire['content'].strip():raise Failure('invalid_output','模型没有返回自然回答或工具调用；未自动重发',usage)
        reasoning=getattr(message,'reasoning_content',None)
        if not summary and isinstance(reasoning,str):wire['reasoning_content']=reasoning
        return wire,usage
    except Failure:raise
    except APIStatusError as error:
        usage.update(http_status=error.status_code,elapsed_ms=round((time.monotonic()-started)*1000))
        raise Failure('http_error','DeepSeek返回HTTP '+str(error.status_code)+'；请核对服务或额度，不自动重发',usage) from None
    except (ValueError,TypeError,AttributeError,IndexError,RecursionError):raise Failure('invalid_output','DeepSeek回包结构异常，原回执已保留，不自动重发',usage) from None
    except Exception:raise Failure('unknown','模型请求未收到可靠回执，可能已计费；仅核对原记录，不自动重发',usage) from None
