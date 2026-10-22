"""一次运营方案调用，复用既有DeepSeek配置；未知回执不自动重复收费。"""
import json
import time
from openai import APIStatusError
from services.deepseekAI import get_deepseek_client
from . import plan_contract as contract

PROVIDER = 'deepseek'
VERSION = 'deepseek-conversation-real-v1'
TIMEOUT = 180.0


def request_spec(mode=None):
    """返回实际DeepSeek请求参数，不包含账户凭据。"""
    result={'provider':PROVIDER,'model':contract.MODEL,'execution_mode':'real',
            'endpoint':'https://api.deepseek.com/chat/completions','timeout_seconds':TIMEOUT,
            'max_retries':0,'response_format':{'type':'json_object'},'max_tokens':contract.MAX_OUTPUT_TOKENS,
            'thinking':{'type':'enabled'},'reasoning_effort':'high','stream':False}
    if mode=='discussion':result.pop('response_format')
    return result


def readable_reply(raw):
    """格式失败仅提取首个可解码对象的自然回复供阅读，绝不合并JSON或将其变为可执行方案。"""
    try:
        value,_=json.JSONDecoder().raw_decode(raw.lstrip())
        text=value.get('reply') if isinstance(value,dict) else None
        return text[:20000] if isinstance(text,str) else None
    except (ValueError,TypeError,RecursionError):return None


class Failure(Exception):
    def __init__(self, code, message, usage=None):
        """保存脱敏失败类别和已知用量，供调用链保留费用事实。"""
        super().__init__(message)
        self.code, self.usage = code, usage


def create_client():
    """复用现有DeepSeek配置并关闭自动重试。"""
    try:
        return get_deepseek_client().with_options(timeout=TIMEOUT, max_retries=0)
    except Exception:
        raise Failure('configuration', 'AI配置不可用，未调用模型，请检查已有DEEPSEEK_API_KEY配置') from None


def generate(client, prepared):
    """发出一轮真实模型请求，记录原回答和实际用量，不自动重发。"""
    usage = {'provider':PROVIDER,'billable':True,'requested_model':contract.MODEL,'execution_mode':'real'}
    discussion=prepared.get('conversation',{}).get('mode')=='discussion'
    started = time.monotonic()
    try:
        reply = client.chat.completions.create(model=contract.MODEL,
            messages=prepared.get('messages') or [{'role': 'system', 'content': prepared['system']}, {'role': 'user', 'content': prepared['user']}],
            **({} if discussion else {'response_format':{'type':'json_object'}}), max_tokens=contract.MAX_OUTPUT_TOKENS,
            stream=False, extra_body={'thinking': {'type': 'enabled'},'reasoning_effort':'high'})
        usage.update(request_id=str(getattr(reply,'id','') or '')[:200],
                     returned_model=str(getattr(reply,'model','') or '')[:100],
                     elapsed_ms=round((time.monotonic()-started)*1000))
        data = getattr(reply, 'usage', None)
        counts = {k: getattr(data, k, None) for k in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
        if all(type(v) is int and 0 <= v <= 10000000 for v in counts.values()) and counts['total_tokens'] == counts['prompt_tokens'] + counts['completion_tokens']:
            usage.update(counts)
            # 缓存是供应商性能/费用指标，不是会话记忆；缺失时不伪造0或承诺命中。
            cached={k:getattr(data,k,None) for k in ('prompt_cache_hit_tokens','prompt_cache_miss_tokens')}
            if all(type(v) is int and v>=0 for v in cached.values()) and sum(cached.values())==counts['prompt_tokens']:
                usage.update(cached)
        choice = reply.choices[0]; raw = choice.message.content
        usage['finish_reason']=choice.finish_reason
        # 留存模型的最终JSON，不保存或回传隐式思维链；JSON不通过仍能核查原始回答。
        if isinstance(raw,str) and len(raw.encode('utf8'))<=160000:usage['raw_output']=raw
        if choice.finish_reason != 'stop' or not isinstance(raw, str) or not raw.strip() or len(raw.encode('utf8')) > 160000:
            raise Failure('incomplete_output', 'AI输出为空、截断或超限；原始回答已保留，本次未形成完整结果，不自动重发', usage)
        if discussion:return raw,usage
        def unique(pairs):
            """解析JSON时拒绝重复键，避免模型覆盖同名字段。"""
            result = {}
            for key, value in pairs:
                if key in result:raise ValueError()
                result[key] = value
            return result
        def invalid(_):
            """拒绝JSON中的NaN或Infinity等非标准数值。"""
            raise ValueError()
        return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid), usage
    except Failure:
        raise
    except (ValueError, TypeError, AttributeError, IndexError, RecursionError) as error:
        usage['parse_error']=('存在多个JSON或对象外多余内容' if isinstance(error,json.JSONDecodeError) and error.msg=='Extra data' else '返回结构不符合单个完整JSON要求')
        text=readable_reply(usage.get('raw_output',''))
        if text:usage['readable_reply']=text
        raise Failure('invalid_output', '行动清单格式异常，尚不可执行；原始回答已保留，不自动重发', usage) from None
    except APIStatusError as error:
        usage.update(http_status=error.status_code,elapsed_ms=round((time.monotonic()-started)*1000))
        raise Failure('http_error','DeepSeek返回HTTP '+str(error.status_code)+'；请检查账户额度/配置或服务状态，不自动重发',usage) from None
    except Exception:
        usage['elapsed_ms']=round((time.monotonic()-started)*1000)
        raise Failure('unknown', '模型请求未收到可靠回执，可能已计费；请查看记录，不会自动重发', usage) from None
