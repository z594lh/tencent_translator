"""任务累计输入额度：只计算真实序列化内容，提醒文本统一来自冻结配置。"""
from .metrics import dumps,digest

TECH_CONTEXT_BYTES=2000000


def size(value):
    return len(dumps(value).encode('utf8'))


def request(messages,tools,used,config):
    """替换本轮额度消息而非重复堆积；固定点包含提醒自身字节。"""
    limits=config['limits'];cap=limits['total_input_bytes'];texts=config['prompts']
    base=list(messages);length=size(base)+size(tools);stage='normal';level=0
    for _ in range(12):
        percent=(used+length)*100/cap
        level=max(level,2 if percent>=limits['urgent_percent'] else 1 if percent>=limits['warn_percent'] else 0)
        stage=('normal','warning','urgent')[level]
        budget={'limit_bytes':cap,'sent_input_bytes':used,'next_request_bytes':length,
                'projected_total_bytes':used+length,'remaining_after_request_bytes':max(0,cap-used-length),'stage':stage}
        row={'role':'user','content':dumps({'task_budget':budget,'instruction':texts['budget_'+stage]})}
        result=[*base,row];measured=size(result)+size(tools)
        if measured==length:return result,budget,measured
        length=measured
    # 数字位数或阈值跨越无法收敛时，采用实际最终测量，不低估拦截所需额度。
    return result,budget,size(result)+size(tools)


def delivery(result,tool,args,instruction):
    """只对未交付/分页补充可执行查询参数，不生成不存在的资料。"""
    if not isinstance(result,dict):return result
    if result.get('status') in ('budget_exceeded','too_large') or result.get('has_more') or result.get('byte_limited'):
        result['instruction']=instruction
        if tool not in ('read_data_bundle','get_operation_overview'):
            query=dict(args or {})
            if result.get('next_offset') is not None:query['offset']=result['next_offset']
            result['retry_query']={'tool':tool,'arguments':query}
    for item in result.get('queries',result.get('sections',[])):
        delivery(item.get('result'),item.get('tool'),item.get('query',{}),instruction)
    if 'data_hash' in result:
        result.pop('data_hash');result['data_hash']=digest(result)
    return result
