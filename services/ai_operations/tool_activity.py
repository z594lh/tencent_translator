"""工具逐轮阅读投影和只读使用统计；兼容MySQL5.7，不读写模型长期记忆。"""
import json
from collections import Counter,defaultdict
from datetime import timezone
from services.listing_performance.common import DomainError,positive_id
from . import plan_store
from .metrics import dumps
from .tools.specs import definitions,READ_TOOLS

MAX_EVENTS=1500
TOKEN_KEYS=('prompt_tokens','completion_tokens','total_tokens','prompt_cache_hit_tokens','prompt_cache_miss_tokens')
WINDOW_LABELS={'all':'全部统计窗口','daily':'逐日明细','d1':'近1日','d3':'近3日','d7':'近7日','p7':'前7日','d14':'近14日','p14':'前14日','d30':'近30日','p30':'前30日'}
REF_LABELS={'spAdvertisedProduct':'本ASIN商品广告','spCampaigns':'关联广告活动','spCampaignsPlacement':'活动广告位','spTargeting':'自动/手动投放','spSearchTerm':'广告搜索词','campaign':'广告活动','group':'广告组','target':'投放对象','keyword':'关键词','listing':'Listing','history':'历史记录','sku':'SKU经营','pricing':'售价成本'}


def label(name):
    """工具代码名称映射为稳定中文名；未知历史名称也不丢弃。"""
    return definitions().get(name,{}).get('label',name)


def query_text(name,args):
    """只展示明确对象和窗口，不把计划JSON或隐式思维链塞入主阅读区。"""
    if name=='submit_optimization_plan':return '提交方案校验；不代表已执行'
    if name=='get_operation_overview':return '当前ASIN · 全周期运营概览'
    if not isinstance(args,dict):return '参数格式异常，原文可查'
    if name=='read_data_bundle':return '批量查询 '+str(len(args.get('queries',[])))+' 项资料' if isinstance(args.get('queries'),list) else '批量取数'
    if name=='simulate_price':return 'SKU '+str(args.get('sku',''))[:100]+' · 假设售价 '+str(args.get('selling_price',''))[:30]+' · 广告费率 '+str(args.get('ad_rate',''))[:30]+' · 退款费率 '+str(args.get('refund_rate',''))[:30]
    if not isinstance(args.get('window','all'),str) or type(args.get('offset',0)) is not int or type(args.get('limit',3)) is not int or not isinstance(args.get('ref',''),str):return '参数类型不符合工具约定，完整参数见原文'
    parts=[WINDOW_LABELS.get(args.get('window','all'),'指定窗口')]
    if args.get('ref'):parts.append('对象：'+REF_LABELS.get(args['ref'],str(args['ref'])[:200]))
    if args.get('sku'):parts.append('SKU：'+str(args['sku'])[:100])
    if args.get('offset'):parts.append('从第'+str(args['offset']+1)+'条起')
    if args.get('limit'):parts.append('最多'+str(args['limit'])+'条')
    return ' · '.join(parts)


def result_status(result):
    """没有status的历史概况/观察返回也保留成功、空缺和异常差别。"""
    if result.get('status'):return result['status']
    if result.get('error'):return 'error'
    if 'rows' in result:return 'provided' if result['rows'] else 'unavailable'
    return 'provided'


def call_summary(name,args,result):
    """把真实结果压成安全阅读元信息；不包含整份报表、文案或内部思考。"""
    children=[]
    for child in result.get('queries',result.get('sections',[])):
        if isinstance(child,dict) and isinstance(child.get('result'),dict):
            value=child['result'];tool=child.get('tool','未知工具');query=child.get('query',{})
            children.append({'name':tool,'label':label(tool),'query':query_text(tool,query),'window':query.get('window','all'),
                'ref_type':str(query.get('ref') or '').split(':')[0][:40], 'status':result_status(value),
                'returned':value.get('returned',len(value['rows']) if isinstance(value.get('rows'),list) else None),
                'message':str(value.get('message') or value.get('error') or '')[:500]})
    if name=='get_operation_overview' and isinstance(result.get('advertising'),list):
        children.insert(0,{'name':'read_advertising','label':label('read_advertising'),'query':'全部广告报告汇总 · 全部统计窗口',
            'window':'all','ref_type':'','status':'provided','returned':len(result['advertising']),'message':'各报告范围独立，不可相加销售'})
    status=result_status(result)
    if children and any(r['status'] not in ('provided','validated') for r in children):status='partial'
    return {'status':status,'returned':result.get('returned',len(result['rows']) if isinstance(result.get('rows'),list) else None),
            'message':str(result.get('message') or result.get('error') or '')[:800],'children':children}


def json_value(raw,default):
    """兼容SQL的NULL/JSON null；解析失败不伪造业务结果。"""
    if not raw:return default
    try:value=json.loads(raw) if isinstance(raw,str) else raw
    except (ValueError,TypeError):return default
    return default if value is None else value


def projection_sql():
    """固定最多8调用/轮的MySQL5.7投影，SQL只取元信息，不下载整份上下文/工具数据。"""
    calls=[];results=[]
    for i in range(8):
        cp="'$.response.calls["+str(i)+"]";rp="'$.tool_results["+str(i)+"]"
        def path(prefix,field):
            """JSON路径仅由固定字段构造，绝不拼接用户输入。"""
            return 'JSON_EXTRACT(m.payload_json,'+prefix+'.'+field+"')"
        calls.append("JSON_OBJECT('id',"+path(cp,'id')+",'name',"+path(cp,'function.name')+",'arguments',CASE WHEN JSON_UNQUOTE("+path(cp,'function.name')+")='submit_optimization_plan' THEN NULL ELSE LEFT(JSON_UNQUOTE("+path(cp,'function.arguments')+"),4000) END)")
        results.append("JSON_OBJECT('id',"+path(rp,'tool_call_id')+",'name',"+path(rp,'name')+",'status',"+path(rp,'result.status')+",'message',"+path(rp,'result.message')+",'returned',COALESCE("+path(rp,'result.returned')+",JSON_LENGTH("+path(rp,'result.rows')+")),'bytes',COALESCE("+path(rp,'result_bytes')+",OCTET_LENGTH("+path(rp,'result')+")),'estimated',"+path(rp,'result_bytes')+" IS NULL,'elapsed_ms',"+path(rp,'elapsed_ms')+",'activity',"+path(rp,'activity')+")")
    usage=','.join("'"+k+"',JSON_EXTRACT(m.payload_json,'$.usage."+k+"')" for k in (*TOKEN_KEYS,'elapsed_ms'))
    return """SELECT m.id,m.conversation_id,m.task_id,m.plan_id,m.round_no,m.event_type,m.created_at,
        LEFT(JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.response.reply')),20000) reply,
        CHAR_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.response.reply')))>20000 text_truncated,
        JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.error_code')) error_code,
        LEFT(JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.message')),1000) message,
        CASE WHEN m.event_type='model_response' THEN JSON_ARRAY("""+','.join(calls)+") END calls_json,"+\
        "CASE WHEN m.event_type='data_response' THEN JSON_ARRAY("+','.join(results)+") END results_json,"+\
        "JSON_OBJECT("+usage+") usage_json FROM ai_operation_messages m JOIN ai_operation_conversations c ON c.id=m.conversation_id WHERE "


def load(where,params,limit):
    """限定样本事件数量，只读投影。统计按时间窗，单次详情按原任务/方案范围。"""
    with plan_store.transaction() as cur:
        sql=projection_sql()+where+' ORDER BY m.id DESC LIMIT %s'
        # 旧版异常JSON不得让整个统计/轮次页面报错；无效旧事件当空对象，不恢复旧解析器。
        sql=sql.replace('m.payload_json',"(CASE WHEN JSON_VALID(m.payload_json) THEN m.payload_json ELSE '{}' END)")
        cur.execute(sql,tuple([*params,limit+1]));rows=list(cur.fetchall())
    more=len(rows)>limit;rows=rows[:limit]
    for row in rows:
        row['calls']=[r for r in json_value(row.pop('calls_json'),[]) if r.get('name')]
        row['results']=[r for r in json_value(row.pop('results_json'),[]) if r.get('name')]
        for result in row['results']:
            for key in ('bytes','returned','elapsed_ms'):
                if isinstance(result.get(key),str) and result[key].isdecimal():result[key]=int(result[key])
        row['usage']=json_value(row.pop('usage_json'),{})
    return sorted(rows,key=lambda r:r['id']),more


def rounds(rows):
    """按真实轮次和call_id关联请求、公开回复和查询结果，不根据数组位置猜测。"""
    grouped={}
    for row in rows:
        n=row['round_no'];current=grouped.setdefault(n,{'round':n,'created_at':row['created_at'],'tools':[],'usage':{},'text':'','has_request':False})
        kind=row['event_type']
        if kind=='model_request':current.update(has_request=True,request_id=str(row['id']))
        elif kind=='model_response':
            current.update(response_id=str(row['id']),text=row['reply'] or '',text_truncated=bool(row['text_truncated']),usage=row['usage'],error_code=row['error_code'],message=row['message'] or '')
            current['tools']=[]
            for call in row['calls']:
                args=json_value(call.get('arguments'),None) if call.get('arguments') else {}
                ref=str((args or {}).get('ref') or '') if isinstance(args,dict) else ''
                ref_type=ref.split(':')[0][:40] if ':' in ref or ref.startswith('sp') else '指定对象' if ref else ''
                current['tools'].append({'id':call['id'],'name':call['name'],'label':label(call['name']),
                    'query':query_text(call['name'],args),'window':args.get('window','all') if isinstance(args,dict) and isinstance(args.get('window','all'),str) else 'invalid',
                    'ref_type':ref_type,'status':'pending','children':[]})
        elif kind=='data_response':
            current['result_id']=str(row['id']);mapped={r['id']:r for r in row['results']}
            for tool in current['tools']:
                result=mapped.get(tool['id'])
                if not result:continue
                activity=json_value(result.get('activity'),{})
                tool.update(status=activity.get('status') or result.get('status') or ('provided' if result.get('returned') is None or result['returned']>0 else 'unavailable'),
                    message=activity.get('message') or result.get('message') or '',returned=activity.get('returned',result.get('returned')),
                    result_bytes=result.get('bytes'),bytes_estimated=bool(result.get('estimated')),elapsed_ms=result.get('elapsed_ms'),children=activity.get('children',[]))
    return sorted(grouped.values(),key=lambda r:r['round'])


def detail(session_id,message_id):
    """根据所属会话的原消息绑定读取完整轮次，防跨范围读取和聊天分页缺轮。"""
    from .conversation_queries import require_session
    session,_=require_session(session_id)
    with plan_store.transaction() as cur:
        cur.execute('SELECT task_id,plan_id FROM ai_operation_messages WHERE id=%s AND conversation_id=%s',(positive_id(message_id,'消息'),session['id']));anchor=cur.fetchone()
    if not anchor:raise DomainError('本会话没有此消息',404)
    field,value=('task_id',anchor['task_id']) if anchor['task_id'] is not None else ('plan_id',anchor['plan_id'])
    if value is None:raise DomainError('消息没有可核对的任务或方案',422)
    rows,more=load("m.conversation_id=%s AND m."+field+"=%s AND m.event_type IN ('model_request','model_response','data_response') AND JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.engine'))='native-tools-v1'",[session['id'],value],100)
    items=rounds(rows)
    return {'list':items,'model_rounds':sum(r['has_request'] for r in items),'tool_calls':sum(len(r['tools']) for r in items),'truncated':more,
            'note':'只展示模型公开回复和实际工具记录，不展示隐式思维链。查看不会调用AI；历史没有耗时记录时显示未知。'}


def statistics(params):
    """基于已有审计统计频次、组合、顺序和模型用量；有界样本，不虚分工具token或自动收费。"""
    if set(params)-{'days','shop_id'}:raise DomainError('统计参数无效',422)
    try:days=int(params.get('days',7))
    except (TypeError,ValueError):raise DomainError('请选择7/14/30/90天',422) from None
    if days not in (7,14,30,90):raise DomainError('请选择7/14/30/90天',422)
    where="m.created_at>=DATE_SUB(UTC_TIMESTAMP(3),INTERVAL %s DAY) AND m.event_type IN ('model_request','model_response','data_response') AND JSON_UNQUOTE(JSON_EXTRACT(m.payload_json,'$.engine'))='native-tools-v1'"
    values=[days]
    if params.get('shop_id'):where+=' AND c.shop_id=%s';values.append(positive_id(params['shop_id'],'店铺'))
    rows,more=load(where,values,MAX_EVENTS);by_job=defaultdict(list)
    for row in rows:by_job[('task:'+str(row['task_id'])) if row['task_id'] is not None else 'plan:'+str(row['plan_id'])].append(row)
    tools={};combinations=Counter();sequences=Counter();shapes=Counter();data_combinations=Counter();tokens={k:0 for k in TOKEN_KEYS};known=Counter();model_rounds=0
    for events in by_job.values():
        previous=None
        for item in rounds(events):
            model_rounds+=int(item['has_request'])
            for key in TOKEN_KEYS:
                value=item['usage'].get(key)
                if type(value) is int:tokens[key]+=value;known[key]+=1
            names=tuple(sorted({r['name'] for r in item['tools']}))
            if names:
                combinations[names]+=1
                if previous:sequences[(previous,names)]+=1
                previous=names
            round_shapes=[]
            for call in item['tools']:
                record=tools.setdefault(call['name'],{'name':call['name'],'label':call['label'],'calls':0,'statuses':Counter(),'result_bytes':0,'estimated_bytes_calls':0,'elapsed_ms':0,'timed_calls':0})
                record['calls']+=1;record['statuses'][call['status']]+=1
                if type(call.get('result_bytes')) is int:record['result_bytes']+=call['result_bytes'];record['estimated_bytes_calls']+=int(call['bytes_estimated'])
                if type(call.get('elapsed_ms')) is int:record['elapsed_ms']+=call['elapsed_ms'];record['timed_calls']+=1
                queries=call['children'] or [call]
                for child in queries:
                    if child['name'] not in (*READ_TOOLS,'get_asin_overview'):continue
                    key=(child['name'],child.get('window','all'),child.get('ref_type',''))
                    shapes[key]+=1;round_shapes.append(key)
            if round_shapes:data_combinations[tuple(sorted(set(round_shapes)))]+=1
    return {'days':days,'jobs':len(by_job),'model_rounds':model_rounds,'tool_calls':sum(r['calls'] for r in tools.values()),
            'tokens':{k:tokens[k] if known[k] else None for k in TOKEN_KEYS},'token_coverage':dict(known),
            'tools':sorted(tools.values(),key=lambda r:(-r['calls'],r['name'])),
            'combinations':[{'tools':list(k),'label':' + '.join(label(n) for n in k),'count':v} for k,v in combinations.most_common(20)],
            'sequences':[{'label':' + '.join(label(n) for n in k[0])+' → '+' + '.join(label(n) for n in k[1]),'count':v} for k,v in sequences.most_common(20)],
            'data_combinations':[{'label':' + '.join(label(q[0])+'('+WINDOW_LABELS.get(q[1],q[1])+(' / '+REF_LABELS.get(q[2],q[2]) if q[2] else '')+')' for q in k),'count':v} for k,v in data_combinations.most_common(20)],
            'queries':[{'label':label(k[0])+' · '+WINDOW_LABELS.get(k[1],k[1])+(' · '+REF_LABELS.get(k[2],k[2]) if k[2] else ''),'count':v} for k,v in shapes.most_common(30)],
            'sample_events':len(rows),'truncated':more,'earliest_at':rows[0]['created_at'] if rows else None,
            'note':'UTC滚动时间窗；最多统计最近1500条原生事件，截断或时间边界可能缺轮。工具数据量不等于token，模型token为已记录分析回执、不含独立历史摘要，不分摊到工具或冒充完整账单；历史数据量为估算，未记录耗时不补0。频次/组合供人工研究，不自动请求AI或新增工具。'}
