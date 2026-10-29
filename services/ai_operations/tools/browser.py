"""工具数据浏览：隔离只读试查与真正发给AI的历史回包，不创建任务或执行计划。"""
import json
from datetime import datetime, timedelta, timezone
from services.listing_performance.common import DomainError, positive_id
from .. import plan_store, conversation_queries, conversation_store, protocol
from ..metrics import digest, dumps
from . import registry
from .specs import definitions


def allowed(name):
    """仅允许注册的查询/测算工具试查，提交方案永远不能由管理页执行。"""
    definition=definitions().get(name)
    if not definition:raise DomainError('工具不在代码注册目录',404)
    if definition['required']:raise DomainError('方案提交不是查询，不能在工具浏览器试运行',422)
    return definition


def boundary(identifier,product=False):
    """按实时托管账户建立只读边界，不能靠输入ASIN切换广告账户。"""
    from services.ai_workspace import products
    if product:target=products.get(identifier)
    else:
        managed,identity=conversation_queries.managed_identity(identifier);target=products.resolve(managed['shop_id'],managed['asin'])
    return target,target['identity'],target['scope_key']


def sources(name,params):
    """返回可复查冻结任务与近期实际调用元信息，不读取大请求正文。"""
    allowed(name)
    if set(params) not in ({'managed_id'},{'product_id'}):raise DomainError('请选择商品',422)
    managed,identity,key=boundary(params.get('product_id',params.get('managed_id')),'product_id' in params);archives=[]
    safe="(CASE WHEN JSON_VALID(m.payload_json) THEN m.payload_json ELSE '{}' END)"
    fields=[]
    for index in range(8):
        prefix="'$.tool_results["+str(index)+"]"
        fields.append("JSON_OBJECT('id',JSON_EXTRACT("+safe+","+prefix+".tool_call_id'),'name',JSON_EXTRACT("+safe+","+prefix+".name'))")
    with plan_store.transaction() as cur:
        cur.execute('''SELECT id,report_date,created_at FROM ai_operation_plans WHERE shop_id=%s AND asin=%s AND marketplace_id=%s
            AND JSON_UNQUOTE(JSON_EXTRACT(CASE WHEN JSON_VALID(prompt_json) THEN prompt_json ELSE '{}' END,'$.conversation.protocol'))=%s
            ORDER BY id DESC LIMIT 20''',(managed['shop_id'],managed['asin'],managed['marketplace_id'],protocol.VERSION))
        archives.extend({'kind':'plan','id':str(r['id']),'report_date':r['report_date'],'created_at':r['created_at']} for r in cur.fetchall())
        cur.execute('''SELECT t.id,t.report_date,t.created_at FROM ai_operation_tasks t
            JOIN ai_operation_conversations c ON c.id=t.conversation_id
            WHERE c.scope_key=%s AND t.input_json IS NOT NULL AND t.plan_id IS NULL
              AND JSON_VALID(t.input_json) AND JSON_UNQUOTE(JSON_EXTRACT(t.input_json,'$[2].conversation.protocol'))=%s
            ORDER BY t.id DESC LIMIT 20''',(key,protocol.VERSION))
        archives.extend({'kind':'task','id':str(r['id']),'report_date':r['report_date'],'created_at':r['created_at']} for r in cur.fetchall())
        cur.execute('SELECT m.id,m.round_no,m.created_at,JSON_ARRAY('+','.join(fields)+''') calls_json
            FROM ai_operation_messages m JOIN ai_operation_conversations c ON c.id=m.conversation_id
            WHERE c.scope_key=%s AND m.event_type='data_response'
              AND JSON_UNQUOTE(JSON_EXTRACT('''+safe+''','$.engine'))=%s
              AND JSON_SEARCH('''+safe+''','one',%s,NULL,'$.tool_results[*].name') IS NOT NULL
            ORDER BY m.id DESC LIMIT 20''',(key,protocol.VERSION,name))
        events=list(cur.fetchall())
    calls=[]
    for event in events:
        for call in json.loads(event['calls_json']):
            if call.get('name')==name and call.get('id'):
                calls.append({'message_id':str(event['id']),'call_id':call['id'],'round':event['round_no'],'created_at':event['created_at']})
    archives.sort(key=lambda r:(str(r['created_at']),r['kind'],int(r['id'])),reverse=True)
    from .. import daily_runner
    now=daily_runner.clock()
    end=min(now.astimezone(daily_runner.BJ).date()-timedelta(days=1),now.astimezone(daily_runner.SOURCE_ZONE).date()-timedelta(days=1))
    return {'archives':archives[:20],'calls':calls,'identity':identity,'suggested_report_end':end,
            'note':'冻结资料试查不代表已发给AI；实际调用原文保持当时结果，不使用今天的数据补写历史。'}


def frozen(body,key):
    """校验冻结输入hash及账户边界，旧协议或损坏输入明确不可浏览。"""
    record_id=positive_id(body.get('archive_id'),'资料编号');kind=body.get('archive_kind')
    with plan_store.transaction() as cur:
        if kind=='plan':
            cur.execute('SELECT facts_json,dataset_json,prompt_json,input_hash FROM ai_operation_plans WHERE id=%s',(record_id,));row=cur.fetchone()
            if not row or not protocol.native_prompt(row['prompt_json']):raise DomainError('旧数据或资料不存在，无法查看',410)
            values=[protocol.object_value(row[k]) for k in ('facts_json','dataset_json','prompt_json')]
        elif kind=='task':
            cur.execute('SELECT input_json,input_hash FROM ai_operation_tasks WHERE id=%s',(record_id,));row=cur.fetchone()
            values=protocol.object_value(row['input_json']) if row else None
        else:raise DomainError('请选择有效冻结资料',422)
    if not row or not isinstance(values,list) or len(values)!=3 or not all(isinstance(r,dict) for r in values) or digest(values)!=row['input_hash']:
        raise DomainError('冻结资料完整性校验失败，不能试查',409)
    pack,data,prepared=values
    if not protocol.native_prompt(prepared) or conversation_store.scope(data['content']['identity'])[1]!=key:
        raise DomainError('冻结资料与当前账户/ASIN不一致，不能跨账户读取',409)
    sid=prepared['conversation'].get('id')
    session={'id':sid}
    if sid:
        checked,_=conversation_queries.require_session(sid)
        if checked['scope_key']!=key:raise DomainError('资料会话范围不一致',409)
        session=checked
    return pack,data,session


def actual(name,body,key):
    """读取原事件中指定call_id的真实返回，保留参数、时间和原始hash，不重新查询。"""
    with plan_store.transaction() as cur:
        cur.execute('''SELECT m.* FROM ai_operation_messages m JOIN ai_operation_conversations c ON c.id=m.conversation_id
            WHERE m.id=%s AND m.event_type='data_response' AND c.scope_key=%s''',
            (positive_id(body.get('message_id'),'调用消息编号'),key));row=cur.fetchone()
    if not row:raise DomainError('该ASIN不存在此调用记录',404)
    payload=protocol.object_value(row['payload_json'])
    if not isinstance(payload,dict) or payload.get('engine')!=protocol.VERSION or digest(payload)!=row['payload_hash']:
        raise DomainError('调用记录不完整或不是当前协议，无法查看',410)
    for call in payload.get('tool_results',[]):
        if call.get('name')==name and call.get('tool_call_id')==body.get('call_id'):
            return {'mode':'actual','result':call.get('result'),'arguments':protocol.object_value(call.get('arguments')),
                    'created_at':row['created_at'],'message_id':str(row['id']),'round':row['round_no'],
                    'payload_hash':row['payload_hash'],'result_bytes':len(dumps(call.get('result')).encode('utf8')),
                    'note':'这是当时ERP实际交给AI的工具返回；查看不调用AI，不修改历史。'}
    raise DomainError('该消息没有此工具调用',404)


def preview(name,body):
    """显式点击后只读试查，或查看历史原回包；绝无模型请求和Amazon写入口。"""
    allowed(name)
    if not isinstance(body,dict):raise DomainError('查询参数必须是对象',422)
    mode=body.get('mode');base={'product_id' if 'product_id' in body else 'managed_id','mode'}
    keys={'actual':{'message_id','call_id'},'snapshot':{'archive_id','archive_kind','arguments'},'current':{'report_date','arguments'}}
    if not isinstance(mode,str) or mode not in keys or set(body)!=base|keys[mode]:raise DomainError('请选择有效数据来源和查询参数',422)
    managed,identity,key=boundary(body.get('product_id',body.get('managed_id')),'product_id' in body)
    if mode=='actual':return actual(name,body,key)
    if not isinstance(body['arguments'],dict):raise DomainError('工具参数必须是JSON对象',422)
    if mode=='snapshot':pack,data,session=frozen(body,key)
    else:
        from .. import plans,daily_runner
        from services.listing_performance.common import parse_date
        end=parse_date(body['report_date'])
        if end>=datetime.now(timezone.utc).date():raise DomainError('请选择已结束的报表日期',422)
        pack,data=plans.build_data(managed['shop_id'],managed['asin'],str(end),
            {'managed_id':managed['id'],'run_id':0,'run_date':datetime.now(daily_runner.BJ).date(),'now':daily_runner.clock()})
        if conversation_store.scope(data['content']['identity'])[1]!=key:raise DomainError('读取期间账户已变化，请刷新',409)
        session=conversation_store.peek_session(data['content']['identity'])
    if name=='read_chat_history' and not session.get('id'):
        result={'rows':[],'total':0,'next_offset':None,'note':'尚无当前会话，不为预览创建会话。'}
    else:
        from ..conversation import ToolContext
        ctx=ToolContext(data,session,'preview');ctx.pack=pack
        # 试查可以检查已关闭工具，但组合查询继续尊重其他工具开关；不保存读取证据。
        snapshot=registry.snapshot(allow_plan=False)
        if name not in snapshot['names']:snapshot['names'].append(name)
        result=registry.dispatch(ctx,snapshot,name,body['arguments'])
    return {'mode':mode,'result':result,'arguments':body['arguments'],'identity':identity,
            'report_end':data['content']['time']['report_end'],'created_at':datetime.now(timezone.utc),
            'result_bytes':len(dumps(result).encode('utf8')),
            'note':'只读查询预览，不代表AI已经读取；不收费、不创建任务/会话、不修改Amazon。资料/成本均来自ERP本地。'}
