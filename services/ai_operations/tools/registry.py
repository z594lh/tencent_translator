"""工具目录管理、不可变任务快照和白名单分发；页面不能新增函数/URL/SQL。"""
from copy import deepcopy
import pymysql
from services.listing_performance.common import DomainError,positive_id
from .. import plan_store
from ..metrics import digest
from .specs import definitions


def check_schema(cur):
    """只读预检工具、自然问答和摘要结构；不注册任务或调用模型。"""
    for table,fields in (('ai_operation_tools','tool_name,enabled,description,revision'),
            ('ai_operation_chat_turns','id,conversation_id,source_key,question,answer,content_hash'),
            ('ai_operation_chat_summaries','id,request_key,source_hash,status,summary_text,covered_turn_id'),
            ('ai_operation_prompt_profiles','id,revision,published_version_id,draft_json'),
            ('ai_operation_prompt_versions','id,config_json,config_hash,actor_id,created_at')):
        cur.execute('SELECT '+fields+' FROM '+table+' WHERE 1=0')


def listing():
    """合并代码定义与人工说明/开关；GET不创建配置行、不调用模型。"""
    rows=definitions()
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_tools');saved={r['tool_name']:r for r in cur.fetchall()}
    result=[]
    for name,base in rows.items():
        row=saved.get(name,{})
        result.append({**base,'enabled':True if base['required'] else bool(row.get('enabled',True)),
                       'description':row.get('description',base['description']),'default_description':base['description'],
                       'revision':row.get('revision',0),'updated_at':row.get('updated_at'),'updated_by':row.get('updated_by')})
    return {'list':result,'note':'只管理代码注册工具的可见性和说明。新任务固定一份快照；关闭工具不撤回已执行操作，不影响在途任务。'}


def update(name,body,actor):
    """CAS保存工具说明/启停或重置；不能通过页面修改参数Schema或执行任意接口。"""
    base=definitions().get(name)
    if not base:raise DomainError('工具不在代码注册目录',404)
    if not isinstance(body,dict) or set(body)!={'revision','enabled','description','reset'} or type(body['revision']) is not int or body['revision']<0 or type(body['enabled']) is not bool or type(body['reset']) is not bool:
        raise DomainError('工具编辑参数无效')
    description=base['description'] if body['reset'] else body['description'];enabled=True if body['reset'] else body['enabled']
    if not isinstance(description,str) or not description.strip() or len(description)>2000 or any(ord(c)<32 and c not in '\n\t' for c in description):raise DomainError('工具说明不能为空、超过2000字符或包含控制字符')
    if base['required'] and not enabled:raise DomainError('提交计划是必要能力，不能关闭',422)
    try:
        with plan_store.transaction(True) as cur:
            cur.execute('SELECT revision FROM ai_operation_tools WHERE tool_name=%s FOR UPDATE',(name,));old=cur.fetchone()
            if (old['revision'] if old else 0)!=body['revision']:raise DomainError('工具已被其他用户修改，请刷新',409)
            if old:cur.execute('UPDATE ai_operation_tools SET enabled=%s,description=%s,revision=revision+1,updated_by=%s,updated_at=UTC_TIMESTAMP(3) WHERE tool_name=%s',(enabled,description,positive_id(actor,'用户'),name))
            else:cur.execute('INSERT INTO ai_operation_tools(tool_name,enabled,description,revision,updated_by,updated_at) VALUES(%s,%s,%s,1,%s,UTC_TIMESTAMP(3))',(name,enabled,description,positive_id(actor,'用户')))
    except pymysql.IntegrityError:raise DomainError('工具配置出现并发修改，请刷新',409) from None
    return listing()


def snapshot(allow_plan=True):
    """冻结本任务可调用工具及参数；可选工具关闭后不发送，也不允许模型强行调用。"""
    rows=[r for r in listing()['list'] if r['enabled'] and (allow_plan or not r['required'])]
    tools=[{'type':'function','function':{'name':r['name'],'description':r['description'],'parameters':deepcopy(r['parameters'])}} for r in rows]
    return {'tools':tools,'hash':digest(tools),'names':[r['name'] for r in rows]}


def dispatch(context,snapshot,name,args):
    """只分发到固定业务函数，运行时再次核对任务白名单和对象范围。"""
    if name not in snapshot['names']:raise DomainError('此工具未对本任务开放',422)
    if not isinstance(args,dict):raise DomainError('工具参数必须是JSON对象',422)
    from . import readers, planning, bundles
    if name=='get_operation_overview':return bundles.overview(context,snapshot,args)
    if name=='read_data_bundle':return bundles.batch(context,snapshot,args)
    if name=='submit_optimization_plan':return planning.submit(context,args)
    if name=='simulate_price':return readers.simulate_price(context,args)
    return readers.read(context,name,args)
