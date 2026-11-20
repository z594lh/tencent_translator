"""综合概览与定向批量只读取数；保持源口径、底层开关和已实际返回的证据边界。"""
from ..language import text as model_text

from copy import deepcopy
import time
from services.listing_performance.common import DomainError
from . import data,readers
from .specs import READ_TOOLS

MAX_BYTES=24000


def marks(ctx):
    """保存查询前已读状态，预算拒绝或精简时不误认整份资料已交给模型。"""
    return set(ctx.settings),set(ctx.evidence),set(ctx.observations),ctx.observe_checked


def restore(ctx,state):
    """恢复未实际返回的查询标记；不回滚或修改任何业务表。"""
    ctx.settings,ctx.evidence,ctx.observations,ctx.observe_checked=state


def compact(ctx,name,result):
    """概览只精简明确标注的模块；完整文案/逐日明细仍由原工具读取。"""
    result=deepcopy(result)
    if name in ('read_listing','read_pricing'):
        for row in result.get('rows',[]):
            kind='listing' if name=='read_listing' else 'pricing'
            ref=next((ref for ref,entry in ctx.data['evidence'].items() if entry['kind']==kind and entry['data'].get('sku')==row.get('sku')),None)
            if ref:row['evidence_id']=ref
            if name=='read_listing':
                fields=row.pop('fields',{})
                row['key_fields']={k:v for k,v in fields.items() if k in ('item_name','title_differentiation')}
                row['other_field_counts']={k:len(v) for k,v in fields.items() if k not in row['key_fields']}
                row.pop('content_policy',None);row.pop('pricing_attributes',None)
                row['detail_note']=model_text('bundles.643397f7694c')
            else:
                row['target_price_examples_omitted']=len(row.pop('target_price_examples',[]))
                original=row.get('scenarios',[]);row['scenarios']=original[:2]
                row['scenarios_omitted']=max(0,len(original)-2)
                row['detail_note']=model_text('bundles.6183f321056e')
    if name=='read_observations':
        for row in result.get('rows',[]):row.pop('baseline',None)
        result['detail_note']=model_text('bundles.c5c5459ddc37')
    result.pop('data_hash',None)
    return result


def query(ctx,snapshot,request,remaining,summary=False):
    """单项预算与底层开关校验；只有成功包含的结果能留下已读标记。"""
    name=request['tool'];args={k:v for k,v in request.items() if k!='tool'}
    if name not in READ_TOOLS:raise DomainError(model_text('bundles.ab60f1668f3a'),422)
    if name not in snapshot['names']:return {'tool':name,'query':args,'result':{'status':'disabled','message':model_text('bundles.afe70dabdefc')}}
    state=marks(ctx);started=time.monotonic()
    try:
        original_budget=getattr(ctx,'output_budget',data.MAX_DATA_BYTES)
        ctx.output_budget=min(original_budget,max(0,remaining-1500))
        try:result=readers.read(ctx,name,{'limit':3 if summary else 1000,**args})
        finally:ctx.output_budget=original_budget
        if summary:
            result=compact(ctx,name,result);restore(ctx,state);ctx.capture(result)
            if name=='read_observations':
                ctx.observations.update(str(r['execution_id']) for r in result.get('rows',[]));ctx.observe_checked=True
        entry={'tool':name,'query':args,'result':result,'elapsed_ms':round((time.monotonic()-started)*1000)}
        if data.size(entry)>remaining:
            restore(ctx,state)
            entry['result']={'status':'budget_exceeded','message':model_text('bundles.c2b32eabb76b'),'returned':0}
        return entry
    except DomainError as error:
        restore(ctx,state);return {'tool':name,'query':args,'result':{'status':'error','message':str(error)}}
    except Exception:
        restore(ctx,state);raise


def batch(ctx,snapshot,args):
    """最多四项独立只读查询，失败不覆盖已成功项，不接受任意接口/跨账户参数。"""
    if set(args)!={'queries'} or not isinstance(args['queries'],list) or not 1<=len(args['queries'])<=4:raise DomainError(model_text('bundles.1d28327f6fd5'),422)
    for request in args['queries']:
        if not isinstance(request,dict) or request.get('tool') not in READ_TOOLS:raise DomainError(model_text('bundles.40880b4656bd'),422)
        readers.page({k:v for k,v in request.items() if k!='tool'})
    result={'status':'provided','queries':[],'note':model_text('bundles.8835c155dade')}
    budget=min(data.MAX_DATA_BYTES,getattr(ctx,'output_budget',data.MAX_DATA_BYTES))
    for request in args['queries']:result['queries'].append(query(ctx,snapshot,request,budget-data.size(result)))
    return result


def overview(ctx,snapshot,args):
    """保持全部统计窗口、来源与缺失信息的紧凑运营概览，不默认发送大量逐日/逐词明细。"""
    if args:raise DomainError(model_text('bundles.d9f80c49a295'),422)
    content=ctx.data['content']
    result={'status':'provided','identity':content['identity'],'time':content['time'],'inventory':content.get('inventory'),
            'gaps':content.get('gaps'),'sections':[],'note':model_text('bundles.1f63e3323e91')}
    if 'read_advertising' in snapshot['names']:
        result['advertising']=[{k:v for k,v in row.items() if k!='details'} for row in content['ads']]
        result['advertising_note']=model_text('bundles.53227506545e')
    else:result['advertising']={'status':'disabled','message':model_text('bundles.f42f4d50e48f')}
    if data.size(result)>MAX_BYTES:
        # 多SKU/异常来源过大时保留明确入口，不截断指标或把漏掉的维度冒充已提供。
        result.pop('advertising',None);result['advertising_note']=model_text('bundles.36476647b3df')
    for name,limit in (('read_business',3),('read_pricing',3),('read_listing',3),('read_keywords',5),('read_observations',5)):
        section=query(ctx,snapshot,{'tool':name,'limit':limit},MAX_BYTES-data.size(result),summary=True)
        if name=='read_keywords':
            section.update(snapshot=content['keywords']['snapshot'],total_keywords=content['keywords']['total'])
            if section['result'].get('status')=='provided' and 'read_keyword_history' in snapshot['names']:
                words={r['keyword'] for r in section['result']['rows']};history=[]
                for version in content['keywords'].get('history',[])[:2]:
                    columns=version['columns'];index=columns.index('keyword')
                    history.append({'snapshot':version['snapshot'],'columns':columns,'rows':[r for r in version['rows'] if r[index] in words]})
                if data.size(result)+data.size(section)+data.size(history)<MAX_BYTES:
                    section['recent_history']=history;section['history_note']=model_text('bundles.09145bf51a84')
        result['sections'].append(section)
    result['detail_tools']=['read_data_bundle','read_current_settings','read_observations']
    if 'read_competitors' in snapshot['names']:
        result['detail_tools'].append('read_competitors')
        result['competitor_bound_count']=content.get('competitors',{}).get('bound_count')
    return result
