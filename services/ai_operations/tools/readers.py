"""查询与售价测算工具；数据来自当前任务冻结的本地业务来源，不直接调用Amazon。"""
from decimal import Decimal,InvalidOperation
from copy import deepcopy
from services.listing_performance.common import DomainError
from services import profit_calculator
from .. import plan_store
from . import data

SOURCES={'read_listing':'listing','read_business':'business','read_advertising':'advertising','read_keywords':'keywords',
         'read_keyword_history':'keyword_history','read_pricing':'pricing','read_current_settings':'current_settings',
         'read_operations':'erp_operations','read_plan_history':'history'}


def page(args):
    """限制分页/日期参数，防止任意SQL、URL、超大读取及布尔冒充整数。"""
    if set(args)-{'ref','window','offset','limit'}:raise DomainError('查询参数不在工具白名单',422)
    value={'ref':'','window':'all','offset':0,'limit':1000,**args}
    if not isinstance(value['ref'],str) or len(value['ref'])>200 or not isinstance(value['window'],str) or value['window'] not in data.WINDOWS or type(value['offset']) is not int or not 0<=value['offset']<=10000 or type(value['limit']) is not int or not 1<=value['limit']<=1000:raise DomainError('查询对象、时间窗口或分页无效（limit为1～1000）',422)
    return value


def read(ctx,name,args):
    """按工具名称查询当前账户ASIN；历史问答只读自然表，不读附件/工具事件。"""
    if name=='get_asin_overview':
        if args:raise DomainError('商品概况不接受切店或其他参数',422)
        content=ctx.data['content']
        return {'identity':content['identity'],'time':content['time'],'products':[{k:r.get(k) for k in ('sku','asin','product_name','product_type')} for r in ctx.pack['products']],
                'inventory':content.get('inventory'),'data_gaps':content.get('gaps'),
                'note':'经营/广告按已入库report_date查询；当前设置为本地缓存，执行前另做平台实时前值核对'}
    query=page(args)
    if name=='read_competitors':
        from . import competitors
        return competitors.read(ctx,query)
    if name=='read_chat_history':
        from .. import natural_history
        return natural_history.search(ctx.session['id'],query['ref'],query['offset'],query['limit'],max_bytes=getattr(ctx,'output_budget',data.MAX_DATA_BYTES))
    if name=='read_observations':
        management=ctx.data['content'].get('management_context',{})
        rows=[{'type':'observation',**data.watch(r)} for r in management.get('observations',[])]+[{'type':'pending',**data.watch(r)} for r in management.get('pending_executions',[])]
        for row in rows:
            # 观察首屏给基线窗口与真实改后指标；大量逐词基线按原执行引用另查，不反复塞整表。
            if isinstance(row.get('baseline'),dict):row['baseline']={k:v for k,v in row['baseline'].items() if k!='keywords'}
            if isinstance(row.get('post_evidence'),dict):row['post_evidence']={k:v for k,v in row['post_evidence'].items() if k not in ('items','business')}
        if query['ref']:rows=[r for r in rows if query['ref'] in (str(r['execution_id']),r['ref'])]
        selected=rows[query['offset']:query['offset']+query['limit']]
        budget=getattr(ctx,'output_budget',data.MAX_DATA_BYTES)
        selected=data.fit(selected,max(0,budget-3000))
        ctx.observations.update(str(r['execution_id']) for r in selected)
        result={'rows':selected,'total':len(rows),'returned':len(selected),'requested_limit':query['limit'],'offset':query['offset'],
                'has_more':query['offset']+len(selected)<len(rows),'byte_limited':len(selected)<len(rows[query['offset']:query['offset']+query['limit']]),
                'next_offset':query['offset']+len(selected) if selected and query['offset']+len(selected)<len(rows) else None,
                'execution_facts':management.get('execution_facts',[])[:10],
                'note':'真实执行账本优先；未发送、拒绝、未知都不是成功。观察未满期不能当完整改后效果。'}
        if data.size(result)>budget:result.pop('execution_facts')
        result['status']=('partial' if result['has_more'] else 'provided') if selected else ('budget_exceeded' if rows[query['offset']:query['offset']+query['limit']] else 'empty')
        if result['status']=='budget_exceeded':result['reason']='单个完整观察超过本次返回预算，正文未交付'
        ctx.observe_checked=True
        ctx.capture(result)
        return result
    source=SOURCES.get(name)
    if not source:raise DomainError('工具没有对应只读适配器',422)
    result=data.fetch(ctx.data,{'source':source,**query},session=ctx.session,max_bytes=getattr(ctx,'output_budget',data.MAX_DATA_BYTES))
    if name=='read_current_settings':ctx.settings.update(r['ref'] for r in result.get('rows',[]))
    ctx.capture(result)
    return result


def simulate_price(ctx,args):
    """显式假设费率，复用ERP售价公式；禁止跨SKU测算或用假设值修改实际售价。"""
    if set(args)!={'sku','selling_price','ad_rate','refund_rate'} or not isinstance(args['sku'],str):raise DomainError('售价测算参数无效',422)
    source=next((r for r in ctx.data['content']['pricing'] if r['sku']==args['sku']),None)
    if not source or source.get('currency')!='USD':raise DomainError('SKU不在当前ASIN或售价币种不支持',422)
    try:
        values={k:Decimal(args[k]) for k in ('selling_price','ad_rate','refund_rate') if isinstance(args[k],str)}
        if len(values)!=3 or not all(v.is_finite() for v in values.values()) or not Decimal('.01')<=values['selling_price']<=Decimal('100000') or any(not 0<=values[k]<1 for k in ('ad_rate','refund_rate')):raise ValueError()
        costs=source['unit_costs']
        if costs['fixed_total'] is None or costs['commission_rate'] is None:raise ValueError()
        result=profit_calculator.calculate_profit_rate(Decimal(str(costs['fixed_total'])),values['selling_price'],Decimal(str(costs['commission_rate'])),values['ad_rate'],values['refund_rate'])
    except (InvalidOperation,ValueError,TypeError):raise DomainError('售价/费率或成本资料无效，不能把缺失成本当0',422) from None
    return {'sku':args['sku'],'currency':'USD','assumptions':values,'result':result,'note':'公式测算，不是销量预测或实际历史利润；没有修改售价'}
