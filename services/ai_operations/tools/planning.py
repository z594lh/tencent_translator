"""结构化计划工具：只校验计划，不直接执行；复用原Listing/金额/观察约束。"""
from copy import deepcopy
from services.listing_performance.common import DomainError
from .. import plan_contract
from ..metrics import digest


def review_scope(ctx,args):
    """人工保留行动相关或显式复盘的已知历史；不把合法额外复盘裁成未知。"""
    management=deepcopy(ctx.data['content'].get('management_context',{}))
    actions=args.get('actions') if isinstance(args.get('actions'),list) else []
    reviews=args.get('history_reviews') if isinstance(args.get('history_reviews'),list) else []
    refs={r.get('ref') for r in actions if isinstance(r,dict) and isinstance(r.get('ref'),str)}
    mentioned={r.get('execution_id') for r in reviews if isinstance(r,dict) and isinstance(r.get('execution_id'),str)}
    if ctx.purpose!='daily':
        for field in ('observations','pending_executions'):
            management[field]=[r for r in management.get(field,[]) if r['ref'] in refs or str(r['execution_id']) in mentioned]
    return management


def repair_hint(ctx,args):
    """返回已实际读取的精确编号和引用供纠错，不替AI改值、不放松契约或暴露未读资料。"""
    args=args if isinstance(args,dict) else {}
    management=ctx.data['content'].get('management_context',{})
    known={str(r['execution_id']):r for field in ('observations','pending_executions') for r in management.get(field,[])}
    scoped=review_scope(ctx,args)
    issues=[];related=set()
    for field in ('actions','history_reviews'):
        for index,row in enumerate(args.get(field,[]) if isinstance(args.get(field),list) else []):
            if not isinstance(row,dict):continue
            candidates=set()
            if field=='history_reviews':
                ref='history:execution:'+str(row.get('execution_id',''))
                if ref in ctx.evidence:candidates.add(ref)
                record=known.get(str(row.get('execution_id')),{});candidates.update(record.get('evidence_refs',[]))
                candidates.update(ctx.data.get('catalog',{}).get(record.get('ref'),{}).get('evidence_refs',[]))
                object_ref=record.get('ref','')
                prefix='spTargeting:' if object_ref.startswith(('target:','keyword:')) else 'spCampaignsPlacement:' if 'PLACEMENT_' in object_ref else None
                subject=ctx.data.get('catalog',{}).get(object_ref,{}).get('subject',{})
                if prefix:
                    for ref in ctx.evidence:
                        if not ref.startswith(prefix):continue
                        dimensions=ctx.data['evidence'].get(ref,{}).get('data',{}).get('dimensions',{})
                        if subject.get('entity_id') and str(dimensions.get('keyword_id'))!=str(subject['entity_id']):continue
                        candidates.add(ref)
            else:
                action_ref=row.get('ref')
                if isinstance(action_ref,str):candidates.update(ctx.data.get('catalog',{}).get(action_ref,{}).get('evidence_refs',[]))
            candidates&=ctx.evidence;related.update(candidates)
            if field=='actions':
                from .specs import OBSERVATION
                observation=row.get('observation')
                if isinstance(observation,dict):
                    expected=set(OBSERVATION['required']);actual=set(observation)
                    if expected!=actual:issues.append({'path':f'actions[{index}].observation','missing_fields':sorted(expected-actual),'unknown_fields':sorted(actual-expected)})
            refs=row.get('evidence_refs')
            if isinstance(refs,list):
                for j,ref in enumerate(refs):
                    if not isinstance(ref,str) or ref not in ctx.evidence:
                        issues.append({'path':f'{field}[{index}].evidence_refs[{j}]','invalid_ref':ref,'related_read_refs':sorted(candidates)})
    return {'invalid_evidence':issues,'required_execution_ids':sorted({str(r['execution_id']) for field in ('observations','pending_executions') for r in scoped.get(field,[])}),
            'read_history_items':[{'execution_id':eid,'evidence_ref':'history:execution:'+eid} for eid in sorted(ctx.observations) if eid in known],
            'related_read_evidence_refs':sorted(related)}


def submit(ctx,args):
    """要求行动/证据来自实际工具查询；完整计划校验通过后交给原归档和执行链路。"""
    if ctx.plan is not None:
        if digest(args)==ctx.plan_input_hash:return {'status':'validated','message':'复用本任务相同计划，尚未执行'}
        raise DomainError('本任务已校验一份不同计划，请结束回复后另发新问题调整，未覆盖原计划',422)
    plan_contract.fields(args,('summary','actions','notes','history_reviews'),'工具方案')
    if not isinstance(args['actions'],list) or len(args['actions'])>12:raise DomainError('行动列表无效或超过12项',422)
    refs={r.get('ref') for r in args['actions'] if isinstance(r,dict) and isinstance(r.get('ref'),str)}
    if refs-ctx.settings:raise DomainError('请先用read_current_settings查询方案涉及的每个精确对象及完整前值',422)
    management=review_scope(ctx,args)
    known={str(r['execution_id']) for key in ('observations','pending_executions') for r in management.get(key,[])}
    if (ctx.purpose=='daily' and not ctx.observe_checked) or known-ctx.observations:raise DomainError('请先分页查询read_observations，覆盖本次计划相关的全部待观察/待核实项',422)
    provided={**ctx.data,'content':{**ctx.data['content'],'management_context':management}}
    validated=plan_contract.validate(args,provided)
    all_refs={ref for row in validated['actions'] for ref in row['evidence_refs']}
    all_refs.update(ref for review in validated['history_reviews'] for ref in review['evidence_refs'])
    if all_refs-ctx.evidence:raise DomainError('方案包含未通过本次工具实际读取的证据，请先查询对应资料',422)
    ctx.plan=validated;ctx.plan_input_hash=digest(args)
    return {'status':'validated','action_count':len(validated['actions']),'message':'计划已校验并形成分析成果；人工需明确确认，每日按托管流程执行。此刻尚未修改Amazon。'}
