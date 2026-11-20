"""连续托管记忆：历史结论、执行快照与观察周期；不调用模型，不生成虚构效果。"""
from .language import text as model_text

from datetime import date, datetime, timedelta, timezone
import json
from . import plan_store, execution_store, platform_operations as interfaces
from .metrics import digest, dumps, ADS_FIELDS, SKU_FIELDS, totals, ratios
from .observation_status import review_due
from services.listing_performance.common import DomainError

BJ=timezone(timedelta(hours=8))
SOURCE_ZONE=timezone(timedelta(hours=-7))


def same_account(before,now):
    """核对历史与本次资料的店铺账户边界，避免跨凭证或站点继承状态。"""
    return all(str(before.get(k) or '')==str(now.get(k) or '')
               for k in ('id','credential_group_id','region','marketplace_id','ads_profile_id'))


def day(value):
    """统一解析报表业务日期，不引入当前机器时区。"""
    return value if type(value) is date else date.fromisoformat(str(value)[:10])


def build(pack,data,management):
    """恢复正式会话与观察事实；不以旧执行值覆盖当前资料。"""
    identity=data['content']['identity'];today=day(management['run_date'])
    refs=list(data['catalog']);keys=[interfaces.resource_key(identity,ref) for ref in refs]
    resource_keys=keys
    context={'run_id':str(management['run_id']),'run_date':today,'runtime_mode':interfaces.MODE,
             'recent_runs':[],'observations':[],'pending_executions':[],'execution_facts':[],'unstarted_actions':[],
             'notes':[model_text('management_context.49e51c525166'),
                      model_text('management_context.2f4d1ffb3d31')]}
    from . import confirmations,conversation_store
    context['human_decisions']=confirmations.context(management['managed_id'],conversation_store.scope(identity)[1])
    with plan_store.transaction() as cur:
        cur.execute('''SELECT p.*,r.id managed_run_id,r.run_date management_date,r.status daily_status
            FROM ai_operation_daily_runs r JOIN ai_operation_plans p ON p.id=r.plan_id
            WHERE r.managed_id=%s AND r.id<>%s AND r.runtime_mode='conv_real' ORDER BY r.run_date DESC,r.id DESC LIMIT 8''',
            (management['managed_id'],management['run_id']))
        previous=cur.fetchall()
        context['history_limit']=7;context['older_history_omitted']=len(previous)>7
        for row in previous[:7]:
            plan=plan_store.serialize(row,True,include_events=False)
            if plan.get('legacy'):continue
            if not same_account(plan['facts']['identity'],identity):continue
            cur.execute('SELECT action_id,status,verified_at FROM ai_operation_executions WHERE plan_id=%s ORDER BY id',(plan['id'],))
            completed=cur.fetchall()
            actual={r['action_id']:r['status'] for r in completed}
            for action in (plan.get('result') or {}).get('actions',[]):
                if action['mode']=='change' and action['action_id'] not in actual:
                    context['unstarted_actions'].append({'plan_id':plan['id'],'run_id':str(row['managed_run_id']),
                        'action_id':action['action_id'],'ref':action['ref'],'label':action['label'],
                        'status':'not_started','note':model_text('management_context.f3c14d09dab4')})
            context['recent_runs'].append({'run_id':str(row['managed_run_id']),'run_date':row['management_date'],
                'conversation_id':plan['ai_call']['prompt']['conversation']['id'],
                'status':row['daily_status'],'executions':completed,'plan_id':plan['id'],'report_date':plan['report_date'],
                'model':plan['model'],'analysis_provider':'deepseek',
                'execution_mode':plan['runtime_mode'],
                'input_hash':row['input_hash'],'summary':(plan['result'] or {}).get('summary'),
                'actions':[{**{k:a[k] for k in ('action_id','ref','mode','label','kind','reason','observation')},
                            'execution_status':actual.get(a['action_id'],'not_started' if a['mode']=='change' else 'observe_only')}
                           for a in (plan['result'] or {}).get('actions',[])],
                'snapshot':{'business':[{k:b[k] for k in ('sku','evidence_id','identity_ambiguous','periods')}
                                        for b in plan['dataset']['content']['business']],
                            'keyword_snapshot':plan['dataset']['content']['keywords']['snapshot']}})
            context['recent_runs'][-1]['complete_actions_in_plan']=plan['id']
        extra=(' OR e.resource_key IN ('+','.join(['%s']*len(resource_keys))+')') if resource_keys else ''
        cur.execute('''SELECT e.*, (SELECT p.prompt_json FROM ai_operation_plans p WHERE p.id=e.plan_id) origin_prompt,
            (SELECT MAX(n.id) FROM ai_operation_executions n
                WHERE n.resource_key=e.resource_key AND n.runtime_mode='real'
                AND n.status IN ('confirmed','submitting','submitted','pending','unknown','issues')) last_execution_id FROM ai_operation_executions e
            WHERE e.shop_id=%s AND e.marketplace_id=%s AND e.runtime_mode='real'
            AND (BINARY e.asin=BINARY %s'''+extra+''') AND e.created_at>=%s ORDER BY e.id DESC LIMIT 201''',
            (identity['id'],identity['marketplace_id'],identity['asin'],*resource_keys,
             management['now'].replace(tzinfo=None)-timedelta(days=60)))
        rows=cur.fetchall()
        if len(rows)>200:raise DomainError(model_text('management_context.d78f4b748bd9'),422)
    for row in rows:
        from .protocol import native_prompt
        if not native_prompt(row.get('origin_prompt')):
            # 只保留旧真实状态用于防重，不解析旧action/baseline或恢复旧观察格式。
            context['execution_facts'].append({'execution_id':str(row['id']),'plan_id':str(row['plan_id']),
                'label':'旧版执行 #'+str(row['id']),'ref':'legacy:execution:'+str(row['id']),
                'status':row['status'],'legacy':True,'note':model_text('management_context.923de6cd2357')})
            continue
        record=execution_store.serialize(row);action=record['action'];baseline=record['baseline']
        if not same_account(baseline['identity'],identity):continue
        # 失败也是真实事实：不能只给成功观察，否则模型会将旧方案中的“已调整”当成执行结果。
        receipt=record.get('receipt') or {}
        context['execution_facts'].append({'execution_id':record['id'],'plan_id':record['plan_id'],
            'action_id':action['action_id'],'ref':action['ref'],'label':action['label'],'status':row['status'],
            'before':action['before'],'proposed_after':action['after'],'accepted':receipt.get('accepted'),
            'submitted_at':receipt.get('submitted_at'),'verified_at':row['verified_at'],
            'note':model_text('management_context.74c37b96d16f') if row['status']=='not_sent' else
                   model_text('management_context.c7ae760ef53d') if row['status']=='confirmed' else model_text('management_context.ed1dfc2e3f42')})
        if row['last_execution_id']!=row['id']:continue
        if row['status'] in ('preparing','submitting','submitted','pending','unknown','issues'):
            context['pending_executions'].append({'execution_id':record['id'],'ref':action['ref'],'label':action['label'],
                                                   'status':row['status'],'before':action['before'],'after':action['after']})
        if row['status']!='confirmed' or row['verified_at'] is None:continue
        duration=action['observation']['duration_days']
        confirmed=row['verified_at'].replace(tzinfo=timezone.utc)
        operated=confirmed.astimezone(BJ).date()
        start=confirmed.astimezone(SOURCE_ZONE).date()+timedelta(days=1);end=start+timedelta(days=duration-1)
        elapsed=min(duration,max(0,(today-operated).days));remaining=max(0,duration-elapsed)
        due=review_due({'elapsed_days':elapsed,'duration_days':duration})
        phase='ready_review' if due else 'observing'
        progress=model_text('management_context.108ba443476a')
        post=post_evidence(pack,action,start,end)
        if due:
            progress=model_text('management_context.38dd32c0cbdd')
            if post['ready']:
                progress=model_text('management_context.e7fe11d97387')
        context['observations'].append({'execution_id':record['id'],'plan_id':record['plan_id'],
            'ref':action['ref'],'label':action['label'],'before':action['before'],'after':action['after'],
            'unit':action['unit'],'kind':action['kind'],
            'changed_at':(record['receipt'] or {}).get('submitted_at') or row['created_at'],'confirmed_at':row['verified_at'],
            'reason':action['reason'],'duration_days':duration,'started_on':start,'due_on':end,
            'elapsed_days':elapsed,'remaining_days':remaining,'phase':phase,'review_due':due,
            'data_complete':bool(post['ready']),'progress_note':progress,
            'report_date':pack['time']['report_end'],'observation_plan':action['observation'],
            'baseline_snapshot':baseline,'post_evidence':post})
    return context


def post_evidence(pack,action,start,end):
    """读精确观察区间，不受页面前6个对象逐日明细的裁剪影响；缺报告不补零。"""
    report_end=day(pack['time']['report_end'])
    business=business_evidence(pack,action,start,end)
    if action['kind'] in ('listing_text_update','listing_price_update'):
        snap=pack['keywords'].get('snapshot') or {}
        needs_keywords=any(m in ('organic_rank','sponsored_rank','traffic_share','organic_share','sponsored_share','weekly_impressions') for m in action['observation']['metrics'])
        return {'source':model_text('management_context.4deb6f8ef4d9'),'ready':business['ready'] and (not needs_keywords or bool(snap.get('record_date') and day(snap['record_date'])>=end and report_end>=end)),
                'business':business,'business_metrics':business['metrics'],
                'snapshot':snap,'items':[{k:r.get(k) for k in ('keyword','organic_rank','sponsored_rank','traffic_share','organic_share')}
                    for r in pack['keywords']['items'] if r['keyword'] in action['observation']['keywords']],
                'note':model_text('management_context.9b6c448f873e')}
    kind={'ads_placement_update':'spCampaignsPlacement','ads_budget_update':'spCampaigns'}.get(action['kind'],'spTargeting')
    subject=action['subject'];identity=pack['identity']
    where="shop_id=%s AND report_type=%s AND campaign_id=%s AND currency='USD' AND report_date BETWEEN %s AND %s"
    args=[identity['id'],kind,subject['campaign_id'],start,min(end,report_end)]
    if kind=='spCampaignsPlacement':
        from .catalog import REPORT_PLACEMENTS
        aliases=[k for k,v in REPORT_PLACEMENTS.items() if v==subject['placement']]+[subject['placement']]
        where+=' AND placement IN ('+','.join(['%s']*len(aliases))+')';args.extend(aliases)
    elif kind=='spTargeting':
        where+=' AND ad_group_id=%s';args.append(subject['ad_group_id'])
        if subject.get('entity_type')!='group':where+=' AND keyword_id=%s';args.append(subject['entity_id'])
    rows=[]
    if start<=report_end:
        with plan_store.transaction() as cur:
            fields=','.join(f'IF(COUNT({f})=COUNT(*),SUM({f}),NULL) {f}' for f in ADS_FIELDS)
            cur.execute('SELECT report_date,'+fields+' FROM amazon_ads_raw_reports WHERE '+where+' GROUP BY report_date ORDER BY report_date',args)
            rows=cur.fetchall()
    observed={r['report_date'] for r in rows if all(r[f] is not None for f in ADS_FIELDS)}
    expected={start+timedelta(days=n) for n in range((end-start).days+1)}
    return {'source':kind,'period_start':start,'period_end':end,'report_through':report_end,
            'business':business,'business_metrics':business['metrics'],
            'ready':expected.issubset(observed),'observed_days':len(observed),
            'missing_days':sorted(d for d in expected-observed if d<=report_end),
            'not_yet_reported_days':sorted(d for d in expected if d>report_end),
            'metrics':ratios(totals(rows,ADS_FIELDS),True),
            'scope':'活动广告位' if kind=='spCampaignsPlacement' else '指定组/投放对象',
            'note':model_text('management_context.4ffadc313296')}


def business_evidence(pack,action,start,end):
    """按修改后的实际起止日读已确认SKU利润口径，不借滚动窗口冒充改后数据。"""
    sku=action['subject'].get('sku')
    if not sku and len(pack['business'])==1:sku=pack['business'][0]['sku']
    rows=[];report_end=day(pack['time']['report_end'])
    if sku and start<=report_end:
        with plan_store.transaction() as cur:
            # 原始表已有SKU/day；仍按日聚合并保留空值，兼容既有历史多行。
            fields=','.join(f'IF(COUNT({f})=COUNT(*),SUM({f}),NULL) {f}' for f in SKU_FIELDS)
            cur.execute('SELECT report_date,'+fields+' FROM sku_profit WHERE shop_id=%s AND BINARY sku=BINARY %s AND report_date BETWEEN %s AND %s GROUP BY report_date ORDER BY report_date',
                        (pack['identity']['id'],sku,start,min(end,report_end)))
            rows=cur.fetchall()
    expected={start+timedelta(days=n) for n in range((end-start).days+1)}
    present={r['report_date'] for r in rows if all(r[f] is not None for f in ('net_profit','sales_amount','sales_qty','ad_cost'))}
    return {'sku':sku,'period_start':start,'period_end':end,'observed_days':len(present),
            'ready':bool(sku) and expected.issubset(present),'metrics':ratios(totals(rows,SKU_FIELDS)),
            'missing_days':sorted(d for d in expected-present if d<=report_end),
            'not_yet_reported_days':sorted(d for d in expected if d>report_end),
            'scope':model_text('management_context.1287a8ab7d8b') if sku else model_text('management_context.a6d1c19f2931'),
            'note':model_text('management_context.87acb6e3b436')}


def attach(data,context):
    """把本轮会话观察和近期结论登记为可核对证据，并校验归档大小。"""
    # 历史证据与当前证据使用独立编号，模型可明确引用上次执行而非编造关联。
    for row in context.get('execution_facts',[])+context.get('observations',[])+context.get('pending_executions',[]):
        ref='history:execution:'+str(row['execution_id'])
        entry={'label':row['label']+' · 往期执行','kind':'execution_history','data':row}
        data['evidence'][ref]=entry
        data['content']['evidence_index'][ref]={k:v for k,v in entry.items() if k!='data'}
    for row in context.get('recent_runs',[]):
        ref='history:run:'+str(row['run_id'])
        entry={'label':str(row['run_date'])+' · 往期AI结论','kind':'analysis_history','data':row}
        data['evidence'][ref]=entry
        data['content']['evidence_index'][ref]={k:v for k,v in entry.items() if k!='data'}
    data['content']['management_context']=context
    data['content_hash']=digest(data['content']);data['size_bytes']=len(dumps(data['content']).encode('utf8'))
    from .dataset import MAX_INPUT_BYTES
    if data['size_bytes']>MAX_INPUT_BYTES:
        raise DomainError(model_text('management_context.91d2e87cc0d6'),422)
