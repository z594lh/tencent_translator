"""单站点SP创建编排：每次推进一步，先保存发送意图，未知结果禁止重发。"""
import copy
import hashlib
import uuid
from contextlib import contextmanager

import requests
from flask import g, has_request_context

from services import ads_creation_repository as repo
from services import ads_creation_validation as v
from services import ads_history_audit as audit
from services import ads_entity_sync_service as entities
from services import ads_negative_service as negatives

Error = repo.CreationError
RESOURCES = {
    'campaign': ('campaigns', 'campaigns', 'campaignId', 'create_campaigns'),
    'group': ('adGroups', 'adGroups', 'adGroupId', 'create_ad_groups'),
    'ads': ('productAds', 'productAds', 'adId', 'create_product_ads'),
    'keywords': ('keywords', 'keywords', 'keywordId', 'create_keywords'),
    'targets': ('targets', 'targetingClauses', 'targetId', 'create_targets'),
    'auto': ('targets', 'targetingClauses', 'targetId', None),
    'negative_keywords': ('negativeKeywords', 'negativeKeywords', 'keywordId', 'create_negative_keywords'),
    'negative_products': ('negativeTargets', 'negativeTargetingClauses', 'targetId', 'create_negative_targets'),
}
LABELS = {'campaign':'创建活动', 'group':'创建广告组', 'ads':'添加推广商品', 'keywords':'添加关键词',
          'targets':'添加商品投放', 'auto':'配置四类自动投放', 'negative_keywords':'添加否定关键词',
          'negative_products':'添加否定商品'}


def _uuid(value):
    try: return str(uuid.UUID(str(value)))
    except (ValueError, TypeError): raise Error('创建编号须为有效UUID') from None


@contextmanager
def locked(key):
    conn = repo.get_db_connection()
    held = False
    try:
        conn.autocommit(True)
        with conn.cursor() as c:
            c.execute('SELECT GET_LOCK(%s,0) n', ('ads_create_'+hashlib.sha256(key.encode()).hexdigest()[:40],))
            held = c.fetchone()['n'] == 1
        if not held: raise Error('此创建记录正在执行，请稍后读取进度，勿重复提交', 409)
        yield
    finally:
        if held:
            with conn.cursor() as c: c.execute('SELECT RELEASE_LOCK(%s)', ('ads_create_'+hashlib.sha256(key.encode()).hexdigest()[:40],))
        conn.close()


def _step(key, kind, operation, payloads=None, label=None):
    return {'key':key, 'kind':kind, 'operation':operation, 'label':label or LABELS.get(kind,kind),
            'status':'pending', 'items':[{'payload':x,'status':'pending','id':None,'error':'','attempt':0} for x in (payloads or [])]}


def plan(data):
    steps = []
    if not data.get('campaign_id'):
        steps += [_step('campaign','campaign','create',[data['campaign']]), _step('verify_campaign','campaign','read',label='核对活动并保存本地')]
    group = {**data['group'], 'state':'PAUSED' if data.get('campaign_id') else 'ENABLED'}
    steps += [_step('group','group','create',[group]), _step('verify_group','group','read',label='核对广告组并保存本地'),
              _step('ads','ads','create',data['products']), _step('verify_ads','ads','read',label='核对推广商品')]
    if data['mode'] == 'AUTO':
        steps += [_step('discover_auto','auto','discover',label='取得四类自动投放真实ID'),
                  _step('auto','auto','update',data['targets']), _step('verify_auto','auto','read',label='核对自动投放状态及竞价')]
    else:
        kind = 'keywords' if data['mode']=='KEYWORDS' else 'targets'
        steps += [_step(kind,kind,'create',data['targets']), _step('verify_'+kind,kind,'read',label='核对投放内容及竞价')]
    for kind in ('negative_keywords','negative_products'):
        if data[kind]: steps += [_step(kind,kind,'create',data[kind]),_step('verify_'+kind,kind,'read',label='核对'+LABELS[kind][2:])]
    if data['launch']:
        kind = 'group' if data.get('campaign_id') else 'campaign'
        steps += [_step('launch',kind,'update',[{'state':'ENABLED'}],label='启动本次创建的对象'),
                  _step('verify_launch',kind,'read',label='核对启动结果')]
    return steps


def view(job):
    return {k:job.get(k) for k in ('id','shop_id','campaign_id','ad_group_id','status','error_message','created_at','updated_at')} | {
        'configuration':job['request_json'], 'steps':job['progress_json'],
        'completed_steps':sum(s['status']=='complete' for s in job['progress_json'])}


def load(job_id, shop_id, operator_id):
    job = repo.get(_uuid(job_id), int(v.identifier(shop_id,'店铺ID')), operator_id)
    acct = v.account(job['shop_id'])
    for key in ('profile_id','region','marketplace_id','credential_group_id'):
        if str(job[key]) != str(acct[key]): raise Error('创建时的账户身份已变化，停止执行，请人工核实',409)
    return job, acct


def start(body, operator_id):
    if not isinstance(body, dict): raise Error('创建参数须为JSON对象')
    job_id = _uuid(body.get('submission_id'))
    meta = v.metadata(body.get('shop_id'), body.get('campaign_id'))
    data = v.normalize(body, meta)
    fingerprint = hashlib.sha256(repo.dumps(data).encode()).hexdigest()
    with locked('submit:'+str(meta['shop_id'])+':'+str(operator_id)):
        with repo.transaction() as c:
            c.execute('SELECT id FROM amazon_ads_creation_jobs WHERE id=%s', (job_id,))
            old = c.fetchone()
            if not old:
                c.execute('''SELECT id FROM amazon_ads_creation_jobs WHERE shop_id=%s AND operator_id=%s AND request_hash=%s
                             AND created_at>=UTC_TIMESTAMP()-INTERVAL 10 MINUTE ORDER BY created_at DESC LIMIT 1''',
                          (meta['shop_id'],operator_id,fingerprint))
                old = c.fetchone()
            if not old:
                c.execute('''INSERT INTO amazon_ads_creation_jobs
                    (id,shop_id,credential_group_id,region,marketplace_id,profile_id,operator_id,request_hash,request_json,
                     progress_json,campaign_id,status,created_at,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ready',UTC_TIMESTAMP(),UTC_TIMESTAMP())''',
                          (job_id,meta['shop_id'],meta['credential_group_id'],meta['region'],meta['marketplace_id'],meta['profile_id'],
                           operator_id,fingerprint,repo.dumps(data),repo.dumps(plan(data)),data.get('campaign_id')))
        job = repo.get(old['id'] if old else job_id, meta['shop_id'], operator_id)
        if job['request_hash'] != fingerprint: raise Error('同一创建编号参数已变化，请恢复原记录或新建草稿',409)
    return view(job)


def _payload(job, step, item):
    data, kind = job['request_json'], step['kind']
    row = copy.deepcopy(item['payload'])
    if kind != 'campaign': row['campaignId'] = job['campaign_id']
    if kind not in ('campaign','group') and not (kind.startswith('negative_') and data['negative_scope']=='campaign'):
        row['adGroupId'] = job['ad_group_id']
    if kind == 'auto':
        # null清除独立竞价，真正继承组默认值，而不是复制一个以后不联动的值。
        row = {'targetId':item['id'],'state':row['state'],'bid':row['bid']}
    elif step['operation']=='update':
        row[RESOURCES[kind][2]] = job['ad_group_id'] if kind=='group' else job['campaign_id']
    return row


def _read(client, job, kind, candidate=None):
    data = job['request_json']
    cid, gid = job['campaign_id'], job['ad_group_id']
    if kind == 'campaign': return v.pages(client.list_sp_campaigns,'campaigns',campaign_id=candidate or cid)
    if kind == 'group': return v.pages(client.list_ad_groups,'adGroups',campaign_id=cid,ad_group_id=candidate or gid)
    if kind.startswith('negative_'):
        keyword = kind=='negative_keywords'
        level = data['negative_scope']=='campaign'
        fn = client.list_negative_keywords if keyword else client.list_negative_targets
        return v.pages(fn, negatives._key('keywords' if keyword else 'targets',level),campaign_id=cid,
                       **({} if level else {'ad_group_id':gid}),campaign_level=level)
    family = 'product_ads' if kind=='ads' else 'targets' if kind=='auto' else kind
    method,key,*_ = entities.FAMILIES[family]
    return v.pages(getattr(client,method),key,campaign_id=cid,ad_group_id=gid)


def _owner(job, kind, row):
    if kind != 'campaign' and str(row.get('campaignId')) != str(job['campaign_id']): raise Error('回读对象活动归属不符',502)
    if kind not in ('campaign','group'):
        group = None if kind.startswith('negative_') and job['request_json']['negative_scope']=='campaign' else job['ad_group_id']
        if str(row.get('adGroupId') or '') != str(group or ''): raise Error('回读对象广告组层级不符',502)


def _equal(expected, row):
    for key in ('name','sku','asin','keywordText','matchType','targetingType','state','startDate','endDate','portfolioId'):
        if key in expected and str(row.get(key) or '') != str(expected[key] or ''): return False
    for key in ('bid','defaultBid'):
        if key in expected:
            if expected[key] is None:
                if row.get(key) is not None: return False
                continue
            try:
                if abs(float(row.get(key))-float(expected[key])) > .00001: return False
            except (TypeError,ValueError): return False
    if 'expression' in expected and repo.dumps(row.get('expression')) != repo.dumps(expected['expression']): return False
    if 'budget' in expected:
        try:
            if row['budget']['budgetType'] != expected['budget']['budgetType'] or abs(float(row['budget']['budget'])-expected['budget']['budget'])>.00001: return False
        except (KeyError,TypeError,ValueError): return False
    if 'dynamicBidding' in expected:
        actual = row.get('dynamicBidding') or {}
        wanted = expected['dynamicBidding']
        if actual.get('strategy') != wanted.get('strategy'): return False
        places = {x.get('placement'):x.get('percentage') for x in actual.get('placementBidding',[])}
        if any(places.get(x['placement'],0) != x['percentage'] for x in wanted.get('placementBidding',[])): return False
    return True


def _cache(job, acct, kind, rows):
    from blueprints.amazon import advertising_manage as legacy
    family = {'campaign':'campaigns','group':'ad_groups','ads':'product_ads','auto':'targets'}.get(kind,kind)
    with repo.transaction() as c:
        if kind.startswith('negative_'):
            negatives.save_rows(c,'keywords' if kind=='negative_keywords' else 'targets',rows,job['request_json']['negative_scope']=='campaign')
            return
        datasets = {f:[] for f in entities.FAMILIES}
        datasets[family] = rows
        entities._check_existing(c,datasets,acct)
        if family not in ('campaigns','ad_groups'):
            table, column = entities.FAMILIES[family][3:]
            for row in rows:
                c.execute(f'SELECT ad_group_id FROM {table} WHERE {column}=%s',(row[entities.FAMILIES[family][2]],))
                old = c.fetchone()
                if old and str(old['ad_group_id']) != str(row['adGroupId']): raise Error('既有实体ID的组归属冲突，未覆盖',409)
        snapshot = entities._SnapshotClient(acct['profile_id'],datasets)
        fn = getattr(legacy,'_sync_'+family)
        if family=='campaigns': fn(c,snapshot,acct['shop_id'],acct['profile_id'])
        else: fn(c,snapshot,acct['shop_id'])


def _verify(job, acct, step, client):
    kind = step['kind']
    rows = _read(client,job,kind)
    id_key = RESOURCES[kind][2]
    mapped = {}
    for row in rows:
        eid = v.identifier(row.get(id_key),'平台实体ID')
        _owner(job,kind,row)
        if eid in mapped: raise Error('回读包含重复ID，未确认结果',502)
        mapped[eid] = row
    if step['operation']=='discover':
        wanted = next(s for s in job['progress_json'] if s['key']=='auto')
        by_type = {}
        for row in rows:
            expr = row.get('expression') or row.get('resolvedExpression') or []
            for x in expr:
                target = x.get('type')
                if target in v.AUTO_TYPES:
                    if target in by_type: raise Error('同一自动类型有多个目标，不能猜测ID',502)
                    by_type[target] = row
        if set(by_type) != set(v.AUTO_TYPES): raise Error('四类自动目标尚未完整返回，请稍后继续；不会重复建组',409)
        for item in wanted['items']: item['id'] = str(by_type[item['payload']['type']]['targetId'])
        _cache(job,acct,kind,list(by_type.values()))
    else:
        source = next((s for s in job['progress_json'] if s['key']==('launch' if step['key']=='verify_launch' else kind)),None)
        if not source: raise Error('核验步骤缺少创建证据',502)
        verified = []
        for item in source['items']:
            if item['status']=='skipped': continue
            eid = str(item.get('id') or '')
            row = mapped.get(eid)
            if not row or not _equal(_payload(job,source,item),row): raise Error('平台尚未返回与提交一致的实体、竞价或状态，请稍后重读核验',409)
            verified.append(row)
        if not verified: raise Error('没有可核验的成功对象，不能完成创建',409)
        _cache(job,acct,kind,verified)
    step['status'] = 'complete'


def _write(job, step, client, retry_failed=False):
    selected = [x for x in step['items'] if x['status']=='pending' or retry_failed and x['status']=='failed']
    if any(x['status'] in ('unknown','requesting') for x in step['items']):
        raise Error('存在发送结果待确认项，必须先核实，禁止重新创建',409)
    if not selected: raise Error('没有待执行项；明确失败请使用仅重试失败项',409)
    for item in selected:
        item.update(status='requesting',attempt=item['attempt']+1,error='')
    job['status'],step['status'] = 'running','running'
    repo.save(job)  # 必须先提交发送证据；失败即不发送。
    payloads = [_payload(job,step,x) for x in selected]
    kind = step['kind']; resource,key,id_key,method = RESOURCES[kind]
    if kind == 'ads':
        # 本版仅Seller：API用SKU创建，ASIN仅用于本地选品及平台回读核对。
        payloads = [{k:v for k,v in row.items() if k!='asin'} for row in payloads]
    campaign_level = kind.startswith('negative_') and job['request_json']['negative_scope']=='campaign'
    if campaign_level:
        resource,key,*_ = client._negative_resource('keywords' if kind=='negative_keywords' else 'targets',True)
    try:
        if step['operation']=='create':
            kwargs = {'campaign_level':campaign_level} if kind.startswith('negative_') else {}
            result = getattr(client,method)(payloads,**kwargs)
        else:
            result = client._request('PUT','/sp/'+resource,json_data={key:payloads},
                                     headers=client._media_type_headers({'campaign':client._V3_SP_CAMPAIGN_CT,'group':client._V3_SP_ADGROUP_CT,'auto':client._V3_SP_TARGET_CT}[kind]))
        outcomes = audit.results(result,key,[{}]*len(selected))
        for index,item in enumerate(selected):
            status, entry = outcomes.get(index,('unknown',{}))
            eid = audit._response_id(entry,id_key)
            if status=='accepted':
                if not eid or item.get('id') and str(item['id']) != str(eid): status='unknown'
                else:
                    try: eid = v.identifier(eid,'返回ID')
                    except Error: status='unknown'
                for parent_key in ('campaignId','adGroupId'):
                    returned = audit._response_id(entry,parent_key)
                    expected = payloads[index].get(parent_key)
                    if expected and returned and str(expected)!=str(returned): status='unknown'
            if status=='accepted':
                item.update(status='accepted',id=eid,error='')
                if kind=='campaign': job['campaign_id']=eid
                if kind=='group': job['ad_group_id']=eid
            else:
                item.update(status=status,error=negatives._error_message(entry) if status=='failed' else '响应缺少可信结果或ID，须核实后关联，禁止重复创建')
    except Exception as exc:
        status = exc.response.status_code if isinstance(exc,requests.HTTPError) and exc.response is not None else None
        definite = status in (400,401,403,404,409,422,429)
        message = f'Amazon明确拒绝（HTTP {status}），请核对后仅重试失败项' if definite else '发送结果待确认，可能已在平台创建；禁止整批重发，请先核实ID'
        for item in selected:
            if item['status']=='requesting': item.update(status='failed' if definite else 'unknown',error=message)
    statuses = {x['status'] for x in step['items']}
    step['status'] = 'complete' if statuses <= {'accepted','skipped'} else 'unknown' if statuses & {'unknown','requesting'} else 'failed'


def run(job_id, shop_id, operator_id, retry_failed=False):
    with locked(_uuid(job_id)):
        job, acct = load(job_id,shop_id,operator_id)
        if job['status']=='complete': return view(job)
        step = next((s for s in job['progress_json'] if s['status']!='complete'),None)
        if not step: job['status']='complete';repo.save(job);return view(job)
        client = v.client_for(acct)
        # 每步重新核对父活动真实类型及归属，已有活动也不能被浏览器篡改模式。
        if job['campaign_id']:
            parents = v.pages(client.list_sp_campaigns,'campaigns',campaign_id=job['campaign_id'])
            if len(parents)!=1 or str(parents[0]['campaignId'])!=str(job['campaign_id']) or parents[0].get('targetingType')!=job['request_json']['targeting_type'] or parents[0].get('state')=='ARCHIVED':
                raise Error('父活动真实类型、状态或归属不符，停止创建',409)
            if not job['request_json'].get('campaign_id') and step['key']!='verify_launch' and parents[0].get('state')!='PAUSED':
                raise Error('本次新活动已不处于暂停保护，停止继续配置，请先核实平台状态',409)
        if step['status']=='running' or any(x['status']=='requesting' for x in step['items']):
            for item in step['items']:
                if item['status']=='requesting': item.update(status='unknown',error='上次发送中断，结果待确认，禁止自动重发')
            step['status']='unknown'
        try:
            if step['operation'] in ('read','discover'):
                _verify(job,acct,step,client)
            else:
                previous = g.get('ads_creation_partial_results',False) if has_request_context() else False
                if has_request_context(): g.ads_creation_partial_results = True
                try: _write(job,step,client,retry_failed)
                finally:
                    if has_request_context(): g.ads_creation_partial_results = previous
            job['error_message'] = ''
        except (Error,requests.RequestException) as exc:
            if step['operation'] in ('read','discover'):
                step['status']='waiting'
                job['error_message']=str(exc)[:500] if isinstance(exc,Error) else '回读失败或限流，已创建对象保留，请稍后继续；不会重复创建'
            else:
                job['error_message']=str(exc)[:500]
        job['status'] = ('complete' if all(s['status']=='complete' for s in job['progress_json']) else
                         step['status'] if step['status'] in ('failed','unknown','waiting') else 'ready')
        repo.save(job)
        return view(job)


def repair(job_id, shop_id, operator_id, body):
    """只修正平台明确拒绝的项；已接受和未知项不可改写或再次创建。"""
    with locked(_uuid(job_id)):
        job,acct = load(job_id,shop_id,operator_id)
        step = next((s for s in job['progress_json'] if s['status']!='complete'),None)
        index = body.get('index')
        if not step or isinstance(index,bool) or not isinstance(index,int) or not 0<=index<len(step['items']): raise Error('修正项不存在')
        item = step['items'][index]
        if item['status']!='failed': raise Error('只允许修正明确失败项，未知或成功项不能重发')
        if body.get('skip'):
            if step['kind'] in ('campaign','group','auto') or step['key']=='launch': raise Error('关键步骤不能跳过')
            item.update(status='skipped',error='操作人明确跳过失败项')
        else:
            proposed = body.get('payload')
            if not isinstance(proposed,dict): raise Error('修正内容须为JSON对象')
            original = item['payload']; kind = step['kind']; currency = job['request_json']['currency']
            if set(proposed)-set(original): raise Error('不能增加原请求之外的字段')
            row = {**original,**proposed}
            if kind in ('campaign','group') and row.get('name'):
                row['name']=v.text(row['name'],'名称')
            if any(row.get(k)!=original.get(k) for k in ('state','targetingType','expressionType','type','startDate','endDate','portfolioId','dynamicBidding')):
                raise Error('只能修正名称、文本或竞价，不能改变类型、状态和活动设置')
            for key in ('bid','defaultBid'):
                if key in row: row[key]=v.money(row[key],'竞价',currency,nullable=kind=='auto')
            if kind=='campaign':
                row['budget']={'budgetType':'DAILY','budget':v.money((row.get('budget') or {}).get('budget'),'预算',currency)}
            if kind=='keywords':
                row['keywordText']=v.text(row.get('keywordText'),'关键词',80)
                if len(row['keywordText'].split())>10 or row.get('matchType') not in ('EXACT','PHRASE','BROAD'): raise Error('关键词匹配或长度无效')
            if kind=='ads':
                with repo.transaction() as c:
                    c.execute('''SELECT sku,asin,parentage_level FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s
                                 AND seller_id=%s AND sku=%s AND asin=%s AND is_deleted=0''',
                              (acct['shop_id'],acct['marketplace_id'],acct['seller_id'],row.get('sku'),row.get('asin')))
                    found=c.fetchall()
                if len(found)!=1 or str(found[0]['parentage_level'] or '').lower()=='parent': raise Error('修正商品不属于当前店铺或不是可选子体')
            if kind in ('targets','negative_keywords','negative_products'):
                fake={'campaign_id':1,'ad_group_id':2,'scope':'group','campaign':{'state':'PAUSED','targeting_type':job['request_json']['targeting_type']}}
                if kind=='targets':
                    expr=row.get('expression')
                    import re
                    if not isinstance(expr,list) or len(expr)!=1 or expr[0].get('type')!='ASIN_SAME_AS' or not re.fullmatch('[A-Z0-9]{10}',str(expr[0].get('value',''))): raise Error('只支持有效ASIN商品投放')
                else:
                    try: negatives.prepare_items('keywords' if kind=='negative_keywords' else 'targets',fake,[{**row,'campaignId':'1','adGroupId':'2'}])
                    except negatives.NegativeError as exc: raise Error(str(exc)) from None
            item.update(payload=row,status='failed',error='已修正，请仅重试失败项')
        if all(x['status'] in ('accepted','skipped') for x in step['items']): step['status']='complete';job['status']='ready'
        repo.save(job)
        return view(job)


def resolve(job_id, shop_id, operator_id, body):
    """操作人明确给出核实ID后做只读检查；不按同名自动认领，不重发写请求。"""
    with locked(_uuid(job_id)):
        job,acct=load(job_id,shop_id,operator_id)
        step=next((s for s in job['progress_json'] if s['status']!='complete'),None)
        index=body.get('index')
        if not step or isinstance(index,bool) or not isinstance(index,int) or not 0<=index<len(step['items']): raise Error('待核实项不存在')
        item=step['items'][index]
        if item['status'] not in ('unknown','requesting'): raise Error('此项不属于结果待确认状态')
        eid=v.identifier(body.get('entity_id') or item.get('id'),'核实实体ID')
        client=v.client_for(acct)
        rows=_read(client,job,step['kind'],eid if step['kind'] in ('campaign','group') else None)
        matched=[row for row in rows if str(row.get(RESOURCES[step['kind']][2]))==eid]
        if len(matched)!=1: raise Error('未回读到指定ID，禁止自动重新创建',409)
        _owner(job,step['kind'],matched[0])
        if not _equal(_payload(job,step,item),matched[0]): raise Error('指定ID与原提交内容、竞价或状态不符，未关联',409)
        item.update(id=eid,status='accepted',error='',confirmed_by=operator_id)
        if step['kind']=='campaign': job['campaign_id']=eid
        if step['kind']=='group': job['ad_group_id']=eid
        if all(x['status'] in ('accepted','skipped') for x in step['items']): step['status']='complete';job['status']='ready'
        repo.save(job)
        return view(job)
