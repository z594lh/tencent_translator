"""历史只读消费者：近期补采优先、每日一次三天回扫、首次回填单独推进；不修改广告。"""
import time
from email.utils import parsedate_to_datetime
import requests
from services import ads_history_repository as repo,ads_history_jobs as jobs
from services import ads_history_service as history
from services.shop_service import get_ads_api_client,get_all_active_shops

PAGE_SIZE=100
MAX_CALLS=4
MAX_ENTITY_IDS=10
RETRY_DELAYS=(300000,900000,3600000,10800000,43200000)


def account_where(ctx):
    """所有队列SQL固定按区域和Profile隔离，再于消费前核对店铺/凭证/站点。"""
    return (ctx['region'],str(ctx['profile_id']))


def save(c,job,**values):
    """短事务保存任务进度；字段白名单，不能把外部数据拼为SQL标识符。"""
    allowed={'status','attempts','next_attempt_ms','from_ms','to_ms','page_no','error_message','work_json','saved_count'}
    if set(values)-allowed:raise ValueError('队列更新字段不合法')
    if 'work_json' in values:values['work_json']=repo.dumps(values['work_json'])
    c.execute('UPDATE amazon_ads_history_jobs SET '+','.join(k+'=%s' for k in values)+' WHERE id=%s',(*values.values(),job['id']))


def select_job(c,ctx,turn,clock):
    """四请求轮次保底；运维修复可限定已校验活动/组，仍遵守同一账户锁与冷却。"""
    lanes=(('erp','manual'),('daily',),('erp','manual'),('initial',))[turn%MAX_CALLS]
    base="SELECT * FROM amazon_ads_history_jobs WHERE region=%s AND profile_id=%s AND status IN ('queued','retry') AND next_attempt_ms<=%s"
    args=[*account_where(ctx),clock]
    if ctx.get('scope') in ('campaign','group'):
        base+=' AND campaign_id=%s';args.append(str(ctx['campaign_id']))
        if ctx['scope']=='group':base+=' AND ad_group_id=%s';args.append(str(ctx['ad_group_id']))
    order=" ORDER BY to_ms DESC,updated_at,id LIMIT 1 FOR UPDATE"
    c.execute(base+' AND lane IN ('+','.join(['%s']*len(lanes))+')'+order,(*args,*lanes))
    job=c.fetchone()
    if not job:
        c.execute(base+" ORDER BY FIELD(lane,'erp','manual','daily','initial'),to_ms DESC,updated_at,id LIMIT 1 FOR UPDATE",args)
        job=c.fetchone()
    if job:job['work']=repo.loads(job['work_json'])
    return job


def merge_changes(c,ctx,job,clock):
    """同活动/组/类型的重叠窄窗最多合并10个实体，保留源事件和被合并任务引用。"""
    if job['lane']!='erp' or job['page_no'] or job['saved_count']:return
    c.execute('''SELECT * FROM amazon_ads_history_jobs WHERE region=%s AND profile_id=%s AND lane='erp'
        AND campaign_id=%s AND ad_group_id<=>%s AND entity_type=%s AND status IN ('queued','retry')
        AND id<>%s AND page_no=0 AND saved_count=0 AND attempts=%s AND next_attempt_ms<=%s
        AND from_ms<=%s AND to_ms>=%s ORDER BY id LIMIT 49 FOR UPDATE''',
        (*account_where(ctx),job['campaign_id'],job['ad_group_id'],job['entity_type'],job['id'],job['attempts'],clock,job['to_ms'],job['from_ms']))
    for other in c.fetchall():
        data=repo.loads(other['work_json']);ids=set(job['work'].get('entity_ids',[])+data.get('entity_ids',[]))
        if len(ids)>MAX_ENTITY_IDS:continue
        job['work']['entity_ids']=sorted(ids)
        job['work']['history_ids']=sorted(set(job['work'].get('history_ids',[])+data.get('history_ids',[])))
        job['from_ms']=min(job['from_ms'],other['from_ms']);job['to_ms']=max(job['to_ms'],other['to_ms'])
        data['merged_into']=job['id'];save(c,other,status='merged',work_json=data,error_message='已合并到同范围窄窗任务')
    job['work'].pop('query_to',None)
    save(c,job,from_ms=job['from_ms'],to_ms=job['to_ms'],work_json=job['work'])


def trim_expired(c,job,clock):
    """过期部分只记录缺口，不反复抛错阻塞整账户；改变窗口必须重置页码。"""
    floor=clock-90*jobs.DAY+60000
    if job['from_ms']>=floor:return False
    gap={'from_ms':job['from_ms'],'to_ms':min(job['to_ms'],floor-1),'reason':'已超过本系统历史可查询保留范围'}
    job['work'].setdefault('gaps',[]).append(gap)
    if job['to_ms']<floor:
        save(c,job,status='gap',work_json=job['work'],error_message='该窗口已过期，记录缺口；其他近期任务继续')
        ids=job['work'].get('history_ids',[])
        if ids:c.execute('UPDATE amazon_ads_history SET error_message=%s WHERE id IN ('+','.join(['%s']*len(ids))+') AND platform_time_ms IS NULL',('原操作时间超过可查范围，历史有缺口；保留修改回执',*ids))
        return True
    job['from_ms']=floor;job['page_no']=0;job['work'].pop('query_to',None);job['work'].pop('page_hash',None)
    save(c,job,from_ms=floor,page_no=0,work_json=job['work'],error_message='旧片段不可查询，其余片段继续')
    return False


def scoped(ctx,job):
    """账户身份必须与队列冻结值一致；目录/活动/组上下文均不能跨Profile或凭证。"""
    for key in ('shop_id','credential_group_id','marketplace_id','region','profile_id'):
        if str(ctx[key] or '')!=str(job[key] or ''):raise repo.HistoryError('队列账户身份已变化，未继续采集',409)
    return {**ctx,'scope':'group' if job['ad_group_id'] else 'campaign','campaign_id':job['campaign_id'],'ad_group_id':job['ad_group_id']}


def finalize_batch(c,ctx,job):
    """只在同批所有目录/窗口终态后推进覆盖水位；有缺口保持partial，绝不伪报完整。"""
    if job['lane'] not in ('initial','daily'):return
    c.execute('SELECT status,COUNT(*) n FROM amazon_ads_history_jobs WHERE batch_key=%s GROUP BY status',(job['batch_key'],))
    states={r['status']:r['n'] for r in c.fetchall()}
    if any(k not in jobs.TERMINAL for k in states):return
    c.execute("SELECT * FROM amazon_ads_history_jobs WHERE batch_key=%s AND entity_type='CATALOG'",(job['batch_key'],));catalog=c.fetchone()
    if not catalog:return
    root=repo.ensure_state(c,jobs.root_context(ctx));meta=repo.loads(root['work_json'])
    partial=bool(states.get('partial') or states.get('gap'))
    if job['lane']=='daily':meta['daily_through_ms']=max(meta.get('daily_through_ms',0),catalog['to_ms'])
    else:meta['initial_completed_ms']=catalog['to_ms']
    c.execute('''UPDATE amazon_ads_history_sync SET work_json=%s,status=%s,error_message=%s,
        last_success_ms=IF(%s,%s,last_success_ms),deep_success_ms=IF(%s,%s,deep_success_ms) WHERE id=%s''',
        (repo.dumps(meta),'partial' if partial else 'success','存在已记录的过期缺口，非完整历史' if partial else '',
         job['lane']=='daily',catalog['to_ms'],job['lane']=='initial' and not partial,catalog['to_ms'],root['id']))


def catalog_result(c,ctx,job,result):
    """分页枚举启用/暂停/归档活动，生成该批次固定上界的活动/单类型/短时间片任务。"""
    rows=result.get('campaigns') if isinstance(result,dict) else None
    token=result.get('nextToken') if isinstance(result,dict) else None
    if not isinstance(rows,list) or len(rows)>100 or token is not None and not isinstance(token,str):
        raise repo.HistoryError('活动目录分页响应异常，未标记回扫完成',502)
    work=job['work'];seen=set(work.get('campaign_ids',[]));ids=[]
    for row in rows:
        value=str(row.get('campaignId') or '') if isinstance(row,dict) else ''
        if not value.isdecimal() or int(value)<=0:raise repo.HistoryError('活动目录缺少有效活动编号',502)
        if value not in seen:ids.append(value);seen.add(value)
    if token and (not ids or repo.digest(token) in work.get('tokens',[])):
        raise repo.HistoryError('活动目录游标重复或没有推进，未漏页标完成',502)
    if len(seen)>10000:raise repo.HistoryError('活动目录超过安全规模，需人工核对后扩展',422)
    for campaign in ids:
        hi=job['to_ms']
        while hi>=job['from_ms']:
            lo=max(job['from_ms'],hi-work['chunk_ms']+1)
            for kind in history.KINDS:
                entity_ids=[campaign] if kind=='CAMPAIGN' else []
                jobs.insert(c,ctx,job['lane'],kind,lo,hi,job['batch_key'],campaign=campaign,work={'entity_ids':entity_ids})
            hi=lo-1
    work['campaign_ids']=sorted(seen);work['token']=token or None
    if token:work.setdefault('tokens',[]).append(repo.digest(token))
    save(c,job,status='queued' if token else 'partial' if work.get('gaps') else 'completed',work_json=work,attempts=0,error_message='')


def remaining(c,job):
    """只核对原事件是否已获平台历史关联；未知写入不借匹配推断为成功。"""
    ids=job['work'].get('history_ids',[])
    if not ids:return []
    c.execute('SELECT id FROM amazon_ads_history WHERE id IN ('+','.join(['%s']*len(ids))+') AND platform_time_ms IS NULL',tuple(ids))
    return [r['id'] for r in c.fetchall()]


def history_result(c,ctx,job,result):
    """原事件和页码在同事务内提交；完整窗口之后才按已有唯一匹配规则补关联。"""
    total=result.get('totalRecords',result.get('totalResults')) if isinstance(result,dict) else None
    rows=result.get('events') if isinstance(result,dict) else None
    if isinstance(total,bool) or not isinstance(total,int) or total<0 or not isinstance(rows,list) or len(rows)>PAGE_SIZE:
        raise repo.HistoryError('历史分页响应异常，未推进检查点',502)
    hi=job['work']['query_to']
    if total>=10000:
        if job['from_ms']>=hi:raise repo.HistoryError('同毫秒历史超过分页上限，不能截断',502)
        middle=(job['from_ms']+hi)//2
        for lo,end in ((job['from_ms'],middle),(middle+1,hi)):
            jobs.insert(c,ctx,job['lane'],job['entity_type'],lo,end,job['batch_key'],campaign=job['campaign_id'],group=job['ad_group_id'],
                work={k:v for k,v in job['work'].items() if k not in ('query_to','page_hash')},key=['split',job['job_key'],lo,end])
        save(c,job,status='merged',error_message='窗口超过分页上限，已拆分继续')
        return 0
    if len(rows)!=min(PAGE_SIZE,max(0,total-job['page_no']*PAGE_SIZE)):
        raise repo.HistoryError('历史页条数与总数不符，未将缺页标记完整',502)
    local=scoped(ctx,job)
    events=[history.normalize_event(c,local,row,job['from_ms'],hi) for row in rows]
    fingerprints=[e['fingerprint'] for e in events]
    page_hash=repo.digest(fingerprints)
    if len(set(fingerprints))!=len(fingerprints) or rows and job['page_no'] and page_hash==job['work'].get('page_hash'):
        raise repo.HistoryError('历史页重复，未推进检查点',502)
    if job['work'].get('entity_ids') and any(e['entity_id'] not in job['work']['entity_ids'] for e in events):
        raise repo.HistoryError('平台返回其他实体历史，未混入精准补采',502)
    if any(('PRODUCT_TARGETING' if e['entity_type']=='NEGATIVE_PRODUCT_TARGETING' else e['entity_type'])!=job['entity_type'] for e in events):
        raise repo.HistoryError('历史实体类型与本次请求不符',502)
    saved=sum(repo.save_event(c,local,e) for e in events)
    complete=(job['page_no']+1)*PAGE_SIZE>=total
    job['work']['page_hash']=page_hash
    if not complete:
        save(c,job,page_no=job['page_no']+1,work_json=job['work'],saved_count=job['saved_count']+saved,status='queued',error_message='')
        return saved
    # 完成该窗口所有页后，使用本地已保存原文，保留ERP操作人和原提交证据。
    c.execute('''SELECT platform_event FROM amazon_ads_history WHERE region=%s AND profile_id=%s AND campaign_id=%s
        AND source='PLATFORM' AND platform_time_ms BETWEEN %s AND %s AND merged_into_id IS NULL''',
        (*account_where(ctx),job['campaign_id'],job['from_ms'],hi))
    for row in c.fetchall():
        raw=repo.loads(row['platform_event'])
        if raw.get('entityType')!=job['entity_type'] or job['work'].get('entity_ids') and str(raw.get('entityId')) not in job['work']['entity_ids']:continue
        event=history.normalize_event(c,local,raw,job['from_ms'],hi);saved+=repo.save_event(c,local,event,allow_merge=True)
    unmatched=remaining(c,job);attempts=job['attempts']+1
    job['work'].pop('query_to',None);job['work'].pop('page_hash',None)
    state='partial' if job['work'].get('gaps') else 'completed';due=0;note=''
    if unmatched:
        state='unmatched' if attempts>=6 else 'retry'
        due=repo.now_ms()+RETRY_DELAYS[min(attempts-1,len(RETRY_DELAYS)-1)]
        note='历史尚未唯一匹配；待人工核对，不代表广告写入失败' if state=='unmatched' else '历史暂未出现或未能唯一匹配，稍后只读补查'
        c.execute('UPDATE amazon_ads_history SET sync_status=%s,error_message=%s WHERE id IN ('+','.join(['%s']*len(unmatched))+') AND platform_time_ms IS NULL',('pending',note,*unmatched))
    save(c,job,status=state,page_no=0 if unmatched else job['page_no'],attempts=attempts,next_attempt_ms=due,
         work_json=job['work'],error_message=note,saved_count=job['saved_count']+saved)
    return saved


def fail(ctx,job,error):
    """单查询失败只影响本任务；只有真实限流/网络/授权异常才建立账户冷却。"""
    status=error.response.status_code if isinstance(error,requests.HTTPError) and error.response is not None else None
    note=('Amazon历史限流，已保留页码等待重试' if status==429 else '广告历史授权不可用，请核对账户' if status in (401,403)
          else str(error) if isinstance(error,repo.HistoryError) else f'历史接口HTTP {status}，原事件和页码保留' if status
          else '历史接口网络超时，等待只读重试' if isinstance(error,requests.Timeout)
          else '历史接口网络连接异常，等待只读重试' if isinstance(error,requests.ConnectionError)
          else f'历史查询异常（{type(error).__name__}），原事件和页码保留')[:500]
    clock=repo.now_ms();attempts=job['attempts']+1;delay=min(3600000,300000*2**min(attempts-1,4))
    if status==429:
        value=error.response.headers.get('Retry-After','')
        try:delay=max(delay,int(value)*1000 if str(value).isdigit() else int(parsedate_to_datetime(value).timestamp()*1000)-clock)
        except (ValueError,TypeError,OverflowError):pass
    with repo.transaction() as c:
        # 响应处理事务可能回滚；不能把内存里已推进的页码/摘要当作落库检查点。
        c.execute('SELECT work_json FROM amazon_ads_history_jobs WHERE id=%s FOR UPDATE',(job['id'],))
        persisted=c.fetchone()
        work=repo.loads(persisted['work_json'])
        if status:
            # 仅保留平台排障白名单响应头，不保存授权、Cookie或完整响应正文。
            allowed=('x-amzn-RequestId','x-amzn-ErrorType','amazon-advertising-api-throttled','Retry-After','Date')
            work['last_error']={'status':status,'at_ms':clock,'headers':{key:str(error.response.headers[key])[:200] for key in allowed if key in error.response.headers}}
        save(c,job,status='failed' if attempts>=6 else 'retry',attempts=attempts,next_attempt_ms=clock+delay,error_message=note,work_json=work)
        ids=job['work'].get('history_ids',[])
        if ids:c.execute('UPDATE amazon_ads_history SET sync_status=%s,error_message=%s WHERE id IN ('+','.join(['%s']*len(ids))+') AND platform_time_ms IS NULL',('failed' if attempts>=6 else 'pending',note,*ids))
        if status in (401,403,429) or isinstance(error,requests.RequestException):
            root=repo.ensure_state(c,jobs.root_context(ctx))
            c.execute('UPDATE amazon_ads_history_sync SET next_retry_ms=%s,failures=failures+1,error_message=%s WHERE id=%s',
                      (max(root['next_retry_ms'],clock+delay),note,root['id']))
        else:
            root=repo.ensure_state(c,jobs.root_context(ctx))
            c.execute('UPDATE amazon_ads_history_sync SET next_retry_ms=GREATEST(next_retry_ms,%s) WHERE id=%s',(clock+2000,root['id']))
    return status in (401,403,429) or isinstance(error,requests.RequestException)


def run_account(ctx,budget=90):
    """同账户单消费者，有界请求/时间预算，分页事务不跨网络；缺表时明确阻止新采集。"""
    lock_conn=repo.get_db_connection();acquired=False;calls=saved=merged=0;errors=0;deadline=time.monotonic()+budget;client=None
    lock_name='ads_hist_'+repo.digest([ctx['region'],ctx['profile_id']])[:40]
    try:
        lock_conn.autocommit(True)
        with lock_conn.cursor() as c:c.execute('SELECT GET_LOCK(%s,0) acquired',(lock_name,));acquired=c.fetchone()['acquired']==1
        if not acquired:return {'status':'pending','message':'同账户历史消费者正在处理，本轮无外部请求','calls':0}
        with repo.transaction() as c:
            jobs.prepare(c,ctx,repo.now_ms())
            merged=history.reconcile_saved(c,jobs.root_context(ctx))
        for _ in range(40):
            if calls>=MAX_CALLS or time.monotonic()>=deadline:break
            with repo.transaction() as c:
                root=repo.ensure_state(c,jobs.root_context(ctx));clock=repo.now_ms()
                if root['next_retry_ms']>clock:
                    if calls and root['next_retry_ms']-clock<=2000:wait=(root['next_retry_ms']-clock)/1000
                    else:return {'status':'pending','message':'账户冷却中，未请求历史接口','next_retry_ms':root['next_retry_ms'],'calls':calls,'saved':saved,'reconciled':merged}
                else:wait=0
            if wait:time.sleep(wait)
            with repo.transaction() as c:
                job=select_job(c,ctx,calls,repo.now_ms())
                if not job:break
                scoped(ctx,job)
                merge_changes(c,ctx,job,repo.now_ms())
                if trim_expired(c,job,repo.now_ms()):finalize_batch(c,ctx,job);continue
                if job['lane']=='erp' and not remaining(c,job):
                    save(c,job,status='completed',error_message='已由本地平台证据关联，无需再次请求');continue
                if job['entity_type']!='CATALOG' and not job['campaign_id']:
                    save(c,job,status='unmatched',error_message='原请求缺少可靠活动归属，未扩大成全账户查询');continue
                root=repo.ensure_state(c,jobs.root_context(ctx))
                c.execute('UPDATE amazon_ads_history_sync SET last_attempt_ms=%s WHERE id=%s',(repo.now_ms(),root['id']))
            try:
                latest=repo.account(ctx['shop_id']);scoped(latest,job)
                if client is None:client=get_ads_api_client(ctx['shop_id'])
                if str(client.profile_id)!=ctx['profile_id'] or client.region!=ctx['region']:raise repo.HistoryError('采集客户端身份与队列不符',409)
                calls+=1
                if job['entity_type']=='CATALOG':
                    result=client.list_sp_campaigns(state_filter=['ENABLED','PAUSED','ARCHIVED'],max_results=100,next_token=job['work'].get('token'))
                else:
                    spec=history.event_types(scoped(ctx,job))[job['entity_type']]
                    if job['work'].get('entity_ids'):spec={**spec,'eventTypeIds':job['work']['entity_ids']}
                    job['work'].setdefault('query_to',min(job['to_ms'],repo.now_ms()))
                    result=client.get_change_history({job['entity_type']:spec},job['from_ms'],job['work']['query_to'],PAGE_SIZE,job['page_no'])
                with repo.transaction() as c:
                    if job['entity_type']=='CATALOG':catalog_result(c,ctx,job,result)
                    count=0 if job['entity_type']=='CATALOG' else history_result(c,ctx,job,result)
                    finalize_batch(c,ctx,job)
                    root=repo.ensure_state(c,jobs.root_context(ctx))
                    c.execute("UPDATE amazon_ads_history_sync SET next_retry_ms=%s,failures=0,error_message=IF(error_message LIKE %s OR error_message LIKE %s,'',error_message) WHERE id=%s",(repo.now_ms()+2000,'Amazon历史限流%','广告历史授权不可用%',root['id']))
                saved+=count
                print(f"历史只读任务#{job['id']} · {job['lane']} · 活动{job['campaign_id'] or '目录'} · {job['entity_type']} · 页{job['page_no']}已处理",flush=True)
            except Exception as error:
                errors+=1
                stop=fail(ctx,job,error)
                with repo.transaction() as c:
                    c.execute('SELECT error_message,next_attempt_ms FROM amazon_ads_history_jobs WHERE id=%s',(job['id'],))
                    failed=c.fetchone()
                print(f"历史只读任务#{job['id']} · {job['lane']} · 活动{job['campaign_id'] or '目录'} · {job['entity_type']}：{failed['error_message']}；下次到期UTC毫秒{failed['next_attempt_ms']}",flush=True)
                if stop:break
        return {'status':'failed' if errors else 'partial' if calls else 'idle','message':'部分历史查询需重试或核对，其他任务保留' if errors else '已处理到期队列；每日/首次和未匹配任务按原断点继续',
                'calls':calls,'saved':saved,'reconciled':merged}
    finally:
        if acquired:
            with lock_conn.cursor() as c:c.execute('SELECT RELEASE_LOCK(%s)',(lock_name,))
        lock_conn.close()


def run_due():
    """只消费启用且账户唯一的店铺；一个账户失败不能中断其他店铺。"""
    results=[]
    for shop in get_all_active_shops():
        if not shop.get('ads_profile_id'):continue
        try:results.append(run_account(repo.account(shop['id'])))
        except Exception as error:
            message=str(error) if isinstance(error,repo.HistoryError) else '历史队列未能处理，请核对建表SQL及任务日志'
            results.append({'status':'failed','message':message,'calls':0})
    return results
