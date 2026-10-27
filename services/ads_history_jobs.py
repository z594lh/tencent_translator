"""广告历史持久队列：仅登记只读查询，旧审计/回执是唯一操作来源，不重发广告写入。"""
import logging
from datetime import datetime,timezone,timedelta
import pymysql
from services import ads_history_repository as repo

DAY=86400000
SCHEMA=3
TERMINAL=('completed','partial','gap','merged')


def root_context(ctx):
    """保留完整账户身份，只把范围提升至当前账户，不跨店铺合并。"""
    return {**ctx,'scope':'shop','campaign_id':None,'ad_group_id':None}


def insert(c,ctx,lane,kind,lo,hi,batch,*,campaign=None,group=None,erp_id=None,work=None,due=0,key=None):
    """确定窗口和唯一源事件入队；重复登记不覆盖原页码、回执或失败证据。"""
    data=work or {}
    job_key=repo.digest(key or [batch,lane,campaign,group,kind,lo,hi])
    fields=('job_key','batch_key','erp_history_id','shop_id','credential_group_id','marketplace_id','region','profile_id',
            'lane','campaign_id','ad_group_id','entity_type','from_ms','to_ms','next_attempt_ms','work_json')
    values=(job_key,batch,erp_id,ctx['shop_id'],ctx['credential_group_id'],ctx['marketplace_id'],ctx['region'],str(ctx['profile_id']),
            lane,str(campaign) if campaign else None,str(group) if group else None,kind,lo,hi,due,repo.dumps(data))
    c.execute('INSERT IGNORE INTO amazon_ads_history_jobs ('+','.join(fields)+') VALUES ('+','.join(['%s']*len(fields))+')',values)
    return c.rowcount


def enqueue_events(c,ctx,ids):
    """与原审计同事务登记每条已接受/未知写入；新表缺失时仍保留回执，稍后由扫描补登记。"""
    if not ids:return 0
    try:
        c.execute('''SELECT id,shop_id,credential_group_id,marketplace_id,region,profile_id,campaign_id,ad_group_id,
            entity_type,entity_id,local_started_ms,local_finished_ms FROM amazon_ads_history
            WHERE id IN ('''+','.join(['%s']*len(ids))+''') AND source='ERP' AND platform_time_ms IS NULL
            AND write_status IN ('accepted','unknown') AND merged_into_id IS NULL''',tuple(ids))
        rows=c.fetchall();count=0
        for row in rows:
            if any(str(row[k] or '')!=str(ctx[k] or '') for k in ('shop_id','credential_group_id','marketplace_id','region','profile_id')):
                raise repo.HistoryError('历史队列的操作与账户身份不符，未跨账户登记',409)
            start=row['local_started_ms'];end=row['local_finished_ms'] or start
            if not start:raise repo.HistoryError('原广告操作缺少时间，不能猜测补采窗口',409)
            kind='PRODUCT_TARGETING' if row['entity_type']=='NEGATIVE_PRODUCT_TARGETING' else row['entity_type']
            count+=insert(c,ctx,'erp',kind,start-300000,end+300000,repo.digest(['erp',row['id']]),
                campaign=row['campaign_id'],group=row['ad_group_id'],erp_id=row['id'],due=end+120000,
                work={'history_ids':[row['id']],'entity_ids':[str(row['entity_id'])] if row['entity_id'] else []},key=['erp',row['id']])
        return count
    except pymysql.err.ProgrammingError as exc:
        if exc.args[0]!=1146:raise
        logging.getLogger(__name__).warning('历史采集队列表尚未安装；原广告回执保留，安装后按原审计补登记')
        return 0


def prepare(c,ctx,clock):
    """账号首次幂等转换旧断点，并从真实未关联审计补齐队列；不会调用任何外部接口。"""
    c.execute('SELECT id FROM amazon_ads_history_jobs LIMIT 0')
    root=repo.ensure_state(c,root_context(ctx));meta=repo.loads(root['work_json']) or {}
    if meta.get('schema')!=SCHEMA:
        meta={'schema':SCHEMA,'migrated_at_ms':clock,'legacy_checkpoint':meta,'legacy_error':root['error_message'],
              'initial_seeded':bool(root['deep_success_ms']),'initial_inherited_ms':root['deep_success_ms'],
              'daily_through_ms':root['last_success_ms'] or 0,'daily_day':None}
        # 过期属于旧编排本地错误，不能把它的退避继续当作Amazon限流；其他账户冷却保留。
        expired='90天保留期' in root['error_message']
        c.execute('''UPDATE amazon_ads_history_sync SET work_json=%s,status='pending',error_message='',
            failures=IF(%s,0,failures),next_retry_ms=IF(%s,0,next_retry_ms) WHERE id=%s''',
            (repo.dumps(meta),expired,expired,root['id']))
        c.execute('''UPDATE amazon_ads_history_sync SET status='pending',error_message='已转入精准补采队列，未重发广告修改'
            WHERE region=%s AND profile_id=%s AND scope<>'shop' AND status='failed' AND error_message LIKE %s''',
            (ctx['region'],ctx['profile_id'],'%90天保留期%'))
    c.execute('''SELECT h.id FROM amazon_ads_history h LEFT JOIN amazon_ads_history_jobs j
        ON j.region=h.region AND j.profile_id=h.profile_id AND j.erp_history_id=h.id
        WHERE h.region=%s AND h.profile_id=%s AND h.shop_id=%s AND h.source='ERP' AND h.platform_time_ms IS NULL
        AND h.merged_into_id IS NULL AND h.write_status IN ('accepted','unknown') AND j.id IS NULL
        ORDER BY h.local_started_ms DESC,h.id DESC LIMIT 100''',(ctx['region'],ctx['profile_id'],ctx['shop_id']))
    seeded=enqueue_events(c,ctx,[r['id'] for r in c.fetchall()])
    if seeded:
        c.execute('''UPDATE amazon_ads_history h JOIN amazon_ads_history_jobs j ON j.erp_history_id=h.id
            AND j.region=h.region AND j.profile_id=h.profile_id SET h.sync_status='pending',h.error_message=''
            WHERE h.region=%s AND h.profile_id=%s AND h.write_status IN ('accepted','unknown')
            AND h.platform_time_ms IS NULL AND h.error_message LIKE %s''',(ctx['region'],ctx['profile_id'],'%90天保留期%'))
    day=datetime.fromtimestamp(clock/1000,timezone.utc).astimezone(timezone(timedelta(hours=8))).date().isoformat()
    if not meta.get('initial_seeded'):
        batch=repo.digest([ctx['region'],ctx['profile_id'],'initial',clock])
        insert(c,ctx,'initial','CATALOG',clock-90*DAY+60000,clock,batch,work={'chunk_ms':7*DAY})
        meta['initial_seeded']=True
    if meta.get('daily_day')!=day:
        batch=repo.digest([ctx['region'],ctx['profile_id'],'daily',day])
        lo=min(clock-3*DAY,meta.get('daily_through_ms',0)-3600000) if meta.get('daily_through_ms') else clock-3*DAY
        insert(c,ctx,'daily','CATALOG',max(0,lo),clock,batch,work={'chunk_ms':3*DAY})
        meta['daily_day']=day
    c.execute('UPDATE amazon_ads_history_sync SET work_json=%s WHERE id=%s',(repo.dumps(meta),root['id']))
    return root,meta,seeded


def request_scope(ctx):
    """兼容人工入口只登记近72小时范围，Web不直接抓取历史；重复两分钟内请求幂等。"""
    clock=repo.now_ms()
    from services.ads_history_service import event_types
    with repo.transaction() as c:
        state=repo.ensure_state(c,ctx);repo.mark_dirty(c,ctx)
        where=' AND campaign_id=%s' if ctx['scope']!='shop' else ''
        args=[ctx['region'],ctx['profile_id']]
        if where:args.append(str(ctx['campaign_id']))
        if ctx['scope']=='group':where+=' AND ad_group_id=%s';args.append(str(ctx['ad_group_id']))
        c.execute("SELECT * FROM amazon_ads_history_jobs WHERE region=%s AND profile_id=%s AND status IN ('failed','unmatched')"+where+' ORDER BY id LIMIT 100 FOR UPDATE',args)
        for old in c.fetchall():
            work=repo.loads(old['work_json']);work['manual_retry_count']=work.get('manual_retry_count',0)+1
            work['previous_failure']={'at_ms':clock,'attempts':old['attempts'],'error':old['error_message']}
            work.pop('query_to',None);work.pop('page_hash',None)
            c.execute("UPDATE amazon_ads_history_jobs SET status='queued',attempts=0,page_no=0,next_attempt_ms=%s,work_json=%s,error_message='人工已加入补采队列' WHERE id=%s",(clock,repo.dumps(work),old['id']))
        if ctx['scope']=='shop':prepare(c,ctx,clock)
        else:
            batch=repo.digest(['manual',ctx['region'],ctx['profile_id'],ctx['scope'],state['scope_id'],clock//120000])
            for kind in event_types(ctx):
                ids=[ctx['campaign_id']] if kind=='CAMPAIGN' else [ctx['ad_group_id']] if kind=='AD_GROUP' and ctx['scope']=='group' else []
                c.execute('''SELECT id FROM amazon_ads_history_jobs WHERE region=%s AND profile_id=%s
                    AND lane='manual' AND campaign_id=%s AND ad_group_id<=>%s AND entity_type=%s
                    AND status IN ('queued','retry') LIMIT 1''',
                    (ctx['region'],ctx['profile_id'],str(ctx['campaign_id']),ctx.get('ad_group_id'),kind))
                if not c.fetchone():
                    insert(c,ctx,'manual',kind,clock-3*DAY,clock,batch,campaign=ctx['campaign_id'],group=ctx.get('ad_group_id'),work={'entity_ids':ids},key=[batch,kind])
    return {'status':'pending','saved':0,'reconciled':0,'message':'已加入历史补采队列，由计划任务只读查询；不会再次修改广告'}


def projection(c,ctx,clock):
    """页面只读账户队列与冷却摘要，不生成任务，不把局部失败冒充账号全部失败。"""
    try:
        c.execute('''SELECT lane,status,COUNT(*) n,MIN(next_attempt_ms) next_ms FROM amazon_ads_history_jobs
            WHERE region=%s AND profile_id=%s GROUP BY lane,status''',(ctx['region'],ctx['profile_id']))
        lanes=list(c.fetchall())
    except pymysql.err.ProgrammingError as exc:
        if exc.args[0]!=1146:raise
        return {'ready':False,'note':'新版历史采集队列表尚未安装，请先执行建表SQL；原历史仍可查看'}
    c.execute("SELECT work_json,next_retry_ms,error_message FROM amazon_ads_history_sync WHERE region=%s AND profile_id=%s AND scope='shop'",(ctx['region'],ctx['profile_id']))
    root=c.fetchone() or {};meta=repo.loads(root.get('work_json')) or {}
    c.execute('''SELECT id,error_message,work_json FROM amazon_ads_history_jobs WHERE region=%s AND profile_id=%s
        AND status IN ('retry','failed','unmatched') AND error_message<>'' ORDER BY updated_at DESC,id DESC LIMIT 1''',
        (ctx['region'],ctx['profile_id']))
    error=c.fetchone();recent_error=None
    if error:
        proof=(repo.loads(error['work_json']) or {}).get('last_error') or {}
        recent_error={'job_id':str(error['id']),'message':error['error_message'],'status':proof.get('status'),
                      'request_id':(proof.get('headers') or {}).get('x-amzn-RequestId')}
    return {'ready':True,'lanes':lanes,'schema':meta.get('schema'),'daily_through_ms':meta.get('daily_through_ms'),
            'recent_error':recent_error,
            'initial_inherited_ms':meta.get('initial_inherited_ms'),'initial_completed_ms':meta.get('initial_completed_ms'),'cooldown_until_ms':root.get('next_retry_ms') if (root.get('next_retry_ms') or 0)>clock else None,
            'note':'首次回填一次；每日登记近3天及停机缺口；修改按实体和时间窄窗补采。队列检查不等于每次请求Amazon。'}
