"""查竞品配置、领取和单商品报告入库；与关键词数据表隔离，共用机器鉴权。"""
import hashlib
import hmac
import json
import re
import secrets
import tempfile
import os
from datetime import timedelta
from pathlib import Path

from services import sellersprite_portal as portal
from services.mysql_service import get_db_connection
from services.sellersprite_common import PipelineError, record_date, utcnow, storage_root, MAX_FILE_SIZE
from services.sellersprite_competitor_excel import parse_report,parse_reports
from services.sellersprite_excel_common import ExcelValidationError


def root_path():
    return storage_root()/'competitors'


def config_list(params):
    page=portal.integer(params.get('page',1),'页码')
    size=portal.integer(params.get('page_size',20),'每页数量',1,100)
    search=str(params.get('search') or '').strip()
    if len(search)>100: raise PipelineError('搜索词过长')
    where,args="c.marketplace='US'",[]
    if search:
        where+=' AND (CONVERT(c.asin USING utf8mb4) LIKE %s OR c.remark LIKE %s)';args+=['%'+search+'%']*2
    if params.get('enabled') not in (None,''):
        where+=' AND c.enabled=%s';args.append(portal.integer(params['enabled'],'启用状态',0,1))
    with portal.session() as cur:
        cur.execute('SELECT COUNT(*) AS n FROM sellersprite_competitor_config c WHERE '+where,args)
        total=cur.fetchone()['n']
        cur.execute('''SELECT c.*,(SELECT t.id FROM sellersprite_competitor_tasks t WHERE t.asin=c.asin
            AND t.marketplace=c.marketplace ORDER BY t.record_date DESC,t.id DESC LIMIT 1) AS latest_task_id
            FROM sellersprite_competitor_config c WHERE '''+where+' ORDER BY c.priority DESC,c.id DESC LIMIT %s OFFSET %s',args+[size,(page-1)*size])
        rows=[{k:portal.iso(v) for k,v in r.items()} for r in cur.fetchall()]
        for row in rows:
            row['latest_status']=None
            if row['latest_task_id']:
                cur.execute('SELECT status,record_date,error_message,row_count FROM sellersprite_competitor_tasks WHERE id=%s',(row['latest_task_id'],))
                task=cur.fetchone();row.update(latest_status=task['status'],latest_date=portal.iso(task['record_date']),latest_error=task['error_message'])
        portal.config_identities(cur,rows)
        return {'list':rows,'total':total,'page':page}


def add_config(data,actor):
    """配置页批量添加竞品采集目标，独立提交并保持已有配置。"""
    with portal.session(write=True) as cur:
        return add_config_in_session(cur,data,actor)


def add_config_in_session(cur,data,actor):
    """复用竞品批量规则，在调用方事务内添加目标，供Listing绑定原子保存。"""
    raw=data.get('asins')
    if isinstance(raw,str): raw=re.split(r'[\s,，;；]+',raw.strip())
    if not isinstance(raw,list) or not 1<=len(raw)<=200: raise PipelineError('每次添加1～200个ASIN')
    asins=list(dict.fromkeys(portal.asin_value(v) for v in raw))
    enabled,priority,remark=portal.fields(data);now=utcnow();created=0
    for asin in asins:
        cur.execute('''INSERT INTO sellersprite_competitor_config
            (asin,marketplace,enabled,priority,remark,updated_by,updated_by_name,created_at,updated_at)
            VALUES (%s,'US',%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE id=id''',
            (asin,enabled,priority,remark,actor['id'],(actor.get('nickname') or actor['username'])[:255],now,now))
        created+=int(cur.rowcount==1)
    return {'created':created,'existing':len(asins)-created}


def update_config(identifier,data,actor):
    version=portal.integer(data.get('version'),'配置版本');enabled,priority,remark=portal.fields(data)
    with portal.session(write=True) as cur:
        cur.execute('''UPDATE sellersprite_competitor_config SET enabled=%s,priority=%s,remark=%s,
            updated_by=%s,updated_by_name=%s,updated_at=%s,version=version+1 WHERE id=%s AND version=%s''',
            (enabled,priority,remark,actor['id'],(actor.get('nickname') or actor['username'])[:255],utcnow(),identifier,version))
        if cur.rowcount!=1: raise PipelineError('配置不存在或已被修改，请刷新',409)
    return {'id':identifier,'version':version+1}


def candidates():
    with portal.session() as cur:
        cur.execute("SELECT asin FROM sellersprite_competitor_config WHERE marketplace='US' AND enabled=1 ORDER BY priority DESC,id LIMIT 10001")
        rows=cur.fetchall()
        if len(rows)>10000: raise PipelineError('启用ASIN超过10000',422)
        portal.config_identities(cur,rows)
        return {r['asin']:[{k:p.get(k) for k in ('shop_id','sku')} for p in r.get('related_products',[])] for r in rows}


def pending(day=None,limit=50):
    day=record_date(day);eligible=candidates();now=utcnow()
    with portal.session() as cur:
        cur.execute("SELECT asin,status,lease_until FROM sellersprite_competitor_tasks WHERE record_date=%s AND marketplace='US'",(day,))
        existing={r['asin']:r for r in cur.fetchall()}
    items=[]
    for asin,sources in eligible.items():
        task=existing.get(asin)
        if task and (task['status']=='completed' or (task['status'] in ('running','downloaded') and task['lease_until'] and task['lease_until']>now)): continue
        items.append({'asin':asin,'marketplace':'US','record_date':day.isoformat(),'sources':sources})
    return items[:limit],len(items)


def claim(asin,day,worker):
    asin=portal.asin_value(asin);day=record_date(day);worker=str(worker or '').strip()
    if not worker or len(worker)>100: raise PipelineError('worker_id无效')
    eligible=candidates();conn=get_db_connection();now=utcnow()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM sellersprite_competitor_tasks WHERE asin=%s AND record_date=%s AND marketplace='US'",(asin,day))
            prior=cur.fetchone()
            if asin not in eligible and not prior: raise PipelineError('ASIN不在查竞品采集清单',404)
            cur.execute("""INSERT INTO sellersprite_competitor_tasks
                (asin,marketplace,record_date,period,status,sources_json,created_at,updated_at)
                VALUES (%s,'US',%s,'last30days','pending',%s,%s,%s) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
                (asin,day,json.dumps(eligible.get(asin,[]),ensure_ascii=False),now,now))
            task_id=cur.lastrowid
            cur.execute('SELECT * FROM sellersprite_competitor_tasks WHERE id=%s FOR UPDATE',(task_id,));task=cur.fetchone()
            if task['status']=='completed':
                conn.commit();return {'id':task_id,'asin':asin,'status':'completed','row_count':task['row_count']}
            if task['status'] in ('running','downloaded') and task['lease_until'] and task['lease_until']>now:
                raise PipelineError('任务已被领取',409)
            token=secrets.token_hex(32)
            cur.execute("""UPDATE sellersprite_competitor_tasks SET status='running',claim_token=%s,worker_id=%s,
                lease_until=%s,attempts=attempts+1,error_message=NULL,started_at=%s,updated_at=%s WHERE id=%s""",
                (token,worker,now+timedelta(hours=2),now,now,task_id))
        conn.commit()
        return {'id':task_id,'asin':asin,'marketplace':'US','record_date':day.isoformat(),'status':'running','claim_token':token}
    except Exception:
        conn.rollback();raise
    finally:conn.close()


def locked(cur,identifier,token,completed=False):
    cur.execute('SELECT * FROM sellersprite_competitor_tasks WHERE id=%s FOR UPDATE',(identifier,));task=cur.fetchone()
    if not task: raise PipelineError('任务不存在',404)
    if not token or not hmac.compare_digest(str(task['claim_token'] or '').encode(),str(token).encode()): raise PipelineError('任务令牌不匹配',409)
    if completed and task['status']=='completed': return task
    if task['status'] not in ('running','downloaded') or not task['lease_until'] or task['lease_until']<utcnow(): raise PipelineError('任务租约失效',409)
    return task


def progress(identifier,token,data):
    status=data.get('status')
    if status not in ('running','downloaded','failed','action_required'):raise PipelineError('进度状态无效')
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            task=locked(cur,identifier,token)
            path=str(data.get('local_path') or task['local_path'] or '')[:1000] or None
            name=str(data.get('file_name') or task['file_name'] or '')[:255] or None
            if status=='downloaded' and (not path or not name): raise PipelineError('缺少下载文件路径或名称')
            cur.execute('''UPDATE sellersprite_competitor_tasks SET status=%s,local_path=%s,file_name=%s,
                lease_until=%s,error_message=%s,updated_at=%s WHERE id=%s''',
                (status,path,name,utcnow()+timedelta(hours=2) if status in ('running','downloaded') else None,
                 str(data.get('error_message') or '')[:1000] or None,utcnow(),identifier))
        conn.commit();return {'id':identifier,'status':status}
    except Exception:conn.rollback();raise
    finally:conn.close()


def receive(identifier,token,upload,sha,selected,report_id,batch_asins=None,query_asins=None,proof=None):
    selected=portal.asin_value(selected)
    if not re.fullmatch('[a-f0-9]{64}',str(sha or '')):raise PipelineError('sha256无效')
    if not re.fullmatch('[0-9]{1,64}',str(report_id or '')):raise PipelineError('必须提供网站报告编号')
    if not upload.filename.lower().endswith('.xlsx'):raise PipelineError('仅支持普通导出的xlsx')
    if batch_asins is not None:
        if not isinstance(batch_asins,list) or not 1<=len(batch_asins)<=60 or len(set(batch_asins))!=len(batch_asins):raise PipelineError('批量勾选集合无效')
        batch_asins=[portal.asin_value(a) for a in batch_asins]
        if not isinstance(query_asins,list) or len(query_asins)!=len(batch_asins) or len(set(query_asins))!=len(query_asins):raise PipelineError('批量查询集合无效')
        query_asins=[portal.asin_value(a) for a in query_asins]
        if selected not in batch_asins:raise PipelineError('选中ASIN不属于报告批次')
    root=root_path();root.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(dir=root,suffix='.xlsx');temporary=Path(name);conn=None
    try:
        digest=hashlib.sha256();size=0
        with os.fdopen(fd,'wb') as f:
            while True:
                chunk=upload.stream.read(1024*1024)
                if not chunk:break
                size+=len(chunk)
                if size>MAX_FILE_SIZE:raise PipelineError('报告超过20MB',413)
                digest.update(chunk);f.write(chunk)
        if not hmac.compare_digest(digest.hexdigest(),sha):raise PipelineError('SHA256不一致')
        conn=get_db_connection()
        with conn.cursor() as cur:
            task=locked(cur,identifier,token,completed=True)
            if batch_asins is not None and (task['asin'] not in query_asins or (len(batch_asins)>1 and (set(batch_asins)!=set(query_asins) or selected!=task['asin']))):raise PipelineError('批量报告与当前ASIN任务不对应')
            cur.execute('SELECT id,status,row_count,error_message,metadata_json FROM sellersprite_competitor_files WHERE task_id=%s AND sha256=%s',(identifier,sha));prior=cur.fetchone()
            if prior:
                if task.get('selected_asin')!=selected:raise PipelineError('重复上传选择身份不一致',409)
                stored=json.loads(prior.pop('metadata_json') or '{}')
                if batch_asins is not None and (set(stored.get('selected_asins') or [stored.get('selected_asin')])!=set(batch_asins) or set(stored.get('query_asins') or [stored.get('query_asin')])!=set(query_asins)):
                    raise PipelineError('重复上传批次清单不一致',409)
                status='completed' if prior['status']=='imported' else 'parse_failed'
                cur.execute('UPDATE sellersprite_competitor_tasks SET status=%s,latest_file_id=%s,row_count=%s,error_message=%s,lease_until=NULL,updated_at=%s WHERE id=%s',
                    (status,prior['id'],prior['row_count'],prior['error_message'],utcnow(),identifier))
                conn.commit();return {**prior,'file_id':prior['id'],'duplicate':True}
            if task['status']=='completed':raise PipelineError('任务已完成，不允许覆盖另一份报告',409)
            try:
                if batch_asins is None:product,metadata=parse_report(temporary,selected)
                else:
                    products,metadata=parse_reports(temporary,batch_asins)
                    product=products[selected]
                error=None
            except Exception as exc:
                product=None;metadata={};error=str(exc)[:1000] if isinstance(exc,ExcelValidationError) else 'Excel解析失败：'+type(exc).__name__
            # 同一批报告只保存一份物理原件；各ASIN保留独立任务、文件引用和对应商品行。
            relative=Path(task['record_date'].isoformat())/(f'batch_{sha}.xlsx' if batch_asins is not None else f'{identifier}_{sha}.xlsx')
            dest=root/relative;dest.parent.mkdir(parents=True,exist_ok=True)
            if not dest.exists():temporary.replace(dest)
            filename=f"Competitor_Batch_US_{len(batch_asins)}asins_{task['record_date']}_{sha[:10]}.xlsx" if batch_asins is not None else f"Competitor_US_{task['asin']}_{task['record_date']}.xlsx"
            now=utcnow()
            metadata.update(query_asin=task['asin'],selected_asin=selected,report_id=str(report_id))
            if batch_asins is not None:metadata.update(query_asins=query_asins,report_row_count=len(batch_asins))
            if proof:
                method=proof.get('identity_method')
                fingerprint=proof.get('request_fingerprint')
                if method in ('unique-new-report-content','pinned-report-content'):metadata['identity_method']=method
                if re.fullmatch('[a-f0-9]{64}',str(fingerprint or '')):metadata['request_fingerprint']=fingerprint
            cur.execute('''INSERT INTO sellersprite_competitor_files
                (task_id,sha256,file_name,storage_path,byte_size,status,parser_version,row_count,metadata_json,error_message,uploaded_at,imported_at)
                VALUES (%s,%s,%s,%s,%s,%s,'1',%s,%s,%s,%s,%s)''',
                (identifier,sha,filename,relative.as_posix(),size,'parse_failed' if error else 'imported',0 if error else 1,json.dumps(metadata,ensure_ascii=False),error,now,None if error else now))
            file_id=cur.lastrowid
            if product:
                columns=['file_id','task_id']+list(product)
                cur.execute('INSERT INTO sellersprite_competitor_products ('+','.join(columns)+') VALUES ('+','.join(['%s']*len(columns))+')',[file_id,identifier]+list(product.values()))
            cur.execute('''UPDATE sellersprite_competitor_tasks SET status=%s,selected_asin=%s,report_id=%s,latest_file_id=%s,
                file_name=%s,row_count=%s,error_message=%s,lease_until=NULL,completed_at=%s,updated_at=%s WHERE id=%s''',
                ('parse_failed' if error else 'completed',selected,str(report_id),file_id,filename,0 if error else 1,error,None if error else now,now,identifier))
        conn.commit();return {'file_id':file_id,'status':'parse_failed' if error else 'imported','row_count':0 if error else 1,'error_message':error}
    except Exception:
        if conn:conn.rollback()
        raise
    finally:
        temporary.unlink(missing_ok=True)
        if conn:conn.close()


def history(asin,page=1):
    asin=portal.asin_value(asin);page=portal.integer(page,'页码')
    with portal.session() as cur:
        cur.execute('SELECT COUNT(*) AS n FROM sellersprite_competitor_tasks WHERE asin=%s',(asin,));total=cur.fetchone()['n']
        cur.execute('''SELECT t.id,t.asin,t.record_date,t.status,t.selected_asin,t.report_id,t.file_name,t.row_count,
            t.error_message,t.attempts,t.local_path,t.latest_file_id,t.updated_at,f.storage_path,p.title,p.monthly_sales,
            p.monthly_revenue,p.price,p.bsr,p.ratings,p.rating,p.parent_asin
            FROM sellersprite_competitor_tasks t LEFT JOIN sellersprite_competitor_files f ON f.id=t.latest_file_id
            LEFT JOIN sellersprite_competitor_products p ON p.file_id=f.id WHERE t.asin=%s
            ORDER BY t.record_date DESC,t.id DESC LIMIT 20 OFFSET %s''',(asin,(page-1)*20))
        return {'list':[{k:portal.iso(v) for k,v in r.items()} for r in cur.fetchall()],'total':total,'page':page}


def product(identifier):
    with portal.session() as cur:
        cur.execute('''SELECT p.* FROM sellersprite_competitor_products p JOIN sellersprite_competitor_tasks t
            ON t.id=p.task_id AND t.latest_file_id=p.file_id WHERE t.id=%s AND t.status='completed' ''',(identifier,))
        row=cur.fetchone()
        if not row:raise PipelineError('没有成功商品快照',404)
        return {k:portal.iso(v) for k,v in row.items()}


def download(identifier):
    with portal.session() as cur:
        cur.execute('SELECT storage_path,file_name FROM sellersprite_competitor_files WHERE id=%s',(identifier,));row=cur.fetchone()
        if not row:raise PipelineError('文件不存在',404)
        root=root_path().resolve();path=(root/row['storage_path']).resolve()
        if not path.is_relative_to(root) or not path.is_file():raise PipelineError('归档文件不存在',404)
        return path,row['file_name']
