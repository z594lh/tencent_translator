"""绑定竞品冻结与只读工具；只查当前托管范围，不即时采集或修改平台。"""
from ..language import text as model_text

import json
from copy import deepcopy
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pymysql
from services import sellersprite_portal as portal
from services.listing_performance.common import DomainError
from ..metrics import digest
from . import data as tool_data

NOTE=model_text('competitors.71c17cf1e104')


def freeze(identity, now=None):
    """同一准备时刻冻结有效绑定及最近成功报告；跨SKU去重，保留来源归属和缺口。"""
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None:raise DomainError(model_text('competitors.ad1b0a4f43d1'),422)
    instant=now.astimezone(timezone.utc).replace(tzinfo=None);day=now.astimezone(ZoneInfo('Asia/Shanghai')).date()
    result={'status':'provided','as_of':now.isoformat(),'bound_count':0,'rows':[],'note':NOTE}
    if identity.get('marketplace_id')!=portal.US:
        return {**result,'status':'unavailable','reason':model_text('competitors.d0268a86a9b3')}
    try:
        with portal.session() as cur:
            cur.execute('''SELECT c.competitor_asin,l.sku FROM amazon_listing_competitors c
                JOIN amazon_listings l ON l.id=c.listing_id AND l.shop_id=c.shop_id
                    AND BINARY l.marketplace_id=BINARY c.marketplace_id AND BINARY l.asin=BINARY c.source_asin
                WHERE c.shop_id=%s AND c.marketplace_id=%s AND c.source_asin=%s AND l.is_deleted=0
                ORDER BY c.competitor_asin,l.sku LIMIT 1001''',(identity['id'],identity['marketplace_id'],identity['asin']))
            bindings=cur.fetchall()
            if len(bindings)>1000:return {**result,'status':'unavailable','reason':model_text('competitors.dd15cc3dd842')}
            by_asin={}
            for item in bindings:by_asin.setdefault(item['competitor_asin'],set()).add(item['sku'])
            result['bound_count']=len(by_asin)
            if not by_asin:return {**result,'note':model_text('competitors.740b12ecc35a', p0=NOTE)}
            placeholders=','.join(['%s']*len(by_asin));args=list(by_asin)
            cur.execute('''SELECT t.asin AS query_asin,t.id AS task_id,t.record_date,t.report_id,
                p.asin AS product_asin,p.raw_row_json,f.id AS file_id,f.sha256
                FROM sellersprite_competitor_tasks t
                JOIN sellersprite_competitor_files f ON f.id=t.latest_file_id AND f.task_id=t.id AND f.status='imported'
                JOIN sellersprite_competitor_products p ON p.file_id=f.id AND p.task_id=t.id
                WHERE t.marketplace='US' AND t.period='last30days' AND t.status='completed'
                AND p.asin=COALESCE(t.selected_asin,t.asin) AND t.asin IN ('''+placeholders+''')
                AND t.id=(SELECT n.id FROM sellersprite_competitor_tasks n
                    JOIN sellersprite_competitor_files nf ON nf.id=n.latest_file_id AND nf.task_id=n.id AND nf.status='imported'
                    JOIN sellersprite_competitor_products np ON np.file_id=nf.id AND np.task_id=n.id
                    WHERE n.asin=t.asin AND n.marketplace=t.marketplace AND n.period=t.period AND n.status='completed'
                        AND np.asin=COALESCE(n.selected_asin,n.asin) AND n.record_date<=%s AND nf.uploaded_at<=%s
                    ORDER BY n.record_date DESC,n.id DESC LIMIT 1)''',args+[day,instant])
            products={r['query_asin']:r for r in cur.fetchall()}
            cur.execute('''SELECT t.asin,t.record_date,t.status,t.error_message FROM sellersprite_competitor_tasks t
                WHERE t.marketplace='US' AND t.period='last30days' AND t.asin IN ('''+placeholders+''')
                AND t.id=(SELECT n.id FROM sellersprite_competitor_tasks n WHERE n.asin=t.asin
                    AND n.marketplace=t.marketplace AND n.period=t.period AND n.created_at<=%s AND n.record_date<=%s
                    ORDER BY n.record_date DESC,n.id DESC LIMIT 1)''',args+[instant,day])
            latest={r['asin']:{k:portal.iso(v) for k,v in r.items() if k!='asin'} for r in cur.fetchall()}
            for asin,skus in by_asin.items():
                row={'query_asin':asin,'bound_skus':sorted(skus),'latest_attempt':latest.get(asin),'status':'unavailable','product':None}
                product=products.get(asin)
                if product:
                    try:
                        fields=json.loads(product['raw_row_json'])
                        if not isinstance(fields,dict) or fields.get('ASIN')!=product['product_asin']:raise ValueError()
                        row.update(status='provided',evidence_id=f"competitor:{asin}:{product['file_id']}",
                            product={'asin':product['product_asin'],'record_date':portal.iso(product['record_date']),
                                'task_id':str(product['task_id']),'file_id':str(product['file_id']),'report_id':product['report_id'],
                                'sha256':product['sha256'],'currency':'USD','period':'last30days','report_fields':fields})
                    except (ValueError,TypeError):row['reason']=model_text('competitors.eb262f3860c2')
                else:row['reason']=model_text('competitors.615b40dff70e')
                result['rows'].append(row)
    except (portal.PortalError,pymysql.MySQLError) as exc:
        if isinstance(exc,portal.PortalError) and exc.code==503 or isinstance(exc,pymysql.MySQLError) and exc.args[0] in (1146,1054):
            return {**result,'status':'unavailable','rows':[],'reason':model_text('competitors.c0be499f4096')}
        raise
    return result


def attach(dataset,now=None):
    """加入当前任务资料及证据索引；不把资料自动发送给模型或加入长期问答。"""
    value=freeze(dataset['content']['identity'],now);dataset['content']['competitors']=value
    for row in value['rows']:
        if row.get('evidence_id'):
            ref=row['evidence_id'];label=row['query_asin']+' · 绑定竞品报告'
            dataset['evidence'][ref]={'kind':'competitor','label':label,'data':row}
            dataset['content']['evidence_index'][ref]={'kind':'competitor','label':label}


def read(ctx,args):
    """只读冻结绑定集合；ref不能扩大范围，完整行按现有字节预算分页。"""
    if args['window']!='all':raise DomainError(model_text('competitors.12b159e28981'),422)
    frozen=ctx.data['content'].get('competitors')
    if frozen is None:return {'status':'unavailable','reason':model_text('competitors.03db5573c1f5'),'rows':[]}
    if frozen['status']!='provided':return deepcopy(frozen)
    rows=[r for r in frozen['rows'] if not args['ref'] or args['ref'] in (r['query_asin'],r.get('evidence_id'))]
    selected=deepcopy(rows[args['offset']:args['offset']+args['limit']])
    fitted=tool_data.fit(selected,max(0,getattr(ctx,'output_budget',tool_data.MAX_DATA_BYTES)-4000))
    end=args['offset']+len(fitted)
    result={'status':'provided','rows':fitted,'bound_count':frozen['bound_count'],'as_of':frozen['as_of'],
        'total_in_archive':len(rows),'returned':len(fitted),'offset':args['offset'],'requested_limit':args['limit'],
        'has_more':end<len(rows),'next_offset':end if fitted and end<len(rows) else None,
        'byte_limited':len(fitted)<len(selected),'note':frozen['note']}
    if fitted and result['has_more']:result['status']='partial'
    if selected and not fitted:result.update(status='budget_exceeded',reason=model_text('competitors.b85f498e1cb6'))
    if not rows and not args['ref']:result.update(status='empty',reason=model_text('competitors.c8a52021ba87'))
    if args['ref'] and not rows:result.update(status='unavailable',reason=model_text('competitors.712493b44dd7'))
    result['data_hash']=digest(result);ctx.capture(result)
    return result
