"""单一默认字典、数据库覆盖、缓存及审计；未知代码不伪造中文业务含义。"""
import json
import logging
import re
import threading
import time
from copy import deepcopy
from collections.abc import Mapping
from pathlib import Path
import pymysql
from services.mysql_service import get_db_connection
from services.listing_performance.common import DomainError, positive_id

DEFAULTS=json.loads(Path(__file__).with_name('defaults.json').read_text(encoding='utf8'))['groups']
CACHE_SECONDS=15
_lock=threading.RLock()
_cache=None
_expires=0
logger=logging.getLogger(__name__)


def canonical(group,code):
    """按固定注册别名定位标准代码；人工不能改变别名指向或API转换。"""
    spec=DEFAULTS.get(group)
    if not spec or not isinstance(code,str):return code
    normalize=(lambda value:value) if spec['case_sensitive'] else str.casefold
    target=normalize(code)
    for value in spec['labels']:
        if normalize(value)==target:return value
    for alias,value in spec['aliases'].items():
        if normalize(alias)==target:return value
    return code if spec['case_sensitive'] else code.upper()


def aliases(group):
    """提供只读别名副本给原业务适配，配置页面不允许编辑标准关联。"""
    return dict(DEFAULTS.get(group,{}).get('aliases',{}))


def base_entry(group,code):
    """组装默认元数据，不读取任何业务资料、账户或外部接口。"""
    spec=DEFAULTS[group];builtin=code in spec['labels']
    return {'group':group,'code':code,'label':spec['labels'].get(code),'description':spec['descriptions'].get(code,''),
            'default_label':spec['labels'].get(code),'default_description':spec['descriptions'].get(code,''),
            'builtin':builtin,'aliases':[a for a,v in spec['aliases'].items() if v==code],
            'format':spec.get('formats',{}).get(code,'text'),'default_value':spec.get('defaults',{}).get(code),
            'version':0,'updated_by_name':'','updated_at':None,'overridden':False}


def snapshot(force=False,strict=False,_internal=False):
    """每进程15秒缓存；缺表/故障时只读退回程序默认，管理保存则明确失败。"""
    global _cache,_expires
    with _lock:
        if not force and _cache is not None and time.monotonic()<_expires:
            if strict and not _cache['storage_ready']:raise DomainError(_cache['warning'],503)
            return _cache if _internal else deepcopy(_cache)
        rows=[];warning=''
        conn=None
        try:
            conn=get_db_connection()
            with conn.cursor() as cur:
                cur.execute('SELECT * FROM system_business_dictionary');rows=cur.fetchall()
        except pymysql.MySQLError as error:
            warning='业务字典覆盖暂不可用，当前使用程序默认名称；请核对本轮SQL和数据库状态。'
            logger.warning('业务字典读取降级：%s',type(error).__name__)
        finally:
            if conn:conn.close()
        entries={(g,c):base_entry(g,c) for g,s in DEFAULTS.items() for c in s['labels']}
        for row in rows:
            group,code=row['group_code'],row['item_code']
            if group not in DEFAULTS or canonical(group,code)!=code:continue
            item=entries.setdefault((group,code),base_entry(group,code))
            if row['label'] is not None:
                item.update(label=row['label'],description=row['description'] or '',overridden=True)
            item.update(version=int(row['version']),updated_by_name=row['updated_by_name'],
                        updated_at=row['updated_at'].isoformat()+'Z' if row['updated_at'] else None)
        result={'groups':[{'code':k,'name':v['name'],'case_sensitive':v['case_sensitive']} for k,v in DEFAULTS.items()],
                'list':[v for v in entries.values() if v['label'] is not None],
                'storage_ready':not warning,'warning':warning,'cache_seconds':CACHE_SECONDS}
        _cache=result;_expires=time.monotonic()+CACHE_SECONDS
        if strict and warning:raise DomainError(warning,503)
        return result if _internal else deepcopy(result)


def entry(group,code):
    """返回匹配代码的展示说明；未知字段为None，不把商品名当作枚举翻译。"""
    value=canonical(group,code)
    row=next((r for r in snapshot(_internal=True)['list'] if r['group']==group and r['code']==value),None)
    return deepcopy(row) if row else None


def label(group,code,fallback=None):
    """统一中文名称读取，未注册时保留调用方明确的原值或占位。"""
    row=entry(group,code)
    return row['label'] if row else (code if fallback is None else fallback)


def labels(group,builtin_only=False):
    """按组返回展示映射；用于业务允许列表时必须选builtin_only，不接纳人工补充代码。"""
    return {r['code']:r['label'] for r in snapshot(_internal=True)['list'] if r['group']==group and (not builtin_only or r['builtin'])}


class DictionaryMapping(Mapping):
    """为旧公共常量提供动态只读Mapping，消除前后端重复名称但保留原调用接口。"""
    def __init__(self,group,builtin_only=False):
        """绑定命名空间；内置枚举集合与可编辑展示标签分离。"""
        self.group=group;self.builtin_only=builtin_only

    def __getitem__(self,key):
        """通过标准别名读取当前名称；未知值仍遵循Mapping的KeyError约定。"""
        row=entry(self.group,key)
        if row is None or self.builtin_only and not row['builtin']:raise KeyError(key)
        return row['label']

    def __iter__(self):
        """只枚举标准词条，不把别名重复当作新业务对象。"""
        return iter(labels(self.group,self.builtin_only))

    def __len__(self):
        """返回本组展示词条数量，不代表报表数据行数。"""
        return len(labels(self.group,self.builtin_only))


def clean_text(value,name,limit,empty=False):
    """仅接收有界纯文本，不允许HTML或控制字符进入名称与说明。"""
    if not isinstance(value,str) or len(value)>limit or not empty and not value.strip() or '<' in value or '>' in value or any(ord(c)==127 or ord(c)<32 and not (empty and c in '\n\t') for c in value):
        raise DomainError(name+'必须为有效纯文本且不超过'+str(limit)+'字符',422)
    return value.strip()


def update(body,user):
    """CAS修改/补充或恢复默认，同事务记录前后快照，不允许改代码/别名/格式语义。"""
    if not isinstance(body,dict) or set(body)!={'group','code','label','description','version','reset'}:raise DomainError('字典提交字段无效',422)
    group,code=body['group'],body['code']
    # 固定报告表头可能含$、&或撇号；只对程序注册词条放行，人工新增仍沿原代码限制。
    if not isinstance(group,str) or group not in DEFAULTS or not isinstance(code,str) or not (code in DEFAULTS[group]['labels'] or re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.:/ ()-]{0,179}',code)):raise DomainError('请选择已注册分类并填写有效英文代码',422)
    code=canonical(group,code)
    if type(body['version']) is not int or not 0<=body['version']<2**63 or type(body['reset']) is not bool:raise DomainError('字典版本或操作无效',422)
    value=None if body['reset'] else clean_text(body['label'],'中文名称',100)
    description=None if body['reset'] else clean_text(body['description'],'解释',2000,True)
    operator=positive_id(user['id'],'操作人');operator_name=str(user.get('username') or user.get('nickname') or operator)[:100]
    conn=get_db_connection()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute('SELECT * FROM system_business_dictionary WHERE group_code=%s AND item_code=%s FOR UPDATE',(group,code));old=cur.fetchone()
            version=int(old['version']) if old else 0
            if version!=body['version']:raise DomainError('词条已被他人修改，请重新加载后再保存',409)
            before={'label':old['label'],'description':old['description']} if old else None
            after={'label':value,'description':description}
            # 补充项移除后也从审计续版本，防止删除重建后旧编辑版本再次命中。
            cur.execute('SELECT COALESCE(MAX(item_version),0) v FROM system_business_dictionary_history WHERE group_code=%s AND item_code=%s',(group,code))
            revision=max(version,int(cur.fetchone()['v']))+1
            if body['reset'] and code not in DEFAULTS[group]['labels']:
                if not old:raise DomainError('补充词条已不存在，请重新加载',404)
                cur.execute('DELETE FROM system_business_dictionary WHERE id=%s',(old['id'],))
            elif old:
                cur.execute('''UPDATE system_business_dictionary SET label=%s,description=%s,version=%s,
                    updated_by=%s,updated_by_name=%s,updated_at=UTC_TIMESTAMP(3) WHERE id=%s''',
                    (value,description,revision,operator,operator_name,old['id']))
            else:
                cur.execute('''INSERT INTO system_business_dictionary(group_code,item_code,label,description,version,updated_by,updated_by_name,updated_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',(group,code,value,description,revision,operator,operator_name))
            cur.execute('''INSERT INTO system_business_dictionary_history(group_code,item_code,item_version,action,before_json,after_json,operator_id,operator_name,created_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
                (group,code,revision,'reset' if body['reset'] else 'update',json.dumps(before,ensure_ascii=False),json.dumps(after,ensure_ascii=False),operator,operator_name))
        conn.commit()
    except pymysql.IntegrityError:conn.rollback();raise DomainError('字典并发修改冲突，请重新加载',409) from None
    except Exception:conn.rollback();raise
    finally:conn.close()
    return snapshot(force=True,strict=True)


def history(params):
    """分页只读修改历史，按词条定位，不加载订单、密钥或用户其他资料。"""
    if set(params)-{'group','code','page'}:raise DomainError('历史查询参数无效',422)
    page=positive_id(params.get('page',1),'页码')
    if page>100000:raise DomainError('页码过大',422)
    where=[];values=[]
    for name,column in (('group','group_code'),('code','item_code')):
        if params.get(name):where.append(column+'=%s');values.append(params[name])
    condition=' WHERE '+' AND '.join(where) if where else ''
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) total FROM system_business_dictionary_history'+condition,tuple(values));total=cur.fetchone()['total']
            cur.execute('SELECT * FROM system_business_dictionary_history'+condition+' ORDER BY id DESC LIMIT 10 OFFSET %s',tuple([*values,(page-1)*10]));rows=list(cur.fetchall())
    finally:conn.close()
    for row in rows:
        row['before']=json.loads(row.pop('before_json'));row['after']=json.loads(row.pop('after_json'))
        row['id']=str(row['id']);row['created_at']=row['created_at'].isoformat()+'Z'
    return {'list':rows,'total':total,'page':page}
