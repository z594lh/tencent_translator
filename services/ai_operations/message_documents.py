"""不可变附件和实际请求绑定；事件、资料及绑定必须在调用AI之前同事务落库。"""
import hashlib
import json
from services.listing_performance.common import DomainError, positive_id
from .metrics import dumps
from . import plan_store

LABELS={'listing':'当前Listing','pricing':'售价与成本','business':'SKU经营汇总','ads':'广告汇总',
        'keywords':'卖家精灵关键词','management_context':'执行账本与观察','conversation':'会话记忆与近期结论',
        'reading_guide':'数据阅读说明','action_catalog':'行动对象与前值','data_access':'取数白名单说明',
        'data_responses':'本轮补充资料','task_control':'本轮收尾控制','validation_feedback':'方案纠错要求',
        'recent_operations':'ERP操作预览','human_sender':'本次提问人及时间'}


def content_hash(text):
    """对附件准确文本计算摘要，下载文本可独立复核。"""
    return hashlib.sha256(text.encode('utf8')).hexdigest()


def parts(messages):
    """按真实messages索引和JSON指针拆附件，绝不按文件名猜测发送范围。"""
    for index,message in enumerate(messages):
        text=message['content']
        try:value=json.loads(text)
        except (ValueError,TypeError):value=None
        if isinstance(value,dict):
            for key,item in value.items():
                pointer='/'+key.replace('~','~0').replace('/','~1')
                kind='history' if message['role']=='assistant' or key in ('conversation','management_context') else 'rule' if key in ('task_control','validation_feedback','reading_guide','data_access','delivery_policy','context_policy') else 'data'
                name=('AI历史回答·' if message['role']=='assistant' else '')+LABELS.get(key,key)+'.json'
                yield index,pointer,name,kind,dumps(item),'application/json'
        else:
            kind='rule' if message['role']=='system' else 'message'
            yield index,'','固定协议与Listing规范.txt' if kind=='rule' else ('AI原回答.txt' if message['role']=='assistant' else 'ERP问题.txt'),kind,text,'text/plain'


def register(cur, message_id, session_id, payload, archives):
    """在请求事件同一事务内保存不可变资料和范围绑定，任意失败均阻止HTTP调用。"""
    cur.execute('SELECT scope_key FROM ai_operation_conversations WHERE id=%s',(session_id,))
    session=cur.fetchone()
    if not session or not session['scope_key']:raise DomainError('附件缺少明确账户范围，未调用AI',503)
    metadata=payload.get('attachment_manifest',{})
    rows=[(*row,'included') for row in parts(payload['messages'])]
    rows.extend((2,'/archive/'+key,LABELS.get(key,key)+'·归档完整模块.json','data',dumps(value),'application/json','archived') for key,value in archives.items())
    for index,pointer,name,kind,text,mime,delivery in rows:
        hashed=content_hash(text)
        cur.execute('''INSERT INTO ai_operation_documents
            (scope_key,content_hash,kind,name,mime_type,content_text,byte_count,metadata_json,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))
            ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)''',
            (session['scope_key'],hashed,kind,name,mime,text,len(text.encode('utf8')),dumps({'format':'interaction-document-v1'})))
        document_id=cur.lastrowid
        cur.execute('''INSERT INTO ai_operation_message_documents
            (message_id,document_id,message_index,json_pointer,delivery,metadata_json) VALUES (%s,%s,%s,%s,%s,%s)''',
            (message_id,document_id,index,pointer,delivery,dumps({**metadata,'name':name})))


def listing(cur, message_id):
    """在消息分页的现有只读事务中取附件目录，不加载附件正文。"""
    cur.execute('''SELECT b.id,b.document_id,b.message_index,b.json_pointer,b.delivery,b.metadata_json,d.name,d.kind,d.content_hash,d.byte_count,d.mime_type
        FROM ai_operation_message_documents b JOIN ai_operation_documents d ON d.id=b.document_id
        WHERE b.message_id=%s ORDER BY b.id''',(message_id,))
    result=[]
    for row in cur.fetchall():
        metadata=json.loads(row.pop('metadata_json'))
        result.append({**row,'id':str(row['id']),'document_id':str(row['document_id']),
                       'name':metadata.get('name',row['name']),'source':metadata})
    return result


def read(session_id, message_id, binding_id):
    """只按所属会话/消息读取已绑定原文；三层核对，禁止任意文件路径与跨账户下载。"""
    from .conversation_queries import require_session
    session,_=require_session(session_id)
    with plan_store.transaction() as cur:
        cur.execute('''SELECT d.*,b.metadata_json binding_metadata,b.delivery,b.message_index,b.json_pointer
            FROM ai_operation_message_documents b JOIN ai_operation_documents d ON d.id=b.document_id
            JOIN ai_operation_messages m ON m.id=b.message_id
            WHERE b.id=%s AND m.id=%s AND m.conversation_id=%s AND d.scope_key=%s''',
            (positive_id(binding_id,'附件绑定'),positive_id(message_id,'消息'),session['id'],session['scope_key']))
        row=cur.fetchone()
    if not row:raise DomainError('此消息没有该附件',404)
    if content_hash(row['content_text'])!=row['content_hash']:raise DomainError('附件内容摘要不符，请核对存储',503)
    return {'name':json.loads(row['binding_metadata']).get('name',row['name']), 'kind':row['kind'],
            'text':row['content_text'],'hash':row['content_hash'],'byte_count':row['byte_count'],
            'delivery':row['delivery'],'source':json.loads(row['binding_metadata']),
            'message_index':row['message_index'],'json_pointer':row['json_pointer']}
