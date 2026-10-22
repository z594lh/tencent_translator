"""免费只读请求预览：共用真实资料与编译器，不开会话、不保存任务、不调用模型。"""
from datetime import datetime, timezone, timedelta
import re
from services.listing_performance.common import DomainError
from . import conversation_queries, conversation_store, plans, prompt_configuration, prompt_compiler, conversation_context, message_documents
from .metrics import digest


def preview(body):
    """基于当前真实托管范围预览草稿或发布版；不存在历史时明确标记虚拟会话编号。"""
    if not isinstance(body,dict) or not {'managed_id','report_date','config'}<=set(body) or set(body)-{'managed_id','report_date','config','mode','question'}:
        raise DomainError('请选择托管商品、截止日和预览配置')
    mode=body.get('mode','daily');question=body.get('question','')
    if not isinstance(mode,str) or mode not in ('daily','discussion','proposal'):raise DomainError('预览模式无效')
    if mode!='daily' and (not isinstance(question,str) or not question.strip() or len(question)>4000):raise DomainError('请填写预览用的人工问题，最多4000字符')
    if not isinstance(body['report_date'],str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',body['report_date']):raise DomainError('请明确选择报表截止日')
    managed,identity=conversation_queries.managed_identity(body['managed_id'])
    snapshot=prompt_configuration.active()
    if body['config'] is not None:
        settings=prompt_configuration.validate(body['config'])
        snapshot={'version_id':None,'config':settings,'hash':digest(settings),'source':'草稿预览，未发布'}
    today=datetime.now(timezone(timedelta(hours=8))).date()
    pack,data=plans.build_data(managed['shop_id'],managed['asin'],body['report_date'],
        {'managed_id':managed['id'],'run_id':0,'run_date':today,'now':datetime.now(timezone.utc)})
    if conversation_store.scope(identity)[1]!=conversation_store.scope(data['content']['identity'])[1]:raise DomainError('预览期间账户配置变化，请刷新后重试',409)
    session=conversation_store.peek_session(data['content']['identity'])
    prepared=prompt_compiler.prepare(data,session,snapshot) if mode=='daily' else prompt_compiler.prepare_human(data,session,snapshot,question,mode)
    if mode!='daily':prepared['human_actor']={'source':'免费预览，实际运行附带真实提问人及时间'}
    calls=prepared['conversation'].get('max_calls',3)
    allowed=prompt_compiler.allow_data(prepared,[],calls,0,0)
    messages,control=prompt_compiler.messages(prepared,[],calls,allowed)
    byte_count=conversation_context.size(messages)
    if byte_count>conversation_context.MAX_CONTEXT_BYTES:raise DomainError('编译后超过实际请求长度上限，请缩短话术或调整资料策略；未调用AI',422)
    files=[{'message_index':i,'json_pointer':ptr,'name':name,'kind':kind,'byte_count':len(text.encode('utf8')),
            'hash':message_documents.content_hash(text),'delivery':'included'} for i,ptr,name,kind,text,mime in message_documents.parts(messages,mode)]
    return {'messages':messages,'request':prepared['request'],'interaction':{k:v for k,v in prepared['interaction'].items() if k!='config'},
            'manifest':prompt_compiler.manifest(prepared,data),'documents':files,'input_bytes':byte_count,
            'messages_hash':digest(messages),'control':control,'preview_only':True,
            'virtual_session':bool(session.get('preview_only')),
            'note':'首轮只读免费预览；发送时重新冻结当天数据，因此请求摘要可能变化。后续补资料/纠错取决于AI实际回答，执行后从消息附件核对。没有会话时编号为null，仅正式任务创建。字节数不是token或费用承诺。'}
