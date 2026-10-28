"""工具管理的人类可读说明；不改模型参数名或开放任意函数。"""
from .specs import READ_TOOLS
from services.business_dictionary import DictionaryMapping, entry

WINDOWS = DictionaryMapping('data.window')
REF_HELP = {
    'read_competitors':'留空列出当前ASIN绑定竞品；可填绑定竞品ASIN或本次返回的竞品证据引用。不能查询未绑定ASIN。仅window=all，数据时间见每条采集日期。',
    'read_advertising':'可填 spAdvertisedProduct（本ASIN商品广告）、spCampaigns（关联活动）、spCampaignsPlacement（广告位）、spTargeting（投放）、spSearchTerm（搜索词），或返回的精确证据编号。',
    'read_keywords':'输入完整关键词精确匹配，例如 egg slicer；留空列出当前快照。',
    'read_keyword_history':'输入完整关键词精确匹配；每行包含真实采集日期，缺日不会补0。',
    'read_current_settings':'填实际行动对象ref，例如工具返回的listing/广告对象引用；留空列出所有可调整对象。',
    'read_chat_history':'这里是文字检索，留空查询当前会话自然问答；不返回工具过程。',
    'read_plan_history':'留空列出本会话方案索引；可填 plan:编号、plan:编号:action:a1 或 history:execution:编号。',
    'read_observations':'可填执行编号或行动ref；留空查询全部待观察与待核实项。',
    'read_operations':'仅支持留空列出近期日志，不支持按ref搜索。',
}


def parameters(schema, tool, prefix=''):
    """递归说明嵌套参数，保留必填、类型、范围与原始路径供人工核对。"""
    result=[]
    for key, spec in schema.get('properties',{}).items():
        term=entry('ai.parameter',key)
        label,help_text,default=(term['label'],term['description'],term['default_value']) if term else (key,spec.get('description','见原始Schema'),None)
        if key=='ref':help_text=REF_HELP.get(tool,help_text)
        path=prefix+key
        result.append({'key':path,'label':label,'help':help_text,'type':spec.get('type','多种类型'),
                       'required':key in schema.get('required',[]),'default':default,
                       'minimum':spec.get('minimum',spec.get('minItems')),'maximum':spec.get('maximum',spec.get('maxItems')),
                       'choices':[{'value':v,'label':WINDOWS.get(v,v)} for v in spec.get('enum',[])]})
        nested=spec.get('items',{}) if spec.get('type')=='array' else spec
        if nested.get('properties'):result.extend(parameters(nested,tool,path+('[]' if spec.get('type')=='array' else '')+'.'))
    return result


def describe(row):
    """向目录追加中文说明与示例，不把管理字段发送给模型。"""
    name=row['name'];paged=name in READ_TOOLS
    example={'ref':'','window':'all','offset':0,'limit':1000} if paged else {}
    if name=='read_data_bundle':example={'queries':[{'tool':'read_business','window':'d7'},{'tool':'read_advertising','ref':'spTargeting','window':'d30'}]}
    if name=='simulate_price':example={'sku':'请填当前ASIN的SKU','selling_price':'11.99','ad_rate':'0.15','refund_rate':'0.03'}
    return {'parameters':parameters(row['parameters'],name),'example':example,'previewable':not row['required'],
            'purpose':{'查询':'读取ERP本地资料；不是实时重新抓取Amazon。','测算':'使用现有成本公式计算假设售价的利润，不修改价格。','计划':'AI最终提交结构化方案，经校验后沿原流程确认/执行；这里不能手动试运行。'}[row['group']],
            'scope':'店铺、广告账户、站点和ASIN由托管资料绑定，不能通过参数切换。',
            'pagination':'AI查询默认最多1000条；受单次96KB及任务剩余预算约束，不足时返回续查位置。概览是摘要，不冒充全量。页面表格默认10条/页。',
            'source_limits':'本轮新资料每类广告最多1000对象、最新关键词最多1000个完整词组；来源省略量会明确返回。历史任务仍按当时冻结范围展示。'}
