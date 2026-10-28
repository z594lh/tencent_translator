"""原生Function Calling参数声明；字段校验与真实业务约束仍由服务端强制执行。"""
from copy import deepcopy
from ..plan_contract import METRICS
from services.business_dictionary import label as dictionary_label

PAGE = {'type':'object','properties':{'ref':{'type':'string','description':'精确SKU、证据/对象引用或完整关键词；空表示分页列出'},
    'window':{'type':'string','enum':['all','daily','d1','d3','d7','p7','d14','p14','d30','p30']},
    'offset':{'type':'integer','minimum':0,'maximum':10000,'default':0},
    'limit':{'type':'integer','minimum':1,'maximum':1000,'default':1000,
             'description':'默认最多1000条；实际受完整对象/字节预算限制，查看next_offset续查，不把部分结果当全量'}},'additionalProperties':False}
EMPTY = {'type':'object','properties':{},'additionalProperties':False}
COMPETITORS = deepcopy(PAGE)
COMPETITORS['properties']['window']['enum']=['all']
READ_TOOLS=('read_listing','read_business','read_advertising','read_keywords','read_keyword_history','read_competitors','read_pricing',
            'read_current_settings','read_operations','read_observations','read_plan_history','read_chat_history')
BUNDLE={'type':'object','properties':{'queries':{'type':'array','minItems':1,'maxItems':4,'items':{
    'type':'object','properties':{'tool':{'type':'string','enum':list(READ_TOOLS)},**deepcopy(PAGE['properties'])},
    'required':['tool'],'additionalProperties':False}}},'required':['queries'],'additionalProperties':False}
OBSERVATION = {'type':'object','properties':{'metrics':{'type':'array','items':{'type':'string','enum':sorted(METRICS)}},
    'keywords':{'type':'array','items':{'type':'string'}},'check_days':{'type':'array','items':{'type':'integer','enum':[3,7,14,30]}},
    'duration_days':{'type':'integer','minimum':1,'maximum':30},'hypothesis':{'type':'string'},'review_when':{'type':'string'}},
    'required':['metrics','keywords','check_days','duration_days','hypothesis','review_when'],'additionalProperties':False}
ACTION = {'type':'object','properties':{'ref':{'type':'string','description':'read_current_settings实际返回的精确ref，不得自造'},
    'mode':{'type':'string','enum':['change','observe']},'after':{'anyOf':[{'type':'string'},{'type':'integer'},{'type':'array','items':{'type':'string'}},{'type':'null'}],
        'description':'文案为完整字符串数组；金额为精确十进制字符串；广告位为整数百分比；观察为null'},
    'reason':{'type':'string'},'priority':{'type':'string','enum':['优先处理','常规关注','持续观察']},
    'evidence_refs':{'type':'array','items':{'type':'string'}},'observation':OBSERVATION},
    'required':['ref','mode','after','reason','priority','evidence_refs','observation'],'additionalProperties':False}
PLAN = {'type':'object','properties':{'summary':{'type':'string'},'actions':{'type':'array','items':ACTION},
    'notes':{'type':'array','items':{'type':'string'}},'history_reviews':{'type':'array','items':{'type':'object','properties':{
        'execution_id':{'type':'string','description':'read_observations返回的原始编号，如1，不填history:execution:1'},'decision':{'type':'string','enum':['继续观察','到期复评','提前调整','等待回读']},
        'conclusion':{'type':'string'},'evidence_refs':{'type':'array','items':{'type':'string'},'description':'仅用实际读取的完整evidence_ref，例如history:execution:1；不拼接Listing字段引用'}},
        'required':['execution_id','decision','conclusion','evidence_refs'],'additionalProperties':False}}},
    'required':['summary','actions','notes','history_reviews'],'additionalProperties':False}


def definition(name, description, parameters=None, group='查询', required=False):
    """同一声明供模型和工具管理页复用，不让页面改变调用函数或参数白名单。"""
    effect='仅保存方案，不直接写Amazon' if required else '只读本地业务资料或进行公式计算'
    if name=='read_observations':effect+='；关闭后每日优化会在收费前停止，无法复盘观察'
    return {'name':name,'label':dictionary_label('ai.tool',name),'description':description,'parameters':deepcopy(parameters if parameters is not None else PAGE),
            'group':group,'required':required,'effect':effect}


def definitions():
    """返回代码白名单目录；所有工具均由当前托管任务绑定店铺、ASIN和截止日。"""
    rows=[
        definition('get_operation_overview','优化运营时优先查询：一次取得身份/库存、Listing关键字段、售价成本、经营和各类广告全部1/3/7/14/30日及前等长窗口、主要完整关键词、待观察摘要。无逐日/全广告明细，缺失不补0；再按需要定向补查。底层关闭模块不可绕过。',EMPTY),
        definition('read_data_bundle','一次批量查询1到4个明确资料对象，可各自指定窗口和分页；默认每项最多1000条，受总字节预算限制，按next_offset续查。适合概览后同时补查广告对象、完整文案和关键词。只读、保留每项错误/分页，超限明确说明，不修改Amazon。',BUNDLE),
        definition('get_asin_overview','查询当前托管ASIN概况、SKU、库存、报表截止日及可用数据范围。无参数，不允许换店或换ASIN。',EMPTY),
        definition('read_listing','查询完整Listing当前缓存文案和各字段规范；ref可为SKU。准备修改文案时先读取，不编造商品事实。'),
        definition('read_business','查询SKU日明细或d1/d3/d7/d14/d30及前等长窗口。利润直接使用用户确认的报表，窗口不能互加。'),
        definition('read_advertising','查询商品广告、活动、广告位、自动/手动投放、搜索词。ref可为spAdvertisedProduct/spCampaigns/spCampaignsPlacement/spTargeting/spSearchTerm或具体对象引用；活动/组数据可能含其他SKU。'),
        definition('read_keywords','分页查询卖家精灵完整词组和排名流量；ref为完整关键词或证据引用，数据是近30日快照，不是单日流量。'),
        definition('read_keyword_history','查询完整词组在真实采集日期的排名和流量变化；缺日不伪造。'),
        definition('read_competitors','查询当前托管ASIN显式绑定竞品的已采集商品报告，含销量、价格、排名、评分、文案和原字段。只读本任务冻结快照，不能查询未绑定ASIN；ref留空或绑定竞品ASIN/证据引用。仅window=all。检查实际导出ASIN、采集日、最近失败和缺口；Sales是父体所有跟卖近30天预测销量，不是本店销量。默认1000条，按next_offset续查，不即时采集或修改竞品。',COMPETITORS),
        definition('read_pricing','查询当前售价、成本来源、佣金及明确假设的利润场景；不是历史实际利润。ref可为SKU。'),
        definition('read_current_settings','分页查询可调整对象的精确ref、完整before、单位、约束及evidence_refs。广告位是竞价加价，不是流量比例。提交任何行动前必须实际查询对应对象。'),
        definition('read_operations','查询近期人工/AI实际操作日志，不把模型建议当作已执行。'),
        definition('read_observations','分页查询真实执行状态、往期改动、观察周期和改后数据。每日提交方案前须查询全部待观察/待核实项；人工只需相关对象。'),
        definition('read_plan_history','查询原方案或执行完整记录；ref为plan:编号、plan:编号:action:a1或history:execution:编号；空时列历史索引。'),
        definition('read_chat_history','只查询本会话的原问题和最终回答。ref可输入文字检索；offset/limit分页。旧讨论不是执行事实。'),
        definition('simulate_price','复用ERP统一成本和利润公式，假设广告/退款费率必须明确传入；不会修改售价。',
            {'type':'object','properties':{'sku':{'type':'string'},'selling_price':{'type':'string'},'ad_rate':{'type':'string'},'refund_rate':{'type':'string'}},
             'required':['sku','selling_price','ad_rate','refund_rate'],'additionalProperties':False},'测算'),
        definition('submit_optimization_plan','提交可执行的完整计划，也可actions为空表示今天不改。先查询实际对象、对应证据和相关观察。只做校验/归档，不代表已修改；错误会返回，可纠正后再提交。最后用自然语言回答用户，不输出整段JSON。',PLAN,'计划',True),
    ]
    return {row['name']:row for row in rows}
