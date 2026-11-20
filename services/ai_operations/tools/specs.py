"""原生Function Calling参数声明；字段校验与真实业务约束仍由服务端强制执行。"""
from ..language import text as model_text

from copy import deepcopy
from ..plan_contract import METRICS
from services.business_dictionary import label as dictionary_label

PAGE = {'type':'object','properties':{'ref':{'type':'string','description':model_text('specs.ef85248a0a70')},
    'window':{'type':'string','enum':['all','daily','d1','d3','d7','p7','d14','p14','d30','p30']},
    'offset':{'type':'integer','minimum':0,'maximum':10000,'default':0},
    'limit':{'type':'integer','minimum':1,'maximum':1000,'default':1000,
             'description':model_text('specs.68b29589bca9')}},'additionalProperties':False}
EMPTY = {'type':'object','properties':{},'additionalProperties':False}
COMPETITORS = deepcopy(PAGE)
COMPETITORS['properties']['window']['enum']=['all']
READ_TOOLS=('read_listing','read_business','read_advertising','read_keywords','read_keyword_history','read_competitors','read_storage_fees','read_pricing',
            'read_current_settings','read_operations','read_observations','read_plan_history','read_chat_history')
BUNDLE={'type':'object','properties':{'queries':{'type':'array','minItems':1,'maxItems':4,'items':{
    'type':'object','properties':{'tool':{'type':'string','enum':list(READ_TOOLS)},**deepcopy(PAGE['properties'])},
    'required':['tool'],'additionalProperties':False}}},'required':['queries'],'additionalProperties':False}
OBSERVATION = {'type':'object','properties':{'metrics':{'type':'array','items':{'type':'string','enum':sorted(METRICS)}},
    'keywords':{'type':'array','items':{'type':'string'}},'check_days':{'type':'array','items':{'type':'integer','enum':[3,7,14,30]}},
    'duration_days':{'type':'integer','minimum':1,'maximum':30},'hypothesis':{'type':'string'},'review_when':{'type':'string'}},
    'required':['metrics','keywords','check_days','duration_days','hypothesis','review_when'],'additionalProperties':False}
ACTION = {'type':'object','properties':{'ref':{'type':'string','description':model_text('specs.eaf7ae003c75')},
    'mode':{'type':'string','enum':['change','observe']},'after':{'anyOf':[{'type':'string'},{'type':'integer'},{'type':'array','items':{'type':'string'}},{'type':'null'}],
        'description':model_text('specs.1acef8036ff6')},
    'reason':{'type':'string'},'priority':{'type':'string','enum':['优先处理','常规关注','持续观察']},
    'evidence_refs':{'type':'array','items':{'type':'string'}},'observation':OBSERVATION},
    'required':['ref','mode','after','reason','priority','evidence_refs','observation'],'additionalProperties':False}
PLAN = {'type':'object','properties':{'summary':{'type':'string'},'actions':{'type':'array','items':ACTION},
    'notes':{'type':'array','items':{'type':'string'}},'history_reviews':{'type':'array','items':{'type':'object','properties':{
        'execution_id':{'type':'string','description':model_text('specs.c423e3669458')},'decision':{'type':'string','enum':['继续观察','到期复评','提前调整','等待回读']},
        'conclusion':{'type':'string'},'evidence_refs':{'type':'array','items':{'type':'string'},'description':model_text('specs.cf36ae6d4fc4')}},
        'required':['execution_id','decision','conclusion','evidence_refs'],'additionalProperties':False}}},
    'required':['summary','actions','notes','history_reviews'],'additionalProperties':False}


def definition(name, description, parameters=None, group='查询', required=False):
    """同一声明供模型和工具管理页复用，不让页面改变调用函数或参数白名单。"""
    effect=model_text('specs.11a7d5b0dcbc') if required else model_text('specs.fcb2fc2574ab')
    if name=='read_observations':effect+=model_text('specs.9f484f577ae4')
    return {'name':name,'label':dictionary_label('ai.tool',name),'description':description,'parameters':deepcopy(parameters if parameters is not None else PAGE),
            'group':group,'required':required,'effect':effect}


def definitions():
    """返回代码白名单目录；所有工具均由当前托管任务绑定店铺、ASIN和截止日。"""
    rows=[
        definition('get_operation_overview',model_text('specs.8712522b1041'),EMPTY),
        definition('read_data_bundle',model_text('specs.329bbf76fece'),BUNDLE),
        definition('get_asin_overview',model_text('specs.e01b6410afd5'),EMPTY),
        definition('read_listing',model_text('specs.739de8a69cc2')),
        definition('read_business',model_text('specs.b8f96a17b8e1')),
        definition('read_advertising',model_text('specs.a4c3fb3ed856')),
        definition('read_keywords',model_text('specs.6835daed2d76')),
        definition('read_keyword_history',model_text('specs.9de42f22d14e')),
        definition('read_storage_fees',model_text('specs.15c60968c467'),COMPETITORS),
        definition('read_competitors',model_text('specs.f737ac3ab34b'),COMPETITORS),
        definition('read_pricing',model_text('specs.eb3911b32a70')),
        definition('read_current_settings',model_text('specs.8d2667ce8c8c')),
        definition('read_operations',model_text('specs.bb8309244227')),
        definition('read_observations',model_text('specs.fd6a72680bed')),
        definition('read_plan_history',model_text('specs.ec287380647c')),
        definition('read_chat_history',model_text('specs.d008fd29aceb')),
        definition('simulate_price',model_text('specs.eae1f0824817'),
            {'type':'object','properties':{'sku':{'type':'string'},'selling_price':{'type':'string'},'ad_rate':{'type':'string'},'refund_rate':{'type':'string'}},
             'required':['sku','selling_price','ad_rate','refund_rate'],'additionalProperties':False},'测算'),
        definition('submit_optimization_plan',model_text('specs.c74aa2858cc2'),PLAN,'计划',True),
    ]
    return {row['name']:row for row in rows}
