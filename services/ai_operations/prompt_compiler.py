"""每日提示唯一编译入口；配置快照随方案固定，预览和实际请求采用同一结构。"""
from copy import deepcopy
import json
from . import prompt_configuration as config, conversation_context as context, plan_provider
from .metrics import dumps, digest


def variables(data, session):
    """从冻结事实提取白名单变量，不接受浏览器提供经营资料或模板环境。"""
    content=data['content']
    return {'asin':content['identity']['asin'],'report_end':str(content['time']['report_end']),
            'task_date':str(content.get('management_context',{}).get('run_date') or content['time']['report_end']),
            'conversation_generation':session.get('generation',1)}


def scene(prepared, name):
    """按任务已经固定的版本渲染后续话术，不读取当前发布指针。"""
    return config.render(prepared['interaction']['config']['scenes'][name],prepared['interaction']['variables'])


def prepare(data, session, snapshot):
    """生成首轮问题与资料选择；核心执行账本、身份、覆盖及观察无可关闭开关。"""
    values=variables(data,session); settings=snapshot['config']; seed=context.seed(data,session)
    notes={key:config.render(value,values) for key,value in settings['data_notes'].items()}
    seed['analysis']['note']=notes['analysis']
    seed['reading_guide']={key:notes[key] for key in ('windows','matrices','ads','keywords','history')}
    seed['conversation']['baseline_note']=notes['baseline_initialize' if session.get('daily_baseline_pending') else 'baseline_continue']
    for source in context.SOURCES:seed['data_access'][source]=notes['access_'+source]
    management=seed.get('management_context',{})
    matched=['daily_initialize' if session.get('daily_baseline_pending') else 'daily_continue']
    if management.get('observations'):matched.append('active_observations')
    if any(row.get('status') in ('not_sent','rejected') for row in management.get('execution_facts',[])):matched.append('failed_executions')
    if management.get('pending_executions'):matched.append('pending_executions')
    if seed.get('gaps'):matched.append('data_gaps')
    question='\n\n'.join(config.render(settings['scenes'][key],values) for key in matched)
    question+='\n\n'+config.render(settings['business_rules'],values)
    # 仅首轮省略可选模块；原归档及按需接口保留，固定核心不可被业务配置关闭。
    for source,mode in settings['data'].items():
        if mode=='on_demand':
            for field in config.DATA[source][1]:seed.pop(field,None)
    seed['delivery_policy']={'modules':settings['data'],'meaning':'send为首轮摘要/预览；on_demand不发送该模块正文，可在本任务按需取明细；核心执行事实和观察中相关指标始终保留'}
    from .conversation import MAX_CALLS, MAX_TOTAL_INPUT_BYTES, MAX_ELAPSED_SECONDS
    return {'system':context.SYSTEM,'question':question,'user':dumps(seed),'request':plan_provider.request_spec(),
            'interaction':{**deepcopy(snapshot),'variables':values,'matched_scenes':matched,'compiler':'interaction-v1'},
            'conversation':{'id':str(session['id']) if session['id'] is not None else None,
                'context_version':session['revision'],'protocol':context.VERSION,'max_calls':MAX_CALLS,
                'max_context_bytes':context.MAX_CONTEXT_BYTES,'generation':session.get('generation',1),
                'daily_baseline_pending':bool(session.get('daily_baseline_pending')),
                'max_total_input_bytes':MAX_TOTAL_INPUT_BYTES,'max_elapsed_seconds':MAX_ELAPSED_SECONDS,
                'archived_bytes':data['size_bytes'],'seed_bytes':context.size(seed)}}


def messages(prepared, history, remaining, allow_data):
    """构建真实messages数组；内部标记不发给供应商，收尾场景也能免费预览。"""
    human=prepared.get('conversation',{}).get('purpose')=='chat'
    control={'remaining_model_calls':remaining if allow_data else 1,'data_requests_allowed':allow_data,
             'instruction':scene(prepared,('human_reply' if allow_data else 'human_last_call') if human else ('allow_data' if allow_data else 'last_call'))}
    result=[{'role':'system','content':prepared['system']}, {'role':'user','content':prepared['question']},
            {'role':'user','content':prepared['user']},
            *([{'role':'user','content':dumps({'human_sender':prepared['human_actor']})}] if prepared.get('human_actor') else []),
            *([{'role':'user','content':prepared['human_question']}] if human else []),
            *[{k:v for k,v in message.items() if k!='validation_feedback'} for message in history],
            {'role':'user','content':dumps({'task_control':control})}]
    return result,control


def prepare_human(data,session,snapshot,question):
    """人工与每日共用资料、版本及条件，独立提供原话而不强迫生成每日方案。"""
    from . import human_contract
    prepared=prepare(data,session,snapshot)
    prepared['system']=human_contract.SYSTEM
    prepared['conversation']['purpose']='chat'
    prepared['human_question']=question
    selected=['human_followup',*prepared['interaction']['matched_scenes'][1:]]
    prepared['interaction']['matched_scenes']=selected
    prepared['question']='\n\n'.join(scene(prepared,key) for key in selected)
    prepared['question']+='\n\n'+config.render(snapshot['config']['business_rules'],prepared['interaction']['variables'])
    return prepared


def allow_data(prepared, history, remaining, input_bytes, elapsed):
    """预览与实际共用轮次/字节/超时判断，纠错轮不得继续取数。"""
    from .conversation import MAX_TOTAL_INPUT_BYTES, MAX_ELAPSED_SECONDS
    base=[{'role':'system','content':prepared['system']},{'role':'user','content':prepared['question']},
          {'role':'user','content':prepared['user']},
          *([{'role':'user','content':dumps({'human_sender':prepared['human_actor']})}] if prepared.get('human_actor') else []),
          *([{'role':'user','content':prepared['human_question']}] if 'human_question' in prepared else []),*history]
    base_bytes=context.size(base)+1000
    return bool(remaining>1 and not (history and history[-1].get('validation_feedback'))
        and input_bytes+2*base_bytes+context.MAX_DATA_BYTES<=MAX_TOTAL_INPUT_BYTES
        and base_bytes+context.MAX_DATA_BYTES+2000<=context.MAX_CONTEXT_BYTES
        and elapsed+2*plan_provider.TIMEOUT<=MAX_ELAPSED_SECONDS)


def manifest(prepared, data):
    """声明归档边界，避免把完整资料归档或关键词截选说成全部已发送。"""
    return {'config_version_id':prepared['interaction']['version_id'],
            'config_hash':prepared['interaction']['hash'],'archive_hash':data['content_hash'],
            'published_config_hash':prepared['interaction'].get('published_hash'),
            'configuration_schema_upgraded':prepared['interaction'].get('schema_migration',False),
            'report_end':str(data['content']['time']['report_end']),
            'data_policy':deepcopy(prepared['interaction']['config']['data']),
            'keyword_archive_count':len(data['content']['keywords']['items']),
            'keyword_sent_count':json.loads(prepared['user']).get('keywords',{}).get('provided_count',0),
            'note':'included仅表示组入已登记请求，不保证供应商收到或模型读完；archived表示未发送原明细，HTTP结果另查对应回答事件'}
