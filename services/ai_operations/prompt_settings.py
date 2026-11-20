"""原生工具任务的精简提示词配置；复用旧发布表，不覆盖旧草稿或恢复旧编译器。"""
import json
from copy import deepcopy
from pathlib import Path
from . import language
import pymysql
from services.listing_performance.common import DomainError, positive_id
from . import plan_store
from .metrics import digest

SCHEMA='native-prompts-v1'
LIMIT_DEFAULTS={'total_input_bytes':2000000,'warn_percent':50,'urgent_percent':75}
CATALOG=json.loads(Path(__file__).with_name('prompt_catalog.json').read_text(encoding='utf-8'))
FIELDS=tuple((r['key'],r['label'],r['trigger'],r['help'],r['default']) for r in CATALOG['fields'])
ORIGINAL_KEYS=set(CATALOG['original_keys'])


def defaults():
    """返回独立默认配置，不引用或迁移旧多场景话术。"""
    return {'schema':SCHEMA,'prompts':{key:value for key,_,_,_,value in FIELDS},'language':language.defaults(),'limits':deepcopy(LIMIT_DEFAULTS)}


def validate(config):
    """校验纯文本与数值；旧版本仅补新增默认项，不改写原9项。"""
    if not isinstance(config,dict) or set(config)-{'schema','prompts','limits','language'} or config.get('schema')!=SCHEMA:
        raise DomainError('不是当前精简提示词配置',422)
    values=config.get('prompts')
    if not isinstance(values,dict) or not ORIGINAL_KEYS<=set(values) or set(values)-{r[0] for r in FIELDS}:raise DomainError('提示词字段缺失或包含未知项',422)
    for text in values.values():
        if not isinstance(text,str) or not text.strip() or len(text)>6000 or any(ord(c)<32 and c not in '\n\t' for c in text):
            raise DomainError('提示词不能为空、超过6000字符或含控制字符',422)
    result=deepcopy(config)
    result['prompts']={**defaults()['prompts'],**values}
    try:result['language']=language.validate(config.get('language',{}))
    except ValueError as error:raise DomainError(str(error),422) from None
    limits=result.setdefault('limits',deepcopy(LIMIT_DEFAULTS))
    if not isinstance(limits,dict) or set(limits)!=set(LIMIT_DEFAULTS) or any(type(v) is not int for v in limits.values()):raise DomainError('任务额度配置格式无效',422)
    if not 100000<=limits['total_input_bytes']<=20000000 or not 1<=limits['warn_percent']<limits['urgent_percent']<=99:raise DomainError('累计输入须为100000～20000000字节，提醒阈值须递增且介于1～99%',422)
    return result


def version(row):
    """读取不可变版本并验证原hash；旧格式仅保留，不启用旧资料首包。"""
    try:config=json.loads(row['config_json'])
    except (ValueError,TypeError):raise DomainError('提示词版本无法读取，未调用AI',503) from None
    if digest(config)!=row['config_hash']:raise DomainError('提示词版本hash不一致，未调用AI',503)
    if not isinstance(config,dict):raise DomainError('提示词版本结构无效，未调用AI',503)
    if config.get('schema')!=SCHEMA:return None
    return validate(config)


def active(cur):
    """只读发布指针；首次访问不写默认值，旧草稿完整保留。"""
    cur.execute('SELECT revision,published_version_id,draft_json FROM ai_operation_prompt_profiles WHERE id=1');profile=cur.fetchone() or {}
    config=None;row=None
    if profile.get('published_version_id'):
        cur.execute('SELECT * FROM ai_operation_prompt_versions WHERE id=%s',(profile['published_version_id'],));row=cur.fetchone()
        if not row:raise DomainError('提示词发布版本缺失，未调用AI',503)
        config=version(row)
    return {'revision':profile.get('revision',0),'version_id':str(row['id']) if config else None,
            'config':config or defaults(),'legacy_draft_preserved':bool(profile.get('draft_json')),
            'legacy_published_preserved':bool(row and config is None)}


def snapshot():
    """任务开始冻结全套提示词，之后发布不影响本任务或它的摘要调用。"""
    with plan_store.transaction() as cur:result=active(cur)
    return {**result,'hash':digest(result['config'])}


def listing():
    """配置页返回实际生效内容、默认值与各触发说明，不查询经营数据。"""
    categories={'summary_system':'summary','summary_prefix':'summary','summary_retry_question':'summary',
                'budget_normal':'budget','budget_warning':'budget','budget_urgent':'budget','tool_delivery':'budget',
                'plan_repair':'validation','plan_required':'validation'}
    fields=[{'key':key,'label':label,'trigger':trigger,'help':help_text,'default':value,'section':'prompts',
             'category':categories.get(key,'task'),'variables':[]} for key,label,trigger,help_text,value in FIELDS]
    from .tools.specs import definitions
    tool_defaults={value['description'].key:name for name,value in definitions().items() if isinstance(value['description'],language.ConfiguredText)}
    fields.extend({**row,'section':'language','tool_name':tool_defaults.get(row['key']),
                   'trigger':row.get('trigger') or '、'.join(row['locations']),
                   'help':'本项只调整文字，不改变发送时机、参数结构、状态判断或业务校验。模板变量必须完整保留。'} for row in language.CATALOG)
    return {**snapshot(),'fields':fields,
            'note':'通用AI对话与托管共用。保存生成不可变发布版本，在任务开始准备时固定；进行中的任务不受影响。工具说明另在AI工具管理。旧草稿和旧发布版本保留。'}


def publish(body,actor):
    """CAS发布完整精简配置，旧draft_json不变；并发编辑必须刷新，历史版本从不覆盖。"""
    if not isinstance(body,dict) or set(body)!={'revision','config'} or type(body['revision']) is not int or body['revision']<0:
        raise DomainError('提示词保存参数无效',422)
    config=validate(body['config']);actor=positive_id(actor,'用户')
    try:
        with plan_store.transaction(True) as cur:
            cur.execute('SELECT revision FROM ai_operation_prompt_profiles WHERE id=1 FOR UPDATE');old=cur.fetchone()
            if (old['revision'] if old else 0)!=body['revision']:raise DomainError('提示词已被其他用户发布，请刷新后再保存',409)
            cur.execute('INSERT INTO ai_operation_prompt_versions(config_json,config_hash,actor_id,created_at) VALUES(%s,%s,%s,UTC_TIMESTAMP(3))',
                        (json.dumps(config,ensure_ascii=False),digest(config),actor));vid=cur.lastrowid
            if old:cur.execute('UPDATE ai_operation_prompt_profiles SET revision=revision+1,published_version_id=%s,updated_by=%s,updated_at=UTC_TIMESTAMP(3) WHERE id=1',(vid,actor))
            else:cur.execute('INSERT INTO ai_operation_prompt_profiles(id,revision,published_version_id,updated_by,updated_at) VALUES(1,1,%s,%s,UTC_TIMESTAMP(3))',(vid,actor))
    except pymysql.IntegrityError:raise DomainError('提示词发布冲突，请刷新核对',409) from None
    return listing()


def history():
    """只读最近50个发布版本中的原生配置；恢复时复制内容再发布为新版本。"""
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_prompt_versions ORDER BY id DESC LIMIT 51');rows=list(cur.fetchall())
    result=[]
    for row in rows[:50]:
        config=version(row)
        if config:result.append({'id':str(row['id']),'config':config,'hash':row['config_hash'],'actor_id':row['actor_id'],'created_at':row['created_at']})
    return {'list':result,'limited':len(rows)>50}


def system(frozen,mode):
    """两模式共享通用提示；只叠加对应的最终输出要求，旧agent仍可自然回复。"""
    values=frozen['config']['prompts']
    return values['system']+('\n\n'+values[mode] if mode in ('discussion','proposal') else '')+('\n\n'+values.get('plan_submission',defaults()['prompts']['plan_submission']) if mode=='proposal' else '')


def preview(body):
    """用待保存文本生成实际请求结构示例；不收费、不取经营数据、不创建会话。"""
    if not isinstance(body,dict) or set(body)!={'config','mode'} or body['mode'] not in ('discussion','proposal','daily','summary'):
        raise DomainError('请选择讨论、方案、每日或摘要预览',422)
    config=validate(body['config']);values=config['prompts'];mode=body['mode'];frozen={'config':config}
    if mode=='summary':messages=[{'role':'system','content':values['summary_system']},{'role':'user','content':json.dumps({'previous_summary':None,'dialogue':[{'role':'user','content':'此前自然问题示例'},{'role':'assistant','content':'此前自然回答示例'}]},ensure_ascii=False)}]
    else:
        from . import tools
        history=[{'role':'user','content':values['summary_prefix']+'\n已生成的历史摘要示例'},
                 {'role':'user','content':'昨日为什么保持观察？'},{'role':'assistant','content':'当时数据不足，建议继续观察。'}]
        messages=[{'role':'system','content':system(frozen,'proposal' if mode=='daily' else mode)},
                  {'role':'user','content':json.dumps({'current_scope':{'identity':{'asin':'示例ASIN','shop_id':'示例店铺'},'time':{'report_end':'示例报表截止日'}},'past_question_answers':history},ensure_ascii=False)},
                  {'role':'user','content':values['daily_question'] if mode=='daily' else '用户实际输入的自然问题（示例，不会发送）'}]
        with language.bind(config):toolset=tools.snapshot(allow_plan=mode!='discussion')
        from . import task_budget
        messages,_,_=task_budget.request(messages,toolset['tools'],0,config)
    return {'messages':messages,'tools':toolset['tools'] if mode!='summary' else [],
            'later_messages':([{'when':'尚未提交计划','role':'user','content':values['plan_required']}] if mode in ('proposal','daily') else [])+[
                {'when':key,'role':'tool' if key in ('plan_repair','tool_delivery') else 'user','content':values[key]}
                for key in ('budget_normal','budget_warning','budget_urgent','plan_repair','tool_delivery')] if mode!='summary' else [],
            'configured_language':[{'key':row['key'],'category':row['category'],'template':config['language'][row['key']],
                                    'variables':row['variables'],'locations':row['locations']} for row in language.CATALOG],
            'note':'免费预览，不调用模型。任务固定一份文字与工具快照；资料解释、Schema说明和错误模板均可配置，动态数值与业务原文不在模板中编辑。'}
