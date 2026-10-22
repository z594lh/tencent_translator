"""全局交互配置：只渲染白名单变量，草稿不影响运行，任务固定发布快照。"""
import re
import json
from copy import deepcopy
from services.listing_performance.common import DomainError, positive_id
from . import plan_store
from .metrics import digest, dumps

VARIABLES = ('asin', 'report_end', 'task_date', 'conversation_generation')
SCENES = {
    'daily_initialize': ('新会话首次例行', '请开始托管ASIN {{ asin }}，任务日{{ task_date }}，报表截至{{ report_end }}。这是第{{ conversation_generation }}代新会话，请重新建立产品及运营认识，不假定继承旧对话记忆。'),
    'daily_continue': ('每日续接', '请继续运营ASIN {{ asin }}，任务日{{ task_date }}，报表截至{{ report_end }}。先依据ERP真实执行账本复盘往期改动，纠正记忆与执行状态的矛盾，再分析最新资料。'),
    'active_observations': ('有往期观察', '本次有往期改动待观察，请逐项说明观察目标、已过和剩余周期、数据覆盖及继续观察或提前调整的理由。不要把滚动窗口当成完整改后效果。'),
    'failed_executions': ('存在未发送或拒绝', '执行账本包含未发送或拒绝的操作，不能认为原建议已经实施，请据实际状态重新判断。'),
    'pending_executions': ('存在待核实执行', '仍有执行待回读或结果未知，先说明状态，不重复修改同一字段。'),
    'data_gaps': ('资料有缺口', '资料中存在覆盖缺口，请明确哪些结论受影响；缺失不是0，不因此放弃其他有充分依据的分析。'),
    'allow_data': ('允许继续索取资料', '如需补充资料请按request_data索取；资料足够则给final_plan。'),
    'last_call': ('预算收尾', '本轮必须给完整final_plan，顶层type/plan/memory，plan的notes/history_reviews即使为空也给[]；缺资料的对象保持观察，不再索取资料。'),
    'duplicate_data': ('重复取数', '本次任务没有更新版本，请使用前文已提供资料，不重复索取。'),
    'data_response': ('补资料说明', 'ERP只读本次本地归档；缺失不补0，无真实Amazon写入或报表重抓。'),
    'data_unavailable': ('资料不可用', '本次归档没有匹配资料，不代表数值为0；请根据可用证据分析或保持观察。'),
    'data_too_large': ('补资料过大', '本轮补充资料合计过大，请按具体对象/更小limit索取；没有截断全文。'),
    'validation_feedback': ('方案校验纠错', '请修正并给完整final_plan；同一ref只保留一项，最多12项，不输出占位动作；必要时只保留最重要的改动。'),
    'execution_feedback': ('执行结果说明', 'ERP真实接口回读事实；属性一致不代表收益改善。'),
    'human_followup': ('人工继续提问', '请在当前共享对话中回答用户关于ASIN {{ asin }}的问题，资料截至{{ report_end }}。先理解用户问题，再结合记忆、近期原话和真实执行状态回答；允许只解释、澄清或观察，不必提出修改。用户问题本身不授权修改平台。'),
    'human_reply': ('人工对话可继续取数', '需要本次归档明细时按request_data补查；可以直接解释时返回reply，确有具体调整建议时返回propose_plan。所有建议均未执行，不得把建议或用户口头同意说成已修改。'),
    'human_last_call': ('人工对话结束本轮', '请利用已有资料完整回答用户，不再索取资料。无需修改时返回reply；提出具体调整时返回propose_plan。缺证据的部分如实说明，不猜测、不为了凑方案而修改。'),
    'human_validation_feedback': ('人工回复校验纠错', '请按ERP列出的具体错误修正回复或建议，返回完整reply或propose_plan，不再索取资料。保持用户当前问题原意；仅复盘与建议相关的往期改动。'),
    'discussion_followup': ('人工讨论', '请接着当前ASIN {{ asin }}的对话回答用户，资料截至{{ report_end }}。直接用自然语言解释和讨论，先回答当前问题，不要求全面复盘或形成操作清单。'),
    'proposal_followup': ('人工生成优化方案', '请根据用户当前要求和共享讨论，为ASIN {{ asin }}生成有依据的优化方案，资料截至{{ report_end }}。需要明细可按request_data补查，最终返回一个完整final_plan对象。允许只观察或零改动；本次只保存建议，须人工选择并确认后才执行。'),
}
HUMAN_SCENES=('human_followup','human_reply','human_last_call','human_validation_feedback')
MODE_SCENES=('discussion_followup','proposal_followup')
DATA_NOTES = {
    'analysis':'AI提出可执行方案；ERP以实时前值预检后提交，接受不等于属性生效，业绩变化不直接归因于单次改动。',
    'windows':'d1/d3/d7/d14/d30=截止日向前对应天数；p7/p14/p30=前等长区间，互相重叠不可累加',
    'matrices':'columns给字段顺序，rows给逐行值；coverage表示日期覆盖，null未知不等于0',
    'ads':'spAdvertisedProduct仅本ASIN；spCampaigns/Placement为关联活动；spTargeting/SearchTerm为关联组，可能含其他SKU。五类报表不是五份可累加销售',
    'keywords':'卖家精灵snapshot为近30日关键词曝光流量结构；月搜索量等是市场级，不能当本ASIN经营量',
    'history':'recent_runs给近期AI结论及实际执行状态；observations给仍需复盘的具体改动、执行基线和改后区间证据；pending_executions不可认定已改成功',
    'baseline_initialize':'新会话重新读取全部资料类别及完整窗口，不继承旧会话策略；明细归档可按需读取',
    'baseline_continue':'使用当前会话记忆及本次重新读取的资料',
    'access_listing':'ref=SKU或空，返回真实缓存完整文案；当前文字是否首轮发送以delivery_policy为准',
    'access_current_settings':'ref=精确action_catalog.ref或空；读取本次完整缓存前值，不代表平台当前值或已修改',
    'access_erp_operations':'ref为空；本地最近ERP人工及AI操作，按执行引用去重',
    'access_pricing':'ref=SKU或空，返回已归档的计算售价成本及价格场景',
    'access_business':'ref=SKU或经营evidence_id或空；指定window可取日明细或原多窗矩阵',
    'access_advertising':'ref=广告报告kind、精确广告证据id或action_catalog.ref；可分页；仅本ASIN已关联对象',
    'access_keywords':'ref=完整词组或空；空按已归档流量序分页，不能拆成单词翻译',
    'access_keyword_history':'ref=完整词组或空；返回真实版本日期和排名流量历史',
    'access_history':'ref为空分页查本会话历史索引；或history:execution:编号、history:run:编号、plan:方案编号、plan:编号:action:a1精确取旧行动；本账户本ASIN历史',
}
DATA = {
    'listing': ('当前Listing', ('listing',)),
    'pricing': ('计算售价与成本', ('pricing',)),
    'business': ('SKU多窗口经营汇总', ('business',)),
    'advertising': ('广告多窗口汇总', ('ads',)),
    'keywords': ('卖家精灵关键词预览', ('keywords',)),
    'erp_operations': ('近期ERP操作预览', ('recent_operations',)),
}
TEMPLATE = re.compile(r'\{\{\s*([a-z_]+)\s*\}\}')
TABLE_COLUMNS = {
    'ai_operation_prompt_profiles':'id,revision,draft_json,published_version_id',
    'ai_operation_prompt_versions':'id,config_json,config_hash',
    'ai_operation_documents':'id,scope_key,content_text,content_hash,metadata_json',
    'ai_operation_message_documents':'id,message_id,document_id,message_index,json_pointer,delivery,metadata_json',
}
DEFAULT_RULES = ('综合Listing、售价成本、经营、广告与关键词资料，给出有依据的具体运营建议。'
                 '允许今日只观察或不修改，不要求固定数量的行动；已知文案违规需单独纠错。'
                 '资料足够后返回最终方案及下一版运营记忆。最终行动通过ERP正式接口提交，接受与生效必须分开核对。')


def defaults():
    """提供显式标注的系统初始配置；读取不写库，不隐式发布。"""
    return {'schema': 3, 'scenes': {k: v[1] for k, v in SCENES.items()},
            'business_rules': DEFAULT_RULES, 'data_notes':deepcopy(DATA_NOTES), 'data': {key: 'send' for key in DATA}}


def render(text, variables):
    """仅替换简单变量，不执行模板表达式、SQL、文件或环境变量。"""
    names = TEMPLATE.findall(text)
    if any(name not in VARIABLES or name not in variables for name in names):
        raise DomainError('话术包含未知或缺失变量，只支持：'+', '.join(VARIABLES), 422)
    residue = TEMPLATE.sub('', text)
    if '{{' in residue or '}}' in residue or '{%' in text or '{#' in text:
        raise DomainError('话术只支持简单的{{ 白名单变量 }}，不能包含模板表达式', 422)
    return TEMPLATE.sub(lambda match: str(variables[match.group(1)]), text)


def validate(value):
    """校验完整配置、长度与资料策略；核心身份/执行事实/观察不可移除。"""
    if not isinstance(value, dict) or set(value) != {'schema','scenes','business_rules','data_notes','data'} or type(value['schema']) is not int or value['schema'] not in (1,2,3):
        raise DomainError('交互配置结构无效', 422)
    expected=set(SCENES)-(set(HUMAN_SCENES) if value['schema']==1 else set())-(set(MODE_SCENES) if value['schema']<3 else set())
    if not isinstance(value['scenes'], dict) or set(value['scenes']) != expected:
        raise DomainError('场景话术必须完整，不能添加脚本条件', 422)
    value=deepcopy(value)
    if value['schema']==1:
        value['schema']=2
        value['scenes'].update({key:SCENES[key][1] for key in HUMAN_SCENES})
    if value['schema']==2:
        value['schema']=3
        value['scenes'].update({key:SCENES[key][1] for key in MODE_SCENES})
    if not isinstance(value['data_notes'],dict) or set(value['data_notes'])!=set(DATA_NOTES):raise DomainError('数据阅读及取数说明必须完整',422)
    if not isinstance(value['data'], dict) or set(value['data']) != set(DATA) or any(mode not in ('send','on_demand') for mode in value['data'].values()):
        raise DomainError('本期资料仅支持首轮发送或按需提供；核心执行事实始终发送', 422)
    for text in [value['business_rules'], *value['scenes'].values(),*value['data_notes'].values()]:
        if not isinstance(text,str) or not text.strip() or len(text)>6000 or any(ord(c)<32 and c not in '\n\t' for c in text):
            raise DomainError('话术/业务规则不能为空、超过6000字符或包含控制字符', 422)
        render(text, {key: '示例' for key in VARIABLES})
    if len(dumps(value).encode('utf8'))>96000:raise DomainError('配置超过96KB', 422)
    return deepcopy(value)


def version(cur, version_id):
    """读取并校验不可变发布版本，不用今天的草稿解释旧任务。"""
    cur.execute('SELECT * FROM ai_operation_prompt_versions WHERE id=%s', (positive_id(version_id,'配置版本'),))
    row=cur.fetchone()
    if not row:raise DomainError('配置版本不存在',404)
    config=json.loads(row['config_json'])
    if digest(config)!=row['config_hash']:raise DomainError('交互配置摘要不符，未调用AI',503)
    upgraded=validate(config)
    return {'version_id':str(row['id']), 'hash':digest(upgraded), 'published_hash':row['config_hash'],
            'schema_migration':config['schema']!=upgraded['schema'], 'config':upgraded,
            'created_at':row['created_at'], 'actor_id':row['actor_id']}


def active():
    """任务开始时只读固定一次配置版本；未迁移直接报错而非隐藏降级。"""
    with plan_store.transaction() as cur:
        check_schema(cur)
        cur.execute('SELECT * FROM ai_operation_prompt_profiles WHERE id=1')
        row=cur.fetchone()
        if not row:raise DomainError('请先执行AI交互配置迁移SQL',503)
        if row['published_version_id']:return version(cur,row['published_version_id'])
    config=defaults()
    return {'version_id':None,'hash':digest(config),'config':config,'source':'系统初始配置（尚未发布自定义版本）'}


def check_schema(cur):
    """调用前验证配置与附件部署完整性，缺表时在登记计划或收费之前停止。"""
    for table,columns in TABLE_COLUMNS.items():cur.execute('SELECT '+columns+' FROM '+table+' WHERE 1=0')


def overview():
    """返回草稿、版本索引及固定协议；GET不保存草稿、不调用模型。"""
    from . import conversation_context
    with plan_store.transaction() as cur:
        cur.execute('SELECT * FROM ai_operation_prompt_profiles WHERE id=1')
        row=cur.fetchone()
        if not row:raise DomainError('请先执行AI交互配置迁移SQL',503)
        published=version(cur,row['published_version_id']) if row['published_version_id'] else None
        cur.execute('SELECT id,config_hash,actor_id,created_at FROM ai_operation_prompt_versions ORDER BY id DESC LIMIT 50')
        versions=[{**r,'id':str(r['id'])} for r in cur.fetchall()]
    from . import human_contract, discussion
    return {'revision':row['revision'],'published':published,'draft':validate(json.loads(row['draft_json'])) if row['draft_json'] else (published['config'] if published else defaults()),
            'versions':versions,'scene_options':[{'key':k,'label':v[0]} for k,v in SCENES.items()],
            'data_options':[{'key':k,'label':v[0]} for k,v in DATA.items()], 'variables':list(VARIABLES),
            'fixed_protocol':conversation_context.SYSTEM,'human_protocol':human_contract.SYSTEM,
            'discussion_protocol':discussion.SYSTEM,'proposal_protocol':conversation_context.SYSTEM+'\n'+discussion.PROPOSAL_BOUNDARY,
            'configuration_note':'讨论和生成方案使用独立可配置场景；旧发布版本缺少的新场景只读补入初始话术，并记录原发布摘要。旧人工场景仅用于无模式的历史任务。讨论不发送运营要求、Listing规范及方案协议；方案与每日使用同一种JSON协议。',
            'fixed_note':'每日与生成方案使用JSON、数据口径、Listing规范及观察约束；讨论只有最小事实边界，不发送这些执行协议。实际请求以所选模式的免费预览和消息附件为准。'}


def save(body, actor):
    """带并发版本保存草稿或发布；发布版本不可覆盖，回退也生成新版本。"""
    if not isinstance(body,dict) or set(body)!={'revision','operation','config','restore_version_id'} or type(body['revision']) is not int or body['revision']<0 or body['operation'] not in ('draft','publish','restore'):
        raise DomainError('配置保存参数无效')
    config=validate(body['config']) if body['operation']!='restore' else None
    if body['operation']!='restore' and body['restore_version_id'] is not None:raise DomainError('非回退操作不接受历史版本')
    with plan_store.transaction(True) as cur:
        cur.execute('SELECT * FROM ai_operation_prompt_profiles WHERE id=1 FOR UPDATE')
        row=cur.fetchone()
        if not row or row['revision']!=body['revision']:raise DomainError('配置已被其他用户更新，请重新打开后编辑',409)
        if body['operation']=='restore':
            if body['config'] is not None:raise DomainError('回退不接受混合草稿')
            config=version(cur,body['restore_version_id'])['config']
        published=row['published_version_id']
        if body['operation']!='draft':
            cur.execute('INSERT INTO ai_operation_prompt_versions (config_json,config_hash,actor_id,created_at) VALUES (%s,%s,%s,UTC_TIMESTAMP(3))',
                        (dumps(config),digest(config),positive_id(actor,'发布用户')))
            published=cur.lastrowid
        cur.execute('UPDATE ai_operation_prompt_profiles SET revision=revision+1,draft_json=%s,published_version_id=%s,updated_by=%s,updated_at=UTC_TIMESTAMP(3) WHERE id=1',
                    (dumps(config),published,actor))
    return overview()
