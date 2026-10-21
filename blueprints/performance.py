"""财务人员绩效HTTP入口，URL前缀/api/performance。

当前只读本人/全员绩效及SKU/每日明细，金额与SKU页面来源一致，查询时按历史负责人汇总。
本人/全员/成本权限独立校验，owner由服务端确定，不信任前端user_id。
没有生成/任务/影子入口，也不查询历史任务表；原SKU报表生成链路保持不变。
经营分析配置/查询、资料下载与收费AI独立鉴权；只在明确确认后调用DeepSeek，不修改广告。
业务和读写事务在Service中；不改变SKU报表生成、不执行迁移，仅收费AI入口有模型外部调用。
"""
from io import BytesIO
from flask import Blueprint, request, send_file
from blueprints.user_auth import login_required, permission_required
from blueprints.amazon.listing_ownership import domain_response
from services.listing_performance import performance_service as service
from services.listing_performance import breakdown_service as breakdown
from services.listing_performance import analysis_service as analysis
from services.listing_performance import analysis_export
from services.listing_performance import analysis_history
from services.permissions_service import get_user_permission_codes
from services.listing_performance.common import positive_id, DomainError

performance_bp = Blueprint('performance',__name__,url_prefix='/api/performance')


def permissions():
    """读取当前登录用户权限并返回code集合。

    request.current_user由登录装饰器提供；只查询既有授权，不给用户自动新增权限。"""
    return set(get_user_permission_codes(request.current_user['id']))


@performance_bp.get('/me')
@login_required
@permission_required('performance:view_self')
@domain_response
def me():
    """GET /me返回本人绩效汇总及趋势。

    拒绝user_id参数，强制owner为当前用户；成本字段按view_costs独立控制。"""
    if request.args.get('user_id'):
        raise DomainError('本人接口不接受 user_id')
    return service.query(request.args,owner=request.current_user['id'],show_costs='performance:view_costs' in permissions())


@performance_bp.get('/users')
@login_required
@permission_required('performance:view_all')
@domain_response
def users():
    """GET /users返回全员或指定人员经营表现。

    需要view_all，user_id可限定正整数人员；成本可见性仍单独判断，不在此重算。"""
    owner = positive_id(request.args['user_id'],'user_id') if request.args.get('user_id') else None
    return service.query(request.args,owner=owner,show_costs='performance:view_costs' in permissions())


def breakdown_owner():
    """统一校验SKU/每日明细人员范围，返回(owner,可看成本)。

    本人只可查自己；全员必须选人员或单独选未分配，非法组合/越权报错。
    仅权限/输入处理，不查SKU、不执行利润计算。"""
    codes = permissions()
    if not codes.intersection({'performance:view_self','performance:view_all'}):
        raise DomainError('无绩效查看权限',403)
    owner = None if 'performance:view_all' in codes else request.current_user['id']
    if request.args.get('user_id'):
        chosen = positive_id(request.args['user_id'],'user_id')
        if owner is not None and chosen != owner:
            raise DomainError('不能查看他人的绩效',403)
        owner = chosen
    if request.args.get('unassigned') == '1' and ('performance:view_all' not in codes or request.args.get('user_id')):
        raise DomainError('未分配明细仅允许管理员单独查询',403)
    if owner is None and request.args.get('unassigned') != '1':
        raise DomainError('请选择需要查看的人员')
    return owner,'performance:view_costs' in codes


@performance_bp.get('/skus')
@login_required
@domain_response
def skus():
    """GET /skus返回选定人员的SKU汇总、服务端排序和分页。

    owner/costs由breakdown_owner限定，参数交breakdown.query；只读SKU日报。"""
    owner,costs = breakdown_owner()
    return breakdown.query(request.args,owner=owner,show_costs=costs)


@performance_bp.get('/sku-daily')
@login_required
@domain_response
def sku_daily():
    """GET /sku-daily返回指定SKU的逐日明细及分页。

    Service要求seller_sku，默认日期倒序；趋势可显式请求升序完整范围，不写数据。"""
    owner,costs = breakdown_owner()
    return breakdown.query(request.args,owner=owner,daily=True,show_costs=costs)


@performance_bp.get('/config')
@login_required
@domain_response
def config():
    """GET /config返回启用店铺、站点公开配置和已有SKU日期覆盖。

    先检查核心表可读，再读取sku_profit每店最早/最晚日报用于默认15天。
    仅返回时区/币种，绝不返回签名密钥；独立只读连接关闭，不迁移或生成。"""
    from services.listing_performance.common import market_config, configured_marketplaces
    from services.listing_performance.assignment_policy import default_date, DESCRIPTION
    configured = configured_marketplaces()
    from services.mysql_service import get_db_connection
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for schema_table in ('listing_responsibility_scopes','listing_responsibility_history',
                          'listing_responsibility_audit'):
                cursor.execute('SELECT id FROM '+schema_table+' LIMIT 0')
            cursor.execute('SELECT id,shop_name,marketplace_id,region FROM amazon_shops WHERE status=1 ORDER BY id')
            shops = cursor.fetchall()
            for shop in shops:
                cursor.execute('SELECT MIN(report_date) AS first, MAX(report_date) AS last FROM sku_profit WHERE shop_id=%s', (shop['id'],))
                coverage = cursor.fetchone()
                shop['sku_report_date_from'] = coverage['first']
                shop['sku_report_date_to'] = coverage['last']
        codes = permissions()
        return {'direct_sku_report':True,
                'assignment_policy': {'default_date': default_date().isoformat(), 'description': DESCRIPTION},
                'analysis_access': {'user_id': request.current_user['id'],
                    'self': 'performance:analysis_self' in codes and bool(codes.intersection({'performance:view_self','performance:view_all'})),
                    'ads': 'performance:analysis_ads' in codes,
                    'ai': {'performance:analysis_ads','performance:analysis_ai'}.issubset(codes),
                    'all': {'performance:view_all','performance:analysis_all'}.issubset(codes)},
                'shops':shops,'markets':{key:{k:v for k,v in market_config(key).items() if k in
            ('timezone','currency','report_currency')} for key in configured}}
    finally:
        conn.close()


@performance_bp.get('/owners')
@login_required
@domain_response
def owners():
    """GET /owners提供人员筛选选项，必须有绩效查看权限。

    全员权限可读所选店铺历史负责人；本人权限只返回自己，不允许user_id越权枚举。
    不复用要求Listing分配权限的人员接口，以便财务查看者独立使用。
    """
    codes = permissions()
    if not codes.intersection({'performance:view_self','performance:view_all'}):
        raise DomainError('无绩效查看权限',403)
    owner = None if 'performance:view_all' in codes else request.current_user['id']
    return service.owner_options(request.args,owner=owner)


def analysis_owner(params):
    """校验独立经营分析权限，返回可操作的负责人 ID。

    分析允许当前 SKU 的接手前历史，不能只凭 view_self 开放；全员分析同时要求
    view_all+analysis_all，本人分析要求页面查看权+analysis_self，拒绝参数越权。
    """
    if not hasattr(params, 'get'):
        raise DomainError('请求参数必须为对象')
    codes = permissions()
    actor = request.current_user['id']
    owner = positive_id(params.get('owner_user_id', actor), 'owner_user_id')
    if {'performance:view_all', 'performance:analysis_all'}.issubset(codes):
        return owner
    if ('performance:analysis_self' in codes and
            codes.intersection({'performance:view_self', 'performance:view_all'}) and owner == actor):
        return owner
    raise DomainError('无该人员的 SKU 经营分析权限，请配置本人/全员分析权限', 403)


@performance_bp.get('/analysis/config')
@login_required
@domain_response
def analysis_config():
    """获取关注 SKU/指标配置，失效 SKU 标记失效；不保存默认配置。"""
    return analysis.get_config(request.args, analysis_owner(request.args))


@performance_bp.put('/analysis/config')
@login_required
@domain_response
def save_analysis_config():
    """保存已鉴权人员配置，Service 校验当前归属和版本；不修改任何源报表。"""
    data = request.get_json(silent=True)
    owner = analysis_owner(data)
    return analysis.save_config(data, owner, request.current_user['id'])


@performance_bp.get('/analysis/export')
@login_required
@domain_response
def export_analysis():
    """下载单SKU经营/自动广告Markdown，需人员分析权及独立广告资料导出权。

    截止日/配置版本由已查询表格传入；服务只读，固定三日逐日与7/15/30天。
    返回前复核权限，附件不缓存，不保存文件或调用AI/Amazon。
    """
    owner = analysis_owner(request.args)
    if 'performance:analysis_ads' not in permissions():
        raise DomainError('需要 SKU 广告分析资料导出权限', 403)
    include_pricing = bool(permissions().intersection({'pricing:calculate', 'performance:view_costs'}))
    document, filename = analysis_export.build_document(request.args, owner, include_pricing=include_pricing)
    if analysis_owner(request.args) != owner or 'performance:analysis_ads' not in permissions():
        raise DomainError('分析权限已变更，请重新登录确认', 403)
    if include_pricing and not permissions().intersection({'pricing:calculate', 'performance:view_costs'}):
        raise DomainError('售价/成本查看权限已变更，请重新查询', 403)
    response = send_file(BytesIO(document.encode('utf-8')), mimetype='text/markdown',
                         as_attachment=True, download_name=filename, max_age=0)
    response.headers['Cache-Control'] = 'no-store'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    return response


@performance_bp.post('/analysis/ai')
@login_required
@domain_response
def ai_analysis():
    """明确确认后分析单SKU：人员/广告资料/收费权限分开验证，不接收外部提示词或模型。

    Service复用下载资料和deepseekAI，限制并发/输入/费用重试，返回可安全文本展示的JSON；
    成功结果保存历史，但不执行建议、不生成任务。费用提示由前端确认，服务端也要求confirm_ai=true。
    """
    params = request.get_json(silent=True)
    owner = analysis_owner(params)
    include_pricing = bool(permissions().intersection({'pricing:calculate', 'performance:view_costs'}))

    def check_permissions():
        """调用前后重新读取DB授权，避免分析期间撤权后仍返回结果。"""
        if analysis_owner(params) != owner or not {'performance:analysis_ads','performance:analysis_ai'}.issubset(permissions()):
            raise DomainError('需要SKU广告资料导出及SKU AI分析权限', 403)
        if include_pricing and not permissions().intersection({'pricing:calculate', 'performance:view_costs'}):
            raise DomainError('售价/成本查看权限已变更，请重新查询', 403)

    check_permissions()
    if any(key in params for key in ('document', 'prompt', 'model', 'messages')):
        raise DomainError('AI分析资料和模型由系统确定，不接受自定义提示词或模型')
    result = analysis_export.analyze_document(params, owner, check_permissions, include_pricing=include_pricing,
                                             actor=request.current_user['id'])
    # 明确构造JSON包络Response以设置no-store；domain_response保留业务错误处理。
    from flask import jsonify
    from services.listing_performance.common import json_data
    import json
    response = jsonify(json.loads(json_data({'status': 'success', 'data': result})))
    response.headers['Cache-Control'] = 'no-store'
    return response


@performance_bp.get('/analysis/ai/history')
@performance_bp.get('/analysis/ai/latest')
@performance_bp.get('/analysis/ai/history/<int:record_id>')
@login_required
@domain_response
def ai_history(record_id=None):
    """只读AI历史列表/最近可查看记录/指定详情；沿用分析权限，不调用收费模型。

    每次按登录用户核对人员范围和售价/成本资格；Service再限制当前负责SKU及记录身份。
    不把旧config_version当历史读取条件，查看历史不会改变关注配置或分析快照。
    """
    from flask import jsonify
    from services.listing_performance.common import json_data
    import json
    owner = analysis_owner(request.args)
    codes = permissions()
    if not {'performance:analysis_ads', 'performance:analysis_ai'}.issubset(codes):
        raise DomainError('需要SKU广告资料导出及SKU AI分析权限', 403)
    include_pricing = bool(codes.intersection({'pricing:calculate', 'performance:view_costs'}))
    result = analysis_history.query(request.args, owner, include_pricing=include_pricing,
                                    latest=request.path.endswith('/latest'), record_id=record_id)
    current = permissions()
    if (analysis_owner(request.args) != owner or
            not {'performance:analysis_ads', 'performance:analysis_ai'}.issubset(current) or
            (include_pricing and not current.intersection({'pricing:calculate', 'performance:view_costs'}))):
        raise DomainError('分析查看权限已变更，请重新登录确认', 403)
    response = jsonify(json.loads(json_data({'status': 'success', 'data': result})))
    response.headers['Cache-Control'] = 'no-store'
    return response


@performance_bp.get('/analysis/candidates')
@login_required
@domain_response
def analysis_candidates():
    """提供当前负责子 SKU 的搜索分页，不扩大历史/他人 SKU 访问资格。"""
    return analysis.candidates(request.args, analysis_owner(request.args))


@performance_bp.get('/analysis/data')
@login_required
@domain_response
def analysis_data():
    """查询已存清单的 N 天/前 N 天经营对比，不启动任务，不调用 AI/广告接口。"""
    return analysis.query(request.args, analysis_owner(request.args))
