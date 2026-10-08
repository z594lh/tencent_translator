"""财务人员绩效HTTP入口，URL前缀/api/performance。

当前只读本人/全员绩效及SKU/每日明细，金额与SKU页面来源一致，查询时按历史负责人汇总。
本人/全员/成本权限独立校验，owner由服务端确定，不信任前端user_id。
没有生成/任务/影子入口，也不查询历史任务表；原SKU报表生成链路保持不变。
业务和只读事务在Service中；不改变SKU报表生成，不执行迁移或外部调用。
"""
from flask import Blueprint, request
from blueprints.user_auth import login_required, permission_required
from blueprints.amazon.listing_ownership import domain_response
from services.listing_performance import performance_service as service
from services.listing_performance import breakdown_service as breakdown
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
        return {'direct_sku_report':True,
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
