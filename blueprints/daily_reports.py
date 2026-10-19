"""工具栏运营日报HTTP接口：独立权限、本人填写、历史查询与删除。

这里只处理输入/鉴权/响应；经营统计、推荐及事务在Service，不调用AI或广告接口。
沿用归属模块总开关与安全响应包络，所有日报响应禁止浏览器/代理缓存。
"""
from flask import Blueprint, request
from blueprints.user_auth import login_required
from blueprints.amazon.listing_ownership import domain_response
from services.permissions_service import get_user_permission_codes
from services.listing_performance.common import DomainError
from services import operations_daily_report_service as service

daily_reports_bp = Blueprint('daily_reports', __name__, url_prefix='/api/daily-reports')


def access(write=False, delete=False):
    """从登录身份取得独立日报权限；永不信任浏览器can_all/owner。"""
    actor = request.current_user['id']
    codes = set(get_user_permission_codes(actor))
    if 'daily_reports:view' not in codes:
        raise DomainError('需要运营日报查看权限', 403)
    if write and 'daily_reports:write' not in codes:
        raise DomainError('需要运营日报填写权限', 403)
    can_all = 'daily_reports:view_all' in codes
    can_delete = 'daily_reports:delete' in codes
    if delete and not can_delete:
        raise DomainError('需要运营日报删除权限', 403)
    return actor, can_all, 'daily_reports:write' in codes, can_delete


@daily_reports_bp.after_request
def no_cache(response):
    """人员工作汇报和经营信息不得被共享代理缓存。"""
    response.headers['Cache-Control'] = 'no-store'
    return response


@daily_reports_bp.get('/config')
@login_required
@domain_response
def config():
    """获取公开页面配置，不隐式授予任何用户权限。"""
    return service.configuration(*access())


@daily_reports_bp.get('')
@login_required
@domain_response
def reports():
    """按已授权本人/全员范围查询分页历史，过滤字段由Service白名单校验。"""
    actor, can_all, _, _ = access()
    return service.listing(request.args, actor, can_all)


@daily_reports_bp.get('/current')
@login_required
@domain_response
def current():
    """查找登录人某日已有日报；尚未创建返回null，不预先创建空日报。"""
    actor, _, _, _ = access()
    return service.detail(request.args, actor, False)


@daily_reports_bp.get('/<int:record_id>')
@login_required
@domain_response
def detail(record_id):
    """读取指定日报已保存快照，不因SKU移交丢失本人工作存档。"""
    actor, can_all, _, _ = access()
    return service.detail(request.args, actor, can_all, record_id)


@daily_reports_bp.get('/preview')
@login_required
@domain_response
def preview():
    """整理登录人全部当前SKU的经营数据，不允许管理员替其他人员生成日报。"""
    actor, _, _, _ = access(write=True)
    return service.preview(request.args, actor)


@daily_reports_bp.get('/operations')
@login_required
@domain_response
def operation_candidates():
    """操作人只取当前登录身份，全员查看权限也不能代选他人操作。"""
    actor, _, _, _ = access(write=True)
    return service.operations(request.args, actor)


@daily_reports_bp.put('')
@login_required
@domain_response
def save():
    """保存/提交本人日报，不接受代填身份或浏览器上传的统计快照。"""
    actor, _, _, _ = access(write=True)
    return service.save(request.get_json(silent=True), actor)


@daily_reports_bp.delete('/<int:record_id>')
@login_required
@domain_response
def delete_report(record_id):
    """删除本人/获准全员查看范围内的日报；单独授权且必须提交当前版本。"""
    actor, can_all, _, _ = access(delete=True)
    return service.delete_report(request.get_json(silent=True), actor, can_all, record_id)
