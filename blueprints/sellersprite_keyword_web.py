"""关键词反查ERP会话接口：ASIN配置、历史快照、Listing关键词和补救队列。

不暴露机器凭证、不触发网页采集；竞品由sellersprite_competitors.py独立注册。
保留原Web接口地址和权限标识，公共响应/JSON校验位于sellersprite_common.py。"""
from flask import Blueprint, jsonify, request
from blueprints.user_auth import login_required
from blueprints.permissions import require_permission
from services import sellersprite_portal as service
from services import sellersprite_keyword_reverse as sellersprite_reverse
from services import sellersprite_keyword_recovery as recovery

sellersprite_web_bp=Blueprint('sellersprite_web',__name__,url_prefix='/api')


@sellersprite_web_bp.after_request
def no_cache(response):
    response.headers['Cache-Control']='no-store'
    return response


from blueprints.sellersprite_common import response, payload


@sellersprite_web_bp.get('/system/sellersprite/asins')
@login_required
@require_permission('system:sellersprite_config')
@response
def configs(): return service.config_list(request.args)


@sellersprite_web_bp.post('/system/sellersprite/asins')
@login_required
@require_permission('system:sellersprite_config')
@response
def add(): return service.add_config(payload(),request.current_user)


@sellersprite_web_bp.put('/system/sellersprite/asins/<int:identifier>')
@login_required
@require_permission('system:sellersprite_config')
@response
def update(identifier): return service.update_config(identifier,payload(),request.current_user)


@sellersprite_web_bp.get('/tools/keyword-reverse/<asin>/tasks')
@login_required
@require_permission('keyword_data:view')
@response
def reverse_tasks(asin):
    """关键词反查仅查看脱敏任务历史，不赋予补救权限。"""
    return recovery.history(asin, request.args.get('page', 1))


@sellersprite_web_bp.get('/system/sellersprite/tasks/<asin>')
@login_required
@require_permission('system:sellersprite_config')
@response
def task_history(asin):
    """配置页查看成功、失败、阶段事件和补救队列。"""
    return recovery.history(asin, request.args.get('page', 1))


@sellersprite_web_bp.post('/system/sellersprite/tasks/<int:identifier>/recover')
@login_required
@require_permission('system:sellersprite_config')
@response
def recover_task(identifier):
    """人工仅排队，虚拟机cron实际执行；阻塞问题要求明确确认。"""
    data = payload()
    actor = request.current_user
    return recovery.enqueue(identifier, actor.get('nickname') or actor['username'], data.get('confirm') is True)


@sellersprite_web_bp.post('/system/sellersprite/recovery/resume')
@login_required
@require_permission('system:sellersprite_config')
@response
def resume_collection():
    """用户处理浏览器登录/验证后恢复账号，不自动解除文件结构异常。"""
    if payload().get('confirm') is not True:
        raise service.PortalError('请确认已在虚拟机处理登录/验证问题')
    actor = request.current_user
    return recovery.resume_account(actor.get('nickname') or actor['username'])


@sellersprite_web_bp.post('/system/sellersprite/recovery/<int:identifier>/cancel')
@login_required
@require_permission('system:sellersprite_config')
@response
def cancel_recovery(identifier):
    """取消还没有运行的补救，不能打断正在上传或入库的任务。"""
    actor = request.current_user
    return recovery.cancel(identifier, actor.get('nickname') or actor['username'])


@sellersprite_web_bp.get('/amazon/listings/<path:sku>/sellersprite')
@login_required
@require_permission('keyword_data:view')
@response
def keywords(sku): return service.listing_data(request.args.get('shop_id'),sku,request.args)


@sellersprite_web_bp.get('/tools/keyword-reverse/options')
@login_required
@require_permission('keyword_data:view')
@response
def reverse_options(): return sellersprite_reverse.asin_options(request.args)


@sellersprite_web_bp.get('/tools/keyword-reverse/<asin>')
@login_required
@require_permission('keyword_data:view')
@response
def reverse_keywords(asin): return sellersprite_reverse.reverse_data(asin,request.args)
