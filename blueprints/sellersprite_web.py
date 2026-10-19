"""ERP会话鉴权入口，与机器X-API-Key接口完全分离，不向浏览器暴露机器凭证。"""
from functools import wraps
from flask import Blueprint, jsonify, request, current_app
from blueprints.user_auth import login_required
from blueprints.permissions import require_permission
from services import sellersprite_portal as service

sellersprite_web_bp=Blueprint('sellersprite_web',__name__,url_prefix='/api')


@sellersprite_web_bp.after_request
def no_cache(response):
    response.headers['Cache-Control']='no-store'
    return response


def response(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        try: return jsonify(status='success',data=function(*args,**kwargs))
        except service.PortalError as exc: return jsonify(status='error',message=str(exc)),exc.code
        except Exception:
            current_app.logger.exception('卖家精灵页面接口异常')
            return jsonify(status='error',message='卖家精灵数据暂不可用，请检查数据表或稍后重试'),503
    return wrapped


def payload():
    if not request.is_json or (request.content_length or 0)>32768: raise service.PortalError('请提交不超过32KB的JSON数据')
    value=request.get_json(silent=True)
    if not isinstance(value,dict): raise service.PortalError('请求体必须为JSON对象')
    return value


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


@sellersprite_web_bp.get('/amazon/listings/<path:sku>/sellersprite')
@login_required
@require_permission('amazon_listings:page')
@response
def keywords(sku): return service.listing_data(request.args.get('shop_id'),sku,request.args)
