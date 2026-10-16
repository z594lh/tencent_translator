"""界面配置薄接口：公开只读白名单，管理接口要求独立权限。"""
import logging
from functools import wraps

from flask import Blueprint, jsonify, request
from blueprints.user_auth import login_required
from blueprints.permissions import require_permission
from services import ui_config_service as service

ui_config_bp = Blueprint('ui_config', __name__, url_prefix='/api')
logger = logging.getLogger(__name__)


@ui_config_bp.after_request
def no_cache(result):
    # 鉴权失败响应也不缓存，避免代理复用管理员接口的旧状态。
    result.headers['Cache-Control'] = 'no-store'
    return result


def response(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            result = jsonify({'status': 'success', 'data': function(*args, **kwargs)})
        except service.UiConfigError as exc:
            message = '界面配置暂不可用，页面将保留默认文案' if request.endpoint == 'ui_config.public_config' else str(exc)
            result = jsonify({'status': 'error', 'message': message})
            result.status_code = exc.status
        except Exception as exc:
            logger.error('界面配置接口异常：%s', type(exc).__name__)
            result = jsonify({'status': 'error', 'message': '界面配置暂不可用，请检查配置表初始化或稍后重试'})
            result.status_code = 503
        result.headers['Cache-Control'] = 'no-store'
        return result
    return wrapped


def payload():
    if not request.is_json or (request.content_length or 0) > 8192:
        raise service.UiConfigError('请提交有效且不超过8KB的JSON配置')
    return request.get_json(silent=True)


@ui_config_bp.get('/public/ui-config')
@response
def public_config():
    return service.read_config(public=True)


@ui_config_bp.get('/system/ui-config')
@login_required
@require_permission('system:ui_config')
@response
def get_config():
    return service.read_config()


@ui_config_bp.put('/system/ui-config')
@login_required
@require_permission('system:ui_config')
@response
def update_config():
    return service.write_config(payload(), request.current_user)


@ui_config_bp.post('/system/ui-config/reset')
@login_required
@require_permission('system:ui_config')
@response
def reset_config():
    return service.write_config(payload(), request.current_user, reset=True)


@ui_config_bp.get('/system/ui-config/history')
@login_required
@require_permission('system:ui_config')
@response
def config_history():
    try:
        page, size = int(request.args.get('page', '1')), int(request.args.get('page_size', '10'))
        if not 1 <= page <= 100000 or not 1 <= size <= 100:
            raise ValueError()
    except (ValueError, TypeError):
        raise service.UiConfigError('分页参数无效') from None
    return service.history(page, size)
