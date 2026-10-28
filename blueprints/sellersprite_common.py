"""卖家精灵公共HTTP辅助：机器鉴权、JSON输入和Web错误响应。

关键词与竞品Blueprint分别注册各自路由，此处不注册业务接口、不访问业务表。
"""
import hmac
import os
from functools import wraps
from flask import current_app, jsonify, request
from services import sellersprite_common as service
from services.sellersprite_portal import PortalError


def machine_api(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        expected = os.environ.get('SELLERSPRITE_API_KEY') or os.environ.get('BSR_API_KEY')
        if not expected:
            return jsonify(status='error', message='服务端未配置采集API密钥'), 503
        if not hmac.compare_digest(expected.encode(), request.headers.get('X-API-Key', '').encode()):
            return jsonify(status='error', message='API Key无效'), 401
        try:
            return function(*args, **kwargs)
        except service.PipelineError as exc:
            return jsonify(status='error', message=str(exc)), exc.code
        except (ValueError, TypeError) as exc:
            return jsonify(status='error', message='参数格式错误'), 400
        except Exception:
            current_app.logger.exception('卖家精灵任务处理失败')
            return jsonify(status='error', message='采集服务处理失败，请查看服务器日志'), 500
    return decorated


def bounded_int(name, default, maximum):
    value = int(request.args.get(name, default))
    if value < 1 or value > maximum:
        raise service.PipelineError(f'{name}超出范围')
    return value


def json_object():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise service.PipelineError('请求体必须为JSON对象')
    return data


def response(function):
    @wraps(function)
    def wrapped(*args,**kwargs):
        try: return jsonify(status='success',data=function(*args,**kwargs))
        except PortalError as exc: return jsonify(status='error',message=str(exc)),exc.code
        except service.PipelineError as exc: return jsonify(status='error',message=str(exc)),exc.code
        except Exception:
            current_app.logger.exception('卖家精灵页面接口异常')
            return jsonify(status='error',message='卖家精灵数据暂不可用，请检查数据表或稍后重试'),503
    return wrapped


def payload():
    if not request.is_json or (request.content_length or 0)>32768: raise PortalError('请提交不超过32KB的JSON数据')
    value=request.get_json(silent=True)
    if not isinstance(value,dict): raise PortalError('请求体必须为JSON对象')
    return value
