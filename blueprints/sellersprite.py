"""机器接口只处理鉴权和HTTP，领取/清洗事务由独立Service负责。"""
import hmac
import os
from functools import wraps

from flask import Blueprint, current_app, jsonify, request, send_file
from services import sellersprite_service as service

sellersprite_bp = Blueprint('sellersprite', __name__, url_prefix='/api/sellersprite')


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


@sellersprite_bp.get('/asins')
@machine_api
def asins():
    data, total = service.pending_asins(request.args.get('record_date'), bounded_int('limit', 50, 200))
    return jsonify(status='success', data=data, count=len(data), total=total)


@sellersprite_bp.post('/tasks/claim')
@machine_api
def claim():
    data = json_object()
    result = service.claim_task(data.get('asin'), data.get('record_date'), data.get('worker_id'))
    return jsonify(status='success', data=result)


@sellersprite_bp.post('/tasks/<int:task_id>/progress')
@machine_api
def progress(task_id):
    result = service.update_progress(task_id, request.headers.get('X-Task-Token'), json_object())
    return jsonify(status='success', data=result)


@sellersprite_bp.post('/tasks/<int:task_id>/file')
@machine_api
def upload(task_id):
    # 同时限制multipart包络和实际文件读取量，不放宽应用其它上传接口限制。
    if request.content_length is None:
        raise service.PipelineError('上传请求必须提供Content-Length', 411)
    if request.content_length > service.MAX_FILE_SIZE + 1024 * 1024:
        raise service.PipelineError('上传请求超过大小限制', 413)
    file = request.files.get('file')
    if not file:
        raise service.PipelineError('缺少file上传字段')
    result = service.receive_file(task_id, request.headers.get('X-Task-Token'), file, request.form.get('sha256'))
    if result['status'] == 'parse_failed':
        return jsonify(status='error', message=result.get('error_message') or '清洗失败，原文件已保存', data=result), 422
    return jsonify(status='success', data=result)


@sellersprite_bp.get('/tasks')
@machine_api
def tasks():
    data = service.list_tasks(request.args.get('record_date'), request.args.get('status'),
                              bounded_int('page', 1, 100000), bounded_int('page_size', 50, 200))
    return jsonify(status='success', data=data)


@sellersprite_bp.get('/tasks/<int:task_id>/keywords')
@machine_api
def keywords(task_id):
    result = service.keyword_rows(task_id, bounded_int('page', 1, 100000), bounded_int('page_size', 100, 200))
    return jsonify(status='success', data=result)


@sellersprite_bp.get('/files/<int:file_id>')
@machine_api
def download(file_id):
    path, filename = service.file_details(file_id)
    return send_file(path, as_attachment=True, download_name=filename, conditional=True)
