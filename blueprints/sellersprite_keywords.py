"""关键词反查机器接口：接收虚拟机领取、进度、报告上传与失败恢复请求。

业务委托services/sellersprite_keywords.py；共用鉴权在sellersprite_common.py。
为兼容现有VM保留/api/sellersprite地址及Blueprint标识，不处理竞品路由。"""

from flask import Blueprint, jsonify, request, send_file
from services import sellersprite_keywords as service
from services import sellersprite_keyword_recovery as recovery

sellersprite_bp = Blueprint('sellersprite', __name__, url_prefix='/api/sellersprite')


from blueprints.sellersprite_common import machine_api, bounded_int, json_object


@sellersprite_bp.get('/asins')
@machine_api
def asins():
    data, total = service.pending_asins(request.args.get('record_date'), bounded_int('limit', 50, 200))
    return jsonify(status='success', data=data, count=len(data), total=total)


@sellersprite_bp.post('/tasks/claim')
@machine_api
def claim():
    data = json_object()
    result = service.claim_task(data.get('asin'), data.get('record_date'), data.get('worker_id'), data.get('recovery_id'))
    return jsonify(status='success', data=result)


@sellersprite_bp.post('/recovery/prepare')
@machine_api
def prepare_recovery():
    """由虚拟机小时任务发现失败和消费到期补救；浏览器不能使用此机器入口。"""
    data = json_object()
    limit = int(data.get('limit', 20))
    if not 1 <= limit <= 200:
        raise service.PipelineError('limit超出范围')
    return jsonify(status='success', data=recovery.prepare(limit, data.get('worker_id', '')))


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
