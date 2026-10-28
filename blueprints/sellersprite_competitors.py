"""查竞品机器接口与ERP会话接口；浏览器不接触机器API密钥。"""
from functools import wraps
import json
from flask import Blueprint, request, jsonify, send_file
from blueprints.sellersprite_common import machine_api, json_object, bounded_int
from blueprints.sellersprite_common import response, payload
from blueprints.user_auth import login_required
from blueprints.permissions import require_permission
from services import sellersprite_competitors as service
from services.sellersprite_common import PipelineError, MAX_FILE_SIZE
from services.sellersprite_portal import PortalError

competitors_bp=Blueprint('sellersprite_competitors',__name__,url_prefix='/api')


@competitors_bp.after_request
def no_cache(result):
    result.headers['Cache-Control']='no-store'
    return result


def machine(function):
    @wraps(function)
    def guarded(*args,**kwargs):
        try:return function(*args,**kwargs)
        except PortalError as exc:raise PipelineError(str(exc),exc.code) from None
    return machine_api(guarded)


@competitors_bp.get('/sellersprite/competitors/asins')
@machine
def pending():
    items,total=service.pending(request.args.get('record_date'),bounded_int('limit',50,200))
    return jsonify(status='success',data=items,total=total,count=len(items),batch_reports=True,protocol_version=2,max_batch_size=60)


@competitors_bp.post('/sellersprite/competitors/tasks/claim')
@machine
def claim():
    data=json_object()
    return jsonify(status='success',data=service.claim(data.get('asin'),data.get('record_date'),data.get('worker_id')))


@competitors_bp.post('/sellersprite/competitors/tasks/<int:identifier>/progress')
@machine
def progress(identifier):
    return jsonify(status='success',data=service.progress(identifier,request.headers.get('X-Task-Token'),json_object()))


@competitors_bp.post('/sellersprite/competitors/tasks/<int:identifier>/file')
@machine
def upload(identifier):
    if request.content_length is None or request.content_length>MAX_FILE_SIZE+1024*1024:
        raise PipelineError('上传请求缺少长度或超过21MB',413)
    file=request.files.get('file')
    if not file:raise PipelineError('缺少file字段')
    batch=json.loads(request.form['batch_asins']) if 'batch_asins' in request.form else None
    queries=json.loads(request.form['query_asins']) if 'query_asins' in request.form else None
    proof={k:request.form.get(k) for k in ('identity_method','request_fingerprint')}
    result=service.receive(identifier,request.headers.get('X-Task-Token'),file,request.form.get('sha256'),request.form.get('selected_asin'),request.form.get('report_id'),batch,queries,proof)
    if result['status']=='parse_failed':return jsonify(status='error',message=result.get('error_message'),data=result),422
    return jsonify(status='success',data=result)


@competitors_bp.get('/sellersprite/competitors/tasks/<asin>')
@machine
def machine_history(asin):return jsonify(status='success',data=service.history(asin,request.args.get('page',1)))


@competitors_bp.get('/system/sellersprite/competitors/asins')
@login_required
@require_permission('system:sellersprite_config')
@response
def configs():return service.config_list(request.args)


@competitors_bp.post('/system/sellersprite/competitors/asins')
@login_required
@require_permission('system:sellersprite_config')
@response
def add():return service.add_config(payload(),request.current_user)


@competitors_bp.put('/system/sellersprite/competitors/asins/<int:identifier>')
@login_required
@require_permission('system:sellersprite_config')
@response
def update(identifier):return service.update_config(identifier,payload(),request.current_user)


@competitors_bp.get('/system/sellersprite/competitors/tasks/<asin>')
@login_required
@require_permission('system:sellersprite_config')
@response
def history(asin):return service.history(asin,request.args.get('page',1))


@competitors_bp.get('/system/sellersprite/competitors/snapshots/<int:identifier>')
@login_required
@require_permission('system:sellersprite_config')
@response
def snapshot(identifier):return service.product(identifier)


@competitors_bp.get('/system/sellersprite/competitors/files/<int:identifier>')
@login_required
@require_permission('system:sellersprite_config')
def download(identifier):
    # 下载同样沿用JSON错误处理，但成功响应保持二进制文件。
    try:
        path,name=service.download(identifier)
        return send_file(path,as_attachment=True,download_name=name,conditional=True)
    except (PipelineError,PortalError) as exc:return jsonify(status='error',message=str(exc)),exc.code
