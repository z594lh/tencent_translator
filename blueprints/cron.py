"""计划任务管理：只处理HTTP输入、鉴权和响应，不执行命令。"""
import logging
from functools import wraps

from flask import Blueprint, jsonify, request
from blueprints.user_auth import login_required, permission_required
from services.permissions_service import get_user_permission_codes
from services.cron_tasks import service, repository
from services import translation_management
from services.baidu_translator import TranslationError
from blueprints.permissions import _check_admin_role

cron_bp = Blueprint('cron', __name__, url_prefix='/api/cron')
logger = logging.getLogger(__name__)


def response(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            payload = function(*args, **kwargs)
            result = jsonify(payload[0]) if isinstance(payload, tuple) else jsonify(payload)
            if isinstance(payload, tuple):
                result.status_code = payload[1]
        except (repository.TaskError, TranslationError) as exc:
            result = jsonify({'success': False, 'message': str(exc)})
            result.status_code = exc.status
        except ValueError:
            result = jsonify({'success': False, 'message': '任务或请求参数无效'})
            result.status_code = 400
        except Exception as exc:
            logger.error('计划任务接口异常：%s', type(exc).__name__)
            result = jsonify({'success': False, 'message': '计划任务服务暂不可用，请核对数据库迁移和服务配置；不会自动重试'})
            result.status_code = 503
        result.headers['Cache-Control'] = 'no-store'
        return result
    return wrapped


def pagination():
    page = max(1, min(request.args.get('page', 1, type=int), 100000))
    per_page = max(1, min(request.args.get('per_page', 10, type=int), 100))
    return page, per_page


@cron_bp.get('/tasks')
@login_required
@permission_required('tasks:page')
@response
def get_tasks():
    page, per_page = pagination()
    codes = get_user_permission_codes(request.current_user['id'])
    return service.list_tasks(request.args.get('keyword', '').strip().lower()[:100], page, per_page, 'tasks:execute' in codes)


@cron_bp.post('/tasks/<task_id>/execute')
@login_required
@permission_required('tasks:page')
@permission_required('tasks:execute')
@response
def execute_task(task_id):
    record, created = service.submit(task_id, request.get_json(silent=True), request.current_user)
    return {'success': True, 'record': record, 'message': '请求已接收' if created else '已找到原请求记录，未重复执行'}, 202 if created else 200


@cron_bp.get('/tasks/<task_id>/runs')
@login_required
@permission_required('tasks:page')
@response
def task_history(task_id):
    return service.history(task_id, *pagination())


@cron_bp.get('/runs/<int:run_id>')
@login_required
@permission_required('tasks:page')
@response
def run_detail(run_id):
    return service.detail(run_id)


@cron_bp.get('/translation/status')
@login_required
@permission_required('tasks:page')
@response
def translation_status():
    data = translation_management.snapshot()
    data['can_resume'] = (_check_admin_role(request.current_user['id']) and
                          'tasks:execute' in get_user_permission_codes(request.current_user['id']))
    return {'success':True,'data':data}


@cron_bp.post('/translation/resume')
@login_required
@permission_required('tasks:page')
@permission_required('tasks:execute')
@response
def resume_translation():
    if not _check_admin_role(request.current_user['id']):
        return {'success':False,'message':'只有管理员可以恢复翻译账户'},403
    if (request.content_length or 0)>2048:
        return {'success':False,'message':'请求体过大'},413
    result = translation_management.resume(request.get_json(silent=True))
    logger.info('翻译账户人工恢复：用户ID=%s，恢复任务数=%s',request.current_user['id'],result['restored'])
    return {'success':True,**result}
