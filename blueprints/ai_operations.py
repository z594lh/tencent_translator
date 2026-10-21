"""AI托管独立权限入口；权限同时包含敏感资料、AI归属分配和真实运营执行。"""
from functools import wraps
from flask import Blueprint, Response, request
import pymysql
from blueprints.user_auth import login_required
from services.permissions_service import get_user_permission_codes
from services.listing_performance.common import DomainError, positive_id
from services.ai_operations import repository, managed_store, daily_runner
from services.ai_operations.metrics import dumps

ai_operations_bp = Blueprint('ai_operations', __name__, url_prefix='/api/ai-operations')
REQUIRED = {'ai_operations:manage'}


def authorize():
    """只接受显式AI托管权限，不以管理员身份或旧绩效权限兜底放行。"""
    if not REQUIRED.issubset(set(get_user_permission_codes(request.current_user['id']))):
        raise DomainError('需要管理员分配“AI托管”权限', 403)


def response(value, status=200):
    """输出禁止缓存的JSON响应，不把敏感异常原文暴露到页面。"""
    return Response(dumps(value), status=status, mimetype='application/json',
                    headers={'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})


def endpoint(function):
    """统一接口鉴权及脱敏异常处理，不自动重发付费操作。"""
    @wraps(function)
    def wrapped(*args, **kwargs):
        """在调用前后复核权限，并将业务错误转换为明确HTTP状态。"""
        try:
            authorize()
            result = function(*args, **kwargs)
            authorize()
            return response({'status': 'success', 'data': result})
        except DomainError as error:
            return response({'status': 'error', 'message': str(error)}, error.status)
        except pymysql.MySQLError:
            return response({'status': 'error', 'message': 'AI托管存储暂不可用；可能已计费或提交平台，请核对执行历史，不重复运行'}, 503)
        except Exception:
            return response({'status': 'error', 'message': 'AI托管流程未完成；可能已计费或提交平台，请核对执行历史，不重复运行'}, 503)
    return wrapped


@ai_operations_bp.get('/options')
@login_required
@endpoint
def options():
    """只读返回支持店铺与本店可托管商品选项。"""
    shop = positive_id(request.args['shop_id'], 'shop_id') if request.args.get('shop_id') else None
    return repository.options(shop)


def request_body():
    """限制托管配置/运行请求体大小，不接受浏览器提交的模型资料。"""
    if request.content_length is None or request.content_length > 8192:
        raise DomainError('托管操作请求为空或超过8KB')
    return request.get_json(silent=True)


@ai_operations_bp.get('/managed')
@login_required
@endpoint
def managed_list():
    """分页返回托管列表与当前保留的会话运行摘要。"""
    return managed_store.listing(request.args)


@ai_operations_bp.post('/managed')
@login_required
@endpoint
def add_managed():
    """通过AI托管权限保存配置及AI负责交接，不要求普通人工分配权限。"""
    authorize_assignment()
    return managed_store.add(request_body(),request.current_user['id'])


@ai_operations_bp.patch('/managed/<managed_id>')
@login_required
@endpoint
def change_managed(managed_id):
    """显式暂停或恢复托管，不立即修改商品或广告。"""
    body=request_body()
    if isinstance(body,dict) and body.get('enabled') is True:authorize_assignment()
    return managed_store.set_enabled(managed_id,body,request.current_user['id'])


def authorize_assignment():
    """AI负责交接属于专用托管权限，普通人工负责人分配保持原有权限。"""
    authorize()


@ai_operations_bp.get('/managed/<managed_id>/runs')
@login_required
@endpoint
def managed_history(managed_id):
    """只读分页查询指定托管ASIN的每日会话历史。"""
    return managed_store.history(managed_id,request.args)


@ai_operations_bp.post('/managed/<managed_id>/run')
@login_required
@endpoint
def run_managed(managed_id):
    """显式触发当天会话主干，拒绝浏览器自定义日期和方案。"""
    if request_body()!={}:raise DomainError('立即运行不接受任意日期或自定义方案参数')
    return daily_runner.run_one(managed_id,request.current_user['id'],authorize=authorize)


@ai_operations_bp.get('/runs/<run_id>')
@login_required
@endpoint
def managed_run_detail(run_id):
    """只读返回优化详情弹框所需的保存会话与执行资料。"""
    return daily_runner.detail(run_id)
