"""AI托管独立权限入口；权限同时包含敏感资料、AI归属分配和真实运营执行。"""
from functools import wraps
from flask import Blueprint, Response, request
import pymysql
from blueprints.user_auth import login_required
from services.permissions_service import get_user_permission_codes
from services.listing_performance.common import DomainError, positive_id
from services.ai_operations import repository, managed_store, daily_runner, execution_recovery, conversation_queries, tools, natural_history,optimization_history
from services.ai_operations.metrics import dumps
from services.ai_operations import chat_tasks, daily_queue, confirmations
from services.ai_operations import tool_activity,prompt_settings

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
            value,status=result if isinstance(result,tuple) and len(result)==2 else (result,200)
            return response({'status': 'success', 'data': value},status)
        except DomainError as error:
            return response({'status': 'error', 'message': str(error)}, error.status)
        except pymysql.MySQLError as error:
            if error.args and error.args[0] in (1054,1146):
                return response({'status':'error','message':'AI工具/共享会话数据库结构未就绪，请先执行本轮新表SQL；页面读取不会调用AI'},503)
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
    return optimization_history.listing(managed_id,request.args)


@ai_operations_bp.get('/history/<source>/<record_id>')
@login_required
@endpoint
def unified_history_detail(source,record_id):
    """每日与人工共用详情入口；旧记录返回明确占位，不恢复旧方案。"""
    return optimization_history.detail(source,record_id)


@ai_operations_bp.post('/managed/<managed_id>/run')
@login_required
@endpoint
def run_managed(managed_id):
    """显式登记当天统一队列，拒绝浏览器自定义日期和方案，不在HTTP等待模型。"""
    if request_body()!={}:raise DomainError('立即运行不接受任意日期或自定义方案参数')
    return daily_queue.enqueue(managed_id,request.current_user['id']),202


@ai_operations_bp.get('/runs/<run_id>')
@login_required
@endpoint
def managed_run_detail(run_id):
    """只读返回优化详情弹框所需的保存会话与执行资料。"""
    return daily_runner.detail(run_id)


@ai_operations_bp.post('/runs/<run_id>/retry-ads')
@login_required
@endpoint
def retry_unsent_ads(run_id):
    """明确确认后，仅补原方案确定未发送的广告；不重新调用AI或重放Listing。"""
    return execution_recovery.retry_ads(run_id, request_body(), request.current_user['id'], authorize)


@ai_operations_bp.get('/managed/<managed_id>/conversations')
@login_required
@endpoint
def conversation_list(managed_id):
    """独立会话目录，只读当前范围历次会话，打开页面不创建或收费。"""
    return {**conversation_queries.listing(managed_id,request.args),'viewer_id':str(request.current_user['id'])}


@ai_operations_bp.post('/managed/<managed_id>/conversations')
@login_required
@endpoint
def new_conversation(managed_id):
    """明确确认后归档并新建共享会话，原执行与每日唯一性不重置。"""
    return conversation_queries.create(managed_id,request_body(),request.current_user['id'])


@ai_operations_bp.get('/conversations/<session_id>/messages')
@login_required
@endpoint
def conversation_messages(session_id):
    """分页读取真实时间线，不调用AI或触发执行/平台回读。"""
    return conversation_queries.messages(session_id,request.args)


@ai_operations_bp.get('/conversations/<session_id>/messages/<message_id>')
@login_required
@endpoint
def conversation_original(session_id,message_id):
    """按所属会话查看完整原始资料，不开放任意磁盘文件读取。"""
    return conversation_queries.original(session_id,message_id)


@ai_operations_bp.get('/conversations/<session_id>/messages/<message_id>/rounds')
@login_required
@endpoint
def conversation_tool_rounds(session_id,message_id):
    """按所属原消息查看完整工具轮次，不调用AI，不受聊天消息页边界影响。"""
    return tool_activity.detail(session_id,message_id)


@ai_operations_bp.get('/tools/statistics')
@login_required
@endpoint
def tool_usage_statistics():
    """只读已有工具频次/组合和模型用量，不自动付费分析或创建工具。"""
    return tool_activity.statistics(request.args)


@ai_operations_bp.get('/tools')
@login_required
@endpoint
def tool_directory():
    """只读代码注册的工具目录、参数及人工说明，打开不调用模型。"""
    return tools.listing()


def configuration_body():
    """单独限制配置大文本到128KB，不扩大每日执行等旧入口的8KB限制。"""
    if request.content_length is None or request.content_length>131072:raise DomainError('配置请求为空或超过128KB')
    return request.get_json(silent=True)


@ai_operations_bp.get('/prompts')
@login_required
@endpoint
def prompt_configuration():
    """只读当前精简提示词、实际触发条件及默认值；不自动发布或调用AI。"""
    return prompt_settings.listing()


@ai_operations_bp.put('/prompts')
@login_required
@endpoint
def publish_prompt_configuration():
    """发布新的不可变提示词版本，不影响在途任务或覆盖旧草稿。"""
    return prompt_settings.publish(configuration_body(),request.current_user['id'])


@ai_operations_bp.get('/prompts/versions')
@login_required
@endpoint
def prompt_versions():
    """只读历史精简版本，回退也须用户另行确认发布。"""
    return prompt_settings.history()


@ai_operations_bp.post('/prompts/preview')
@login_required
@endpoint
def preview_prompts():
    """用编辑文本及示例问答免费预览结构；无真实模型和平台调用。"""
    return prompt_settings.preview(configuration_body())


@ai_operations_bp.put('/tools/<name>')
@login_required
@endpoint
def save_tool(name):
    """仅编辑白名单工具说明或启停，不能在页面编写SQL、URL或代码。"""
    return tools.update(name,configuration_body(),request.current_user['id'])


@ai_operations_bp.get('/conversations/<session_id>/memory')
@login_required
@endpoint
def conversation_memory(session_id):
    """只读已生成的会话摘要及覆盖范围，不触发收费压缩。"""
    session,_=conversation_queries.require_session(session_id)
    return natural_history.describe(session['id'])


@ai_operations_bp.get('/conversations/<session_id>/messages/<message_id>/documents/<binding_id>')
@login_required
@endpoint
def conversation_document(session_id,message_id,binding_id):
    """旧附件入口只返回停止支持，不访问已删除表或跨账户资料。"""
    raise DomainError('旧数据，无法查看；当前请求请在分析过程查看完整原文',410)


@ai_operations_bp.post('/conversations/<session_id>/messages')
@login_required
@endpoint
def send_conversation_message(session_id):
    """先保存人工问题和任务后快速返回，长分析由独立消费者处理，不能触发平台修改。"""
    if request.content_length is None or request.content_length>32768:raise DomainError('人工问题请求为空或超过32KB')
    return chat_tasks.enqueue(session_id,request.get_json(silent=True),request.current_user['id']),202


@ai_operations_bp.get('/tasks/<task_id>')
@login_required
@endpoint
def chat_task_status(task_id):
    """只读后台任务阶段/回复，刷新不重试收费请求。"""
    return chat_tasks.get(task_id)


@ai_operations_bp.post('/tasks/<task_id>/cancel')
@login_required
@endpoint
def cancel_chat_task(task_id):
    """取消未开始问题或停止后续调用，不撤回已产生费用和响应。"""
    if request_body()!={}:raise DomainError('取消任务不接受其他参数')
    return chat_tasks.cancel(task_id,request.current_user['id'])


@ai_operations_bp.get('/plans/<plan_id>/confirmation')
@login_required
@endpoint
def plan_confirmation(plan_id):
    """只读人工方案逐项确认/失效/真实执行状态，不调用平台。"""
    return confirmations.view(plan_id)


@ai_operations_bp.post('/plans/<plan_id>/confirmation')
@login_required
@endpoint
def decide_plan_actions(plan_id):
    """原hash与行动编号的明确确认/拒绝，确认只入队，正式修改由独立消费者完成。"""
    return confirmations.decide(plan_id,request_body(),request.current_user['id']),202


@ai_operations_bp.get('/plans/<plan_id>/confirmation/executions')
@login_required
@endpoint
def confirmed_execution_records(plan_id):
    """只读人工确认对应的完整原执行记录，查看不触发提交或平台回读。"""
    return confirmations.records(plan_id)
