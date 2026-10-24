"""业务字典薄接口：登录可读，维护沿用系统界面配置权限。"""
import logging
from functools import wraps
import pymysql
from flask import Blueprint,jsonify,request
from blueprints.user_auth import login_required, permission_required
from services.listing_performance.common import DomainError
from services import business_dictionary as service

business_dictionary_bp=Blueprint('business_dictionary',__name__,url_prefix='/api')
logger=logging.getLogger(__name__)


@business_dictionary_bp.after_request
def no_cache(response):
    """所有响应禁止浏览器/代理缓存，包括鉴权失败。"""
    response.headers['Cache-Control']='no-store';return response


def endpoint(function):
    """只返回中文业务错误；不暴露数据库原文或凭据。"""
    @wraps(function)
    def wrapped(*args,**kwargs):
        """统一只读/保存响应，保存失败不会自动重试。"""
        try:return jsonify(status='success',data=function(*args,**kwargs))
        except DomainError as error:return jsonify(status='error',message=str(error)),error.status
        except pymysql.MySQLError:return jsonify(status='error',message='业务字典存储未就绪，请核对本轮SQL；不自动重试保存'),503
        except Exception as error:
            logger.error('业务字典接口异常：%s',type(error).__name__)
            return jsonify(status='error',message='业务字典处理未完成，请刷新核对'),503
    return wrapped


@business_dictionary_bp.get('/business-dictionary')
@login_required
@endpoint
def shared_dictionary():
    """已登录页面共享同一份字典；不开放登录前公开读，也不带审计人员信息。"""
    value=service.snapshot()
    value['list']=[{k:v for k,v in row.items() if k not in ('updated_by_name','updated_at')} for row in value['list']]
    return value


@business_dictionary_bp.get('/system/business-dictionary')
@login_required
@permission_required('system:ui_config')
@endpoint
def dictionary_management():
    """维护页面读取默认、覆盖和版本，不创建配置记录。"""
    return service.snapshot(force=True)


@business_dictionary_bp.put('/system/business-dictionary')
@login_required
@permission_required('system:ui_config')
@endpoint
def save_dictionary():
    """只保存单词条的名称/解释，代码和别名由后端固定注册。"""
    if not request.is_json or not 0<(request.content_length or 0)<=16384:raise DomainError('请提交不超过16KB的JSON词条',422)
    return service.update(request.get_json(silent=True),request.current_user)


@business_dictionary_bp.get('/system/business-dictionary/history')
@login_required
@permission_required('system:ui_config')
@endpoint
def dictionary_history():
    """查看词条修改前后与操作人，恢复默认也保留记录。"""
    return service.history(request.args)
