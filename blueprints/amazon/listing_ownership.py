"""亚马逊Listing负责人分配HTTP入口，不修改Listing商品内容。

URL前缀/api/amazon；候选/列表/预览/提交/取消需assign权限，两类历史需assignment_history。
业务交给listing_performance.ownership_service，事务和并发由Service处理。
domain_response也被绩效路由复用，统一总开关、安全JSON和业务错误响应；
本文件不直接实现负责区间SQL、利润计算或外部同步。
"""
from functools import wraps
from flask import Blueprint, Response, jsonify, request
from blueprints.user_auth import login_required, permission_required
from services.listing_performance import ownership_service as service
from services.listing_performance.common import DomainError, require_enabled, positive_id, json_data
import json
import logging

listing_ownership_bp = Blueprint('listing_ownership', __name__, url_prefix='/api/amazon')


def domain_response(fn):
    """装饰归属/绩效HTTP方法，统一功能开关与响应包络。

    fn返回业务数据或(data,status)；序列化日期/Decimal后输出success/data。
    已鉴权下载路由返回Response时原样传递附件，不把文件塞进JSON包络。
    DomainError按其HTTP状态安全返回，系统异常记录日志并给通用500提示。
    不代替登录/权限装饰器，不启动计算任务。"""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        """执行已鉴权路由，包装JSON或传递文件响应。

        先require_enabled，处理服务自定义状态码和安全错误；不吞异常为成功数据。"""
        try:
            require_enabled()
            data = fn(*args, **kwargs)
            if isinstance(data, Response):
                return data
            status = 200
            if isinstance(data,tuple):
                data,status = data
            return jsonify(json.loads(json_data({'status': 'success', 'data': data}))),status
        except DomainError as error:
            return jsonify(status='error', message=str(error)), error.status
        except Exception:
            logging.getLogger(__name__).exception('Listing ownership request failed')
            return jsonify(status='error', message='操作失败，请联系管理员检查模块配置与数据库'), 500
    return wrapped


@listing_ownership_bp.get('/listing-owners')
@login_required
@permission_required('amazon_listings:assign')
@domain_response
def owners():
    """GET /listing-owners返回启用用户的负责人候选列表。

    assign权限由装饰器检查；只读Service，不新增用户/授权。"""
    return service.candidates()


@listing_ownership_bp.get('/listing-assignments/listings')
@login_required
@permission_required('amazon_listings:assign')
@domain_response
def assignment_list():
    """GET /listing-assignments/listings读取独立分配列表。

    将当前用户ID和query参数交Service校验，返回分页/中文产品名/归属；不改原Listing页。"""
    return service.assignment_list(request.current_user['id'],request.args)


@listing_ownership_bp.post('/listing-assignments/preview')
@login_required
@permission_required('amazon_listings:assign')
@domain_response
def preview():
    """POST /listing-assignments/preview返回签名预览及子SKU影响范围。

    JSON包含身份、操作、目标负责人和日期，默认次日由Service冻结；只读不交接。"""
    return service.preview(request.current_user['id'], request.get_json(silent=True))


@listing_ownership_bp.post('/listing-assignments')
@login_required
@permission_required('amazon_listings:assign')
@domain_response
def assign():
    """POST /listing-assignments提交已签名分配/交接/解除命令。

    只接受preview_id/request_id，实际事务、幂等与审计由Service.apply处理；不改Amazon。"""
    return service.apply(request.current_user['id'], request.get_json(silent=True))


@listing_ownership_bp.post('/listing-assignments/cancel')
@login_required
@permission_required('amazon_listings:assign')
@domain_response
def cancel():
    """POST /listing-assignments/cancel提交取消未来交接命令。

    必须使用cancel动作预览；不会删除已发生负责历史，Service标作废并恢复前区间。"""
    return service.apply(request.current_user['id'], request.get_json(silent=True), 'cancel')


@listing_ownership_bp.get('/listing-assignments/history')
@login_required
@permission_required('amazon_listings:assignment_history')
@domain_response
def history():
    """GET /listing-assignments/history返回SKU完整负责区间历史。

    query须shop_id/marketplace_id/seller_sku，历史权限由装饰器检查；只读含已取消记录。"""
    shop = positive_id(request.args.get('shop_id'), 'shop_id')
    market, sku = request.args.get('marketplace_id'), request.args.get('seller_sku')
    if not market or not sku:
        raise DomainError('需要 marketplace_id 和 seller_sku')
    return service.history(shop, market, sku)


@listing_ownership_bp.get('/listing-assignments/operations')
@login_required
@permission_required('amazon_listings:assignment_history')
@domain_response
def operations():
    """GET /listing-assignments/operations返回SKU分页操作审计。

    校验店铺/站点/SKU及分页，包含谁操作、变更前后负责人和理由；不是负责区间列表。"""
    shop = positive_id(request.args.get('shop_id'),'shop_id')
    market,sku = request.args.get('marketplace_id'),request.args.get('seller_sku')
    if not market or not sku or len(market) > 32 or len(sku) > 255:
        raise DomainError('需要有效的 marketplace_id 和 seller_sku')
    return service.operation_history(shop,market,sku,
        positive_id(request.args.get('page',1),'page'),positive_id(request.args.get('page_size',20),'page_size'))
