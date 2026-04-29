"""
Amazon SP-API 完整接口 - 蓝图路由

包含所有Amazon SP-API接口的完整实现
需要在 .env 中配置好亚马逊凭证后使用
"""
from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient

amazon_api_bp = Blueprint('amazon_api', __name__, url_prefix='/api')


def _to_camel(data):
    """将下划线键转为驼峰（适配前端习惯，可选）"""
    if isinstance(data, dict):
        return {k: _to_camel(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_to_camel(v) for v in data]
    return data


# ==================== 订单相关 ====================

@amazon_api_bp.route('/amazon/orders', methods=['GET'])
@login_required
def amazon_orders():
    """
    获取订单列表
    查询参数:
        created_after      - 创建时间起，ISO8601，如 2026-04-01T00:00:00Z
        created_before     - 创建时间止
        last_updated_after - 更新时间起
        order_status       - 状态，如 Unshipped,Shipped（逗号分隔）
        max_results        - 每页数量，默认 20
        next_token         - 分页令牌
    """
    try:
        client = AmazonSpApiClient()

        created_after = request.args.get('created_after', '').strip()
        created_before = request.args.get('created_before', '').strip()
        last_updated_after = request.args.get('last_updated_after', '').strip()
        order_status = request.args.get('order_status', '').strip()
        max_results = int(request.args.get('max_results', 20))
        next_token = request.args.get('next_token', '').strip() or None

        kwargs = {"max_results": max_results}
        if created_after:
            kwargs["created_after"] = created_after
        if created_before:
            kwargs["created_before"] = created_before
        if last_updated_after:
            kwargs["last_updated_after"] = last_updated_after
        if order_status:
            kwargs["order_statuses"] = [s.strip() for s in order_status.split(",") if s.strip()]
        if next_token:
            kwargs["next_token"] = next_token

        result = client.get_orders(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"获取亚马逊订单异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/orders/<order_id>', methods=['GET'])
@login_required
def amazon_order_detail(order_id):
    """获取单个订单详情"""
    try:
        client = AmazonSpApiClient()
        result = client.get_order(order_id)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取订单详情异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/orders/<order_id>/items', methods=['GET'])
@login_required
def amazon_order_items(order_id):
    """获取订单商品列表"""
    try:
        client = AmazonSpApiClient()
        result = client.get_order_items(order_id)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取订单商品异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 库存相关 ====================

@amazon_api_bp.route('/amazon/inventory', methods=['GET'])
@login_required
def amazon_inventory():
    """
    获取库存汇总
    查询参数:
        sku        - 指定 SKU（逗号分隔多个）
        details    - 是否包含详情，1 或 0
        next_token - 分页令牌
    """
    try:
        client = AmazonSpApiClient()

        sku = request.args.get('sku', '').strip()
        details = request.args.get('details', '0') == '1'
        next_token = request.args.get('next_token', '').strip() or None

        kwargs = {"details": details}
        if sku:
            kwargs["seller_skus"] = [s.strip() for s in sku.split(",") if s.strip()]
        if next_token:
            kwargs["next_token"] = next_token

        result = client.get_inventory_summaries(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"获取库存异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 报告相关 ====================

@amazon_api_bp.route('/amazon/reports', methods=['GET'])
@login_required
def amazon_reports():
    """
    获取报告列表
    查询参数:
        report_type   - 报告类型，如 GET_FLAT_FILE_OPEN_LISTINGS_DATA
        status        - 处理状态，如 DONE,IN_PROGRESS（逗号分隔）
        page_size     - 每页数量，默认 10
        next_token    - 分页令牌
    """
    try:
        client = AmazonSpApiClient()

        report_type = request.args.get('report_type', '').strip()
        status = request.args.get('status', '').strip()
        page_size = int(request.args.get('page_size', 10))
        next_token = request.args.get('next_token', '').strip() or None

        kwargs = {"page_size": page_size}
        if report_type:
            kwargs["report_types"] = [rt.strip() for rt in report_type.split(",") if rt.strip()]
        if status:
            kwargs["processing_statuses"] = [s.strip() for s in status.split(",") if s.strip()]
        if next_token:
            kwargs["next_token"] = next_token

        result = client.get_reports(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"获取报告列表异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/reports', methods=['POST'])
@login_required
def amazon_create_report():
    """
    创建报告任务
    请求体: { "report_type": "GET_FLAT_FILE_OPEN_LISTINGS_DATA", ... }
    """
    try:
        data = request.get_json() or {}
        report_type = data.get('report_type', '').strip()
        if not report_type:
            return jsonify({"status": "error", "message": "report_type 不能为空"}), 400

        client = AmazonSpApiClient()
        # 透传其余字段（如 marketplace_ids, data_start_time 等）
        extra = {k: v for k, v in data.items() if k != 'report_type'}
        result = client.create_report(report_type, **extra)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"创建报告异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/reports/<report_id>', methods=['GET'])
@login_required
def amazon_report_detail(report_id):
    """获取单个报告详情"""
    try:
        client = AmazonSpApiClient()
        result = client.get_report(report_id)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取报告详情异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/reports/document/<document_id>', methods=['GET'])
@login_required
def amazon_report_document(document_id):
    """获取报告下载信息（含下载 URL）"""
    try:
        client = AmazonSpApiClient()
        result = client.get_report_document(document_id)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取报告文档异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 定价相关 ====================

@amazon_api_bp.route('/amazon/pricing', methods=['GET'])
@login_required
def amazon_pricing():
    """
    获取竞争定价
    查询参数（至少传一个）:
        asin - ASIN（逗号分隔多个）
        sku  - Seller SKU（逗号分隔多个）
    """
    try:
        client = AmazonSpApiClient()

        asin = request.args.get('asin', '').strip()
        sku = request.args.get('sku', '').strip()

        kwargs = {}
        if asin:
            kwargs["asins"] = [a.strip() for a in asin.split(",") if a.strip()]
        if sku:
            kwargs["skus"] = [s.strip() for s in sku.split(",") if s.strip()]

        if not kwargs:
            return jsonify({"status": "error", "message": "请传入 asin 或 sku"}), 400

        result = client.get_competitive_pricing(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"获取定价异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 货件管理 ====================

@amazon_api_bp.route('/amazon/shipments', methods=['GET'])
@login_required
def amazon_shipments():
    """
    获取货件列表
    查询参数:
        days            - 最近天数，默认 30
        status          - 状态，如 WORKING,SHIPPED,RECEIVING（逗号分隔）
        shipment_id     - 货件ID（逗号分隔多个）
        last_update_after - 最后更新时间起
    """
    try:
        client = AmazonSpApiClient()

        days = int(request.args.get('days', 30))
        status = request.args.get('status', '').strip()
        shipment_id = request.args.get('shipment_id', '').strip()
        last_update_after = request.args.get('last_update_after', '').strip()

        from datetime import datetime, timedelta, timezone
        if not last_update_after and days:
            last_update_after = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

        kwargs = {}
        if status:
            kwargs["shipment_status_list"] = [s.strip() for s in status.split(",") if s.strip()]
        elif not shipment_id:  # 如果没有提供shipment_id，则提供默认状态
            kwargs["shipment_status_list"] = ["WORKING", "SHIPPED", "RECEIVING", "CLOSED"]
        if shipment_id:
            kwargs["shipment_id_list"] = [s.strip() for s in shipment_id.split(",") if s.strip()]
        if last_update_after:
            kwargs["last_update_after"] = last_update_after

        result = client.get_shipments(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"获取货件列表异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/shipments/<shipment_id>/items', methods=['GET'])
@login_required
def amazon_shipment_items(shipment_id):
    """获取货件商品列表"""
    try:
        client = AmazonSpApiClient()
        result = client.get_shipment_items(shipment_id)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取货件商品异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 商品目录 ====================

@amazon_api_bp.route('/amazon/catalog/search', methods=['GET'])
@login_required
def amazon_catalog_search():
    """
    搜索商品目录
    查询参数:
        keywords    - 搜索关键词（逗号分隔多个）
        included_data - 包含数据类型，如 summaries,images,offers（逗号分隔）
    """
    try:
        client = AmazonSpApiClient()

        keywords = request.args.get('keywords', '').strip()
        included_data = request.args.get('included_data', 'summaries').strip()

        if not keywords:
            return jsonify({"status": "error", "message": "keywords 不能为空"}), 400

        kwargs = {
            "keywords": [k.strip() for k in keywords.split(",") if k.strip()],
            "included_data": [d.strip() for d in included_data.split(",") if d.strip()]
        }

        result = client.search_catalog_items(**kwargs)
        return jsonify({"status": "success", "data": result})

    except Exception as e:
        print(f"搜索商品目录异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/catalog/items/<asin>', methods=['GET'])
@login_required
def amazon_catalog_item(asin):
    """获取单个商品详情"""
    try:
        client = AmazonSpApiClient()
        included_data = request.args.get('included_data', 'summaries').strip()

        kwargs = {
            "included_data": [d.strip() for d in included_data.split(",") if d.strip()]
        }

        result = client.get_catalog_item(asin, **kwargs)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取商品详情异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 商品Listing ====================

@amazon_api_bp.route('/amazon/listings', methods=['GET'])
@login_required
def amazon_listings():
    """获取所有商品Listing列表"""
    try:
        client = AmazonSpApiClient()
        included_data = request.args.get('included_data', 'summaries').strip()

        kwargs = {}
        if included_data:
            kwargs["included_data"] = [d.strip() for d in included_data.split(",") if d.strip()]

        result = client.get_listings_items(**kwargs)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取商品Listing异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/listings/<sku>', methods=['GET'])
@login_required
def amazon_listing_item(sku):
    """获取单个商品Listing详情"""
    try:
        client = AmazonSpApiClient()
        included_data = request.args.get('included_data', 'summaries').strip()

        kwargs = {}
        if included_data:
            kwargs["included_data"] = [d.strip() for d in included_data.split(",") if d.strip()]

        result = client.get_listings_item(sku, **kwargs)
        return jsonify({"status": "success", "data": result})
    except Exception as e:
        print(f"获取商品Listing详情异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 连接测试 ====================

@amazon_api_bp.route('/amazon/test', methods=['GET'])
@login_required
def amazon_test():
    """
    测试亚马逊 API 连通性
    返回连接状态和所有可用接口概览
    """
    try:
        client = AmazonSpApiClient()

        # 测试多个接口
        test_results = {}

        # 测试库存接口
        try:
            inventory = client.get_inventory_summaries(details=False, max_results=1)
            test_results['inventory'] = {'status': 'success', 'count': len(inventory.get('payload', {}).get('inventorySummaries', []))}
        except Exception as e:
            test_results['inventory'] = {'status': 'error', 'message': str(e)}

        # 测试货件接口
        try:
            shipments = client.get_shipments(max_results=1)
            test_results['shipments'] = {'status': 'success', 'count': len(shipments.get('payload', []))}
        except Exception as e:
            test_results['shipments'] = {'status': 'error', 'message': str(e)}

        # 测试商品接口
        try:
            catalog = client.search_catalog_items(keywords=['test'], max_results=1)
            test_results['catalog'] = {'status': 'success', 'count': len(catalog.get('items', []))}
        except Exception as e:
            test_results['catalog'] = {'status': 'error', 'message': str(e)}

        return jsonify({
            "status": "success",
            "message": "API连接测试完成",
            "tests": test_results,
            "marketplace": client.marketplace_id,
            "region": client.region
        })
    except Exception as e:
        print(f"亚马逊 API 测试异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
