"""
Amazon SP-API 数据查询接口 - 蓝图路由

从 MySQL 数据库查询已同步的 Amazon 数据，支持前端分页
数据通过 services/amazon_db_sync.py 的同步方法从 API 写入

需要在 .env 中配置好亚马逊凭证后，先调用同步接口写入数据
"""
from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_db_sync import AmazonDbSyncService

amazon_api_bp = Blueprint('amazon_api', __name__, url_prefix='/api')


def _to_camel(data):
    """将下划线键转为驼峰（适配前端习惯，可选）"""
    if isinstance(data, dict):
        return {k: _to_camel(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_to_camel(v) for v in data]
    return data


# ==================== 库存查询 ====================

@amazon_api_bp.route('/amazon/inventory', methods=['GET'])
@login_required
def amazon_inventory():
    """
    从数据库分页查询库存汇总数据
    查询参数:
        seller_sku   - 按卖家SKU筛选
        asin         - 按ASIN筛选
        page         - 页码，默认 1
        page_size    - 每页数量，默认 20
    """
    try:
        seller_sku = request.args.get('seller_sku', '').strip() or None
        asin = request.args.get('asin', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        service = AmazonDbSyncService()
        result = service.get_inventory(
            seller_sku=seller_sku,
            asin=asin,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Inventory DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 货件查询 ====================

@amazon_api_bp.route('/amazon/warehouses', methods=['GET'])
@login_required
def amazon_warehouses():
    """
    查询 FBA 目的仓库列表（用于前端下拉筛选）
    """
    try:
        service = AmazonDbSyncService()
        result = service.get_fba_warehouses()

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Warehouses DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/shipments', methods=['GET'])
@login_required
def amazon_shipments():
    """
    从数据库分页查询货件列表数据
    查询参数:
        status          - 按货件状态筛选，如 WORKING, SHIPPED, RECEIVING
        destination_fc  - 按目的仓库筛选，如 IND9, SCK4
        page            - 页码，默认 1
        page_size       - 每页数量，默认 20
    """
    try:
        status = request.args.get('status', '').strip() or None
        destination_fc = request.args.get('destination_fc', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        service = AmazonDbSyncService()
        result = service.get_shipments(
            shipment_status=status,
            destination_fc=destination_fc,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Shipments DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 货件商品查询 ====================

@amazon_api_bp.route('/amazon/shipments/<shipment_id>/items', methods=['GET'])
@login_required
def amazon_shipment_items(shipment_id):
    """
    从数据库分页查询指定货件的商品数据
    查询参数:
        page       - 页码，默认 1
        page_size  - 每页数量，默认 20
    """
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        service = AmazonDbSyncService()
        result = service.get_shipment_items(
            shipment_id=shipment_id,
            page=page,
            page_size=page_size
        )

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Shipment Items DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 数据同步接口（后台用）====================

@amazon_api_bp.route('/amazon/sync/inventory', methods=['POST'])
@login_required
def sync_amazon_inventory():
    """
    手动触发库存数据同步（从 API 写入数据库）
    请求体可选:
        seller_skus      - SKU列表，如 ["SKU1", "SKU2"]
        start_date_time  - 开始时间，ISO8601
    """
    try:
        data = request.get_json() or {}
        service = AmazonDbSyncService()

        result = service.sync_inventory(
            seller_skus=data.get('seller_skus'),
            start_date_time=data.get('start_date_time'),
            details=True
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 库存同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/sync/shipments', methods=['POST'])
@login_required
def sync_amazon_shipments():
    """
    手动触发货件数据同步（从 API 写入数据库）
    请求体可选:
        status_list      - 状态列表，如 ["WORKING", "SHIPPED"]
        last_update_after - 开始时间，ISO8601
    """
    try:
        data = request.get_json() or {}
        service = AmazonDbSyncService()

        result = service.sync_all_shipments(
            shipment_status_list=data.get('status_list'),
            last_update_after=data.get('last_update_after')
        )

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 货件同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/sync/shipments/<shipment_id>/items', methods=['POST'])
@login_required
def sync_amazon_shipment_items(shipment_id):
    """
    手动触发指定货件的商品数据同步（从 API 写入数据库）
    """
    try:
        service = AmazonDbSyncService()
        result = service.sync_shipment_items_by_id(shipment_id)

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 货件商品同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_api_bp.route('/amazon/sync/all', methods=['POST'])
@login_required
def sync_amazon_all():
    """
    一键同步所有 Amazon 数据（库存 + 货件 + 货件商品）
    """
    try:
        service = AmazonDbSyncService()
        result = service.sync_all()

        return jsonify({
            "status": "success",
            "message": "全量同步完成",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Sync] 全量同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
