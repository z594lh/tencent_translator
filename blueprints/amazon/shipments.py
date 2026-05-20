"""
Amazon 货件模块（多店铺支持版）
提供货件、货件商品、仓库查询与同步路由，以及底层数据库操作

注意：所有接口必须传入 shop_id，不传直接返回 400
"""
from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_sp_api_client, get_shop_by_id
from services.mysql_service import get_db_connection

amazon_shipments_bp = Blueprint('amazon_shipments', __name__, url_prefix='/api')


def _require_shop_id() -> int:
    """强制获取 shop_id，不传则抛异常"""
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


# ==================== 路由（前端调用）====================

@amazon_shipments_bp.route('/amazon/warehouses', methods=['GET'])
@login_required
@permission_required('amazon_shipments:warehouses')
def amazon_warehouses():
    """
    查询 FBA 目的仓库列表（用于前端下拉筛选）
    查询参数（必填）:
        shop_id - 店铺ID
    """
    try:
        shop_id = _require_shop_id()
        result = _get_fba_warehouses(shop_id=shop_id)

        return jsonify({
            "status": "success",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Warehouses DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/shipments/<shipment_id>/labels', methods=['GET'])
@login_required
@permission_required('amazon_shipments:labels')
def amazon_shipment_labels(shipment_id):
    """
    获取指定货件的 FBA 箱贴标签
    查询参数（必填）:
        shop_id     - 店铺ID
    查询参数（可选）:
        box_id      - 指定箱子编号，传了则打印单箱唛；不传则打印该货件所有箱子
        page_type   - 标签页面类型，默认 PackageLabel_Thermal_NonPCP
        label_type  - 标签类型，默认 UNIQUE
    """
    try:
        shop_id = _require_shop_id()
        page_type = request.args.get('page_type', 'PackageLabel_Thermal_NonPCP').strip() or 'PackageLabel_Thermal_NonPCP'
        label_type = request.args.get('label_type', 'UNIQUE').strip() or 'UNIQUE'
        box_id = request.args.get('box_id', '').strip() or None

        if box_id:
            box_ids = [box_id]
        else:
            box_ids = _get_box_ids_by_shipment_id(shop_id=shop_id, shipment_id=shipment_id)
            if not box_ids:
                return jsonify({
                    "status": "error",
                    "message": f"未找到货件 {shipment_id} 的箱子记录，请先同步入库计划箱子数据"
                }), 404

        client = get_sp_api_client(shop_id=shop_id)
        labels = client.get_shipment_labels(
            shipment_id=shipment_id,
            carton_ids=box_ids,
            page_type=page_type,
            label_type=label_type
        )

        return jsonify({
            "status": "success",
            "data": labels
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Shipment Labels] 获取异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 同步与数据库操作 ====================

def _get_box_ids_by_shipment_id(shop_id, shipment_id):
    """
    根据货件编号从 amazon_inbound_plan_boxes 表查询所有 box_id
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT box_id FROM amazon_inbound_plan_boxes
                WHERE shop_id = %s AND shipment_id = %s AND box_id IS NOT NULL AND box_id != ''
                ORDER BY box_id
            """
            cursor.execute(sql, (shop_id, shipment_id))
            rows = cursor.fetchall()
            return [row['box_id'] for row in rows]
    finally:
        conn.close()


def _get_fba_warehouses(shop_id):
    """从数据库查询 FBA 仓库列表
    
    说明：fba_warehouses 是亚马逊官方仓库，按 marketplace_id 查询即可，
          同一站点下所有店铺看到同一批仓库，不需要按 shop_id 隔离。
    """
    shop = get_shop_by_id(shop_id)
    if not shop:
        raise ValueError(f"未找到店铺 (shop_id={shop_id})")
    return get_fba_warehouses_from_db(marketplace_id=shop["marketplace_id"])


# ==================== 数据库操作 ====================

def get_fba_warehouses_from_db(marketplace_id=None):
    """
    查询 FBA 仓库列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if marketplace_id:
                sql = """
                    SELECT warehouse_id, marketplace_id, sync_time
                    FROM fba_warehouses
                    WHERE marketplace_id = %s
                    ORDER BY warehouse_id
                """
                cursor.execute(sql, (marketplace_id,))
            else:
                sql = """
                    SELECT warehouse_id, marketplace_id, sync_time
                    FROM fba_warehouses
                    ORDER BY warehouse_id
                """
                cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()
