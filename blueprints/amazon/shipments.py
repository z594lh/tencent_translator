"""
Amazon 货件模块
提供货件、货件商品、仓库查询与同步路由，以及底层数据库操作
"""
import os
from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required
from services.amazon_sp_client import AmazonSpApiClient
from services.mysql_service import get_db_connection

amazon_shipments_bp = Blueprint('amazon_shipments', __name__, url_prefix='/api')

MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")


def _get_client(marketplace_id=None, region=None):
    """获取 Amazon SP-API 客户端实例"""
    return AmazonSpApiClient(
        marketplace_id=marketplace_id or MARKETPLACE_ID,
        region=region
    )


# ==================== 路由（前端调用）====================

@amazon_shipments_bp.route('/amazon/warehouses', methods=['GET'])
@login_required
def amazon_warehouses():
    """
    查询 FBA 目的仓库列表（用于前端下拉筛选）
    """
    try:
        result = _get_fba_warehouses()

        return jsonify({
            "status": "success",
            "data": result
        })

    except Exception as e:
        print(f"[Amazon Warehouses DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_shipments_bp.route('/amazon/shipments/<shipment_id>/labels', methods=['GET'])
@login_required
def amazon_shipment_labels(shipment_id):
    """
    获取指定货件的 FBA 箱贴标签
    查询参数:
        box_id      - 指定箱子编号，传了则打印单箱唛；不传则打印该货件所有箱子
        page_type   - 标签页面类型，默认 PackageLabel_Thermal_NonPCP
        label_type  - 标签类型，默认 UNIQUE
    """
    try:
        page_type = request.args.get('page_type', 'PackageLabel_Thermal_NonPCP').strip() or 'PackageLabel_Thermal_NonPCP'
        label_type = request.args.get('label_type', 'UNIQUE').strip() or 'UNIQUE'
        box_id = request.args.get('box_id', '').strip() or None

        if box_id:
            box_ids = [box_id]
        else:
            box_ids = _get_box_ids_by_shipment_id(shipment_id)
            if not box_ids:
                return jsonify({
                    "status": "error",
                    "message": f"未找到货件 {shipment_id} 的箱子记录，请先同步入库计划箱子数据"
                }), 404

        client = _get_client()
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

    except Exception as e:
        print(f"[Amazon Shipment Labels] 获取异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 分割线 ====================


# ==================== 同步与数据库操作 ====================

def _get_box_ids_by_shipment_id(shipment_id):
    """
    根据货件编号从 amazon_inbound_plan_boxes 表查询所有 box_id
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT box_id FROM amazon_inbound_plan_boxes
                WHERE shipment_id = %s AND box_id IS NOT NULL AND box_id != ''
                ORDER BY box_id
            """
            cursor.execute(sql, (shipment_id,))
            rows = cursor.fetchall()
            return [row['box_id'] for row in rows]
    finally:
        conn.close()



def _get_fba_warehouses():
    """从数据库查询 FBA 仓库列表"""
    return get_fba_warehouses_from_db(marketplace_id=MARKETPLACE_ID)




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

