"""
供应商管理模块 - 供应商 CRUD + 进货单 CRUD
"""
from flask import Blueprint, request, jsonify
import uuid
from datetime import datetime
from decimal import Decimal

from blueprints.user_auth import login_required
from services.mysql_service import get_db_connection

supplier_bp = Blueprint('supplier', __name__, url_prefix='/api')


def _generate_order_no():
    """生成进货单号：PO + 年月日 + 4位随机数"""
    date_str = datetime.now().strftime('%Y%m%d')
    random_suffix = str(uuid.uuid4().int % 10000).zfill(4)
    return f"PO{date_str}{random_suffix}"


def _get_conn():
    return get_db_connection()


# ==================== 供应商 CRUD ====================

@supplier_bp.route('/suppliers', methods=['GET'])
@login_required
def list_suppliers():
    """查询供应商列表（支持分页、搜索）"""
    try:
        keyword = request.args.get('keyword', '').strip() or None
        status = request.args.get('status', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                conditions = ["1=1"]
                params = []

                if keyword:
                    conditions.append("(name LIKE %s OR contact_person LIKE %s OR phone LIKE %s)")
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val])

                if status is not None:
                    conditions.append("status = %s")
                    params.append(int(status))

                where_clause = " AND ".join(conditions)

                # 统计总数
                cursor.execute(f"SELECT COUNT(*) as total FROM suppliers WHERE {where_clause}", tuple(params))
                total = cursor.fetchone()['total']

                # 分页查询
                offset = (page - 1) * page_size
                sql = f"""
                    SELECT id, name, contact_person, phone, email, address, shop_address, remark, status, created_at, updated_at
                    FROM suppliers
                    WHERE {where_clause}
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, tuple(params + [page_size, offset]))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "list": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Suppliers] 查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/suppliers/<int:supplier_id>', methods=['GET'])
@login_required
def get_supplier(supplier_id):
    """查询单个供应商详情"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, contact_person, phone, email, address, shop_address, remark, status, created_at, updated_at
                    FROM suppliers WHERE id = %s
                """, (supplier_id,))
                row = cursor.fetchone()

                if not row:
                    return jsonify({"status": "error", "message": "供应商不存在"}), 404

                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Suppliers] 查询详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/suppliers', methods=['POST'])
@login_required
def create_supplier():
    """创建供应商"""
    try:
        data = request.get_json() or {}
        name = data.get('name', '').strip()
        if not name:
            return jsonify({"status": "error", "message": "供应商名称不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO suppliers (name, contact_person, phone, email, address, shop_address, remark, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    name,
                    data.get('contact_person', '').strip() or None,
                    data.get('phone', '').strip() or None,
                    data.get('email', '').strip() or None,
                    data.get('address', '').strip() or None,
                    data.get('shop_address', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1)
                ))
                conn.commit()
                new_id = cursor.lastrowid

                return jsonify({"status": "success", "message": "创建成功", "data": {"id": new_id}})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Suppliers] 创建异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/suppliers/<int:supplier_id>', methods=['PUT'])
@login_required
def update_supplier(supplier_id):
    """更新供应商"""
    try:
        data = request.get_json() or {}
        name = data.get('name', '').strip()
        if not name:
            return jsonify({"status": "error", "message": "供应商名称不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                sql = """
                    UPDATE suppliers
                    SET name=%s, contact_person=%s, phone=%s, email=%s, address=%s, shop_address=%s, remark=%s, status=%s
                    WHERE id = %s
                """
                cursor.execute(sql, (
                    name,
                    data.get('contact_person', '').strip() or None,
                    data.get('phone', '').strip() or None,
                    data.get('email', '').strip() or None,
                    data.get('address', '').strip() or None,
                    data.get('shop_address', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1),
                    supplier_id
                ))
                conn.commit()

                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "供应商不存在或无需更新"}), 404

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Suppliers] 更新异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/suppliers/<int:supplier_id>', methods=['DELETE'])
@login_required
def delete_supplier(supplier_id):
    """删除供应商（有关联进货单时禁止删除）"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查是否有关联进货单
                cursor.execute("SELECT COUNT(*) as cnt FROM purchase_orders WHERE supplier_id = %s", (supplier_id,))
                cnt = cursor.fetchone()['cnt']
                if cnt > 0:
                    return jsonify({"status": "error", "message": f"该供应商存在 {cnt} 个进货单，无法删除"}), 400

                cursor.execute("DELETE FROM suppliers WHERE id = %s", (supplier_id,))
                conn.commit()

                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "供应商不存在"}), 404

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Suppliers] 删除异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 进货单 CRUD ====================

@supplier_bp.route('/purchase-orders', methods=['GET'])
@login_required
def list_purchase_orders():
    """查询进货单列表（支持分页、按供应商筛选）"""
    try:
        supplier_id = request.args.get('supplier_id', '').strip() or None
        status = request.args.get('status', '').strip() or None
        keyword = request.args.get('keyword', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                conditions = ["1=1"]
                params = []

                if supplier_id:
                    conditions.append("po.supplier_id = %s")
                    params.append(int(supplier_id))

                if status is not None:
                    conditions.append("po.status = %s")
                    params.append(int(status))

                if keyword:
                    conditions.append("(po.order_no LIKE %s OR s.name LIKE %s)")
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val])

                where_clause = " AND ".join(conditions)

                # 统计总数
                count_sql = f"""
                    SELECT COUNT(*) as total FROM purchase_orders po
                    LEFT JOIN suppliers s ON po.supplier_id = s.id
                    WHERE {where_clause}
                """
                cursor.execute(count_sql, tuple(params))
                total = cursor.fetchone()['total']

                # 分页查询
                offset = (page - 1) * page_size
                sql = f"""
                    SELECT po.id, po.order_no, po.supplier_id, s.name as supplier_name,
                           po.product_amount, po.shipping_amount, po.misc_amount, po.total_amount,
                           po.status, po.remark, po.created_at, po.updated_at
                    FROM purchase_orders po
                    LEFT JOIN suppliers s ON po.supplier_id = s.id
                    WHERE {where_clause}
                    ORDER BY po.id DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, tuple(params + [page_size, offset]))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "list": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[PurchaseOrders] 查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/purchase-orders/<int:order_id>', methods=['GET'])
@login_required
def get_purchase_order(order_id):
    """查询单个进货单详情（含明细）"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 主表
                cursor.execute("""
                    SELECT po.id, po.order_no, po.supplier_id, s.name as supplier_name,
                           po.product_amount, po.shipping_amount, po.misc_amount, po.total_amount,
                           po.status, po.remark, po.created_at, po.updated_at
                    FROM purchase_orders po
                    LEFT JOIN suppliers s ON po.supplier_id = s.id
                    WHERE po.id = %s
                """, (order_id,))
                order = cursor.fetchone()

                if not order:
                    return jsonify({"status": "error", "message": "进货单不存在"}), 404

                # 明细
                cursor.execute("""
                    SELECT id, seller_sku, quantity, unit_price, total_price, remark
                    FROM purchase_order_items
                    WHERE order_id = %s
                """, (order_id,))
                items = cursor.fetchall()
                order['items'] = items

                return jsonify({"status": "success", "data": order})
        finally:
            conn.close()

    except Exception as e:
        print(f"[PurchaseOrders] 查询详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/purchase-orders', methods=['POST'])
@login_required
def create_purchase_order():
    """创建进货单（含明细）"""
    try:
        data = request.get_json() or {}
        supplier_id = data.get('supplier_id')
        items = data.get('items', [])

        if not supplier_id:
            return jsonify({"status": "error", "message": "供应商ID不能为空"}), 400

        if not items or not isinstance(items, list):
            return jsonify({"status": "error", "message": "进货明细不能为空"}), 400

        # 计算商品金额并校验明细
        product_amount = Decimal('0')
        for item in items:
            if not item.get('seller_sku', '').strip():
                return jsonify({"status": "error", "message": "进货明细的 Seller SKU 不能为空"}), 400
            qty = int(item.get('quantity', 0))
            price = Decimal(str(item.get('unit_price', 0)))
            item['quantity'] = qty
            item['unit_price'] = float(price)
            item['total_price'] = float(price * qty)
            product_amount += Decimal(str(item['total_price']))

        shipping_amount = Decimal(str(data.get('shipping_amount', 0)))
        misc_amount = Decimal(str(data.get('misc_amount', 0)))
        total_amount = product_amount + shipping_amount + misc_amount

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 插入主表
                order_no = _generate_order_no()
                cursor.execute("""
                    INSERT INTO purchase_orders (order_no, supplier_id, product_amount, shipping_amount, misc_amount, total_amount, status, remark)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order_no,
                    supplier_id,
                    float(product_amount),
                    float(shipping_amount),
                    float(misc_amount),
                    float(total_amount),
                    data.get('status', 0),
                    data.get('remark', '').strip() or None
                ))
                order_id = cursor.lastrowid

                # 插入明细
                for item in items:
                    cursor.execute("""
                        INSERT INTO purchase_order_items (order_id, seller_sku, quantity, unit_price, total_price, remark)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        order_id,
                        item.get('seller_sku', '').strip() or None,
                        item['quantity'],
                        item['unit_price'],
                        item['total_price'],
                        item.get('remark', '').strip() or None
                    ))

                conn.commit()
                return jsonify({"status": "success", "message": "创建成功", "data": {"id": order_id, "order_no": order_no}})
        finally:
            conn.close()

    except Exception as e:
        print(f"[PurchaseOrders] 创建异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/purchase-orders/<int:order_id>', methods=['PUT'])
@login_required
def update_purchase_order(order_id):
    """更新进货单（含明细：先删后插）"""
    try:
        data = request.get_json() or {}
        supplier_id = data.get('supplier_id')
        items = data.get('items', [])

        if not supplier_id:
            return jsonify({"status": "error", "message": "供应商ID不能为空"}), 400

        if not items or not isinstance(items, list):
            return jsonify({"status": "error", "message": "进货明细不能为空"}), 400

        # 计算商品金额并校验明细
        product_amount = Decimal('0')
        for item in items:
            if not item.get('seller_sku', '').strip():
                return jsonify({"status": "error", "message": "进货明细的 Seller SKU 不能为空"}), 400
            qty = int(item.get('quantity', 0))
            price = Decimal(str(item.get('unit_price', 0)))
            item['quantity'] = qty
            item['unit_price'] = float(price)
            item['total_price'] = float(price * qty)
            product_amount += Decimal(str(item['total_price']))

        shipping_amount = Decimal(str(data.get('shipping_amount', 0)))
        misc_amount = Decimal(str(data.get('misc_amount', 0)))
        total_amount = product_amount + shipping_amount + misc_amount

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 先确认进货单存在（避免 rowcount==0 误判）
                cursor.execute("SELECT id FROM purchase_orders WHERE id = %s", (order_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "进货单不存在"}), 404

                # 更新主表
                cursor.execute("""
                    UPDATE purchase_orders
                    SET supplier_id=%s, product_amount=%s, shipping_amount=%s, misc_amount=%s, total_amount=%s, status=%s, remark=%s
                    WHERE id = %s
                """, (
                    supplier_id,
                    float(product_amount),
                    float(shipping_amount),
                    float(misc_amount),
                    float(total_amount),
                    data.get('status', 0),
                    data.get('remark', '').strip() or None,
                    order_id
                ))

                # 删除旧明细
                cursor.execute("DELETE FROM purchase_order_items WHERE order_id = %s", (order_id,))

                # 插入新明细
                for item in items:
                    cursor.execute("""
                        INSERT INTO purchase_order_items (order_id, seller_sku, quantity, unit_price, total_price, remark)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        order_id,
                        item.get('seller_sku', '').strip() or None,
                        item['quantity'],
                        item['unit_price'],
                        item['total_price'],
                        item.get('remark', '').strip() or None
                    ))

                conn.commit()
                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[PurchaseOrders] 更新异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@supplier_bp.route('/purchase-orders/<int:order_id>', methods=['DELETE'])
@login_required
def delete_purchase_order(order_id):
    """删除进货单（级联删除明细）"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM purchase_orders WHERE id = %s", (order_id,))
                conn.commit()

                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "进货单不存在"}), 404

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[PurchaseOrders] 删除异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
