"""
产品管理模块 - 系统产品 CRUD
以 seller_sku 为核心维度维护产品信息
"""
from flask import Blueprint, request, jsonify
from datetime import datetime

from blueprints.user_auth import login_required
from services.mysql_service import get_db_connection

products_bp = Blueprint('products', __name__, url_prefix='/api')


def _get_conn():
    return get_db_connection()


@products_bp.route('/products', methods=['GET'])
@login_required
def list_products():
    """查询产品列表（支持分页、关键字搜索）"""
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
                    conditions.append("(seller_sku LIKE %s OR product_name LIKE %s OR declare_name_cn LIKE %s)")
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val])

                if status is not None:
                    conditions.append("status = %s")
                    params.append(int(status))

                where_clause = " AND ".join(conditions)

                # 统计总数
                cursor.execute(f"SELECT COUNT(*) as total FROM products WHERE {where_clause}", tuple(params))
                total = cursor.fetchone()['total']

                # 分页查询
                offset = (page - 1) * page_size
                sql = f"""
                    SELECT id, seller_sku, product_name, declare_name_cn, declare_name_en,
                           remark, status, created_at, updated_at
                    FROM products
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
        print(f"[Products] 查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@products_bp.route('/products/<int:product_id>', methods=['GET'])
@login_required
def get_product(product_id):
    """查询单个产品详情"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, seller_sku, product_name, declare_name_cn, declare_name_en,
                           remark, status, created_at, updated_at
                    FROM products WHERE id = %s
                """, (product_id,))
                row = cursor.fetchone()

                if not row:
                    return jsonify({"status": "error", "message": "产品不存在"}), 404

                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Products] 查询详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@products_bp.route('/products', methods=['POST'])
@login_required
def create_product():
    """创建产品"""
    try:
        data = request.get_json() or {}
        seller_sku = data.get('seller_sku', '').strip()
        product_name = data.get('product_name', '').strip()

        if not seller_sku:
            return jsonify({"status": "error", "message": "Seller SKU 不能为空"}), 400
        if not product_name:
            return jsonify({"status": "error", "message": "产品名称不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查 seller_sku 是否已存在
                cursor.execute("SELECT id FROM products WHERE seller_sku = %s", (seller_sku,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "Seller SKU 已存在"}), 400

                sql = """
                    INSERT INTO products (seller_sku, product_name, declare_name_cn, declare_name_en, remark, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    seller_sku,
                    product_name,
                    data.get('declare_name_cn', '').strip() or None,
                    data.get('declare_name_en', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1)
                ))
                conn.commit()
                new_id = cursor.lastrowid

                return jsonify({"status": "success", "message": "创建成功", "data": {"id": new_id}})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Products] 创建异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@products_bp.route('/products/<int:product_id>', methods=['PUT'])
@login_required
def update_product(product_id):
    """更新产品"""
    try:
        data = request.get_json() or {}
        seller_sku = data.get('seller_sku', '').strip()
        product_name = data.get('product_name', '').strip()

        if not seller_sku:
            return jsonify({"status": "error", "message": "Seller SKU 不能为空"}), 400
        if not product_name:
            return jsonify({"status": "error", "message": "产品名称不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查产品是否存在
                cursor.execute("SELECT id FROM products WHERE id = %s", (product_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "产品不存在"}), 404

                # 检查 seller_sku 是否被其他产品占用
                cursor.execute("SELECT id FROM products WHERE seller_sku = %s AND id != %s", (seller_sku, product_id))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "Seller SKU 已存在"}), 400

                sql = """
                    UPDATE products
                    SET seller_sku=%s, product_name=%s, declare_name_cn=%s, declare_name_en=%s, remark=%s, status=%s
                    WHERE id = %s
                """
                cursor.execute(sql, (
                    seller_sku,
                    product_name,
                    data.get('declare_name_cn', '').strip() or None,
                    data.get('declare_name_en', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1),
                    product_id
                ))
                conn.commit()

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Products] 更新异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@products_bp.route('/products/<int:product_id>', methods=['DELETE'])
@login_required
def delete_product(product_id):
    """删除产品"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
                conn.commit()

                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "产品不存在"}), 404

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Products] 删除异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
