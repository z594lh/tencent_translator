"""
产品管理模块 - 系统产品 CRUD
以 seller_sku 为核心维度维护产品信息
支持 multipart/form-data 图片上传
"""
import os
import uuid
import json
import threading
from flask import Blueprint, request, jsonify
from datetime import datetime
from dotenv import load_dotenv

from blueprints.user_auth import login_required
from services.mysql_service import get_db_connection

products_bp = Blueprint('products', __name__, url_prefix='/api')

# 加载环境变量，拼接完整 URL
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

# 产品图片保存目录
PRODUCT_UPLOAD_DIR = os.path.join('static', 'products')
os.makedirs(PRODUCT_UPLOAD_DIR, exist_ok=True)

# 允许的图片格式
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}


NEW_FIELDS = [
    'material_cn', 'material_en',
    'purpose', 'brand', 'model',
    'declare_value', 'currency',
    'hs_code', 'is_electric', 'is_magnetic',
    'amazon_internal_id', 'asin', 'fnsku',
    'vat_number', 'eori_number',
    'sales_url', 'weight_kg', 'dimensions_cm',
    'image_url', 'image_urls'
]


def _get_conn():
    return get_db_connection()


def _build_field_select(base_fields=None):
    """构建查询字段列表"""
    if base_fields is None:
        base_fields = [
            'id', 'seller_sku', 'product_name', 'declare_name_cn', 'declare_name_en',
            'remark', 'status', 'created_at', 'updated_at'
        ]
    return ', '.join(base_fields + NEW_FIELDS)


def _is_multipart():
    """判断当前请求是否为 multipart/form-data"""
    ct = request.content_type
    return ct is not None and 'multipart/form-data' in ct


def _allowed_image(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def _save_product_image(file):
    """
    保存产品图片到本地，返回可访问的完整 URL
    :param file: werkzeug FileStorage
    :return: 完整 URL 或 None
    :raises ValueError: 文件格式不合法
    """
    if not file or file.filename == '':
        return None

    if not _allowed_image(file.filename):
        raise ValueError(f"不支持的图片格式，仅允许: {', '.join(ALLOWED_IMAGE_EXTENSIONS)}")

    ext = file.filename.rsplit('.', 1)[1].lower()
    unique_name = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
    save_path = os.path.join(PRODUCT_UPLOAD_DIR, unique_name)
    file.save(save_path)

    relative_url = f"/static/products/{unique_name}"
    return f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url


def _delete_image_file(image_url):
    """删除本地图片文件（建议在线程中异步调用）"""
    if not image_url:
        return
    try:
        rel = None
        base = BASE_URL.rstrip('/') if BASE_URL else ''
        if base and image_url.startswith(base + '/static/products/'):
            rel = image_url[len(base):]
        elif image_url.startswith('/static/products/'):
            rel = image_url
        if rel:
            file_path = os.path.join('.', rel.lstrip('/'))
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[Products] 已删除旧图: {file_path}")
    except Exception as e:
        print(f"[Products] 删除旧图失败 {image_url}: {e}")


def _extract_product_data(data):
    """从请求数据中提取新增字段值（兼容 dict / werkzeug MultiDict）"""
    def _val(key, default=None, strip=False):
        v = data.get(key, default)
        if v is None:
            return default
        if strip and isinstance(v, str):
            v = v.strip() or None
        return v

    return {
        'material_cn': _val('material_cn', '', True),
        'material_en': _val('material_en', '', True),
        'purpose': _val('purpose', '', True),
        'brand': _val('brand', '', True),
        'model': _val('model', '', True),
        'declare_value': _val('declare_value') or None,
        'currency': _val('currency', 'USD', True),
        'hs_code': _val('hs_code', '', True),
        'is_electric': int(_val('is_electric', 0) or 0),
        'is_magnetic': int(_val('is_magnetic', 0) or 0),
        'amazon_internal_id': _val('amazon_internal_id', '', True),
        'asin': _val('asin', '', True),
        'fnsku': _val('fnsku', '', True),
        'vat_number': _val('vat_number', '', True),
        'eori_number': _val('eori_number', '', True),
        'sales_url': _val('sales_url', '', True),
        'weight_kg': _val('weight_kg') or None,
        'dimensions_cm': _val('dimensions_cm', '', True),
        'image_url': _val('image_url', '', True),
        'image_urls': _val('image_urls', '', True),
    }


# ==================== 独立上传接口 ====================
@products_bp.route('/products/upload-image', methods=['POST'])
@login_required
def upload_product_image():
    """独立上传产品图片，返回完整 URL
    请求: multipart/form-data, 字段名 image
    """
    try:
        file = request.files.get('image')
        if not file:
            return jsonify({"status": "error", "message": "未找到上传文件，字段名应为 image"}), 400

        image_url = _save_product_image(file)
        if not image_url:
            return jsonify({"status": "error", "message": "上传失败"}), 400

        return jsonify({
            "status": "success",
            "message": "上传成功",
            "data": {"url": image_url}
        })
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Products] 上传图片异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== CRUD 接口 ====================
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
                    conditions.append(
                        "(seller_sku LIKE %s OR product_name LIKE %s OR declare_name_cn LIKE %s OR brand LIKE %s OR asin LIKE %s)"
                    )
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val, like_val, like_val])

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
                    SELECT {_build_field_select()}
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
                cursor.execute(f"""
                    SELECT {_build_field_select()}
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
    """创建产品（支持 multipart/form-data 图片上传）"""
    try:
        data = request.form if _is_multipart() else (request.get_json() or {})

        seller_sku = data.get('seller_sku', '').strip()
        product_name = data.get('product_name', '').strip()

        if not seller_sku:
            return jsonify({"status": "error", "message": "Seller SKU 不能为空"}), 400
        if not product_name:
            return jsonify({"status": "error", "message": "产品名称不能为空"}), 400

        extra = _extract_product_data(data)

        # multipart 模式下处理文件上传
        if _is_multipart():
            main_image = request.files.get('main_image')
            if main_image and main_image.filename:
                extra['image_url'] = _save_product_image(main_image)

            images = request.files.getlist('images')
            uploaded_urls = []
            for img in images:
                if img and img.filename:
                    url = _save_product_image(img)
                    if url:
                        uploaded_urls.append(url)
            if uploaded_urls:
                extra['image_urls'] = json.dumps(uploaded_urls, ensure_ascii=False)

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查 seller_sku 是否已存在
                cursor.execute("SELECT id FROM products WHERE seller_sku = %s", (seller_sku,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "Seller SKU 已存在"}), 400

                columns = [
                    'seller_sku', 'product_name', 'declare_name_cn', 'declare_name_en',
                    'remark', 'status'
                ] + NEW_FIELDS
                placeholders = ', '.join(['%s'] * len(columns))
                sql = f"""
                    INSERT INTO products ({', '.join(columns)})
                    VALUES ({placeholders})
                """
                cursor.execute(sql, (
                    seller_sku,
                    product_name,
                    data.get('declare_name_cn', '').strip() or None,
                    data.get('declare_name_en', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1),
                    *[extra[f] for f in NEW_FIELDS]
                ))
                conn.commit()
                new_id = cursor.lastrowid

                return jsonify({"status": "success", "message": "创建成功", "data": {"id": new_id}})
        finally:
            conn.close()

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Products] 创建异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@products_bp.route('/products/<int:product_id>', methods=['PUT'])
@login_required
def update_product(product_id):
    """更新产品（支持 multipart/form-data 图片上传替换）"""
    try:
        data = request.form if _is_multipart() else (request.get_json() or {})

        seller_sku = data.get('seller_sku', '').strip()
        product_name = data.get('product_name', '').strip()

        if not seller_sku:
            return jsonify({"status": "error", "message": "Seller SKU 不能为空"}), 400
        if not product_name:
            return jsonify({"status": "error", "message": "产品名称不能为空"}), 400

        extra = _extract_product_data(data)
        deleted_urls = []

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 查询旧数据（含图片信息）
                cursor.execute(f"""
                    SELECT {_build_field_select()}
                    FROM products WHERE id = %s
                """, (product_id,))
                old_product = cursor.fetchone()

                if not old_product:
                    return jsonify({"status": "error", "message": "产品不存在"}), 404

                # 检查 seller_sku 是否被其他产品占用
                cursor.execute(
                    "SELECT id FROM products WHERE seller_sku = %s AND id != %s",
                    (seller_sku, product_id)
                )
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "Seller SKU 已存在"}), 400

                # 解析旧多图列表
                old_image_url = old_product.get('image_url')
                old_image_urls_str = old_product.get('image_urls') or '[]'
                try:
                    old_image_urls = json.loads(old_image_urls_str) if old_image_urls_str else []
                except Exception:
                    old_image_urls = []

                # multipart 模式下处理文件上传和保留逻辑
                if _is_multipart():
                    # 主图处理
                    main_image = request.files.get('main_image')
                    if main_image and main_image.filename:
                        extra['image_url'] = _save_product_image(main_image)
                        if old_image_url and old_image_url != extra['image_url']:
                            deleted_urls.append(old_image_url)
                    elif 'existing_image_url' in data:
                        # 前端显式指定保留/删除主图
                        extra['image_url'] = data.get('existing_image_url', '').strip() or None
                        if old_image_url and old_image_url != extra['image_url']:
                            deleted_urls.append(old_image_url)
                    else:
                        # 未传文件也未指定 existing，保留原值
                        if old_image_url:
                            extra['image_url'] = old_image_url

                    # 多图处理
                    images = request.files.getlist('images')
                    uploaded_urls = []
                    for img in images:
                        if img and img.filename:
                            url = _save_product_image(img)
                            if url:
                                uploaded_urls.append(url)

                    # 保留的已有图片
                    if 'existing_image_urls' in data:
                        existing_str = data.get('existing_image_urls', '[]').strip()
                        try:
                            existing_urls = json.loads(existing_str) if existing_str else []
                        except Exception:
                            existing_urls = []
                    else:
                        existing_urls = old_image_urls

                    # 合并：保留的旧图 + 新上传（去重保序）
                    merged = list(dict.fromkeys(existing_urls + uploaded_urls))
                    extra['image_urls'] = json.dumps(merged, ensure_ascii=False) if merged else None

                    # 找出被删除的旧图
                    for old_url in old_image_urls:
                        if old_url not in merged:
                            deleted_urls.append(old_url)
                else:
                    # JSON 模式下：如果前端传了 image_url / image_urls 直接用；
                    # 如果没传（None），则保留原值
                    if extra.get('image_url') is None and old_image_url:
                        extra['image_url'] = old_image_url
                    new_urls_str = extra.get('image_urls')
                    if new_urls_str:
                        try:
                            new_urls = json.loads(new_urls_str) if new_urls_str else []
                        except Exception:
                            new_urls = []
                        for old_url in old_image_urls:
                            if old_url not in new_urls:
                                deleted_urls.append(old_url)
                    else:
                        # 没传 image_urls，保留原值
                        if old_image_urls:
                            extra['image_urls'] = old_image_urls_str

                set_clause = ', '.join([
                    'seller_sku=%s', 'product_name=%s', 'declare_name_cn=%s',
                    'declare_name_en=%s', 'remark=%s', 'status=%s'
                ] + [f"{f}=%s" for f in NEW_FIELDS])
                sql = f"""
                    UPDATE products
                    SET {set_clause}
                    WHERE id = %s
                """
                cursor.execute(sql, (
                    seller_sku,
                    product_name,
                    data.get('declare_name_cn', '').strip() or None,
                    data.get('declare_name_en', '').strip() or None,
                    data.get('remark', '').strip() or None,
                    data.get('status', 1),
                    *[extra[f] for f in NEW_FIELDS],
                    product_id
                ))
                conn.commit()

                # 异步清理被删除的旧图
                if deleted_urls:
                    threading.Thread(
                        target=lambda urls: [_delete_image_file(u) for u in urls],
                        args=(deleted_urls,),
                        daemon=True
                    ).start()

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
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
