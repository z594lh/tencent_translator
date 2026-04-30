"""
记账模块 - 支出记录管理（增删改查、报销状态切换、发票上传）
"""
from flask import Blueprint, request, jsonify
import os
import re
import json
import base64
import uuid
from datetime import datetime
from dotenv import load_dotenv

# 导入登录验证装饰器
from blueprints.user_auth import login_required

# 导入数据库连接
from services.mysql_service import get_db_connection

# 创建 Blueprint
expenses_bp = Blueprint('expenses', __name__, url_prefix='/api')

# 加载环境变量
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

# 发票图片保存目录
INVOICE_UPLOAD_DIR = os.path.join('static', 'invoices')
os.makedirs(INVOICE_UPLOAD_DIR, exist_ok=True)

# 允许的图片格式
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}


def _to_json_serializable(obj):
    """将数据库返回的 Decimal、datetime 等类型转为可 JSON 序列化的值"""
    from decimal import Decimal
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_serializable(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return obj


def _diff_fields(old_data, new_data):
    """对比两个字典，只返回值真正发生变化的字段"""
    from decimal import Decimal
    diff_old = {}
    diff_new = {}
    for k, v in new_data.items():
        old_v = old_data.get(k)
        # 数值类型（int/float/Decimal）统一用 float 比较
        if isinstance(old_v, (int, float, Decimal)) or isinstance(v, (int, float, Decimal)):
            try:
                if float(old_v) != float(v):
                    diff_old[k] = old_v
                    diff_new[k] = v
            except (ValueError, TypeError):
                if str(old_v) != str(v):
                    diff_old[k] = old_v
                    diff_new[k] = v
        elif str(old_v) != str(v):
            diff_old[k] = old_v
            diff_new[k] = v
    return diff_old, diff_new


def log_expense_action(conn, expense_id, action, user_id, old_data=None, new_data=None):
    """
    记录支出操作日志
    action: CREATE / UPDATE / DELETE
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO expense_logs (expense_id, action, user_id, old_data, new_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (expense_id, action, user_id,
                  json.dumps(_to_json_serializable(old_data), ensure_ascii=False) if old_data else None,
                  json.dumps(_to_json_serializable(new_data), ensure_ascii=False) if new_data else None))
            conn.commit()
    except Exception as e:
        print(f"记录日志异常: {str(e)}")
        # 日志记录失败不影响主流程


def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def save_base64_image(image_data):
    """
    将 base64 图片数据保存为文件，返回可访问的 URL。
    支持 data URI 格式 和 纯 base64 字符串。
    如果不是 base64 格式，则原样返回。
    """
    if not image_data:
        return image_data

    # 判断是否是 data URI: data:image/jpeg;base64,/9j/4AAQ...
    mime_type = None
    base64_str = image_data

    if image_data.startswith('data:'):
        match = re.match(r'data:([^;]+);base64,(.+)', image_data)
        if match:
            mime_type = match.group(1)
            base64_str = match.group(2)
        else:
            # 不是标准 data URI，原样返回
            return image_data
    else:
        # 纯 base64，尝试通过前缀判断类型（可选）
        pass

    try:
        img_bytes = base64.b64decode(base64_str)
    except Exception:
        # 解码失败，原样返回（可能是已有 URL）
        return image_data

    # 根据 mime_type 确定扩展名
    ext_map = {
        'image/jpeg': 'jpg',
        'image/jpg': 'jpg',
        'image/png': 'png',
        'image/webp': 'webp',
        'image/gif': 'gif',
    }
    ext = ext_map.get(mime_type, 'jpg')

    unique_name = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
    save_path = os.path.join(INVOICE_UPLOAD_DIR, unique_name)

    with open(save_path, 'wb') as f:
        f.write(img_bytes)

    relative_url = f"/static/invoices/{unique_name}"
    return f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url


# ==================== 获取支出列表 ====================
@expenses_bp.route('/expenses/list', methods=['GET'])
@login_required
def get_expense_list():
    """
    获取支出列表
    查询参数: month, category, account_type, reimbursed, created_by, page, page_size
    """
    try:
        # 查询参数
        month = request.args.get('month', '').strip()
        category = request.args.get('category', '').strip()
        account_type = request.args.get('account_type', '').strip()
        reimbursed = request.args.get('reimbursed', '').strip()
        created_by = request.args.get('created_by', '').strip()
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:
            page_size = 20

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 构建查询条件
                conditions = []
                params = []

                if month:
                    conditions.append("DATE_FORMAT(e.date, '%Y-%m') = %s")
                    params.append(month)
                if category:
                    conditions.append("e.category = %s")
                    params.append(category)
                if account_type:
                    conditions.append("e.account_type = %s")
                    params.append(account_type)
                if reimbursed in ('0', '1'):
                    conditions.append("e.reimbursed = %s")
                    params.append(int(reimbursed))
                if created_by:
                    conditions.append("e.created_by = %s")
                    params.append(int(created_by))

                where_clause = " AND ".join(conditions) if conditions else "1=1"

                # 统计总数
                count_sql = f"SELECT COUNT(*) as total FROM expenses e WHERE {where_clause}"
                cursor.execute(count_sql, tuple(params))
                total = cursor.fetchone()['total']

                # 分页查询
                offset = (page - 1) * page_size
                sql = f"""
                    SELECT
                        e.id, e.date, e.category, e.amount, e.remark,
                        e.has_invoice, e.invoice_image, e.account_type, e.reimbursed,
                        e.created_at, e.updated_at,
                        e.created_by, cu.nickname as created_by_name,
                        e.updated_by, uu.nickname as updated_by_name
                    FROM expenses e
                    LEFT JOIN users cu ON e.created_by = cu.id
                    LEFT JOIN users uu ON e.updated_by = uu.id
                    WHERE {where_clause}
                    ORDER BY e.date DESC, e.created_at DESC
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
        print(f"获取支出列表异常: {str(e)}")
        return jsonify({"status": "error", "message": f"获取失败: {str(e)}"}), 500


# ==================== 新增支出记录 ====================
@expenses_bp.route('/expenses/add', methods=['POST'])
@login_required
def create_expense():
    """
    新增支出记录
    请求参数: {date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed}
    """
    try:
        user_id = request.current_user['id']
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        date = data.get('date', '').strip()
        category = data.get('category', '').strip()
        amount = data.get('amount')
        remark = data.get('remark', '').strip()
        has_invoice = 1 if data.get('has_invoice') else 0
        invoice_image = data.get('invoice_image', '').strip()
        account_type = data.get('account_type', '').strip()
        reimbursed = 1 if data.get('reimbursed') else 0

        # 参数校验
        if not date:
            return jsonify({"status": "error", "message": "日期不能为空"}), 400
        if not category:
            return jsonify({"status": "error", "message": "分类不能为空"}), 400
        if amount is None:
            return jsonify({"status": "error", "message": "金额不能为空"}), 400
        try:
            amount = float(amount)
            if amount < 0:
                return jsonify({"status": "error", "message": "金额不能为负数"}), 400
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "金额格式错误"}), 400
        if not account_type:
            return jsonify({"status": "error", "message": "账户类型不能为空"}), 400

        # 若 invoice_image 是 base64，保存为文件并替换为 URL
        if invoice_image:
            invoice_image = save_base64_image(invoice_image)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO expenses
                    (user_id, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (user_id, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed, user_id, user_id))
                conn.commit()
                new_id = cursor.lastrowid

                # 记录创建日志
                log_expense_action(
                    conn, new_id, 'CREATE', user_id,
                    old_data=None,
                    new_data={
                        'date': date, 'category': category, 'amount': amount,
                        'remark': remark, 'has_invoice': has_invoice,
                        'invoice_image': invoice_image, 'account_type': account_type,
                        'reimbursed': reimbursed
                    }
                )

                return jsonify({
                    "status": "success",
                    "message": "新增成功",
                    "data": {"id": new_id}
                }), 201
        finally:
            conn.close()

    except Exception as e:
        print(f"新增支出异常: {str(e)}")
        return jsonify({"status": "error", "message": f"新增失败: {str(e)}"}), 500


# ==================== 更新支出记录 ====================
@expenses_bp.route('/expenses/<int:id>', methods=['PUT'])
@login_required
def update_expense(id):
    """
    更新支出记录
    请求参数: 可部分更新 {date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed}
    """
    try:
        user_id = request.current_user['id']
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 先查询旧数据
                cursor.execute("""
                    SELECT id, date, category, amount, remark, has_invoice,
                           invoice_image, account_type, reimbursed
                    FROM expenses WHERE id = %s AND user_id = %s
                """, (id, user_id))
                old_row = cursor.fetchone()
                if not old_row:
                    return jsonify({"status": "error", "message": "记录不存在或无权修改"}), 404

                # 构建更新字段，同时收集实际修改后的值用于日志
                update_fields = []
                params = []
                changed_data = {}

                if 'date' in data:
                    update_fields.append("date = %s")
                    params.append(data['date'])
                    changed_data['date'] = data['date']
                if 'category' in data:
                    update_fields.append("category = %s")
                    params.append(data['category'])
                    changed_data['category'] = data['category']
                if 'amount' in data:
                    try:
                        amt = float(data['amount'])
                        if amt < 0:
                            return jsonify({"status": "error", "message": "金额不能为负数"}), 400
                        update_fields.append("amount = %s")
                        params.append(amt)
                        changed_data['amount'] = amt
                    except (ValueError, TypeError):
                        return jsonify({"status": "error", "message": "金额格式错误"}), 400
                if 'remark' in data:
                    update_fields.append("remark = %s")
                    params.append(data['remark'])
                    changed_data['remark'] = data['remark']
                if 'has_invoice' in data:
                    val = 1 if data['has_invoice'] else 0
                    update_fields.append("has_invoice = %s")
                    params.append(val)
                    changed_data['has_invoice'] = val
                if 'invoice_image' in data:
                    invoice_image_val = save_base64_image(data['invoice_image'])
                    update_fields.append("invoice_image = %s")
                    params.append(invoice_image_val)
                    changed_data['invoice_image'] = invoice_image_val
                if 'account_type' in data:
                    update_fields.append("account_type = %s")
                    params.append(data['account_type'])
                    changed_data['account_type'] = data['account_type']
                if 'reimbursed' in data:
                    val = 1 if data['reimbursed'] else 0
                    update_fields.append("reimbursed = %s")
                    params.append(val)
                    changed_data['reimbursed'] = val

                if not update_fields:
                    return jsonify({"status": "error", "message": "没有需要更新的字段"}), 400

                # 加上 updated_by
                update_fields.append("updated_by = %s")
                params.append(user_id)

                params.append(id)
                sql = f"UPDATE expenses SET {', '.join(update_fields)} WHERE id = %s"
                cursor.execute(sql, tuple(params))
                conn.commit()

                # 记录更新日志（只记录真正变化的字段）
                diff_old, diff_new = _diff_fields(old_row, changed_data)
                log_expense_action(
                    conn, id, 'UPDATE', user_id,
                    old_data=diff_old,
                    new_data=diff_new
                )

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"更新支出异常: {str(e)}")
        return jsonify({"status": "error", "message": f"更新失败: {str(e)}"}), 500


# ==================== 切换私账报销状态 ====================
@expenses_bp.route('/expenses/<int:id>/reimburse', methods=['PATCH'])
@login_required
def toggle_reimburse_status(id):
    """
    切换私账报销状态（取反）
    """
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 检查记录是否存在且属于当前用户
                cursor.execute(
                    "SELECT id, reimbursed FROM expenses WHERE id = %s AND user_id = %s",
                    (id, user_id)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在或无权修改"}), 404

                new_status = 0 if row['reimbursed'] else 1
                cursor.execute(
                    "UPDATE expenses SET reimbursed = %s, updated_by = %s WHERE id = %s",
                    (new_status, user_id, id)
                )
                conn.commit()

                # 记录更新日志
                log_expense_action(
                    conn, id, 'UPDATE', user_id,
                    old_data={'reimbursed': row['reimbursed']},
                    new_data={'reimbursed': new_status}
                )

                return jsonify({
                    "status": "success",
                    "message": "状态切换成功",
                    "data": {"reimbursed": new_status}
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"切换报销状态异常: {str(e)}")
        return jsonify({"status": "error", "message": f"切换失败: {str(e)}"}), 500


# ==================== 删除支出记录 ====================
@expenses_bp.route('/expenses/<int:id>', methods=['DELETE'])
@login_required
def delete_expense(id):
    """
    删除支出记录
    """
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 查询记录，同时获取发票图片路径以便删除文件
                cursor.execute(
                    """SELECT id, date, category, amount, remark, has_invoice,
                              invoice_image, account_type, reimbursed
                       FROM expenses WHERE id = %s AND user_id = %s""",
                    (id, user_id)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在或无权删除"}), 404

                # 删除数据库记录
                cursor.execute("DELETE FROM expenses WHERE id = %s", (id,))
                conn.commit()

                # 记录删除日志
                log_expense_action(
                    conn, id, 'DELETE', user_id,
                    old_data=row,
                    new_data=None
                )

                # 尝试删除本地发票图片文件
                invoice_image = row.get('invoice_image', '')
                if invoice_image and invoice_image.startswith('/static/'):
                    local_path = invoice_image.lstrip('/')
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                        except Exception as file_err:
                            print(f"删除发票文件失败: {file_err}")

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"删除支出异常: {str(e)}")
        return jsonify({"status": "error", "message": f"删除失败: {str(e)}"}), 500


# ==================== 查询支出日志 ====================
@expenses_bp.route('/expenses/<int:id>/logs', methods=['GET'])
@login_required
def get_expense_logs(id):
    """
    获取某笔支出的操作日志
    """
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 先校验记录是否属于当前用户
                cursor.execute(
                    "SELECT id FROM expenses WHERE id = %s AND user_id = %s",
                    (id, user_id)
                )
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "记录不存在或无权查看"}), 404

                # 查询日志，关联 users 取操作人昵称
                cursor.execute("""
                    SELECT
                        l.id, l.expense_id, l.action, l.user_id,
                        u.nickname as operator_name,
                        l.old_data, l.new_data, l.created_at
                    FROM expense_logs l
                    LEFT JOIN users u ON l.user_id = u.id
                    WHERE l.expense_id = %s
                    ORDER BY l.created_at DESC
                """, (id,))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": rows
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"查询日志异常: {str(e)}")
        return jsonify({"status": "error", "message": f"查询失败: {str(e)}"}), 500


# ==================== 获取用户列表（用于筛选） ====================
@expenses_bp.route('/expenses/users', methods=['GET'])
@login_required
def get_expense_users():
    """
    获取所有用户列表，用于支出筛选下拉框
    """
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, username, nickname
                    FROM users
                    WHERE status = 1
                    ORDER BY nickname, username
                """)
                rows = cursor.fetchall()
                return jsonify({
                    "status": "success",
                    "data": rows
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"查询用户列表异常: {str(e)}")
        return jsonify({"status": "error", "message": f"查询失败: {str(e)}"}), 500


# ==================== 上传发票图片 ====================
@expenses_bp.route('/expenses/upload-invoice', methods=['POST'])
@login_required
def upload_invoice_image():
    """
    上传发票图片（独立接口，返回图片URL）
    请求: multipart/form-data, 字段名 invoice
    """
    try:
        if 'invoice' not in request.files:
            return jsonify({"status": "error", "message": "未找到上传文件，字段名应为 invoice"}), 400

        file = request.files['invoice']
        if file.filename == '':
            return jsonify({"status": "error", "message": "文件名不能为空"}), 400

        if not allowed_file(file.filename):
            return jsonify({
                "status": "error",
                "message": f"不支持的文件格式，仅允许: {', '.join(ALLOWED_IMAGE_EXTENSIONS)}"
            }), 400

        # 生成唯一文件名
        ext = file.filename.rsplit('.', 1)[1].lower()
        unique_name = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
        save_path = os.path.join(INVOICE_UPLOAD_DIR, unique_name)
        file.save(save_path)

        # 返回可访问的完整URL
        relative_url = f"/static/invoices/{unique_name}"
        image_url = f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url

        return jsonify({
            "status": "success",
            "message": "上传成功",
            "data": {"url": image_url}
        })

    except Exception as e:
        print(f"上传发票异常: {str(e)}")
        return jsonify({"status": "error", "message": f"上传失败: {str(e)}"}), 500
