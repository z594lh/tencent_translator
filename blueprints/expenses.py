"""
记账模块 — 支出记录管理（增删改查、报销状态切换、发票上传、统计汇总）

路由一览:
  GET     /api/expenses/list                 分页查询支出列表
  GET     /api/expenses/summary              统计汇总（总计 / 按月 / 按分类 / 按账户）
  POST    /api/expenses/add                  新增支出
  PUT     /api/expenses/<id>                 编辑支出
  PATCH   /api/expenses/<id>/reimburse       切换报销状态（取反）
  DELETE  /api/expenses/<id>                 删除支出
  GET     /api/expenses/<id>/logs            操作日志
  GET     /api/expenses/users                用户列表（筛选下拉）
  POST    /api/expenses/upload-invoice       上传发票图片
"""
# ============================================================
# 导入
# ============================================================
from flask import Blueprint, request, jsonify
import os
import re
import json
import base64
import uuid
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

from blueprints.user_auth import login_required, permission_required
from services.mysql_service import get_db_connection

expenses_bp = Blueprint('expenses', __name__, url_prefix='/api')

# ============================================================
# 配置 & 常量
# ============================================================
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

INVOICE_UPLOAD_DIR = os.path.join('static', 'invoices')
os.makedirs(INVOICE_UPLOAD_DIR, exist_ok=True)

ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}


# ============================================================
# 工具: 序列化（Decimal / datetime → JSON-safe）
# ============================================================
def _to_json_serializable(obj):
    """将数据库返回的 Decimal、datetime 等类型转为可 JSON 序列化的值"""
    if isinstance(obj, dict):
        return {k: _to_json_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_json_serializable(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return obj


# ============================================================
# 工具: 字段差异对比（用于日志精确定位变更）
# ============================================================
def _diff_fields(old_data, new_data):
    """对比两个字典，只返回值真正发生变化的字段"""
    diff_old = {}
    diff_new = {}
    for k, v in new_data.items():
        old_v = old_data.get(k)
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


# ============================================================
# 工具: 操作日志
# ============================================================
def log_expense_action(conn, expense_id, action, user_id, old_data=None, new_data=None):
    """记录支出操作日志（CREATE / UPDATE / DELETE）"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO expense_logs (expense_id, action, user_id, old_data, new_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (expense_id, action, user_id,
                  json.dumps(_to_json_serializable(old_data), ensure_ascii=False) if old_data else None,
                  json.dumps(_to_json_serializable(new_data), ensure_ascii=False) if new_data else None))
            conn.commit()
    except Exception:
        pass  # 日志记录失败不影响主流程


# ============================================================
# 工具: 图片/文件
# ============================================================
def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def save_base64_image(image_data):
    """
    将 base64 图片数据保存为文件，返回可访问的 URL。
    支持 data URI 格式和纯 base64 字符串；不是 base64 则原样返回。
    """
    if not image_data:
        return image_data

    ext_map = {
        'image/jpeg': 'jpg', 'image/jpg': 'jpg',
        'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif',
    }

    if image_data.startswith('data:'):
        match = re.match(r'data:([^;]+);base64,(.+)', image_data)
        if match:
            mime_type = match.group(1)
            base64_str = match.group(2)
            ext = ext_map.get(mime_type, 'jpg')
        else:
            return image_data
    else:
        base64_str = image_data
        ext = 'jpg'

    try:
        img_bytes = base64.b64decode(base64_str)
    except Exception:
        return image_data

    unique_name = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
    save_path = os.path.join(INVOICE_UPLOAD_DIR, unique_name)
    with open(save_path, 'wb') as f:
        f.write(img_bytes)

    relative_url = f"/static/invoices/{unique_name}"
    return f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url


# ============================================================
# 跨模块 API: 供其他模块在事务内创建支出记录
# ============================================================
def create_expense_for_source(conn, category, amount, date, remark, source_type, source_no, account_type='company'):
    """
    供其他模块调用的支出记录创建函数（在已有事务中调用）。
    返回新创建的 expense id，失败返回 None。
    user_id/created_by/updated_by 固定为 0（系统），日志也记录为系统操作。
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO expenses (user_id, date, category, source_type, source_no, amount, remark, has_invoice, account_type, reimbursed, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, 0, %s, %s)
            """, (0, date, category, source_type, source_no, amount, remark, account_type, 0, 0))
            new_id = cursor.lastrowid

        log_expense_action(
            conn, new_id, 'CREATE', 0,
            old_data=None,
            new_data={'date': str(date), 'category': category, 'source_type': source_type, 'source_no': source_no,
                      'amount': float(amount), 'remark': remark, 'account_type': account_type}
        )
        return new_id
    except Exception as e:
        print(f"[Expenses] 自动创建支出记录异常: {e}")
        return None


# ============================================================
# 辅助: 构建筛选条件（多个路由共用）
# ============================================================
def _build_filter_conditions(month, category, account_type, reimbursed, created_by, source_no):
    """根据查询参数构建 WHERE 条件和参数列表"""
    conditions = []
    params = []

    if month:
        conditions.append("DATE_FORMAT(e.date, '%%Y-%%m') = %s")
        params.append(month)
    if category:
        conditions.append("e.category = %s")
        params.append(category)
    if account_type:
        conditions.append("e.account_type = %s")
        params.append(account_type)
    if reimbursed:
        val = reimbursed.lower()
        if val == 'false':
            conditions.append("e.reimbursed = %s")
            params.append(0)
        elif val == 'true':
            conditions.append("e.reimbursed = %s")
            params.append(1)
        elif val in ('0', '1'):
            conditions.append("e.reimbursed = %s")
            params.append(int(val))
    if created_by:
        conditions.append("e.created_by = %s")
        params.append(int(created_by))
    if source_no:
        conditions.append("e.source_no LIKE %s")
        params.append(f"%{source_no}%")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, tuple(params)


# ============================================================
# 路由: 支出列表（分页 + 筛选）
# ============================================================
@expenses_bp.route('/expenses/list', methods=['GET'])
@login_required
@permission_required('expenses:page')
def get_expense_list():
    """
    获取支出列表
    查询参数: month, category, account_type, reimbursed, created_by, source_no, page, page_size
    """
    try:
        month = request.args.get('month', '').strip()
        category = request.args.get('category', '').strip()
        account_type = request.args.get('account_type', '').strip()
        reimbursed = request.args.get('reimbursed', '').strip()
        created_by = request.args.get('created_by', '').strip()
        source_no = request.args.get('source_no', '').strip()
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:
            page_size = 20

        conn = get_db_connection()
        try:
            where_clause, where_params = _build_filter_conditions(
                month, category, account_type, reimbursed, created_by, source_no
            )

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as total FROM expenses e WHERE {where_clause}", where_params)
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                list_sql = f"""
                    SELECT
                        e.id, e.date, e.category, e.source_type, e.source_no, e.amount, e.remark,
                        e.has_invoice, e.invoice_image, e.account_type, e.reimbursed,
                        e.created_at, e.updated_at,
                        e.created_by, COALESCE(cu.nickname, '系统') as created_by_name,
                        e.updated_by, COALESCE(uu.nickname, '系统') as updated_by_name
                    FROM expenses e
                    LEFT JOIN users cu ON e.created_by = cu.id
                    LEFT JOIN users uu ON e.updated_by = uu.id
                    WHERE {where_clause}
                    ORDER BY e.date DESC, e.created_at DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(list_sql, where_params + (page_size, offset))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {"list": rows, "total": total, "page": page, "page_size": page_size}
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 获取列表异常: {e}")
        return jsonify({"status": "error", "message": f"获取失败: {e}"}), 500


# ============================================================
# 路由: 新增支出
# ============================================================
@expenses_bp.route('/expenses/add', methods=['POST'])
@login_required
@permission_required('expenses:create')
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

                log_expense_action(
                    conn, new_id, 'CREATE', user_id,
                    old_data=None,
                    new_data={'date': date, 'category': category, 'amount': amount,
                              'remark': remark, 'has_invoice': has_invoice,
                              'invoice_image': invoice_image, 'account_type': account_type,
                              'reimbursed': reimbursed}
                )

                return jsonify({"status": "success", "message": "新增成功", "data": {"id": new_id}}), 201
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 新增异常: {e}")
        return jsonify({"status": "error", "message": f"新增失败: {e}"}), 500


# ============================================================
# 路由: 编辑支出
# ============================================================
@expenses_bp.route('/expenses/<int:id>', methods=['PUT'])
@login_required
@permission_required('expenses:edit')
def update_expense(id):
    """
    更新支出记录（支持部分更新）
    请求参数: {date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed}
    """
    try:
        user_id = request.current_user['id']
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, date, category, amount, remark, has_invoice,
                           invoice_image, account_type, reimbursed
                    FROM expenses WHERE id = %s AND user_id = %s
                """, (id, user_id))
                old_row = cursor.fetchone()
                if not old_row:
                    return jsonify({"status": "error", "message": "记录不存在或无权修改"}), 404

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

                update_fields.append("updated_by = %s")
                params.append(user_id)
                params.append(id)

                sql = f"UPDATE expenses SET {', '.join(update_fields)} WHERE id = %s"
                cursor.execute(sql, tuple(params))
                conn.commit()

                diff_old, diff_new = _diff_fields(old_row, changed_data)
                log_expense_action(conn, id, 'UPDATE', user_id, old_data=diff_old, new_data=diff_new)

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 更新异常: {e}")
        return jsonify({"status": "error", "message": f"更新失败: {e}"}), 500


# ============================================================
# 路由: 切换报销状态（取反）
# ============================================================
@expenses_bp.route('/expenses/<int:id>/reimburse', methods=['PATCH'])
@login_required
@permission_required('expenses:reimburse')
def toggle_reimburse_status(id):
    """切换私账报销状态（取反）"""
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, reimbursed FROM expenses WHERE id = %s", (id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在"}), 404

                new_status = 0 if row['reimbursed'] else 1
                cursor.execute(
                    "UPDATE expenses SET reimbursed = %s, updated_by = %s WHERE id = %s",
                    (new_status, user_id, id)
                )
                conn.commit()

                log_expense_action(
                    conn, id, 'UPDATE', user_id,
                    old_data={'reimbursed': row['reimbursed']},
                    new_data={'reimbursed': new_status}
                )

                return jsonify({"status": "success", "message": "状态切换成功", "data": {"reimbursed": new_status}})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 切换报销状态异常: {e}")
        return jsonify({"status": "error", "message": f"切换失败: {e}"}), 500


# ============================================================
# 路由: 删除支出
# ============================================================
@expenses_bp.route('/expenses/<int:id>', methods=['DELETE'])
@login_required
@permission_required('expenses:delete')
def delete_expense(id):
    """删除支出记录（同时删除关联发票文件）"""
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT id, date, category, amount, remark, has_invoice,
                              invoice_image, account_type, reimbursed
                       FROM expenses WHERE id = %s AND user_id = %s""",
                    (id, user_id)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在或无权删除"}), 404

                cursor.execute("DELETE FROM expenses WHERE id = %s", (id,))
                conn.commit()

                log_expense_action(conn, id, 'DELETE', user_id, old_data=row, new_data=None)

                # 删除本地发票文件
                invoice_image = row.get('invoice_image', '')
                if invoice_image and invoice_image.startswith('/static/'):
                    local_path = invoice_image.lstrip('/')
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                        except Exception as file_err:
                            print(f"[Expenses] 删除发票文件失败: {file_err}")

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 删除异常: {e}")
        return jsonify({"status": "error", "message": f"删除失败: {e}"}), 500


# ============================================================
# 路由: 操作日志
# ============================================================
@expenses_bp.route('/expenses/<int:id>/logs', methods=['GET'])
@login_required
@permission_required('expenses:page')
def get_expense_logs(id):
    """获取某笔支出的操作日志"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        l.id, l.expense_id, l.action, l.user_id,
                        COALESCE(u.nickname, '系统') as operator_name,
                        l.old_data, l.new_data, l.created_at
                    FROM expense_logs l
                    LEFT JOIN users u ON l.user_id = u.id
                    WHERE l.expense_id = %s
                    ORDER BY l.created_at DESC
                """, (id,))
                rows = cursor.fetchall()

                return jsonify({"status": "success", "data": rows})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 查询日志异常: {e}")
        return jsonify({"status": "error", "message": f"查询失败: {e}"}), 500


# ============================================================
# 路由: 用户列表（筛选下拉）
# ============================================================
@expenses_bp.route('/expenses/users', methods=['GET'])
@login_required
@permission_required('expenses:page')
def get_expense_users():
    """获取所有活跃用户列表，供支出筛选下拉框使用"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, username, nickname FROM users
                    WHERE status = 1 ORDER BY nickname, username
                """)
                rows = cursor.fetchall()
                return jsonify({"status": "success", "data": rows})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Expenses] 查询用户列表异常: {e}")
        return jsonify({"status": "error", "message": f"查询失败: {e}"}), 500


# ============================================================
# 路由: 上传发票图片
# ============================================================
@expenses_bp.route('/expenses/upload-invoice', methods=['POST'])
@login_required
@permission_required('expenses:upload_invoice')
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

        ext = file.filename.rsplit('.', 1)[1].lower()
        unique_name = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
        save_path = os.path.join(INVOICE_UPLOAD_DIR, unique_name)
        file.save(save_path)

        relative_url = f"/static/invoices/{unique_name}"
        image_url = f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url

        return jsonify({"status": "success", "message": "上传成功", "data": {"url": image_url}})

    except Exception as e:
        print(f"[Expenses] 上传发票异常: {e}")
        return jsonify({"status": "error", "message": f"上传失败: {e}"}), 500


# ============================================================
# 路由: 统计汇总（总计 / 按月 / 按分类 / 按账户）
# ============================================================
@expenses_bp.route('/expenses/summary', methods=['GET'])
@login_required
@permission_required('expenses:page')
def get_expense_summary():
    """
    支出统计汇总，支持与列表相同的筛选条件。
    查询参数: month, category, account_type, reimbursed, created_by, source_no
    返回:
      total_amount, total_count          — 总计
      by_month  [{month, amount, count}] — 按月汇总
      by_category [{category, amount, count}] — 按分类汇总
      by_account_type [{account_type, amount, count}] — 按账户汇总
      by_reimbursed [{reimbursed, amount, count}] — 按报销状态汇总（0=未报销, 1=已报销）
    """
    try:
        month = request.args.get('month', '').strip()
        category = request.args.get('category', '').strip()
        account_type = request.args.get('account_type', '').strip()
        reimbursed = request.args.get('reimbursed', '').strip()
        created_by = request.args.get('created_by', '').strip()
        source_no = request.args.get('source_no', '').strip()

        conn = get_db_connection()
        try:
            where_clause, where_params = _build_filter_conditions(
                month, category, account_type, reimbursed, created_by, source_no
            )

            with conn.cursor() as cursor:
                # 总计
                cursor.execute(f"""
                    SELECT COUNT(*) as total_count, COALESCE(SUM(e.amount), 0) as total_amount
                    FROM expenses e WHERE {where_clause}
                """, where_params)
                totals = cursor.fetchone()

                # 按月汇总
                cursor.execute(f"""
                    SELECT DATE_FORMAT(e.date, '%%Y-%%m') as month,
                           COALESCE(SUM(e.amount), 0) as amount,
                           COUNT(*) as count
                    FROM expenses e
                    WHERE {where_clause}
                    GROUP BY DATE_FORMAT(e.date, '%%Y-%%m')
                    ORDER BY month DESC
                """, where_params)
                by_month = cursor.fetchall()

                # 按分类汇总
                cursor.execute(f"""
                    SELECT e.category,
                           COALESCE(SUM(e.amount), 0) as amount,
                           COUNT(*) as count
                    FROM expenses e
                    WHERE {where_clause}
                    GROUP BY e.category
                    ORDER BY amount DESC
                """, where_params)
                by_category = cursor.fetchall()

                # 按账户汇总
                cursor.execute(f"""
                    SELECT e.account_type,
                           COALESCE(SUM(e.amount), 0) as amount,
                           COUNT(*) as count
                    FROM expenses e
                    WHERE {where_clause}
                    GROUP BY e.account_type
                    ORDER BY amount DESC
                """, where_params)
                by_account_type = cursor.fetchall()

                # 按报销状态汇总
                cursor.execute(f"""
                    SELECT e.reimbursed,
                           COALESCE(SUM(e.amount), 0) as amount,
                           COUNT(*) as count
                    FROM expenses e
                    WHERE {where_clause}
                    GROUP BY e.reimbursed
                    ORDER BY e.reimbursed
                """, where_params)
                by_reimbursed = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "total_amount": float(totals['total_amount']),
                        "total_count": totals['total_count'],
                        "by_month": by_month,
                        "by_category": by_category,
                        "by_account_type": by_account_type,
                        "by_reimbursed": by_reimbursed,
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Expenses] 统计汇总异常: {e}")
        return jsonify({"status": "error", "message": f"查询失败: {e}"}), 500
