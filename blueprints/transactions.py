"""
收支记账模块 — 交易记录管理（增删改查、报销状态切换、发票上传、统计汇总）

数据模型:
  transactions 表: id, transaction_type, user_id, date, category, source_type, source_no,
    amount, remark, has_invoice, invoice_image, account_type, reimbursed, created_by, updated_by
  transaction_categories 表: id, code, name, type, color, sort_order, is_active

路由一览:
  GET     /api/options/transactions/categories    下拉框分类列表（在 options.py）
  GET     /api/transactions/list                  分页查询列表
  GET     /api/transactions/summary               统计汇总
  POST    /api/transactions/add                   新增记录
  PUT     /api/transactions/<id>                  编辑记录
  PATCH   /api/transactions/<id>/reimburse        切换报销状态
  DELETE  /api/transactions/<id>                  删除记录
  GET     /api/transactions/<id>/logs             操作日志
  POST    /api/transactions/upload-invoice        上传发票图片
  GET     /api/transactions/categories            管理页分类列表（可分页）
  POST    /api/transactions/categories            新增分类
  PUT     /api/transactions/categories/<id>       更新分类
  DELETE  /api/transactions/categories/<id>       删除分类
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

transactions_bp = Blueprint('transactions', __name__, url_prefix='/api')

# ============================================================
# 配置 & 常量
# ============================================================
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

INVOICE_UPLOAD_DIR = os.path.join('static', 'invoices')
os.makedirs(INVOICE_UPLOAD_DIR, exist_ok=True)

ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}

VALID_TRANSACTION_TYPES = {'expense', 'income', 'adjustment'}


# ============================================================
# 工具: 序列化（Decimal / datetime → JSON-safe）
# ============================================================
def _to_json_serializable(obj):
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
# 工具: 字段差异对比
# ============================================================
def _diff_fields(old_data, new_data):
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
# 工具: 分类校验
# ============================================================
def _validate_category(conn, category_code, transaction_type):
    """校验分类编码是否属于当前 transaction_type 或 all"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM transaction_categories WHERE code = %s AND is_active = 1 AND type IN (%s, 'all')",
                (category_code, transaction_type)
            )
            return cursor.fetchone() is not None
    except Exception:
        return True  # 分类表可能尚未创建，兼容处理


# ============================================================
# 工具: 操作日志
# ============================================================
def log_transaction_action(conn, transaction_id, action, user_id, old_data=None, new_data=None):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO transaction_logs (transaction_id, action, user_id, old_data, new_data)
                VALUES (%s, %s, %s, %s, %s)
            """, (transaction_id, action, user_id,
                  json.dumps(_to_json_serializable(old_data), ensure_ascii=False) if old_data else None,
                  json.dumps(_to_json_serializable(new_data), ensure_ascii=False) if new_data else None))
            conn.commit()
    except Exception:
        pass


# ============================================================
# 工具: 图片/文件
# ============================================================
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def save_base64_image(image_data):
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
# 跨模块 API: 供其他模块在事务内创建交易记录
# ============================================================
def create_transaction_for_source(conn, category, amount, date, remark, source_type, source_no, account_type='company'):
    """
    供其他模块调用的交易记录创建函数（在已有事务中调用）。
    transaction_type 固定为 'expense'。
    返回新创建的 transaction id，失败返回 None。
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO transactions
                (transaction_type, user_id, date, category, source_type, source_no, amount, remark, has_invoice, account_type, reimbursed, created_by, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, 0, %s, %s)
            """, ('expense', 0, date, category, source_type, source_no, amount, remark, account_type, 0, 0))
            new_id = cursor.lastrowid

        log_transaction_action(
            conn, new_id, 'CREATE', 0,
            old_data=None,
            new_data={'transaction_type': 'expense', 'date': str(date), 'category': category,
                      'source_type': source_type, 'source_no': source_no,
                      'amount': float(amount), 'remark': remark, 'account_type': account_type}
        )
        return new_id
    except Exception as e:
        print(f"[Transactions] 自动创建交易记录异常: {e}")
        return None


# ============================================================
# 辅助: 构建筛选条件
# ============================================================
def _build_filter_conditions(start_date, end_date, category, account_type, reimbursed, created_by, source_no, transaction_type=None):
    conditions = []
    params = []

    if transaction_type:
        types = [t.strip() for t in transaction_type.split(',') if t.strip() in VALID_TRANSACTION_TYPES]
        if types:
            placeholders = ','.join(['%s'] * len(types))
            conditions.append(f"t.transaction_type IN ({placeholders})")
            params.extend(types)

    if start_date:
        conditions.append("t.date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("t.date <= %s")
        params.append(end_date)
    if category:
        conditions.append("t.category = %s")
        params.append(category)
    if account_type:
        conditions.append("t.account_type = %s")
        params.append(account_type)
    if reimbursed:
        val = reimbursed.lower()
        if val == 'false':
            conditions.append("t.reimbursed = %s")
            params.append(0)
        elif val == 'true':
            conditions.append("t.reimbursed = %s")
            params.append(1)
        elif val in ('0', '1'):
            conditions.append("t.reimbursed = %s")
            params.append(int(val))
    if created_by:
        conditions.append("t.created_by = %s")
        params.append(int(created_by))
    if source_no:
        conditions.append("t.source_no LIKE %s")
        params.append(f"%{source_no}%")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, tuple(params)


# ============================================================
# ============================================================
# 路由: 交易记录
# ============================================================
# ============================================================


# ============================================================
# 路由: 交易列表（分页 + 筛选）
# ============================================================
@transactions_bp.route('/transactions/list', methods=['GET'])
@login_required
@permission_required('transactions:page')
def get_transaction_list():
    """
    获取交易列表
    查询参数: transaction_type, start_date, end_date, category, account_type, reimbursed, created_by, source_no, page, page_size
    """
    try:
        start_date = request.args.get('start_date', '').strip()
        end_date = request.args.get('end_date', '').strip()
        category = request.args.get('category', '').strip()
        account_type = request.args.get('account_type', '').strip()
        reimbursed = request.args.get('reimbursed', '').strip()
        created_by = request.args.get('created_by', '').strip()
        source_no = request.args.get('source_no', '').strip()
        transaction_type = request.args.get('transaction_type', '').strip()
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 100:
            page_size = 20

        conn = get_db_connection()
        try:
            where_clause, where_params = _build_filter_conditions(
                start_date, end_date, category, account_type, reimbursed, created_by, source_no, transaction_type
            )

            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as total FROM transactions t WHERE {where_clause}", where_params)
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                list_sql = f"""
                    SELECT
                        t.id, t.transaction_type, t.date, t.category, t.source_type, t.source_no, t.amount, t.remark,
                        t.has_invoice, t.invoice_image, t.account_type, t.reimbursed,
                        t.created_at, t.updated_at,
                        t.created_by, COALESCE(cu.nickname, '系统') as created_by_name,
                        t.updated_by, COALESCE(uu.nickname, '系统') as updated_by_name
                    FROM transactions t
                    LEFT JOIN users cu ON t.created_by = cu.id
                    LEFT JOIN users uu ON t.updated_by = uu.id
                    WHERE {where_clause}
                    ORDER BY t.date DESC, t.created_at DESC
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
        print(f"[Transactions] 获取列表异常: {e}")
        return jsonify({"status": "error", "message": f"获取失败: {e}"}), 500


# ============================================================
# 路由: 新增交易记录
# ============================================================
@transactions_bp.route('/transactions/add', methods=['POST'])
@login_required
@permission_required('transactions:create')
def create_transaction():
    """
    新增交易记录
    请求参数: {transaction_type, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed}
    """
    try:
        user_id = request.current_user['id']
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        transaction_type = data.get('transaction_type', 'expense').strip()
        date = data.get('date', '').strip()
        category = data.get('category', '').strip()
        amount = data.get('amount')
        remark = data.get('remark', '').strip()
        has_invoice = 1 if data.get('has_invoice') else 0
        invoice_image = data.get('invoice_image', '').strip()
        account_type = data.get('account_type', '').strip()
        reimbursed = 1 if data.get('reimbursed') else 0

        if transaction_type not in VALID_TRANSACTION_TYPES:
            return jsonify({"status": "error", "message": f"交易类型无效，仅支持: {', '.join(VALID_TRANSACTION_TYPES)}"}), 400
        if not date:
            return jsonify({"status": "error", "message": "日期不能为空"}), 400
        if not category:
            return jsonify({"status": "error", "message": "分类不能为空"}), 400
        if amount is None:
            return jsonify({"status": "error", "message": "金额不能为空"}), 400
        try:
            amount = float(amount)
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "金额格式错误"}), 400

        if transaction_type in ('expense', 'income'):
            if amount < 0:
                return jsonify({"status": "error", "message": "支出/收入金额不能为负数"}), 400
        elif transaction_type == 'adjustment':
            if amount == 0:
                return jsonify({"status": "error", "message": "盘盈冲正金额不能为0"}), 400

        if not account_type:
            return jsonify({"status": "error", "message": "账户类型不能为空"}), 400

        if account_type == 'company' and reimbursed:
            return jsonify({"status": "error", "message": "公账记录不支持报销"}), 400

        if invoice_image:
            invoice_image = save_base64_image(invoice_image)

        conn = get_db_connection()
        try:
            if not _validate_category(conn, category, transaction_type):
                return jsonify({"status": "error", "message": f"分类 '{category}' 不适用于 {transaction_type} 类型"}), 400

            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO transactions
                    (transaction_type, user_id, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed, created_by, updated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (transaction_type, user_id, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed, user_id, user_id))
                conn.commit()
                new_id = cursor.lastrowid

                log_transaction_action(
                    conn, new_id, 'CREATE', user_id,
                    old_data=None,
                    new_data={'transaction_type': transaction_type, 'date': date, 'category': category,
                              'amount': amount, 'remark': remark, 'has_invoice': has_invoice,
                              'invoice_image': invoice_image, 'account_type': account_type,
                              'reimbursed': reimbursed}
                )

                return jsonify({"status": "success", "message": "新增成功", "data": {"id": new_id}}), 201
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 新增异常: {e}")
        return jsonify({"status": "error", "message": f"新增失败: {e}"}), 500


# ============================================================
# 路由: 编辑交易记录
# ============================================================
@transactions_bp.route('/transactions/<int:id>', methods=['PUT'])
@login_required
@permission_required('transactions:edit')
def update_transaction(id):
    """
    更新交易记录（支持部分更新）
    请求参数: {transaction_type, date, category, amount, remark, has_invoice, invoice_image, account_type, reimbursed}
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
                    SELECT id, transaction_type, date, category, amount, remark, has_invoice,
                           invoice_image, account_type, reimbursed
                    FROM transactions WHERE id = %s AND user_id = %s
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
                    except (ValueError, TypeError):
                        return jsonify({"status": "error", "message": "金额格式错误"}), 400

                    ttype = data.get('transaction_type', old_row['transaction_type'])
                    if ttype in ('expense', 'income'):
                        if amt < 0:
                            return jsonify({"status": "error", "message": "支出/收入金额不能为负数"}), 400
                    elif ttype == 'adjustment':
                        if amt == 0:
                            return jsonify({"status": "error", "message": "盘盈冲正金额不能为0"}), 400

                    update_fields.append("amount = %s")
                    params.append(amt)
                    changed_data['amount'] = amt
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

                # 校验 account_type + reimbursed
                ttype = data.get('transaction_type', old_row['transaction_type'])
                atype = data.get('account_type', old_row['account_type'])
                rmb = changed_data.get('reimbursed', old_row.get('reimbursed', 0))
                if atype == 'company' and rmb:
                    return jsonify({"status": "error", "message": "公账记录不支持报销"}), 400

                # 校验分类
                new_cat = data.get('category', old_row['category'])
                if 'category' in data and not _validate_category(conn, new_cat, ttype):
                    return jsonify({"status": "error", "message": f"分类 '{new_cat}' 不适用于 {ttype} 类型"}), 400
                if 'transaction_type' in data and not _validate_category(conn, new_cat, ttype):
                    return jsonify({"status": "error", "message": f"分类 '{new_cat}' 不适用于 {ttype} 类型"}), 400

                update_fields.append("updated_by = %s")
                params.append(user_id)
                params.append(id)

                sql = f"UPDATE transactions SET {', '.join(update_fields)} WHERE id = %s"
                cursor.execute(sql, tuple(params))
                conn.commit()

                diff_old, diff_new = _diff_fields(old_row, changed_data)
                log_transaction_action(conn, id, 'UPDATE', user_id, old_data=diff_old, new_data=diff_new)

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 更新异常: {e}")
        return jsonify({"status": "error", "message": f"更新失败: {e}"}), 500


# ============================================================
# 路由: 切换报销状态
# ============================================================
@transactions_bp.route('/transactions/<int:id>/reimburse', methods=['PATCH'])
@login_required
@permission_required('transactions:reimburse')
def toggle_reimburse_status(id):
    """切换报销状态（仅 transaction_type=expense 且 account_type=personal 的记录有效）"""
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id, transaction_type, account_type, reimbursed FROM transactions WHERE id = %s",
                    (id,)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在"}), 404

                if row['transaction_type'] != 'expense':
                    return jsonify({"status": "error", "message": "仅支出记录支持报销"}), 400
                if row['account_type'] != 'personal':
                    return jsonify({"status": "error", "message": "仅私账支出支持报销"}), 400

                new_status = 0 if row['reimbursed'] else 1
                cursor.execute(
                    "UPDATE transactions SET reimbursed = %s, updated_by = %s WHERE id = %s",
                    (new_status, user_id, id)
                )
                conn.commit()

                log_transaction_action(
                    conn, id, 'UPDATE', user_id,
                    old_data={'reimbursed': row['reimbursed']},
                    new_data={'reimbursed': new_status}
                )

                return jsonify({"status": "success", "message": "状态切换成功", "data": {"reimbursed": new_status}})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 切换报销状态异常: {e}")
        return jsonify({"status": "error", "message": f"切换失败: {e}"}), 500


# ============================================================
# 路由: 删除交易记录
# ============================================================
@transactions_bp.route('/transactions/<int:id>', methods=['DELETE'])
@login_required
@permission_required('transactions:delete')
def delete_transaction(id):
    """删除交易记录（同时删除关联发票文件）"""
    try:
        user_id = request.current_user['id']

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT id, transaction_type, date, category, amount, remark, has_invoice,
                              invoice_image, account_type, reimbursed
                       FROM transactions WHERE id = %s AND user_id = %s""",
                    (id, user_id)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在或无权删除"}), 404

                cursor.execute("DELETE FROM transactions WHERE id = %s", (id,))
                conn.commit()

                log_transaction_action(conn, id, 'DELETE', user_id, old_data=row, new_data=None)

                invoice_image = row.get('invoice_image', '')
                if invoice_image and invoice_image.startswith('/static/'):
                    local_path = invoice_image.lstrip('/')
                    if os.path.exists(local_path):
                        try:
                            os.remove(local_path)
                        except Exception as file_err:
                            print(f"[Transactions] 删除发票文件失败: {file_err}")

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 删除异常: {e}")
        return jsonify({"status": "error", "message": f"删除失败: {e}"}), 500


# ============================================================
# 路由: 操作日志
# ============================================================
@transactions_bp.route('/transactions/<int:id>/logs', methods=['GET'])
@login_required
@permission_required('transactions:page')
def get_transaction_logs(id):
    """获取某笔交易记录的操作日志"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        l.id, l.transaction_id, l.action, l.user_id,
                        COALESCE(u.nickname, '系统') as operator_name,
                        l.old_data, l.new_data, l.created_at
                    FROM transaction_logs l
                    LEFT JOIN users u ON l.user_id = u.id
                    WHERE l.transaction_id = %s
                    ORDER BY l.created_at DESC
                """, (id,))
                rows = cursor.fetchall()

                return jsonify({"status": "success", "data": rows})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 查询日志异常: {e}")
        return jsonify({"status": "error", "message": f"查询失败: {e}"}), 500


# ============================================================
# 路由: 上传发票图片
# ============================================================
@transactions_bp.route('/transactions/upload-invoice', methods=['POST'])
@login_required
@permission_required('transactions:upload_invoice')
def upload_invoice_image():
    """
    上传发票/凭证图片（独立接口，返回图片URL）
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
        print(f"[Transactions] 上传发票异常: {e}")
        return jsonify({"status": "error", "message": f"上传失败: {e}"}), 500


# ============================================================
# 路由: 统计汇总
# ============================================================
@transactions_bp.route('/transactions/summary', methods=['GET'])
@login_required
@permission_required('transactions:page')
def get_transaction_summary():
    """
    交易统计汇总，支持与列表相同的筛选条件。
    查询参数: transaction_type, start_date, end_date, category, account_type, reimbursed, created_by, source_no
    返回:
      total_count, net_amount,
      by_type      [{transaction_type, amount, count}]
      by_category  [{transaction_type, category, amount, count}]
    """
    try:
        start_date = request.args.get('start_date', '').strip()
        end_date = request.args.get('end_date', '').strip()
        category = request.args.get('category', '').strip()
        account_type = request.args.get('account_type', '').strip()
        reimbursed = request.args.get('reimbursed', '').strip()
        created_by = request.args.get('created_by', '').strip()
        source_no = request.args.get('source_no', '').strip()
        transaction_type = request.args.get('transaction_type', '').strip()

        conn = get_db_connection()
        try:
            where_clause, where_params = _build_filter_conditions(
                start_date, end_date, category, account_type, reimbursed, created_by, source_no, transaction_type
            )

            with conn.cursor() as cursor:
                # 总计（净值: 收入 - 支出 + 调整）
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_count,
                        COALESCE(SUM(CASE
                            WHEN t.transaction_type = 'income' THEN t.amount
                            WHEN t.transaction_type = 'expense' THEN -t.amount
                            ELSE t.amount
                        END), 0) as net_amount
                    FROM transactions t WHERE {where_clause}
                """, where_params)
                totals = cursor.fetchone()

                # 按类型分组
                cursor.execute(f"""
                    SELECT t.transaction_type,
                           COALESCE(SUM(t.amount), 0) as amount,
                           COUNT(*) as count
                    FROM transactions t
                    WHERE {where_clause}
                    GROUP BY t.transaction_type
                    ORDER BY t.transaction_type
                """, where_params)
                by_type = cursor.fetchall()

                # 按分类分组（带 transaction_type）
                cursor.execute(f"""
                    SELECT t.transaction_type, t.category,
                           COALESCE(SUM(t.amount), 0) as amount,
                           COUNT(*) as count
                    FROM transactions t
                    WHERE {where_clause}
                    GROUP BY t.transaction_type, t.category
                    ORDER BY t.transaction_type, amount DESC
                """, where_params)
                by_category = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "total_count": totals['total_count'],
                        "net_amount": float(totals['net_amount']) if totals['net_amount'] is not None else 0,
                        "by_type": by_type,
                        "by_category": by_category,
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 统计汇总异常: {e}")
        return jsonify({"status": "error", "message": f"查询失败: {e}"}), 500


# ============================================================
# ============================================================
# 路由: 分类管理（CRUD）
# ============================================================
# ============================================================


# ============================================================
# 分类管理: 列表（可分页）
# ============================================================
@transactions_bp.route('/transactions/categories', methods=['GET'])
@login_required
@permission_required('transactions:page')
def get_categories():
    """管理页分类列表，支持分页与 type 筛选"""
    try:
        cat_type = request.args.get('type', '').strip()
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 50))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 200:
            page_size = 50

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                conditions = []
                params = []
                if cat_type:
                    conditions.append("type IN (%s, 'all')")
                    params.append(cat_type)

                where_sql = "WHERE is_active = 1" + (" AND " + " AND ".join(conditions) if conditions else "")
                if cat_type:
                    where_sql = "WHERE " + " AND ".join(conditions) + " AND is_active = 1"

                cursor.execute(f"SELECT COUNT(*) as total FROM transaction_categories {where_sql}", params)
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                cursor.execute(f"""
                    SELECT id, code, name, type, color, sort_order, is_active, created_at, updated_at
                    FROM transaction_categories
                    {where_sql}
                    ORDER BY sort_order ASC, id ASC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {"list": rows, "total": total, "page": page, "page_size": page_size}
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 获取分类列表异常: {e}")
        return jsonify({"status": "error", "message": f"获取失败: {e}"}), 500


# ============================================================
# 分类管理: 新增
# ============================================================
@transactions_bp.route('/transactions/categories', methods=['POST'])
@login_required
@permission_required('transactions:create')
def create_category():
    """新增交易分类"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        code = data.get('code', '').strip()
        name = data.get('name', '').strip()
        cat_type = data.get('type', 'expense').strip()
        color = data.get('color', '#95a5a6').strip()
        sort_order = int(data.get('sort_order', 0))

        if not code:
            return jsonify({"status": "error", "message": "编码不能为空"}), 400
        if not name:
            return jsonify({"status": "error", "message": "名称不能为空"}), 400
        if cat_type not in {'expense', 'income', 'adjustment', 'all'}:
            return jsonify({"status": "error", "message": "无效的分类类型"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM transaction_categories WHERE code = %s", (code,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "编码已存在"}), 400

                cursor.execute("""
                    INSERT INTO transaction_categories (code, name, type, color, sort_order)
                    VALUES (%s, %s, %s, %s, %s)
                """, (code, name, cat_type, color, sort_order))
                conn.commit()
                new_id = cursor.lastrowid

                return jsonify({"status": "success", "message": "新增成功", "data": {"id": new_id}}), 201
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 新增分类异常: {e}")
        return jsonify({"status": "error", "message": f"新增失败: {e}"}), 500


# ============================================================
# 分类管理: 编辑
# ============================================================
@transactions_bp.route('/transactions/categories/<int:id>', methods=['PUT'])
@login_required
@permission_required('transactions:edit')
def update_category(id):
    """编辑交易分类"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM transaction_categories WHERE id = %s", (id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "分类不存在"}), 404

                update_fields = []
                params = []

                if 'code' in data:
                    new_code = data['code'].strip()
                    cursor.execute("SELECT id FROM transaction_categories WHERE code = %s AND id != %s", (new_code, id))
                    if cursor.fetchone():
                        return jsonify({"status": "error", "message": "编码已存在"}), 400
                    update_fields.append("code = %s")
                    params.append(new_code)
                if 'name' in data:
                    update_fields.append("name = %s")
                    params.append(data['name'].strip())
                if 'type' in data:
                    new_type = data['type'].strip()
                    if new_type not in {'expense', 'income', 'adjustment', 'all'}:
                        return jsonify({"status": "error", "message": "无效的分类类型"}), 400
                    update_fields.append("type = %s")
                    params.append(new_type)
                if 'color' in data:
                    update_fields.append("color = %s")
                    params.append(data['color'].strip())
                if 'sort_order' in data:
                    update_fields.append("sort_order = %s")
                    params.append(int(data['sort_order']))
                if 'is_active' in data:
                    update_fields.append("is_active = %s")
                    params.append(1 if data['is_active'] else 0)

                if not update_fields:
                    return jsonify({"status": "error", "message": "没有需要更新的字段"}), 400

                params.append(id)
                sql = f"UPDATE transaction_categories SET {', '.join(update_fields)} WHERE id = %s"
                cursor.execute(sql, tuple(params))
                conn.commit()

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 更新分类异常: {e}")
        return jsonify({"status": "error", "message": f"更新失败: {e}"}), 500


# ============================================================
# 分类管理: 删除（仅允许未关联数据时）
# ============================================================
@transactions_bp.route('/transactions/categories/<int:id>', methods=['DELETE'])
@login_required
@permission_required('transactions:delete')
def delete_category(id):
    """删除交易分类（仅允许未被 transactions 引用的分类）"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT code FROM transaction_categories WHERE id = %s", (id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "分类不存在"}), 404

                cursor.execute("SELECT COUNT(*) as cnt FROM transactions WHERE category = %s", (row['code'],))
                usage = cursor.fetchone()
                if usage['cnt'] > 0:
                    return jsonify({"status": "error", "message": f"该分类已被 {usage['cnt']} 条交易记录引用，无法删除"}), 400

                cursor.execute("DELETE FROM transaction_categories WHERE id = %s", (id,))
                conn.commit()

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()

    except Exception as e:
        print(f"[Transactions] 删除分类异常: {e}")
        return jsonify({"status": "error", "message": f"删除失败: {e}"}), 500
