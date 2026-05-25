"""
货代管理与货代运单管理模块
- logistics_providers      货代管理表 CRUD
- logistics_waybills       货代运单表 CURD（一个运单对应一个 FBA 货件）
"""
import io
from datetime import datetime

import openpyxl
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from blueprints.user_auth import login_required, permission_required

logistics_bp = Blueprint('logistics', __name__, url_prefix='/api')

# 运单最终状态（已完成）
WAYBILL_STATUS_COMPLETED = 5


def _get_conn():
    return get_db_connection()


def _val_or_none(val, cast_type=None):
    """如果值为 None 或空字符串则返回 None，否则按类型转换"""
    if val is None or val == '':
        return None
    if cast_type is not None:
        return cast_type(val)
    return val


# ==================== 货代管理 logistics_providers ====================

@logistics_bp.route('/logistics-providers', methods=['GET'])
@login_required
@permission_required('logistics_providers:page')
def list_providers():
    """货代列表（分页 + 关键字搜索）"""
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
                        "(name LIKE %s OR contact_person LIKE %s OR phone LIKE %s)"
                    )
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val])

                if status is not None:
                    conditions.append("status = %s")
                    params.append(int(status))

                where_clause = " AND ".join(conditions)

                cursor.execute(
                    f"SELECT COUNT(*) as total FROM logistics_providers WHERE {where_clause}",
                    tuple(params)
                )
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                sql = f"""
                    SELECT id, name, contact_person, phone, wechat, email,
                           address, payment_terms, default_route, status,
                           remark, created_at, updated_at
                    FROM logistics_providers
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
        print(f"[Logistics] 查询货代列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-providers/<int:provider_id>', methods=['GET'])
@login_required
@permission_required('logistics_providers:page')
def get_provider(provider_id):
    """单个货代详情"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, contact_person, phone, wechat, email,
                           address, payment_terms, default_route, status,
                           remark, created_at, updated_at
                    FROM logistics_providers WHERE id = %s
                """, (provider_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "货代不存在"}), 404
                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 查询货代详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-providers', methods=['POST'])
@login_required
@permission_required('logistics_providers:create')
def create_provider():
    """创建货代"""
    try:
        data = request.get_json() or {}
        name = data.get('name', '').strip()
        if not name:
            return jsonify({"status": "error", "message": "货代名称不能为空"}), 400

        fields = [
            'name', 'contact_person', 'phone', 'wechat', 'email',
            'address', 'payment_terms', 'default_route', 'status', 'remark'
        ]
        values = [
            name,
            data.get('contact_person', '').strip() or None,
            data.get('phone', '').strip() or None,
            data.get('wechat', '').strip() or None,
            data.get('email', '').strip() or None,
            data.get('address', '').strip() or None,
            data.get('payment_terms', '').strip() or None,
            data.get('default_route', '').strip() or None,
            data.get('status', 1),
            data.get('remark', '').strip() or None,
        ]

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(fields))
                sql = f"INSERT INTO logistics_providers ({', '.join(fields)}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(values))
                conn.commit()
                new_id = cursor.lastrowid
                return jsonify({"status": "success", "message": "创建成功", "data": {"id": new_id}})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 创建货代异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-providers/<int:provider_id>', methods=['PUT'])
@login_required
@permission_required('logistics_providers:edit')
def update_provider(provider_id):
    """更新货代"""
    try:
        data = request.get_json() or {}
        name = data.get('name', '').strip()
        if not name:
            return jsonify({"status": "error", "message": "货代名称不能为空"}), 400

        fields = [
            'name', 'contact_person', 'phone', 'wechat', 'email',
            'address', 'payment_terms', 'default_route', 'status', 'remark'
        ]
        values = [
            name,
            data.get('contact_person', '').strip() or None,
            data.get('phone', '').strip() or None,
            data.get('wechat', '').strip() or None,
            data.get('email', '').strip() or None,
            data.get('address', '').strip() or None,
            data.get('payment_terms', '').strip() or None,
            data.get('default_route', '').strip() or None,
            data.get('status', 1),
            data.get('remark', '').strip() or None,
        ]

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                set_clause = ', '.join([f"{f} = %s" for f in fields])
                sql = f"UPDATE logistics_providers SET {set_clause} WHERE id = %s"
                cursor.execute(sql, tuple(values + [provider_id]))
                conn.commit()
                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "货代不存在"}), 404
                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 更新货代异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-providers/batch-status', methods=['PUT'])
@login_required
@permission_required('logistics_providers:edit')
def batch_update_provider_status():
    """批量修改货代状态"""
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
        status = data.get('status')

        if not ids or not isinstance(ids, list):
            return jsonify({"status": "error", "message": "请提供要修改的ID列表"}), 400
        if status is None:
            return jsonify({"status": "error", "message": "请提供目标状态"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(ids))
                sql = f"UPDATE logistics_providers SET status = %s WHERE id IN ({placeholders})"
                cursor.execute(sql, tuple([int(status)] + [int(i) for i in ids]))
                conn.commit()
                return jsonify({
                    "status": "success",
                    "message": f"成功更新 {cursor.rowcount} 条记录",
                    "data": {"affected": cursor.rowcount}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 批量更新货代状态异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-providers/<int:provider_id>', methods=['DELETE'])
@login_required
@permission_required('logistics_providers:delete')
def delete_provider(provider_id):
    """删除货代（有关联运单时禁止删除）"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM logistics_waybills WHERE provider_id = %s LIMIT 1", (provider_id,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "该货代下存在运单，无法删除"}), 400

                cursor.execute("DELETE FROM logistics_providers WHERE id = %s", (provider_id,))
                conn.commit()
                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "货代不存在"}), 404
                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 删除货代异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 货代运单 logistics_waybills ====================

@logistics_bp.route('/logistics-waybills', methods=['GET'])
@login_required
@permission_required('logistics_waybills:page')
def list_waybills():
    """运单列表（分页 + 筛选）"""
    try:
        keyword = request.args.get('keyword', '').strip() or None
        provider_id = request.args.get('provider_id', '').strip() or None
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
                        "(w.waybill_no LIKE %s OR w.route_name LIKE %s OR w.destination_warehouse LIKE %s)"
                    )
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val])

                if provider_id is not None:
                    conditions.append("w.provider_id = %s")
                    params.append(int(provider_id))

                if status is not None:
                    conditions.append("w.status = %s")
                    params.append(int(status))

                where_clause = " AND ".join(conditions)

                # 总数
                cursor.execute(f"""
                    SELECT COUNT(*) as total
                    FROM logistics_waybills w
                    WHERE {where_clause}
                """, tuple(params))
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                sql = f"""
                    SELECT w.*, p.name as provider_name,
                           s.name as shipment_name, s.destination_warehouse_id as destination_fulfillment_center_id, s.status as shipment_status
                    FROM logistics_waybills w
                    LEFT JOIN logistics_providers p ON w.provider_id = p.id
                    LEFT JOIN amazon_inbound_shipments_detail s ON w.shipment_id = s.shipment_confirmation_id
                    WHERE {where_clause}
                    ORDER BY w.id DESC
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
        print(f"[Logistics] 查询运单列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills/<int:waybill_id>', methods=['GET'])
@login_required
@permission_required('logistics_waybills:page')
def get_waybill(waybill_id):
    """单个运单详情（含货代名称与 FBA 货件信息）"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT w.*, p.name as provider_name,
                           s.name as shipment_name, s.destination_warehouse_id as destination_fulfillment_center_id, s.status as shipment_status
                    FROM logistics_waybills w
                    LEFT JOIN logistics_providers p ON w.provider_id = p.id
                    LEFT JOIN amazon_inbound_shipments_detail s ON w.shipment_id = s.shipment_confirmation_id
                    WHERE w.id = %s
                """, (waybill_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "运单不存在"}), 404

                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 查询运单详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills', methods=['POST'])
@login_required
@permission_required('logistics_waybills:create')
def create_waybill():
    """创建运单"""
    try:
        data = request.get_json() or {}
        waybill_no = data.get('waybill_no', '').strip() or None
        provider_id = data.get('provider_id')

        if provider_id is None:
            return jsonify({"status": "error", "message": "货代ID不能为空"}), 400

        shipment_id = data.get('shipment_id', '').strip()
        if not shipment_id:
            return jsonify({"status": "error", "message": "货件ID不能为空"}), 400

        # 校验货代存在
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM logistics_providers WHERE id = %s", (provider_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "货代不存在"}), 400

                fields = [
                    'waybill_no', 'provider_id', 'shipment_id', 'transport_type', 'route_name',
                    'departure_port', 'destination_port', 'destination_warehouse',
                    'total_weight_kg', 'total_volume_cbm', 'total_cartons',
                    'freight_cost_cny', 'tax_cost_cny', 'misc_cost_cny',
                    'total_cost_cny', 'cost_per_kg', 'currency', 'status',
                    'ship_date', 'eta_date', 'arrival_date', 'delivery_date',
                    'tracking_url', 'remark'
                ]
                values = [
                    waybill_no,
                    int(provider_id),
                    shipment_id,
                    data.get('transport_type', 1),
                    data.get('route_name', '').strip() or None,
                    data.get('departure_port', '').strip() or None,
                    data.get('destination_port', '').strip() or None,
                    data.get('destination_warehouse', '').strip() or None,
                    _val_or_none(data.get('total_weight_kg'), float),
                    _val_or_none(data.get('total_volume_cbm'), float),
                    _val_or_none(data.get('total_cartons'), int),
                    _val_or_none(data.get('freight_cost_cny'), float),
                    _val_or_none(data.get('tax_cost_cny'), float),
                    _val_or_none(data.get('misc_cost_cny'), float),
                    _val_or_none(data.get('total_cost_cny'), float),
                    _val_or_none(data.get('cost_per_kg'), float),
                    data.get('currency', '').strip() or None,
                    data.get('status', 0),
                    data.get('ship_date') or None,
                    data.get('eta_date') or None,
                    data.get('arrival_date') or None,
                    data.get('delivery_date') or None,
                    data.get('tracking_url', '').strip() or None,
                    data.get('remark', '').strip() or None,
                ]

                placeholders = ', '.join(['%s'] * len(fields))
                sql = f"INSERT INTO logistics_waybills ({', '.join(fields)}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(values))
                conn.commit()
                new_id = cursor.lastrowid
                return jsonify({"status": "success", "message": "创建成功", "data": {"id": new_id}})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 创建运单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills/<int:waybill_id>', methods=['PUT'])
@login_required
@permission_required('logistics_waybills:edit')
def update_waybill(waybill_id):
    """更新运单"""
    try:
        data = request.get_json() or {}
        waybill_no = data.get('waybill_no', '').strip() or None
        provider_id = data.get('provider_id')

        if provider_id is None:
            return jsonify({"status": "error", "message": "货代ID不能为空"}), 400

        shipment_id = data.get('shipment_id', '').strip()
        if not shipment_id:
            return jsonify({"status": "error", "message": "货件ID不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM logistics_providers WHERE id = %s", (provider_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "货代不存在"}), 400

                fields = [
                    'waybill_no', 'provider_id', 'shipment_id', 'transport_type', 'route_name',
                    'departure_port', 'destination_port', 'destination_warehouse',
                    'total_weight_kg', 'total_volume_cbm', 'total_cartons',
                    'freight_cost_cny', 'tax_cost_cny', 'misc_cost_cny',
                    'total_cost_cny', 'cost_per_kg', 'currency', 'status',
                    'ship_date', 'eta_date', 'arrival_date', 'delivery_date',
                    'tracking_url', 'remark'
                ]
                values = [
                    waybill_no,
                    int(provider_id),
                    shipment_id,
                    data.get('transport_type', 1),
                    data.get('route_name', '').strip() or None,
                    data.get('departure_port', '').strip() or None,
                    data.get('destination_port', '').strip() or None,
                    data.get('destination_warehouse', '').strip() or None,
                    _val_or_none(data.get('total_weight_kg'), float),
                    _val_or_none(data.get('total_volume_cbm'), float),
                    _val_or_none(data.get('total_cartons'), int),
                    _val_or_none(data.get('freight_cost_cny'), float),
                    _val_or_none(data.get('tax_cost_cny'), float),
                    _val_or_none(data.get('misc_cost_cny'), float),
                    _val_or_none(data.get('total_cost_cny'), float),
                    _val_or_none(data.get('cost_per_kg'), float),
                    data.get('currency', '').strip() or None,
                    data.get('status', 0),
                    data.get('ship_date') or None,
                    data.get('eta_date') or None,
                    data.get('arrival_date') or None,
                    data.get('delivery_date') or None,
                    data.get('tracking_url', '').strip() or None,
                    data.get('remark', '').strip() or None,
                ]

                set_clause = ', '.join([f"{f} = %s" for f in fields])
                sql = f"UPDATE logistics_waybills SET {set_clause} WHERE id = %s"
                cursor.execute(sql, tuple(values + [waybill_id]))
                conn.commit()
                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "运单不存在"}), 404

                # 状态变为已完成时，自动创建支出记录
                if data.get('status') == WAYBILL_STATUS_COMPLETED:
                    try:
                        from blueprints.expenses import create_expense_for_source
                        wb_no = waybill_no
                        if not wb_no:
                            cursor.execute("SELECT waybill_no, total_cost_cny FROM logistics_waybills WHERE id = %s", (waybill_id,))
                            row = cursor.fetchone()
                            if row:
                                wb_no = row['waybill_no']
                        if wb_no:
                            cursor.execute(
                                "SELECT id FROM expenses WHERE category = %s AND remark = %s LIMIT 1",
                                ('物流/头程', f"运单 {wb_no}")
                            )
                            if not cursor.fetchone():
                                cursor.execute("SELECT total_cost_cny FROM logistics_waybills WHERE id = %s", (waybill_id,))
                                cost_row = cursor.fetchone()
                                total_cost = float(cost_row['total_cost_cny'] or 0) if cost_row else 0
                                create_expense_for_source(
                                    conn, '物流/头程',
                                    total_cost, datetime.now().strftime('%Y-%m-%d'),
                                    f"运单 {wb_no}", 'company'
                                )
                    except Exception as e:
                        print(f"[Logistics] 自动创建支出记录异常: {e}")

                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 更新运单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills/batch-status', methods=['PUT'])
@login_required
@permission_required('logistics_waybills:edit')
def batch_update_waybill_status():
    """批量修改运单状态"""
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
        status = data.get('status')

        if not ids or not isinstance(ids, list):
            return jsonify({"status": "error", "message": "请提供要修改的ID列表"}), 400
        if status is None:
            return jsonify({"status": "error", "message": "请提供目标状态"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(ids))
                sql = f"UPDATE logistics_waybills SET status = %s WHERE id IN ({placeholders})"
                cursor.execute(sql, tuple([int(status)] + [int(i) for i in ids]))
                conn.commit()

                # 状态变为已完成时，自动创建支出记录
                if int(status) == WAYBILL_STATUS_COMPLETED:
                    try:
                        from blueprints.expenses import create_expense_for_source
                        placeholders2 = ', '.join(['%s'] * len(ids))
                        int_ids = [int(i) for i in ids]
                        cursor.execute(f"""
                            SELECT id, waybill_no, total_cost_cny FROM logistics_waybills
                            WHERE id IN ({placeholders2})
                            AND waybill_no IS NOT NULL
                            AND NOT EXISTS (
                                SELECT 1 FROM expenses
                                WHERE expenses.category = '物流/头程'
                                AND expenses.remark = CONCAT('运单 ', logistics_waybills.waybill_no)
                            )
                        """, tuple(int_ids))
                        pending = cursor.fetchall()
                        for row in pending:
                            try:
                                create_expense_for_source(
                                    conn, '物流/头程',
                                    float(row['total_cost_cny'] or 0), datetime.now().strftime('%Y-%m-%d'),
                                    f"运单 {row['waybill_no']}", 'company'
                                )
                            except Exception as e:
                                print(f"[Logistics] 为运单 {row['waybill_no']} 创建支出记录失败: {e}")
                    except Exception as e:
                        print(f"[Logistics] 批量创建支出记录异常: {e}")

                return jsonify({
                    "status": "success",
                    "message": f"成功更新 {cursor.rowcount} 条记录",
                    "data": {"affected": cursor.rowcount}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 批量更新运单状态异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills/<int:waybill_id>', methods=['DELETE'])
@login_required
@permission_required('logistics_waybills:delete')
def delete_waybill(waybill_id):
    """删除运单"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM logistics_waybills WHERE id = %s", (waybill_id,))
                conn.commit()
                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "运单不存在"}), 404
                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 删除运单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 导入货代账单 Excel ====================

# Excel 列名 → 数据库字段 映射
WAYBILL_IMPORT_COL_MAP = {
    '原单号': 'waybill_no',
    '收货日期': 'ship_date',
    '运输方式': 'route_name',
    '件数': 'total_cartons',
    '收费重': 'total_weight_kg',
    '单价': 'cost_per_kg',
    '运费': 'freight_cost_cny',
    '附加费': 'misc_cost_cny',
    '总金额': 'total_cost_cny',
    'FBA号码': 'shipment_id',
}


def _find_header_row(ws):
    """扫描工作表找到表头行（包含"序号"和"原单号"），返回 (row_index, headers)"""
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=20, values_only=True), 1):
        vals = [str(v).strip() if v is not None else '' for v in row]
        if '序号' in vals and '原单号' in vals:
            return i, vals
    return None, None


def _build_col_indices(headers):
    """根据表头建立 DB 字段 → 列索引 的映射"""
    indices = {}
    for idx, h in enumerate(headers):
        h_clean = str(h).strip() if h else ''
        for excel_key, db_field in WAYBILL_IMPORT_COL_MAP.items():
            if excel_key in h_clean:
                indices[db_field] = idx
        if '仓库编码' in h_clean or '仓库' in h_clean:
            indices['destination_warehouse'] = idx
    return indices


def _parse_date(val):
    """将 Excel 日期值转为 'YYYY-MM-DD' 字符串"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    if not s:
        return None
    return s[:10]  # "2026-05-18 00:00:00" → "2026-05-18"


def _parse_num(val, cast=float):
    """安全地将值转为数字，失败返回 None"""
    if val is None or val == '':
        return None
    try:
        return cast(val)
    except (ValueError, TypeError):
        return None


@logistics_bp.route('/logistics-waybills/import', methods=['POST'])
@login_required
@permission_required('logistics_waybills:import')
def import_waybills():
    """导入货代账单 Excel"""
    try:
        provider_id = request.form.get('provider_id', '').strip()
        if not provider_id:
            return jsonify({"status": "error", "message": "货代ID不能为空"}), 400
        provider_id = int(provider_id)

        if 'file' not in request.files:
            return jsonify({"status": "error", "message": "请上传文件"}), 400

        file = request.files['file']
        filename = (file.filename or '').lower()
        if not filename.endswith(('.xlsx', '.xls')):
            return jsonify({"status": "error", "message": "仅支持 .xlsx / .xls 文件"}), 400

        raw = file.read()
        wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=True)
        ws = wb.active

        # 1. 找表头
        header_row_idx, headers = _find_header_row(ws)
        if header_row_idx is None:
            wb.close()
            return jsonify({"status": "error", "message": "未找到数据表头，表头需包含「序号」和「原单号」"}), 400

        col_indices = _build_col_indices(headers)
        if 'waybill_no' not in col_indices or 'shipment_id' not in col_indices:
            wb.close()
            return jsonify({"status": "error", "message": "表头缺少「原单号」或「FBA号码」列"}), 400

        # 2. 解析数据行
        data_rows = []
        max_col = max(col_indices.values()) + 1
        for row in ws.iter_rows(min_row=header_row_idx + 1, max_col=max_col, values_only=True):
            first_val = str(row[0]).strip() if row and row[0] is not None else ''
            if not first_val or '合计' in first_val:
                break

            mapped = {}
            for db_field, col_idx in col_indices.items():
                val = row[col_idx] if col_idx < len(row) else None
                mapped[db_field] = val
            mapped['provider_id'] = provider_id
            data_rows.append(mapped)

        wb.close()

        if not data_rows:
            return jsonify({"status": "error", "message": "未解析到任何数据行"}), 400

        # 3. 校验货代存在，然后逐行写入
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, name FROM logistics_providers WHERE id = %s", (provider_id,))
                provider = cursor.fetchone()
                if not provider:
                    return jsonify({"status": "error", "message": "货代不存在"}), 400

                inserted = 0
                skipped = 0
                errors = []

                insert_fields = [
                    'waybill_no', 'provider_id', 'shipment_id', 'route_name',
                    'total_weight_kg', 'total_cartons', 'freight_cost_cny',
                    'misc_cost_cny', 'total_cost_cny', 'cost_per_kg',
                    'destination_warehouse', 'ship_date', 'status'
                ]

                for i, row in enumerate(data_rows):
                    waybill_no = str(row.get('waybill_no', '')).strip() if row.get('waybill_no') else None
                    shipment_id = str(row.get('shipment_id', '')).strip() if row.get('shipment_id') else None

                    if not waybill_no:
                        skipped += 1
                        errors.append(f"第{i + 1}行：原单号为空")
                        continue
                    if not shipment_id:
                        skipped += 1
                        errors.append(f"第{i + 1}行({waybill_no})：FBA号码为空")
                        continue

                    cursor.execute("SELECT id FROM logistics_waybills WHERE waybill_no = %s", (waybill_no,))
                    if cursor.fetchone():
                        skipped += 1
                        errors.append(f"第{i + 1}行({waybill_no})：运单号已存在")
                        continue

                    values = [
                        waybill_no,
                        provider_id,
                        shipment_id,
                        str(row.get('route_name', '')).strip() or None,
                        _parse_num(row.get('total_weight_kg')),
                        _parse_num(row.get('total_cartons'), int),
                        _parse_num(row.get('freight_cost_cny')),
                        _parse_num(row.get('misc_cost_cny')),
                        _parse_num(row.get('total_cost_cny')),
                        _parse_num(row.get('cost_per_kg')),
                        str(row.get('destination_warehouse', '')).strip() or None,
                        _parse_date(row.get('ship_date')),
                        0,
                    ]

                    placeholders = ', '.join(['%s'] * len(insert_fields))
                    sql = f"INSERT INTO logistics_waybills ({', '.join(insert_fields)}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(values))
                    inserted += 1

                conn.commit()

                return jsonify({
                    "status": "success",
                    "message": f"导入完成：新增 {inserted} 条，跳过 {skipped} 条",
                    "data": {
                        "inserted": inserted,
                        "skipped": skipped,
                        "provider_name": provider['name'],
                        "errors": errors[:20]
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Logistics] 导入运单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 可供选择的 FBA 货件 ====================

@logistics_bp.route('/logistics-waybills/available-shipments', methods=['GET'])
@login_required
def list_available_shipments():
    """
    查询可供绑定的 FBA 货件列表
    查询参数（可选）:
        status_list       - 逗号分隔的状态，默认 WORKING,SHIPPED
        keyword           - 搜索 shipment_id / shipment_name / destination_fulfillment_center_id
        exclude_waybill_id- 可选，当前正在编辑的运单ID
    """
    try:
        status_list = request.args.get('status_list', 'WORKING,SHIPPED').strip()
        keyword = request.args.get('keyword', '').strip() or None
        exclude_waybill_id = request.args.get('exclude_waybill_id', '').strip() or None

        statuses = [s.strip() for s in status_list.split(',') if s.strip()]
        if not statuses:
            statuses = ['WORKING', 'SHIPPED']

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                conditions = ["1=1"]
                params = []

                placeholders = ', '.join(['%s'] * len(statuses))
                conditions.append(f"status IN ({placeholders})")
                params.extend(statuses)

                if keyword:
                    conditions.append(
                        "(shipment_confirmation_id LIKE %s OR name LIKE %s OR destination_warehouse_id LIKE %s)"
                    )
                    like_val = f"%{keyword}%"
                    params.extend([like_val, like_val, like_val])

                # 排除已被其他运单绑定的货件
                exclude_sub = """
                    shipment_confirmation_id NOT IN (
                        SELECT shipment_id FROM logistics_waybills
                        WHERE shipment_id IS NOT NULL
                    )
                """
                if exclude_waybill_id:
                    exclude_sub = """
                        (
                            shipment_confirmation_id NOT IN (
                                SELECT shipment_id FROM logistics_waybills
                                WHERE shipment_id IS NOT NULL AND id != %s
                            )
                            OR shipment_confirmation_id = (SELECT shipment_id FROM logistics_waybills WHERE id = %s)
                        )
                    """
                    params.extend([exclude_waybill_id, exclude_waybill_id])

                conditions.append(exclude_sub)
                where_clause = " AND ".join(conditions)

                sql = f"""
                    SELECT shipment_confirmation_id as shipment_id, name as shipment_name, status as shipment_status, destination_warehouse_id as destination_fulfillment_center_id
                    FROM amazon_inbound_shipments_detail
                    WHERE {where_clause}
                    ORDER BY sync_time DESC
                    LIMIT 500
                """
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "list": rows
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 查询可选货件异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
