"""
货代管理与货代运单管理模块
- logistics_providers      货代管理表 CRUD
- logistics_waybills       货代运单表 CURD（一个运单对应一个 FBA 货件）
"""
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from blueprints.user_auth import login_required

logistics_bp = Blueprint('logistics', __name__, url_prefix='/api')


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


@logistics_bp.route('/logistics-providers/<int:provider_id>', methods=['DELETE'])
@login_required
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
                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Logistics] 更新运单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@logistics_bp.route('/logistics-waybills/<int:waybill_id>', methods=['DELETE'])
@login_required
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


# ==================== 可供选择的 FBA 货件 ====================

@logistics_bp.route('/logistics-waybills/available-shipments', methods=['GET'])
@login_required
def list_available_shipments():
    """
    查询可供绑定的 FBA 货件列表
    - 默认返回 shipment_status 为 WORKING / SHIPPED 的货件
    - 自动排除已被其他运单占用的货件（编辑时可传 exclude_waybill_id 保留当前运单自己的货件）
    查询参数:
        status_list       - 逗号分隔的状态，默认 WORKING,SHIPPED
        keyword           - 搜索 shipment_id / shipment_name / destination_fulfillment_center_id
        exclude_waybill_id- 可选，当前正在编辑的运单ID，其已绑定货件仍可出现
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
