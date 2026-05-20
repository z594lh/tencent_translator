"""
RBAC 权限管理模块 - 角色、权限、用户角色分配
仅管理员可访问
"""
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection
from services.permissions_service import (
    get_all_permissions,
    get_role_permissions,
    set_role_permissions,
    set_user_roles,
    get_user_permissions,
    get_user_permission_codes,
    get_user_direct_permissions,
    set_user_permissions,
    create_permission,
    update_permission,
    delete_permission,
    get_all_menus,
    create_menu,
    update_menu,
    delete_menu,
)
from blueprints.user_auth import login_required

permissions_bp = Blueprint('permissions', __name__, url_prefix='/api')

# ========== 权限校验装饰器 ==========

def _check_admin_role(user_id):
    """兜底：检查用户是否绑定了 admin 角色"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 1 FROM user_roles ur
                INNER JOIN roles r ON ur.role_id = r.id
                WHERE ur.user_id = %s AND r.code = 'admin' AND r.status = 1
                LIMIT 1
            """, (user_id,))
            return cursor.fetchone() is not None
    finally:
        conn.close()


def require_permission(code):
    """权限校验装饰器工厂：检查指定权限码，admin 角色兜底
    用法: @require_permission('system:user_manage')
    """
    from functools import wraps
    from services.permissions_service import has_permission

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = request.current_user
            if has_permission(user['id'], code):
                return f(*args, **kwargs)
            if _check_admin_role(user['id']):
                return f(*args, **kwargs)
            return jsonify({"status": "error", "message": "无权限访问"}), 403
        return decorated_function
    return decorator


# ========== 权限定义管理 ==========

@permissions_bp.route('/permissions', methods=['GET'])
@login_required
@require_permission('system:permission_manage')
def list_permissions():
    """获取全部权限列表，支持按模块分组"""
    try:
        module = request.args.get('module', '').strip() or None
        perms = get_all_permissions(module)

        # 按模块分组
        grouped = {}
        for p in perms:
            mod = p['module']
            if mod not in grouped:
                grouped[mod] = []
            grouped[mod].append({
                "id": p['id'],
                "code": p['code'],
                "name": p['name'],
                "type": p['type'],
                "description": p['description']
            })

        return jsonify({"status": "success", "data": grouped})
    except Exception as e:
        print(f"[Permissions] 获取权限列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/permissions', methods=['POST'])
@login_required
@require_permission('system:permission_manage')
def create_permission_endpoint():
    """创建权限定义"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        code = data.get('code', '').strip()
        name = data.get('name', '').strip()
        perm_type = data.get('type', 'action').strip()
        module = data.get('module', '').strip()
        description = data.get('description', '').strip()

        if not code or not name:
            return jsonify({"status": "error", "message": "权限码和名称不能为空"}), 400
        if perm_type not in ('page', 'action'):
            return jsonify({"status": "error", "message": "type 只能是 page 或 action"}), 400

        perm_id = create_permission(code, name, perm_type, module, description)
        return jsonify({
            "status": "success",
            "message": "创建成功",
            "data": {"id": perm_id}
        }), 201
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 409
    except Exception as e:
        print(f"[Permissions] 创建权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/permissions/<int:perm_id>', methods=['PUT'])
@login_required
@require_permission('system:permission_manage')
def update_permission_endpoint(perm_id):
    """修改权限定义"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        update_permission(perm_id, **data)
        return jsonify({"status": "success", "message": "修改成功"})
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 409
    except Exception as e:
        print(f"[Permissions] 修改权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/permissions/<int:perm_id>', methods=['DELETE'])
@login_required
@require_permission('system:permission_manage')
def delete_permission_endpoint(perm_id):
    """删除权限定义（同时清理角色关联和菜单关联）"""
    try:
        delete_permission(perm_id)
        return jsonify({"status": "success", "message": "删除成功"})
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        print(f"[Permissions] 删除权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========== 角色管理 ==========

@permissions_bp.route('/roles', methods=['GET'])
@login_required
@require_permission('system:role_manage')
def list_roles():
    """获取角色列表"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, code, description, status, created_at
                    FROM roles ORDER BY id
                """)
                roles = cursor.fetchall()

                # 获取每个角色的权限数量
                for r in roles:
                    cursor.execute(
                        "SELECT COUNT(*) as cnt FROM role_permissions WHERE role_id = %s",
                        (r['id'],)
                    )
                    r['permission_count'] = cursor.fetchone()['cnt']

                return jsonify({"status": "success", "data": roles})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 获取角色列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/roles', methods=['POST'])
@login_required
@require_permission('system:role_manage')
def create_role():
    """创建角色"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        name = data.get('name', '').strip()
        code = data.get('code', '').strip()
        description = data.get('description', '').strip()

        if not name or not code:
            return jsonify({"status": "error", "message": "角色名称和标识不能为空"}), 400
        if len(code) > 50:
            return jsonify({"status": "error", "message": "角色标识不能超过50个字符"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM roles WHERE code = %s", (code,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "角色标识已存在"}), 409

                cursor.execute("""
                    INSERT INTO roles (name, code, description, status)
                    VALUES (%s, %s, %s, 1)
                """, (name, code, description))
                conn.commit()
                role_id = cursor.lastrowid

                return jsonify({
                    "status": "success",
                    "message": "创建成功",
                    "data": {"id": role_id, "name": name, "code": code}
                }), 201
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 创建角色异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/roles/<int:role_id>', methods=['PUT'])
@login_required
@require_permission('system:role_manage')
def update_role(role_id):
    """修改角色信息"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        updates = {}
        for field in ['name', 'code', 'description', 'status']:
            if field in data:
                val = data[field]
                if isinstance(val, str):
                    val = val.strip()
                updates[field] = val

        if not updates:
            return jsonify({"status": "error", "message": "没有可更新的字段"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                if 'code' in updates:
                    cursor.execute(
                        "SELECT id FROM roles WHERE code = %s AND id != %s",
                        (updates['code'], role_id)
                    )
                    if cursor.fetchone():
                        return jsonify({"status": "error", "message": "角色标识已存在"}), 409

                fields = list(updates.keys())
                sql = "UPDATE roles SET " + ", ".join([f"{f} = %s" for f in fields]) + " WHERE id = %s"
                params = [updates[f] for f in fields] + [role_id]
                cursor.execute(sql, tuple(params))
                conn.commit()

                return jsonify({"status": "success", "message": "修改成功", "data": updates})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 修改角色异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/roles/<int:role_id>', methods=['DELETE'])
@login_required
@require_permission('system:role_manage')
def delete_role(role_id):
    """删除角色（同时清理关联数据）"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM roles WHERE id = %s", (role_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "角色不存在"}), 404

                cursor.execute("DELETE FROM role_permissions WHERE role_id = %s", (role_id,))
                cursor.execute("DELETE FROM user_roles WHERE role_id = %s", (role_id,))
                cursor.execute("DELETE FROM roles WHERE id = %s", (role_id,))
                conn.commit()

                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 删除角色异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/roles/<int:role_id>/permissions', methods=['GET'])
@login_required
@require_permission('system:role_manage')
def get_role_perm_list(role_id):
    """获取角色的权限ID列表"""
    try:
        ids = get_role_permissions(role_id)
        return jsonify({"status": "success", "data": ids})
    except Exception as e:
        print(f"[Permissions] 获取角色权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/roles/<int:role_id>/permissions', methods=['PUT'])
@login_required
@require_permission('system:role_manage')
def update_role_perm_list(role_id):
    """批量设置角色权限"""
    try:
        data = request.get_json()
        permission_ids = data.get('permission_ids', []) if data else []
        if not isinstance(permission_ids, list):
            return jsonify({"status": "error", "message": "permission_ids 必须是数组"}), 400
        if permission_ids and not all(isinstance(i, int) for i in permission_ids):
            return jsonify({"status": "error", "message": "permission_ids 必须是整数数组"}), 400

        set_role_permissions(role_id, permission_ids)
        return jsonify({"status": "success", "message": "权限设置成功"})
    except Exception as e:
        print(f"[Permissions] 设置角色权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========== 用户角色/权限管理 ==========

@permissions_bp.route('/users', methods=['GET'])
@login_required
@require_permission('system:user_manage')
def list_users():
    """获取用户列表（含角色信息）"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, uuid, username, nickname, avatar, email, phone,
                           status, last_login_at, created_at
                    FROM users ORDER BY id
                """)
                users = cursor.fetchall()

                for u in users:
                    cursor.execute("""
                        SELECT r.id, r.name, r.code
                        FROM roles r
                        INNER JOIN user_roles ur ON r.id = ur.role_id
                        WHERE ur.user_id = %s AND r.status = 1
                    """, (u['id'],))
                    u['roles'] = cursor.fetchall()

                return jsonify({"status": "success", "data": users})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 获取用户列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/users/<int:user_id>/roles', methods=['PUT'])
@login_required
@require_permission('system:user_manage')
def update_user_roles(user_id):
    """设置用户角色（全量覆盖）"""
    try:
        data = request.get_json()
        role_ids = data.get('role_ids', []) if data else []
        if not isinstance(role_ids, list):
            return jsonify({"status": "error", "message": "role_ids 必须是数组"}), 400

        set_user_roles(user_id, role_ids)
        return jsonify({"status": "success", "message": "用户角色设置成功"})
    except Exception as e:
        print(f"[Permissions] 设置用户角色异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/users/<int:user_id>/permissions', methods=['GET'])
@login_required
@require_permission('system:user_manage')
def get_user_perm_detail(user_id):
    """获取用户最终权限详情"""
    try:
        perms = get_user_permissions(user_id)
        return jsonify({
            "status": "success",
            "data": {
                "permissions": [p['code'] for p in perms],
                "detail": perms
            }
        })
    except Exception as e:
        print(f"[Permissions] 获取用户权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/users/<int:user_id>/permissions/direct', methods=['GET'])
@login_required
@require_permission('system:user_manage')
def get_user_direct_perm_list(user_id):
    """获取用户直接授予的权限ID列表（不含角色继承）"""
    try:
        ids = get_user_direct_permissions(user_id)
        return jsonify({"status": "success", "data": ids})
    except Exception as e:
        print(f"[Permissions] 获取用户直接权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/users/<int:user_id>/permissions', methods=['PUT'])
@login_required
@require_permission('system:user_manage')
def update_user_perm_list(user_id):
    """设置用户直接权限（全量覆盖）"""
    try:
        data = request.get_json()
        permission_ids = data.get('permission_ids', []) if data else []
        if not isinstance(permission_ids, list):
            return jsonify({"status": "error", "message": "permission_ids 必须是数组"}), 400
        if permission_ids and not all(isinstance(i, int) for i in permission_ids):
            return jsonify({"status": "error", "message": "permission_ids 必须是整数数组"}), 400

        set_user_permissions(user_id, permission_ids)
        return jsonify({"status": "success", "message": "用户权限设置成功"})
    except Exception as e:
        print(f"[Permissions] 设置用户权限异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============== 前端菜单接口（用户端） ==============

@permissions_bp.route('/menus', methods=['GET'])
@login_required
def get_menus():
    """获取当前用户的前端导航菜单（根据权限过滤）"""
    try:
        user = request.current_user
        user_codes = get_user_permission_codes(user['id'])

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT m.id, m.parent_id, m.label, m.path, m.icon,
                           m.permission_id, p.code AS permission_code, m.sort_order
                    FROM menus m
                    LEFT JOIN permissions p ON m.permission_id = p.id
                    WHERE m.status = 1
                    ORDER BY m.sort_order, m.id
                """)
                all_menus = cursor.fetchall()

                # 过滤有权限的菜单项
                filtered = []
                for m in all_menus:
                    if m['parent_id'] == 0:
                        filtered.append(m)
                    elif not m['permission_code']:
                        filtered.append(m)
                    elif m['permission_code'] in user_codes:
                        filtered.append(m)

                # 清理空分组
                parent_ids = {m['parent_id'] for m in filtered if m['parent_id'] > 0}
                result = [m for m in filtered if m['parent_id'] == 0 or m['parent_id'] in parent_ids]
                result = [m for m in result if m['parent_id'] > 0 or m['id'] in parent_ids or m['permission_code'] in user_codes]

                return jsonify({"status": "success", "data": result})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 获取菜单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ========== 菜单管理（管理员端） ==========

@permissions_bp.route('/menus/admin', methods=['GET'])
@login_required
@require_permission('system:menu_manage')
def list_menus_admin():
    """获取全部菜单列表（含权限信息，供管理端使用）"""
    try:
        menus = get_all_menus()
        return jsonify({"status": "success", "data": menus})
    except Exception as e:
        print(f"[Permissions] 获取菜单列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/menus', methods=['POST'])
@login_required
@require_permission('system:menu_manage')
def create_menu_endpoint():
    """创建菜单"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        label = data.get('label', '').strip()
        if not label:
            return jsonify({"status": "error", "message": "菜单名称不能为空"}), 400

        menu_id = create_menu(
            parent_id=data.get('parent_id', 0),
            label=label,
            path=data.get('path', ''),
            icon=data.get('icon', ''),
            permission_id=data.get('permission_id') or None,
            sort_order=data.get('sort_order', 0),
            status=data.get('status', 1)
        )
        return jsonify({
            "status": "success",
            "message": "创建成功",
            "data": {"id": menu_id}
        }), 201
    except Exception as e:
        print(f"[Permissions] 创建菜单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/menus/<int:menu_id>', methods=['PUT'])
@login_required
@require_permission('system:menu_manage')
def update_menu_endpoint(menu_id):
    """修改菜单"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        update_menu(menu_id, **data)
        return jsonify({"status": "success", "message": "修改成功"})
    except Exception as e:
        print(f"[Permissions] 修改菜单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/menus/<int:menu_id>', methods=['DELETE'])
@login_required
@require_permission('system:menu_manage')
def delete_menu_endpoint(menu_id):
    """删除菜单（级联删除所有子菜单）"""
    try:
        delete_menu(menu_id)
        return jsonify({"status": "success", "message": "删除成功"})
    except Exception as e:
        print(f"[Permissions] 删除菜单异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@permissions_bp.route('/users/<int:user_id>/status', methods=['PUT'])
@login_required
@require_permission('system:user_manage')
def update_user_status(user_id):
    """启用/禁用用户账号"""
    try:
        data = request.get_json()
        status = data.get('status')
        if status not in [0, 1]:
            return jsonify({"status": "error", "message": "status 只能是 0 或 1"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE users SET status = %s WHERE id = %s", (status, user_id))
                conn.commit()
                return jsonify({"status": "success", "message": "状态修改成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Permissions] 修改用户状态异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
