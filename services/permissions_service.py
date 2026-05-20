"""
RBAC 权限服务模块
提供用户权限查询、权限校验等核心功能
"""
from services.mysql_service import get_db_connection


def get_user_permissions(user_id):
    """
    获取用户最终权限列表（角色权限并集 + 用户直接权限）
    返回: [{id, code, name, type, module}, ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 获取角色授予的权限
            cursor.execute("""
                SELECT DISTINCT p.id, p.code, p.name, p.type, p.module
                FROM permissions p
                INNER JOIN role_permissions rp ON p.id = rp.permission_id
                INNER JOIN user_roles ur ON rp.role_id = ur.role_id
                INNER JOIN roles r ON ur.role_id = r.id
                WHERE ur.user_id = %s AND r.status = 1
            """, (user_id,))
            role_perms = {row['id']: row for row in cursor.fetchall()}

            # 2. 获取用户直接权限（覆盖角色权限）
            cursor.execute("""
                SELECT p.id, p.code, p.name, p.type, p.module, up.is_grant
                FROM permissions p
                INNER JOIN user_permissions up ON p.id = up.permission_id
                WHERE up.user_id = %s
            """, (user_id,))
            user_perms = cursor.fetchall()

            # 3. 合并：用户直接拒绝的权限去掉，直接授予的加上
            for row in user_perms:
                if row['is_grant'] == 0:
                    role_perms.pop(row['id'], None)
                else:
                    role_perms[row['id']] = row

            return list(role_perms.values())
    finally:
        conn.close()


def get_user_permission_codes(user_id):
    """
    获取用户权限码列表（仅返回 code 字符串列表）
    返回: ['expenses:view', 'expenses:create', ...]
    """
    perms = get_user_permissions(user_id)
    return [p['code'] for p in perms]


def has_permission(user_id, code):
    """判断用户是否有指定权限码"""
    codes = get_user_permission_codes(user_id)
    return code in codes


def get_all_permissions(module=None):
    """获取全部权限定义，可按模块筛选"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if module:
                cursor.execute(
                    "SELECT * FROM permissions WHERE module = %s ORDER BY module, type, id",
                    (module,)
                )
            else:
                cursor.execute("SELECT * FROM permissions ORDER BY module, type, id")
            return cursor.fetchall()
    finally:
        conn.close()


def get_role_permissions(role_id):
    """获取角色拥有的权限ID列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT permission_id FROM role_permissions WHERE role_id = %s
            """, (role_id,))
            return [row['permission_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def set_role_permissions(role_id, permission_ids):
    """
    批量设置角色权限（全量覆盖）
    permission_ids: [1, 2, 3, ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM role_permissions WHERE role_id = %s", (role_id,))
            for pid in permission_ids:
                cursor.execute(
                    "INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s)",
                    (role_id, pid)
                )
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def set_user_roles(user_id, role_ids):
    """批量设置用户角色（全量覆盖）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user_roles WHERE user_id = %s", (user_id,))
            for rid in role_ids:
                cursor.execute(
                    "INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s)",
                    (user_id, rid)
                )
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ========== 用户直接权限 ==========

def get_user_direct_permissions(user_id):
    """获取用户直接授予的权限ID列表（不含角色继承）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT permission_id FROM user_permissions
                WHERE user_id = %s AND is_grant = 1
            """, (user_id,))
            return [row['permission_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def set_user_permissions(user_id, permission_ids):
    """
    设置用户直接权限（全量覆盖，仅授予）
    permission_ids: [1, 2, 3, ...]
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user_permissions WHERE user_id = %s", (user_id,))
            for pid in permission_ids:
                cursor.execute(
                    "INSERT INTO user_permissions (user_id, permission_id, is_grant) VALUES (%s, %s, 1)",
                    (user_id, pid)
                )
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ========== 权限 CRUD ==========

def create_permission(code, name, perm_type='action', module='', description=''):
    """创建权限定义"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM permissions WHERE code = %s", (code,))
            if cursor.fetchone():
                raise ValueError(f"权限码 {code} 已存在")
            cursor.execute("""
                INSERT INTO permissions (code, name, type, module, description)
                VALUES (%s, %s, %s, %s, %s)
            """, (code, name, perm_type, module, description))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def update_permission(perm_id, **kwargs):
    """更新权限定义"""
    allowed = ['code', 'name', 'type', 'module', 'description']
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            if 'code' in updates:
                cursor.execute(
                    "SELECT id FROM permissions WHERE code = %s AND id != %s",
                    (updates['code'], perm_id)
                )
                if cursor.fetchone():
                    raise ValueError(f"权限码 {updates['code']} 已存在")
            fields = list(updates.keys())
            sql = "UPDATE permissions SET " + ", ".join([f"{f} = %s" for f in fields]) + " WHERE id = %s"
            params = [updates[f] for f in fields] + [perm_id]
            cursor.execute(sql, tuple(params))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def delete_permission(perm_id):
    """删除权限定义，同时清理关联数据"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM permissions WHERE id = %s", (perm_id,))
            if not cursor.fetchone():
                raise ValueError("权限不存在")
            cursor.execute("DELETE FROM role_permissions WHERE permission_id = %s", (perm_id,))
            cursor.execute("DELETE FROM user_permissions WHERE permission_id = %s", (perm_id,))
            cursor.execute("UPDATE menus SET permission_id = NULL WHERE permission_id = %s", (perm_id,))
            cursor.execute("DELETE FROM permissions WHERE id = %s", (perm_id,))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# ========== 菜单 CRUD ==========

def get_all_menus():
    """获取全部菜单列表（管理员用，含权限信息）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT m.id, m.parent_id, m.label, m.path, m.icon,
                       m.permission_id, p.code AS permission_code, p.name AS permission_name,
                       m.sort_order, m.status, m.created_at
                FROM menus m
                LEFT JOIN permissions p ON m.permission_id = p.id
                ORDER BY m.sort_order, m.id
            """)
            return cursor.fetchall()
    finally:
        conn.close()


def create_menu(parent_id, label, path='', icon='', permission_id=None, sort_order=0, status=1):
    """创建菜单"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO menus (parent_id, label, path, icon, permission_id, sort_order, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (parent_id, label, path, icon, permission_id, sort_order, status))
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def update_menu(menu_id, **kwargs):
    """更新菜单"""
    allowed = ['parent_id', 'label', 'path', 'icon', 'permission_id', 'sort_order', 'status']
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            fields = list(updates.keys())
            sql = "UPDATE menus SET " + ", ".join([f"{f} = %s" for f in fields]) + " WHERE id = %s"
            params = [updates[f] for f in fields] + [menu_id]
            cursor.execute(sql, tuple(params))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def delete_menu(menu_id):
    """删除菜单及其所有子菜单"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 递归查找所有子孙节点
            ids_to_delete = [menu_id]
            to_check = [menu_id]
            while to_check:
                placeholders = ','.join(['%s'] * len(to_check))
                cursor.execute(f"""
                    SELECT id FROM menus WHERE parent_id IN ({placeholders})
                """, tuple(to_check))
                children = [row['id'] for row in cursor.fetchall()]
                ids_to_delete.extend(children)
                to_check = children
            placeholders = ','.join(['%s'] * len(ids_to_delete))
            cursor.execute(f"DELETE FROM menus WHERE id IN ({placeholders})", tuple(ids_to_delete))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
