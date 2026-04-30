"""
用户认证模块 - 注册、登录、登出、用户信息管理
"""
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime, timedelta
import hashlib
import uuid
import secrets
from functools import wraps

# 导入数据库连接
from services.mysql_service import get_db_connection

# 创建 Blueprint
auth_bp = Blueprint('auth', __name__, url_prefix='/api/user')


def hash_password(password):
    """使用 SHA256 加密密码"""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def generate_token():
    """生成安全的登录令牌"""
    return secrets.token_urlsafe(32)


def get_user_by_token(token):
    """根据登录令牌获取用户信息"""
    if not token:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, uuid, username, nickname, status
                FROM users
                WHERE login_token = %s AND token_expire > NOW() AND status = 1
            """, (token,))
            return cursor.fetchone()
    finally:
        conn.close()


def login_required(f):
    """登录验证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token and token.startswith('Bearer '):
            token = token[7:]
        if not token:
            return jsonify({"status": "error", "message": "请先登录"}), 401
        user = get_user_by_token(token)
        if not user:
            return jsonify({"status": "error", "message": "登录已过期，请重新登录"}), 401
        request.current_user = user
        return f(*args, **kwargs)
    return decorated_function


# ============== 用户注册接口 ==============
@auth_bp.route('/register', methods=['POST'])
def user_register():
    """
    用户注册
    请求参数: {username: 用户名, password: 密码, nickname: 昵称}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        nickname = data.get('nickname', '').strip()

        # 参数验证
        if not username:
            return jsonify({"status": "error", "message": "用户名不能为空"}), 400
        if not password:
            return jsonify({"status": "error", "message": "密码不能为空"}), 400
        if len(username) < 3 or len(username) > 50:
            return jsonify({"status": "error", "message": "用户名长度需在3-50个字符之间"}), 400
        if len(password) < 6:
            return jsonify({"status": "error", "message": "密码长度至少6个字符"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 检查用户名是否已存在
                cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
                if cursor.fetchone():
                    return jsonify({"status": "error", "message": "用户名已被注册"}), 409

                # 生成用户UUID和加密密码
                user_uuid = str(uuid.uuid4())
                hashed_pwd = hash_password(password)

                # 插入新用户
                cursor.execute("""
                    INSERT INTO users (uuid, username, password, nickname, status)
                    VALUES (%s, %s, %s, %s, 1)
                """, (user_uuid, username, hashed_pwd, nickname or username))
                conn.commit()

                return jsonify({
                    "status": "success",
                    "message": "注册成功",
                    "data": {
                        "uuid": user_uuid,
                        "username": username,
                        "nickname": nickname or username
                    }
                }), 201
        finally:
            conn.close()

    except Exception as e:
        print(f"注册异常: {str(e)}")
        return jsonify({"status": "error", "message": f"注册失败: {str(e)}"}), 500


# ============== 用户登录接口 ==============
@auth_bp.route('/login', methods=['POST'])
def user_login():
    """
    用户登录
    请求参数: {username: 用户名, password: 密码}
    返回: {token: 登录令牌, user: 用户信息}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        username = data.get('username', '').strip()
        password = data.get('password', '').strip()

        if not username or not password:
            return jsonify({"status": "error", "message": "用户名和密码不能为空"}), 400

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 查询用户
                cursor.execute("""
                    SELECT id, uuid, username, nickname, password, status
                    FROM users WHERE username = %s
                """, (username,))
                user = cursor.fetchone()

                if not user:
                    return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

                if user['status'] != 1:
                    return jsonify({"status": "error", "message": "账号已被禁用"}), 403

                # 验证密码
                if hash_password(password) != user['password']:
                    return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

                # 生成登录令牌 (30天有效期)
                token = generate_token()
                expire_time = datetime.now() + timedelta(days=30)

                # 更新登录信息
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                cursor.execute("""
                    UPDATE users
                    SET login_token = %s, token_expire = %s, last_login_at = NOW(), last_login_ip = %s
                    WHERE id = %s
                """, (token, expire_time, ip, user['id']))
                conn.commit()

                return jsonify({
                    "status": "success",
                    "message": "登录成功",
                    "data": {
                        "token": token,
                        "expire": expire_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "user": {
                            "uuid": user['uuid'],
                            "username": user['username'],
                            "nickname": user['nickname']
                        }
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"登录异常: {str(e)}")
        return jsonify({"status": "error", "message": f"登录失败: {str(e)}"}), 500


# ============== 用户登出接口 ==============
@auth_bp.route('/logout', methods=['POST'])
@login_required
def user_logout():
    """用户登出，清除登录令牌"""
    try:
        user = request.current_user
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE users SET login_token = NULL, token_expire = NULL WHERE id = %s
                """, (user['id'],))
                conn.commit()
                return jsonify({"status": "success", "message": "登出成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"登出异常: {str(e)}")
        return jsonify({"status": "error", "message": f"登出失败: {str(e)}"}), 500


# ============== 获取当前用户信息接口 ==============
@auth_bp.route('/profile', methods=['GET'])
@login_required
def user_profile():
    """获取当前登录用户信息"""
    user = request.current_user
    return jsonify({
        "status": "success",
        "data": {
            "uuid": user['uuid'],
            "username": user['username'],
            "nickname": user['nickname']
        }
    })


# ============== 修改用户信息接口 ==============
@auth_bp.route('/profile', methods=['PUT'])
@login_required
def update_profile():
    """修改当前用户信息（目前只支持修改昵称）"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        nickname = data.get('nickname', '').strip()
        if not nickname:
            return jsonify({"status": "error", "message": "昵称不能为空"}), 400

        user = request.current_user
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE users SET nickname = %s WHERE id = %s", (nickname, user['id']))
                conn.commit()
                return jsonify({
                    "status": "success",
                    "message": "修改成功",
                    "data": {"nickname": nickname}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"修改资料异常: {str(e)}")
        return jsonify({"status": "error", "message": f"修改失败: {str(e)}"}), 500


# ============== 修改密码接口 ==============
@auth_bp.route('/password', methods=['PUT'])
@login_required
def change_password():
    """修改密码"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        old_password = data.get('old_password', '').strip()
        new_password = data.get('new_password', '').strip()

        if not old_password or not new_password:
            return jsonify({"status": "error", "message": "原密码和新密码不能为空"}), 400
        if len(new_password) < 6:
            return jsonify({"status": "error", "message": "新密码长度至少6个字符"}), 400

        user = request.current_user
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 验证原密码
                cursor.execute("SELECT password FROM users WHERE id = %s", (user['id'],))
                result = cursor.fetchone()
                if hash_password(old_password) != result['password']:
                    return jsonify({"status": "error", "message": "原密码错误"}), 401

                # 更新密码
                cursor.execute("UPDATE users SET password = %s WHERE id = %s",
                               (hash_password(new_password), user['id']))
                conn.commit()
                return jsonify({"status": "success", "message": "密码修改成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"修改密码异常: {str(e)}")
        return jsonify({"status": "error", "message": f"修改失败: {str(e)}"}), 500
