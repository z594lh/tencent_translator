"""
用户认证模块 - 注册、登录、登出、用户信息管理、头像上传
"""
import os
import uuid
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime, timedelta
import hashlib
import secrets
from functools import wraps
from dotenv import load_dotenv

# 导入数据库连接
from services.mysql_service import get_db_connection
from services.permissions_service import get_user_permission_codes

# 创建 Blueprint
auth_bp = Blueprint('auth', __name__, url_prefix='/api/user')

# 加载环境变量，拼接完整 URL
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

# 头像保存目录
AVATAR_UPLOAD_DIR = os.path.join('static', 'avatars')
os.makedirs(AVATAR_UPLOAD_DIR, exist_ok=True)

# 允许的图片格式
ALLOWED_IMAGE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'webp', 'gif'}

# 允许修改的用户资料字段
ALLOWED_PROFILE_FIELDS = ['nickname', 'email', 'phone', 'bio']


def hash_password(password):
    """使用 SHA256 加密密码"""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def generate_token():
    """生成安全的登录令牌"""
    return secrets.token_urlsafe(32)


def _allowed_image(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_IMAGE_EXTENSIONS


def _save_avatar(file):
    """
    保存头像到本地，返回可访问的完整 URL
    :param file: werkzeug FileStorage
    :return: 完整 URL 或 None
    :raises ValueError: 文件格式不合法
    """
    if not file or file.filename == '':
        return None

    if not _allowed_image(file.filename):
        raise ValueError(f"不支持的图片格式，仅允许: {', '.join(ALLOWED_IMAGE_EXTENSIONS)}")

    ext = file.filename.rsplit('.', 1)[1].lower()
    unique_name = f"avatar_{uuid.uuid4().hex}_{int(datetime.now().timestamp())}.{ext}"
    save_path = os.path.join(AVATAR_UPLOAD_DIR, unique_name)
    file.save(save_path)

    relative_url = f"/static/avatars/{unique_name}"
    return f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url


def _delete_avatar_file(avatar_url):
    """删除本地旧头像文件"""
    if not avatar_url:
        return
    try:
        rel = None
        base = BASE_URL.rstrip('/') if BASE_URL else ''
        if base and avatar_url.startswith(base + '/static/avatars/'):
            rel = avatar_url[len(base):]
        elif avatar_url.startswith('/static/avatars/'):
            rel = avatar_url
        if rel:
            file_path = os.path.join('.', rel.lstrip('/'))
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[Auth] 已删除旧头像: {file_path}")
    except Exception as e:
        print(f"[Auth] 删除旧头像失败 {avatar_url}: {e}")


def get_user_by_token(token):
    """根据登录令牌获取用户信息"""
    if not token:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, uuid, username, nickname, status,
                       avatar, email, phone, bio,
                       last_login_at, created_at
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


def permission_required(code):
    """权限校验装饰器（需配合 @login_required 使用）
    用法: @permission_required('expenses:create')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = getattr(request, 'current_user', None)
            if not user:
                return jsonify({"status": "error", "message": "请先登录"}), 401
            codes = get_user_permission_codes(user['id'])
            if code not in codes:
                return jsonify({"status": "error", "message": "无权限执行此操作"}), 403
            return f(*args, **kwargs)
        return decorated_function
    return decorator


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
                    SELECT id, uuid, username, nickname, password, status,
                           avatar, email, phone, bio
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

                # 查询用户权限码列表
                user_permissions = get_user_permission_codes(user['id'])

                return jsonify({
                    "status": "success",
                    "message": "登录成功",
                    "data": {
                        "token": token,
                        "expire": expire_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "user": {
                            "uuid": user['uuid'],
                            "username": user['username'],
                            "nickname": user['nickname'],
                            "avatar": user['avatar'] or '',
                            "email": user['email'] or '',
                            "phone": user['phone'] or '',
                            "bio": user['bio'] or ''
                        },
                        "permissions": user_permissions
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
    """获取当前登录用户信息（含权限列表）"""
    user = request.current_user
    permissions = get_user_permission_codes(user['id'])
    return jsonify({
        "status": "success",
        "data": {
            "uuid": user['uuid'],
            "username": user['username'],
            "nickname": user['nickname'],
            "avatar": user.get('avatar') or '',
            "email": user.get('email') or '',
            "phone": user.get('phone') or '',
            "bio": user.get('bio') or '',
            "last_login_at": user.get('last_login_at'),
            "created_at": user.get('created_at'),
            "permissions": permissions
        }
    })


# ============== 修改用户资料接口 ==============
@auth_bp.route('/profile', methods=['PUT'])
@login_required
def update_profile():
    """
    修改当前用户资料
    支持字段: nickname(昵称), email(邮箱), phone(手机号), bio(个人简介)
    请求参数示例: {"nickname": "新昵称", "email": "test@example.com"}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        # 过滤出允许修改的字段
        updates = {}
        for field in ALLOWED_PROFILE_FIELDS:
            if field in data:
                val = data[field]
                if isinstance(val, str):
                    val = val.strip()
                updates[field] = val

        if not updates:
            return jsonify({"status": "error", "message": "没有可更新的字段"}), 400

        # 字段格式校验
        if 'nickname' in updates and not updates['nickname']:
            return jsonify({"status": "error", "message": "昵称不能为空"}), 400
        if 'email' in updates:
            email = updates['email']
            if email and len(email) > 100:
                return jsonify({"status": "error", "message": "邮箱长度不能超过100个字符"}), 400
        if 'phone' in updates:
            phone = updates['phone']
            if phone and len(phone) > 20:
                return jsonify({"status": "error", "message": "手机号长度不能超过20个字符"}), 400
        if 'bio' in updates:
            bio = updates['bio']
            if bio and len(bio) > 500:
                return jsonify({"status": "error", "message": "个人简介长度不能超过500个字符"}), 400

        user = request.current_user
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 检查邮箱是否已被其他用户使用
                if 'email' in updates and updates['email']:
                    cursor.execute(
                        "SELECT id FROM users WHERE email = %s AND id != %s",
                        (updates['email'], user['id'])
                    )
                    if cursor.fetchone():
                        return jsonify({"status": "error", "message": "该邮箱已被其他账号绑定"}), 409

                # 构建动态 UPDATE SQL
                fields = list(updates.keys())
                sql = "UPDATE users SET " + ", ".join([f"{f} = %s" for f in fields]) + " WHERE id = %s"
                params = [updates[f] for f in fields] + [user['id']]
                cursor.execute(sql, tuple(params))
                conn.commit()

                return jsonify({
                    "status": "success",
                    "message": "修改成功",
                    "data": updates
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"修改资料异常: {str(e)}")
        return jsonify({"status": "error", "message": f"修改失败: {str(e)}"}), 500


# ============== 头像上传接口 ==============
@auth_bp.route('/profile/avatar', methods=['POST'])
@login_required
def upload_avatar():
    """
    上传/修改用户头像
    请求: multipart/form-data, 字段名 avatar
    返回: {url: 头像完整URL}
    """
    try:
        file = request.files.get('avatar')
        if not file:
            return jsonify({"status": "error", "message": "未找到上传文件，字段名应为 avatar"}), 400

        # 保存新头像
        avatar_url = _save_avatar(file)
        if not avatar_url:
            return jsonify({"status": "error", "message": "头像保存失败"}), 400

        user = request.current_user
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 获取旧头像用于删除
                cursor.execute("SELECT avatar FROM users WHERE id = %s", (user['id'],))
                row = cursor.fetchone()
                old_avatar = row['avatar'] if row else None

                # 更新数据库
                cursor.execute("UPDATE users SET avatar = %s WHERE id = %s", (avatar_url, user['id']))
                conn.commit()

                # 删除旧头像文件
                if old_avatar and old_avatar != avatar_url:
                    _delete_avatar_file(old_avatar)

                return jsonify({
                    "status": "success",
                    "message": "头像上传成功",
                    "data": {"url": avatar_url}
                })
        finally:
            conn.close()
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"上传头像异常: {str(e)}")
        return jsonify({"status": "error", "message": f"上传失败: {str(e)}"}), 500


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
