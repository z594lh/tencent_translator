"""人员账号创建与 AI 执行身份：AI 是不可登录的系统人员，不自动授予任何角色。

目前统一一个 AI 运营账号；users.id 同时用于原修改日志、负责历史与人员绩效。
人工启用人/触发人与 AI 执行人分开保存，不使用 AI 账号替代 HTTP 登录鉴权。
"""
import hashlib
import re
import uuid
import pymysql
from services.mysql_service import get_db_connection
from services.listing_performance.common import DomainError, positive_id


def read_ai(cursor, user_id=None):
    """读取唯一且启用的 AI 人员，可限定已绑定编号；缺失或停用时明确停止。"""
    cursor.execute("SELECT id,username,nickname,status,account_type FROM users WHERE account_type='ai' ORDER BY id LIMIT 2")
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise DomainError('请在系统用户管理创建唯一的 AI 运营账号后再开启托管', 409)
    user = rows[0]
    if user['status'] != 1:
        raise DomainError('AI 运营账号已停用，请在用户管理启用；不会继续执行', 409)
    if user_id is not None and positive_id(user_id, 'AI执行人') != user['id']:
        raise DomainError('托管绑定的 AI 账号已变化，请重新恢复托管配置', 409)
    return user


def snapshot(user):
    """只取可留痕的人员编号、显示名和类型，绝不复制密码或登录凭证。"""
    return {'id': int(user['id']), 'name': user.get('nickname') or user['username'],
            'username': user['username'], 'account_type': user['account_type']}


def get_user(cursor, user_id):
    """读取用于日志快照的启用人员，不查询密码或登录令牌。"""
    cursor.execute('SELECT id,username,nickname,status,account_type FROM users WHERE id=%s',
                   (positive_id(user_id, '人员'),))
    user = cursor.fetchone()
    if not user or user['status'] != 1:
        raise DomainError('操作人员不存在或已停用', 403)
    return user


def create(body, created_by):
    """管理员创建普通或唯一 AI 人员；事务失败回滚，重名与重复 AI 明确返回冲突。"""
    if not isinstance(body, dict) or set(body) - {'username', 'nickname', 'account_type', 'password'}:
        raise DomainError('仅接受用户名、昵称、账号类型和普通人员密码')
    kind = body.get('account_type', 'human')
    username, nickname = body.get('username'), body.get('nickname', '')
    if kind not in ('human', 'ai'):
        raise DomainError('账号类型必须为普通人员或 AI 运营')
    if not isinstance(username, str) or not re.fullmatch(r'[A-Za-z0-9_\-.]{3,50}', username):
        raise DomainError('用户名须为 3~50 位字母、数字、下划线、点或短横线')
    if not isinstance(nickname, str) or len(nickname.strip()) > 50:
        raise DomainError('昵称不能超过 50 字')
    password = body.get('password', '')
    if kind == 'human':
        if not isinstance(password, str) or not 8 <= len(password.strip()) <= 128:
            raise DomainError('普通人员密码须为 8~128 位')
        # 保持现有登录密码算法；密码体系升级不在本次身份衔接范围。
        stored_password = hashlib.sha256(password.strip().encode('utf-8')).hexdigest()
    else:
        if password:
            raise DomainError('AI 系统账号不能设置登录密码')
        stored_password = '!AI_LOGIN_DISABLED'
    conn = get_db_connection()
    locked = False
    try:
        with conn.cursor() as cur:
            get_user(cur, created_by)
            # 多进程同时新增 AI 也只能建立一个；锁在提交/回滚后释放。
            if kind == 'ai':
                cur.execute("SELECT GET_LOCK('erp:create_ai_operator',5) acquired")
                locked = cur.fetchone()['acquired'] == 1
                if not locked:
                    raise DomainError('另一请求正在创建 AI 账号，请刷新用户列表', 409)
                cur.execute("SELECT id FROM users WHERE account_type='ai' LIMIT 1")
                if cur.fetchone():
                    raise DomainError('AI 运营账号已存在，请使用现有账号', 409)
            cur.execute('''INSERT INTO users (uuid,username,password,nickname,status,account_type)
                VALUES (%s,%s,%s,%s,1,%s)''',
                        (str(uuid.uuid4()), username, stored_password, nickname.strip() or username, kind))
            result = snapshot(get_user(cur, cur.lastrowid))
        conn.commit()
        return result
    except pymysql.IntegrityError:
        conn.rollback()
        raise DomainError('用户名已存在，请更换用户名或刷新列表', 409) from None
    except Exception:
        conn.rollback()
        raise
    finally:
        if locked:
            with conn.cursor() as cur:
                cur.execute("SELECT RELEASE_LOCK('erp:create_ai_operator')")
        conn.close()


def listing(params):
    """按用户名/昵称分页读取人员及角色，保留AI类型，不返回密码或登录令牌。"""
    page=positive_id(params.get('page',1),'页码')
    size=positive_id(params.get('page_size',20),'每页数量')
    keyword=str(params.get('keyword','')).strip()
    if size>100 or page>100000 or len(keyword)>100:
        raise DomainError('人员查询条件超过范围')
    conn=get_db_connection()
    try:
        with conn.cursor() as cur:
            where=' WHERE username LIKE %s OR nickname LIKE %s' if keyword else ''
            args=['%'+keyword+'%']*2 if keyword else []
            cur.execute('SELECT COUNT(*) total FROM users'+where,args)
            total=cur.fetchone()['total']
            cur.execute('''SELECT id,uuid,username,nickname,avatar,email,phone,status,account_type,last_login_at,created_at
                FROM users'''+where+' ORDER BY id DESC LIMIT %s OFFSET %s',(*args,size,(page-1)*size))
            rows=cur.fetchall()
            for user in rows:
                cur.execute('''SELECT r.id,r.name,r.code FROM roles r JOIN user_roles ur ON ur.role_id=r.id
                    WHERE ur.user_id=%s AND r.status=1''',(user['id'],))
                user['roles']=cur.fetchall()
            return {'list':rows,'total':total,'page':page,'page_size':size}
    finally:
        conn.close()
