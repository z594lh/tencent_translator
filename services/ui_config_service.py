"""公开界面文本白名单；不读取密钥，不修改业务配置或环境文件。"""
import json
from datetime import datetime, timezone

from services.mysql_service import get_db_connection


FIELDS = (
    {'key': 'brand_name', 'label': '系统名称', 'default_value': '勤发跨境ERP', 'max_length': 64,
     'multiline': False, 'description': '导航栏和登录页共同显示的名称；留空使用默认名称。'},
    {'key': 'login_welcome', 'label': '登录欢迎语', 'default_value': '欢迎回来', 'max_length': 120,
     'multiline': False, 'description': '登录页名称下方的欢迎语；留空使用默认文案。'},
    {'key': 'footer_text', 'label': '页脚文字', 'default_value': '', 'max_length': 300,
     'multiline': True, 'description': '全站页面底部的公开文字，可换行；留空不显示。'},
)
KEYS = {field['key'] for field in FIELDS}


class UiConfigError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def normalize_value(value, field):
    if not isinstance(value, str):
        raise UiConfigError(f"{field['label']}必须是文本")
    value = value.replace('\r\n', '\n').strip()
    if len(value) > field['max_length']:
        raise UiConfigError(f"{field['label']}不能超过{field['max_length']}个字符")
    if '<' in value or '>' in value or any(
            (ord(char) < 32 or ord(char) == 127) and not (char == '\n' and field['multiline']) for char in value):
        raise UiConfigError(f"{field['label']}不能包含HTML标签或控制字符")
    return value or field['default_value']


def values_from_json(raw):
    try:
        data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        if not isinstance(data, dict):
            raise ValueError('配置结构异常')
        # 查询任何时候都仅投影白名单，未知键不能进入公开响应或审计详情。
        return {field['key']: normalize_value(data.get(field['key'], field['default_value']), field) for field in FIELDS}
    except (ValueError, TypeError, UiConfigError):
        raise UiConfigError('界面配置记录异常，请由管理员核对', 503) from None


def snapshot(cursor, lock=False):
    cursor.execute('SELECT config_values,version,updated_by,updated_by_name,updated_at '
                   'FROM system_ui_config WHERE id=1' + (' FOR UPDATE' if lock else ''))
    row = cursor.fetchone()
    if row is None:
        raise UiConfigError('界面配置尚未初始化，请执行上线SQL', 503)
    row['values'] = values_from_json(row.pop('config_values'))
    return row


def utc_text(value):
    return value.strftime('%Y-%m-%dT%H:%M:%SZ') if value else None


def management_view(row):
    return {'version': row['version'], 'values': row['values'],
            'fields': [{**field, 'value': row['values'][field['key']]} for field in FIELDS],
            'updated_by_name': row['updated_by_name'], 'updated_at': utc_text(row['updated_at'])}


def read_config(public=False):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            row = snapshot(cursor)
        return row['values'] if public else management_view(row)
    finally:
        conn.close()


def write_config(payload, user, reset=False):
    expected_keys = {'version'} if reset else {'version', 'values'}
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise UiConfigError('配置请求字段不完整或包含不支持的字段')
    version = payload['version']
    if type(version) is not int or not 0 <= version < 2 ** 63:
        raise UiConfigError('配置版本无效，请重新加载')
    if not reset and (not isinstance(payload['values'], dict) or set(payload['values']) != KEYS):
        raise UiConfigError('仅支持完整提交系统名称、登录欢迎语和页脚文字')
    values = {field['key']: field['default_value'] if reset else normalize_value(payload['values'][field['key']], field)
              for field in FIELDS}
    conn = get_db_connection()
    try:
        conn.begin()
        with conn.cursor() as cursor:
            row = snapshot(cursor, lock=True)
            if row['version'] != version:
                raise UiConfigError('配置已被其他人修改，请重新加载后再保存；不会自动覆盖', 409)
            changed = [field['key'] for field in FIELDS if values[field['key']] != row['values'][field['key']]]
            if changed:
                now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
                operator = str(user['username'])[:100]
                cursor.execute('UPDATE system_ui_config SET config_values=%s,version=%s,updated_by=%s,'
                               'updated_by_name=%s,updated_at=%s WHERE id=1 AND version=%s',
                               (json.dumps(values, ensure_ascii=False), version + 1, user['id'], operator, now, version))
                if cursor.rowcount != 1:
                    raise UiConfigError('配置版本冲突，请重新加载', 409)
                cursor.execute('INSERT INTO system_ui_config_history '
                               '(config_version,action,before_values,after_values,changed_keys,operator_id,operator_name,created_at) '
                               'VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                               (version + 1, 'reset' if reset else 'update', json.dumps(row['values'], ensure_ascii=False),
                                json.dumps(values, ensure_ascii=False), json.dumps(changed), user['id'], operator, now))
                row.update(values=values, version=version + 1, updated_by=user['id'], updated_by_name=operator, updated_at=now)
            result = management_view(row)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def history(page, page_size):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) AS total FROM system_ui_config_history')
            total = cursor.fetchone()['total']
            cursor.execute('SELECT id,config_version,action,before_values,after_values,changed_keys,operator_name,created_at '
                           'FROM system_ui_config_history ORDER BY config_version DESC LIMIT %s OFFSET %s',
                           (page_size, (page - 1) * page_size))
            rows = cursor.fetchall()
        for row in rows:
            row['before_values'] = values_from_json(row['before_values'])
            row['after_values'] = values_from_json(row['after_values'])
            row['changed_keys'] = [key for key in json.loads(row['changed_keys']) if key in KEYS]
            row['created_at'] = utc_text(row['created_at'])
        return {'list': rows, 'total': total, 'page': page, 'page_size': page_size}
    finally:
        conn.close()
