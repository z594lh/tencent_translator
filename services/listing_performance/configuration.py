"""本版本SQL初始化配置的只读入口。

读取listing_performance_settings中的固定白名单，支持功能开关、站点映射和预览签名密钥。
显式环境变量（含0/空值）优先于数据库；数据库配置按进程缓存，修改后需重启。
不自动建表、写配置或生成密钥；不输出配置中的签名密钥。
"""
import os
from functools import lru_cache
import pymysql
from services.mysql_service import get_db_connection

KEYS = frozenset(('LISTING_PERFORMANCE_ENABLED',
                 'PERFORMANCE_MARKETPLACES','OWNERSHIP_PREVIEW_SECRET'))


@lru_cache(maxsize=1)
def settings():
    """读取并缓存数据库配置，返回{setting_key:setting_value}。

    只接受KEYS白名单，独立连接读取后关闭；结果（含缺表空配置）缓存至进程重启。
    仅缺表1146视为尚未部署并返回空字典，其他DB故障抛出，避免误判功能关闭。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT setting_key,setting_value FROM listing_performance_settings')
            return {row['setting_key']:row['setting_value'] for row in cursor.fetchall() if row['setting_key'] in KEYS}
    except pymysql.err.ProgrammingError as error:
        if error.args and error.args[0] == 1146:
            # 尚未部署新表时保持原禁用行为，其他DB故障不能静默吞掉。
            return {}
        raise
    finally:
        conn.close()


def value(key, default=''):
    """返回白名单key的配置值，缺项返回default。

    先看显式环境变量，再读缓存数据库；连接期间可能加载.env，因此返回前再次核对环境。
    未知key报ValueError；此方法不把数据库密码等其他配置开放给调用方。"""
    if key not in KEYS:
        raise ValueError('不支持的绩效配置项')
    if key in os.environ:
        return os.environ[key]
    values = settings()
    # 连接函数可能加载.env；显式已有配置仍优先。
    return os.environ.get(key,values.get(key,default))


def flag(key):
    """把配置key转换为开关布尔值。

    仅字符串1或true（忽略大小写）视为开启，其余值关闭；配置读取异常继续抛出。"""
    return str(value(key)).lower() in ('1','true')
