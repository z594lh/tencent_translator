"""Listing归属与绩效共用的参数、时区、身份和JSON工具。

调用方：两个Blueprint、归属服务及人员绩效查询。
只做验证和纯转换；配置读取委托configuration，不直接修改任何业务表。
本模块时间约定：业务日按站点时区解释，落库时间使用无tzinfo的UTC；
day_bounds返回左闭右开区间，不能把订单源固定UTC-7存储时间直接当UTC。
"""
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from services.listing_performance.configuration import flag, value as configured_value


class DomainError(Exception):
    """可安全返回HTTP调用方的业务异常，携带message与status。

    与SQL/系统异常区分，响应包装只直接展示这类明确业务提示。
    """
    def __init__(self, message, status=400):
        """创建可安全返回前端的业务异常。

        message为业务提示，status为HTTP状态码，默认400；不承载敏感SQL或凭证。"""
        super().__init__(message)
        self.status = status


def enabled():
    """读取归属/绩效总开关，返回布尔值。

    配置优先级遵循configuration；不影响原店铺/SKU报表生成和读取。"""
    return flag('LISTING_PERFORMANCE_ENABLED')


def require_enabled():
    """校验功能已启用；关闭时抛出503业务异常。

    用于HTTP响应包装，避免功能未部署时继续访问新业务表。"""
    if not enabled():
        raise DomainError('Listing 归属与绩效功能尚未启用', 503)


def positive_id(value, name):
    """把value校验为正整数ID并返回int。

    name用于错误提示；拒绝布尔值、非整数表示及零/负数，不访问数据库。"""
    if isinstance(value, bool):
        raise DomainError(f'{name} 必须是正整数')
    try:
        result = int(str(value))
    except (ValueError, TypeError):
        raise DomainError(f'{name} 必须是正整数') from None
    if result <= 0:
        raise DomainError(f'{name} 必须是正整数')
    return result


def parse_date(value):
    """校验严格YYYY-MM-DD字符串并返回date。

    不接受时间部分或非标准日期；非法输入抛出400业务异常。"""
    try:
        result = date.fromisoformat(str(value))
        if result.isoformat() != value:
            raise ValueError()
        return result
    except (ValueError, TypeError):
        raise DomainError('日期必须为 YYYY-MM-DD') from None


def configured_marketplaces():
    """读取PERFORMANCE_MARKETPLACES并解析为站点配置字典。

    非JSON对象或解析失败报503；不猜测站点、币种和时区。"""
    try:
        configured = json.loads(configured_value('PERFORMANCE_MARKETPLACES','{}'))
        if not isinstance(configured,dict):
            raise ValueError()
        return configured
    except (ValueError,TypeError):
        raise DomainError('PERFORMANCE_MARKETPLACES 配置无效',503) from None


def validate_shop(cursor, shop, market):
    """核实只读绩效查询的启用店铺和站点。

    归属写操作由ownership_service自行锁定店铺，此处不锁定、不触发生成。
    调用方持有连接/事务，本函数不提交、不创建店铺，也不校验用户权限。
    """
    cursor.execute('SELECT id,marketplace_id FROM amazon_shops WHERE id=%s AND status=1', (shop,))
    row = cursor.fetchone()
    if not row or row['marketplace_id'] != market:
        raise DomainError('店铺/站点不匹配或店铺已停用')


def market_config(marketplace_id):
    """按marketplace_id读取并校验站点配置，返回该站点字典。

    校验IANA时区及三字符币种；缺站点或配置无效报503。
    绩效只读取已有日报，不验证或转换原订单/广告同步时区。"""
    try:
        configured = configured_marketplaces()
        if not isinstance(configured,dict):
            raise ValueError()
        config = configured.get(marketplace_id)
        if not config:
            raise DomainError('该站点尚未配置业务时区和币种', 503)
        ZoneInfo(config['timezone'])
        if not isinstance(config['currency'], str) or len(config['currency']) != 3:
            raise ValueError()
        if config.get('report_currency') and len(config['report_currency']) != 3:
            raise ValueError()
        return config
    except (KeyError, ValueError, TypeError, ZoneInfoNotFoundError):
        raise DomainError('PERFORMANCE_MARKETPLACES 配置无效', 503) from None


def utc_now():
    """返回当前UTC时间，移除tzinfo以适配MySQL DATETIME。

    返回值仍表示UTC，不是服务器本地时间。"""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def day_bounds(day, tz):
    """将业务日day和IANA时区tz转换为UTC起止时间元组。

    区间为[start,end)，分别按两天本地零点转换，因此适应夏令时23/25小时日。
    返回无tzinfo的UTC datetime，不读写数据库。"""
    zone = ZoneInfo(tz)
    start = datetime.combine(day, time.min, zone).astimezone(timezone.utc).replace(tzinfo=None)
    end = datetime.combine(day + timedelta(days=1), time.min, zone).astimezone(timezone.utc).replace(tzinfo=None)
    return start, end


def effective_time(day, tz, now=None):
    """校验常规交接日期并返回该日站点零点对应UTC。

    day必须晚于站点今天且不超过90天；now可注入无tzinfo的UTC供验证。
    不允许经常规分配接口追溯修改过去，非法日期抛出业务异常。"""
    now = now or utc_now()
    today = now.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(tz)).date()
    if day <= today:
        raise DomainError('常规分配只能从站点次日或更晚的零点生效')
    if day > today + timedelta(days=90):
        raise DomainError('交接日期不能超过站点当前日期后 90 天')
    return day_bounds(day, tz)[0]


def next_business_date(tz, now=None):
    """返回指定时区tz的明天日期字符串。

    now按UTC解释；用于分配未填写日期时冻结默认次日，不写归属。"""
    now = now or utc_now()
    return (now.replace(tzinfo=timezone.utc).astimezone(ZoneInfo(tz)).date() + timedelta(days=1)).isoformat()


def identity(shop_id, marketplace_id, sku):
    """对[shop_id,marketplace_id,sku]序列生成SHA-256身份键。

    SKU保留原始大小写；不同店铺/站点不会共用一个负责人历史。"""
    return hashlib.sha256(json.dumps([shop_id, marketplace_id, sku], ensure_ascii=False).encode()).hexdigest()


def json_data(value):
    """将日期、Decimal和普通JSON对象稳定序列化为字符串。

    键排序用于签名/哈希；Decimal保留字符串精度，无tzinfo datetime标记Z。
    不支持的类型抛TypeError；不是脱敏函数，不能直接用于公开源数据日志。"""
    def convert(obj):
        """供json_data处理JSON原生不支持的对象。

        日期返回ISO格式，Decimal返回字符串；未知类型报TypeError，不修改输入。"""
        if isinstance(obj, datetime):
            return obj.isoformat() + ('Z' if obj.tzinfo is None else '')
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError(type(obj).__name__)
    return json.dumps(value, ensure_ascii=False, default=convert, sort_keys=True)


def decode(value, default=None):
    """解析字符串/bytes JSON，或直接返回已解析对象。

    value为None时返回default；非法JSON继续抛异常，不默默替换成零或空数据。"""
    return json.loads(value) if isinstance(value, (str, bytes)) else (value if value is not None else default)
