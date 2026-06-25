"""
亚马逊店铺管理服务
提供店铺查询、SP-API / 广告 API 客户端初始化等统一入口

凭证分层:
  - 应用级凭证(凭证组): 由 services.credential_service 解析(凭证组优先, 空则回退 .env)
  - 授权级凭证(店铺): sp_refresh_token / ads_refresh_token / ads_profile_id 存于 amazon_shops
"""
from typing import List, Optional, Dict

from services.mysql_service import get_db_connection
from services.amazon_sp_client import AmazonSpApiClient
from services.amazon_ads_client import AmazonAdsApiClient
from services.credential_service import get_sp_app_credentials, get_ads_app_credentials


# 店铺查询统一列（含凭证组关联与广告字段）
_SHOP_COLUMNS = """
    id, credential_group_id, shop_name, seller_id,
    sp_refresh_token, ads_refresh_token, ads_profile_id,
    marketplace_id, region, status, is_default
"""


def get_shop_by_id(shop_id: int) -> Optional[Dict]:
    """根据 ID 查询单个店铺信息"""
    if not shop_id:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT {_SHOP_COLUMNS}
                FROM amazon_shops
                WHERE id = %s AND status = 1
                LIMIT 1
            """, (shop_id,))
            return cursor.fetchone()
    finally:
        conn.close()


def get_default_shop() -> Optional[Dict]:
    """查询默认店铺（历史数据归属的店铺）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT {_SHOP_COLUMNS}
                FROM amazon_shops
                WHERE is_default = 1 AND status = 1
                LIMIT 1
            """)
            return cursor.fetchone()
    finally:
        conn.close()


def get_all_active_shops() -> List[Dict]:
    """查询所有启用的店铺"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT {_SHOP_COLUMNS}
                FROM amazon_shops
                WHERE status = 1
                ORDER BY id
            """)
            return cursor.fetchall()
    finally:
        conn.close()


def get_sp_api_client(shop_id: int, marketplace_id: str = None, region: str = None) -> AmazonSpApiClient:
    """
    根据店铺 ID 初始化 SP-API 客户端

    参数:
        shop_id: 店铺ID（必填）
        marketplace_id: 可覆盖店铺配置中的 marketplace_id
        region: 可覆盖店铺配置中的 region

    返回:
        AmazonSpApiClient 实例
    """
    if not shop_id:
        raise ValueError("shop_id 不能为空")

    shop = get_shop_by_id(shop_id)
    if not shop:
        raise ValueError(f"未找到有效的店铺配置 (shop_id={shop_id})")

    creds = get_sp_app_credentials(shop["credential_group_id"])

    return AmazonSpApiClient(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=shop["sp_refresh_token"],
        access_key=creds["access_key"],
        secret_key=creds["secret_key"],
        marketplace_id=marketplace_id or shop["marketplace_id"],
        region=region or shop["region"],
        seller_id=shop["seller_id"],
        proxies=creds["proxies"],
    )


def get_ads_api_client(shop_id: int, region: str = None) -> AmazonAdsApiClient:
    """
    根据店铺 ID 初始化广告 API 客户端

    参数:
        shop_id: 店铺ID（必填）
        region: 可覆盖店铺配置中的 region

    返回:
        AmazonAdsApiClient 实例

    说明:
        店铺需已授权广告 API（amazon_shops.ads_refresh_token 不为空）。
        ads_profile_id 为空时仅可调用不需要 Scope 的接口（如 list_profiles）。
    """
    if not shop_id:
        raise ValueError("shop_id 不能为空")

    shop = get_shop_by_id(shop_id)
    if not shop:
        raise ValueError(f"未找到有效的店铺配置 (shop_id={shop_id})")

    if not shop.get("ads_refresh_token"):
        raise ValueError(f"店铺未授权广告 API (shop_id={shop_id}, 缺少 ads_refresh_token)")

    creds = get_ads_app_credentials(shop["credential_group_id"])

    return AmazonAdsApiClient(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        refresh_token=shop["ads_refresh_token"],
        profile_id=shop.get("ads_profile_id"),
        region=region or shop["region"],
        proxies=creds["proxies"],
    )
