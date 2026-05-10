"""
亚马逊店铺管理服务
提供店铺查询、SP-API 客户端初始化等统一入口
"""
import os
from typing import List, Optional, Dict

from services.mysql_service import get_db_connection
from services.amazon_sp_client import AmazonSpApiClient

# 全局开发者应用凭证（所有店铺共用）
_AMAZON_CLIENT_ID = os.getenv("AMAZON_CLIENT_ID", "")
_AMAZON_CLIENT_SECRET = os.getenv("AMAZON_CLIENT_SECRET", "")
_AMAZON_ACCESS_KEY = os.getenv("AMAZON_ACCESS_KEY", "")
_AMAZON_SECRET_KEY = os.getenv("AMAZON_SECRET_KEY", "")


def _check_global_credentials():
    """检查全局凭证是否配置"""
    missing = []
    if not _AMAZON_CLIENT_ID:
        missing.append("AMAZON_CLIENT_ID")
    if not _AMAZON_CLIENT_SECRET:
        missing.append("AMAZON_CLIENT_SECRET")
    if not _AMAZON_ACCESS_KEY:
        missing.append("AMAZON_ACCESS_KEY")
    if not _AMAZON_SECRET_KEY:
        missing.append("AMAZON_SECRET_KEY")
    if missing:
        raise ValueError(f"缺少全局 Amazon 开发者应用凭证: {', '.join(missing)}")


def get_shop_by_id(shop_id: int) -> Optional[Dict]:
    """根据 ID 查询单个店铺信息"""
    if not shop_id:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, shop_name, seller_id, refresh_token,
                       marketplace_id, region, status, is_default
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
            cursor.execute("""
                SELECT id, shop_name, seller_id, refresh_token,
                       marketplace_id, region, status, is_default
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
            cursor.execute("""
                SELECT id, shop_name, seller_id, refresh_token,
                       marketplace_id, region, status, is_default
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
    _check_global_credentials()

    if not shop_id:
        raise ValueError("shop_id 不能为空")

    shop = get_shop_by_id(shop_id)
    if not shop:
        raise ValueError(f"未找到有效的店铺配置 (shop_id={shop_id})")

    return AmazonSpApiClient(
        client_id=_AMAZON_CLIENT_ID,
        client_secret=_AMAZON_CLIENT_SECRET,
        refresh_token=shop["refresh_token"],
        access_key=_AMAZON_ACCESS_KEY,
        secret_key=_AMAZON_SECRET_KEY,
        marketplace_id=marketplace_id or shop["marketplace_id"],
        region=region or shop["region"],
        seller_id=shop["seller_id"],
    )
