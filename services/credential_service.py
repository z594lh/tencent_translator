"""
Amazon 凭证组服务

统一管理"开发者应用级凭证"(凭证组)与代理解析：
  - 凭证组: SP / Ads 两套 LWA 应用 + AWS(SigV4, 可空) + 代理，按品牌/主体拆分，多店铺共用
  - 店铺(amazon_shops)仅保存授权级凭证(sp_refresh_token / ads_refresh_token / ads_profile_id)

过渡期回退: 凭证组对应字段为空时，自动回退到 .env 环境变量，保证迁移期间业务不中断。
"""
import os
from typing import Optional, Dict

from services.mysql_service import get_db_connection


def get_credential_group(group_id: int) -> Optional[Dict]:
    """根据 ID 查询启用的凭证组，找不到返回 None"""
    if not group_id:
        return None
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, group_name, group_code,
                       sp_client_id, sp_client_secret,
                       ads_client_id, ads_client_secret,
                       aws_access_key, aws_secret_key,
                       proxy_url, status
                FROM amazon_credential_groups
                WHERE id = %s AND status = 1
                LIMIT 1
            """, (group_id,))
            return cursor.fetchone()
    finally:
        conn.close()


def _build_proxies(proxy_url: Optional[str] = None) -> Optional[Dict]:
    """构建 requests 代理字典。

    优先使用凭证组的 proxy_url；为空则回退环境变量 HTTP_PROXY / HTTPS_PROXY。
    """
    if proxy_url:
        return {"http": proxy_url, "https": proxy_url}

    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    return proxies if proxies else None


def get_sp_app_credentials(group_id: int = 1) -> Dict:
    """获取 SP-API 应用级凭证（凭证组优先，空则回退 .env）。

    返回: {client_id, client_secret, access_key, secret_key, proxies}
    """
    group = get_credential_group(group_id) or {}
    return {
        "client_id": group.get("sp_client_id") or os.getenv("AMAZON_CLIENT_ID", ""),
        "client_secret": group.get("sp_client_secret") or os.getenv("AMAZON_CLIENT_SECRET", ""),
        "access_key": group.get("aws_access_key") or os.getenv("AMAZON_ACCESS_KEY", ""),
        "secret_key": group.get("aws_secret_key") or os.getenv("AMAZON_SECRET_KEY", ""),
        "proxies": _build_proxies(group.get("proxy_url")),
    }


def get_ads_app_credentials(group_id: int = 1) -> Dict:
    """获取广告 API 应用级凭证（凭证组优先，空则回退 .env）。

    返回: {client_id, client_secret, proxies}
    """
    group = get_credential_group(group_id) or {}
    return {
        "client_id": group.get("ads_client_id") or os.getenv("AMAZON_ADS_CLIENT_ID", ""),
        "client_secret": group.get("ads_client_secret") or os.getenv("AMAZON_ADS_CLIENT_SECRET", ""),
        "proxies": _build_proxies(group.get("proxy_url")),
    }
