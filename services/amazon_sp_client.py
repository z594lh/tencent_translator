"""
Amazon Selling Partner API (SP-API) 客户端
支持 AWS SigV4 签名、LWA Token 自动刷新、HTTP 代理

国内环境必备：配置 HTTP_PROXY / HTTPS_PROXY 环境变量

需要环境变量:
    AMAZON_CLIENT_ID        - LWA App Client ID
    AMAZON_CLIENT_SECRET    - LWA App Client Secret
    AMAZON_REFRESH_TOKEN    - 店铺授权后的 Refresh Token
    AMAZON_ACCESS_KEY       - AWS IAM User Access Key ID
    AMAZON_SECRET_KEY       - AWS IAM User Secret Access Key
    AMAZON_MARKETPLACE_ID   - Marketplace ID (默认 US: ATVPDKIKX0DER)
    AMAZON_REGION           - API 区域: na|eu|fe (默认 na)
    HTTP_PROXY              - HTTP 代理地址，如 http://127.0.0.1:7890
    HTTPS_PROXY             - HTTPS 代理地址，如 http://127.0.0.1:7890

参考文档:
    https://developer-docs.amazon.com/sp-api/docs/connecting-to-the-selling-partner-api
"""
import os
import time
from datetime import datetime
from urllib.parse import urlencode

import requests
from requests_aws4auth import AWS4Auth


# ==================== 常量配置 ====================
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

SP_API_ENDPOINTS = {
    "na": "https://sellingpartnerapi-na.amazon.com",
    "eu": "https://sellingpartnerapi-eu.amazon.com",
    "fe": "https://sellingpartnerapi-fe.amazon.com",
}

# SP-API AWS SigV4 签名 Region 映射
AWS_SIGN_REGIONS = {
    "na": "us-east-1",
    "eu": "eu-west-1",
    "fe": "us-west-2",
}

SP_API_SERVICE = "execute-api"

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
}


# ==================== 读取环境变量默认值 ====================
_AMAZON_CLIENT_ID = os.getenv("AMAZON_CLIENT_ID", "")
_AMAZON_CLIENT_SECRET = os.getenv("AMAZON_CLIENT_SECRET", "")
_AMAZON_REFRESH_TOKEN = os.getenv("AMAZON_REFRESH_TOKEN", "")
_AMAZON_ACCESS_KEY = os.getenv("AMAZON_ACCESS_KEY", "")
_AMAZON_SECRET_KEY = os.getenv("AMAZON_SECRET_KEY", "")
_AMAZON_MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")
_AMAZON_REGION = os.getenv("AMAZON_REGION", "na").lower()


def _build_proxies():
    """从环境变量构建 requests 代理字典"""
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
    https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    return proxies if proxies else None


class AmazonSpApiClient:
    """
    Amazon SP-API 客户端

    用法示例:
        client = AmazonSpApiClient()
        orders = client.get_orders(created_after="2026-04-01T00:00:00Z")
    """

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        access_key: str = None,
        secret_key: str = None,
        marketplace_id: str = None,
        region: str = None,
        proxies: dict = None,
    ):
        self.client_id = client_id or _AMAZON_CLIENT_ID
        self.client_secret = client_secret or _AMAZON_CLIENT_SECRET
        self.refresh_token = refresh_token or _AMAZON_REFRESH_TOKEN
        self.access_key = access_key or _AMAZON_ACCESS_KEY
        self.secret_key = secret_key or _AMAZON_SECRET_KEY
        self.marketplace_id = marketplace_id or _AMAZON_MARKETPLACE_ID
        self.region = (region or _AMAZON_REGION).lower()
        self.base_url = SP_API_ENDPOINTS.get(self.region, SP_API_ENDPOINTS["na"])
        self.aws_region = AWS_SIGN_REGIONS.get(self.region, "us-east-1")
        self.proxies = proxies if proxies is not None else _build_proxies()

        self._access_token: str = None
        self._token_expires_at: float = 0  # 时间戳

        # 校验必填参数
        missing = []
        if not self.client_id:
            missing.append("client_id / AMAZON_CLIENT_ID")
        if not self.client_secret:
            missing.append("client_secret / AMAZON_CLIENT_SECRET")
        if not self.refresh_token:
            missing.append("refresh_token / AMAZON_REFRESH_TOKEN")
        if not self.access_key:
            missing.append("access_key / AMAZON_ACCESS_KEY")
        if not self.secret_key:
            missing.append("secret_key / AMAZON_SECRET_KEY")
        if missing:
            raise ValueError(f"缺少 Amazon API 凭证: {', '.join(missing)}")

    # -------------------- Token 管理 --------------------

    def _refresh_access_token(self) -> str:
        """用 refresh_token 换取新的 access_token"""
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = requests.post(
            LWA_TOKEN_URL,
            data=payload,
            proxies=self.proxies,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        # access_token 有效期通常为 1 小时，提前 5 分钟刷新
        expires_in = data.get("expires_in", 3600)
        self._token_expires_at = time.time() + expires_in - 300
        return self._access_token

    def _get_access_token(self) -> str:
        """获取有效的 access_token，过期自动刷新"""
        if not self._access_token or time.time() >= self._token_expires_at:
            return self._refresh_access_token()
        return self._access_token

    def _get_aws_auth(self):
        """生成 AWS SigV4 认证对象"""
        return AWS4Auth(
            self.access_key,
            self.secret_key,
            self.aws_region,
            SP_API_SERVICE,
        )

    # -------------------- 基础请求 --------------------

    def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        json_data: dict = None,
        headers: dict = None,
    ) -> dict:
        """发送带 AWS SigV4 签名 + LWA Token 的 SP-API 请求"""
        url = f"{self.base_url}{path}"
        token = self._get_access_token()

        h = {**DEFAULT_HEADERS, "x-amz-access-token": token}
        if headers:
            h.update(headers)

        auth = self._get_aws_auth()

        resp = requests.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            headers=h,
            auth=auth,
            proxies=self.proxies,
            timeout=60,
        )

        # 401 时尝试刷新 token 重试一次
        if resp.status_code == 401:
            self._access_token = None
            h["x-amz-access-token"] = self._get_access_token()
            resp = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=h,
                auth=auth,
                proxies=self.proxies,
                timeout=60,
            )

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # 打印亚马逊返回的详细错误信息，便于调试
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text
            print(f"[SP-API Error] {resp.status_code} {resp.url}")
            print(f"[SP-API Error Body] {err_body}")
            raise
        if resp.status_code == 204 or not resp.text:
            return {}
        return resp.json()

    # -------------------- Orders API --------------------

    def get_orders(
        self,
        created_after: str = None,
        created_before: str = None,
        last_updated_after: str = None,
        order_statuses: list = None,
        marketplace_ids: list = None,
        max_results: int = 100,
        next_token: str = None,
    ) -> dict:
        """
        获取订单列表
        https://developer-docs.amazon.com/sp-api/docs/orders-api-v0-reference#getorders
        """
        params = {"MaxResultsPerPage": max(1, min(max_results, 100))}
        if created_after:
            params["CreatedAfter"] = created_after
        if created_before:
            params["CreatedBefore"] = created_before
        if last_updated_after:
            params["LastUpdatedAfter"] = last_updated_after
        if order_statuses:
            params["OrderStatuses"] = ",".join(order_statuses)
        if marketplace_ids:
            params["MarketplaceIds"] = ",".join(marketplace_ids)
        else:
            params["MarketplaceIds"] = self.marketplace_id
        if next_token:
            params["NextToken"] = next_token

        return self._request("GET", "/orders/v0/orders", params=params)

    def get_order(self, order_id: str) -> dict:
        """获取单个订单详情"""
        return self._request("GET", f"/orders/v0/orders/{order_id}")

    def get_order_items(self, order_id: str) -> dict:
        """获取订单商品列表"""
        return self._request("GET", f"/orders/v0/orders/{order_id}/orderItems")

    def get_order_buyer_info(self, order_id: str) -> dict:
        """获取订单买家信息（PII 权限需要额外申请）"""
        return self._request("GET", f"/orders/v0/orders/{order_id}/buyerInfo")

    # -------------------- Reports API --------------------

    def get_reports(
        self,
        report_types: list = None,
        processing_statuses: list = None,
        marketplace_ids: list = None,
        created_since: str = None,
        created_until: str = None,
        page_size: int = 10,
        next_token: str = None,
    ) -> dict:
        """获取报告列表"""
        params = {"pageSize": max(1, min(page_size, 100))}
        if report_types:
            params["reportTypes"] = ",".join(report_types)
        if processing_statuses:
            params["processingStatuses"] = ",".join(processing_statuses)
        if marketplace_ids:
            params["marketplaceIds"] = ",".join(marketplace_ids)
        else:
            params["marketplaceIds"] = self.marketplace_id
        if created_since:
            params["createdSince"] = created_since
        if created_until:
            params["createdUntil"] = created_until
        if next_token:
            params["nextToken"] = next_token

        return self._request("GET", "/reports/2021-06-30/reports", params=params)

    def get_report(self, report_id: str) -> dict:
        """获取单个报告详情"""
        return self._request("GET", f"/reports/2021-06-30/reports/{report_id}")

    def get_report_document(self, report_document_id: str) -> dict:
        """获取报告下载信息（含下载 URL）"""
        return self._request(
            "GET",
            f"/reports/2021-06-30/documents/{report_document_id}",
        )

    def create_report(self, report_type: str, marketplace_ids: list = None, **kwargs) -> dict:
        """创建报告任务"""
        body = {"reportType": report_type}
        if marketplace_ids:
            body["marketplaceIds"] = marketplace_ids
        else:
            body["marketplaceIds"] = [self.marketplace_id]
        body.update(kwargs)
        return self._request("POST", "/reports/2021-06-30/reports", json_data=body)

    # -------------------- Inventory API --------------------

    def get_inventory_summaries(
        self,
        seller_skus: list = None,
        granularity_type: str = "Marketplace",
        granularity_id: str = None,
        marketplace_ids: list = None,
        details: bool = False,
        start_date_time: str = None,
        next_token: str = None,
    ) -> dict:
        """获取库存汇总

        Args:
            seller_skus: SKU列表，可选
            granularity_type: 粒度类型，默认"Marketplace"
            granularity_id: 粒度ID，可选
            marketplace_ids: 市场ID列表，可选
            details: 是否显示详细信息
            start_date_time: 开始时间
            next_token: 分页令牌
        """
        params = {
            "details": str(details).lower(),
            "granularityType": granularity_type,
            "marketplaceIds": ",".join(marketplace_ids) if marketplace_ids else self.marketplace_id
        }

        if granularity_id:
            params["granularityId"] = granularity_id
        else:
            params["granularityId"] = self.marketplace_id

        if seller_skus:
            params["sellerSkus"] = ",".join(seller_skus)
        if start_date_time:
            params["startDateTime"] = start_date_time
        if next_token:
            params["nextToken"] = next_token

        return self._request("GET", "/fba/inventory/v1/summaries", params=params)

    # -------------------- Products API (Pricing) --------------------

    def get_competitive_pricing(self, asins: list = None, skus: list = None) -> dict:
        """获取竞争定价信息"""
        params = {"MarketplaceId": self.marketplace_id}
        if asins:
            params["Asins"] = ",".join(asins)
        if skus:
            params["Skus"] = ",".join(skus)
        return self._request("GET", "/products/pricing/v0/competitivePrice", params=params)

    def get_listing_offers(self, asin: str, item_condition: str = "New") -> dict:
        """获取 Listing 的所有报价"""
        params = {"ItemCondition": item_condition, "MarketplaceId": self.marketplace_id}
        return self._request(
            "GET",
            f"/products/pricing/v0/listings/{asin}/offers",
            params=params,
        )

    # -------------------- Catalog Items API --------------------

    def search_catalog_items(
        self,
        keywords: list = None,
        marketplace_ids: list = None,
        included_data: list = None
    ) -> dict:
        """搜索商品目录

        Args:
            keywords: 搜索关键词列表
            marketplace_ids: 市场ID列表
            included_data: 包含的数据类型，如 ['summaries', 'images', 'offers']
        """
        params = {}
        if keywords:
            params["keywords"] = ",".join(keywords)
        if marketplace_ids:
            params["marketplaceIds"] = ",".join(marketplace_ids)
        else:
            params["marketplaceIds"] = self.marketplace_id
        if included_data:
            params["includedData"] = ",".join(included_data)

        return self._request("GET", "/catalog/2022-04-01/items", params=params)

    def get_catalog_item(self, asin: str, included_data: list = None) -> dict:
        """获取单个商品详情"""
        params = {"marketplaceIds": self.marketplace_id}
        if included_data:
            params["includedData"] = ",".join(included_data)

        return self._request("GET", f"/catalog/2022-04-01/items/{asin}", params=params)

    # -------------------- Fulfillment Inbound API --------------------

    def get_shipments(
        self,
        shipment_status_list: list = None,
        shipment_id_list: list = None,
        last_update_after: str = None,
        last_update_before: str = None,
        query_type: str = "SHIPMENT"
    ) -> dict:
        """获取货件列表

        Args:
            shipment_status_list: 货件状态列表，如 ['WORKING', 'SHIPPED', 'RECEIVING']
            shipment_id_list: 货件ID列表
            last_update_after: 最后更新时间（ISO格式）
            last_update_before: 最后更新时间（ISO格式）
            query_type: 查询类型，默认"SHIPMENT"
        """
        params = {"QueryType": query_type}

        if shipment_status_list:
            params["ShipmentStatusList"] = ",".join(shipment_status_list)
        if shipment_id_list:
            params["ShipmentIdList"] = ",".join(shipment_id_list)
        if last_update_after:
            params["LastUpdatedAfter"] = last_update_after
        if last_update_before:
            params["LastUpdatedBefore"] = last_update_before

        return self._request("GET", "/fba/inbound/v0/shipments", params=params)

    def get_shipment_items(self, shipment_id: str) -> dict:
        """获取货件商品列表"""
        return self._request("GET", f"/fba/inbound/v0/shipments/{shipment_id}/items")

    # -------------------- Listings Items API --------------------

    def get_listings_item(self, sku: str, included_data: list = None) -> dict:
        """获取商品Listing详情"""
        params = {"marketplaceIds": self.marketplace_id}
        if included_data:
            params["includedData"] = ",".join(included_data)

        return self._request("GET", f"/listings/2021-08-01/items/{sku}", params=params)

    def get_listings_items(self, included_data: list = None) -> dict:
        """获取所有商品Listing列表"""
        params = {"marketplaceIds": self.marketplace_id}
        if included_data:
            params["includedData"] = ",".join(included_data)

        return self._request("GET", "/listings/2021-08-01/items", params=params)


# ==================== 快捷函数（单例风格）====================

def get_orders(**kwargs) -> dict:
    """快捷获取订单列表（使用环境变量配置）"""
    client = AmazonSpApiClient()
    return client.get_orders(**kwargs)


def get_order(order_id: str) -> dict:
    """快捷获取订单详情"""
    client = AmazonSpApiClient()
    return client.get_order(order_id)


def get_order_items(order_id: str) -> dict:
    """快捷获取订单商品"""
    client = AmazonSpApiClient()
    return client.get_order_items(order_id)
