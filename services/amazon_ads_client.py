"""
Amazon Advertising API (广告 API) 客户端

与 SP-API 的区别：
  - 使用独立的 LWA 应用 (ads_client_id / ads_client_secret) 与独立 refresh_token
  - 不使用 AWS SigV4 签名，仅 LWA Bearer Token
  - 每次业务调用需带头部:
      Amazon-Advertising-API-ClientId: <ads_client_id>
      Amazon-Advertising-API-Scope:    <profile_id>   (profiles 列表接口不需要)
  - endpoint 按区域区分: na / eu / fe
  - token 端点与 SP-API 一致: https://api.amazon.com/auth/o2/token

典型流程: 先 list_profiles() 拿到 profile_id，写入店铺 ads_profile_id，再调业务接口。

参考文档:
    https://advertising.amazon.com/API/docs/en-us/guides/get-started/overview
"""
import time
import gzip
import json

import requests


LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

ADS_API_ENDPOINTS = {
    "na": "https://advertising-api.amazon.com",
    "eu": "https://advertising-api-eu.amazon.com",
    "fe": "https://advertising-api-fe.amazon.com",
}


def _upper(val):
    """Amazon Ads API 的 state 字段必须大写"""
    if val and isinstance(val, str):
        return val.upper()
    return val


class AmazonAdsApiClient:
    """Amazon 广告 API 客户端

    用法示例:
        client = AmazonAdsApiClient(client_id, client_secret, refresh_token, region="na")
        profiles = client.list_profiles()          # 首次获取 profile_id
        client.profile_id = profiles[0]["profileId"]
        campaigns = client.list_sp_campaigns()
    """

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        refresh_token: str = None,
        profile_id: str = None,
        region: str = "na",
        proxies: dict = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.profile_id = profile_id
        self.region = (region or "na").lower()
        self.base_url = ADS_API_ENDPOINTS.get(self.region, ADS_API_ENDPOINTS["na"])
        self.proxies = proxies

        self._access_token: str = None
        self._token_expires_at: float = 0

        missing = []
        if not self.client_id:
            missing.append("ads_client_id")
        if not self.client_secret:
            missing.append("ads_client_secret")
        if not self.refresh_token:
            missing.append("ads_refresh_token")
        if missing:
            raise ValueError(f"缺少 Amazon 广告 API 凭证: {', '.join(missing)}")

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
        expires_in = data.get("expires_in", 3600)
        self._token_expires_at = time.time() + expires_in - 300
        return self._access_token

    def _get_access_token(self) -> str:
        """获取有效的 access_token，过期自动刷新"""
        if not self._access_token or time.time() >= self._token_expires_at:
            return self._refresh_access_token()
        return self._access_token

    # -------------------- 基础请求 --------------------

    def _headers(self, with_scope: bool = True, extra: dict = None) -> dict:
        """构建广告 API 请求头"""
        h = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if with_scope:
            if not self.profile_id:
                raise ValueError("缺少 profile_id (ads_profile_id)，该接口需要广告账户 Scope")
            h["Amazon-Advertising-API-Scope"] = str(self.profile_id)
        if extra:
            h.update(extra)
        return h

    def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        json_data: dict = None,
        with_scope: bool = True,
        headers: dict = None,
    ) -> dict:
        """发送带 LWA Token + ClientId/Scope 头的广告 API 请求"""
        url = f"{self.base_url}{path}"
        h = self._headers(with_scope=with_scope, extra=headers)

        resp = requests.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            headers=h,
            proxies=self.proxies,
            timeout=60,
        )

        # 401 时刷新 token 重试一次
        if resp.status_code == 401:
            self._access_token = None
            h = self._headers(with_scope=with_scope, extra=headers)
            resp = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_data,
                headers=h,
                proxies=self.proxies,
                timeout=60,
            )

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text
            print(f"[Ads-API Error] {resp.status_code} {resp.url}")
            print(f"[Ads-API Error Body] {err_body}")
            raise

        if resp.status_code == 204 or not resp.text:
            return {}
        return resp.json()

    # -------------------- Profiles --------------------

    def list_profiles(self) -> list:
        """获取广告账户 profiles（不需要 Scope 头）。

        返回元素含 profileId，用于设置店铺 ads_profile_id。
        """
        return self._request("GET", "/v2/profiles", with_scope=False)

    # -------------------- Sponsored Products (v3) --------------------

    _V3_SP_CAMPAIGN_CT = "application/vnd.spcampaign.v3+json"

    def list_sp_campaigns(self, state_filter: list = None, max_results: int = 100, next_token: str = None) -> dict:
        """获取 Sponsored Products 广告活动列表 (v3)"""
        body = {"maxResults": max(1, min(max_results, 100))}
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request("POST", "/sp/campaigns/list", json_data=body,
                             headers={"Content-Type": self._V3_SP_CAMPAIGN_CT, "Accept": self._V3_SP_CAMPAIGN_CT})

    # -------------------- 异步报告 (v3 Reporting) --------------------

    _V3_REPORT_CT = "application/vnd.createasyncreportrequest.v3+json"

    def _create_async_report(self, report_body: dict) -> str:
        """创建异步报告，返回 reportId"""
        resp = self._request(
            "POST", "/reporting/reports",
            json_data=report_body,
            headers={"Content-Type": self._V3_REPORT_CT, "Accept": self._V3_REPORT_CT},
        )
        report_id = resp.get("reportId")
        if not report_id:
            raise ValueError(f"创建报告失败，响应无 reportId: {resp}")
        return str(report_id)

    def _get_report_status(self, report_id: str) -> dict:
        """查询报告状态，完成时包含 url 字段"""
        return self._request("GET", f"/reporting/reports/{report_id}")

    def _poll_report_completion(self, report_id: str, max_wait: int = 300, interval: int = 5) -> str:
        """轮询直到报告完成，返回下载 URL"""
        deadline = time.time() + max_wait
        while time.time() < deadline:
            status = self._get_report_status(report_id)
            state = (status.get("status") or "").upper()
            if state == "COMPLETED":
                url = status.get("url")
                if not url:
                    raise ValueError(f"报告 {report_id} 已完成但缺少下载 URL")
                return url
            if state in ("FAILURE", "CANCELLED"):
                raise RuntimeError(f"报告 {report_id} 失败: {status}")
            print(f"[Ads Report] {report_id} 状态={state}, {interval}s 后重试...")
            time.sleep(interval)
        raise TimeoutError(f"报告 {report_id} 在 {max_wait}s 内未完成")

    def _download_report_content(self, url: str) -> list:
        """下载报告内容（GZIP JSON），返回行列表。

        处理三种可能的格式: GZIP 压缩的 JSON 数组 / GZIP 压缩的 JSON-Lines / 未压缩 JSON。
        """
        resp = requests.get(url, timeout=120, proxies=self.proxies)
        resp.raise_for_status()
        data = resp.content
        try:
            data = gzip.decompress(data)
        except gzip.BadGzipFile:
            pass
        text = data.decode("utf-8").strip()
        if not text:
            return []
        # 尝试整体解析为 JSON 数组
        if text.startswith("["):
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        # JSON-Lines: 每行一个 JSON 对象
        rows = []
        for line in text.split("\n"):
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    def create_sp_product_ads_report(self, start_date: str, end_date: str) -> str:
        """创建 SP Advertised Product 报告（ASIN 维度），返回 reportId。

        用于同步 amazon_ad_spend 表。
        """
        body = {
            "name": "SP Product Ads Daily Sync",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser"],
                "columns": [
                    "date",
                    "campaignId", "campaignName",
                    "adGroupId", "adGroupName",
                    "advertisedAsin", "advertisedSku",
                    "impressions", "clicks", "cost",
                    "purchases7d", "purchases30d",
                    "sales7d", "sales30d",
                ],
                "reportTypeId": "spAdvertisedProduct",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }
        return self._create_async_report(body)

    def create_sp_campaigns_report(self, start_date: str, end_date: str) -> str:
        """创建 SP Campaigns 报告（活动维度），返回 reportId。

        用于补充 campaign_name 等字段。
        """
        body = {
            "name": "SP Campaigns Daily Sync",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": [
                    "date",
                    "campaignId", "campaignName",
                    "impressions", "clicks", "cost",
                    "purchases7d", "purchases30d",
                    "sales7d", "sales30d",
                ],
                "reportTypeId": "spCampaigns",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }
        return self._create_async_report(body)

    def fetch_report_rows(self, report_id: str, max_wait: int = 300) -> list:
        """创建 → 轮询 → 下载 → 返回行列表（一站式）"""
        url = self._poll_report_completion(report_id, max_wait=max_wait)
        return self._download_report_content(url)

    # ==================== 广告活动 (Campaigns) v3 ====================

    def update_campaign(self, campaign_id: int, updates: dict) -> dict:
        """更新 SP 广告活动属性 (state / dailyBudget / bidding / startDate / endDate)"""
        body = {"campaignId": str(campaign_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        if "dailyBudget" in updates:
            body["budget"] = {"budgetType": "DAILY", "budget": updates["dailyBudget"]}
        if "startDate" in updates:
            body["startDate"] = updates["startDate"]
        if "endDate" in updates:
            body["endDate"] = updates["endDate"]
        if "bidding" in updates:
            body["dynamicBidding"] = updates["bidding"]
        return self._request(
            "PUT", "/sp/campaigns",
            json_data={"campaigns": [body]},
            headers={"Content-Type": self._V3_SP_CAMPAIGN_CT, "Accept": self._V3_SP_CAMPAIGN_CT},
        )

    _ID_FIELDS = {"campaignId", "adGroupId", "keywordId", "targetId", "adId"}

    @staticmethod
    def _stringify_ids(items):
        for item in items:
            for f in AmazonAdsApiClient._ID_FIELDS:
                if f in item and not isinstance(item[f], str):
                    item[f] = str(item[f])
            if "state" in item and item["state"] and isinstance(item["state"], str):
                item["state"] = item["state"].upper()

    def create_campaigns(self, campaigns: list) -> dict:
        """批量创建 SP 广告活动"""
        self._stringify_ids(campaigns)
        return self._request(
            "POST", "/sp/campaigns",
            json_data={"campaigns": campaigns},
            headers={"Content-Type": self._V3_SP_CAMPAIGN_CT, "Accept": self._V3_SP_CAMPAIGN_CT},
        )

    # ==================== 广告组 (Ad Groups) v3 ====================

    _V3_SP_ADGROUP_CT = "application/vnd.spadgroup.v3+json"

    def list_ad_groups(self, campaign_id: int = None, state_filter: list = None,
                       max_results: int = 100, next_token: str = None) -> dict:
        """获取 SP 广告组列表"""
        body = {"maxResults": max(1, min(max_results, 100))}
        if campaign_id:
            body["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/adGroups/list", json_data=body,
            headers={"Content-Type": self._V3_SP_ADGROUP_CT, "Accept": self._V3_SP_ADGROUP_CT},
        )

    def update_ad_group(self, ad_group_id: int, updates: dict) -> dict:
        """更新 SP 广告组 (state / defaultBid)"""
        body = {"adGroupId": str(ad_group_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        if "defaultBid" in updates:
            body["defaultBid"] = updates["defaultBid"]
        return self._request(
            "PUT", "/sp/adGroups",
            json_data={"adGroups": [body]},
            headers={"Content-Type": self._V3_SP_ADGROUP_CT, "Accept": self._V3_SP_ADGROUP_CT},
        )

    def create_ad_groups(self, ad_groups: list) -> dict:
        """批量创建 SP 广告组"""
        self._stringify_ids(ad_groups)
        return self._request(
            "POST", "/sp/adGroups",
            json_data={"adGroups": ad_groups},
            headers={"Content-Type": self._V3_SP_ADGROUP_CT, "Accept": self._V3_SP_ADGROUP_CT},
        )

    # ==================== 产品广告 (Product Ads) v3 ====================

    _V3_SP_PRODUCT_AD_CT = "application/vnd.spproductad.v3+json"

    def list_product_ads(self, campaign_id: int = None, ad_group_id: int = None,
                         state_filter: list = None, max_results: int = 100,
                         next_token: str = None) -> dict:
        """获取 SP 产品广告列表"""
        body = {"maxResults": max(1, min(max_results, 100))}
        filters = {}
        if campaign_id:
            filters["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if ad_group_id:
            filters["adGroupIdFilter"] = {"include": [str(ad_group_id)]}
        if filters:
            body["filters"] = filters
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/productAds/list", json_data=body,
            headers={"Content-Type": self._V3_SP_PRODUCT_AD_CT, "Accept": self._V3_SP_PRODUCT_AD_CT},
        )

    def update_product_ad(self, ad_id: int, updates: dict) -> dict:
        """更新 SP 产品广告状态"""
        body = {"adId": str(ad_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        return self._request(
            "PUT", "/sp/productAds",
            json_data={"productAds": [body]},
            headers={"Content-Type": self._V3_SP_PRODUCT_AD_CT, "Accept": self._V3_SP_PRODUCT_AD_CT},
        )

    def create_product_ads(self, product_ads: list) -> dict:
        """批量创建 SP 产品广告"""
        self._stringify_ids(product_ads)
        return self._request(
            "POST", "/sp/productAds",
            json_data={"productAds": product_ads},
            headers={"Content-Type": self._V3_SP_PRODUCT_AD_CT, "Accept": self._V3_SP_PRODUCT_AD_CT},
        )

    # ==================== 关键词 (Keywords) v3 ====================

    _V3_SP_KEYWORD_CT = "application/vnd.spkeyword.v3+json"
    _V3_SP_RECOMMENDATION_CT = "application/vnd.sptargetrecommendation.v3+json"

    def list_keywords(self, campaign_id: int = None, ad_group_id: int = None,
                      state_filter: list = None, max_results: int = 100,
                      next_token: str = None) -> dict:
        """获取 SP 关键词列表"""
        body = {"maxResults": max(1, min(max_results, 100))}
        filters = {}
        if campaign_id:
            filters["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if ad_group_id:
            filters["adGroupIdFilter"] = {"include": [str(ad_group_id)]}
        if filters:
            body["filters"] = filters
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/keywords/list", json_data=body,
            headers={"Content-Type": self._V3_SP_KEYWORD_CT, "Accept": self._V3_SP_KEYWORD_CT},
        )

    def update_keyword(self, keyword_id: int, updates: dict) -> dict:
        """更新 SP 关键词 (state / bid)"""
        body = {"keywordId": str(keyword_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        if "bid" in updates:
            body["bid"] = updates["bid"]
        return self._request(
            "PUT", "/sp/keywords",
            json_data={"keywords": [body]},
            headers={"Content-Type": self._V3_SP_KEYWORD_CT, "Accept": self._V3_SP_KEYWORD_CT},
        )

    def create_keywords(self, keywords: list) -> dict:
        """批量创建 SP 关键词"""
        self._stringify_ids(keywords)
        return self._request(
            "POST", "/sp/keywords",
            json_data={"keywords": keywords},
            headers={"Content-Type": self._V3_SP_KEYWORD_CT, "Accept": self._V3_SP_KEYWORD_CT},
        )

    def get_keyword_recommendations(self, payload: dict) -> dict:
        """获取关键词推荐 & 建议竞价"""
        return self._request(
            "POST", "/sp/targets/keywords/recommendations",
            json_data=payload,
        )

    # ==================== 投放 (Targets) v3 ====================

    _V3_SP_TARGET_CT = "application/vnd.sptargetingClause.v3+json"

    def list_targets(self, campaign_id: int = None, ad_group_id: int = None,
                     state_filter: list = None, max_results: int = 100,
                     next_token: str = None) -> dict:
        """获取 SP 投放列表 (手动 + 自动)"""
        body = {"maxResults": max(1, min(max_results, 100))}
        filters = {}
        if campaign_id:
            filters["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if ad_group_id:
            filters["adGroupIdFilter"] = {"include": [str(ad_group_id)]}
        if filters:
            body["filters"] = filters
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/targets/list", json_data=body,
            headers={"Content-Type": self._V3_SP_TARGET_CT, "Accept": self._V3_SP_TARGET_CT},
        )

    def update_target(self, target_id: int, updates: dict) -> dict:
        """更新 SP 投放 (state / bid)"""
        body = {"targetId": str(target_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        if "bid" in updates:
            body["bid"] = updates["bid"]
        return self._request(
            "PUT", "/sp/targets",
            json_data={"targetingClauses": [body]},
            headers={"Content-Type": self._V3_SP_TARGET_CT, "Accept": self._V3_SP_TARGET_CT},
        )

    def create_targets(self, targets: list) -> dict:
        """批量创建 SP 手动投放"""
        self._stringify_ids(targets)
        return self._request(
            "POST", "/sp/targets",
            json_data={"targetingClauses": targets},
            headers={"Content-Type": self._V3_SP_TARGET_CT, "Accept": self._V3_SP_TARGET_CT},
        )

    def get_target_bid_recommendations(self, payload: dict) -> dict:
        """获取投放建议竞价"""
        return self._request(
            "POST", "/sp/targets/bid/recommendations",
            json_data=payload,
        )

    def get_target_product_recommendations(self, payload: dict) -> dict:
        """获取产品投放推荐 (ASIN)"""
        return self._request(
            "POST", "/sp/targets/products/recommendations",
            json_data=payload,
        )

    def get_categories(self, payload: dict) -> dict:
        """获取类目树"""
        return self._request(
            "POST", "/sp/targets/categories",
            json_data=payload,
            headers={"Content-Type": self._V3_SP_TARGET_CT, "Accept": self._V3_SP_TARGET_CT},
        )

    # ==================== 否定关键词 (Negative Keywords) v3 ====================

    _V3_SP_NEG_KEYWORD_CT = "application/vnd.spnegativekeyword.v3+json"

    def list_negative_keywords(self, campaign_id: int = None, ad_group_id: int = None,
                               state_filter: list = None, max_results: int = 100,
                               next_token: str = None) -> dict:
        """获取 SP 否定关键词列表"""
        body = {"maxResults": max(1, min(max_results, 100))}
        filters = {}
        if campaign_id:
            filters["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if ad_group_id is not None:
            filters["adGroupIdFilter"] = {"include": [str(ad_group_id)]}
        if filters:
            body["filters"] = filters
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/negativeKeywords/list", json_data=body,
            headers={"Content-Type": self._V3_SP_NEG_KEYWORD_CT, "Accept": self._V3_SP_NEG_KEYWORD_CT},
        )

    def update_negative_keyword(self, keyword_id: int, updates: dict) -> dict:
        """更新 SP 否定关键词状态"""
        body = {"keywordId": str(keyword_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        return self._request(
            "PUT", "/sp/negativeKeywords",
            json_data={"negativeKeywords": [body]},
            headers={"Content-Type": self._V3_SP_NEG_KEYWORD_CT, "Accept": self._V3_SP_NEG_KEYWORD_CT},
        )

    def create_negative_keywords(self, keywords: list) -> dict:
        """批量创建 SP 否定关键词"""
        self._stringify_ids(keywords)
        return self._request(
            "POST", "/sp/negativeKeywords",
            json_data={"negativeKeywords": keywords},
            headers={"Content-Type": self._V3_SP_NEG_KEYWORD_CT, "Accept": self._V3_SP_NEG_KEYWORD_CT},
        )

    def archive_negative_keyword(self, keyword_id: int) -> dict:
        """归档 SP 否定关键词（直连 DELETE，不送 Content-Type）"""
        url = f"{self.base_url}/sp/negativeKeywords/{keyword_id}"
        h = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": str(self.profile_id),
        }
        resp = requests.delete(url, headers=h, proxies=self.proxies, timeout=60)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text
            print(f"[Ads-API Error] {resp.status_code} {resp.url}")
            print(f"[Ads-API Error Body] {err_body}")
            raise
        if resp.status_code == 204 or not resp.text:
            return {}
        return resp.json()

    # ==================== 否定投放 (Negative Targets) v3 ====================

    _V3_SP_NEG_TARGET_CT = "application/vnd.spnegativeTargetingClause.v3+json"

    def list_negative_targets(self, campaign_id: int = None, ad_group_id: int = None,
                              state_filter: list = None, max_results: int = 100,
                              next_token: str = None) -> dict:
        """获取 SP 否定投放列表"""
        body = {"maxResults": max(1, min(max_results, 100))}
        filters = {}
        if campaign_id:
            filters["campaignIdFilter"] = {"include": [str(campaign_id)]}
        if ad_group_id is not None:
            filters["adGroupIdFilter"] = {"include": [str(ad_group_id)]}
        if filters:
            body["filters"] = filters
        if state_filter:
            body["stateFilter"] = {"include": state_filter}
        if next_token:
            body["nextToken"] = next_token
        return self._request(
            "POST", "/sp/negativeTargets/list", json_data=body,
            headers={"Content-Type": self._V3_SP_NEG_TARGET_CT, "Accept": self._V3_SP_NEG_TARGET_CT},
        )

    def update_negative_target(self, target_id: int, updates: dict) -> dict:
        """更新 SP 否定投放状态"""
        body = {"targetId": str(target_id)}
        if "state" in updates:
            body["state"] = _upper(updates["state"])
        return self._request(
            "PUT", "/sp/negativeTargets",
            json_data={"negativeTargetingClauses": [body]},
            headers={"Content-Type": self._V3_SP_NEG_TARGET_CT, "Accept": self._V3_SP_NEG_TARGET_CT},
        )

    def create_negative_targets(self, targets: list) -> dict:
        """批量创建 SP 否定投放"""
        self._stringify_ids(targets)
        return self._request(
            "POST", "/sp/negativeTargets",
            json_data={"negativeTargetingClauses": targets},
            headers={"Content-Type": self._V3_SP_NEG_TARGET_CT, "Accept": self._V3_SP_NEG_TARGET_CT},
        )

    def archive_negative_target(self, target_id: int) -> dict:
        """归档 SP 否定投放（直连 DELETE，不送 Content-Type）"""
        url = f"{self.base_url}/sp/negativeTargets/{target_id}"
        h = {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Amazon-Advertising-API-Scope": str(self.profile_id),
        }
        resp = requests.delete(url, headers=h, proxies=self.proxies, timeout=60)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text
            print(f"[Ads-API Error] {resp.status_code} {resp.url}")
            print(f"[Ads-API Error Body] {err_body}")
            raise
        if resp.status_code == 204 or not resp.text:
            return {}
        return resp.json()

    # ==================== 广告组合 (Portfolios) ====================

    def list_portfolios(self, state_filter: list = None) -> list:
        """获取广告组合列表"""
        resp = self._request(
            "GET", "/v2/portfolios/extended",
            params={"stateFilter": "enabled"},
        )
        if isinstance(resp, list):
            return resp
        return resp.get("portfolios", []) if isinstance(resp, dict) else []
