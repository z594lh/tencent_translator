"""
Amazon SP-API 数据同步服务
提供从 API 获取数据并同步到 MySQL 数据库的封装
支持增量同步和全量同步
"""
import os
import time
from datetime import datetime, timedelta

from services.amazon_sp_client import AmazonSpApiClient
from services.mysql_service import (
    sync_inventory_summaries,
    sync_shipments,
    sync_shipment_items,
    get_inventory_summaries_from_db,
    get_shipments_from_db,
    get_shipment_items_from_db,
    get_shipment_items_by_shipment_ids,
    sync_fba_warehouses,
    get_fba_warehouses as get_fba_warehouses_from_db,
)


class AmazonDbSyncService:
    """
    Amazon 数据同步服务
    将 SP-API 接口数据同步到本地 MySQL，支持前端分页查询
    """

    def __init__(self, marketplace_id=None, region=None):
        self.client = AmazonSpApiClient(
            marketplace_id=marketplace_id,
            region=region
        )
        self.marketplace_id = marketplace_id or os.getenv("AMAZON_MARKETPLACE_ID", "ATVPDKIKX0DER")

    # ==================== 库存同步 ====================

    def sync_inventory(self, seller_skus=None, start_date_time=None, details=True):
        """
        同步库存汇总数据（自动处理分页）
        
        Args:
            seller_skus: SKU列表，可选
            start_date_time: 开始时间，可选
            details: 是否获取详细信息
        
        Returns:
            dict: {synced_count, error, next_token}
        """
        all_items = []
        next_token = None
        page = 0

        try:
            while True:
                page += 1
                print(f"[Inventory Sync] 正在获取第 {page} 页...")

                result = self.client.get_inventory_summaries(
                    seller_skus=seller_skus,
                    details=details,
                    start_date_time=start_date_time,
                    next_token=next_token
                )

                payload = result.get('payload', {})
                items = payload.get('inventorySummaries', [])
                all_items.extend(items)

                next_token = payload.get('nextToken')
                if not next_token:
                    break

                # 避免请求过快
                time.sleep(0.5)

            # 同步到数据库
            synced_count, error = sync_inventory_summaries(self.marketplace_id, all_items)

            return {
                "synced_count": synced_count,
                "total_fetched": len(all_items),
                "error": error,
                "next_token": None
            }

        except Exception as e:
            return {
                "synced_count": 0,
                "total_fetched": len(all_items),
                "error": str(e),
                "next_token": next_token
            }

    def get_inventory(self, seller_sku=None, asin=None, page=1, page_size=20):
        """
        从数据库查询库存数据（支持分页）
        
        Returns:
            dict: {list, total, page, page_size}
        """
        return get_inventory_summaries_from_db(
            marketplace_id=self.marketplace_id,
            seller_sku=seller_sku,
            asin=asin,
            page=page,
            page_size=page_size
        )

    # ==================== 货件同步 ====================

    def sync_all_shipments(
        self,
        shipment_status_list=None,
        last_update_after=None,
        last_update_before=None
    ):
        """
        同步货件列表数据（自动处理分页）
        
        Args:
            shipment_status_list: 货件状态列表，如 ['WORKING', 'SHIPPED']
            last_update_after: 最后更新时间开始
            last_update_before: 最后更新时间结束
        
        Returns:
            dict: {synced_count, error}
        """
        all_shipments = []
        seen_shipment_ids = set()
        next_token = None
        page = 0
        max_pages = 50  # 安全上限

        # 根据参数自动选择 QueryType：
        if last_update_after or last_update_before:
            query_type = "DATE_RANGE"
            if not last_update_before:
                last_update_before = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif shipment_status_list:
            query_type = "SHIPMENT"
        else:
            query_type = "SHIPMENT"
            shipment_status_list = ['WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED', 'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN']

        try:
            while True:
                page += 1
                if page > max_pages:
                    print(f"[Shipments Sync] 达到最大分页限制 {max_pages}，停止获取")
                    break

                print(f"[Shipments Sync] 正在获取第 {page} 页...")

                result = self.client.get_shipments(
                    shipment_status_list=shipment_status_list,
                    last_update_after=last_update_after,
                    last_update_before=last_update_before,
                    query_type=query_type,
                    next_token=next_token
                )

                payload = result.get('payload', {})
                shipments = payload.get('ShipmentData', [])
                page_ids = [s.get('ShipmentId') for s in shipments]
                print(f"[Shipments Sync] 第 {page} 页 ShipmentIds: {page_ids}")

                # 去重：只保留新的 ShipmentId
                new_shipments = []
                for s in shipments:
                    sid = s.get('ShipmentId')
                    if sid and sid not in seen_shipment_ids:
                        seen_shipment_ids.add(sid)
                        new_shipments.append(s)

                all_shipments.extend(new_shipments)
                print(f"[Shipments Sync] 第 {page} 页原始 {len(shipments)} 条，新增 {len(new_shipments)} 条，累计不重复 {len(all_shipments)} 条")

                # 如果这页有数据但全是重复的，说明 API 在循环返回同一批数据，直接退出
                if len(shipments) > 0 and len(new_shipments) == 0:
                    print(f"[Shipments Sync] 检测到重复数据，终止分页")
                    break

                next_token = payload.get('NextToken')
                print(f"[Shipments Sync] NextToken: {'有' if next_token else '无'} ({str(next_token)[:30]}..." if next_token else "[Shipments Sync] NextToken: 无")
                if not next_token:
                    print(f"[Shipments Sync] 分页结束，共 {page} 页")
                    break

                time.sleep(0.5)

        except Exception as e:
            print(f"[Shipments Sync] 异常: {e}")
            return {
                "synced_count": 0,
                "total_fetched": len(all_shipments),
                "error": str(e)
            }

        # 同步货件数据到数据库
        synced_count, error = sync_shipments(self.marketplace_id, all_shipments)

        # 顺便同步仓库列表（提取货件中的 destination_fulfillment_center_id）
        warehouse_ids = list({
            s.get('DestinationFulfillmentCenterId')
            for s in all_shipments
            if s.get('DestinationFulfillmentCenterId')
        })
        if warehouse_ids:
            w_count, w_error = sync_fba_warehouses(self.marketplace_id, warehouse_ids)
            print(f"[Shipments Sync] 同步仓库 {len(warehouse_ids)} 个，成功 {w_count} 个，错误: {w_error}")

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_shipments),
            "error": error
        }

    def get_shipments(
        self,
        shipment_status=None,
        destination_fc=None,
        page=1,
        page_size=20
    ):
        """
        从数据库查询货件列表（支持分页）
        
        Returns:
            dict: {list, total, page, page_size}
        """
        return get_shipments_from_db(
            marketplace_id=self.marketplace_id,
            shipment_status=shipment_status,
            destination_fc=destination_fc,
            page=page,
            page_size=page_size
        )

    def get_fba_warehouses(self):
        """
        从数据库查询 FBA 仓库列表（用于前端下拉筛选）
        
        Returns:
            list: 仓库数据列表
        """
        return get_fba_warehouses_from_db(marketplace_id=self.marketplace_id)

    # ==================== 货件商品同步 ====================

    def sync_shipment_items_by_id(self, shipment_id):
        """
        同步指定货件的商品数据
        
        Args:
            shipment_id: 货件ID
        
        Returns:
            dict: {synced_count, error}
        """
        try:
            result = self.client.get_shipment_items(shipment_id)
            payload = result.get('payload', {})
            items = payload.get('ItemData', [])

            synced_count, error = sync_shipment_items(shipment_id, items)

            return {
                "synced_count": synced_count,
                "total_fetched": len(items),
                "error": error
            }

        except Exception as e:
            return {
                "synced_count": 0,
                "total_fetched": 0,
                "error": str(e)
            }

    def sync_all_shipment_items(self, shipment_ids):
        """
        批量同步多个货件的商品数据
        
        Args:
            shipment_ids: 货件ID列表
        
        Returns:
            dict: {total_synced, errors}
        """
        total_synced = 0
        errors = []

        for shipment_id in shipment_ids:
            result = self.sync_shipment_items_by_id(shipment_id)
            total_synced += result.get('synced_count', 0)
            if result.get('error'):
                errors.append({"shipment_id": shipment_id, "error": result['error']})
            time.sleep(0.3)

        return {
            "total_synced": total_synced,
            "errors": errors
        }

    def get_shipment_items(
        self,
        shipment_id=None,
        seller_sku=None,
        page=1,
        page_size=20
    ):
        """
        从数据库查询货件商品（支持分页）
        
        Returns:
            dict: {list, total, page, page_size}
        """
        return get_shipment_items_from_db(
            shipment_id=shipment_id,
            seller_sku=seller_sku,
            page=page,
            page_size=page_size
        )

    def get_shipment_items_for_shipments(self, shipment_ids):
        """
        根据货件ID列表批量查询商品（不分页，返回全部）
        
        Returns:
            list: 商品数据列表
        """
        return get_shipment_items_by_shipment_ids(shipment_ids)

    # ==================== 一键同步 ====================

    def sync_all(self, sync_inventory=True, sync_shipments=True, sync_items=True):
        """
        一键同步所有数据
        
        Args:
            sync_inventory: 是否同步库存
            sync_shipments: 是否同步货件
            sync_items: 是否同步货件商品（需要先同步货件获取 shipment_ids）
        
        Returns:
            dict: 各模块同步结果
        """
        results = {}

        if sync_inventory:
            print("=" * 50)
            print("开始同步库存数据...")
            results['inventory'] = self.sync_inventory()
            print(f"库存同步完成: {results['inventory']}")

        if sync_shipments:
            print("=" * 50)
            print("开始同步货件数据...")
            # 默认同步最近30天的货件（DATE_RANGE 模式 + 全状态列表）
            last_update_after = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            default_statuses = ['WORKING', 'SHIPPED', 'RECEIVING', 'CANCELLED', 'DELETED', 'CLOSED', 'ERROR', 'IN_TRANSIT', 'DELIVERED', 'CHECKED_IN']
            results['shipments'] = self.sync_all_shipments(
                shipment_status_list=default_statuses,
                last_update_after=last_update_after
            )
            print(f"货件同步完成: {results['shipments']}")

        if sync_items and results.get('shipments', {}).get('synced_count', 0) > 0:
            print("=" * 50)
            print("开始同步货件商品数据...")
            # 获取刚同步的货件ID列表
            shipment_list = self.get_shipments(page=1, page_size=1000)
            shipment_ids = [s['shipment_id'] for s in shipment_list.get('list', [])]
            results['shipment_items'] = self.sync_all_shipment_items(shipment_ids)
            print(f"货件商品同步完成: {results['shipment_items']}")

        return results


# ==================== 快捷函数 ====================

def sync_amazon_inventory(**kwargs):
    """快捷同步库存数据"""
    service = AmazonDbSyncService()
    return service.sync_inventory(**kwargs)


def sync_amazon_shipments(**kwargs):
    """快捷同步货件数据"""
    service = AmazonDbSyncService()
    return service.sync_all_shipments(**kwargs)


def sync_amazon_shipment_items(shipment_id):
    """快捷同步指定货件商品"""
    service = AmazonDbSyncService()
    return service.sync_shipment_items_by_id(shipment_id)


def get_amazon_inventory(**kwargs):
    """快捷查询库存数据（从数据库）"""
    service = AmazonDbSyncService()
    return service.get_inventory(**kwargs)


def get_amazon_shipments(**kwargs):
    """快捷查询货件数据（从数据库）"""
    service = AmazonDbSyncService()
    return service.get_shipments(**kwargs)


def get_amazon_shipment_items(**kwargs):
    """快捷查询货件商品（从数据库）"""
    service = AmazonDbSyncService()
    return service.get_shipment_items(**kwargs)
