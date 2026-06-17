"""
通知处理器：负责将业务事件转换为企业微信消息
所有 @on 装饰器在模块导入时自动注册到 dispatcher
"""
from services.notification_dispatcher import on
from services.wecom_notification import send_markdown
from services.shop_service import get_shop_by_id


def _shop_label(shop_id: int) -> str:
    shop = get_shop_by_id(shop_id)
    return shop['shop_name'] if shop else f"店铺#{shop_id}"


# ==================== 库存通知 ====================

@on('inventory_stock_changed')
def on_inventory_stock_changed(shop_id: int, sku: str, asin: str,
                                product_name: str, old_qty: int, new_qty: int):
    shop = _shop_label(shop_id)
    name = product_name or sku

    if old_qty == 0 and new_qty > 0:
        send_markdown(
            f"## \U0001f7e2 库存恢复\n"
            f"> 店铺：**{shop}**\n"
            f"> SKU：`{sku}`\n"
            f"> ASIN：`{asin}`\n"
            f"> 商品：{name}\n"
            f"> 可售库存：0 → **{new_qty}**"
        )
    elif old_qty > 0 and new_qty == 0:
        send_markdown(
            f"## \U0001f534 库存断货\n"
            f"> 店铺：**{shop}**\n"
            f"> SKU：`{sku}`\n"
            f"> ASIN：`{asin}`\n"
            f"> 商品：{name}\n"
            f"> 可售库存：{old_qty} → **0**"
        )


# ==================== 订单通知 ====================

@on('order_new')
def on_order_new(shop_id: int, order_id: str, order_status: str,
                 buyer_name: str, purchase_date: str, item_count: int):
    shop = _shop_label(shop_id)
    send_markdown(
        f"## \U0001f4e6 新订单\n"
        f"> 店铺：**{shop}**\n"
        f"> 订单号：`{order_id}`\n"
        f"> 状态：{order_status}\n"
        f"> 买家：{buyer_name or '-'}\n"
        f"> 时间：{purchase_date}\n"
        f"> 商品数：{item_count} 件"
    )


@on('order_cancelled')
def on_order_cancelled(shop_id: int, order_id: str, buyer_name: str):
    shop = _shop_label(shop_id)
    send_markdown(
        f"## \u274c 订单取消\n"
        f"> 店铺：**{shop}**\n"
        f"> 订单号：`{order_id}`\n"
        f"> 买家：{buyer_name or '-'}"
    )
