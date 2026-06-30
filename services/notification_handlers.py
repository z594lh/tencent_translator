"""
通知处理器：负责将业务事件转换为企业微信消息
所有 @on 装饰器在模块导入时自动注册到 dispatcher
"""
from datetime import datetime, timedelta, timezone
from services.notification_dispatcher import on
from services.wecom_notification import send_markdown
from services.shop_service import get_shop_by_id
from services.mysql_service import get_db_connection


def _shop_label(shop_id: int) -> str:
    shop = get_shop_by_id(shop_id)
    return shop['shop_name'] if shop else f"店铺#{shop_id}"


def _to_pdt(iso_str: str) -> str:
    """将 UTC ISO 时间字符串转为太平洋夏令时 (PDT) 格式显示"""
    if not iso_str:
        return '-'
    try:
        s = iso_str.replace('Z', '+00:00')
        utc_dt = datetime.fromisoformat(s)
        pdt_dt = utc_dt.astimezone(timezone(timedelta(hours=-7)))
        return pdt_dt.strftime('%Y-%m-%d %H:%M:%S PDT')
    except (ValueError, TypeError):
        return iso_str


def _get_product_cn_name(cursor, seller_sku: str) -> str:
    """根据 seller_sku 查询 products 表获取中文 product_name"""
    cursor.execute(
        "SELECT product_name FROM products WHERE seller_sku = %s LIMIT 1",
        (seller_sku,)
    )
    row = cursor.fetchone()
    return (row['product_name'] or '').strip() if row else ''


# ==================== 库存通知 ====================

@on('inventory_stock_changed')
def on_inventory_stock_changed(shop_id: int, sku: str, asin: str,
                                product_name: str, old_qty: int, new_qty: int):
    shop = _shop_label(shop_id)

    cn_name = ''
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cn_name = _get_product_cn_name(cursor, sku)
    except Exception:
        pass
    finally:
        conn.close()

    if cn_name:
        name = f"{cn_name}-{product_name}"
    else:
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

    pdt_time = _to_pdt(purchase_date)

    sku_lines = ''
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT oi.seller_sku, COALESCE(oi.quantity_shipped, oi.quantity_ordered, 1) AS qty
                FROM amazon_order_items oi
                WHERE oi.amazon_order_id = %s AND oi.shop_id = %s
            """, (order_id, shop_id))
            items = cursor.fetchall()
            if items:
                sku_parts = []
                for item in items:
                    sku = item['seller_sku'] or ''
                    qty = int(item['qty'] or 1)
                    cn = _get_product_cn_name(cursor, sku)
                    if cn:
                        sku_parts.append(f"{sku}-{cn}*{qty}")
                    else:
                        sku_parts.append(f"{sku}*{qty}")
                sku_lines = ', '.join(sku_parts)
    except Exception:
        pass
    finally:
        conn.close()

    msg = (
        f"## \U0001f4e6 新订单\n"
        f"> 店铺：**{shop}**\n"
        f"> 订单号：`{order_id}`\n"
        f"> 状态：{order_status}\n"
        f"> 买家：{buyer_name or '-'}\n"
        f"> 时间：{pdt_time}\n"
        f"> 商品数：{item_count} 件"
    )
    if sku_lines:
        msg += f"\n> SKU 明细：{sku_lines}"

    send_markdown(msg)


@on('order_cancelled')
def on_order_cancelled(shop_id: int, order_id: str, buyer_name: str):
    shop = _shop_label(shop_id)
    send_markdown(
        f"## \u274c 订单取消\n"
        f"> 店铺：**{shop}**\n"
        f"> 订单号：`{order_id}`\n"
        f"> 买家：{buyer_name or '-'}"
    )
