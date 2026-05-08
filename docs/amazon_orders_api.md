# Amazon 订单模块 - 前端接口文档

> 基础路径：`/api`
> 
> 所有接口均需要登录（带登录态请求即可）

---

## 一、订单列表查询

### GET `/amazon/orders`

从数据库分页查询已同步的订单列表。

#### 查询参数（Query String）

| 参数名 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `order_status` | string | 否 | 订单状态筛选，如 `Pending`、`Unshipped`、`Shipped`、`Canceled` |
| `amazon_order_id` | string | 否 | 亚马逊订单号，精确匹配 |
| `buyer_name` | string | 否 | 买家姓名，模糊匹配 |
| `purchase_date_from` | string | 否 | 下单开始日期，格式 `YYYY-MM-DD` |
| `purchase_date_to` | string | 否 | 下单结束日期，格式 `YYYY-MM-DD` |
| `page` | int | 否 | 页码，默认 `1` |
| `page_size` | int | 否 | 每页数量，默认 `20`，最大 `500` |

#### 响应示例（成功）

```json
{
  "status": "success",
  "data": {
    "list": [
      {
        "amazon_order_id": "902-3159896-1390916",
        "marketplace_id": "ATVPDKIKX0DER",
        "purchase_date": "2017-01-20 19:49:35",
        "last_update_date": "2017-01-20 19:49:35",
        "order_status": "Pending",
        "fulfillment_channel": "MFN",
        "number_of_items_shipped": 0,
        "number_of_items_unshipped": 0,
        "order_total_currency_code": "USD",
        "order_total_amount": "99.99",
        "payment_method": "Other",
        "shipment_service_level_category": "Standard",
        "order_type": "StandardOrder",
        "shipping_name": "Michigan address",
        "shipping_city": "Canton",
        "shipping_state_or_region": "MI",
        "shipping_country_code": "US",
        "buyer_name": "John Doe",
        "buyer_email": "user@example.com",
        "is_business_order": 0,
        "is_prime": 0,
        "sync_time": "2026-05-08 14:00:00",
        "item_count": 2
      }
    ],
    "total": 156,
    "page": 1,
    "page_size": 20
  }
}
```

#### 响应示例（失败）

```json
{
  "status": "error",
  "message": "查询异常信息..."
}
```

---

## 二、订单详情查询

### GET `/amazon/orders/{order_id}`

查询单个订单详情，**自动附带该订单的商品列表**，并左关联本地产品表。

#### 路径参数

| 参数名 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `order_id` | string | 是 | 亚马逊订单号，如 `902-3159896-1390916` |

#### 响应示例（成功）

```json
{
  "status": "success",
  "data": {
    "amazon_order_id": "902-3159896-1390916",
    "marketplace_id": "ATVPDKIKX0DER",
    "purchase_date": "2017-01-20 19:49:35",
    "last_update_date": "2017-01-20 19:49:35",
    "order_status": "Pending",
    "fulfillment_channel": "MFN",
    "number_of_items_shipped": 0,
    "number_of_items_unshipped": 0,
    "payment_method": "Other",
    "payment_method_details": "[\"CreditCard\",\"GiftCertificate\"]",
    "order_total_currency_code": null,
    "order_total_amount": null,
    "shipment_service_level_category": "Standard",
    "order_type": "StandardOrder",
    "earliest_ship_date": "2017-01-20 19:51:16",
    "latest_ship_date": "2017-01-25 19:49:35",
    "shipping_name": "Michigan address",
    "shipping_address_line1": "1 Cross St.",
    "shipping_city": "Canton",
    "shipping_state_or_region": "MI",
    "shipping_postal_code": "48817",
    "shipping_country_code": "US",
    "buyer_email": "user@example.com",
    "buyer_name": "John Doe",
    "buyer_tax_company_legal_name": "A Company Name",
    "purchase_order_number": "1234567890123",
    "is_business_order": 0,
    "is_prime": 0,
    "is_access_point_order": 0,
    "is_global_express_enabled": 0,
    "is_premium_order": 0,
    "is_sold_by_ab": 0,
    "is_iba": 0,
    "sync_time": "2026-05-08 14:00:00",
    "items_sync_time": "2026-05-08 14:05:00",
    "items": [
      {
        "id": 1,
        "amazon_order_id": "902-3159896-1390916",
        "order_item_id": "68828574383266",
        "asin": "BT0093TELA",
        "seller_sku": "CBA_OTF_1",
        "title": "Example item name",
        "quantity_ordered": 1,
        "quantity_shipped": 1,
        "item_price_currency_code": "JPY",
        "item_price_amount": "25.99",
        "shipping_price_currency_code": "JPY",
        "shipping_price_amount": "1.26",
        "price_designation": "BusinessPrice",
        "condition_id": null,
        "condition_subtype_id": null,
        "buyer_requested_cancel": 1,
        "buyer_cancel_reason": "Found cheaper somewhere else.",
        "promotion_ids": "[\"FREESHIP\"]",
        "serial_numbers": "[\"854\"]",
        "points_granted": "{\"PointsNumber\":10,...}",
        "sync_time": "2026-05-08 14:05:00",
        "local_product_name": "本地产品名称",
        "declare_name_cn": "中文申报名",
        "declare_name_en": "English Declare Name",
        "local_image_url": "/static/products/xxx.jpg"
      }
    ]
  }
}
```

#### 响应示例（订单不存在）

```json
{
  "status": "error",
  "message": "订单不存在"
}
```

---

## 三、手动同步订单列表

### POST `/amazon/sync/orders`

从 Amazon SP-API 拉取订单列表并写入数据库（**不自动拉商品**）。

#### 请求体（JSON，可选）

| 参数名 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `created_after` | string | 否 | 创建开始时间，ISO8601，默认30天前 |
| `created_before` | string | 否 | 创建结束时间，ISO8601 |
| `order_statuses` | array | 否 | 状态列表，如 `["Unshipped", "Shipped"]` |
| `marketplace_ids` | array | 否 | 市场ID列表，默认取环境变量配置 |

#### 请求示例

```json
{
  "created_after": "2026-04-01T00:00:00Z",
  "order_statuses": ["Unshipped", "Shipped"]
}
```

#### 响应示例

```json
{
  "status": "success",
  "message": "同步完成，共处理 42 条订单",
  "data": {
    "synced_count": 42,
    "total_fetched": 42,
    "error": null,
    "next_token": null
  }
}
```

---

## 四、手动同步订单商品

### POST `/amazon/sync/orders/{order_id}/items`

从 Amazon SP-API 拉取指定订单的商品列表并写入数据库。

#### 路径参数

| 参数名 | 类型 | 必填 | 说明 |
|:---|:---|:---|:---|
| `order_id` | string | 是 | 亚马逊订单号 |

#### 响应示例

```json
{
  "status": "success",
  "message": "同步完成，共处理 3 条商品",
  "data": {
    "synced_count": 3,
    "total_fetched": 3,
    "error": null
  }
}
```

---

## 五、一键全量同步（订单 + 商品）

### POST `/amazon/sync/orders-all`

先同步订单列表，再逐个同步所有相关订单的商品明细。耗时较长，请做好前端 loading。

#### 请求体（JSON，可选）

与 **接口三** 参数相同，不传则默认同步最近30天的订单。

#### 请求示例

```json
{
  "created_after": "2026-04-01T00:00:00Z"
}
```

#### 响应示例

```json
{
  "status": "success",
  "message": "订单全量同步完成",
  "data": {
    "orders": {
      "synced_count": 42,
      "total_fetched": 42,
      "error": null
    },
    "items_synced": 86,
    "items_errors": []
  }
}
```

---

## 六、数据字典

### 订单状态（order_status）

| 状态值 | 说明 |
|:---|:---|
| `Pending` | 待处理 |
| `Unshipped` | 未发货 |
| `PartiallyShipped` | 部分发货 |
| `Shipped` | 已发货 |
| `InvoiceUnconfirmed` | 发票未确认 |
| `Canceled` | 已取消 |
| `Unfulfillable` | 无法配送 |

### 配送渠道（fulfillment_channel）

| 值 | 说明 |
|:---|:---|
| `MFN` | 卖家自配送 |
| `AFN` | 亚马逊配送（FBA） |

### 配送服务类别（shipment_service_level_category）

| 值 | 说明 |
|:---|:---|
| `Standard` | 标准配送 |
| `Expedited` | 加急配送 |
| `NextDay` | 次日达 |
| `SecondDay` | 隔日达 |
| `Scheduled` | 预约配送 |

### 布尔字段说明

订单表中所有 `is_` 开头字段在数据库中均为 `TINYINT(1)`，返回给前端时值为：
- `1` — 是
- `0` — 否

---

## 七、定时同步策略（后端已配置，前端无需关心）

| 任务 | 频率 | 范围 | 行为 |
|:---|:---|:---|:---|
| 近期订单同步 | 每15分钟 | 最近24h有更新 | 列表 + 商品全量 |
| 本周订单同步 | 每3小时 | 最近7天有更新 | 列表 + 商品（去重24h内） |
| 本月订单同步 | 每6小时 | 最近30天有更新 | 仅列表，不抓商品 |

前端页面展示时，**优先读数据库接口**（接口一、二），同步按钮可调用接口三、四、五做手动刷新。
