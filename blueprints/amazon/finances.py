"""
Amazon 订单财务明细模块

简介: 对接 Amazon SP-API Finances (v2024-06-19) 接口，为订单自动抓取费用明细并入库。

前端接口:
  - GET  /api/amazon/orders/<order_id>/finances       查询订单财务明细
  - GET  /api/amazon/finances                          分页查询财务记录列表
  - POST /api/amazon/finances/sync                     触发财务数据同步

详细:
  1. 数据来源: SP-API /finances/2024-06-19/transactions
  2. 通过 relatedIdentifier=ORDER_ID=xxx 精确匹配订单
  3. transaction 包含: 金额、交易类型、过账日期、item 级费用拆分（Product Charges 等）
  4. 支持单品项/订单级别多种费用类型的 breakdown
  5. 定时任务 finances-recent 每 30 分钟自动同步最近 2 天内有更新的订单
"""
import time
import json

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_sp_api_client, get_all_active_shops
from services.mysql_service import get_db_connection

amazon_finances_bp = Blueprint('amazon_finances', __name__, url_prefix='/api')


# ============================================================
# 前端接口
# ============================================================

@amazon_finances_bp.route('/amazon/orders/<order_id>/finances', methods=['GET'])
@login_required
@permission_required('amazon_finances:view')
def get_order_finances(order_id):
    """
    查询订单财务明细

    简介: 根据订单号查询该订单在 Amazon 上的所有财务交易（付款、退款等）。

    详细:
      - 从本地数据库 amazon_order_finances 读取已同步的数据
      - items_json 和 breakdowns_json 字段含完整的费用拆分明细

    查询参数:
        shop_id (必填) 店铺ID

    返回:
        { status, data: [ { transaction_id, transaction_type, total_amount, items_json, ... }, ... ] }
    """
    try:
        shop_id = _require_shop_id()
        records = _get_order_finances_from_db(shop_id, order_id)
        return jsonify({"status": "success", "data": records})
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Finances] 查询订单财务异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_finances_bp.route('/amazon/finances', methods=['GET'])
@login_required
@permission_required('amazon_finances:view')
def list_finances():
    """
    分页查询财务记录列表

    简介: 跨订单查询已同步的财务交易记录，支持按交易类型过滤。

    查询参数:
        shop_id          (必填) 店铺ID
        amazon_order_id  (可选) 按订单号过滤
        transaction_type (可选) 按交易类型过滤
        page             (可选) 页码，默认 1
        page_size        (可选) 每页条数，默认 20，最大 100

    返回:
        { status, data: { list, total, page, page_size } }
    """
    try:
        shop_id = _require_shop_id()
        amazon_order_id = request.args.get('amazon_order_id', '').strip() or None
        transaction_type = request.args.get('transaction_type', '').strip() or None
        page = max(1, int(request.args.get('page', 1)))
        page_size = max(1, min(100, int(request.args.get('page_size', 20))))

        result = _list_finances_from_db(
            shop_id=shop_id,
            amazon_order_id=amazon_order_id,
            transaction_type=transaction_type,
            page=page,
            page_size=page_size,
        )
        return jsonify({"status": "success", "data": result})
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Finances] 查询财务列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_finances_bp.route('/amazon/finances/sync', methods=['POST'])
@login_required
@permission_required('amazon_finances:sync')
def trigger_finances_sync():
    """
    触发财务数据同步

    简介: 按结算窗口或订单号范围同步 Amazon 财务交易数据。

    详细:
      - 默认同步 3~7 天前更新的订单（已结算窗口，跳过 T+2 未结算期）
      - 支持按指定订单ID列表精确定向同步

    请求体 (JSON):
        shop_id         (必填) 店铺ID
        days_to         (可选) 结算窗口上限，默认 3（即 3 天前的订单）
        days_from       (可选) 结算窗口下限，默认 7（即 7 天前的订单）
        order_ids       (可选) 指定订单ID列表，优先级高于日期范围
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        days_to = data.get('days_to')
        if days_to is not None:
            try:
                days_to = int(days_to)
            except ValueError:
                return jsonify({"status": "error", "message": "days_to 必须是整数"}), 400
        else:
            days_to = 3

        days_from = data.get('days_from')
        if days_from is not None:
            try:
                days_from = int(days_from)
            except ValueError:
                return jsonify({"status": "error", "message": "days_from 必须是整数"}), 400
        else:
            days_from = 7

        order_ids = data.get('order_ids')
        if order_ids is not None and isinstance(order_ids, list):
            order_ids = [str(oid).strip() for oid in order_ids if oid]
            if not order_ids:
                return jsonify({"status": "error", "message": "order_ids 不能为空列表"}), 400

        result = _sync_finances_for_shop(shop_id=shop_id, days_to=days_to, days_from=days_from, order_ids=order_ids)
        return jsonify({"status": "success", "message": "同步完成", "data": result})

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Finances] 同步异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 辅助函数 — 参数校验
# ============================================================

def _require_shop_id() -> int:
    """
    从请求参数中提取并校验 shop_id

    简介: 校验 shop_id 存在且为整数，否则抛出 ValueError。
    """
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


def _require_shop_id_from_body(data: dict) -> int:
    """
    从请求体中提取并校验 shop_id
    """
    shop_id = data.get('shop_id')
    if shop_id is None or shop_id == '':
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except (ValueError, TypeError):
        raise ValueError("shop_id 必须是整数")


# ============================================================
# 辅助函数 — 数据查询（SP-API）
# ============================================================

def _fetch_transactions_from_api(shop_id, order_id):
    """
    从 SP-API 获取单个订单的财务交易列表

    简介: 通过 relatedIdentifier=ORDER_ID=xxx 精确定向查询。

    详细:
      - 自动处理分页（nextToken）
      - 返回完整的 transaction 列表（含 items、breakdowns、contexts）

    返回:
        list of dict: 该订单的所有财务交易（可能含多笔—付款+退款等）
    """
    client = get_sp_api_client(shop_id)
    transactions = []
    next_token = None

    from datetime import datetime, timedelta
    posted_after = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        resp = client.list_financial_transactions(
            posted_after=posted_after,
            marketplace_id=client.marketplace_id,
            related_identifier=f"ORDER_ID={order_id}",
            next_token=next_token,
        )
        txn_list = resp.get("payload", {}).get("transactions", []) if resp else []
        transactions.extend(txn_list)
        next_token = resp.get("payload", {}).get("nextToken") if resp else None
        if not next_token:
            break
        time.sleep(0.2)

    return transactions


def _fetch_transactions_by_date_range(shop_id, posted_after, posted_before):
    """
    按时间范围从 SP-API 获取财务交易

    简介: 批量拉取指定时间段内所有类型的财务交易，用于定期同步。

    返回:
        list of dict: 该时间范围内的所有交易
    """
    client = get_sp_api_client(shop_id)
    transactions = []
    next_token = None

    while True:
        resp = client.list_financial_transactions(
            posted_after=posted_after,
            posted_before=posted_before,
            marketplace_id=client.marketplace_id,
            next_token=next_token,
        )
        txn_list = resp.get("payload", {}).get("transactions", []) if resp else []
        transactions.extend(txn_list)
        next_token = resp.get("payload", {}).get("nextToken") if resp else None
        if not next_token:
            break
        time.sleep(0.2)

    return transactions


# ============================================================
# 辅助函数 — 费用解析
# ============================================================

def _extract_fees_from_items(items):
    """从 items_json 数组的 breakdowns 中提取产品售价、FBA费、佣金

    返回: (product_charges, fba_fees, commission) 均为正数
    """
    pc = 0.0   # ProductCharges (售价, 正数)
    fb = 0.0   # FBA fees (正数=费用)
    cm = 0.0   # Commission (正数=费用)

    for item in (items or []):
        for bd in item.get("breakdowns", []) or []:
            bt = bd.get("breakdownType", "")
            if bt == "ProductCharges":
                subs = bd.get("breakdowns", []) or []
                if subs:
                    for sub in subs:
                        amt = float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
                        if amt > 0:
                            pc += amt
                else:
                    amt = float((bd.get("breakdownAmount") or {}).get("currencyAmount", 0))
                    if amt > 0:
                        pc += amt
            elif bt == "AmazonFees":
                for sub in bd.get("breakdowns", []) or []:
                    amt = float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
                    if amt < 0:
                        if sub.get("breakdownType", "").startswith("FBAPer"):
                            fb += abs(amt)
                        elif sub.get("breakdownType", "") == "Commission":
                            cm += abs(amt)

    return round(pc, 2), round(fb, 2), round(cm, 2)


# ============================================================
# 辅助函数 — 回写实际费率
# ============================================================

def _update_product_real_fees(shop_id, items):
    """从 Shipment 的 items_json 解析每 SKU 的实际 FBA 费 + 佣金率，回写 amazon_product_fees"""
    for item in (items or []):
        # 找 SKU + 数量
        sku = ""
        qty = 1
        for ctx in (item.get("contexts", []) or []):
            if ctx.get("contextType") == "ProductContext":
                sku = ctx.get("sku", "") or ""
                qty = max(1, int(ctx.get("quantityShipped", 1) or 1))
                break
        if not sku:
            continue

        # 解析 item-level breakdowns (按件均摊) 取实际费用
        fba = 0.0
        comm = 0.0
        price = 0.0
        for bd in (item.get("breakdowns", []) or []):
            bt = bd.get("breakdownType", "")
            if bt == "ProductCharges":
                subs = bd.get("breakdowns", []) or []
                if subs:
                    for sub in subs:
                        price += float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
                else:
                    price += float((bd.get("breakdownAmount") or {}).get("currencyAmount", 0))
            elif bt == "AmazonFees":
                for sub in (bd.get("breakdowns", []) or []):
                    amt = float((sub.get("breakdownAmount") or {}).get("currencyAmount", 0))
                    if amt < 0:
                        st = sub.get("breakdownType", "")
                        if st.startswith("FBAPer"):
                            fba += abs(amt)
                        elif st == "Commission":
                            comm += abs(amt)

        if fba <= 0 and comm <= 0:
            continue

        # 按数量均摊到单件
        fba_per_unit = round(fba / qty, 2)
        rate = round(comm / price, 4) if price > 0 else None

        conn = get_db_connection()
        try:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE amazon_product_fees
                    SET real_fba_fee = %s, real_commission_rate = %s, updated_at = NOW()
                    WHERE shop_id = %s AND sku = %s
                """, (fba_per_unit, rate, shop_id, sku))
                if c.rowcount == 0:
                    c.execute("""
                        INSERT INTO amazon_product_fees
                            (shop_id, sku, asin, commission_rate, fba_fee, real_fba_fee, real_commission_rate, currency)
                        VALUES (%s, %s, '', 0.15, 0, %s, %s, 'USD')
                    """, (shop_id, sku, fba_per_unit, rate))
            conn.commit()
        finally:
            conn.close()


# ============================================================
# 辅助函数 — 数据库操作
# ============================================================

def _save_transactions_to_db(shop_id, order_id, transactions):
    """
    将财务交易数据写入数据库

    简介: 批量 UPSERT 交易记录，按 transaction_id 去重。

    详细:
      - INSERT ... ON DUPLICATE KEY UPDATE 确保幂等
      - items_json / breakdowns_json / raw_json 保持完整 API 响应结构
      - 从 response 中提取 ORDER_ID 关联的订单号
    """
    if not transactions:
        return 0

    conn = get_db_connection()
    saved = 0
    try:
        with conn.cursor() as cursor:
            for txn in transactions:
                transaction_id = txn.get("transactionId", "")
                if not transaction_id:
                    continue

                posted_date = _iso_to_datetime(txn.get("postedDate"))
                total_amt = txn.get("totalAmount", {}) or {}
                marketplace_details = txn.get("marketplaceDetails", {}) or {}
                marketplace_id = marketplace_details.get("marketplaceId", "")

                items = txn.get("items", []) or []
                items_json = json.dumps(items, ensure_ascii=False)
                breakdowns = txn.get("breakdowns", []) or []
                breakdowns_json = json.dumps(breakdowns, ensure_ascii=False)
                raw_json = json.dumps(txn, ensure_ascii=False)

                # 解析费用明细
                product_charges, fba_fees, commission = _extract_fees_from_items(items)

                # 从 relatedIdentifiers 中提取真实的 ORDER_ID
                effective_order_id = ""
                for ri in txn.get("relatedIdentifiers", []):
                    if ri.get("relatedIdentifierName") == "ORDER_ID":
                        effective_order_id = ri.get("relatedIdentifierValue", "")
                        break

                # 没有 ORDER_ID 的是店铺级费用 (ServiceFee/ProductAdsPayment/月度仓储费等)，也保留
                if not effective_order_id:
                    effective_order_id = ""
                    description = txn.get("description", "")

                sql = """
                    INSERT INTO amazon_order_finances (
                        shop_id, amazon_order_id, transaction_id,
                        transaction_type, transaction_status, description,
                        posted_date, total_amount, total_currency_code,
                        product_charges, fba_fees, commission,
                        marketplace_id, items_json, breakdowns_json, raw_json, sync_time
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        amazon_order_id = VALUES(amazon_order_id),
                        transaction_type = VALUES(transaction_type),
                        transaction_status = VALUES(transaction_status),
                        description = VALUES(description),
                        posted_date = VALUES(posted_date),
                        total_amount = VALUES(total_amount),
                        total_currency_code = VALUES(total_currency_code),
                        product_charges = VALUES(product_charges),
                        fba_fees = VALUES(fba_fees),
                        commission = VALUES(commission),
                        marketplace_id = VALUES(marketplace_id),
                        items_json = VALUES(items_json),
                        breakdowns_json = VALUES(breakdowns_json),
                        raw_json = VALUES(raw_json),
                        sync_time = NOW()
                """
                cursor.execute(sql, (
                    shop_id, effective_order_id, transaction_id,
                    txn.get("transactionType"),
                    txn.get("transactionStatus"),
                    txn.get("description"),
                    posted_date,
                    total_amt.get("currencyAmount"),
                    total_amt.get("currencyCode", "USD"),
                    product_charges or None, fba_fees or None, commission or None,
                    marketplace_id,
                    items_json,
                    breakdowns_json,
                    raw_json,
                ))
                saved += 1

                # Shipment: 回写实际费率到 amazon_product_fees
                if txn.get("transactionType") == "Shipment":
                    _update_product_real_fees(shop_id, items)

            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return saved


def _get_order_finances_from_db(shop_id, order_id):
    """
    从数据库查询指定订单的财务明细

    返回:
        list of dict
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT * FROM amazon_order_finances
                   WHERE shop_id = %s AND amazon_order_id = %s
                   ORDER BY posted_date DESC""",
                (shop_id, order_id),
            )
            rows = cursor.fetchall()
            return _serialize_finance_rows(rows)
    finally:
        conn.close()


def _list_finances_from_db(shop_id, amazon_order_id=None, transaction_type=None, page=1, page_size=20):
    """
    分页查询财务记录列表
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s"]
            params = [shop_id]
            if amazon_order_id:
                conditions.append("amazon_order_id = %s")
                params.append(amazon_order_id)
            if transaction_type:
                conditions.append("transaction_type = %s")
                params.append(transaction_type)
            where_clause = " AND ".join(conditions)

            cursor.execute(
                f"SELECT COUNT(*) AS total FROM amazon_order_finances WHERE {where_clause}",
                tuple(params),
            )
            total = cursor.fetchone()["total"]

            offset = (page - 1) * page_size
            cursor.execute(
                f"""SELECT * FROM amazon_order_finances
                    WHERE {where_clause}
                    ORDER BY posted_date DESC
                    LIMIT %s OFFSET %s""",
                tuple(params + [page_size, offset]),
            )
            return {
                "list": _serialize_finance_rows(cursor.fetchall()),
                "total": total,
                "page": page,
                "page_size": page_size,
            }
    finally:
        conn.close()


def _serialize_finance_rows(rows):
    """
    序列化查询结果为前端可用格式

    简介: 将 JSON 字符串字段转为 dict/list，datetime 转字符串。
    """
    result = []
    for row in rows:
        item = dict(row)
        for field in ("items_json", "breakdowns_json", "raw_json"):
            val = item.get(field)
            if isinstance(val, str):
                try:
                    item[field] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    pass
        for dt_field in ("posted_date", "sync_time"):
            val = item.get(dt_field)
            if val and hasattr(val, "isoformat"):
                item[dt_field] = val.strftime("%Y-%m-%d %H:%M:%S")
        result.append(item)
    return result


# ============================================================
# 辅助函数 — 工具
# ============================================================

def _iso_to_datetime(iso_str):
    """
    将 ISO 8601 时间字符串转为 MySQL DATETIME 格式
    """
    if not iso_str:
        return None
    s = str(iso_str).replace("Z", "").replace("T", " ")
    if "+" in s:
        s = s.split("+")[0]
    if "." in s:
        s = s.split(".")[0]
    return s


# ============================================================
# 辅助函数 — 同步编排
# ============================================================

def _sync_order_finances(shop_id, order_id):
    """
    同步单个订单的财务数据

    简介: 一站式调用——从 SP-API 拉取 → 写入数据库。

    返回:
        dict: { order_id, transactions_fetched, saved }
    """
    try:
        transactions = _fetch_transactions_from_api(shop_id, order_id)
        if not transactions:
            return {"order_id": order_id, "transactions_fetched": 0, "saved": 0}
        saved = _save_transactions_to_db(shop_id, order_id, transactions)
        return {"order_id": order_id, "transactions_fetched": len(transactions), "saved": saved}
    except Exception as e:
        return {"order_id": order_id, "error": str(e)}


def _sync_finances_for_shop(shop_id, days_to=3, days_from=7, order_ids=None):
    """
    同步指定店铺的财务数据

    简介: 为店铺的订单批量拉取财务数据，默认同步 3~7 天前更新的订单（已结算窗口）。

    详细:
      - order_ids 优先于日期范围
      - 如果未指定 order_ids: 查询 days_to ~ days_from 天前 updated 的订单
      - days_to=3, days_from=7 表示 3天前到7天前的订单（避开 T+2 未结算窗口）
      - 每个订单间 sleep 0.3s 控制 API 频率

    返回:
        dict: { total_orders, success, failed, errors }
    """
    from datetime import datetime, timedelta

    conn = get_db_connection()
    try:
        if order_ids:
            target_orders = order_ids
        else:
            to_date = (datetime.now() - timedelta(days=days_to)).strftime("%Y-%m-%d %H:%M:%S")
            from_date = (datetime.now() - timedelta(days=days_from)).strftime("%Y-%m-%d %H:%M:%S")
            with conn.cursor() as cursor:
                cursor.execute(
                    """SELECT amazon_order_id FROM amazon_orders
                       WHERE shop_id = %s AND last_update_date BETWEEN %s AND %s
                       ORDER BY last_update_date DESC""",
                    (shop_id, from_date, to_date),
                )
                target_orders = [row["amazon_order_id"] for row in cursor.fetchall()]
    finally:
        conn.close()

    if not target_orders:
        return {"total_orders": 0, "success": 0, "failed": 0, "errors": []}

    success = 0
    failed = 0
    errors = []

    for oid in target_orders:
        result = _sync_order_finances(shop_id, oid)
        if "error" in result:
            failed += 1
            errors.append(result)
        else:
            success += 1
        time.sleep(0.3)

    return {
        "total_orders": len(target_orders),
        "success": success,
        "failed": failed,
        "errors": errors,
    }


# ============================================================
# 对外暴露 — Cron 任务入口
# ============================================================

def sync_finances_recent(days_to=3, days_from=7):
    """
    批量同步所有启用店铺的已结算订单财务数据（Cron 入口）

    简介: 遍历所有 active 店铺，为已过结算周期（默认 3~7 天前）的订单拉取财务交易。

    详细:
      - days_to=3  days_from=7: 同步 3 天前到 7 天前更新的订单
      - 避开 T+2 未结算窗口，减少无效 API 调用

    返回:
        dict: { shop_id: result, ... }
    """
    results = {}
    shops = get_all_active_shops()
    if not shops:
        print("[Finances Sync] 没有启用的店铺，跳过")
        return results

    for shop in shops:
        shop_name = shop.get("shop_name", f"shop_{shop['id']}")
        shop_id = shop["id"]
        try:
            result = _sync_finances_for_shop(shop_id=shop_id, days_to=days_to, days_from=days_from)
            results[shop_id] = result
            print(f"[Finances Sync] 店铺[{shop_name}] 完成: {result}")
        except Exception as e:
            results[shop_id] = {"error": str(e)}
            print(f"[Finances Sync] 店铺[{shop_name}] 异常: {e}")

    return results
