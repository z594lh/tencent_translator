"""
FBA 月度仓储费同步服务

数据来源: SP-API Reports API v2021-06-30，报告类型 GET_FBA_STORAGE_FEE_CHARGES_DATA
粒度: 每个 SKU(FNSKU) × 仓库 × 计费月 一条仓储费记录

流程:
  create_report → poll(get_report) → get_report_document → 下载(可能 GZIP) → 解析 TSV → upsert

对外入口:
  sync_storage_fees(shop_id, start_date, end_date)  — 同步单个店铺
  sync_all_storage_fees(start_date, end_date)       — 同步所有启用店铺
"""
import csv
import gzip
import io
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

import requests

from services.mysql_service import get_db_connection
from services.shop_service import get_sp_api_client, get_all_active_shops

REPORT_TYPE = "GET_FBA_STORAGE_FEE_CHARGES_DATA"

# 需要落库的数值字段（其余跳过）
_DECIMAL_FIELDS = {
    "average_quantity_on_hand",
    "estimated_total_item_volume",
    "base_rate",
    "utilization_surcharge_rate",
    "estimated_monthly_storage_fee",
    "total_incentive_fee_amount",
}

_STRING_FIELDS = {
    "asin", "fnsku", "product_name", "fulfillment_center", "country_code",
    "product_size_tier", "currency",
}


def _to_decimal(v):
    """字符串 → Decimal，非数字('--'/'' 等)返回 None"""
    if v is None:
        return None
    s = str(v).strip().replace("$", "").replace(",", "")
    if s in ("", "--", "-"):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _clean_fc(v):
    """仓库代码在报告中带单引号，如 'TUL2' → TUL2"""
    if v is None:
        return None
    return str(v).strip().strip("'")


def _parse_tsv(text):
    """解析 TSV 文本 → list[dict]（仅保留我们关心的字段）"""
    rows = []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    for r in reader:
        row = {}
        for f in _STRING_FIELDS:
            val = r.get(f)
            row[f] = val if val not in (None, "") else None
        row["fulfillment_center"] = _clean_fc(row.get("fulfillment_center"))
        for f in _DECIMAL_FIELDS:
            row[f] = _to_decimal(r.get(f))
        month = r.get("month_of_charge") or ""
        row["month_of_charge"] = month[:7] if month else None
        rows.append(row)
    return rows


def _download(url, compression=None, proxies=None):
    resp = requests.get(url, timeout=300, proxies=proxies)
    resp.raise_for_status()
    data = resp.content
    if (compression or "").upper() == "GZIP" or data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except gzip.BadGzipFile:
            pass
    return data.decode("utf-8-sig")


def _fetch_report(client, start_date=None, end_date=None):
    """创建（或复用）仓储费报告并下载解析，返回 list[dict]"""
    report_id = None
    # 1. 复用最近一份 DONE 报告
    try:
        resp = client.get_reports(
            report_types=[REPORT_TYPE],
            processing_statuses=["DONE"],
            page_size=10,
        )
        reports = resp.get("reports", []) or []
        reports.sort(key=lambda r: r.get("createdTime", ""), reverse=True)
        if reports:
            report_id = reports[0].get("reportId")
    except Exception as e:
        print(f"[StorageFee] 查询已有报告失败(继续新建): {e}")

    # 2. 无则新建（仓储费报告必须指定 dataStartTime/dataEndTime，否则可能被 CANCELLED）
    if not report_id:
        end = end_date or datetime.utcnow()
        start = start_date or (end - timedelta(days=120))
        if isinstance(start, str):
            start = datetime.strptime(start, "%Y-%m-%d")
        if isinstance(end, str):
            end = datetime.strptime(end, "%Y-%m-%d")
        resp = client.create_report(
            REPORT_TYPE,
            marketplace_ids=[client.marketplace_id],
            dataStartTime=start.strftime("%Y-%m-%dT00:00:00Z"),
            dataEndTime=end.strftime("%Y-%m-%dT00:00:00Z"),
        )
        report_id = resp.get("reportId")
        if not report_id:
            raise RuntimeError(f"创建仓储费报告失败: {resp}")

    # 3. 轮询直到 DONE
    deadline = time.time() + 15 * 60
    doc_id = None
    while time.time() < deadline:
        r = client.get_report(report_id)
        status = (r.get("processingStatus") or "").upper()
        if status == "DONE":
            doc_id = r.get("reportDocumentId")
            break
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"仓储费报告失败: {r}")
        time.sleep(10)
    if not doc_id:
        raise TimeoutError("仓储费报告 15 分钟内未完成")

    # 4. 下载
    doc = client.get_report_document(doc_id)
    url = doc.get("url")
    if not url:
        raise RuntimeError(f"仓储费报告无下载 URL: {doc}")
    text = _download(url, doc.get("compressionAlgorithm"), proxies=client.proxies)

    return _parse_tsv(text)


def _load_fnsku_sku_map(shop_id):
    """fnsku → seller_sku 映射（amazon_inventory 优先，products 兜底）"""
    mapping = {}
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("SELECT fn_sku, seller_sku FROM amazon_inventory "
                      "WHERE shop_id=%s AND fn_sku IS NOT NULL AND fn_sku != ''", (shop_id,))
            for r in c.fetchall():
                mapping[r["fn_sku"]] = r["seller_sku"]
            c.execute("SELECT fnsku, seller_sku FROM products "
                      "WHERE fnsku IS NOT NULL AND fnsku != ''")
            for r in c.fetchall():
                mapping.setdefault(r["fnsku"], r["seller_sku"])
    finally:
        conn.close()
    return mapping


def sync_storage_fees(shop_id, start_date=None, end_date=None):
    """同步单个店铺的仓储费，返回统计 dict"""
    client = get_sp_api_client(shop_id)
    rows = _fetch_report(client, start_date=start_date, end_date=end_date)

    fnsku_map = _load_fnsku_sku_map(shop_id)

    conn = get_db_connection()
    inserted = updated = 0
    try:
        with conn.cursor() as c:
            for row in rows:
                fnsku = row.get("fnsku")
                month = row.get("month_of_charge")
                fc = row.get("fulfillment_center")
                if not fnsku or not month or not fc:
                    continue
                seller_sku = fnsku_map.get(fnsku)

                c.execute("""
                    INSERT INTO amazon_fba_storage_fees (
                        shop_id, asin, fnsku, seller_sku, product_name,
                        fulfillment_center, country_code, month_of_charge,
                        product_size_tier, average_quantity_on_hand,
                        estimated_total_item_volume, base_rate,
                        utilization_surcharge_rate, currency,
                        estimated_monthly_storage_fee, total_incentive_fee_amount
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        asin = VALUES(asin),
                        seller_sku = VALUES(seller_sku),
                        product_name = VALUES(product_name),
                        country_code = VALUES(country_code),
                        product_size_tier = VALUES(product_size_tier),
                        average_quantity_on_hand = VALUES(average_quantity_on_hand),
                        estimated_total_item_volume = VALUES(estimated_total_item_volume),
                        base_rate = VALUES(base_rate),
                        utilization_surcharge_rate = VALUES(utilization_surcharge_rate),
                        currency = VALUES(currency),
                        estimated_monthly_storage_fee = VALUES(estimated_monthly_storage_fee),
                        total_incentive_fee_amount = VALUES(total_incentive_fee_amount)
                """, (
                    shop_id, row.get("asin"), fnsku, seller_sku, row.get("product_name"),
                    fc, row.get("country_code"), month,
                    row.get("product_size_tier"), row.get("average_quantity_on_hand"),
                    row.get("estimated_total_item_volume"), row.get("base_rate"),
                    row.get("utilization_surcharge_rate"), row.get("currency") or "USD",
                    row.get("estimated_monthly_storage_fee"), row.get("total_incentive_fee_amount"),
                ))
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    return {
        "shop_id": shop_id,
        "rows": len(rows),
        "saved": inserted,
        "skus_mapped": len(fnsku_map),
    }


def sync_all_storage_fees(start_date=None, end_date=None):
    """同步所有启用店铺的仓储费"""
    results = {}
    shops = get_all_active_shops()
    if not shops:
        print("[StorageFee] 没有启用店铺")
        return results
    for shop in shops:
        sid = shop["id"]
        name = shop.get("shop_name", f"shop_{sid}")
        try:
            results[sid] = sync_storage_fees(sid, start_date=start_date, end_date=end_date)
            print(f"[StorageFee] 店铺[{name}] 完成: {results[sid]}")
        except Exception as e:
            results[sid] = {"error": str(e)}
            print(f"[StorageFee] 店铺[{name}] 异常: {e}")
    return results
