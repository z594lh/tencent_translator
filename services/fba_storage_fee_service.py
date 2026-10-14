"""FBA月度仓储费同步服务，真实外部同步入口需明确授权后执行。

来源为SP-API Reports API的GET_FBA_STORAGE_FEE_CHARGES_DATA，粒度为店铺/FNSKU/仓库/计费月。
sync_storage_fees同步单店，sync_all_storage_fees逐启用店；原storage_fee_sync.py调度调用。
先核对UTC查询范围，只复用6小时内同站点/精确范围DONE报告，否则创建、轮询、下载解析。
所有行解析校验完才事务UPSERT到amazon_fba_storage_fees，失败回滚；不因旧DONE永久停留旧月份。
报告查询范围不保证Amazon返回每个月，months是实际返回账期；不得把未返回月份伪造成零。
与纯本地报表流水线不同，此服务会调用Amazon/下载网络并写库，不能作为无副作用测试自动执行。
"""
import csv
import gzip
import io
import time
from datetime import datetime, timedelta, timezone
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
    """将TSV数值字符串转换为有限Decimal，缺/非法返回None。

    清理美元符号及千位逗号，不将缺值当0；必需费用字段由_parse_tsv进一步阻止保存。"""
    if v is None:
        return None
    s = str(v).strip().replace("$", "").replace(",", "")
    if s in ("", "--", "-"):
        return None
    try:
        value=Decimal(s)
        return value if value.is_finite() else None
    except InvalidOperation:
        return None


def _clean_fc(v):
    """清理仓库代码首尾空白/单引号并返回字符串。

    缺值返回None，供后续判断可落库身份，纯函数。"""
    if v is None:
        return None
    return str(v).strip().strip("'")


def _parse_tsv(text):
    """解析完整TSV文本，返回只保留需要字段的记录列表。

    必需列、规范计费月份、有效费用与币种必须存在，失败抛ValueError且尚未写库。
    FNSKU/月/仓库空身份由归集阶段拒绝整批，不保存半批，也不伪造账期。"""
    rows = []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    required = {'fnsku','month_of_charge','fulfillment_center','estimated_monthly_storage_fee','currency'}
    if not required.issubset(set(reader.fieldnames or [])):
        raise ValueError('仓储费报告缺少必需列，未写入任何数据')
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
        if row['month_of_charge']:
            parsed=datetime.strptime(row['month_of_charge'],'%Y-%m')
            if parsed.strftime('%Y-%m')!=row['month_of_charge']:
                raise ValueError('仓储费月份格式无效')
        if row['estimated_monthly_storage_fee'] is None or not row['currency']:
            raise ValueError('仓储费报告金额或币种缺失，未写入任何数据')
        rows.append(row)
    return rows


def aggregate_monthly_rows(rows):
    """同一存储身份的原行是费用明细，先Decimal求和再UPSERT，重复同步不叠加。

    旧唯一键不含国家/币种；碰到不兼容身份必须拒绝整批，不覆盖另一国家费用。
    费率不是可加金额，多费率保留未知；费用、抵扣、数量和体积均只加已知完整列。
    """
    groups = {}
    for original in rows:
        row = dict(original)
        row['fulfillment_center'] = _clean_fc(row.get('fulfillment_center'))
        key = tuple(row.get(k) for k in ('fnsku', 'fulfillment_center', 'month_of_charge'))
        if not all(key):
            raise ValueError('仓储费原行身份缺失，整批未保存')
        groups.setdefault(key, []).append(row)
    result = []
    additive = _DECIMAL_FIELDS - {'base_rate', 'utilization_surcharge_rate'}
    for records in groups.values():
        if len({(r.get('country_code'), r.get('currency')) for r in records}) != 1:
            raise ValueError('仓储费唯一身份存在不同国家或币种，整批未保存')
        row = dict(records[0])
        for field in additive:
            values = [_to_decimal(r.get(field)) for r in records]
            row[field] = sum(values, Decimal('0')) if all(v is not None for v in values) else None
        for field in ('base_rate', 'utilization_surcharge_rate'):
            values = {_to_decimal(r.get(field)) for r in records}
            row[field] = next(iter(values)) if len(values) == 1 else None
        if row['estimated_monthly_storage_fee'] is None or not row.get('currency'):
            raise ValueError('仓储费原行金额或币种缺失，整批未保存')
        result.append(row)
    return result


def project_monthly_rows(cursor, shop_id, rows):
    """调用方事务内更新完整报告覆盖账期/国家/币种的历史投影，不提交，不碰订单/财务。

    与工作台共用报告原行，避免旧UPSERT最后一行覆盖其他同仓费用。
    SKU只从同店库存或既有仓储费映射，未唯一映射保留FNSKU，不跨店猜测。
    """
    grouped = aggregate_monthly_rows(rows)
    cursor.execute('SELECT fn_sku AS fnsku,seller_sku FROM amazon_inventory WHERE shop_id=%s', (shop_id,))
    identities = list(cursor.fetchall())
    cursor.execute('SELECT fnsku,seller_sku,fulfillment_center,month_of_charge,country_code,currency FROM amazon_fba_storage_fees WHERE shop_id=%s FOR UPDATE', (shop_id,))
    existing = list(cursor.fetchall())
    identities += existing
    previous = {(r['fnsku'], _clean_fc(r['fulfillment_center']), r['month_of_charge']): (r['country_code'], r['currency']) for r in existing}
    mapping = {}
    for item in identities:
        if item.get('fnsku') and item.get('seller_sku'):
            mapping.setdefault(item['fnsku'], set()).add(item['seller_sku'])
    for row in grouped:
        key = (row['fnsku'], row['fulfillment_center'], row['month_of_charge'])
        if key in previous and previous[key] != (row.get('country_code'), row['currency']):
            raise ValueError('历史仓储费唯一身份对应另一国家或币种，未覆盖')
    covered = {(r['month_of_charge'], r.get('country_code'), r['currency']) for r in grouped}
    incoming = {(r['fnsku'], r['fulfillment_center'], r['month_of_charge']) for r in grouped}
    # 完整报告中消失的旧明细不能继续残留在年度图；仅清理已覆盖范围，未返回月份/国家保留。
    for item in existing:
        key = (item['fnsku'], _clean_fc(item['fulfillment_center']), item['month_of_charge'])
        if (item['month_of_charge'], item['country_code'], item['currency']) in covered and (key not in incoming or item['fulfillment_center'] != key[1]):
            cursor.execute('''DELETE FROM amazon_fba_storage_fees WHERE shop_id=%s AND fnsku=%s
                AND fulfillment_center=%s AND month_of_charge=%s AND country_code=%s AND currency=%s''',
                (shop_id, item['fnsku'], item['fulfillment_center'], item['month_of_charge'], item['country_code'], item['currency']))
    for row in grouped:
        candidates = mapping.get(row['fnsku'], set())
        seller_sku = next(iter(candidates)) if len(candidates) == 1 else None
        cursor.execute('''INSERT INTO amazon_fba_storage_fees
            (shop_id,asin,fnsku,seller_sku,product_name,fulfillment_center,country_code,month_of_charge,
             product_size_tier,average_quantity_on_hand,estimated_total_item_volume,base_rate,
             utilization_surcharge_rate,currency,estimated_monthly_storage_fee,total_incentive_fee_amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE asin=VALUES(asin),seller_sku=VALUES(seller_sku),product_name=VALUES(product_name),
             country_code=VALUES(country_code),product_size_tier=VALUES(product_size_tier),
             average_quantity_on_hand=VALUES(average_quantity_on_hand),estimated_total_item_volume=VALUES(estimated_total_item_volume),
             base_rate=VALUES(base_rate),utilization_surcharge_rate=VALUES(utilization_surcharge_rate),currency=VALUES(currency),
             estimated_monthly_storage_fee=VALUES(estimated_monthly_storage_fee),total_incentive_fee_amount=VALUES(total_incentive_fee_amount),
             synced_at=CURRENT_TIMESTAMP''',
            (shop_id, row.get('asin'), row['fnsku'], seller_sku, row.get('product_name'), row['fulfillment_center'],
             row.get('country_code'), row['month_of_charge'], row.get('product_size_tier'), row.get('average_quantity_on_hand'),
             row.get('estimated_total_item_volume'), row.get('base_rate'), row.get('utilization_surcharge_rate'),
             row['currency'], row['estimated_monthly_storage_fee'], row.get('total_incentive_fee_amount')))
    return len(grouped)


def _download(url, compression=None, proxies=None):
    """通过HTTP下载报告文件，处理GZIP并解码UTF-8文本。

    url是Amazon返回的临时地址，proxies沿用客户端；300秒超时，网络状态异常抛出。
    有真实网络副作用，不应在未Mock的测试/未授权排查中调用，不输出下载URL。"""
    resp = requests.get(url, timeout=300, proxies=proxies)
    resp.raise_for_status()
    data = resp.content
    if (compression or "").upper() == "GZIP" or data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except gzip.BadGzipFile:
            pass
    return data.decode("utf-8-sig")


def _report_range(start_date=None,end_date=None,now=None):
    """把start_date/end_date转换为UTC零点范围(start,end)。

    允许严格日期字符串或datetime；now可注入UTC，默认终点当前日、起点前120天。
    起点须早于终点、终点不在未来、跨度<=366天；失败报ValueError，不调用外部API。"""
    now = now or datetime.now(timezone.utc)
    def parse(value):
        """将单个日期输入解释为带tzinfo的UTC datetime。

        严格YYYY-MM-DD或datetime；无时区datetime按UTC解释，已有时区转UTC，非法值报错。"""
        if isinstance(value,str):
            result = datetime.strptime(value,'%Y-%m-%d')
            if result.strftime('%Y-%m-%d') != value:
                raise ValueError('仓储费日期必须为 YYYY-MM-DD')
            return result.replace(tzinfo=timezone.utc)
        if isinstance(value,datetime):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        raise ValueError('仓储费日期必须为 YYYY-MM-DD')
    end = parse(end_date) if end_date not in (None,'') else now
    start = parse(start_date) if start_date not in (None,'') else end-timedelta(days=120)
    start = start.replace(hour=0,minute=0,second=0,microsecond=0)
    end = end.replace(hour=0,minute=0,second=0,microsecond=0)
    if start>=end or end>now or (end-start).days>366:
        raise ValueError('仓储费日期范围必须已开始、起点早于终点且不超过 366 天')
    return start,end


def _report_time(value):
    """解析Amazon ISO时间并转为UTC datetime，非法/无时区返回None。

    用于报告匹配，未知时间不能认定新鲜报告；纯函数。"""
    try:
        parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError,TypeError):
        return None


def _reusable_report(report,client,start,end,now):
    """判断report是否可用于client当前站点及精确start/end范围。

    要求DONE、有reportId、站点匹配、起止UTC一致、创建距now为0~6小时。
    返回真假判定结果；不因列表最新一条DONE就复用，不调用API或写库。"""
    created=_report_time(report.get('createdTime'))
    return (report.get('processingStatus')=='DONE' and report.get('reportId') and
            client.marketplace_id in (report.get('marketplaceIds') or []) and
            _report_time(report.get('dataStartTime'))==start and _report_time(report.get('dataEndTime'))==end and
            created is not None and timedelta(0)<=now-created<=timedelta(hours=6))


def _fetch_report(client, start_date=None, end_date=None):
    """获取实际仓储报告并返回已解析记录，可能产生真实Amazon报告任务。

    先校验范围，在近6小时列表最多3页中找可复用项；列表失败才继续尝试创建。
    新报告使用显式站点/起止UTC，轮询最长15分钟、每10秒检查；FATAL/CANCELLED/超时抛出。
    完成后下载并整份解析，返回月份由实际内容决定；此方法不写仓储表。"""
    report_id = None
    now=datetime.now(timezone.utc)
    start,end=_report_range(start_date,end_date,now)
    # 只复用同站点、完全相同范围且足够新鲜的报告，不能永久沿用历史DONE。
    try:
        reports=[]
        token=None
        for _ in range(3):
            resp = client.get_reports(
                report_types=[REPORT_TYPE],processing_statuses=['DONE'],
                marketplace_ids=[client.marketplace_id],created_since=(now-timedelta(hours=6)).isoformat(),
                page_size=100,next_token=token)
            reports.extend(resp.get('reports',[]) or [])
            next_token=resp.get('nextToken')
            if not next_token or next_token==token:
                break
            token=next_token
        reports.sort(key=lambda r: r.get("createdTime", ""), reverse=True)
        for report in reports:
            if _reusable_report(report,client,start,end,now):
                report_id=report['reportId']
                break
    except Exception as e:
        print(f"[StorageFee] 查询已有报告失败(继续新建): {type(e).__name__}")

    # 2. 无则新建（仓储费报告必须指定 dataStartTime/dataEndTime，否则可能被 CANCELLED）
    if not report_id:
        resp = client.create_report(
            REPORT_TYPE,
            marketplace_ids=[client.marketplace_id],
            dataStartTime=start.strftime("%Y-%m-%dT00:00:00Z"),
            dataEndTime=end.strftime("%Y-%m-%dT00:00:00Z"),
        )
        report_id = resp.get("reportId")
        if not report_id:
            raise RuntimeError('创建仓储费报告未返回任务 ID')

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
            raise RuntimeError(f'仓储费报告状态 {status}，请核对站点及月份是否已发布')
        time.sleep(10)
    if not doc_id:
        raise TimeoutError("仓储费报告 15 分钟内未完成")

    # 4. 下载
    doc = client.get_report_document(doc_id)
    url = doc.get("url")
    if not url:
        raise RuntimeError('仓储费报告没有下载地址')
    text = _download(url, doc.get("compressionAlgorithm"), proxies=client.proxies)

    return _parse_tsv(text)


def _load_fnsku_sku_map(shop_id):
    """只读获取FNSKU到seller_sku映射并返回字典。

    优先同shop库存，products补缺映射；独立连接关闭，不改库存或商品。"""
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
    """同步指定shop_id真实仓储费，返回rows/saved/months/skipped等统计。

    先下载校验完整报告，再按同仓原行归集；身份或金额缺失整批不保存。
    按既有唯一键更新投影及同步时间，所有本批行同事务；异常回滚并关闭连接。
    saved是本次成功处理行数，含已有行更新，不等于纯新增条数；不会自动补缺月份。"""
    client = get_sp_api_client(shop_id)
    original_rows = _fetch_report(client, start_date=start_date, end_date=end_date)
    rows = aggregate_monthly_rows(original_rows)

    conn = get_db_connection()
    inserted = 0
    try:
        conn.begin()
        with conn.cursor() as c:
            inserted = project_monthly_rows(c, shop_id, original_rows)
            c.execute('SELECT COUNT(DISTINCT fnsku) AS mapped FROM amazon_fba_storage_fees WHERE shop_id=%s AND seller_sku IS NOT NULL', (shop_id,))
            mapped = c.fetchone()['mapped']
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "shop_id": shop_id,
        "rows": len(original_rows),
        "saved": inserted,
        "skus_mapped": mapped,
        "months": sorted({row['month_of_charge'] for row in rows if row.get('month_of_charge')}),
        "skipped":0,
        "merged_rows":len(original_rows)-len(rows),
    }


def sync_all_storage_fees(start_date=None, end_date=None):
    """逐个启用店铺执行真实单店同步并返回{shop_id:统计或error}。

    一个店铺失败继续其他店铺，不能把整批日志当作每店都成功；无多店原子事务。
    有外部API/库写入副作用，调用前需明确同步范围与授权。"""
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
