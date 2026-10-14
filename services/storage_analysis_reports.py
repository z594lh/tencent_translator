"""显式请求/检查Amazon仓储分析报告；没有后台线程、自动移除或库存修改。"""
import csv
import hashlib
import io
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from services import storage_analysis_repository as repo
from services.shop_service import get_sp_api_client
from services.fba_storage_fee_service import _download, _report_time, _clean_fc

REPORT_TYPES = {
    'health': 'GET_FBA_INVENTORY_PLANNING_DATA',
    'aged': 'GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA',
    'monthly': 'GET_FBA_STORAGE_FEE_CHARGES_DATA',
    'overage': 'GET_FBA_OVERAGE_FEE_CHARGES_DATA',
}
COUNTRIES = {
    'ATVPDKIKX0DER': ('US', 'na'), 'A2EUQ1WTGCTBG2': ('CA', 'na'),
    'A1F83G8C2ARO7P': ('GB', 'eu'), 'A1PA6795UKMFR9': ('DE', 'eu'),
    'A1VC38T7YXB528': ('JP', 'fe'), 'A1RKKUPIHCS9HS': ('ES', 'eu'),
    'APJ6JRA9NG5V4': ('IT', 'eu'), 'A13V1IB3VIYZZH': ('FR', 'eu'),
}
OVERAGE_RETIRED_MARKETS = {'ATVPDKIKX0DER', 'A2EUQ1WTGCTBG2', 'A1F83G8C2ARO7P',
                          'A1PA6795UKMFR9', 'A1RKKUPIHCS9HS', 'APJ6JRA9NG5V4', 'A13V1IB3VIYZZH'}
OVERAGE_POLICY_URL = 'https://sellercentral.amazon.nl/help/hub/reference/external/GV8JEETWV9Q33CMX'


def overage_policy(binding, month):
    """Amazon于2024-07停止美国/加拿大/英国/欧洲超量费；不套用于日本或更早账期。"""
    if binding[1] in OVERAGE_RETIRED_MARKETS and valid_month(month) >= '2024-07':
        return {'kind':'overage', 'period_key':month, 'attempt_status':'not_applicable',
                'attempt_message':'该站点自2024-07-01起不再收取仓储超量费；此账期不需申请超量报告。与利用率附加费、超龄费不同。',
                'row_count':0, 'data_at':None, 'policy_url':OVERAGE_POLICY_URL}
    return None


def report_window(kind, month):
    start, end = period_range(month)
    # 官方要求超龄请求跨度恰好一个月，不能把月末最后一秒推断为修复方式。
    return start, end


def valid_month(value):
    if not isinstance(value, str) or not re.fullmatch(r'[1-9]\d{3}-(0[1-9]|1[0-2])', value):
        raise ValueError('计费月必须为YYYY-MM')
    return value


def period_range(month):
    valid_month(month)
    start = datetime.strptime(month, '%Y-%m').replace(tzinfo=timezone.utc)
    index = start.year * 12 + start.month
    end = datetime(index // 12, index % 12 + 1, 1, tzinfo=timezone.utc)
    if end > datetime.now(timezone.utc):
        raise ValueError('费用报告只能请求已结束的完整计费月')
    return start, end


def number(value, required=False):
    if value is None or str(value).strip().upper() in ('', '--', '-', 'N/A', 'NA'):
        if required:
            raise ValueError('报告必需数值缺失，未保存')
        return None
    try:
        result = Decimal(str(value).strip().replace(',', ''))
        if not result.is_finite():
            raise InvalidOperation()
        return format(result, 'f')
    except InvalidOperation:
        raise ValueError('报告含非法数值，未保存') from None


def quantity(value, required=False):
    """健康报告部分可选库存字段用-1表示未提供，不能展示成负库存。"""
    result = number(value, required)
    if result is not None and Decimal(result) < 0:
        if required:
            raise ValueError('报告必需库存数量为负，未保存')
        return None
    return result


def report_date(value):
    value = str(value or '')[:10]
    try:
        return datetime.strptime(value, '%Y-%m-%d').date().isoformat()
    except ValueError:
        raise ValueError('报告缺少有效快照日期，未保存') from None


def country(value):
    value = str(value or '').strip().upper()
    return {'UNITED STATES': 'US', 'UNITED KINGDOM': 'GB', 'UK': 'GB', 'CANADA': 'CA', 'GERMANY': 'DE', 'FRANCE': 'FR', 'ITALY': 'IT', 'SPAIN': 'ES', 'JAPAN': 'JP'}.get(value, value)


def parse_report(text, kind, period, binding, all_months=False):
    """按实际字段解析，整份验证；重叠库龄档不直接求和，缺金额不算0。"""
    reader = csv.DictReader(io.StringIO(text.lstrip('\ufeff')), delimiter='\t')
    required = {
        'health': {'snapshot-date', 'sku', 'fnsku', 'available'},
        'aged': {'snapshot-date', 'sku', 'fnsku', 'country', 'currency', 'qty-charged', 'amount-charged', 'surcharge-age-tier'},
        'monthly': {'fnsku', 'month_of_charge', 'fulfillment_center', 'country_code', 'currency', 'estimated_monthly_storage_fee'},
        'overage': {'charged_date', 'country_code', 'storage_type', 'charged_fee_amount', 'currency_code'},
    }[kind]
    if not required.issubset(set(reader.fieldnames or [])):
        raise ValueError('报告列与当前支持结构不符，未覆盖已有资料')
    rows, keys, snapshots = [], set(), set()
    monthly_counts, occurrences = {}, {}
    for raw in reader:
        r = {str(k).strip(): str(v or '').strip() for k, v in raw.items() if k is not None}
        sku, fnsku, asin = r.get('sku') or r.get('seller_sku'), r.get('fnsku'), r.get('asin')
        if kind != 'overage' and (not fnsku or (kind != 'monthly' and not sku)):
            raise ValueError('报告业务身份缺失，未保存部分数据')
        cur = (r.get('currency') or r.get('currency_code') or '').upper()
        co = country(r.get('country') or r.get('country_code'))
        date = None
        fc = _clean_fc(r.get('fulfillment_center')) or None
        if kind == 'health':
            expected = COUNTRIES.get(binding[1])
            if not expected or expected[1] != binding[3]:
                raise ValueError('当前健康报告站点国家映射未确认，暂不导入')
            if co and co != expected[0]:
                raise ValueError('健康报告国家与请求站点不一致，未导入')
            domains = {'US': 'amazon.com', 'CA': 'amazon.ca', 'GB': 'amazon.co.uk', 'DE': 'amazon.de',
                       'FR': 'amazon.fr', 'IT': 'amazon.it', 'ES': 'amazon.es', 'JP': 'amazon.co.jp'}
            marketplace = r.get('marketplace', '').lower().removeprefix('www.')
            if marketplace and marketplace not in (binding[1].lower(), expected[0].lower(), domains[expected[0]]):
                raise ValueError('健康报告市场字段与请求站点不一致，未导入')
            co = expected[0]
            date = report_date(r.get('snapshot-date'))
            if date > datetime.now(timezone.utc).date().isoformat():
                raise ValueError('健康报告快照日期在未来，未保存')
            snapshots.add(date)
            # 实测报告的库龄快照早于报告快照，必须保存独立日期；存在空列也不回填今天。
            age_date = (report_date(r['Inventory age snapshot date']) if r.get('Inventory age snapshot date') else None) if 'Inventory age snapshot date' in r else date
            if age_date and age_date > date:
                raise ValueError('库龄快照晚于健康报告日期，未保存')
            ages = [{'tier': key[8:-5], 'quantity': quantity(value)} for key, value in r.items() if re.fullmatch(r'inv-age-\d+(?:-to-\d+|-plus)-days', key)]
            estimates = [{'tier': key[14:], 'amount': number(value), 'quantity': quantity(r.get('quantity-to-be-charged-ais-' + key[14:]))} for key, value in r.items() if key.startswith('estimated-ais-')]
            payload = {'available': quantity(r.get('available'), True), 'sales_30d': quantity(r.get('units-shipped-t30')),
                       'age_buckets': ages, 'estimated_aged': estimates, 'product_name': r.get('product-name'),
                       'age_snapshot_date': age_date, 'condition': r.get('condition'),
                       'excess_quantity': quantity(r.get('estimated-excess-quantity')),
                       'pending_removal': quantity(r.get('pending-removal-quantity')),
                       'unfulfillable': quantity(r.get('unfulfillable-quantity')),
                       'inbound_quantity': quantity(r.get('inbound-quantity')),
                       'reserved_quantity': quantity(r.get('Total Reserved Quantity')),
                       'fc_transfer': quantity(r.get('fc-transfer')),
                       'report_days_of_supply': quantity(r.get('days-of-supply')),
                       'amazon_recommended_action': r.get('recommended-action') or None}
        elif kind == 'aged':
            date = report_date(r.get('snapshot-date'))
            if date[:7] != period:
                raise ValueError('报告收费日期不属于所选月份，未保存')
            payload = {'amount': number(r.get('amount-charged'), True), 'quantity': number(r.get('qty-charged'), True),
                       'tier': r['surcharge-age-tier'], 'rate': number(r.get('rate-surcharge')), 'product_name': r.get('product-name')}
        elif kind == 'monthly':
            charge_month = valid_month(r.get('month_of_charge', '')[:7])
            period_range(charge_month)
            if not all_months and charge_month != period:
                continue
            if not fc:
                raise ValueError('月费报告缺仓库，未保存')
            date = charge_month + '-01'
            payload = {'storage_fee': number(r.get('estimated_monthly_storage_fee'), True),
                       'incentive_fee': number(r.get('total_incentive_fee_amount')),
                       'base_fee': number(r.get('est_base_msf')), 'utilization_fee': number(r.get('est_sus')),
                       'volume_units': r.get('volume_units'), 'product_name': r.get('product_name'),
                       'charge_month': charge_month, 'size_tier': r.get('product_size_tier'),
                       **{target: number(r.get(field)) for target, field in (
                           ('average_quantity', 'average_quantity_on_hand'), ('item_volume', 'item_volume'),
                           ('total_volume', 'estimated_total_item_volume'), ('base_rate', 'base_rate'),
                           ('utilization_rate', 'utilization_surcharge_rate'), ('utilization_ratio', 'storage_utilization_ratio'),
                           ('sus_quantity', 'avg_qty_for_sus'), ('sus_volume', 'est_vol_for_sus'))},
                       'utilization_ratio_units': r.get('storage_utilization_ratio_units')}
        else:
            date = report_date(r.get('charged_date'))
            if date[:7] != period:
                raise ValueError('仓储超量费日期不属于所选月份，未保存')
            if not r.get('storage_type'):
                raise ValueError('仓储超量费缺存储类型，未保存')
            payload = {'storage_type': r['storage_type'], 'amount': number(r.get('charged_fee_amount'), True),
                       'rate': number(r.get('charge_rate')), 'usage_volume': number(r.get('storage_usage_volume')),
                       'limit_volume': number(r.get('storage_limit_volume')), 'overage_volume': number(r.get('overage_volume')),
                       'volume_units': r.get('volume_unit')}
        # 四类固定仓储报告完整留存业务列，便于复查新版字段；不保存授权/买家信息列。
        blocked = re.compile(r'token|secret|authorization|buyer|recipient|email|phone|address', re.I)
        payload['report_fields'] = {k: v for k, v in r.items() if not blocked.search(k)}
        if not co or len(co) > 8 or (kind != 'health' and not cur):
            raise ValueError('报告国家或币种缺失，未保存')
        identity = [sku, fnsku, asin, co, cur, fc, date, r.get('condition'), payload.get('tier'), payload.get('storage_type')]
        key = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode()).hexdigest()
        if kind == 'monthly':
            # 同仓可能多条有效计费明细，保留每行及序号；整批替换防重复同步，不按唯一键丢掉后行。
            payload['business_key'] = key
            monthly_counts[key] = monthly_counts.get(key, 0) + 1
            fingerprint = hashlib.sha256(json.dumps([identity, payload['report_fields']], ensure_ascii=False, sort_keys=True).encode()).hexdigest()
            occurrences[fingerprint] = occurrences.get(fingerprint, 0) + 1
            key = hashlib.sha256(f'{fingerprint}:{occurrences[fingerprint]}'.encode()).hexdigest()
        if key in keys:
            raise ValueError('报告出现重复业务身份，需核对后导入，未重复累计')
        keys.add(key)
        rows.append({'row_key': key, 'sku': sku, 'fnsku': fnsku, 'asin': asin, 'country': co,
                     'currency': cur, 'fc': fc, 'record_date': date, 'payload': payload})
    if kind == 'health' and len(snapshots) > 1:
        raise ValueError('健康报告包含多个快照日期，暂不合并')
    if kind == 'monthly' and not rows:
        raise ValueError('月报未提供可用计费月份，未覆盖原资料')
    if kind == 'monthly':
        for row in rows:
            row['payload']['identity_rows'] = monthly_counts[row['payload']['business_key']]
    return rows


def public_batch(row):
    return {key: (row[key].replace(tzinfo=timezone.utc).isoformat() if isinstance(row.get(key), datetime)
                  else row[key].isoformat() if hasattr(row.get(key), 'isoformat') else row.get(key))
            for key in ('kind', 'period_key', 'attempt_status', 'attempt_message', 'requested_at', 'checked_at', 'data_at', 'snapshot_date', 'row_count')}


def advance(shop_id, kind, month, action, *, automatic=False, expected_binding=None):
    if kind not in REPORT_TYPES or action not in ('start', 'check'):
        raise ValueError('资料类型或操作无效')
    binding = repo.scope(shop_id)
    if expected_binding is not None and binding != expected_binding:
        raise ValueError('店铺范围已变化，未执行自动采集')
    if kind == 'health' and (binding[1] not in COUNTRIES or COUNTRIES[binding[1]][1] != binding[3]):
        raise ValueError('当前站点的健康报告国家映射尚未确认，暂不创建请求')
    period = 'latest' if kind == 'health' else valid_month(month)
    window = None if kind == 'health' else report_window(kind, period)
    if kind == 'overage':
        policy = overage_policy(binding, period)
        if policy:
            return policy
    with repo.report_lock(binding):
        row = repo.batch(binding, kind, period)
        now = datetime.utcnow()
        pending = row and row['attempt_status'] in ('creating', 'pending')
        # 已知ID只续查，报告生成慢不能成为重复创建的理由；只有创建未确认才过期。
        if pending and not row.get('report_id') and now - row['requested_at'] > timedelta(minutes=20):
            repo.state(row['id'], 'failed', '请求已超时；未自动重建，请核对后重新更新')
            return public_batch(repo.batch(binding, kind, period))
        if action == 'check' and not pending:
            return public_batch(row) if row else {'kind': kind, 'attempt_status': 'missing', 'attempt_message': '尚未请求资料'}
        if pending and row['attempt_status'] == 'creating':
            return public_batch(row)
        if pending and row['checked_at'] and now - row['checked_at'] < timedelta(seconds=5):
            return public_batch(row)
        if automatic and not pending and row:
            if row['attempt_status'] == 'failed' and not row.get('report_id'):
                result = public_batch(row)
                result['attempt_message'] = '创建未确认，自动采集不重发；请人工核对后使用补齐按钮'
                return result
            # 锁内重新检查人工/自动采集时间；每天最多申请一次，取消或失败也不高频重建。
            dates = [row.get(key) for key in ('requested_at', 'checked_at', 'data_at') if row.get(key)]
            if dates and now - max(dates) < timedelta(hours=24):
                return public_batch(row)
        if not pending and row and row['data_at'] and now - row['data_at'] < timedelta(hours=6):
            if kind == 'monthly':
                repo.rebuild_monthly_projection(row)
            return public_batch(row)
        if not pending and row and row['attempt_status'] == 'no_data' and row['requested_at'] and now - row['requested_at'] < timedelta(hours=6):
            result = public_batch(row)
            result['attempt_message'] = '最近请求未取得报告，6小时内复用取消状态以避免重复创建；金额仍未知，原成功资料保留'
            return result
        client = get_sp_api_client(shop_id)
        if client.marketplace_id != binding[1] or repo.scope(shop_id) != binding:
            raise ValueError('店铺范围在请求前发生变化，请刷新')
        if not pending:
            row = repo.reserve(binding, kind, period)
            try:
                kwargs = {} if not window else {'dataStartTime': window[0].isoformat(), 'dataEndTime': window[1].isoformat()}
                result = client.create_report(REPORT_TYPES[kind], marketplace_ids=[binding[1]], **kwargs)
                if not result.get('reportId'):
                    raise ValueError('创建报告没有返回ID')
                repo.state(row['id'], 'pending', 'Amazon正在生成报告，可稍后继续检查', result['reportId'])
            except Exception:
                repo.state(row['id'], 'failed', '报告创建未确认；请检查授权和服务日志，不会自动重发')
            return public_batch(repo.batch(binding, kind, period))
        repo.state(row['id'], 'pending', '正在检查Amazon报告')
        try:
            result = client.get_report(row['report_id'])
            if result.get('reportType') != REPORT_TYPES[kind] or binding[1] not in (result.get('marketplaceIds') or []):
                raise ValueError('报告类型或站点与请求不符')
            # 兼容此前次月零点上界的在途ID；收费文件仍逐行校验所属月份。
            valid_ends = {window[1], window[1] - timedelta(seconds=1)} if window else set()
            if window and (_report_time(result.get('dataStartTime')) != window[0] or _report_time(result.get('dataEndTime')) not in valid_ends):
                raise ValueError('报告日期范围与请求不符')
            status = result.get('processingStatus')
            if status == 'DONE':
                doc = client.get_report_document(result['reportDocumentId'])
                if not doc.get('url'):
                    raise ValueError('报告下载地址缺失')
                text = _download(doc['url'], doc.get('compressionAlgorithm'), proxies=client.proxies)
                rows = parse_report(text, kind, period, binding, all_months=kind == 'monthly')
                if repo.scope(shop_id) != binding:
                    raise ValueError('店铺范围已变化，未保存报告')
                if kind == 'monthly':
                    repo.save_monthly(row, rows)
                else:
                    repo.save(row, rows)
            elif status in ('CANCELLED', 'FATAL'):
                message = ('Amazon本次未返回收费明细（可能无可报告费用或重复申请）；不按0收费处理，近期账期由计划任务复查'
                           if status == 'CANCELLED' else 'Amazon本次生成未完成，近期账期由计划任务复查；不影响已获取月费和库存结果')
                if result.get('reportDocumentId'):
                    message = failure_diagnostic(client, result, message)
                repo.state(row['id'], 'no_data' if status == 'CANCELLED' else 'failed', message)
            elif status not in ('IN_QUEUE', 'IN_PROGRESS'):
                raise ValueError('报告状态未知')
        except ValueError as error:
            repo.state(row['id'], 'failed', str(error))
        except Exception:
            # 网络/限流暂时失败保留原reportId；下次只检查，不新建报告。
            repo.state(row['id'], 'pending', '本次检查未完成，可稍后继续；未重建报告，原资料保留')
        return public_batch(repo.batch(binding, kind, period))


def failure_diagnostic(client, result, fallback):
    """失败文档只提取安全错误分类，不把签名地址或完整响应写入资料/页面。"""
    try:
        doc = client.get_report_document(result['reportDocumentId'])
        content = _download(doc['url'], doc.get('compressionAlgorithm'), proxies=client.proxies)
        payload = json.loads(content)
        message = str(payload.get('errorDetails') or payload.get('message') or '').lower()
        if 'permission' in message or 'unauthorized' in message or 'access denied' in message:
            return 'Amazon报告授权不足，系统已定位为接口权限问题；月费和库存结果仍可查看'
        if 'parameter' in message or 'date range' in message:
            return 'Amazon未接受该账期请求参数，系统已记录日期范围问题；不把缺失收费当0'
        if 'no data' in message or 'no records' in message:
            return 'Amazon诊断文件说明本次没有可返回的收费记录；未计入额外收费'
    except Exception:
        pass
    return fallback
