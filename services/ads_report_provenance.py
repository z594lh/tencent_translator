"""广告报告来源证据：平台生成时间、已核实账户与入库内容，不保存下载URL或凭据。"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pymysql
from services.mysql_service import get_db_connection

TABLE = 'amazon_ads_report_imports'
CONTENT_ROW_LIMIT = 50000
# 保留语义字段；不使用本地updated_at冒充生成时间或内容版本。
CONTENT_FIELDS = ('row_hash', 'campaign_id', 'campaign_name', 'campaign_status', 'campaign_budget',
    'campaign_budget_type', 'ad_group_id', 'ad_group_name', 'ad_group_status', 'advertised_asin',
    'advertised_sku', 'purchased_asin', 'keyword_id', 'keyword_text', 'keyword_type', 'keyword_match_type',
    'targeting_id', 'targeting_expression', 'targeting_type', 'customer_search_term', 'placement',
    'currency', 'impressions', 'clicks', 'cost', 'purchases_7d', 'purchases_14d', 'purchases_30d',
    'sales_7d', 'sales_14d', 'sales_30d', 'top_of_search_impression_share')


class EvidenceError(ValueError):
    """只携带可公开的来源核验说明，不附HTTP响应或签名链接。"""


def fingerprint(rows):
    canonical = [[str(r[k]) if r.get(k) is not None else None for k in CONTENT_FIELDS]
                 for r in sorted(rows, key=lambda r: r['row_hash'])]
    return hashlib.sha256(json.dumps(canonical, ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()


def account_hash(shop):
    return hashlib.sha256(json.dumps([str(shop[k]) for k in ('id', 'credential_group_id', 'region',
        'marketplace_id', 'ads_profile_id', 'seller_id')], separators=(',', ':')).encode()).hexdigest()


def schema_ready(cur):
    try:
        cur.execute(f'SELECT id FROM {TABLE} LIMIT 0')
        cur.execute('SELECT report_import_id FROM amazon_ads_raw_reports LIMIT 0')
        return True
    except pymysql.err.ProgrammingError as error:
        if error.args[0] in (1146, 1054):
            return False
        raise


def load_shop(shop_id, cur=None, lock=False):
    if cur is None:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                return load_shop(shop_id, cursor)
        finally:
            conn.close()
    cur.execute('''SELECT id,credential_group_id,region,marketplace_id,ads_profile_id,seller_id
        FROM amazon_shops WHERE id=%s AND status=1''' + (' FOR UPDATE' if lock else ''), (shop_id,))
    row = cur.fetchone()
    if not row or not row['ads_profile_id']:
        raise EvidenceError('广告报告店铺已停用或缺少Profile，未写入报告')
    return row


def context(shop_id, client):
    """每轮下载最多一次Profiles读取；缺迁移时保持原采集，不额外调用API。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if not schema_ready(cur):
                return None
    finally:
        conn.close()
    shop = load_shop(shop_id)
    if str(client.profile_id) != str(shop['ads_profile_id']) or client.region != shop['region']:
        raise EvidenceError('广告客户端账户与当前店铺不一致，未下载报告')
    result = {'shop_id': shop['id'], 'credential_group_id': shop['credential_group_id'],
              'region': shop['region'], 'marketplace_id': shop['marketplace_id'],
              'profile_id': str(shop['ads_profile_id']), 'account_state': 'unknown',
              'account_identity_hash': account_hash(shop),
              'currency': None, 'account_timezone': None, 'profile_observed_at': None}
    try:
        profiles = client.list_profiles()
    except Exception as error:
        if getattr(getattr(error, 'response', None), 'status_code', None) == 429:
            raise EvidenceError('广告账户资料读取被限流，本轮停止下载，等待下一轮任务') from None
        # 账户资料暂不可用仍允许原始资料采集，但不可伪装成已核实来源。
        return result
    if not isinstance(profiles, list):
        return result
    matches = [p for p in profiles if isinstance(p, dict) and str(p.get('profileId')) == result['profile_id']]
    if len(matches) != 1:
        raise EvidenceError('广告Profile无法唯一匹配当前店铺，未写入报告')
    profile = matches[0]
    info = profile.get('accountInfo') or {}
    if (not isinstance(info, dict) or str(info.get('id')) != str(shop['seller_id'])
            or info.get('marketplaceStringId') != shop['marketplace_id']):
        raise EvidenceError('广告Profile真实卖家或站点与店铺不符，未写入报告')
    if str(info.get('type', '')).lower() != 'seller':
        return result
    currency, zone = profile.get('currencyCode'), profile.get('timezone')
    try:
        if not re.fullmatch('[A-Z]{3}', str(currency)):
            return result
        ZoneInfo(zone)
    except (ValueError, TypeError, ZoneInfoNotFoundError):
        return result
    result.update(account_state='verified', currency=currency, account_timezone=zone,
                  profile_observed_at=datetime.utcnow())
    return result


def instant(value):
    if not isinstance(value, str) or len(value) > 80:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else None
    except ValueError:
        return None


def metadata(status, report_id, cfg, report_type, day):
    """仅提取白名单证据。显式范围/配置冲突直接拒绝；缺失字段可采集但标未知。"""
    if not isinstance(status, dict):
        raise EvidenceError('报告状态结构异常，未写入报告')
    expected = {'adProduct': 'SPONSORED_PRODUCTS', 'reportTypeId': cfg.get('_reportTypeId', report_type),
                'timeUnit': 'DAILY', 'format': 'GZIP_JSON', 'groupBy': cfg['groupBy'], 'columns': cfg['columns']}
    complete = True
    for key, value in (('reportId', str(report_id)), ('startDate', str(day)), ('endDate', str(day)), ('status', 'COMPLETED')):
        if key not in status:
            complete = False
        elif str(status[key]) != value:
            raise EvidenceError('报告ID、状态或日期与请求不一致，未写入报告')
    configuration = status.get('configuration')
    if not isinstance(configuration, dict):
        complete = False
    else:
        for key, value in expected.items():
            if key not in configuration:
                complete = False
            elif isinstance(value, list) and (not isinstance(configuration[key], list)
                    or any(not isinstance(v, str) for v in configuration[key])):
                raise EvidenceError('报告指标或粒度列表结构异常，未写入报告')
            elif (sorted(configuration[key]) if isinstance(value, list) and isinstance(configuration[key], list)
                  else configuration[key]) != (sorted(value) if isinstance(value, list) else value):
                raise EvidenceError('报告粒度或指标配置与请求不一致，未写入报告')
        if configuration.get('filters'):
            raise EvidenceError('报告包含非预期筛选范围，未写入报告')
    created, generated = instant(status.get('createdAt')), instant(status.get('generatedAt'))
    if generated and (generated > datetime.utcnow() + timedelta(minutes=5) or (created and generated < created)):
        generated = None
    return {'metadata_state': 'verified' if complete else 'unknown', 'platform_created_at': created,
            'platform_generated_at': generated,
            'configuration_hash': hashlib.sha256(json.dumps(expected, sort_keys=True).encode()).hexdigest()}


def begin_import(cur, ctx, meta, report_type, day, report_id, source_count, normalized_count):
    current = load_shop(ctx['shop_id'], cur, lock=True)
    if account_hash(current) != ctx['account_identity_hash'] or any(str(current[k]) != str(ctx[v]) for k, v in (
        ('credential_group_id', 'credential_group_id'), ('region', 'region'),
        ('marketplace_id', 'marketplace_id'), ('ads_profile_id', 'profile_id'))):
        raise EvidenceError('资料下载期间账户发生变化，未写入报告')
    cur.execute(f'''INSERT INTO {TABLE}
        (shop_id,credential_group_id,region,marketplace_id,profile_id,account_identity_hash,currency,account_timezone,
         account_state,profile_observed_at,report_type,report_date,report_id,metadata_state,
         platform_created_at,platform_generated_at,configuration_hash,source_row_count,normalized_row_count,
         stored_row_count,content_hash,coherent,imported_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,'',0,%s)''',
        (ctx['shop_id'], ctx['credential_group_id'], ctx['region'], ctx['marketplace_id'], ctx['profile_id'],
         ctx['account_identity_hash'], ctx['currency'], ctx['account_timezone'], ctx['account_state'], ctx['profile_observed_at'], report_type, day,
         str(report_id), meta['metadata_state'], meta['platform_created_at'], meta['platform_generated_at'],
         meta['configuration_hash'], source_count, normalized_count, datetime.utcnow()))
    return cur.lastrowid


def finish_import(cur, import_id, shop, kind, day, expected_count):
    cur.execute('SELECT COUNT(*) n FROM amazon_ads_raw_reports WHERE shop_id=%s AND report_type=%s AND report_date=%s',
                (shop, kind, day))
    count = cur.fetchone()['n']
    if count > CONTENT_ROW_LIMIT:
        cur.execute(f'UPDATE {TABLE} SET stored_row_count=%s,content_hash=%s,coherent=0 WHERE id=%s',
                    (count, '', import_id))
        return
    cur.execute('SELECT ' + ','.join(CONTENT_FIELDS) + ''',report_import_id FROM amazon_ads_raw_reports
        WHERE shop_id=%s AND report_type=%s AND report_date=%s ORDER BY row_hash''', (shop, kind, day))
    rows = cur.fetchall()
    coherent = len(rows) == expected_count and all(r['report_import_id'] == import_id for r in rows)
    cur.execute(f'UPDATE {TABLE} SET stored_row_count=%s,content_hash=%s,coherent=%s WHERE id=%s',
                (len(rows), fingerprint(rows), int(coherent), import_id))


def import_report(shop_id, kind, day, report_id, rows, writer, force_refresh=False, evidence=None, source_count=None):
    """同一短事务保存数据、来源和缓存代次；外部状态查询/下载均由调用方提前完成。"""
    conn = get_db_connection()
    try:
        conn.begin()
        with conn.cursor() as cur:
            import_id = None
            if evidence:
                # 与其他业务保持店铺→后续实体的锁序。
                import_id = begin_import(cur, evidence['context'], evidence['metadata'], kind,
                    day, report_id, source_count if source_count is not None else len(rows), len(rows))
            if report_id:
                cur.execute('''SELECT report_id FROM amazon_ads_report_cache
                    WHERE shop_id=%s AND report_type=%s AND report_date=%s FOR UPDATE''', (shop_id, kind, day))
                cached = cur.fetchone()
                if not cached or cached['report_id'] != str(report_id):
                    raise EvidenceError('该报告缓存已被替换，请等待当前报告；未覆盖新报告或数据')
            if force_refresh:
                cur.execute('DELETE FROM amazon_ads_raw_reports WHERE shop_id=%s AND report_date=%s AND report_type=%s',
                            (shop_id, day, kind))
            result = writer(cur, shop_id, kind, rows, report_import_id=import_id,
                currency=evidence['context']['currency'] if evidence else None,
                provenance_enabled=True if evidence else None)
            if evidence:
                finish_import(cur, import_id, shop_id, kind, day, len(rows))
            if report_id:
                cur.execute('''UPDATE amazon_ads_report_cache SET status='COMPLETED',insert_flag=1,updated_at=NOW()
                    WHERE shop_id=%s AND report_type=%s AND report_date=%s AND report_id=%s''',
                    (shop_id, kind, day, str(report_id)))
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def failure_hint(error):
    if isinstance(error, EvidenceError):
        return str(error)
    status = getattr(getattr(error, 'response', None), 'status_code', None)
    code = error.args[0] if isinstance(error, pymysql.MySQLError) and error.args and isinstance(error.args[0], int) else None
    suffix = f'，HTTP {status}' if isinstance(status, int) else f'，数据库 {code}' if code is not None else ''
    return f'报告处理失败（{type(error).__name__}{suffix}），本地事务未确认成功；请核对结构与连接，不自动重试'


def read_sources(cur, shop, start, end):
    """同一只读事务内核对实际内容，防止旧代码或人工改值仍沿用过时来源证明。"""
    if not schema_ready(cur):
        return {}
    cur.execute(f'''SELECT MAX(id) id FROM {TABLE} WHERE shop_id=%s AND report_date BETWEEN %s AND %s
        GROUP BY report_type,report_date''', (shop['id'], start, end))
    ids = [r['id'] for r in cur.fetchall()]
    if not ids:
        return {}
    # 分两次读取也兼容MySQL5.7临时影子表，不在同一语句重复打开表。
    cur.execute(f'SELECT * FROM {TABLE} WHERE id IN (' + ','.join(['%s']*len(ids)) + ')', ids)
    evidence = cur.fetchall()
    current_hash = account_hash(load_shop(shop['id'], cur))
    cur.execute('''SELECT report_type,report_date,report_id,status FROM amazon_ads_report_cache
        WHERE shop_id=%s AND report_date BETWEEN %s AND %s''', (shop['id'], start, end))
    cache = {(r['report_type'], r['report_date']): r for r in cur.fetchall()}
    cur.execute('SELECT COUNT(*) n FROM amazon_ads_raw_reports WHERE shop_id=%s AND report_date BETWEEN %s AND %s',
                (shop['id'], start, end))
    limit_exceeded = cur.fetchone()['n'] > CONTENT_ROW_LIMIT
    rows = []
    if not limit_exceeded:
        cur.execute('SELECT ' + ','.join(CONTENT_FIELDS) + ''',report_type,report_date,report_import_id
            FROM amazon_ads_raw_reports WHERE shop_id=%s AND report_date BETWEEN %s AND %s LIMIT %s''',
                    (shop['id'], start, end, CONTENT_ROW_LIMIT+1))
        rows = cur.fetchall()
        limit_exceeded = len(rows) > CONTENT_ROW_LIMIT
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row['report_type'], row['report_date'])].append(row)
    result = defaultdict(list)
    for item in evidence:
        records = grouped[(item['report_type'], item['report_date'])]
        identity_ok = item['account_identity_hash'] == current_hash and all(str(item[k]) == str(shop[v]) for k, v in (
            ('credential_group_id', 'credential_group_id'), ('region', 'region'),
            ('marketplace_id', 'marketplace_id'), ('profile_id', 'ads_profile_id')))
        content_ok = (not limit_exceeded and item['coherent'] == 1 and len(records) == item['stored_row_count']
                      and all(r['report_import_id'] == item['id'] for r in records)
                      and fingerprint(records) == item['content_hash'])
        cached = cache.get((item['report_type'], item['report_date']))
        if cached and cached['status'] == 'COMPLETED' and cached['report_id'] != item['report_id']:
            content_ok = False
        verified = identity_ok and content_ok and item['account_state'] == 'verified' and item['metadata_state'] == 'verified'
        generation = 'unknown'
        if verified and item['platform_generated_at'] and item['account_timezone']:
            try:
                # 额外48小时是本系统保守缓冲，不是Amazon最终结算承诺。
                cutoff = datetime.combine(item['report_date'] + timedelta(days=10), datetime.min.time(),
                                          ZoneInfo(item['account_timezone'])).astimezone(timezone.utc).replace(tzinfo=None)
                generation = 'after_window' if item['platform_generated_at'] >= cutoff else 'within_window'
            except (ValueError, ZoneInfoNotFoundError):
                pass
        result[item['report_type']].append({'id': str(item['id']), 'date': item['report_date'],
            'state': 'verified' if verified else 'unknown', 'content_matches': content_ok,
            'content_check': 'row_limit' if limit_exceeded else 'match' if content_ok else 'mismatch',
            'account_matches': identity_ok, 'generation': generation, 'currency': item['currency'],
            'timezone': item['account_timezone'], 'generated_at': item['platform_generated_at'],
            'imported_at': item['imported_at'], 'report_id': item['report_id']})
    return dict(result)
