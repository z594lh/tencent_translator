"""只记录ERP认证请求发起的Listing写操作，历史与平台生效状态严格分开。"""
import copy
import json
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pymysql
import requests
from flask import has_request_context, request

from services.mysql_service import get_db_connection

TABLE = 'amazon_listing_change_history'
OPERATIONS = {'price', 'text', 'images', 'variants', 'create', 'update', 'delete'}
STATUSES = {'submitting', 'accepted', 'rejected', 'unknown'}
LABELS = {'item_name': '商品标题', 'title_differentiation': '商品亮点', 'product_description': '商品描述',
          'bullet_point': '五点描述', 'generic_keyword': '搜索关键字', 'main_product_image_locator': '主图',
          'purchasable_offer': '报价配置', 'our_price': '普通价', 'discounted_price': '促销价',
          'minimum_seller_allowed_price': '最低允许价', 'maximum_seller_allowed_price': '最高允许价', 'map_price': '最低广告价MAP'}
LABELS.update({f'other_product_image_locator_{i}': f'副图 {i}' for i in range(1, 9)})


class HistoryError(Exception):
    def __init__(self, message, code=503):
        super().__init__(message)
        self.code = code
        self.details = {'write_state': 'rejected', 'not_sent': True}


def dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str, separators=(',', ':'))


def safe(value):
    """仅保留商品数据，防止通用属性混入认证键或图片签名查询参数。"""
    if isinstance(value, dict):
        return {str(key): ('[已隐藏]' if any(part in str(key).lower() for part in ('token', 'secret', 'password', 'authorization', 'credential')) else safe(item)) for key, item in value.items()}
    if isinstance(value, list): return [safe(item) for item in value]
    if isinstance(value, str) and value.startswith(('https://', 'http://')):
        parts = urlsplit(value)
        query = [(key, item) for key, item in parse_qsl(parts.query, keep_blank_values=True)
                 if not any(part in key.lower() for part in ('token', 'secret', 'password', 'credential', 'signature', 'accesskey', 'x-amz-', 'x-oss-'))]
        return urlunsplit((parts.scheme,parts.netloc,parts.path,urlencode(query),parts.fragment)) if len(query) != len(parse_qsl(parts.query, keep_blank_values=True)) else value
    return value


def safe_issues(rows):
    if not isinstance(rows, list): return []
    return [safe({key: str(row[key])[:1500] if key != 'attributeNames' else row[key] for key in ('code', 'message', 'severity', 'attributeNames') if key in row}) for row in rows[:50] if isinstance(row, dict)]


def identity(shop, client):
    ctx = {'shop_id': int(shop['id']), 'credential_group_id': int(shop['credential_group_id']),
           'region': str(shop['region']).lower(), 'marketplace_id': shop['marketplace_id']}
    if (client.region, client.marketplace_id, client.seller_id) != (ctx['region'], ctx['marketplace_id'], shop['seller_id']):
        raise HistoryError('店铺、凭证区域或站点不一致，未向Amazon提交', 409)
    return ctx


def cached_before(ctx, sku):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT attributes_json FROM amazon_listings WHERE shop_id=%s AND sku=%s AND marketplace_id=%s LIMIT 1',
                           (ctx['shop_id'], sku, ctx['marketplace_id']))
            row = cursor.fetchone()
        if row:
            try: attrs = json.loads(row['attributes_json'] or '{}')
            except (ValueError, TypeError): return {}, 'unavailable'
            if isinstance(attrs, dict): return attrs, 'local_cache'
        return {}, 'unavailable'
    finally: connection.close()


def current_rows(value, market):
    if isinstance(value, list):
        return [copy.deepcopy(row) for row in value if not isinstance(row, dict) or row.get('marketplace_id', market) == market]
    return copy.deepcopy(value)


def changes_for(method, body, attrs, market, known):
    changes = {}
    patches = body.get('patches') or [] if method == 'PATCH' else [
        {'op': 'replace', 'path': '/attributes/' + key, 'value': value} for key, value in (body.get('attributes') or {}).items()]
    if method == 'PUT':
        # 完全覆盖同时记录可能省略的旧属性，标注为“本次未提供”，不猜测平台最终清除行为。
        patches += [{'op': 'omitted', 'path': '/attributes/' + key} for key in attrs if key not in (body.get('attributes') or {})]
    if method == 'DELETE':
        return [{'field': '__listing__', 'label': '删除Listing', 'kind': 'json', 'before': {'存在': True} if known else None,
                 'before_known': known, 'after': '请求删除Listing', 'action': 'delete'}]
    for patch in patches:
        path = patch.get('path', '')
        if not path.startswith('/attributes/'): continue
        name = path.split('/')[2].replace('~1', '/').replace('~0', '~')
        value = patch.get('value')
        if name == 'purchasable_offer' and patch.get('op') == 'merge' and path == '/attributes/' + name and isinstance(value, list):
            for offer in value:
                if not isinstance(offer, dict): continue
                previous = [row for row in attrs.get(name, []) if isinstance(row, dict) and row.get('marketplace_id') == offer.get('marketplace_id') and row.get('currency') == offer.get('currency') and row.get('audience', 'ALL') == offer.get('audience', 'ALL')]
                for key, target in offer.items():
                    if key in ('marketplace_id', 'currency', 'audience'): continue
                    field = name + '.' + key
                    changes[field] = {'field': field, 'label': LABELS.get(key, key), 'kind': 'price',
                                      'before': previous[0].get(key) if len(previous) == 1 else None,
                                      'before_known': known and len(previous) == 1, 'after': target,
                                      'currency': offer.get('currency'), 'action': 'clear' if target is None else 'set'}
            continue
        submitted_ops = changes.get(path, {}).get('submitted_operations', []) + [{'op':patch.get('op'),'path':path,'value':value}]
        changes[path] = {'field': name, 'label': LABELS.get(name, name), 'submitted_operations': submitted_ops,
                         'kind': 'image' if 'product_image_locator' in name else 'text' if name in ('item_name', 'title_differentiation', 'product_description', 'bullet_point', 'generic_keyword') else 'json',
                         'before': current_rows(attrs.get(name), market), 'before_known': known,
                         'after': None if patch.get('op') == 'delete' else value,
                         'action': patch.get('op'), 'path': path}
    return safe(list(changes.values()))


def begin(ctx, sku, method, body, snapshot):
    user = request.current_user
    operation = {'submit_listing_pricing': 'price', 'listing_text_editor': 'text', 'listing_image_editor': 'images',
                 'sync_fields_to_variants': 'variants', 'create_listing': 'create', 'delete_listing': 'delete'}.get(request.endpoint.rsplit('.', 1)[-1], 'update')
    attrs, source = snapshot if snapshot else cached_before(ctx, sku)
    changes = changes_for(method, body, attrs, ctx['marketplace_id'], source != 'unavailable')
    encoded = dumps(changes)
    if len(encoded.encode('utf-8')) > 2 * 1024 * 1024:
        raise HistoryError('本次修改记录过大，未向Amazon提交，请分批修改', 400)
    source_sku = (request.view_args or {}).get('sku') if operation == 'variants' else None
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('''INSERT INTO amazon_listing_change_history
                (operation_id,shop_id,credential_group_id,region,marketplace_id,sku,source_sku,operation_type,
                 actor_user_id,actor_name,http_method,before_source,change_summary,changes_json,requested_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(3))''',
                (uuid.uuid4().hex,ctx['shop_id'],ctx['credential_group_id'],ctx['region'],ctx['marketplace_id'],sku,source_sku,operation,
                 user['id'],str(user.get('nickname') or user.get('username') or f"用户#{user['id']}")[:255],method,source,
                 '、'.join(dict.fromkeys(change['label'] for change in changes))[:1000] or 'Listing修改',encoded))
            row_id = cursor.lastrowid
        connection.commit()
        return row_id
    except Exception:
        connection.rollback(); raise
    finally: connection.close()


def classify(result=None, error=None):
    if error is not None:
        response = getattr(error, 'response', None)
        rejected = response is not None and response.status_code in (400, 401, 403, 404, 413, 415, 422, 429)
        return ('rejected' if rejected else 'unknown'), None, [], ('Amazon明确拒绝请求' if rejected else '未取得可靠回执，请勿重复提交')
    if not isinstance(result, dict): return 'unknown', None, [], '平台返回结构无法核实'
    issues = safe_issues(result.get('issues'))
    submission = str(result.get('submissionId') or '')[:128] or None
    if result.get('status') == 'INVALID' or any(row.get('severity') == 'ERROR' for row in issues):
        return 'rejected', submission, issues, '平台未通过本次提交校验'
    if result.get('status') == 'ACCEPTED' and submission: return 'accepted', submission, issues, None
    return 'unknown', submission, issues, '未取得可靠接受回执，请只读核对'


def finish(row_id, result=None, error=None):
    state, submission, issues, message = classify(result, error)
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute('''UPDATE amazon_listing_change_history SET write_status=%s,submission_id=%s,issues_json=%s,
                error_message=%s,responded_at=UTC_TIMESTAMP(3) WHERE id=%s AND write_status='submitting' ''',
                (state,submission,dumps(issues),message,row_id))
        connection.commit()
    except Exception:
        connection.rollback(); raise
    finally: connection.close()


def instrument(client, shop):
    """仅认证Listing网页请求启用；客户端只包装一次，不影响cron或其他SP接口。"""
    if (not has_request_context() or not getattr(request, 'current_user', None) or
            not (request.endpoint or '').startswith('amazon_listing.') or getattr(client, '_listing_history_instrumented', False) is True):
        return client
    ctx = identity(shop, client)
    original = client._request
    prefix = '/listings/2021-08-01/items/' + client.seller_id + '/'
    snapshots = {}
    def audited(method, path, *args, **kwargs):
        if not path.startswith(prefix) or not path[len(prefix):]: return original(method, path, *args, **kwargs)
        sku = path[len(prefix):]
        if method not in ('PATCH', 'PUT', 'DELETE'):
            result = original(method, path, *args, **kwargs)
            if method == 'GET' and isinstance(result, dict) and result.get('sku') == sku and isinstance(result.get('attributes'), dict):
                snapshots[sku] = (copy.deepcopy(result['attributes']), 'platform_read')
            return result
        identity(shop, client)
        body = kwargs.get('json_data') or {}
        try: row_id = begin(ctx, sku, method, body, snapshots.get(sku))
        except HistoryError: raise
        except Exception:
            raise HistoryError('修改记录表未初始化或不可写，未向Amazon提交；请先执行Listing历史迁移SQL') from None
        try: result = original(method, path, *args, **kwargs)
        except Exception as exc:
            try: finish(row_id, error=exc)
            except Exception: pass
            raise
        warning = None
        try: finish(row_id, result=result)
        except Exception:
            warning = 'Amazon已返回回执，但修改历史回执保存失败；原操作意图已保留，请勿重复提交'
        if isinstance(result, dict):
            result = {**result, 'erp_history_id': row_id}
            if warning: result['history_warning'] = warning
        return result
    client._request = audited
    client._listing_history_instrumented = True
    return client


def confirm(ctx, result, history_id=None):
    """只更新当前已验签回执的原操作，不调用Amazon，不插入历史。"""
    if not has_request_context() or not getattr(request, 'current_user', None) or not result.get('submission_id') or not history_id: return result
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute('''UPDATE amazon_listing_change_history SET write_status='accepted',submission_id=%s,
                verification_status=%s,verification_json=%s,verified_at=UTC_TIMESTAMP(3)
                WHERE shop_id=%s AND credential_group_id=%s AND region=%s AND marketplace_id=%s AND sku=%s AND id=%s
                AND (submission_id=%s AND write_status='accepted' OR submission_id IS NULL AND write_status='submitting') ''',
                (result['submission_id'],result['state'],dumps(safe({'matched_fields': result.get('matched_fields'), 'positions': result.get('positions'), 'issues': safe_issues(result.get('issues'))})),
                 ctx['shop_id'],ctx['credential_group_id'],ctx['region'],ctx['marketplace_id'],ctx['sku'],history_id,result['submission_id']))
        connection.commit()
    except Exception:
        if connection: connection.rollback()
        result['history_warning'] = '平台核对已完成，但修改历史核对状态保存失败；请勿重新提交'
    finally:
        if connection: connection.close()
    return result


def utc_iso(value):
    return value.replace(tzinfo=timezone.utc).isoformat() if isinstance(value, datetime) else value


def query(shop_id, sku, params, row_id=None):
    from services.shop_service import get_shop_by_id
    shop = get_shop_by_id(shop_id)
    if not shop: raise HistoryError('店铺不存在或已停用', 404)
    if not sku or len(sku) > 255: raise HistoryError('SKU无效', 400)
    filters = ['shop_id=%s', 'sku=%s', 'marketplace_id=%s']
    values = [shop_id,sku,shop['marketplace_id']]
    if row_id is not None:
        filters.append('id=%s'); values.append(row_id)
    for name, choices in (('operation_type', OPERATIONS), ('write_status', STATUSES)):
        value = params.get(name)
        if value:
            if value not in choices: raise HistoryError('历史筛选条件无效', 400)
            filters.append(name + '=%s'); values.append(value)
    try: page, size = int(params.get('page', 1)), int(params.get('page_size', 20))
    except (ValueError, TypeError): raise HistoryError('分页参数无效', 400) from None
    if page < 1 or page > 10000 or not 1 <= size <= 50: raise HistoryError('分页超出范围', 400)
    where = ' AND '.join(filters)
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            if row_id is not None:
                cursor.execute('SELECT * FROM ' + TABLE + ' WHERE ' + where, values)
                row = cursor.fetchone()
                if not row: raise HistoryError('修改记录不存在或不属于当前店铺/SKU', 404)
                row['changes'] = json.loads(row.pop('changes_json'))
                row['issues'] = json.loads(row.pop('issues_json') or '[]')
                row['verification'] = json.loads(row.pop('verification_json') or '{}')
                for key in ('requested_at','responded_at','verified_at'): row[key] = utc_iso(row[key])
                return row
            cursor.execute('SELECT COUNT(*) AS total FROM ' + TABLE + ' WHERE ' + where, values)
            total = cursor.fetchone()['total']
            cursor.execute('''SELECT id,operation_type,actor_name,source_sku,before_source,change_summary,write_status,
                verification_status,submission_id,error_message,requested_at,responded_at,verified_at FROM ''' + TABLE + ' WHERE ' + where + ' ORDER BY id DESC LIMIT %s OFFSET %s', values + [size,(page-1)*size])
            rows = cursor.fetchall()
            for row in rows:
                for key in ('requested_at','responded_at','verified_at'): row[key] = utc_iso(row[key])
            return {'list': rows, 'total': total, 'page': page, 'page_size': size}
    except pymysql.err.ProgrammingError as exc:
        if exc.args and exc.args[0] == 1146: raise HistoryError('Listing修改记录表尚未初始化，请先执行迁移SQL') from None
        raise
    finally: connection.close()
