"""Listing归属业务入口：查询、签名预览、提交、负责历史与操作历史。

由blueprints/amazon/listing_ownership.py调用，SQL交给ownership_repository。
preview只读并签名冻结命令；apply以事务写负责区间、身份版本及审计。
人员查询实时读取历史归属，交接不再触发SKU/店铺利润重建或生成人员快照。
交接默认日期遵循固定北京时间15:00：2026-10-26 14:00→27日，15:00→28日；
15点前日期+1，15点整起+2，具体规则见assignment_policy；不改变过去负责区间。
预览15分钟有效，跨分界须重新预览；提交不能另传可编辑字段。
不调用Amazon、不改Listing内容、不自动授权用户；失败整笔回滚。
"""
import hashlib
import re
import uuid
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from services.mysql_service import get_db_connection
from services.listing_performance.configuration import value as configured_value
from services.listing_performance import ownership_repository as repo
from services.listing_performance.common import (
    DomainError, positive_id, parse_date, market_config, utc_now, effective_time,
    json_data, decode, identity, next_business_date,
)


def serializer():
    """创建归属预览的带时效签名器并返回。

    使用配置OWNERSHIP_PREVIEW_SECRET和固定salt隔离用途；密钥不足32字符报503。
    不生成/输出密钥，不接受前端提供的签名密钥。"""
    key = configured_value('OWNERSHIP_PREVIEW_SECRET')
    if len(key) < 32:
        raise DomainError('未配置至少 32 字符的 OWNERSHIP_PREVIEW_SECRET', 503)
    return URLSafeTimedSerializer(key, salt='listing-ownership-v1')


def assignment_list(actor, args):
    """验证分配列表筛选并返回分页数据。

    actor仅用于我负责的筛选；args包含店铺/分页/SKU/关键词/负责人/是否分配。
    冲突筛选、页大小超过100或无有效店铺时报错；只读并负责关闭连接。"""
    shop = positive_id(args.get('shop_id'),'shop_id')
    page = positive_id(args.get('page',1),'page')
    page_size = positive_id(args.get('page_size',20),'page_size')
    if page_size > 100:
        raise DomainError('page_size 不能超过 100')
    chosen = args.get('owner_user_id')
    mine, unassigned = args.get('only_mine') == '1', args.get('unassigned') == '1'
    status = args.get('assignment_status', 'all')
    if status not in ('all', 'assigned', 'unassigned'):
        raise DomainError('assignment_status 必须为 all、assigned 或 unassigned')
    if status == 'assigned' and unassigned:
        raise DomainError('已分配与未分配筛选不能同时选择')
    unassigned = unassigned or status == 'unassigned'
    if sum((bool(chosen),mine,unassigned)) > 1:
        raise DomainError('负责人筛选只能选择一种')
    owner = actor if mine else positive_id(chosen,'owner_user_id') if chosen else None
    sku, keyword = args.get('sku','').strip(), args.get('keyword','').strip()
    if len(sku) > 255 or len(keyword) > 255:
        raise DomainError('搜索条件不能超过 255 字符')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT id FROM amazon_shops WHERE id=%s AND status=1',(shop,))
            if not cursor.fetchone():
                raise DomainError('店铺不存在或未启用')
            return repo.assignment_list(cursor,shop,sku,keyword,owner,unassigned,page,page_size,
                                        assigned=status == 'assigned')
    finally:
        conn.close()


def normalize(payload):
    """校验预览payload并生成可签名的标准命令字典。

    items为1~200个店铺/站点/SKU；action为assign/cancel，reason允许空。
    未填日期按北京时间15点政策冻结最早交接日；owner_user_id=None代表解除分配。
    只做参数/配置处理，不写历史，也不将前端任意字段带入签名。"""
    if not isinstance(payload, dict):
        raise DomainError('请求必须为 JSON 对象')
    items = payload.get('items')
    if not isinstance(items, list) or not 1 <= len(items) <= 200:
        raise DomainError('items 必须包含 1~200 个对象')
    result = []
    for item in items:
        if not isinstance(item, dict):
            raise DomainError('items 中每项必须为对象')
        sku, market = item.get('seller_sku'), item.get('marketplace_id')
        if not isinstance(sku, str) or not sku.strip() or len(sku) > 255:
            raise DomainError('seller_sku 无效')
        if not isinstance(market, str) or not market or len(market) > 32:
            raise DomainError('marketplace_id 无效')
        result.append({'shop_id': positive_id(item.get('shop_id'), 'shop_id'),
                       'marketplace_id': market, 'seller_sku': sku})
    action = payload.get('action', 'assign')
    if action not in ('assign', 'cancel'):
        raise DomainError('action 无效')
    reason = payload.get('reason', '')
    if not isinstance(reason, str) or len(reason) > 500:
        raise DomainError('变更理由不能超过 500 字')
    owner = payload.get('owner_user_id')
    supplied_date = payload.get('effective_date')
    effective_date = parse_date(supplied_date).isoformat() if action == 'assign' and supplied_date else None
    # 默认日期也必须写入签名命令；提交时仍校验15点分界，不能跨界沿用旧日期。
    now = utc_now()
    default_dates = ({market:next_business_date(market_config(market)['timezone'],now)
                      for market in {i['marketplace_id'] for i in result}}
                     if action == 'assign' and effective_date is None else {})
    return {'items': result, 'action': action, 'reason': reason.strip(),
            'owner_user_id': positive_id(owner, 'owner_user_id') if owner is not None else None,
            'effective_date': effective_date, 'effective_dates':default_dates,
            'expand_parents': payload.get('expand_parents') is True}


def owner_valid(cursor, owner):
    """校验非空owner对应启用用户，无效时报业务异常。

    None允许解除分配；使用调用方cursor只读，不给用户新增权限。"""
    if owner is not None:
        cursor.execute('SELECT id FROM users WHERE id=%s AND status=1', (owner,))
        if not cursor.fetchone():
            raise DomainError('负责人不存在或已停用')


def inspect(cursor, data, now, lock=False):
    """解析命令data并返回附带归属/版本/站点信息的实际子SKU列表。

    校验店铺站点和交接日期，拒绝未取消的未来交接或无交接可取消。
    lock=False供只读预览；lock=True按店铺ID排序加锁，再锁/补建身份与历史。
    先锁店铺再锁SKU身份，避免并发首次创建同一身份；由apply负责整个事务，不在此提交。"""
    if lock:
        # 按店铺→身份顺序加锁，首次创建身份也受同一店铺锁保护。
        for shop_id in sorted({i['shop_id'] for i in data['items']}):
            cursor.execute('SELECT id FROM amazon_shops WHERE id=%s FOR UPDATE',(shop_id,))
            if not cursor.fetchone():
                raise DomainError('店铺不存在')
    owner_valid(cursor, data['owner_user_id'])
    items = repo.resolve(cursor, data['items'], data['expand_parents'])
    for item in items:
        config = market_config(item['marketplace_id'])
        cursor.execute('SELECT id, marketplace_id, region, credential_group_id FROM amazon_shops WHERE id=%s AND status=1',
                       (item['shop_id'],))
        shop = cursor.fetchone()
        if not shop or shop['marketplace_id'] != item['marketplace_id']:
            raise DomainError('店铺/站点不匹配或店铺已停用')
        item['timezone'] = config['timezone']
        item['region'] = shop['region']
        item['credential_group_id'] = shop['credential_group_id']
        item['effective_date'] = data['effective_date'] or data.get('effective_dates',{}).get(item['marketplace_id'])
        item['effective_from_utc'] = (effective_time(parse_date(item['effective_date']), config['timezone'], now)
                                      if data['action'] == 'assign' else None)
        scope = repo.scope(cursor, item, lock)
        item['scope_id'] = scope['id'] if scope else None
        item['version'] = scope['version'] if scope else 0
        history = repo.histories(cursor, scope['id'], lock) if scope else []
        item['current'] = repo.active_at(history, now)
        future = [r for r in history if r['effective_from_utc'] > now]
        item['scheduled'] = future[0] if future else None
        if data['action'] == 'assign' and future:
            raise DomainError('存在未来交接，请先取消再重新预览', 409)
        if data['action'] == 'cancel' and not future:
            raise DomainError('没有可取消的未来交接', 409)
    return items


def fingerprint(items):
    """提取实际子SKU的身份键、归属版本及时区列表。

    用于预览后并发变化核验，顺序沿用已排序的子SKU；纯函数，不写入。"""
    return [{k: i[k] for k in ('identity_key', 'version', 'timezone')} for i in items]


def preview(actor, payload):
    """为actor生成只读影响范围预览和15分钟有效的preview_id。

    payload先标准化再查真实子SKU；签名冻结actor、命令和版本指纹。
    返回preview_id/items/expires_in/action，不写scopes、历史或审计；连接始终关闭。"""
    data = normalize(payload)
    signer = serializer()
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            items = inspect(cursor, data, utc_now())
        token = signer.dumps({'actor': actor, 'data': data, 'versions': fingerprint(items)})
        return {'preview_id': token, 'items': items, 'expires_in': 900, 'action': data['action']}
    finally:
        conn.close()


def apply(actor, payload, action='assign'):
    """提交签名命令，返回affected/action/request_id。

    payload仅接受preview_id和8~64位request_id；actor/action必须与签名一致。
    先以操作者+请求命名锁防重复，再核验哈希、15分钟有效期和归属版本。
    assign关闭旧区间并新增未来区间（None为解除）；cancel作废未来区间并恢复前一区间。
    身份版本、负责区间和审计同事务提交；异常回滚，最后释放命名锁/连接。
    同请求同内容返回原审计结果，不重复交接；冲突需重新预览，不在此立即重算利润。"""
    if not isinstance(payload, dict):
        raise DomainError('请求必须为 JSON 对象')
    request_id = payload.get('request_id')
    token = payload.get('preview_id')
    if not isinstance(request_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,64}',request_id) or not isinstance(token, str):
        raise DomainError('需要 request_id 和 preview_id')
    # 签名令牌就是完整不可变命令；提交拒绝额外编辑字段，修改后必须重新预览。
    if set(payload) - {'request_id', 'preview_id'}:
        raise DomainError('提交只接受 request_id 和 preview_id；修改参数须重新预览')
    request_hash = hashlib.sha256((action + token).encode()).hexdigest()
    conn = get_db_connection()
    locked = False
    lock_name = 'own:' + hashlib.sha256(f'{actor}:{request_id}'.encode()).hexdigest()[:56]
    try:
        with conn.cursor() as cursor:
            cursor.execute('SELECT GET_LOCK(%s, 2) AS acquired', (lock_name,))
            locked = cursor.fetchone()['acquired'] == 1
            if not locked:
                raise DomainError('同一请求正在执行，请稍后重试', 409)
            existing = repo.audit_result(cursor, actor, request_id)
            if existing:
                if existing['request_hash'] != request_hash:
                    raise DomainError('同一请求 ID 不能用于不同内容', 409)
                return decode(existing['result_json'])
            try:
                command = serializer().loads(token, max_age=900)
            except (BadSignature, SignatureExpired):
                raise DomainError('预览已过期或被修改，请重新预览', 409) from None
            if command['actor'] != actor or command['data']['action'] != action:
                raise DomainError('预览用户或操作不匹配', 403)
            now = utc_now()
            items = inspect(cursor, command['data'], now, lock=True)
            if fingerprint(items) != command['versions']:
                raise DomainError('预览后对象、时区或负责人已变化，请重新预览', 409)
            result = write_items(cursor, items, command['data'], actor, request_id, request_hash, now)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if locked:
            with conn.cursor() as cursor:
                cursor.execute('SELECT RELEASE_LOCK(%s)', (lock_name,))
        conn.close()


def write_items(cursor, items, data, actor, request_id, request_hash, now):
    """在调用方事务内写负责区间、身份版本与审计，人工和托管交接共用。"""
    action = data['action']
    before = [dict(item) for item in items]
    for item in items:
        if action == 'assign':
            cursor.execute('''UPDATE listing_responsibility_history SET effective_to_utc=%s
                WHERE scope_id=%s AND voided_at IS NULL AND effective_to_utc IS NULL''',
                           (item['effective_from_utc'], item['scope_id']))
            cursor.execute('''INSERT INTO listing_responsibility_history
                (scope_id,owner_user_id,effective_from_utc,business_timezone,created_by,reason)
                VALUES (%s,%s,%s,%s,%s,%s)''',
                           (item['scope_id'], data['owner_user_id'], item['effective_from_utc'],
                            item['timezone'], actor, data['reason']))
        else:
            future = item['scheduled']
            cursor.execute('UPDATE listing_responsibility_history SET voided_at=%s WHERE id=%s', (now, future['id']))
            cursor.execute('''UPDATE listing_responsibility_history SET effective_to_utc=NULL
                WHERE scope_id=%s AND voided_at IS NULL AND effective_to_utc=%s''',
                           (item['scope_id'], future['effective_from_utc']))
        cursor.execute('UPDATE listing_responsibility_scopes SET version=version+1 WHERE id=%s', (item['scope_id'],))
    result = {'affected': len(items), 'action': action, 'request_id': request_id}
    repo.audit(cursor, actor, request_id, request_hash, action, before, data, result, data['reason'])
    return result


def ensure_ai_owner(cursor, shop_id, market, asin, owner, actor, now=None):
    """在托管配置事务中预约全部有效子SKU给AI；同AI不重复交接，人工计划冲突则整笔回滚。

    调用方须先锁店铺再锁托管行，与人工分配保持同一店铺→身份加锁顺序。
    不覆盖人工未来交接，不恢复旧负责人，不调用Amazon或生成绩效报表。
    """
    from .assignment_policy import default_date
    now = now or utc_now()
    cursor.execute('''SELECT sku FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s
        AND BINARY asin=BINARY %s AND is_deleted=0 AND COALESCE(LOWER(parentage_level),'')<>'parent'
        ORDER BY BINARY sku LIMIT 201''', (shop_id, market, asin))
    rows = cursor.fetchall()
    if not 1 <= len(rows) <= 200:
        raise DomainError('托管ASIN需要1~200个有效子SKU，不能遗漏交接', 422)
    items = repo.resolve(cursor, [{'shop_id': shop_id, 'marketplace_id': market, 'seller_sku': row['sku']} for row in rows])
    pending = []
    for item in items:
        scope = repo.scope(cursor, item, lock=True)
        histories = repo.histories(cursor, scope['id'], lock=True)
        current = repo.active_at(histories, now)
        future = [row for row in histories if row['effective_from_utc'] > now]
        if len(future) > 1 or (future and future[0]['owner_user_id'] != owner):
            raise DomainError(f"SKU {item['seller_sku']} 已有其他未来交接，请在负责人分配处理后再恢复托管", 409)
        if future or (current and current['owner_user_id'] == owner):
            continue
        pending.append({'shop_id': shop_id, 'marketplace_id': market, 'seller_sku': item['seller_sku']})
    if not pending:
        return {'affected': 0, 'action': 'assign'}
    data = {'items': pending, 'action': 'assign', 'owner_user_id': owner, 'reason': f'AI托管开启/恢复：ASIN {asin}',
            'effective_date': default_date(now).isoformat(), 'effective_dates': {}, 'expand_parents': False}
    actual = inspect(cursor, data, now, lock=True)
    request_id = 'ai-ownership-' + uuid.uuid4().hex
    return write_items(cursor, actual, data, actor, request_id,
                       hashlib.sha256(json_data(data).encode()).hexdigest(), now)


def candidates():
    """读取启用负责人候选列表并返回。

    独立只读连接，SQL委托Repository；退出关闭，不创建用户或权限。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            return repo.candidates(cursor)
    finally:
        conn.close()


def history(shop_id, marketplace_id, sku):
    """读取店铺/站点/SKU的完整负责区间历史，含已取消记录。

    按生效时间/ID排序，附负责人名称；用于回答谁在何时负责，不是操作审计。
    使用精确身份键，只读并关闭连接。"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute('''SELECT h.*,COALESCE(NULLIF(u.nickname,''),u.username) AS owner_name
                FROM listing_responsibility_history h JOIN listing_responsibility_scopes s ON s.id=h.scope_id
                LEFT JOIN users u ON u.id=h.owner_user_id WHERE s.identity_key=%s ORDER BY h.effective_from_utc,h.id''',
                           (identity(shop_id, marketplace_id, sku),))
            return cursor.fetchall()
    finally:
        conn.close()


def operation_history(shop_id, marketplace_id, sku, page=1, page_size=20):
    """返回指定SKU的分页操作历史及操作者/前后负责人显示名。

    page_size最多100；从审计命令和实际子SKU前快照投影当前SKU，不暴露整批其他SKU。
    取消时展示恢复的当前负责人及被取消负责人，reason允许空；只读并关闭连接。"""
    if page_size > 100:
        raise DomainError('page_size 不能超过 100')
    key = identity(shop_id,marketplace_id,sku)
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            records,total = repo.operation_history(cursor,key,page,page_size)
            commands = [(row,decode(row['after_json'],{}),
                         next((i for i in decode(row['before_json'],[]) if i.get('identity_key') == key),{}))
                        for row in records]
            owners = {command.get('owner_user_id') for _,command,_ in commands if command.get('owner_user_id')}
            names = repo.user_names(cursor,owners)
            result = []
            for row,command,item in commands:
                current, scheduled = item.get('current') or {},item.get('scheduled') or {}
                cancelled = row['action'] == 'cancel'
                result.append({'id':row['id'],'actor_user_id':row['actor_user_id'],
                    'actor_name':row['actor_name'],'created_at':row['created_at'],'action':row['action'],
                    'reason':row['reason'],'request_id':row['request_id'],
                    'previous_owner_name':current.get('owner_name'),
                    'target_owner_name':current.get('owner_name') if cancelled else names.get(command.get('owner_user_id')),
                    'cancelled_owner_name':scheduled.get('owner_name') if cancelled else None,
                    'effective_from_utc':scheduled.get('effective_from_utc') if cancelled else item.get('effective_from_utc')})
            return {'list':result,'total':total,'page':page,'page_size':page_size}
    finally:
        conn.close()
