"""Listing负责人的SQL仓储层。

调用方ownership_service负责连接、提交和回滚；本模块只使用传入cursor，不自行提交。
核心表：listing_responsibility_scopes身份/版本、history负责区间、audit操作审计。
身份是店铺+站点+精确SKU，区间为UTC左闭右开；负责历史与操作历史是不同概念。
列表查询与原Listing接口分离；lock=True的方法可能加行锁/创建身份，不能当只读调用。
"""
from services.listing_performance.common import DomainError, identity, json_data


def candidates(cursor):
    """查询users中status=1的候选负责人，按ID返回用户记录列表。

    使用调用方cursor只读；用户是否能分配由HTTP权限控制，不在此处授权。"""
    cursor.execute('SELECT id, username, nickname, status, account_type FROM users WHERE status = 1 ORDER BY id')
    return cursor.fetchall()


def resolve(cursor, items, expand_parents=False):
    """将items中的Listing身份解析为去重后的实际子SKU列表。

    expand_parents=True才展开父体当前有效子体；精确核对SKU，不推测不存在的Listing。
    身份不唯一、无子体或展开后超过200条报业务异常；只读，不创建负责历史。"""
    resolved = {}
    for item in items:
        cursor.execute('''SELECT shop_id, marketplace_id, sku, parentage_level, item_name
            FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s AND sku=%s AND is_deleted=0''',
                       (item['shop_id'], item['marketplace_id'], item['seller_sku']))
        rows = [r for r in cursor.fetchall() if r['sku'] == item['seller_sku']]
        if len(rows) != 1:
            raise DomainError('Listing 不存在或身份不唯一')
        row = rows[0]
        if str(row.get('parentage_level') or '').lower() == 'parent':
            if not expand_parents:
                raise DomainError('父体必须选择展开现有子体')
            cursor.execute('''SELECT shop_id, marketplace_id, sku, parentage_level, item_name
                FROM amazon_listings WHERE shop_id=%s AND marketplace_id=%s AND parent_sku=%s
                AND is_deleted=0 AND COALESCE(LOWER(parentage_level), '') <> 'parent' ''',
                           (item['shop_id'], item['marketplace_id'], item['seller_sku']))
            rows = cursor.fetchall()
            if not rows:
                raise DomainError('父体当前没有可分配子体')
        for row in rows:
            key = identity(row['shop_id'], row['marketplace_id'], row['sku'])
            resolved[key] = {'identity_key': key, 'shop_id': row['shop_id'],
                             'marketplace_id': row['marketplace_id'], 'seller_sku': row['sku'],
                             'item_name': row.get('item_name')}
    if not resolved or len(resolved) > 200:
        raise DomainError('一次需分配 1~200 条子体 Listing')
    return [resolved[k] for k in sorted(resolved)]


def scope(cursor, item, lock=False):
    """按item.identity_key读取身份记录，返回字典或None。

    lock=True时先幂等补建scopes，再SELECT FOR UPDATE；调用方须先锁店铺。
    不提交事务；只读预览必须保持lock=False。"""
    if lock:
        cursor.execute('''INSERT INTO listing_responsibility_scopes
            (identity_key, shop_id, marketplace_id, seller_sku, version)
            VALUES (%s,%s,%s,%s,0) ON DUPLICATE KEY UPDATE id=id''',
                       (item['identity_key'], item['shop_id'], item['marketplace_id'], item['seller_sku']))
    cursor.execute('SELECT * FROM listing_responsibility_scopes WHERE identity_key=%s' +
                   (' FOR UPDATE' if lock else ''), (item['identity_key'],))
    return cursor.fetchone()


def histories(cursor, scope_id, lock=False):
    """读取scope_id尚未作废的负责区间，附用户显示名并按生效时间排序。

    lock=True加FOR UPDATE；不包含已取消记录，完整追溯由Service.history另行查询。"""
    cursor.execute('''SELECT h.*, COALESCE(NULLIF(u.nickname,''),u.username) AS owner_name
        FROM listing_responsibility_history h LEFT JOIN users u ON u.id=h.owner_user_id
        WHERE h.scope_id=%s AND h.voided_at IS NULL ORDER BY h.effective_from_utc''' +
                   (' FOR UPDATE' if lock else ''), (scope_id,))
    return cursor.fetchall()


def active_at(rows, at):
    """从rows中查找时刻at生效的唯一负责区间，返回记录或None。

    匹配effective_from<=at<effective_to，空结束时间表示持续；重叠区间报409。
    None负责人是解除分配，不用当前负责人替代历史负责人。"""
    matches = [r for r in rows if r['effective_from_utc'] <= at and
               (r['effective_to_utc'] is None or at < r['effective_to_utc'])]
    if len(matches) > 1:
        raise DomainError('负责人历史区间重叠，需要管理员核查', 409)
    return matches[0] if matches else None


def audit_result(cursor, actor, request_id):
    """按操作者actor与request_id读取既有请求哈希和结果。

    返回记录或None，供Service幂等重放验证；不创建新审计。"""
    cursor.execute('SELECT request_hash, result_json FROM listing_responsibility_audit WHERE actor_user_id=%s AND request_id=%s',
                   (actor, request_id))
    return cursor.fetchone()


def audit(cursor, actor, request_id, request_hash, action, before, after, result, reason):
    """追加一次归属操作审计，记录操作者、请求及变更前后快照。

    before为实际子SKU快照，after为签名命令；result用于同请求重试返回。
    参与调用方事务，不单独提交；reason允许空。"""
    cursor.execute('''INSERT INTO listing_responsibility_audit
        (actor_user_id,request_id,request_hash,action,before_json,after_json,result_json,reason)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
                   (actor, request_id, request_hash, action, json_data(before), json_data(after),
                    json_data(result), reason))


def owner_condition(owner_user_id=None, unassigned=False, as_of=None, assigned=False):
    """生成负责人筛选SQL片段及参数列表，未筛选返回(None,[])。

    owner_user_id/已分配/未分配均基于as_of时刻有效非空负责人。
    使用EXISTS避免历史记录放大列表行数；COUNT与分页必须共用此条件。
    站点比较显式排序规则，SKU二进制精确比较；不ALTER旧表。"""
    # 新旧表可能分别使用unicode_ci/general_ci，避免直接比较触发排序规则冲突。
    # 显式统一站点比较的排序规则，不改表；SKU仍按二进制精确匹配。
    base = '''SELECT 1 FROM listing_responsibility_scopes rs
        JOIN listing_responsibility_history rh ON rh.scope_id=rs.id
        WHERE rs.shop_id=l.shop_id
        AND rs.marketplace_id COLLATE utf8mb4_unicode_ci=l.marketplace_id COLLATE utf8mb4_unicode_ci
        AND BINARY rs.seller_sku=BINARY l.sku AND rh.voided_at IS NULL
        AND rh.effective_from_utc<=%s
        AND (rh.effective_to_utc IS NULL OR rh.effective_to_utc>%s)
        AND rh.owner_user_id IS NOT NULL'''
    if unassigned:
        return 'NOT EXISTS (' + base + ')', [as_of,as_of]
    if owner_user_id is not None:
        return 'EXISTS (' + base + ' AND rh.owner_user_id=%s)', [as_of,as_of,owner_user_id]
    if assigned:
        return 'EXISTS (' + base + ')', [as_of,as_of]
    return None, []


def attach(cursor, rows, now):
    """批量读取rows中Listing的归属并原地补充ownership字段。

    now为UTC；字段含身份version、当前current和最早未来scheduled。
    无身份显示version=0；无列表直接返回None，不写数据库。"""
    if not rows:
        return
    keys = [identity(r['shop_id'], r['marketplace_id'], r['sku']) for r in rows]
    placeholders = ','.join(['%s'] * len(keys))
    cursor.execute(f'''SELECT s.identity_key,s.version,h.id,h.owner_user_id,h.effective_from_utc,
        h.effective_to_utc, COALESCE(NULLIF(u.nickname,''),u.username) AS owner_name
        FROM listing_responsibility_scopes s LEFT JOIN listing_responsibility_history h
        ON h.scope_id=s.id AND h.voided_at IS NULL LEFT JOIN users u ON u.id=h.owner_user_id
        WHERE s.identity_key IN ({placeholders}) ORDER BY h.effective_from_utc''', keys)
    groups = {}
    for row in cursor.fetchall():
        groups.setdefault(row['identity_key'], []).append(row)
    for row, key in zip(rows, keys):
        group = groups.get(key, [])
        history = [h for h in group if h['id'] is not None]
        current = active_at(history, now)
        future = [h for h in history if h['effective_from_utc'] > now]
        row['ownership'] = {'version': group[0]['version'] if group else 0,
                            'current': current, 'scheduled': future[0] if future else None}


def assignment_list(cursor, shop_id, sku, keyword, owner_user_id, unassigned, page, page_size, assigned=False):
    """读取独立分配页面的分页Listing，返回list/total/page/page_size。

    按店铺、精确SKU、关键词、负责人及是否分配筛选，补products中文名和归属。
    只读，按Listing ID倒序；统计与分页使用相同时间及负责人条件。"""
    from services.listing_performance.common import utc_now
    now = utc_now()
    conditions, params = ['l.shop_id=%s', 'l.is_deleted=0'], [shop_id]
    if sku:
        conditions.append('BINARY l.sku=BINARY %s')
        params.append(sku)
    if keyword:
        conditions.append('''(l.item_name LIKE %s OR l.sku LIKE %s OR EXISTS
            (SELECT 1 FROM products p WHERE BINARY p.seller_sku=BINARY l.sku AND p.product_name LIKE %s))''')
        params.extend(['%' + keyword + '%'] * 3)
    condition, owner_params = owner_condition(owner_user_id, unassigned, now, assigned=assigned)
    if condition:
        conditions.append(condition)
        params.extend(owner_params)
    where = ' AND '.join(conditions)
    cursor.execute('SELECT COUNT(*) AS total FROM amazon_listings l WHERE ' + where, tuple(params))
    total = cursor.fetchone()['total']
    cursor.execute('''SELECT l.id,l.shop_id,l.marketplace_id,l.sku,l.asin,l.item_name,
        l.parentage_level,l.parent_sku FROM amazon_listings l WHERE ''' + where +
        ' ORDER BY l.id DESC LIMIT %s OFFSET %s', tuple(params + [page_size,(page-1)*page_size]))
    rows = cursor.fetchall()
    if rows:
        skus = sorted({row['sku'] for row in rows})
        cursor.execute('SELECT seller_sku,product_name FROM products WHERE seller_sku IN ('+
                       ','.join(['%s']*len(skus))+')',tuple(skus))
        names = {row['seller_sku']:row['product_name'] for row in cursor.fetchall()}
        for row in rows:
            row['product_name'] = names.get(row['sku'])
    attach(cursor,rows,now)
    return {'list':rows,'total':total,'page':page,'page_size':page_size}


def operation_history(cursor, key, page, page_size):
    """按身份key查询操作审计，返回(分页记录,total)。

    在before_json实际子SKU中查找该身份，保证父体分配也能追溯到子体。
    按审计ID倒序，附操作者名称；JSON筛选可能全扫，不是负责区间查询。"""
    # 分配和取消的before_json都保存展开后的子SKU身份，可据此筛选同一SKU的操作。
    predicate = "JSON_SEARCH(a.before_json,'one',%s,NULL,'$[*].identity_key') IS NOT NULL"
    cursor.execute('SELECT COUNT(*) AS total FROM listing_responsibility_audit a WHERE '+predicate,(key,))
    total = cursor.fetchone()['total']
    cursor.execute('''SELECT a.id,a.actor_user_id,a.action,a.reason,a.request_id,a.created_at,
        a.before_json,a.after_json,COALESCE(NULLIF(u.nickname,''),u.username) AS actor_name
        FROM listing_responsibility_audit a LEFT JOIN users u ON u.id=a.actor_user_id WHERE '''+predicate+
        ' ORDER BY a.id DESC LIMIT %s OFFSET %s',(key,page_size,(page-1)*page_size))
    return cursor.fetchall(),total


def user_names(cursor, ids):
    """批量读取用户显示名，返回{id:名称}映射。

    昵称优先于用户名，空ids返回空字典；只读，用于审计结果补充目标负责人名称。"""
    if not ids:
        return {}
    placeholders = ','.join(['%s']*len(ids))
    cursor.execute('SELECT id,COALESCE(NULLIF(nickname,\'\'),username) AS name FROM users WHERE id IN ('+
                   placeholders+')',tuple(sorted(ids)))
    return {row['id']:row['name'] for row in cursor.fetchall()}
