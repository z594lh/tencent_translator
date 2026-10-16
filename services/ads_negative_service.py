"""SP 两级否定投放：身份校验、逐项结果处理、同步与本地查询。"""
import json
import re
import time
from datetime import datetime, timezone

import requests

from services.mysql_service import get_db_connection
from services.shop_service import get_ads_api_client


class NegativeError(ValueError):
    def __init__(self, message, code=400):
        super().__init__(message)
        self.code = code


def positive_id(value, label):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,18}", str(value or "")):
        raise NegativeError(f"{label} 必须为正整数 ID")
    number = int(value)
    if number > 9223372036854775807:
        raise NegativeError(f"{label} 超出 ID 范围")
    return number


def context(shop_id, campaign_id, ad_group_id=None, scope=None):
    """不信任浏览器传入的店铺/层级，重新验证归属链。"""
    shop_id = positive_id(shop_id, "shop_id")
    campaign_id = positive_id(campaign_id, "campaign_id")
    group = positive_id(ad_group_id, "ad_group_id") if ad_group_id not in (None, "") else None
    level = scope or ("group" if group else "campaign")
    if level not in ("campaign", "group") or (level == "group") != bool(group):
        raise NegativeError("scope 与广告组参数不一致")
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("""SELECT a.campaign_id, a.targeting_type, a.state,
                                a.profile_id, s.ads_profile_id, s.region
                         FROM amazon_ads_campaigns a JOIN amazon_shops s ON s.id=a.shop_id
                         WHERE a.shop_id=%s AND a.campaign_id=%s AND s.status=1""",
                      (shop_id, campaign_id))
            campaign = c.fetchone()
            if not campaign or str(campaign['profile_id']) != str(campaign['ads_profile_id']):
                raise NegativeError("活动不属于所选店铺或广告账户已变更，请重新同步", 404)
            if group:
                c.execute("SELECT ad_group_id FROM amazon_ads_ad_groups WHERE campaign_id=%s AND ad_group_id=%s",
                          (campaign_id, group))
                if not c.fetchone():
                    raise NegativeError("广告组不属于当前活动", 404)
        return dict(shop_id=shop_id, campaign_id=campaign_id, ad_group_id=group,
                    scope=level, campaign=campaign)
    finally:
        conn.close()


def _table(kind):
    if kind not in ("keywords", "targets"):
        raise NegativeError("无效的否定类型")
    return ("amazon_ads_negative_keywords", "keyword_id") if kind == "keywords" else (
        "amazon_ads_negative_targets", "target_id")


def _client(ctx):
    client = get_ads_api_client(ctx['shop_id'])
    profile = ctx['campaign'].get('profile_id')
    if profile is not None and str(client.profile_id) != str(profile):
        raise NegativeError('广告账户已变更，请刷新页面重新同步')
    return client


def _key(kind, campaign_level):
    return ("campaignNegativeKeywords" if campaign_level else "negativeKeywords") if kind == "keywords" else (
        "campaignNegativeTargetingClauses" if campaign_level else "negativeTargetingClauses")


def _where(ctx):
    params = [ctx['campaign_id']]
    if ctx['scope'] == 'group':
        return "campaign_id=%s AND ad_group_id=%s", params + [ctx['ad_group_id']]
    return "campaign_id=%s AND ad_group_id IS NULL", params


def _display_expression(expression):
    if isinstance(expression, str):
        try:
            expression = json.loads(expression)
        except (ValueError, TypeError):
            return expression, ""
    values = expression or []
    return ", ".join(str(x.get('value', '')) for x in values), (values[0].get('type', '') if values else '')


def list_local(kind, ctx, search='', state='unarchived', page=1, page_size=20):
    table, id_key = _table(kind)
    try:
        page, page_size = max(1, int(page)), max(1, min(100, int(page_size)))
    except (ValueError, TypeError):
        raise NegativeError("分页参数无效")
    where, params = _where(ctx)
    state = str(state or 'unarchived').upper()
    if state == 'UNARCHIVED':
        where += " AND UPPER(state)<>'ARCHIVED'"
    elif state != 'ALL':
        if state not in ('ENABLED', 'PAUSED', 'ARCHIVED'):
            raise NegativeError("无效的状态")
        where += " AND UPPER(state)=%s"
        params.append(state)
    if search:
        column = 'keyword_text' if kind == 'keywords' else 'expression'
        where += f" AND {column} LIKE %s"
        params.append('%' + str(search).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%')
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(f"SELECT COUNT(*) AS total FROM {table} WHERE {where}", params)
            total = c.fetchone()['total']
            c.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY {id_key} DESC LIMIT %s OFFSET %s",
                      params + [page_size, (page - 1) * page_size])
            rows = c.fetchall()
        for row in rows:
            for key in (id_key, 'campaign_id', 'ad_group_id'):
                row[key] = str(row[key]) if row.get(key) is not None else None
            row['scope'] = ctx['scope']
            row['state'] = str(row.get('state') or '').upper()
            if kind == 'targets':
                row['target_value'], row['expression_type'] = _display_expression(row['expression'])
        return {'list': rows, 'total': total, 'page': page, 'page_size': page_size}
    finally:
        conn.close()


def fetch_all(client, kind, campaign_level=False, **filters):
    """只对幂等列表查询有限重试；任何分页失败均不返回半份数据。"""
    method = client.list_negative_keywords if kind == 'keywords' else client.list_negative_targets
    key = _key(kind, campaign_level)
    rows, token, seen = [], None, set()
    while True:
        for attempt in range(3):
            try:
                response = method(campaign_level=campaign_level, max_results=100, next_token=token,
                                  state_filter=['ENABLED', 'PAUSED', 'ARCHIVED'], **filters)
                break
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 429 or attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        if not isinstance(response, dict) or not isinstance(response.get(key), list):
            raise NegativeError("Amazon 否定列表响应结构异常，未覆盖本地数据", 502)
        rows.extend(response[key])
        token = response.get('nextToken')
        if not token:
            return rows
        if token in seen:
            raise NegativeError("Amazon 分页游标重复，已中止同步", 502)
        seen.add(token)


def save_rows(cursor, kind, rows, campaign_level):
    """在调用者事务中保存成功实体；NULL 表示活动级。"""
    for item in rows:
        campaign = positive_id(item.get('campaignId'), 'campaignId')
        group = None if campaign_level else positive_id(item.get('adGroupId'), 'adGroupId')
        if campaign_level and item.get('adGroupId'):
            raise NegativeError("Amazon 返回层级不一致，已中止保存", 502)
        state = str(item.get('state') or '').upper()
        if state not in ('ENABLED', 'PAUSED', 'ARCHIVED', 'PROPOSED'):
            raise NegativeError('Amazon 返回未知否定状态，已中止保存', 502)
        table, db_id = _table(kind)
        api_id = 'keywordId' if kind == 'keywords' else 'targetId'
        identifier = positive_id(item.get(api_id), api_id)
        cursor.execute(f'SELECT campaign_id,ad_group_id FROM {table} WHERE {db_id}=%s', (identifier,))
        previous = cursor.fetchone()
        if previous and (int(previous['campaign_id']) != campaign or previous['ad_group_id'] != group):
            raise NegativeError('否定实体 ID 的归属与已有记录冲突，未覆盖旧记录', 502)
        stamp = (item.get('extendedData') or {}).get('lastUpdateDateTime') or item.get('lastUpdateDateTime')
        stamp = datetime.fromisoformat(stamp.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None) if stamp else None
        if kind == 'keywords':
            cursor.execute("""INSERT INTO amazon_ads_negative_keywords
                (campaign_id,ad_group_id,keyword_id,keyword_text,match_type,state,last_update_datetime,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id),ad_group_id=VALUES(ad_group_id),keyword_text=VALUES(keyword_text),
                match_type=VALUES(match_type),state=VALUES(state),last_update_datetime=VALUES(last_update_datetime),synced_at=NOW()""",
                (campaign, group, positive_id(item.get('keywordId'), 'keywordId'), item['keywordText'], item['matchType'], state, stamp))
        else:
            expression = item.get('expression') or []
            label, _ = _display_expression(item.get('resolvedExpression') or expression)
            cursor.execute("""INSERT INTO amazon_ads_negative_targets
                (campaign_id,ad_group_id,target_id,expression,resolved_expression,state,last_update_datetime,synced_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,NOW()) ON DUPLICATE KEY UPDATE
                campaign_id=VALUES(campaign_id),ad_group_id=VALUES(ad_group_id),expression=VALUES(expression),
                resolved_expression=VALUES(resolved_expression),state=VALUES(state),
                last_update_datetime=VALUES(last_update_datetime),synced_at=NOW()""",
                (campaign, group, positive_id(item.get('targetId'), 'targetId'),
                 json.dumps(expression, ensure_ascii=False), label, state, stamp))


def sync_kind(cursor, client, shop_id, kind):
    """正式实体同步复用，两层分别完整读取；验证账户内活动归属。"""
    total = 0
    for campaign_level in (False, True):
        rows = fetch_all(client, kind, campaign_level)
        for campaign in {positive_id(r.get('campaignId'), 'campaignId') for r in rows}:
            cursor.execute('SELECT campaign_id FROM amazon_ads_campaigns WHERE shop_id=%s AND campaign_id=%s AND profile_id=%s',
                           (shop_id, campaign, client.profile_id))
            if not cursor.fetchone():
                raise NegativeError('同步返回了未确认归属的活动，未保存否定数据', 502)
        save_rows(cursor, kind, rows, campaign_level)
        total += len(rows)
    return total


def refresh(kind, ctx):
    client = _client(ctx)
    filters = {'campaign_id': ctx['campaign_id']}
    if ctx['ad_group_id']:
        filters['ad_group_id'] = ctx['ad_group_id']
    rows = fetch_all(client, kind, ctx['scope'] == 'campaign', **filters)
    for row in rows:
        if positive_id(row.get('campaignId'), 'campaignId') != ctx['campaign_id'] or (
            ctx['scope'] == 'group' and positive_id(row.get('adGroupId'), 'adGroupId') != ctx['ad_group_id']):
            raise NegativeError('Amazon 返回其他活动或组的数据，未保存', 502)
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            save_rows(c, kind, rows, ctx['scope'] == 'campaign')
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(rows)


def prepare_items(kind, ctx, items):
    if not isinstance(items, list) or not 1 <= len(items) <= 100:
        raise NegativeError('每批须包含 1～100 条否定规则')
    if str(ctx['campaign']['state']).upper() == 'ARCHIVED':
        raise NegativeError('已归档活动不能新增否定规则')
    if kind == 'targets' and ctx['scope'] == 'campaign' and str(ctx['campaign']['targeting_type']).upper() != 'AUTO':
        raise NegativeError('Amazon 的活动级否定商品仅支持自动投放活动，请在广告组级操作')
    result, seen = [], set()
    for item in items:
        if not isinstance(item, dict) or positive_id(item.get('campaignId'), 'campaignId') != ctx['campaign_id']:
            raise NegativeError('批次活动与当前页面不一致')
        if (positive_id(item.get('adGroupId'), 'adGroupId') if item.get('adGroupId') else None) != ctx['ad_group_id']:
            raise NegativeError('批次广告组与当前页面不一致')
        row = {'campaignId': str(ctx['campaign_id']), 'state': 'ENABLED'}
        if ctx['ad_group_id']:
            row['adGroupId'] = str(ctx['ad_group_id'])
        if kind == 'keywords':
            text, match = item.get('keywordText'), str(item.get('matchType') or '').upper()
            if not isinstance(text, str) or not text.strip() or len(text.strip()) > 80 or len(text.split()) > 10:
                raise NegativeError('否定关键词需为 1～80 字符，且不超过 10 个单词')
            if match not in ('NEGATIVE_EXACT', 'NEGATIVE_PHRASE'):
                raise NegativeError('只支持否定精准和否定词组')
            if match == 'NEGATIVE_PHRASE' and len(text.split()) > 4:
                raise NegativeError('否定词组最多 4 个单词')
            row.update(keywordText=text.strip(), matchType=match)
            identity = (text.strip().casefold(), match)
        else:
            expr = item.get('expression')
            if not isinstance(expr, list) or len(expr) != 1 or not isinstance(expr[0], dict):
                raise NegativeError('每条否定商品必须包含一个有效谓词')
            predicate, value = expr[0].get('type'), str(expr[0].get('value') or '').strip()
            if predicate == 'ASIN_SAME_AS':
                value = value.upper()
                if not re.fullmatch('[A-Z0-9]{10}', value):
                    raise NegativeError('ASIN 必须是 10 位字母或数字')
            elif predicate == 'ASIN_BRAND_SAME_AS':
                positive_id(value, '品牌 ID（不能填写品牌名称）')
            else:
                raise NegativeError('不支持的否定商品类型')
            row['expression'] = [{'type': predicate, 'value': value}]
            identity = (predicate, value)
        if identity in seen:
            raise NegativeError('本批次包含重复规则，请先去重')
        seen.add(identity)
        result.append(row)
    return result


def _error_message(entry):
    """优先展示 Amazon 的业务原因，不把整段嵌套 JSON 丢给运营。"""
    errors = entry.get('errors') or [entry]
    messages = []
    for error in errors:
        if error.get('errorType') == 'duplicateValueError' or error.get('code') == 'DUPLICATE_VALUE':
            messages.append('当前层级已存在相同否定规则，请先同步查看，勿重复添加')
            continue
        def extract(value):
            if not isinstance(value, dict):
                return None
            if value.get('message'):
                return str(value['message'])
            for child in value.values():
                found = extract(child)
                if found:
                    return found
            return None
        messages.append(extract(error) or str(error.get('code') or error.get('errorType') or '未知业务错误，请同步核实'))
    return '；'.join(messages)[:1000]


def parse_result(response, kind, campaign_level, items):
    """不能将 HTTP 207 当作全部成功；按 index 关联，缺结果也算失败。"""
    key = _key(kind, campaign_level)
    block = response.get(key, {}) if isinstance(response, dict) else {}
    if not isinstance(block, dict):
        raise NegativeError('Amazon 批量响应结构异常，请同步核实后再操作', 502)
    id_key = 'keywordId' if kind == 'keywords' else 'targetId'
    entity_keys = ('campaignNegativeKeyword', 'negativeKeyword') if kind == 'keywords' else (
        'campaignNegativeTargetingClauses', 'campaignNegativeTargetingClause', 'negativeTargetingClause')
    id_keys = (id_key, 'campaignNegativeKeywordId', 'negativeKeywordId') if kind == 'keywords' else (
        id_key, 'campaignNegativeTargetingClauseId', 'negativeTargetingClauseId')
    successes, errors, answered = [], [], set()
    for entry in block.get('success', []):
        index = entry.get('index')
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(items) or index in answered:
            raise NegativeError('Amazon 返回无法匹配的批次索引，请同步核实', 502)
        answered.add(index)
        entity = next((entry[k] for k in entity_keys if isinstance(entry.get(k), dict)), {})
        entity_id = entity.get(id_key) or next((entry[k] for k in id_keys if entry.get(k)), None)
        # 更新/归档以已验证的原 ID 校验回包，新增必须由 Amazon 返回 ID。
        expected_id = items[index].get(id_key)
        if not entity_id or (expected_id and str(entity_id) != str(expected_id)):
            errors.append({'index': index, 'message': 'Amazon 成功项缺少 ID 或 ID 不符，请同步核实'})
            continue
        entity_id = str(positive_id(entity_id, id_key))
        row = {**items[index], **entity, id_key: entity_id}
        for field in ('campaignId', 'adGroupId'):
            if field in items[index] and entity.get(field) is not None and str(entity[field]) != str(items[index][field]):
                raise NegativeError('Amazon 成功项归属不符，请同步核实', 502)
        successes.append((index, row))
    for entry in block.get('error', []):
        index = entry.get('index')
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index < len(items) or index in answered:
            raise NegativeError('Amazon 返回无法匹配的错误索引，请同步核实', 502)
        answered.add(index)
        errors.append({'index': index, 'message': _error_message(entry)})
    for index in set(range(len(items))) - answered:
        errors.append({'index': index, 'message': 'Amazon 未返回此项结果，请同步核实后再操作'})
    return successes, errors


def create(kind, ctx, items):
    items = prepare_items(kind, ctx, items)
    client = _client(ctx)
    method = client.create_negative_keywords if kind == 'keywords' else client.create_negative_targets
    # 写操作不自动重试；超时必须先同步确认，避免重复创建。
    response = method(items, campaign_level=ctx['scope'] == 'campaign')
    successes, errors = parse_result(response, kind, ctx['scope'] == 'campaign', items)
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            save_rows(c, kind, [row for _, row in successes], ctx['scope'] == 'campaign')
        conn.commit()
    except Exception:
        conn.rollback()
        raise NegativeError('Amazon 已返回操作结果，但本地保存失败；请先同步，不要重复提交', 502)
    finally:
        conn.close()
    return {'success_count': len(successes), 'error_count': len(errors), 'errors': errors,
            'success_indices': [index for index, _ in successes],
            'success_ids': [row['keywordId' if kind == 'keywords' else 'targetId'] for _, row in successes]}


def update(kind, ctx, entity_id, state):
    table, db_id = _table(kind)
    entity_id = positive_id(entity_id, db_id)
    state = str(state or '').upper()
    if state not in ('ENABLED', 'PAUSED', 'ARCHIVED'):
        raise NegativeError('只支持启用、暂停或归档')
    where, params = _where(ctx)
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(f'SELECT * FROM {table} WHERE {where} AND {db_id}=%s', params + [entity_id])
            row = c.fetchone()
    finally:
        conn.close()
    if not row:
        raise NegativeError('否定规则不属于当前店铺/活动/广告组，请先同步', 404)
    if str(row['state']).upper() == 'ARCHIVED':
        if state == 'ARCHIVED':
            return {'success_count': 1, 'error_count': 0}
        raise NegativeError('已归档规则不能重新启用，请重新添加')
    client = _client(ctx)
    campaign_level = ctx['scope'] == 'campaign'
    if state == 'ARCHIVED':
        method = client.archive_negative_keyword if kind == 'keywords' else client.archive_negative_target
        response = method(entity_id, campaign_level=campaign_level)
    else:
        method = client.update_negative_keyword if kind == 'keywords' else client.update_negative_target
        response = method(entity_id, {'state': state}, campaign_level=campaign_level)
    api_id = 'keywordId' if kind == 'keywords' else 'targetId'
    expected = {api_id: str(entity_id), 'campaignId': str(ctx['campaign_id'])}
    if ctx['ad_group_id']:
        expected['adGroupId'] = str(ctx['ad_group_id'])
    successes, errors = parse_result(response, kind, campaign_level, [expected])
    if errors or not successes:
        raise NegativeError('Amazon 未执行成功：' + (errors[0]['message'] if errors else '结果未知'), 502)
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(f'UPDATE {table} SET state=%s,synced_at=NOW() WHERE {where} AND {db_id}=%s',
                      [state] + params + [entity_id])
        conn.commit()
    except Exception:
        conn.rollback()
        raise NegativeError('Amazon 操作成功但本地状态保存失败，请先同步核实', 502)
    finally:
        conn.close()
    return {'success_count': 1, 'error_count': 0}
