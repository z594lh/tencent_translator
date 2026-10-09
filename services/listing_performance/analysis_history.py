"""SKU AI成功结果保存及历史只读查询，不调用模型，不存输入资料或执行广告。

HTTP层负责人员分析/广告资料/AI权限。本模块再次校验店铺、精确SKU和当前负责人；
曾带售价/成本参考的结果须有对应权限才能列出。配置版本变化不会删历史。
保存由analysis_export的短事务调用，绝不在等待模型时保持数据库事务。
"""
import json

from services.listing_performance import analysis_service as analysis
from services.listing_performance.common import DomainError, identity, json_data, positive_id


def ensure_ready(params, owner):
    """收费前确认历史表可读取；缺迁移时由共用session返回部署提示，不先调用AI。"""
    with analysis.session(params, owner) as (cursor, _, _, _, _):
        cursor.execute('SELECT id FROM performance_analysis_history LIMIT 0')


def save_result(cursor, shop, market, owner, actor, params, result, contains_pricing):
    """在调用方已锁店铺、复核资格的事务中插入一份成功快照，返回记录ID。

    只保存经过校验的服务端结果，actor来自登录身份，不能使用浏览器传入操作人。
    此处不提交/吞异常/重试，外层统一回滚；不保存Markdown输入或供应商原始响应。
    """
    cursor.execute('''INSERT INTO performance_analysis_history
        (identity_key,shop_id,marketplace_id,owner_user_id,seller_sku,product_name,report_date,
         config_version,contains_pricing,model,guide_version,confidence,summary,result_json,created_by,generated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (identity(shop, market, result['seller_sku']), shop, market, owner, result['seller_sku'],
         result['product_name'], result['report_date'], analysis.integer(params.get('config_version'), 'config_version', 1),
         int(contains_pricing), result['model'], result['guide_version'], result['analysis']['confidence'],
         result['analysis']['summary'], json_data(result), positive_id(actor, 'actor'), result['generated_at']))
    return cursor.lastrowid


def query(params, owner, include_pricing=False, latest=False, record_id=None):
    """读取分页历史、最近可查看结果或指定记录，不调用AI且不改历史快照。

    列表按生成UTC时间/ID倒序，仅返回摘要不加载全部JSON。详情ID同时匹配店铺、站点、
    负责人、精确SKU；无成本资格时过滤含参考的记录，避免摘要/建议泄露成本推导结果。
    历史不受关注配置版本影响，但SKU转交/删除后原负责人不可继续读取。
    """
    sku = analysis.text_input(params.get('seller_sku'), 'seller_sku', 255)
    if not sku:
        raise DomainError('请选择需要查看历史的 SKU')
    page = analysis.integer(params.get('page', 1), 'page', 1, 100000)
    size = analysis.integer(params.get('page_size', 20), 'page_size', 1, 100)
    chosen = analysis.integer(record_id, 'record_id', 1, 9223372036854775807) if record_id is not None else None
    with analysis.session(params, owner) as (cursor, shop, market, _, _):
        if sku not in analysis.current_skus(cursor, shop, market, owner):
            raise DomainError('SKU 已转交、删除或不是该人员当前负责，不能查看分析历史', 403)
        where = '''h.identity_key=%s AND h.shop_id=%s AND BINARY h.marketplace_id=BINARY %s
            AND h.owner_user_id=%s AND BINARY h.seller_sku=BINARY %s'''
        args = [identity(shop, market, sku), shop, market, owner, sku]
        if not include_pricing:
            where += ' AND h.contains_pricing=0'
        if chosen is not None:
            where += ' AND h.id=%s'
            args.append(chosen)
        if latest or chosen is not None:
            cursor.execute('SELECT h.id,h.result_json FROM performance_analysis_history h WHERE ' + where +
                           ' ORDER BY h.generated_at DESC,h.id DESC LIMIT 1', args)
            row = cursor.fetchone()
            if not row:
                raise DomainError('暂无可查看的分析结果，请先生成分析；含售价/成本参考的记录需相应查看权限', 404)
            try:
                result = json.loads(row['result_json'])
                if not isinstance(result, dict) or not isinstance(result.get('analysis'), dict):
                    raise ValueError()
            except (TypeError, ValueError):
                raise DomainError('历史分析内容不完整，请联系管理员核查记录', 503) from None
            return {**result, 'history_id': row['id'], 'history_saved': True}
        cursor.execute('SELECT COUNT(*) AS total FROM performance_analysis_history h WHERE ' + where, args)
        total = cursor.fetchone()['total']
        cursor.execute('''SELECT h.id,h.product_name,h.report_date,h.generated_at,h.model,h.confidence,
            h.summary,h.created_by,COALESCE(NULLIF(u.nickname,''),u.username) AS created_by_name
            FROM performance_analysis_history h LEFT JOIN users u ON u.id=h.created_by WHERE ''' + where +
            ' ORDER BY h.generated_at DESC,h.id DESC LIMIT %s OFFSET %s', (*args, size, (page - 1) * size))
        return {'list': cursor.fetchall(), 'total': total, 'page': page, 'page_size': size}
