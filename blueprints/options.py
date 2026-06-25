"""
下拉选项模块 — 所有前端 el-select 下拉框接口统一入口

路由一览:
  GET  /api/options/shops                     店铺列表
  GET  /api/options/suppliers                 供应商列表
  GET  /api/options/products                  产品列表
  GET  /api/options/products/categories        产品分类列表
  GET  /api/options/logistics-providers       货代列表
  GET  /api/options/users                     用户列表
  GET  /api/options/amazon/warehouses         FBA 仓库列表（?shop_id=）
  GET  /api/options/amazon/shipments          FBA 货件列表
  GET  /api/options/amazon/inbound-plans      亚马逊入仓计划列表
  GET  /api/options/product-board/filters     备货看板筛选选项
  GET  /api/options/advertising/campaigns     广告活动下拉（?shop_id=）
  GET  /api/options/advertising/ad-groups     广告组下拉（?shop_id=&campaign_id=）
  GET  /api/options/advertising/asins         推广ASIN下拉（?shop_id=&campaign_id=）

注意：响应统一为 {status, data: [...]}，无分页，字段名与原端点一致。
"""
from flask import Blueprint, request, jsonify
from services.mysql_service import get_db_connection

options_bp = Blueprint('options', __name__, url_prefix='/api/options')


def _get_conn():
    return get_db_connection()


# ============================================================
# 店铺 — 旧端点 GET /api/shops
# ============================================================

@options_bp.route('/shops', methods=['GET'])
def option_shops():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, shop_name FROM amazon_shops WHERE status = 1 ORDER BY shop_name")
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 供应商 — 旧端点 GET /api/suppliers
# ============================================================

@options_bp.route('/suppliers', methods=['GET'])
def option_suppliers():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, name,remark FROM suppliers WHERE status = 1 ORDER BY name")
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 产品 — 旧端点 GET /api/products
# ============================================================

@options_bp.route('/products', methods=['GET'])
def option_products():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, fnsku,declare_name_en,model,seller_sku, COALESCE(product_name, declare_name_cn, '') as product_name
                    FROM products WHERE status = 1
                    ORDER BY created_at desc
                """)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 产品分类 — 旧端点 GET /api/products/categories
# ============================================================

@options_bp.route('/products/categories', methods=['GET'])
def option_product_categories():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, category_cn FROM category_commission_rates ORDER BY category_cn")
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 货代 — 旧端点 GET /api/logistics-providers
# ============================================================

@options_bp.route('/logistics-providers', methods=['GET'])
def option_logistics_providers():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, name FROM logistics_providers WHERE status = 1 ORDER BY created_at DESC")
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 用户 — 旧端点 GET /api/expenses/users
# ============================================================

@options_bp.route('/users', methods=['GET'])
def option_users():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, username, nickname FROM users WHERE status = 1 ORDER BY created_at desc")
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# FBA 仓库 — 旧端点 GET /api/amazon/warehouses
# ============================================================

@options_bp.route('/amazon/warehouses', methods=['GET'])
def option_amazon_warehouses():
    try:
        shop_id = request.args.get('shop_id', '').strip()
        if not shop_id:
            return jsonify({"status": "error", "message": "请提供 shop_id"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT warehouse_id FROM fba_warehouses
                    WHERE shop_id = %s ORDER BY sync_time DESC
                """, (shop_id,))
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# FBA 可绑定货件 — 旧端点 GET /api/logistics-waybills/available-shipments
# ============================================================

@options_bp.route('/amazon/shipments', methods=['GET'])
def option_amazon_shipments():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        b.shipment_confirmation_id as shipment_id,
                        MAX(a.inbound_plan_id) AS inbound_plan_id,
                        MAX(b.name) AS shipment_name,
                        MAX(a.status) AS shipment_status,
                        MAX(b.destination_warehouse_id) as destination_fulfillment_center_id
                    FROM amazon_inbound_shipments a
                    INNER JOIN amazon_inbound_shipments_detail b ON a.inbound_plan_id = b.inbound_plan_id
                    GROUP BY b.shipment_confirmation_id
                """)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 入仓计划 — 旧端点 GET /api/amazon/inbound-plans
# ============================================================

@options_bp.route('/amazon/inbound-plans', methods=['GET'])
def option_amazon_inbound_plans():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT a.inbound_plan_id, MAX(c.name) AS name
                    FROM amazon_inbound_plans a
                    INNER JOIN amazon_inbound_shipments b ON a.inbound_plan_id = b.inbound_plan_id
                    INNER JOIN amazon_inbound_shipments_detail c ON b.inbound_plan_id = c.inbound_plan_id
                    WHERE a.created_at > DATE_SUB(NOW(), INTERVAL 2 MONTH)
                      AND a.status != 'CANCELLED'
                    GROUP BY a.inbound_plan_id
                """)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 备货看板筛选 — 旧端点 GET /api/product-board/filters
# ============================================================

@options_bp.route('/product-board/filters', methods=['GET'])
def option_product_board_filters():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT amazon_status FROM product_board
                    WHERE amazon_status IS NOT NULL AND amazon_status != ''
                    ORDER BY amazon_status
                """)
                rows = [r['amazon_status'] for r in cursor.fetchall()]
                return jsonify({"status": "success", "data": {"amazon_statuses": rows}})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 广告活动下拉 — GET /api/options/advertising/campaigns
# ============================================================

@options_bp.route('/advertising/campaigns', methods=['GET'])
def option_ad_campaigns():
    """广告活动下拉：从原始数据去重 campaign_id + campaign_name"""
    try:
        shop_id = request.args.get('shop_id', '').strip()
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                if shop_id:
                    cursor.execute("""
                        SELECT DISTINCT campaign_id, MAX(campaign_name) AS campaign_name
                        FROM amazon_ads_raw_reports
                        WHERE shop_id = %s AND campaign_id != ''
                        GROUP BY campaign_id
                        ORDER BY campaign_name
                    """, (shop_id,))
                else:
                    cursor.execute("""
                        SELECT DISTINCT campaign_id, MAX(campaign_name) AS campaign_name
                        FROM amazon_ads_raw_reports
                        WHERE campaign_id != ''
                        GROUP BY campaign_id
                        ORDER BY campaign_name
                    """)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 广告组下拉 — GET /api/options/advertising/ad-groups
# ============================================================

@options_bp.route('/advertising/ad-groups', methods=['GET'])
def option_ad_groups():
    """广告组下拉：按 campaign_id 联动筛选"""
    try:
        shop_id = request.args.get('shop_id', '').strip()
        campaign_id = request.args.get('campaign_id', '').strip()
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                where = ["ad_group_id != ''"]
                params = []
                if shop_id:
                    where.append("shop_id = %s")
                    params.append(shop_id)
                if campaign_id:
                    where.append("campaign_id = %s")
                    params.append(campaign_id)
                where_sql = "WHERE " + " AND ".join(where)
                cursor.execute(f"""
                    SELECT DISTINCT ad_group_id, MAX(ad_group_name) AS ad_group_name
                    FROM amazon_ads_raw_reports
                    {where_sql}
                    GROUP BY ad_group_id
                    ORDER BY ad_group_name
                """, params)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# 推广ASIN下拉 — GET /api/options/advertising/asins
# ============================================================

@options_bp.route('/advertising/asins', methods=['GET'])
def option_ad_asins():
    """推广ASIN下拉：按 campaign_id 联动筛选"""
    try:
        shop_id = request.args.get('shop_id', '').strip()
        campaign_id = request.args.get('campaign_id', '').strip()
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                where = ["advertised_asin != ''"]
                params = []
                if shop_id:
                    where.append("shop_id = %s")
                    params.append(shop_id)
                if campaign_id:
                    where.append("campaign_id = %s")
                    params.append(campaign_id)
                where_sql = "WHERE " + " AND ".join(where)
                cursor.execute(f"""
                    SELECT DISTINCT advertised_asin AS asin,
                           MAX(advertised_sku) AS sku
                    FROM amazon_ads_raw_reports
                    {where_sql}
                    GROUP BY advertised_asin
                    ORDER BY advertised_asin
                """, params)
                return jsonify({"status": "success", "data": cursor.fetchall()})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
