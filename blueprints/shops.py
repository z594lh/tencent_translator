"""
店铺管理模块
提供店铺的增删改查、设置默认等接口
"""
from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_all_active_shops
from services.mysql_service import get_db_connection

shops_bp = Blueprint('shops', __name__, url_prefix='/api')


VALID_REGIONS = {'na', 'eu', 'fe'}


def _get_conn():
    return get_db_connection()


def _validate_shop_data(data: dict, is_update: bool = False) -> dict:
    """校验并提取店铺数据，返回 (errors, cleaned_data)"""
    errors = {}
    cleaned = {}

    shop_name = data.get('shop_name', '').strip()
    if not shop_name:
        errors['shop_name'] = '店铺名称不能为空'
    elif len(shop_name) > 100:
        errors['shop_name'] = '店铺名称不能超过100个字符'
    cleaned['shop_name'] = shop_name

    seller_id = data.get('seller_id', '').strip()
    if not seller_id:
        errors['seller_id'] = 'Seller ID 不能为空'
    elif len(seller_id) > 50:
        errors['seller_id'] = 'Seller ID 不能超过50个字符'
    cleaned['seller_id'] = seller_id

    refresh_token = data.get('refresh_token', '').strip()
    if not refresh_token:
        errors['refresh_token'] = 'Refresh Token 不能为空'
    cleaned['refresh_token'] = refresh_token

    marketplace_id = data.get('marketplace_id', '').strip()
    if not marketplace_id:
        errors['marketplace_id'] = 'Marketplace ID 不能为空'
    elif len(marketplace_id) > 20:
        errors['marketplace_id'] = 'Marketplace ID 不能超过20个字符'
    cleaned['marketplace_id'] = marketplace_id

    region = data.get('region', 'na').strip().lower()
    if region not in VALID_REGIONS:
        errors['region'] = f'Region 必须是 na/eu/fe 之一'
    cleaned['region'] = region

    status = data.get('status')
    if status is not None:
        try:
            cleaned['status'] = 1 if int(status) else 0
        except (ValueError, TypeError):
            errors['status'] = 'status 必须是 0 或 1'
    else:
        cleaned['status'] = 1

    is_default = data.get('is_default')
    if is_default is not None:
        try:
            cleaned['is_default'] = 1 if int(is_default) else 0
        except (ValueError, TypeError):
            errors['is_default'] = 'is_default 必须是 0 或 1'
    else:
        cleaned['is_default'] = 0

    return errors, cleaned


@shops_bp.route('/shops', methods=['GET'])
@login_required
@permission_required('shops:page')
def list_shops():
    """
    查询所有启用的店铺列表（用于前端下拉选择器）
    """
    try:
        shops = get_all_active_shops()
        # 只返回前端需要的字段
        data = [
            {
                "id": shop["id"],
                "shop_name": shop["shop_name"],
                "seller_id": shop["seller_id"],
                "marketplace_id": shop["marketplace_id"],
                "region": shop["region"],
                "is_default": shop["is_default"],
            }
            for shop in shops
        ]
        return jsonify({
            "status": "success",
            "data": data
        })
    except Exception as e:
        print(f"[Shops] 查询店铺列表异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops/all', methods=['GET'])
@login_required
@permission_required('shops:page')
def list_all_shops():
    """
    查询所有店铺（含禁用），用于管理后台
    """
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, shop_name, seller_id, marketplace_id, region,
                           status, is_default, created_at, updated_at
                    FROM amazon_shops
                    ORDER BY id
                """)
                rows = cursor.fetchall()
                return jsonify({
                    "status": "success",
                    "data": rows
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 查询所有店铺异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops/<int:shop_id>', methods=['GET'])
@login_required
@permission_required('shops:page')
def get_shop(shop_id):
    """
    查询单个店铺详情（含 refresh_token，用于编辑）
    """
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, shop_name, seller_id, refresh_token,
                           marketplace_id, region, status, is_default,
                           created_at, updated_at
                    FROM amazon_shops
                    WHERE id = %s
                    LIMIT 1
                """, (shop_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "店铺不存在"}), 404
                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 查询店铺详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops', methods=['POST'])
@login_required
@permission_required('shops:create')
def create_shop():
    """
    创建新店铺
    """
    try:
        data = request.get_json() or {}
        errors, cleaned = _validate_shop_data(data)
        if errors:
            return jsonify({"status": "error", "message": "参数校验失败", "errors": errors}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查 seller_id + marketplace_id 是否已存在
                cursor.execute("""
                    SELECT id FROM amazon_shops
                    WHERE seller_id = %s AND marketplace_id = %s
                    LIMIT 1
                """, (cleaned['seller_id'], cleaned['marketplace_id']))
                if cursor.fetchone():
                    return jsonify({
                        "status": "error",
                        "message": "该 Seller ID 与 Marketplace ID 组合已存在"
                    }), 409

                cursor.execute("""
                    INSERT INTO amazon_shops
                    (shop_name, seller_id, refresh_token, marketplace_id, region, status, is_default)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    cleaned['shop_name'],
                    cleaned['seller_id'],
                    cleaned['refresh_token'],
                    cleaned['marketplace_id'],
                    cleaned['region'],
                    cleaned['status'],
                    cleaned['is_default'],
                ))
                new_id = cursor.lastrowid

                # 如果设为默认，取消其他店铺的默认状态
                if cleaned['is_default']:
                    cursor.execute("""
                        UPDATE amazon_shops SET is_default = 0 WHERE id != %s
                    """, (new_id,))

                conn.commit()
                return jsonify({
                    "status": "success",
                    "message": "创建成功",
                    "data": {"id": new_id}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 创建店铺异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops/<int:shop_id>', methods=['PUT'])
@login_required
@permission_required('shops:edit')
def update_shop(shop_id):
    """
    更新店铺信息
    """
    try:
        data = request.get_json() or {}
        errors, cleaned = _validate_shop_data(data, is_update=True)
        if errors:
            return jsonify({"status": "error", "message": "参数校验失败", "errors": errors}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 检查店铺是否存在
                cursor.execute("SELECT id FROM amazon_shops WHERE id = %s LIMIT 1", (shop_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "店铺不存在"}), 404

                # 检查 seller_id + marketplace_id 是否与其他店铺冲突
                cursor.execute("""
                    SELECT id FROM amazon_shops
                    WHERE seller_id = %s AND marketplace_id = %s AND id != %s
                    LIMIT 1
                """, (cleaned['seller_id'], cleaned['marketplace_id'], shop_id))
                if cursor.fetchone():
                    return jsonify({
                        "status": "error",
                        "message": "该 Seller ID 与 Marketplace ID 组合已被其他店铺使用"
                    }), 409

                cursor.execute("""
                    UPDATE amazon_shops SET
                        shop_name = %s,
                        seller_id = %s,
                        refresh_token = %s,
                        marketplace_id = %s,
                        region = %s,
                        status = %s,
                        is_default = %s
                    WHERE id = %s
                """, (
                    cleaned['shop_name'],
                    cleaned['seller_id'],
                    cleaned['refresh_token'],
                    cleaned['marketplace_id'],
                    cleaned['region'],
                    cleaned['status'],
                    cleaned['is_default'],
                    shop_id,
                ))

                # 如果设为默认，取消其他店铺的默认状态
                if cleaned['is_default']:
                    cursor.execute("""
                        UPDATE amazon_shops SET is_default = 0 WHERE id != %s
                    """, (shop_id,))

                conn.commit()
                return jsonify({"status": "success", "message": "更新成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 更新店铺异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops/<int:shop_id>', methods=['DELETE'])
@login_required
@permission_required('shops:delete')
def delete_shop(shop_id):
    """
    删除店铺（软删除：将 status 设为 0）
    禁止删除默认店铺
    """
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, is_default, shop_name FROM amazon_shops WHERE id = %s LIMIT 1
                """, (shop_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "店铺不存在"}), 404

                if row['is_default']:
                    return jsonify({
                        "status": "error",
                        "message": "默认店铺不能删除，请先设置其他店铺为默认"
                    }), 400

                cursor.execute("""
                    UPDATE amazon_shops SET status = 0, is_default = 0 WHERE id = %s
                """, (shop_id,))
                conn.commit()
                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 删除店铺异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@shops_bp.route('/shops/<int:shop_id>/set-default', methods=['POST'])
@login_required
@permission_required('shops:set_default')
def set_default_shop(shop_id):
    """
    设置默认店铺
    """
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, status FROM amazon_shops WHERE id = %s LIMIT 1
                """, (shop_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "店铺不存在"}), 404

                if not row['status']:
                    return jsonify({"status": "error", "message": "禁用的店铺不能设为默认"}), 400

                cursor.execute("""
                    UPDATE amazon_shops SET is_default = 0 WHERE id != %s
                """, (shop_id,))
                cursor.execute("""
                    UPDATE amazon_shops SET is_default = 1 WHERE id = %s
                """, (shop_id,))
                conn.commit()
                return jsonify({"status": "success", "message": "设置默认店铺成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Shops] 设置默认店铺异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
