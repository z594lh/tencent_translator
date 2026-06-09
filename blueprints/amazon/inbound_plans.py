"""
Amazon 入库计划/货件/箱子模块（重构版）

模块结构：
  1. 前端页面接口（路由）      — 货件列表、货件详情、箱子列表
  2. 前端调用同步接口（路由）  — 一键同步、同步箱子、同步货件详情
  3. 各种同步方法             — 供路由和 cron 复用
  4. 数据库操作方法           — 纯数据读写

同步策略：
  - 前端一键同步（/amazon/sync/inbound-shipments）：
      · 新入库计划 → 全量同步（货件列表 + 箱子 + 货件详情）
      · 旧入库计划 → 仅同步货件列表
  - cron 每 30 分钟：同步入库计划列表 + 所有货件列表
  - cron 每 6 小时：同步所有箱子 + 所有货件详情
"""
import time
import json
import re

from flask import Blueprint, request, jsonify
from blueprints.user_auth import login_required, permission_required
from services.shop_service import get_sp_api_client
from services.mysql_service import get_db_connection

amazon_inbound_plans_bp = Blueprint('amazon_inbound_plans', __name__, url_prefix='/api')


# =============================================================================
# Helper
# =============================================================================

def _require_shop_id() -> int:
    """从 query string 强制获取 shop_id"""
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


def _require_shop_id_from_body(data: dict) -> int:
    """从请求体强制获取 shop_id"""
    shop_id = data.get('shop_id')
    if shop_id is None or shop_id == '':
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except (ValueError, TypeError):
        raise ValueError("shop_id 必须是整数")


def _iso_to_datetime(iso_str):
    """将 ISO 8601 时间字符串转为 MySQL DATETIME 格式"""
    if not iso_str:
        return None
    if isinstance(iso_str, str):
        iso_str = iso_str.replace('Z', '')
        if '+' in iso_str:
            iso_str = iso_str.split('+')[0]
    return iso_str


def _extract_shipment_id_from_box(box):
    """
    从 box 信息中提取货件编号（shipment_id）
    优先根据 box_id 去除末尾 U+数字 箱号后缀，
    其次尝试 externalContainerIdentifier，无法解析则返回 None
    """
    box_id = box.get('boxId') or ''
    if box_id:
        shipment_id = re.sub(r'U\d+$', '', box_id)
        if shipment_id and shipment_id != box_id:
            return shipment_id
    ext_id = box.get('externalContainerIdentifier') or ''
    if ext_id and not re.search(r'U\d+$', ext_id):
        return ext_id
    return None


# =============================================================================
# 1. 前端页面接口（路由）
# =============================================================================

@amazon_inbound_plans_bp.route('/amazon/inbound-shipments', methods=['GET'])
@login_required
@permission_required('amazon_inbound_plans:page')
def amazon_inbound_shipments():
    """
    查询入库计划货件列表（连表详情）
    查询参数：shop_id, inbound_plan_id, shipment_confirmation_id,
             amazon_reference_id, destination_warehouse_id, status, shipment_name, page, page_size
    """
    try:
        shop_id = _require_shop_id()
        inbound_plan_id = request.args.get('inbound_plan_id', '').strip() or None
        shipment_confirmation_id = request.args.get('shipment_confirmation_id', '').strip() or None
        amazon_reference_id = request.args.get('amazon_reference_id', '').strip() or None
        destination_warehouse_id = request.args.get('destination_warehouse_id', '').strip() or None
        status = request.args.get('status', '').strip() or None
        shipment_name = request.args.get('shipment_name', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = get_inbound_shipments_list_from_db(
            shop_id=shop_id,
            inbound_plan_id=inbound_plan_id,
            shipment_confirmation_id=shipment_confirmation_id,
            amazon_reference_id=amazon_reference_id,
            destination_warehouse_id=destination_warehouse_id,
            status=status,
            shipment_name=shipment_name,
            page=page,
            page_size=page_size
        )

        return jsonify({"status": "success", "data": result})

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Inbound Shipments DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/inbound-shipments/<shipment_id>/detail', methods=['GET'])
@login_required
@permission_required('amazon_inbound_plans:page')
def amazon_inbound_shipment_detail(shipment_id):
    """
    查询货件详情
    查询参数：shop_id（必填）
    """
    try:
        shop_id = _require_shop_id()
        result = get_inbound_shipment_detail_from_db(shop_id=shop_id, shipment_id=shipment_id)

        if not result:
            return jsonify({"status": "error", "message": "未找到货件详情"}), 404

        return jsonify({"status": "success", "data": result})

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Inbound Shipment Detail DB] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/inbound-plan-boxes', methods=['GET'])
@login_required
@permission_required('amazon_inbound_plans:page')
def amazon_inbound_plan_boxes_by_shipment():
    """
    根据货件编号查询入库计划箱子列表详情
    查询参数：shop_id（必填）, shipment_id（必填）, page, page_size
    """
    try:
        shop_id = _require_shop_id()
        shipment_id = request.args.get('shipment_id', '').strip() or None
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if not shipment_id:
            return jsonify({"status": "error", "message": "shipment_id 必填"}), 400

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        result = get_inbound_plan_boxes_by_shipment_id_from_db(
            shop_id=shop_id, shipment_id=shipment_id, page=page, page_size=page_size
        )

        return jsonify({"status": "success", "data": result})

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Inbound Plan Boxes By Shipment] 查询异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# 2. 前端调用同步接口（路由）
# =============================================================================

@amazon_inbound_plans_bp.route('/amazon/sync/inbound-shipments', methods=['POST'])
@login_required
@permission_required('amazon_inbound_plans:sync')
def sync_amazon_inbound_shipments():
    """
    一键同步最新货件数据
    请求体：{ shop_id }
    逻辑：仅同步2天内创建的 ACTIVE 入库计划（全量：货件列表 + 箱子 + 详情）
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        result = _sync_active_inbound_shipments_full(shop_id=shop_id)

        return jsonify({
            "status": "success",
            "message": (
                f"同步完成，2天内入库计划 {result.get('plan_count', 0)} 个"
                f"，货件 {result['shipments_synced']} 条"
                f"，箱子 {result.get('boxes_synced', 0)} 条"
                f"，详情 {result.get('details_synced', 0)} 条"
            ),
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Sync] 货件全量同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/sync/inbound-plans/<plan_id>/boxes', methods=['POST'])
@login_required
@permission_required('amazon_inbound_plans:sync')
def sync_amazon_inbound_plan_boxes(plan_id):
    """
    手动触发指定入库计划的箱子数据同步
    请求体：{ shop_id }
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        result = _sync_inbound_plan_boxes(shop_id=shop_id, plan_id=plan_id)

        return jsonify({
            "status": "success",
            "message": f"同步完成，共处理 {result.get('synced_count', 0)} 条",
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Sync] 入库计划箱子同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@amazon_inbound_plans_bp.route('/amazon/sync/inbound-shipments/<shipment_id>/detail', methods=['POST'])
@login_required
@permission_required('amazon_inbound_plans:sync')
def sync_amazon_inbound_shipment_detail(shipment_id):
    """
    手动触发单个货件详情同步
    请求体：{ shop_id }
    """
    try:
        data = request.get_json() or {}
        shop_id = _require_shop_id_from_body(data)

        # 从数据库查询该货件对应的入库计划ID
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT inbound_plan_id FROM amazon_inbound_shipments
                    WHERE shop_id = %s AND shipment_id = %s LIMIT 1
                    """,
                    (shop_id, shipment_id)
                )
                row = cursor.fetchone()
                if not row:
                    return jsonify({
                        "status": "error",
                        "message": "未找到该货件对应的入库计划，请先同步货件列表"
                    }), 404
                plan_id = row['inbound_plan_id']
        finally:
            conn.close()

        result = _sync_inbound_shipment_detail(
            shop_id=shop_id, plan_id=plan_id, shipment_id=shipment_id
        )

        return jsonify({
            "status": "success" if not result.get('error') else "error",
            "message": "同步完成" if not result.get('error') else result['error'],
            "data": result
        })

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Amazon Sync] 货件详情同步异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# =============================================================================
# 3. 各种同步方法（供路由和 cron 复用）
# =============================================================================

def _sync_inbound_plans(shop_id, status=None):
    """同步入库计划列表（自动处理分页）"""
    client = get_sp_api_client(shop_id=shop_id)
    all_plans = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Inbound Plans Sync][shop_id={shop_id}] 正在获取第 {page} 页...")

            result = client.list_inbound_plans(
                marketplace_id=client.marketplace_id,
                status=status,
                page_size=20,
                pagination_token=next_token
            )

            plans = result.get('inboundPlans', [])
            all_plans.extend(plans)

            next_token = result.get('pagination', {}).get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_inbound_plans_to_db(shop_id, client.marketplace_id, all_plans)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_plans),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_plans),
            "error": str(e)
        }


def _sync_inbound_plan_shipments(shop_id, plan_id):
    """同步指定入库计划的货件列表"""
    client = get_sp_api_client(shop_id=shop_id)

    try:
        print(f"[Inbound Plan Shipments Sync][shop_id={shop_id}] Plan {plan_id} 正在获取货件列表...")
        result = client._request("GET", f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}")
        shipments = result.get("shipments", [])

        synced_count, error = sync_inbound_shipments_to_db(shop_id, plan_id, shipments)

        return {
            "synced_count": synced_count,
            "total_fetched": len(shipments),
            "error": error,
            "shipment_ids": [s.get("shipmentId") for s in shipments]
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": 0,
            "error": str(e),
            "shipment_ids": []
        }


def _sync_inbound_plan_boxes(shop_id, plan_id):
    """同步指定入库计划的箱子列表（自动处理分页）"""
    client = get_sp_api_client(shop_id=shop_id)
    all_boxes = []
    next_token = None
    page = 0

    try:
        while True:
            page += 1
            print(f"[Inbound Plan Boxes Sync][shop_id={shop_id}] Plan {plan_id} 正在获取第 {page} 页...")

            result = client.list_inbound_plan_boxes(
                inbound_plan_id=plan_id,
                page_size=100,
                pagination_token=next_token
            )

            boxes = result.get('boxes', [])
            all_boxes.extend(boxes)

            next_token = result.get('pagination', {}).get('nextToken')
            if not next_token:
                break

            time.sleep(0.5)

        synced_count, error = sync_inbound_plan_boxes_to_db(shop_id, plan_id, all_boxes)

        return {
            "synced_count": synced_count,
            "total_fetched": len(all_boxes),
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": len(all_boxes),
            "error": str(e)
        }


def _sync_inbound_shipment_detail(shop_id, plan_id, shipment_id):
    """同步指定货件的详情"""
    client = get_sp_api_client(shop_id=shop_id)

    try:
        print(f"[Inbound Shipment Detail Sync][shop_id={shop_id}] Plan {plan_id} Shipment {shipment_id} 正在获取详情...")
        detail = client._request(
            "GET",
            f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}/shipments/{shipment_id}"
        )

        synced_count, error = sync_inbound_shipment_detail_to_db(shop_id, plan_id, shipment_id, detail)

        # 同步仓库代码到 fba_warehouses
        destination = detail.get("destination", {})
        wid = destination.get("warehouseId")
        if wid:
            _sync_warehouse_to_db(warehouse_id=wid, marketplace_id=client.marketplace_id)

        return {
            "synced_count": synced_count,
            "total_fetched": 1,
            "error": error
        }

    except Exception as e:
        return {
            "synced_count": 0,
            "total_fetched": 0,
            "error": str(e)
        }


def _sync_active_inbound_shipments_full(shop_id):
    """
    一键同步 ACTIVE 状态的入库计划
    策略：
      仅同步2天内创建的入库计划，全量同步（货件列表 + 箱子 + 货件详情）
      2天之前的入库计划不再同步
    """
    # 1. 同步入库计划列表（刷新状态）
    plans_result = _sync_inbound_plans(shop_id=shop_id, status='ACTIVE')

    # 2. 查询2天内创建的 ACTIVE 入库计划
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT inbound_plan_id FROM amazon_inbound_plans
                   WHERE shop_id = %s AND status = 'ACTIVE'
                   AND created_at >= NOW() - INTERVAL 2 DAY""",
                (shop_id,)
            )
            recent_plan_ids = [row['inbound_plan_id'] for row in cursor.fetchall()]
    finally:
        conn.close()

    if not recent_plan_ids:
        return {
            "plans": plans_result,
            "shipments_synced": 0,
            "boxes_synced": 0,
            "details_synced": 0,
            "total_shipments": 0,
            "plan_count": 0,
            "plan_ids": [],
            "shipment_errors": [],
            "box_errors": [],
            "detail_errors": []
        }

    total_shipments_synced = 0
    shipment_sync_errors = []
    all_shipment_entries = []

    # 3. 同步货件列表
    for plan_id in recent_plan_ids:
        result = _sync_inbound_plan_shipments(shop_id=shop_id, plan_id=plan_id)
        total_shipments_synced += result.get('synced_count', 0)
        if result.get('error'):
            shipment_sync_errors.append({"plan_id": plan_id, "error": result['error']})
        for sid in result.get('shipment_ids', []):
            all_shipment_entries.append((plan_id, sid))
        time.sleep(0.3)

    # 4. 同步箱子
    total_boxes_synced = 0
    box_sync_errors = []
    for plan_id in recent_plan_ids:
        result = _sync_inbound_plan_boxes(shop_id=shop_id, plan_id=plan_id)
        total_boxes_synced += result.get('synced_count', 0)
        if result.get('error'):
            box_sync_errors.append({"plan_id": plan_id, "error": result['error']})
        time.sleep(0.3)

    # 5. 同步货件详情
    total_details_synced = 0
    detail_sync_errors = []
    for plan_id, shipment_id in all_shipment_entries:
        result = _sync_inbound_shipment_detail(shop_id=shop_id, plan_id=plan_id, shipment_id=shipment_id)
        total_details_synced += result.get('synced_count', 0)
        if result.get('error'):
            detail_sync_errors.append({
                "plan_id": plan_id,
                "shipment_id": shipment_id,
                "error": result['error']
            })
        time.sleep(0.3)

    return {
        "plans": plans_result,
        "shipments_synced": total_shipments_synced,
        "boxes_synced": total_boxes_synced,
        "details_synced": total_details_synced,
        "total_shipments": len(all_shipment_entries),
        "plan_count": len(recent_plan_ids),
        "plan_ids": recent_plan_ids,
        "shipment_errors": shipment_sync_errors,
        "box_errors": box_sync_errors,
        "detail_errors": detail_sync_errors
    }


def _sync_all_inbound_plan_shipments(shop_id, status=None):
    """
    批量同步所有入库计划的货件列表
    供 cron 每 30 分钟调用
    """
    plan_ids = get_inbound_plan_ids_from_db(shop_id=shop_id, status=status)
    if not plan_ids:
        return {"total_synced": 0, "total_plans": 0, "errors": []}

    total_synced = 0
    errors = []

    for plan_id in plan_ids:
        result = _sync_inbound_plan_shipments(shop_id=shop_id, plan_id=plan_id)
        total_synced += result.get('synced_count', 0)
        if result.get('error'):
            errors.append({"plan_id": plan_id, "error": result['error']})
        time.sleep(0.3)

    return {
        "total_synced": total_synced,
        "total_plans": len(plan_ids),
        "errors": errors
    }


def _sync_all_inbound_plan_boxes(shop_id, status=None):
    """
    批量同步所有入库计划的箱子列表
    供 cron 每 6 小时调用
    """
    plan_ids = get_inbound_plan_ids_from_db(shop_id=shop_id, status=status)
    if not plan_ids:
        return {"total_synced": 0, "total_plans": 0, "errors": []}

    total_synced = 0
    errors = []

    for plan_id in plan_ids:
        result = _sync_inbound_plan_boxes(shop_id=shop_id, plan_id=plan_id)
        total_synced += result.get('synced_count', 0)
        if result.get('error'):
            errors.append({"plan_id": plan_id, "error": result['error']})
        time.sleep(0.3)

    return {
        "total_synced": total_synced,
        "total_plans": len(plan_ids),
        "errors": errors
    }


def _sync_all_inbound_shipment_details(shop_id, status=None):
    """
    批量同步所有入库计划下所有货件的详情
    供 cron 每 6 小时调用
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT inbound_plan_id, shipment_id
                FROM amazon_inbound_shipments
                WHERE shop_id = %s
            """
            params = [shop_id]
            if status:
                sql += " AND status = %s"
                params.append(status)
            cursor.execute(sql, tuple(params))
            entries = cursor.fetchall()
    finally:
        conn.close()

    if not entries:
        return {"total_synced": 0, "total_shipments": 0, "errors": []}

    total_synced = 0
    errors = []

    for entry in entries:
        plan_id = entry['inbound_plan_id']
        shipment_id = entry['shipment_id']
        result = _sync_inbound_shipment_detail(shop_id=shop_id, plan_id=plan_id, shipment_id=shipment_id)
        total_synced += result.get('synced_count', 0)
        if result.get('error'):
            errors.append({
                "plan_id": plan_id,
                "shipment_id": shipment_id,
                "error": result['error']
            })
        time.sleep(0.3)

    return {
        "total_synced": total_synced,
        "total_shipments": len(entries),
        "errors": errors
    }


def _sync_warehouse_to_db(warehouse_id, marketplace_id):
    """将仓库代码同步到 fba_warehouses（按 marketplace_id）"""
    if not warehouse_id or not marketplace_id:
        return
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO fba_warehouses (warehouse_id, marketplace_id, sync_time)
                VALUES (%s, %s, NOW())
                ON DUPLICATE KEY UPDATE sync_time = NOW()
            """
            cursor.execute(sql, (warehouse_id, marketplace_id))
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[Warehouse Sync] 仓库同步异常: {e}")
    finally:
        conn.close()


# =============================================================================
# 4. 数据库操作方法
# =============================================================================

def sync_inbound_plans_to_db(shop_id, marketplace_id, plans):
    """同步入库计划列表到数据库"""
    if not plans:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for plan in plans:
                source = plan.get('sourceAddress', {})
                marketplace_ids = plan.get('marketplaceIds', [])

                sql = """
                    INSERT INTO amazon_inbound_plans (
                        shop_id, inbound_plan_id, marketplace_id, marketplace_ids, name, status,
                        created_at, last_updated_at,
                        source_address_line1, source_address_line2, source_city,
                        source_company_name, source_country_code, source_email,
                        source_name, source_phone_number, source_postal_code,
                        source_state_or_province_code, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        marketplace_id = VALUES(marketplace_id),
                        marketplace_ids = VALUES(marketplace_ids),
                        name = VALUES(name),
                        status = VALUES(status),
                        created_at = VALUES(created_at),
                        last_updated_at = VALUES(last_updated_at),
                        source_address_line1 = VALUES(source_address_line1),
                        source_address_line2 = VALUES(source_address_line2),
                        source_city = VALUES(source_city),
                        source_company_name = VALUES(source_company_name),
                        source_country_code = VALUES(source_country_code),
                        source_email = VALUES(source_email),
                        source_name = VALUES(source_name),
                        source_phone_number = VALUES(source_phone_number),
                        source_postal_code = VALUES(source_postal_code),
                        source_state_or_province_code = VALUES(source_state_or_province_code),
                        sync_time = NOW()
                """

                params = (
                    shop_id,
                    plan.get('inboundPlanId'),
                    marketplace_id,
                    json.dumps(marketplace_ids) if marketplace_ids else '[]',
                    plan.get('name'),
                    plan.get('status'),
                    _iso_to_datetime(plan.get('createdAt')),
                    _iso_to_datetime(plan.get('lastUpdatedAt')),
                    source.get('addressLine1'),
                    source.get('addressLine2'),
                    source.get('city'),
                    source.get('companyName'),
                    source.get('countryCode'),
                    source.get('email'),
                    source.get('name'),
                    source.get('phoneNumber'),
                    source.get('postalCode'),
                    source.get('stateOrProvinceCode'),
                )

                cursor.execute(sql, params)
                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def sync_inbound_shipments_to_db(shop_id, plan_id, shipments):
    """同步入库计划货件列表到数据库"""
    if not shipments:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for shipment in shipments:
                sql = """
                    INSERT INTO amazon_inbound_shipments (
                        shop_id, inbound_plan_id, shipment_id, status,
                        sync_time
                    ) VALUES (
                        %s, %s, %s, %s,
                        NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        status = VALUES(status),
                        sync_time = NOW()
                """

                params = (
                    shop_id,
                    plan_id,
                    shipment.get("shipmentId"),
                    shipment.get("status"),
                )

                cursor.execute(sql, params)
                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def sync_inbound_plan_boxes_to_db(shop_id, plan_id, boxes):
    """同步入库计划箱子列表到数据库"""
    if not boxes:
        return 0, None

    conn = get_db_connection()
    count = 0
    try:
        with conn.cursor() as cursor:
            for box in boxes:
                dest = box.get('destinationRegion', {})
                dims = box.get('dimensions', {})
                weight = box.get('weight', {})
                items = box.get('items', [])

                shipment_id = _extract_shipment_id_from_box(box)

                sql = """
                    INSERT INTO amazon_inbound_plan_boxes (
                        shop_id, inbound_plan_id, box_id, package_id, shipment_id, content_information_source,
                        destination_region_country_code, destination_region_state, destination_region_warehouse_id,
                        dimensions_height, dimensions_length, dimensions_unit, dimensions_width,
                        external_container_identifier, external_container_identifier_type,
                        quantity, template_name, weight_unit, weight_value,
                        items_json, sync_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        shipment_id = VALUES(shipment_id),
                        content_information_source = VALUES(content_information_source),
                        destination_region_country_code = VALUES(destination_region_country_code),
                        destination_region_state = VALUES(destination_region_state),
                        destination_region_warehouse_id = VALUES(destination_region_warehouse_id),
                        dimensions_height = VALUES(dimensions_height),
                        dimensions_length = VALUES(dimensions_length),
                        dimensions_unit = VALUES(dimensions_unit),
                        dimensions_width = VALUES(dimensions_width),
                        external_container_identifier = VALUES(external_container_identifier),
                        external_container_identifier_type = VALUES(external_container_identifier_type),
                        quantity = VALUES(quantity),
                        template_name = VALUES(template_name),
                        weight_unit = VALUES(weight_unit),
                        weight_value = VALUES(weight_value),
                        items_json = VALUES(items_json),
                        sync_time = NOW()
                """

                params = (
                    shop_id,
                    plan_id,
                    box.get('boxId'),
                    box.get('packageId'),
                    shipment_id,
                    box.get('contentInformationSource'),
                    dest.get('countryCode'),
                    dest.get('state'),
                    dest.get('warehouseId'),
                    dims.get('height'),
                    dims.get('length'),
                    dims.get('unitOfMeasurement'),
                    dims.get('width'),
                    box.get('externalContainerIdentifier'),
                    box.get('externalContainerIdentifierType'),
                    box.get('quantity', 0),
                    box.get('templateName'),
                    weight.get('unit'),
                    weight.get('value'),
                    json.dumps(items) if items else '[]',
                )

                cursor.execute(sql, params)
                count += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        return count, str(e)
    finally:
        conn.close()

    return count, None


def sync_inbound_shipment_detail_to_db(shop_id, plan_id, shipment_id, detail):
    """同步货件详情到数据库"""
    if not detail:
        return 0, None

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            destination = detail.get("destination", {})
            source = detail.get("source", {})
            selected_window = detail.get("selectedDeliveryWindow", {})
            tracking = detail.get("trackingDetails", {})
            dates = detail.get("dates", {})

            sql = """
                INSERT INTO amazon_inbound_shipments_detail (
                    shop_id, inbound_plan_id, shipment_id, shipment_confirmation_id,
                    amazon_reference_id, name, status,
                    placement_option_id, selected_transportation_option_id,
                    destination_warehouse_id, destination_type,
                    destination_address_json,
                    source_type, source_address_json,
                    selected_delivery_window_json,
                    tracking_details_json,
                    dates_json,
                    sync_time
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s,
                    %s,
                    %s,
                    %s,
                    NOW()
                )
                ON DUPLICATE KEY UPDATE
                    shipment_confirmation_id = VALUES(shipment_confirmation_id),
                    amazon_reference_id = VALUES(amazon_reference_id),
                    name = VALUES(name),
                    status = VALUES(status),
                    placement_option_id = VALUES(placement_option_id),
                    selected_transportation_option_id = VALUES(selected_transportation_option_id),
                    destination_warehouse_id = VALUES(destination_warehouse_id),
                    destination_type = VALUES(destination_type),
                    destination_address_json = VALUES(destination_address_json),
                    source_type = VALUES(source_type),
                    source_address_json = VALUES(source_address_json),
                    selected_delivery_window_json = VALUES(selected_delivery_window_json),
                    tracking_details_json = VALUES(tracking_details_json),
                    dates_json = VALUES(dates_json),
                    sync_time = NOW()
            """

            params = (
                shop_id,
                plan_id,
                shipment_id,
                detail.get("shipmentConfirmationId"),
                detail.get("amazonReferenceId"),
                detail.get("name"),
                detail.get("status"),
                detail.get("placementOptionId"),
                detail.get("selectedTransportationOptionId"),
                destination.get("warehouseId"),
                destination.get("destinationType"),
                json.dumps(destination.get("address")) if destination.get("address") else None,
                source.get("sourceType"),
                json.dumps(source.get("address")) if source.get("address") else None,
                json.dumps(selected_window) if selected_window else None,
                json.dumps(tracking) if tracking else None,
                json.dumps(dates) if dates else None,
            )

            cursor.execute(sql, params)
            conn.commit()
            return 1, None
    except Exception as e:
        conn.rollback()
        return 0, str(e)
    finally:
        conn.close()


def get_inbound_shipments_list_from_db(shop_id, inbound_plan_id=None, shipment_confirmation_id=None,
                                       amazon_reference_id=None, destination_warehouse_id=None,
                                       status=None, shipment_name=None, page=1, page_size=20):
    """
    从数据库分页查询入库计划货件列表（amazon_inbound_shipments 连表 amazon_inbound_shipments_detail）
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["s.shop_id = %s"]
            params = [shop_id]

            if inbound_plan_id:
                conditions.append("s.inbound_plan_id = %s")
                params.append(inbound_plan_id)
            if shipment_confirmation_id:
                conditions.append("d.shipment_confirmation_id = %s")
                params.append(shipment_confirmation_id)
            if amazon_reference_id:
                conditions.append("d.amazon_reference_id = %s")
                params.append(amazon_reference_id)
            if destination_warehouse_id:
                conditions.append("d.destination_warehouse_id = %s")
                params.append(destination_warehouse_id)
            if status:
                conditions.append("s.status = %s")
                params.append(status)
            if shipment_name:
                conditions.append("d.name LIKE %s")
                params.append(f"%{shipment_name}%")

            where_clause = " AND ".join(conditions)

            count_sql = f"""
                SELECT COUNT(*) as total
                FROM amazon_inbound_shipments s
                LEFT JOIN amazon_inbound_shipments_detail d ON s.shipment_id = d.shipment_id AND d.shop_id = s.shop_id
                LEFT JOIN amazon_inbound_plans p ON s.inbound_plan_id = p.inbound_plan_id AND s.shop_id = p.shop_id
                WHERE {where_clause}
            """
            cursor.execute(count_sql, tuple(params))
            total = cursor.fetchone()["total"]

            offset = (page - 1) * page_size
            sql = f"""
                SELECT
                    s.inbound_plan_id,
                    s.shipment_id,
                    s.status AS shipment_status,
                    s.sync_time AS shipment_sync_time,
                    d.shipment_confirmation_id,
                    d.amazon_reference_id,
                    d.name AS shipment_name,
                    d.status AS detail_status,
                    d.placement_option_id,
                    d.selected_transportation_option_id,
                    d.destination_warehouse_id,
                    d.destination_type,
                    d.destination_address_json,
                    d.source_type,
                    d.source_address_json,
                    d.selected_delivery_window_json,
                    d.tracking_details_json,
                    d.dates_json,
                    d.sync_time AS detail_sync_time,
                    p.created_at AS plan_created_at,
                    (SELECT COUNT(*) FROM amazon_inbound_plan_boxes b WHERE b.shipment_id = d.shipment_confirmation_id AND b.shop_id = s.shop_id) AS box_count
                FROM amazon_inbound_shipments s
                LEFT JOIN amazon_inbound_shipments_detail d ON s.shipment_id = d.shipment_id AND d.shop_id = s.shop_id
                LEFT JOIN amazon_inbound_plans p ON s.inbound_plan_id = p.inbound_plan_id AND s.shop_id = p.shop_id
                WHERE {where_clause}
                ORDER BY p.created_at DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()


def get_inbound_shipment_detail_from_db(shop_id, shipment_id):
    """从数据库查询货件详情"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM amazon_inbound_shipments_detail WHERE shop_id = %s AND shipment_id = %s"
            cursor.execute(sql, (shop_id, shipment_id))
            return cursor.fetchone()
    finally:
        conn.close()


def get_inbound_plan_boxes_by_shipment_id_from_db(shop_id, shipment_id=None, page=1, page_size=20):
    """根据货件编号从数据库分页查询入库计划箱子列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s"]
            params = [shop_id]

            if shipment_id:
                conditions.append("shipment_id = %s")
                params.append(shipment_id)

            where_clause = " AND ".join(conditions)

            cursor.execute(
                f"SELECT COUNT(*) as total FROM amazon_inbound_plan_boxes WHERE {where_clause}",
                tuple(params)
            )
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            sql = f"""
                SELECT * FROM amazon_inbound_plan_boxes
                WHERE {where_clause}
                ORDER BY sync_time DESC
                LIMIT %s OFFSET %s
            """
            cursor.execute(sql, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()

            _enrich_boxes_with_product_names(rows)

            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()


def get_inbound_plan_ids_from_db(shop_id, status=None):
    """从数据库获取所有入库计划ID列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s"]
            params = [shop_id]

            if status:
                conditions.append("status = %s")
                params.append(status)

            where_clause = " AND ".join(conditions)
            sql = f"SELECT inbound_plan_id FROM amazon_inbound_plans WHERE {where_clause}"
            cursor.execute(sql, tuple(params))
            return [row['inbound_plan_id'] for row in cursor.fetchall()]
    finally:
        conn.close()


def _enrich_boxes_with_product_names(rows):
    """
    为箱子列表中的 items_json 解析并添加 SKU 中文名
    根据 msku 关联 products 表，优先取 declare_name_cn，其次 product_name
    """
    if not rows:
        return

    all_mskus = set()
    for row in rows:
        items_str = row.get('items_json') or '[]'
        try:
            items = json.loads(items_str)
            if isinstance(items, list):
                for item in items:
                    msku = item.get('msku')
                    if msku:
                        all_mskus.add(msku)
        except Exception:
            continue

    if not all_mskus:
        for row in rows:
            row['items'] = []
        return

    product_map = {}
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(all_mskus))
            sql = f"""
                SELECT seller_sku, product_name, declare_name_cn
                FROM products
                WHERE seller_sku IN ({placeholders})
            """
            cursor.execute(sql, tuple(all_mskus))
            for prod in cursor.fetchall():
                name = prod.get('declare_name_cn') or prod.get('product_name') or ''
                product_map[prod['seller_sku']] = name
    finally:
        conn.close()

    for row in rows:
        items_str = row.get('items_json') or '[]'
        try:
            items = json.loads(items_str)
            if isinstance(items, list):
                for item in items:
                    item['sku_name_cn'] = product_map.get(item.get('msku'), '')
                row['items'] = items
            else:
                row['items'] = []
        except Exception:
            row['items'] = []
