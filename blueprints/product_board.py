"""
选品看板模块
同 ASIN 可多次导入，每次一行记录导入时间，列表按 ASIN 去重展示最新一条。
趋势查看同一 ASIN 所有历史记录。
"""
from flask import Blueprint, request, jsonify, Response
from blueprints.user_auth import login_required, permission_required
from services.mysql_service import get_db_connection
from dotenv import load_dotenv
import json
import csv
import io
import os
import openpyxl
from datetime import datetime

load_dotenv(override=True)
BASE_URL = os.getenv('BASE_URL', '').rstrip('/')

product_board_bp = Blueprint('product_board', __name__, url_prefix='/api')

IMAGE_DIR = os.path.join('static', 'product_board')
os.makedirs(IMAGE_DIR, exist_ok=True)

HEADER_MAP = {
    'asin1': 'asin',
    '产品中文名称': 'product_name_cn',
    '图片': 'image_url',
    '销售链接': 'sales_url',
    'AMZ状态': 'amazon_status',
    '评分': 'rating',
    '售价(USD)': 'selling_price_usd',
    '促销价(USD)': 'promo_price_usd',
    '建议售价(29%)': 'suggested_price_usd',
    '采购成本(RMB)': 'purchase_cost_rmb',
    '1天销量': 'sales_1d',
    '3天销量': 'sales_3d',
    '7天销量': 'sales_7d',
    '14天销量': 'sales_14d',
    '30天销量': 'sales_30d',
    '7天毛利(USD)': 'gross_profit_7d_usd',
    '30天毛利(USD)': 'gross_profit_30d_usd',
    '7天毛利率': 'profit_margin_7d',
    '30天毛利率': 'profit_margin_30d',
    '30天退款率': 'refund_rate_30d',
    '60天退款率': 'refund_rate_60d',
    '当月acos': 'acos_current_month',
    '当月tacos': 'tacos_current_month',
    '昨日广告费': 'ad_spend_yesterday',
    '当月广告费': 'ad_spend_current_month',
    '近30天广告费': 'ad_spend_30d',
    '产品开发时间': 'dev_time',
    '第一次到货时间': 'first_arrival_time',
    '最早到货时间': 'earliest_arrival_time',
    'FBA到货时间': 'fba_arrival_time',
}

ALL_COLUMNS = list(HEADER_MAP.values())

NUMERIC_FIELDS = {
    'rating', 'selling_price_usd', 'promo_price_usd', 'suggested_price_usd',
    'purchase_cost_rmb', 'sales_1d', 'sales_3d', 'sales_7d', 'sales_14d',
    'sales_30d', 'gross_profit_7d_usd', 'gross_profit_30d_usd',
    'profit_margin_7d', 'profit_margin_30d', 'refund_rate_30d',
    'refund_rate_60d', 'acos_current_month', 'tacos_current_month',
    'ad_spend_yesterday', 'ad_spend_current_month', 'ad_spend_30d',
}

PERCENT_FIELDS_RAW = {
    'profit_margin_7d', 'profit_margin_30d', 'refund_rate_30d', 'refund_rate_60d',
}
PERCENT_FIELDS_DECIMAL = {
    'acos_current_month', 'tacos_current_month',
}

# 最新一条 per ASIN 的子查询条件，多处复用
LATEST_PER_ASIN = """
    id = (SELECT MAX(id) FROM product_board t2 WHERE t2.asin = product_board.asin)
"""


def _get_conn():
    return get_db_connection()


def _parse_numeric(value):
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return value
    s = str(value).replace(',', '').replace('$', '').replace('￥', '').replace('%', '').strip()
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except (ValueError, TypeError):
        return None


def _parse_file_rows(file):
    filename = (file.filename or '').lower()
    print(f"[Product Board] 收到文件: {filename}", flush=True)
    raw = file.read()
    print(f"[Product Board] 文件大小: {len(raw)} bytes", flush=True)

    if filename.endswith(('.xlsx', '.xls')):
        print("[Product Board] 开始 openpyxl read_only 加载...", flush=True)
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        print("[Product Board] openpyxl 加载完成", flush=True)
        ws = wb.active
        rows_iter = ws.iter_rows()
        header_row = next(rows_iter, None)
        if not header_row:
            wb.close()
            return None, None, "Excel 文件为空"
        headers = [str(c.value or '') for c in header_row]
        rows = []
        for r in rows_iter:
            vals = [c.value for c in r]
            rows.append(dict(zip(headers, vals)))
        wb.close()
        print(f"[Product Board] 读取到 {len(rows)} 行数据", flush=True)

        print("[Product Board] 开始 zip 图片提取...", flush=True)
        image_map = _extract_images_from_xlsx(raw, len(rows))
        print(f"[Product Board] 图片提取完成，共 {len(image_map)} 张", flush=True)
        return rows, image_map, None

    # CSV
    content = None
    for enc in ('utf-8-sig', 'utf-8', 'gbk', 'gb2312'):
        try:
            content = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if content is None:
        return None, None, "文件编码无法识别"
    rows = list(csv.DictReader(io.StringIO(content)))
    print(f"[Product Board] CSV 解析完成，共 {len(rows)} 行", flush=True)
    return rows, {}, None


def _extract_images_from_xlsx(raw, row_count):
    import zipfile
    from xml.etree import ElementTree

    image_map = {}
    try:
        print("[Product Board] 打开 zip...", flush=True)
        zf = zipfile.ZipFile(io.BytesIO(raw))
        names = zf.namelist()
        print(f"[Product Board] zip 内共 {len(names)} 个文件", flush=True)

        drawing_path = 'xl/drawings/drawing1.xml'
        drawing_rels_path = 'xl/drawings/_rels/drawing1.xml.rels'

        if drawing_path not in names:
            print("[Product Board] xlsx 无 drawing 文件，没有嵌入图片", flush=True)
            zf.close()
            return image_map

        rels = {}
        if drawing_rels_path in names:
            print("[Product Board] 解析 drawing rels...", flush=True)
            rel_xml = ElementTree.fromstring(zf.read(drawing_rels_path))
            ns = '{http://schemas.openxmlformats.org/package/2006/relationships}'
            for rel in rel_xml:
                rid = rel.get('Id')
                target = rel.get('Target', '')
                rels[rid] = target.replace('../', 'xl/')
            print(f"[Product Board] 找到 {len(rels)} 个 rel 映射", flush=True)

        print("[Product Board] 解析 drawing XML...", flush=True)
        draw_xml = ElementTree.fromstring(zf.read(drawing_path))
        nsmap = {
            'xdr': 'http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing',
            'a': 'http://schemas.openxmlformats.org/drawingml/2006/main',
            'r': 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
        }

        anchor_count = 0
        for anchor in draw_xml.iter():
            if 'oneCellAnchor' in anchor.tag or 'twoCellAnchor' in anchor.tag:
                anchor_count += 1
                from_el = anchor.find('xdr:from', nsmap)
                if from_el is None:
                    continue
                row_el = from_el.find('xdr:row', nsmap)
                if row_el is None or row_el.text is None:
                    continue
                anchor_row = int(row_el.text)

                blip = anchor.find('.//a:blip', nsmap)
                if blip is None:
                    continue
                embed = blip.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}embed')
                if not embed or embed not in rels:
                    continue

                img_path = rels[embed]
                if img_path in names:
                    data_row_idx = anchor_row - 1  # 去掉表头
                    if 0 <= data_row_idx < row_count:
                        image_map[data_row_idx] = zf.read(img_path)

        zf.close()
        print(f"[Product Board] 遍历 {anchor_count} 个锚点，提取到 {len(image_map)} 张嵌入图片", flush=True)
    except Exception as e:
        print(f"[Product Board] xlsx 图片提取失败: {e}", flush=True)

    return image_map


def _map_row(row):
    mapped = {}
    for key, value in row.items():
        col = HEADER_MAP.get(key, key)
        if col in ALL_COLUMNS:
            if col in NUMERIC_FIELDS:
                mapped[col] = _parse_numeric(value)
            else:
                mapped[col] = str(value).strip() if value else ''
    return mapped


def _save_image_bytes(asin, image_bytes):
    try:
        if image_bytes[:3] == b'\xff\xd8\xff':
            ext = 'jpg'
        elif image_bytes[:4] == b'\x89PNG':
            ext = 'png'
        elif image_bytes[:6] in (b'GIF87a', b'GIF89a'):
            ext = 'gif'
        elif image_bytes[:4] == b'RIFF':
            ext = 'webp'
        else:
            ext = 'jpg'
        filename = f"{asin}.{ext}"
        filepath = os.path.join(IMAGE_DIR, filename)
        with open(filepath, 'wb') as f:
            f.write(image_bytes)
        print(f"[Product Board] 图片已保存: {asin} -> {filepath}")
        return f"{BASE_URL}/static/product_board/{filename}"
    except Exception as e:
        print(f"[Product Board] 图片保存失败 {asin}: {e}")
    return None


# ==================== 导入 ====================

@product_board_bp.route('/product-board/import', methods=['POST'])
@login_required
@permission_required('product_board:import')
def import_product_board():
    print(f"[Product Board] ====== 开始导入 ======", flush=True)
    inserted = 0
    skipped = 0
    errors = []

    try:
        image_map = {}
        if 'file' in request.files:
            print("[Product Board] 检测到文件上传，开始解析...", flush=True)
            rows, image_map, err = _parse_file_rows(request.files['file'])
            if err:
                return jsonify({"status": "error", "message": err}), 400
        else:
            body = request.get_json() or {}
            rows = body.get('data', [])
            if not rows:
                return jsonify({"status": "error", "message": "请上传文件或传入 data 数组"}), 400

        if not rows:
            return jsonify({"status": "error", "message": "无数据"}), 400

        conn = _get_conn()
        image_saved = 0
        asins_got_image = set()
        updated = 0
        try:
            with conn.cursor() as cursor:
                # 预取所有已有本地图片的 ASIN → image_url
                existing_images = {}
                cursor.execute("""
                    SELECT asin, image_url FROM product_board
                    WHERE image_url IS NOT NULL AND image_url != '' AND image_url LIKE '%%/static/product_board/%%'
                """)
                for r in cursor.fetchall():
                    if r['asin'] not in existing_images:
                        existing_images[r['asin']] = r['image_url']

                # 预取已上架的 ASIN
                cursor.execute("SELECT DISTINCT asin FROM product_board WHERE is_listed = 1")
                listed_asins = {r['asin'] for r in cursor.fetchall()}

                # 预取今日已有数据的 ASIN → id，用于同日期覆盖
                cursor.execute("""
                    SELECT asin, id FROM product_board
                    WHERE DATE(created_at) = CURDATE()
                """)
                today_ids = {r['asin']: r['id'] for r in cursor.fetchall()}

                for i, row in enumerate(rows):
                    mapped = _map_row(row)
                    asin = mapped.get('asin')
                    if not asin:
                        skipped += 1
                        continue

                    # 图片：已有 URL → 复用；没有 → 从 Excel 提取
                    if asin in existing_images:
                        mapped['image_url'] = existing_images[asin]
                    elif i in image_map and image_map[i]:
                        local_url = _save_image_bytes(asin, image_map[i])
                        if local_url:
                            mapped['image_url'] = local_url
                            image_saved += 1
                            asins_got_image.add(asin)
                            existing_images[asin] = local_url

                    # 上架状态：新数据继承老数据的上架状态
                    if asin in listed_asins:
                        mapped['is_listed'] = 1

                    # 同日期覆盖：今日已有该 ASIN → UPDATE；否则 INSERT
                    columns = [k for k in mapped.keys() if k in ALL_COLUMNS]
                    if asin in today_ids:
                        set_clause = ', '.join([f"{c} = %s" for c in columns])
                        sql = f"UPDATE product_board SET {set_clause} WHERE id = %s"
                        cursor.execute(sql, [mapped[c] for c in columns] + [today_ids[asin]])
                        updated += 1
                    else:
                        placeholders = ', '.join(['%s'] * len(columns))
                        sql = f"INSERT INTO product_board ({', '.join(columns)}) VALUES ({placeholders})"
                        cursor.execute(sql, [mapped[c] for c in columns])
                        inserted += 1

                # 新图片：补全该 ASIN 历史行中空的 image_url
                for asin in asins_got_image:
                    cursor.execute("""
                        UPDATE product_board SET image_url = %s
                        WHERE asin = %s AND (image_url IS NULL OR image_url = '')
                    """, (existing_images[asin], asin))

                conn.commit()
        finally:
            conn.close()

        return jsonify({
            "status": "success",
            "message": f"导入完成：新增 {inserted} 条，更新 {updated} 条，保存图片 {image_saved} 张，跳过 {skipped} 条",
            "data": {"inserted": inserted, "updated": updated, "image_saved": image_saved, "skipped": skipped, "errors": errors[:20]}
        })

    except Exception as e:
        print(f"[Product Board] 导入异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 列表（按 ASIN 去重，展示最新一条） ====================

@product_board_bp.route('/product-board', methods=['GET'])
@login_required
@permission_required('product_board:page')
def list_product_board():
    try:
        keyword = request.args.get('keyword', '').strip() or None
        amazon_status = request.args.get('amazon_status', '').strip() or None
        min_sales = request.args.get('min_sales', '').strip() or None
        min_margin = request.args.get('min_margin', '').strip() or None
        is_listed = request.args.get('is_listed', '').strip() or None
        sort_by = request.args.get('sort_by', 'sales_30d')
        sort_dir = request.args.get('sort_dir', 'desc')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        allowed_sort = ALL_COLUMNS + ['id', 'created_at']
        if sort_by not in allowed_sort:
            sort_by = 'sales_30d'
        sort_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                conditions = [LATEST_PER_ASIN]
                params = []

                if keyword:
                    conditions.append("(asin LIKE %s OR product_name_cn LIKE %s)")
                    like = f"%{keyword}%"
                    params.extend([like, like])

                if amazon_status:
                    conditions.append("amazon_status = %s")
                    params.append(amazon_status)

                if min_sales:
                    conditions.append("sales_30d >= %s")
                    params.append(int(min_sales))

                if min_margin:
                    conditions.append("profit_margin_30d >= %s")
                    params.append(float(min_margin))

                if is_listed is not None:
                    conditions.append("is_listed = %s")
                    params.append(1 if is_listed.lower() in ('true', '1') else 0)

                where_clause = " AND ".join(conditions)

                cursor.execute(
                    f"SELECT COUNT(*) as total FROM product_board WHERE {where_clause}",
                    tuple(params)
                )
                total = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                cursor.execute(f"""
                    SELECT * FROM product_board
                    WHERE {where_clause}
                    ORDER BY {sort_by} {sort_dir}
                    LIMIT %s OFFSET %s
                """, tuple(params + [page_size, offset]))
                rows = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "list": rows,
                        "total": total,
                        "page": page,
                        "page_size": page_size
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Product Board] 查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 趋势（同一 ASIN 所有导入记录） ====================

@product_board_bp.route('/product-board/trend', methods=['GET'])
@login_required
@permission_required('product_board:page')
def product_board_trend():
    try:
        asins_raw = request.args.get('asins', '').strip()
        if not asins_raw:
            return jsonify({"status": "error", "message": "asins 不能为空（逗号分隔）"}), 400

        asins = [a.strip() for a in asins_raw.split(',') if a.strip()]

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(asins))
                cursor.execute(f"""
                    SELECT pb.*, p.product_name as system_product_name
                    FROM product_board pb
                    LEFT JOIN products p ON pb.asin = p.asin
                    WHERE pb.asin IN ({placeholders})
                    ORDER BY pb.asin, pb.created_at ASC
                """, tuple(asins))
                rows = cursor.fetchall()

                grouped = {}
                for r in rows:
                    asin = r['asin']
                    if asin not in grouped:
                        grouped[asin] = {
                            'asin': asin,
                            'product_name_cn': r['product_name_cn'],
                            'system_product_name': r.get('system_product_name'),
                            'image_url': r['image_url'],
                            'data_points': []
                        }
                    bt = r['created_at']
                    grouped[asin]['data_points'].append({
                        'id': r['id'],
                        'created_at': bt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(bt, 'strftime') else str(bt),
                        'selling_price_usd': r['selling_price_usd'],
                        'sales_1d': r['sales_1d'],
                        'sales_3d': r['sales_3d'],
                        'sales_7d': r['sales_7d'],
                        'sales_14d': r['sales_14d'],
                        'sales_30d': r['sales_30d'],
                        'gross_profit_30d_usd': r['gross_profit_30d_usd'],
                        'profit_margin_30d': r['profit_margin_30d'],
                        'refund_rate_30d': r['refund_rate_30d'],
                        'ad_spend_30d': r['ad_spend_30d'],
                        'rating': r['rating'],
                        'amazon_status': r['amazon_status'],
                    })

                return jsonify({
                    "status": "success",
                    "data": {
                        "products": list(grouped.values()),
                        "asins": asins
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 趋势查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 详情 ====================

@product_board_bp.route('/product-board/<int:product_id>', methods=['GET'])
@login_required
@permission_required('product_board:page')
def get_product_board_detail(product_id):
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM product_board WHERE id = %s", (product_id,))
                row = cursor.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "记录不存在"}), 404
                return jsonify({"status": "success", "data": row})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 详情异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 删除 ====================

@product_board_bp.route('/product-board/<int:product_id>', methods=['DELETE'])
@login_required
@permission_required('product_board:delete')
def delete_product_board(product_id):
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM product_board WHERE id = %s", (product_id,))
                conn.commit()
                if cursor.rowcount == 0:
                    return jsonify({"status": "error", "message": "记录不存在"}), 404
                return jsonify({"status": "success", "message": "删除成功"})
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 删除异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@product_board_bp.route('/product-board/batch-delete', methods=['POST'])
@login_required
@permission_required('product_board:batch_delete')
def batch_delete_product_board():
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
        if not ids:
            return jsonify({"status": "error", "message": "ids 不能为空"}), 400

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(ids))
                cursor.execute(f"DELETE FROM product_board WHERE id IN ({placeholders})", tuple(ids))
                conn.commit()
                return jsonify({
                    "status": "success",
                    "message": f"已删除 {cursor.rowcount} 条",
                    "data": {"deleted": cursor.rowcount}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 批量删除异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 是否上架 ====================

@product_board_bp.route('/product-board/toggle-listed', methods=['POST'])
@login_required
@permission_required('product_board:toggle_listed')
def toggle_listed():
    try:
        data = request.get_json() or {}
        asin = (data.get('asin') or '').strip()
        is_listed = data.get('is_listed', None)

        if not asin:
            return jsonify({"status": "error", "message": "asin 不能为空"}), 400
        if is_listed is None:
            return jsonify({"status": "error", "message": "is_listed 不能为空"}), 400

        is_listed = 1 if is_listed else 0

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE product_board SET is_listed = %s WHERE asin = %s", (is_listed, asin))
                conn.commit()
                return jsonify({
                    "status": "success",
                    "message": f"已{'上架' if is_listed else '取消上架'} {asin}，更新 {cursor.rowcount} 条记录",
                    "data": {"asin": asin, "is_listed": bool(is_listed), "updated_rows": cursor.rowcount}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 切换上架状态异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 筛选选项 ====================

@product_board_bp.route('/product-board/filters', methods=['GET'])
@login_required
@permission_required('product_board:page')
def product_board_filters():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT amazon_status FROM product_board WHERE amazon_status != '' ORDER BY amazon_status")
                amazon_statuses = [r['amazon_status'] for r in cursor.fetchall()]

                return jsonify({
                    "status": "success",
                    "data": {"amazon_statuses": amazon_statuses}
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 筛选查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 统计 ====================

@product_board_bp.route('/product-board/stats', methods=['GET'])
@login_required
@permission_required('product_board:page')
def product_board_stats():
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 基于最新 per-ASIN 数据统计
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        AVG(selling_price_usd) as avg_price,
                        AVG(profit_margin_30d) as avg_margin_30d,
                        SUM(sales_30d) as total_sales_30d,
                        SUM(gross_profit_30d_usd) as total_profit_30d,
                        AVG(rating) as avg_rating,
                        SUM(ad_spend_30d) as total_ad_spend_30d
                    FROM product_board WHERE {LATEST_PER_ASIN}
                """)
                summary = cursor.fetchone()

                cursor.execute(f"""
                    SELECT amazon_status, COUNT(*) as cnt, SUM(sales_30d) as total_sales
                    FROM product_board WHERE {LATEST_PER_ASIN}
                    GROUP BY amazon_status ORDER BY cnt DESC
                """)
                by_status = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "summary": summary,
                        "by_status": by_status
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 统计异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 导出 ====================

@product_board_bp.route('/product-board/export', methods=['GET'])
@login_required
@permission_required('product_board:export')
def export_product_board():
    try:
        keyword = request.args.get('keyword', '').strip() or None

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                conditions = [LATEST_PER_ASIN]
                params = []

                if keyword:
                    conditions.append("(asin LIKE %s OR product_name_cn LIKE %s)")
                    like = f"%{keyword}%"
                    params.extend([like, like])

                where_clause = " AND ".join(conditions)
                cursor.execute(f"SELECT * FROM product_board WHERE {where_clause} ORDER BY sales_30d DESC", tuple(params))
                rows = cursor.fetchall()
        finally:
            conn.close()

        if not rows:
            return jsonify({"status": "error", "message": "无数据可导出"}), 400

        output = io.StringIO()
        output.write('﻿')
        writer = csv.writer(output)

        col_labels = ['asin', '产品中文名称', '图片', '销售链接', 'AMZ状态', '评分',
                      '售价(USD)', '促销价(USD)', '建议售价(29%)', '采购成本(RMB)',
                      '1天销量', '3天销量', '7天销量', '14天销量', '30天销量',
                      '7天毛利(USD)', '30天毛利(USD)', '7天毛利率', '30天毛利率',
                      '30天退款率', '60天退款率', '当月acos', '当月tacos',
                      '昨日广告费', '当月广告费', '近30天广告费',
                      '产品开发时间', '第一次到货时间', '最早到货时间', 'FBA到货时间',
                      '导入时间']
        writer.writerow(col_labels)

        col_keys = list(HEADER_MAP.keys())
        for row in rows:
            line = []
            for key in col_keys:
                val = row.get(HEADER_MAP[key], '')
                if val is None:
                    val = ''
                col = HEADER_MAP[key]
                if col in PERCENT_FIELDS_RAW and val != '':
                    val = f"{float(val):.2f}%"
                elif col in PERCENT_FIELDS_DECIMAL and val != '':
                    val = f"{float(val) * 100:.2f}%"
                line.append(val)
            ct = row.get('created_at')
            line.append(ct.strftime('%Y-%m-%d %H:%M:%S') if ct else '')
            writer.writerow(line)

        output.seek(0)
        date_str = datetime.now().strftime('%Y%m%d')
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="product_board_{date_str}.csv"'}
        )
    except Exception as e:
        print(f"[Product Board] 导出异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
