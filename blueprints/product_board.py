"""
选品看板模块 - 追加模式
每次导入生成新批次，同一 ASIN 不同批次各存一行，保留完整历史趋势。
默认展示最新批次数据，可切换批次、查看产品跨批次趋势。
"""
from flask import Blueprint, request, jsonify, Response
from blueprints.user_auth import login_required
from services.mysql_service import get_db_connection
from dotenv import load_dotenv
import json
import csv
import io
import os
import requests
import openpyxl
from datetime import datetime

load_dotenv(override=True)
BASE_URL = os.getenv('BASE_URL', '').rstrip('/')

product_board_bp = Blueprint('product_board', __name__, url_prefix='/api')

# 图片本地存储目录
IMAGE_DIR = os.path.join('static', 'product_board')
os.makedirs(IMAGE_DIR, exist_ok=True)

# 表字段映射：前端传中文表头 → 数据库列
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

# 导出时直接加 % 的字段（存的就是百分比值，如 15.0 表示 15%）
PERCENT_FIELDS_RAW = {
    'profit_margin_7d', 'profit_margin_30d', 'refund_rate_30d', 'refund_rate_60d',
}
# 导出时需要 ×100 转百分比的字段（存的是小数，如 0.15 表示 15%）
PERCENT_FIELDS_DECIMAL = {
    'acos_current_month', 'tacos_current_month',
}


def _get_conn():
    return get_db_connection()


def _parse_numeric(value):
    """解析数值，处理 %、$、￥、逗号"""
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
    """解析上传的 CSV / XLSX 文件，返回 (rows, image_map, error)

    rows: [{col: val}, ...]
    image_map: {row_index: image_bytes}  仅 xlsx，从嵌入图片中提取
    """
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
        print(f"[Product Board] 表头: {headers}", flush=True)
        rows = []
        for r in rows_iter:
            vals = [c.value for c in r]
            rows.append(dict(zip(headers, vals)))
        wb.close()
        print(f"[Product Board] 读取到 {len(rows)} 行数据", flush=True)

        # 通过 zip 结构提取嵌入图片（避免 openpyxl 非 read_only 卡死）
        print("[Product Board] 开始 zip 图片提取...", flush=True)
        image_map = _extract_images_from_xlsx(raw, len(rows))
        print(f"[Product Board] 图片提取完成，共 {len(image_map)} 张", flush=True)
        return rows, image_map, None

    # CSV（无嵌入图片）
    print("[Product Board] 尝试 CSV 解析...", flush=True)
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
    """从 xlsx zip 包中直接提取嵌入图片，按锚点行号映射到数据行

    返回: {data_row_index: image_bytes}
    """
    import zipfile
    from xml.etree import ElementTree

    image_map = {}
    try:
        print("[Product Board] 打开 zip...", flush=True)
        zf = zipfile.ZipFile(io.BytesIO(raw))
        names = zf.namelist()
        print(f"[Product Board] zip 内共 {len(names)} 个文件", flush=True)

        # 找到 sheet1 的 drawing 文件，从中读图片锚点行号
        drawing_path = 'xl/drawings/drawing1.xml'
        drawing_rels_path = 'xl/drawings/_rels/drawing1.xml.rels'

        if drawing_path not in names:
            print("[Product Board] xlsx 无 drawing 文件，没有嵌入图片", flush=True)
            zf.close()
            return image_map

        # 解析 drawing rels：rId → 图片文件路径
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

        # 解析 drawing：提取每个图片的锚点行号
        print("[Product Board] 解析 drawing XML...", flush=True)
        draw_xml = ElementTree.fromstring(zf.read(drawing_path))
        # 命名空间
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
                anchor_row = int(row_el.text)  # 0-based 行号，含表头

                # 找图片引用 rId
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
    """将原始行映射为数据库字段"""
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
    """保存图片字节到本地，返回本地 URL；失败返回 None"""
    try:
        # 尝试从 bytes 头识别格式
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


def _get_asins_with_image(cursor):
    """查询已有本地图片的 ASIN（image_url 非空且以 /static/ 开头）"""
    cursor.execute("SELECT DISTINCT asin FROM product_board WHERE image_url IS NOT NULL AND image_url != '' AND image_url LIKE '%%/static/product_board/%%'")
    return {r['asin'] for r in cursor.fetchall()}


# ==================== 导入 ====================

@product_board_bp.route('/product-board/import', methods=['POST'])
@login_required
def import_product_board():
    """
    导入 CSV / XLSX / JSON 数据
    Excel 支持提取嵌入图片；新 ASIN 或没图片的旧 ASIN 会自动保存图片并补全历史批次。
    """
    batch = datetime.now().strftime('%Y%m%d%H%M%S')
    print(f"[Product Board] ====== 开始导入 batch={batch} ======", flush=True)
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
            print(f"[Product Board] JSON 模式，{len(rows)} 条数据", flush=True)

        if not rows:
            return jsonify({"status": "error", "message": "无数据"}), 400

        print("[Product Board] 获取数据库连接...", flush=True)
        conn = _get_conn()
        print("[Product Board] 数据库连接成功", flush=True)
        image_saved = 0
        asins_got_image = set()  # 记录本次拿到图片的 ASIN
        try:
            with conn.cursor() as cursor:
                asins_with_image = _get_asins_with_image(cursor)

                for i, row in enumerate(rows):
                    mapped = _map_row(row)
                    asin = mapped.get('asin')
                    if not asin:
                        skipped += 1
                        continue

                    # 图片来源1：Excel 嵌入图片
                    if i in image_map and image_map[i]:
                        if asin not in asins_with_image:
                            local_url = _save_image_bytes(asin, image_map[i])
                            if local_url:
                                mapped['image_url'] = local_url
                                image_saved += 1
                                asins_got_image.add(asin)

                    # 图片来源2：单元格中的 URL
                    if asin not in asins_with_image and mapped.get('image_url') and '/static/product_board/' not in mapped['image_url']:
                        # 尝试下载 URL
                        try:
                            resp = requests.get(mapped['image_url'], timeout=15, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                            })
                            if resp.status_code == 200:
                                local_url = _save_image_bytes(asin, resp.content)
                                if local_url:
                                    mapped['image_url'] = local_url
                                    image_saved += 1
                                    asins_got_image.add(asin)
                        except Exception:
                            pass  # 下载失败保留原 URL

                    mapped['import_batch'] = batch
                    columns = [k for k in mapped.keys() if k in ALL_COLUMNS + ['import_batch']]
                    placeholders = ', '.join(['%s'] * len(columns))
                    sql = f"""
                        INSERT INTO product_board ({', '.join(columns)})
                        VALUES ({placeholders})
                    """
                    try:
                        cursor.execute(sql, [mapped.get(c) for c in columns])
                        inserted += 1
                    except Exception as e:
                        err_str = str(e)
                        if 'Duplicate' in err_str or 'duplicate' in err_str:
                            skipped += 1
                        else:
                            errors.append(f"{asin}: {err_str}")

                # 补全：把新下载的图片链接更新到该 ASIN 的历史批次
                for asin in asins_got_image:
                    cursor.execute("""
                        SELECT image_url FROM product_board
                        WHERE asin = %s AND image_url IS NOT NULL AND image_url != '' AND image_url LIKE '%%/static/product_board/%%'
                        LIMIT 1
                    """, (asin,))
                    img_row = cursor.fetchone()
                    if img_row:
                        cursor.execute("""
                            UPDATE product_board SET image_url = %s
                            WHERE asin = %s AND (image_url IS NULL OR image_url = '' OR image_url NOT LIKE '%%/static/product_board/%%')
                        """, (img_row['image_url'], asin))

                conn.commit()
        finally:
            conn.close()

        return jsonify({
            "status": "success",
            "message": f"导入完成：新增 {inserted} 条，保存图片 {image_saved} 张，跳过 {skipped} 条",
            "data": {"inserted": inserted, "image_saved": image_saved, "skipped": skipped, "batch": batch, "errors": errors[:20]}
        })

    except Exception as e:
        print(f"[Product Board] 导入异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 批次列表 ====================

@product_board_bp.route('/product-board/batches', methods=['GET'])
@login_required
def list_batches():
    """返回所有导入批次，按时间倒序"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT import_batch, batch_time,
                           COUNT(*) as total,
                           SUM(sales_30d) as total_sales_30d,
                           AVG(profit_margin_30d) as avg_margin_30d
                    FROM product_board
                    GROUP BY import_batch, batch_time
                    ORDER BY batch_time DESC
                    LIMIT 60
                """)
                batches = cursor.fetchall()

                # 标记最新批次
                latest = batches[0]['import_batch'] if batches else None

                return jsonify({
                    "status": "success",
                    "data": {
                        "batches": batches,
                        "latest_batch": latest
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 批次查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 列表 ====================

@product_board_bp.route('/product-board', methods=['GET'])
@login_required
def list_product_board():
    """分页查询（默认展示最新批次）"""
    try:
        keyword = request.args.get('keyword', '').strip() or None
        amazon_status = request.args.get('amazon_status', '').strip() or None
        batch = request.args.get('batch', '').strip() or None
        min_sales = request.args.get('min_sales', '').strip() or None
        min_margin = request.args.get('min_margin', '').strip() or None
        sort_by = request.args.get('sort_by', 'sales_30d')
        sort_dir = request.args.get('sort_dir', 'desc')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        if page < 1:
            page = 1
        if page_size < 1 or page_size > 500:
            page_size = 20

        allowed_sort = ALL_COLUMNS + ['id', 'batch_time', 'created_at']
        if sort_by not in allowed_sort:
            sort_by = 'sales_30d'
        sort_dir = 'DESC' if sort_dir.lower() == 'desc' else 'ASC'

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                # 默认取最新批次
                if not batch:
                    cursor.execute("SELECT MAX(import_batch) FROM product_board")
                    row = cursor.fetchone()
                    batch = list(row.values())[0] if row else None

                if not batch:
                    return jsonify({"status": "success", "data": {"list": [], "total": 0, "page": page, "page_size": page_size, "batch": None}})

                conditions = ["import_batch = %s"]
                params = [batch]

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
                        "page_size": page_size,
                        "batch": batch
                    }
                })
        finally:
            conn.close()

    except Exception as e:
        print(f"[Product Board] 查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 趋势（多产品跨批次） ====================

@product_board_bp.route('/product-board/trend', methods=['GET'])
@login_required
def product_board_trend():
    """查询指定产品在所有批次中的数据变化趋势"""
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
                    ORDER BY pb.asin, pb.batch_time ASC
                """, tuple(asins))
                rows = cursor.fetchall()

                # 按 ASIN 分组
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
                    grouped[asin]['data_points'].append({
                        'id': r['id'],
                        'import_batch': r['import_batch'],
                        'batch_time': r['batch_time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(r['batch_time'], 'strftime') else str(r['batch_time']),
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
def get_product_board_detail(product_id):
    """按 ID 查单条"""
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
def delete_product_board(product_id):
    """删除单条"""
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
def batch_delete_product_board():
    """批量删除"""
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


# ==================== 筛选选项 ====================

@product_board_bp.route('/product-board/filters', methods=['GET'])
@login_required
def product_board_filters():
    """返回筛选下拉框选项"""
    try:
        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT import_batch, batch_time, COUNT(*) as cnt
                    FROM product_board
                    GROUP BY import_batch, batch_time
                    ORDER BY batch_time DESC LIMIT 30
                """)
                batches = cursor.fetchall()

                cursor.execute("""
                    SELECT DISTINCT amazon_status FROM product_board
                    WHERE amazon_status != '' ORDER BY amazon_status
                """)
                amazon_statuses = [r['amazon_status'] for r in cursor.fetchall()]

                return jsonify({
                    "status": "success",
                    "data": {
                        "batches": batches,
                        "amazon_statuses": amazon_statuses
                    }
                })
        finally:
            conn.close()
    except Exception as e:
        print(f"[Product Board] 筛选查询异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 统计 ====================

@product_board_bp.route('/product-board/stats', methods=['GET'])
@login_required
def product_board_stats():
    """统计概览（默认最新批次）"""
    try:
        batch = request.args.get('batch', '').strip() or None

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                if not batch:
                    cursor.execute("SELECT MAX(import_batch) FROM product_board")
                    row = cursor.fetchone()
                    batch = list(row.values())[0] if row else None

                if not batch:
                    return jsonify({"status": "success", "data": {"summary": {}, "by_status": []}})

                cursor.execute("""
                    SELECT
                        COUNT(*) as total,
                        AVG(selling_price_usd) as avg_price,
                        AVG(profit_margin_30d) as avg_margin_30d,
                        SUM(sales_30d) as total_sales_30d,
                        SUM(gross_profit_30d_usd) as total_profit_30d,
                        AVG(rating) as avg_rating,
                        SUM(ad_spend_30d) as total_ad_spend_30d
                    FROM product_board WHERE import_batch = %s
                """, (batch,))
                summary = cursor.fetchone()

                cursor.execute("""
                    SELECT amazon_status, COUNT(*) as cnt, SUM(sales_30d) as total_sales
                    FROM product_board WHERE import_batch = %s
                    GROUP BY amazon_status ORDER BY cnt DESC
                """, (batch,))
                by_status = cursor.fetchall()

                return jsonify({
                    "status": "success",
                    "data": {
                        "batch": batch,
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
def export_product_board():
    """导出为 CSV（默认导出最新批次）"""
    try:
        batch = request.args.get('batch', '').strip() or None
        keyword = request.args.get('keyword', '').strip() or None

        conn = _get_conn()
        try:
            with conn.cursor() as cursor:
                if not batch:
                    cursor.execute("SELECT MAX(import_batch) FROM product_board")
                    row = cursor.fetchone()
                    batch = list(row.values())[0] if row else None

                conditions = ["import_batch = %s"]
                params = [batch]

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
                      '导入批次', '批次时间']
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
            line.append(row.get('import_batch', ''))
            bt = row.get('batch_time')
            line.append(bt.strftime('%Y-%m-%d %H:%M:%S') if bt else '')
            writer.writerow(line)

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="product_board_{batch}.csv"'}
        )
    except Exception as e:
        print(f"[Product Board] 导出异常: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
