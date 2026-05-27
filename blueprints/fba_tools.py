"""
FBA 工具模块 - 亚马逊 SKU 标签生成 / PDF 编辑
"""
from flask import Blueprint, request, jsonify, send_file
import os
from dotenv import load_dotenv

from blueprints.user_auth import login_required, permission_required
from services.fbaFnSkuTag import generate_amazon_label_v4
from services.pdf_editor import crop_pdf, split_pdf
from services.shop_service import get_sp_api_client

import io
import json
import re
import threading
import uuid
import time
import requests
import zipfile
import shutil
from datetime import datetime, timedelta

import fitz
from services.mysql_service import get_db_connection

# 创建 Blueprint
fba_tools_bp = Blueprint('fba_tools', __name__, url_prefix='/api')

# 加载环境变量
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")

# 箱唛整理任务输出目录配置
FBA_LABEL_TASK_DIR = os.path.join('static', 'fba_labels', 'task')
CARGO_AGENT_DIR = os.path.join('static', 'cargo_agent')


@fba_tools_bp.route('/fba/label', methods=['POST'])
@login_required
@permission_required('fba_tools:generate')
def create_fba_label():
    """
    生成亚马逊 FBA SKU 标签 PDF
    请求参数: {fnsku, product_name, extra_info, sku, width_mm?, height_mm?}
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "请求数据不能为空"}), 400

        fnsku = data.get('fnsku', '').strip()
        product_name = data.get('product_name', '').strip()
        extra_info = data.get('extra_info', '').strip()
        sku = data.get('sku', '').strip()
        width_mm = data.get('width_mm', 50)
        height_mm = data.get('height_mm', 30)

        # 参数校验
        if not fnsku:
            return jsonify({"status": "error", "message": "fnsku 不能为空"}), 400
        if not product_name:
            return jsonify({"status": "error", "message": "product_name 不能为空"}), 400
        if not sku:
            return jsonify({"status": "error", "message": "sku 不能为空"}), 400

        # 打印前端传参，方便调试
        print(f"[FBA Label] 前端参数: fnsku={fnsku!r}, sku={sku!r}, product_name={product_name!r}, extra_info={extra_info!r}, width_mm={width_mm}, height_mm={height_mm}")

        # 数值校验
        try:
            width_mm = float(width_mm)
            height_mm = float(height_mm)
            if width_mm <= 0 or height_mm <= 0:
                return jsonify({"status": "error", "message": "标签尺寸必须大于 0"}), 400
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "标签尺寸格式错误"}), 400

        output_dir = os.path.join('static', 'fbatag')
        output_path = generate_amazon_label_v4(
            fnsku=fnsku,
            product_name=product_name,
            extra_info=extra_info,
            sku=sku,
            width_mm=width_mm,
            height_mm=height_mm,
            output_dir=output_dir
        )

        # 构建可访问的 URL
        file_name = os.path.basename(output_path)
        relative_url = f"/static/fbatag/{file_name}"
        file_url = f"{BASE_URL.rstrip('/')}{relative_url}" if BASE_URL else relative_url

        return jsonify({
            "status": "success",
            "message": "标签生成成功",
            "data": {
                "file_name": file_name,
                "file_path": output_path,
                "url": file_url
            }
        })

    except Exception as e:
        print(f"生成 FBA 标签异常: {str(e)}")
        return jsonify({"status": "error", "message": f"生成失败: {str(e)}"}), 500


@fba_tools_bp.route('/pdf/edit', methods=['POST'])
@login_required
@permission_required('fba_tools:generate')
def edit_pdf():
    """
    裁剪 PDF 页面
    请求: multipart/form-data
      - file: 原始 PDF
      - operations: JSON字符串 [{"type":"crop","page":0,"bbox":[left,top,width,height],"scale":1.5}, ...]
    返回: 裁剪后的 PDF 文件流（FBA标签自动提取FBA号+SKU命名，非FBA标签返回原文件名）
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "请上传 PDF 文件"}), 400

    file = request.files['file']
    original_filename = file.filename or 'cropped.pdf'
    operations_raw = request.form.get('operations', '[]')
    try:
        operations = json.loads(operations_raw)
    except Exception:
        return jsonify({"status": "error", "message": "参数格式错误"}), 400

    try:
        file_bytes = file.read()

        # 尝试提取 FBA 信息，用于自定义文件名
        download_name = original_filename
        try:
            doc_temp = fitz.open(stream=file_bytes, filetype="pdf")
            # 取第一个被裁剪页面的文本
            crop_page = operations[0].get('page', 0) if operations else 0
            if crop_page < len(doc_temp):
                text = doc_temp[crop_page].get_text()
                print(f"[PDF Crop] 第{crop_page}页文本前500字: {text[:500]!r}")
                fba_id, sku = _extract_fba_info_from_text(text)
                print(f"[PDF Crop] 提取结果: fba_id={fba_id!r}, sku={sku!r}")
                if fba_id and sku:
                    name_map = _get_product_names_by_skus([sku])
                    name = name_map.get(sku) or ''
                    safe_name = re.sub(r'[\\/:*?"<>|]', '', name)
                    safe_sku = re.sub(r'[\\/:*?"<>|]', '', sku)
                    download_name = f"{fba_id}_{safe_sku}.pdf"
            doc_temp.close()
        except Exception as e:
            print(f"[PDF Crop] 提取FBA信息失败: {e}")

        output = crop_pdf(file_bytes, operations)
        return send_file(
            output,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=download_name
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"裁剪失败: {str(e)}"}), 500


def _extract_fba_info_from_text(text):
    """从页面文本中提取 FBA 号和 SKU"""
    # FBA 号：匹配 FBA 开头字符串，保留完整的 FBA 箱号（含末尾 U+数字）
    fba_id = None
    fba_match = re.search(r'FBA[A-Z0-9]+', text, re.IGNORECASE)
    if fba_match:
        fba_id = fba_match.group(0).upper()

    # SKU：匹配 Single SKU 后的值
    sku = None
    sku_match = re.search(r'Single SKU[:\s]+([A-Z0-9][A-Z0-9\-_]*)', text, re.IGNORECASE)
    if sku_match:
        sku = sku_match.group(1)

    return fba_id, sku


def _get_product_names_by_skus(skus):
    """根据 SKU 列表批量查询中文名"""
    if not skus:
        return {}
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(skus))
            sql = f"""
                SELECT seller_sku,
                       COALESCE(NULLIF(product_name, ''), declare_name_cn) AS name
                FROM products
                WHERE seller_sku IN ({placeholders})
            """
            cursor.execute(sql, tuple(skus))
            rows = cursor.fetchall()
            return {row['seller_sku']: row['name'] for row in rows}
    finally:
        conn.close()

@login_required
@permission_required('fba_tools:generate')
@fba_tools_bp.route('/pdf/split', methods=['POST'])
def split_pdf_route():
    """
    拆分 PDF 页面
    请求: multipart/form-data
      - file: 原始 PDF
      - data: JSON字符串 {"pages": [0, 2, ...]}
    返回: 单页时直接返回 PDF；多页时返回 ZIP
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "请上传 PDF 文件"}), 400

    file = request.files['file']
    data_raw = request.form.get('data', '{}')
    try:
        data = json.loads(data_raw)
    except Exception:
        return jsonify({"status": "error", "message": "参数格式错误"}), 400

    pages = data.get('pages', [])
    if not pages:
        return jsonify({"status": "error", "message": "未指定要拆分的页面"}), 400

    try:
        file_bytes = file.read()

        # 自动提取 FBA 信息并生成自定义文件名
        filenames = {}
        try:
            doc_temp = fitz.open(stream=file_bytes, filetype="pdf")
            page_infos = []
            skus = set()
            for page_num in pages:
                if 0 <= page_num < len(doc_temp):
                    page = doc_temp[page_num]
                    text = page.get_text()
                    fba_id, sku = _extract_fba_info_from_text(text)
                    page_infos.append((page_num, fba_id, sku))
                    if sku:
                        skus.add(sku)
            doc_temp.close()

            if skus:
                name_map = _get_product_names_by_skus(list(skus))
                for page_num, fba_id, sku in page_infos:
                    if fba_id and sku:
                        name = name_map.get(sku) or ''
                        safe_name = re.sub(r'[\\/:*?"<>|]', '', name)
                        safe_sku = re.sub(r'[\\/:*?"<>|]', '', sku)
                        filenames[page_num] = f"{fba_id}_{safe_sku}_{safe_name}.pdf"
        except Exception as e:
            print(f"[PDF Split] 提取FBA信息失败: {e}")
            # 提取失败不影响正常拆分，回退到默认文件名

        output, is_zip, download_name = split_pdf(file_bytes, pages, filenames)
        mimetype = 'application/zip' if is_zip else 'application/pdf'
        return send_file(
            output,
            mimetype=mimetype,
            as_attachment=True,
            download_name=download_name
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"拆分失败: {str(e)}"}), 500


# ==================== 箱唛自动化整理 ====================

def _create_task(task_id, shop_id, inbound_plan_id, crop_config=None, cargo_agent_path=None):
    """创建任务记录"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            expired_at = datetime.now() + timedelta(hours=24)
            crop_config_json = json.dumps(crop_config) if crop_config else None
            sql = """
                INSERT INTO fba_label_tasks (id, shop_id, inbound_plan_id, crop_config, cargo_agent_path, status, progress, expired_at)
                VALUES (%s, %s, %s, %s, %s, 'pending', 0, %s)
            """
            cursor.execute(sql, (task_id, shop_id, inbound_plan_id, crop_config_json, cargo_agent_path, expired_at))
            conn.commit()
    finally:
        conn.close()


def _update_task(task_id, status=None, progress=None, message=None, result_path=None, completed_at=None):
    """更新任务状态"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            fields = []
            values = []
            if status is not None:
                fields.append("status = %s")
                values.append(status)
            if progress is not None:
                fields.append("progress = %s")
                values.append(progress)
            if message is not None:
                fields.append("message = %s")
                values.append(message)
            if result_path is not None:
                fields.append("result_path = %s")
                values.append(result_path)
            if completed_at is not None:
                fields.append("completed_at = %s")
                values.append(completed_at)
            if not fields:
                return
            sql = f"UPDATE fba_label_tasks SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(sql, tuple(values + [task_id]))
            conn.commit()
    finally:
        conn.close()


def _get_task(task_id):
    """查询任务"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, shop_id, inbound_plan_id, crop_config, cargo_agent_path, status, progress, message, result_path,
                       created_at, completed_at, expired_at
                FROM fba_label_tasks WHERE id = %s
            """, (task_id,))
            row = cursor.fetchone()
            if row and row.get('crop_config'):
                try:
                    row['crop_config'] = json.loads(row['crop_config'])
                except Exception:
                    row['crop_config'] = None
            return row
    finally:
        conn.close()


def _cleanup_old_tasks():
    """清理过期任务（24小时前）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, result_path, cargo_agent_path FROM fba_label_tasks
                WHERE expired_at < NOW()
                   OR (created_at < DATE_SUB(NOW(), INTERVAL 24 HOUR)
                       AND status IN ('completed', 'failed'))
            """)
            rows = cursor.fetchall()
            for row in rows:
                task_id = row['id']
                # 清理结果目录
                task_dir = os.path.join(FBA_LABEL_TASK_DIR, task_id)
                if os.path.exists(task_dir):
                    shutil.rmtree(task_dir, ignore_errors=True)
                # 清理货代文件
                if row.get('cargo_agent_path') and os.path.exists(row['cargo_agent_path']):
                    agent_dir = os.path.dirname(row['cargo_agent_path'])
                    if os.path.exists(agent_dir):
                        shutil.rmtree(agent_dir, ignore_errors=True)
                cursor.execute("DELETE FROM fba_label_tasks WHERE id = %s", (task_id,))
            conn.commit()
    finally:
        conn.close()


def _get_shipments_by_plan_id(shop_id, inbound_plan_id):
    """根据入库计划ID查询所有货件"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT s.shipment_id, d.shipment_confirmation_id
                FROM amazon_inbound_shipments s
                LEFT JOIN amazon_inbound_shipments_detail d
                    ON s.shipment_id = d.shipment_id AND d.shop_id = s.shop_id
                WHERE s.shop_id = %s AND s.inbound_plan_id = %s
                ORDER BY s.shipment_id
            """
            cursor.execute(sql, (shop_id, inbound_plan_id))
            return cursor.fetchall()
    finally:
        conn.close()


def _get_box_ids_by_shipment_id(shop_id, shipment_id):
    """根据货件编号查询所有箱子ID"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT box_id FROM amazon_inbound_plan_boxes
                WHERE shop_id = %s AND shipment_id = %s
                  AND box_id IS NOT NULL AND box_id != ''
                ORDER BY box_id
            """
            cursor.execute(sql, (shop_id, shipment_id))
            rows = cursor.fetchall()
            return [row['box_id'] for row in rows]
    finally:
        conn.close()


def _execute_task(task_id, shop_id, inbound_plan_id, crop_config):
    """由工作线程执行单个箱唛整理任务"""
    try:
        _update_task(task_id, status='running', progress=0)

        # 1. 查询货件列表
        shipments = _get_shipments_by_plan_id(shop_id, inbound_plan_id)
        if not shipments:
            _update_task(task_id, status='failed', message='未找到货件记录')
            return

        total = len(shipments)
        task_dir = os.path.join(FBA_LABEL_TASK_DIR, task_id)
        os.makedirs(task_dir, exist_ok=True)

        success_count = 0
        fail_messages = []

        for idx, shipment in enumerate(shipments):
            shipment_id = shipment['shipment_confirmation_id']
            progress = int((idx / total) * 90)
            _update_task(task_id, progress=progress)

            try:
                # 2. 获取箱子列表
                box_ids = _get_box_ids_by_shipment_id(shop_id, shipment_id)
                if not box_ids:
                    fail_messages.append(f'{shipment_id}: 无箱子记录')
                    continue

                # 3. 调用 SP-API 获取标签下载链接
                client = get_sp_api_client(shop_id=shop_id)
                resp = client.get_shipment_labels(
                    shipment_id=shipment_id,
                    carton_ids=box_ids,
                    page_type='PackageLabel_Thermal_NonPCP',
                    label_type='UNIQUE'
                )
                download_url = resp.get('payload', {}).get('DownloadURL')
                if not download_url:
                    fail_messages.append(f'{shipment_id}: 无下载链接')
                    continue

                # 4. 下载 PDF
                pdf_resp = requests.get(download_url, timeout=60)
                pdf_resp.raise_for_status()
                pdf_bytes = pdf_resp.content

                # 5. 裁剪每页
                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                x_ratio = crop_config.get('x_ratio', [0, 1])
                y_ratio = crop_config.get('y_ratio', [0, 0.667])
                for page_num in range(len(doc)):
                    page = doc[page_num]
                    rect = page.rect
                    x0 = rect.width * x_ratio[0]
                    x1 = rect.width * x_ratio[1]
                    y0 = rect.height * y_ratio[0]
                    y1 = rect.height * y_ratio[1]
                    page.set_cropbox(fitz.Rect(x0, y0, x1, y1))

                cropped_bytes = io.BytesIO()
                doc.save(cropped_bytes)
                cropped_bytes.seek(0)
                doc.close()

                # 6. 拆分保存（单页直接保存，多页逐页拆分）
                cropped_doc = fitz.open(stream=cropped_bytes, filetype="pdf")
                shipment_dir = os.path.join(task_dir, shipment_id)
                os.makedirs(shipment_dir, exist_ok=True)

                def _build_filename(fba_id, sku, page_num=0):
                    name = ''
                    if sku:
                        name_map = _get_product_names_by_skus([sku])
                        name = name_map.get(sku) or ''
                    safe_name = re.sub(r'[\\/:*?"<>|]', '', name)
                    safe_sku = re.sub(r'[\\/:*?"<>|]', '', sku or '')
                    if fba_id:
                        return f"{fba_id}_{safe_sku}_{safe_name}.pdf"
                    return f"page_{page_num + 1}_{safe_sku}_{safe_name}.pdf"

                if len(cropped_doc) == 1:
                    # 单页：直接保存，无需拆分
                    page = cropped_doc[0]
                    text = page.get_text()
                    fba_id, sku = _extract_fba_info_from_text(text)
                    filename = _build_filename(fba_id, sku)
                    cropped_doc.save(os.path.join(shipment_dir, filename))
                else:
                    # 多页：逐页拆分
                    skus = set()
                    page_infos = []
                    for page_num in range(len(cropped_doc)):
                        page = cropped_doc[page_num]
                        text = page.get_text()
                        fba_id, sku = _extract_fba_info_from_text(text)
                        page_infos.append((page_num, fba_id, sku))
                        if sku:
                            skus.add(sku)

                    name_map = _get_product_names_by_skus(list(skus)) if skus else {}

                    for page_num, fba_id, sku in page_infos:
                        name = name_map.get(sku) or ''
                        safe_name = re.sub(r'[\\/:*?"<>|]', '', name)
                        safe_sku = re.sub(r'[\\/:*?"<>|]', '', sku or '')

                        if fba_id:
                            filename = f"{fba_id}_{safe_sku}_{safe_name}.pdf"
                        else:
                            filename = f"page_{page_num + 1}_{safe_sku}_{safe_name}.pdf"

                        new_doc = fitz.open()
                        new_doc.insert_pdf(cropped_doc, from_page=page_num, to_page=page_num)
                        new_doc.save(os.path.join(shipment_dir, filename))
                        new_doc.close()

                cropped_doc.close()
                success_count += 1

            except Exception as e:
                print(f"[Organize Task] 货件 {shipment_id} 处理失败: {e}")
                import traceback
                traceback.print_exc()
                fail_messages.append(f'{shipment_id}: {str(e)}')

        # 7. 处理货代箱唛ZIP（如有）
        task_info = _get_task(task_id)
        cargo_agent_path = task_info.get('cargo_agent_path') if task_info else None
        if cargo_agent_path and os.path.exists(cargo_agent_path):
            try:
                shipment_ids = [s['shipment_confirmation_id'] for s in shipments]
                _process_cargo_agent_zip(cargo_agent_path, task_dir, shipment_ids)
            except Exception as e:
                print(f"[Organize Task] 货代箱唛处理失败: {e}")
                import traceback
                traceback.print_exc()
                fail_messages.append(f'货代箱唛: {str(e)}')

        # 8. 打包 ZIP
        if success_count > 0:
            zip_path = os.path.join(task_dir, 'result.zip')
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, files in os.walk(task_dir):
                    for file in files:
                        if file == 'result.zip':
                            continue
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, task_dir)
                        zf.write(file_path, arcname)

            message = '; '.join(fail_messages) if fail_messages else ''
            _update_task(task_id, status='completed', progress=100, message=message,
                         result_path=zip_path, completed_at=datetime.now())
        else:
            _update_task(task_id, status='failed', progress=0,
                         message='所有货件处理失败: ' + '; '.join(fail_messages))

    except Exception as e:
        print(f"[Organize Task] 任务 {task_id} 异常: {e}")
        import traceback
        traceback.print_exc()
        _update_task(task_id, status='failed', message=str(e))


def _process_cargo_agent_zip(cargo_agent_path, task_dir, shipment_ids):
    """解压货代箱唛ZIP，按FBA货件号归类并拆分PDF到对应文件夹"""
    temp_dir = os.path.join(task_dir, '_cargo_agent_temp')
    os.makedirs(temp_dir, exist_ok=True)

    with zipfile.ZipFile(cargo_agent_path, 'r') as zf:
        zf.extractall(temp_dir)

    matched_count = 0
    unmatched = []
    for root, dirs, files in os.walk(temp_dir):
        for filename in files:
            if not filename.lower().endswith('.pdf'):
                continue

            # 提取货代编号（99开头的数字）
            agent_code_match = re.match(r'^(\d+)', filename)
            agent_code = agent_code_match.group(1) if agent_code_match else ''

            # 提取FBA号
            fba_match = re.search(r'FBA[A-Z0-9]+', filename, re.IGNORECASE)
            if not fba_match:
                unmatched.append(filename)
                continue
            fba_id = fba_match.group(0).upper()

            # 找到对应货件文件夹
            matched_shipment = None
            for shipment_id in shipment_ids:
                if fba_id == shipment_id.upper():
                    matched_shipment = shipment_id
                    break

            if not matched_shipment:
                unmatched.append(f'{filename} (FBA: {fba_id})')
                continue

            # 打开PDF按页拆分（货代PDF无需裁剪）
            pdf_path = os.path.join(root, filename)
            try:
                doc = fitz.open(pdf_path)
                dest_dir = os.path.join(task_dir, matched_shipment)
                os.makedirs(dest_dir, exist_ok=True)

                if len(doc) == 1:
                    new_filename = f"货代-{fba_id}-{agent_code}.pdf"
                    doc.save(os.path.join(dest_dir, new_filename))
                else:
                    for page_num in range(len(doc)):
                        new_doc = fitz.open()
                        new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
                        new_filename = f"货代-{fba_id}-{agent_code}_page_{page_num + 1}.pdf"
                        new_doc.save(os.path.join(dest_dir, new_filename))
                        new_doc.close()

                doc.close()
                matched_count += 1
            except Exception as e:
                print(f"[Cargo Agent] 拆分PDF失败 {filename}: {e}")
                import traceback
                traceback.print_exc()
                unmatched.append(f'{filename}: {str(e)}')

    shutil.rmtree(temp_dir, ignore_errors=True)
    if unmatched:
        raise ValueError(f"以下货代箱唛无法匹配到对应货件: {', '.join(unmatched)}")
    print(f"[Cargo Agent] 归类完成: {matched_count} 个文件匹配")


@fba_tools_bp.route('/fba/organize-plan-labels', methods=['POST'])
@login_required
@permission_required('fba_tools:generate')
def organize_plan_labels():
    """
    提交箱唛自动化整理任务
    请求: multipart/form-data
      - shop_id: 店铺ID
      - inbound_plan_id: 入库计划ID
      - crop_config: 裁剪配置JSON字符串（可选）
      - cargo_agent_zip: 货代箱唛ZIP文件（可选）
    """
    try:
        shop_id = request.form.get('shop_id', '').strip()
        inbound_plan_id = request.form.get('inbound_plan_id', '').strip()

        if not shop_id:
            return jsonify({"status": "error", "message": "shop_id 不能为空"}), 400
        try:
            shop_id = int(shop_id)
        except ValueError:
            return jsonify({"status": "error", "message": "shop_id 格式错误"}), 400

        if not inbound_plan_id:
            return jsonify({"status": "error", "message": "inbound_plan_id 不能为空"}), 400

        # 裁剪配置校验
        crop_config_raw = request.form.get('crop_config', '{}')
        try:
            crop_config = json.loads(crop_config_raw) if crop_config_raw else {}
        except Exception:
            crop_config = {}
        if not crop_config:
            crop_config = {"x_ratio": [0, 1], "y_ratio": [0, 0.667]}
        if (not isinstance(crop_config, dict) or
                'x_ratio' not in crop_config or
                'y_ratio' not in crop_config or
                not isinstance(crop_config['x_ratio'], list) or
                not isinstance(crop_config['y_ratio'], list) or
                len(crop_config['x_ratio']) != 2 or
                len(crop_config['y_ratio']) != 2):
            return jsonify({"status": "error", "message": "crop_config 格式错误"}), 400

        # 处理货代箱唛ZIP上传
        cargo_agent_path = None
        cargo_agent_file = request.files.get('cargo_agent_zip')
        if cargo_agent_file and cargo_agent_file.filename:
            task_id = str(uuid.uuid4())
            agent_dir = os.path.join(CARGO_AGENT_DIR, task_id)
            os.makedirs(agent_dir, exist_ok=True)
            cargo_agent_path = os.path.join(agent_dir, 'cargo_agent.zip')
            cargo_agent_file.save(cargo_agent_path)
        else:
            task_id = str(uuid.uuid4())

        # 清理过期任务
        _cleanup_old_tasks()

        # 创建任务（只写数据库，工作线程会自动轮询执行）
        _create_task(task_id, shop_id, inbound_plan_id, crop_config, cargo_agent_path)

        return jsonify({
            "status": "success",
            "message": "任务已提交",
            "data": {"task_id": task_id}
        })

    except Exception as e:
        print(f"[Organize Labels] 提交任务异常: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


@fba_tools_bp.route('/fba/organize-plan-labels/tasks/<task_id>', methods=['GET'])
@login_required
@permission_required('fba_tools:generate')
def get_organize_task(task_id):
    """查询箱唛整理任务进度"""
    try:
        task = _get_task(task_id)
        if not task:
            return jsonify({"status": "error", "message": "任务不存在"}), 404

        result = {
            "task_id": task['id'],
            "status": task['status'],
            "progress": task['progress'],
            "message": task['message'] or '',
            "created_at": task['created_at'].isoformat() if task['created_at'] else None,
            "completed_at": task['completed_at'].isoformat() if task['completed_at'] else None,
        }

        if task['status'] == 'completed' and task['result_path']:
            result['download_url'] = f"/api/fba/organize-plan-labels/tasks/{task_id}/download"
        else:
            result['download_url'] = None

        return jsonify({"status": "success", "data": result})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@fba_tools_bp.route('/fba/organize-plan-labels/tasks/<task_id>/download', methods=['GET'])
@login_required
@permission_required('fba_tools:generate')
def download_organize_task(task_id):
    """下载箱唛整理结果 ZIP"""
    try:
        task = _get_task(task_id)
        if not task:
            return jsonify({"status": "error", "message": "任务不存在"}), 404

        if task['status'] != 'completed':
            return jsonify({"status": "error", "message": "任务尚未完成"}), 400

        result_path = task['result_path']
        if not result_path or not os.path.exists(result_path):
            return jsonify({"status": "error", "message": "结果文件不存在"}), 404

        return send_file(
            result_path,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f"{task['inbound_plan_id']}_labels.zip"
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==================== 后台工作线程 + CURD ====================

def _pick_pending_task():
    """取出一个 pending 任务并锁定为 running"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, shop_id, inbound_plan_id, crop_config, status, progress, message, result_path,
                       created_at, completed_at, expired_at
                FROM fba_label_tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
            """)
            row = cursor.fetchone()
            if row:
                cursor.execute("UPDATE fba_label_tasks SET status = 'running' WHERE id = %s", (row['id'],))
                conn.commit()
                if row.get('crop_config'):
                    try:
                        row['crop_config'] = json.loads(row['crop_config'])
                    except Exception:
                        row['crop_config'] = None
            return row
    finally:
        conn.close()


def _reset_running_tasks():
    """服务启动时，把 running 状态的任务重置为 pending"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE fba_label_tasks SET status = 'pending' WHERE status = 'running'")
            affected = cursor.rowcount
            conn.commit()
            if affected:
                print(f"[Label Worker] 已重置 {affected} 个 running 任务为 pending")
    finally:
        conn.close()


def _label_worker_loop():
    """后台工作线程主循环：轮询 pending 任务并执行"""
    print("[Label Worker] 工作线程已启动")
    while True:
        try:
            task = _pick_pending_task()
            if task:
                task_id = task['id']
                print(f"[Label Worker] 开始执行任务: {task_id}")
                try:
                    _execute_task(
                        task_id=task_id,
                        shop_id=task['shop_id'],
                        inbound_plan_id=task['inbound_plan_id'],
                        crop_config=task.get('crop_config') or {"x_ratio": [0, 1], "y_ratio": [0, 0.667]}
                    )
                except Exception as e:
                    print(f"[Label Worker] 任务 {task_id} 执行异常: {e}")
                    import traceback
                    traceback.print_exc()
                    _update_task(task_id, status='failed', message=str(e)[:500])
            else:
                time.sleep(3)
        except Exception as e:
            print(f"[Label Worker] 轮询异常: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)


def _start_label_workers():
    """启动后台工作线程（服务初始化时调用）"""
    _reset_running_tasks()
    t = threading.Thread(target=_label_worker_loop, daemon=True)
    t.start()
    print("[Label Worker] 后台工作线程已启动")


def _list_tasks(shop_id, status=None, page=1, page_size=10):
    """分页查询任务列表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            conditions = ["shop_id = %s"]
            params = [shop_id]
            if status:
                conditions.append("status = %s")
                params.append(status)
            where_clause = " AND ".join(conditions)

            cursor.execute(
                f"SELECT COUNT(*) as total FROM fba_label_tasks WHERE {where_clause}",
                tuple(params)
            )
            total = cursor.fetchone()['total']

            offset = (page - 1) * page_size
            cursor.execute(f"""
                SELECT id, shop_id, inbound_plan_id, status, progress, message, result_path,
                       created_at, completed_at, expired_at
                FROM fba_label_tasks
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, tuple(params + [page_size, offset]))
            rows = cursor.fetchall()
            return {
                "list": rows,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    finally:
        conn.close()


def _delete_task_db(task_id):
    """删除任务记录及文件"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT result_path, cargo_agent_path FROM fba_label_tasks WHERE id = %s", (task_id,))
            row = cursor.fetchone()
            if row:
                # 清理结果目录
                task_dir = os.path.join(FBA_LABEL_TASK_DIR, task_id)
                if os.path.exists(task_dir):
                    shutil.rmtree(task_dir, ignore_errors=True)
                # 清理货代文件
                if row.get('cargo_agent_path') and os.path.exists(row['cargo_agent_path']):
                    agent_dir = os.path.dirname(row['cargo_agent_path'])
                    if os.path.exists(agent_dir):
                        shutil.rmtree(agent_dir, ignore_errors=True)
                cursor.execute("DELETE FROM fba_label_tasks WHERE id = %s", (task_id,))
                conn.commit()
                return True
            return False
    finally:
        conn.close()


def _cancel_task_db(task_id):
    """取消 pending 状态的任务"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status FROM fba_label_tasks WHERE id = %s", (task_id,))
            row = cursor.fetchone()
            if not row:
                return False, "任务不存在"
            if row['status'] != 'pending':
                return False, "只能取消 pending 状态的任务"
            cursor.execute("UPDATE fba_label_tasks SET status = 'cancelled' WHERE id = %s", (task_id,))
            conn.commit()
            return True, None
    finally:
        conn.close()


def _retry_task_db(task_id):
    """复制失败任务为新任务"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT shop_id, inbound_plan_id, crop_config
                FROM fba_label_tasks WHERE id = %s
            """, (task_id,))
            row = cursor.fetchone()
            if not row:
                return None, "任务不存在"

            new_task_id = str(uuid.uuid4())
            expired_at = datetime.now() + timedelta(hours=24)
            cursor.execute("""
                INSERT INTO fba_label_tasks (id, shop_id, inbound_plan_id, crop_config, status, progress, expired_at)
                VALUES (%s, %s, %s, %s, 'pending', 0, %s)
            """, (new_task_id, row['shop_id'], row['inbound_plan_id'], row.get('crop_config'), expired_at))
            conn.commit()
            return new_task_id, None
    finally:
        conn.close()


@fba_tools_bp.route('/fba/organize-plan-labels/tasks', methods=['GET'])
@login_required
@permission_required('fba_tools:generate')
def list_organize_tasks():
    """查询箱唛整理任务列表"""
    try:
        shop_id = request.args.get('shop_id', '').strip()
        if not shop_id:
            return jsonify({"status": "error", "message": "shop_id 不能为空"}), 400
        try:
            shop_id = int(shop_id)
        except ValueError:
            return jsonify({"status": "error", "message": "shop_id 格式错误"}), 400

        status = request.args.get('status', '').strip() or None
        page = request.args.get('page', 1)
        page_size = request.args.get('page_size', 10)
        try:
            page = max(1, int(page))
            page_size = max(1, min(100, int(page_size)))
        except ValueError:
            page = 1
            page_size = 10

        result = _list_tasks(shop_id, status=status, page=page, page_size=page_size)

        for item in result['list']:
            item['created_at'] = item['created_at'].isoformat() if item['created_at'] else None
            item['completed_at'] = item['completed_at'].isoformat() if item['completed_at'] else None
            item['expired_at'] = item['expired_at'].isoformat() if item['expired_at'] else None
            if item['status'] == 'completed' and item['result_path']:
                item['download_url'] = f"/api/fba/organize-plan-labels/tasks/{item['id']}/download"
            else:
                item['download_url'] = None

        return jsonify({"status": "success", "data": result})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@fba_tools_bp.route('/fba/organize-plan-labels/tasks/<task_id>', methods=['DELETE'])
@login_required
@permission_required('fba_tools:generate')
def delete_organize_task(task_id):
    """删除箱唛整理任务"""
    try:
        success = _delete_task_db(task_id)
        if not success:
            return jsonify({"status": "error", "message": "任务不存在"}), 404
        return jsonify({"status": "success", "message": "任务已删除"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@fba_tools_bp.route('/fba/organize-plan-labels/tasks/<task_id>/cancel', methods=['POST'])
@login_required
@permission_required('fba_tools:generate')
def cancel_organize_task(task_id):
    """取消箱唛整理任务"""
    try:
        success, message = _cancel_task_db(task_id)
        if not success:
            return jsonify({"status": "error", "message": message}), 400
        return jsonify({"status": "success", "message": "任务已取消"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@fba_tools_bp.route('/fba/organize-plan-labels/tasks/<task_id>/retry', methods=['POST'])
@login_required
@permission_required('fba_tools:generate')
def retry_organize_task(task_id):
    """重试箱唛整理任务"""
    try:
        new_task_id, message = _retry_task_db(task_id)
        if not new_task_id:
            return jsonify({"status": "error", "message": message}), 400
        return jsonify({"status": "success", "message": "任务已重新提交", "data": {"task_id": new_task_id}})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
