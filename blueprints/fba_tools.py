"""
FBA 工具模块 - 亚马逊 SKU 标签生成 / PDF 编辑
"""
from flask import Blueprint, request, jsonify, send_file
import os
from dotenv import load_dotenv

from blueprints.user_auth import login_required
from services.fbaFnSkuTag import generate_amazon_label_v4
from services.pdf_editor import crop_pdf, split_pdf

import io
import json
import re

import fitz
from services.mysql_service import get_db_connection

# 创建 Blueprint
fba_tools_bp = Blueprint('fba_tools', __name__, url_prefix='/api')

# 加载环境变量
load_dotenv(override=True)
BASE_URL = os.getenv("BASE_URL", "")


@fba_tools_bp.route('/fba/label', methods=['POST'])
@login_required
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
def edit_pdf():
    """
    裁剪 PDF 页面
    请求: multipart/form-data
      - file: 原始 PDF
      - operations: JSON字符串 [{"type":"crop","page":0,"bbox":[left,top,width,height],"scale":1.5}, ...]
    返回: 裁剪后的 PDF 文件流
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "请上传 PDF 文件"}), 400

    file = request.files['file']
    operations_raw = request.form.get('operations', '[]')
    try:
        operations = json.loads(operations_raw)
    except Exception:
        return jsonify({"status": "error", "message": "参数格式错误"}), 400

    try:
        output = crop_pdf(file.read(), operations)
        return send_file(
            output,
            mimetype='application/pdf',
            as_attachment=True,
            download_name='cropped.pdf'
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"裁剪失败: {str(e)}"}), 500


def _extract_fba_info_from_text(text):
    """从页面文本中提取 FBA 号和 SKU"""
    # FBA 号：匹配 FBA 开头字符串，去掉末尾 U+数字 箱号
    fba_id = None
    fba_match = re.search(r'FBA[A-Z0-9]+', text, re.IGNORECASE)
    if fba_match:
        fba_id = fba_match.group(0).upper()
        fba_id = re.sub(r'U\d+$', '', fba_id)

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
                        filenames[page_num] = f"{fba_id}{safe_name}{safe_sku}.pdf"
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
