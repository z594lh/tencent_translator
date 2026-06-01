"""
Amazon 货件发票导出模块
根据 shipment_id 导出通用发票模板 xlsx
"""
import io
import json
import os
import urllib.request
from decimal import Decimal
from urllib.parse import urlparse

from flask import Blueprint, request, jsonify, send_file
from blueprints.user_auth import login_required, permission_required
from services.mysql_service import get_db_connection
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, Border, Side
from openpyxl.drawing.image import Image as XLImage
from openpyxl.drawing.spreadsheet_drawing import OneCellAnchor, AnchorMarker
from openpyxl.drawing.xdr import XDRPositiveSize2D
from openpyxl.utils.units import pixels_to_EMU
from PIL import Image as PILImage

amazon_invoice_export_bp = Blueprint('amazon_invoice_export', __name__, url_prefix='/api')

TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'static', 'shipping_invoice_templates', 'yuntuo.xlsx'
)
TEMPLATE_HEADER_ROW = 5
TEMPLATE_DATA_START_ROW = 6


def _convert_length(value, unit):
    """长度单位转 cm"""
    if value is None or unit is None:
        return None
    unit = unit.upper()
    if unit == "IN":
        return float(value) * 2.54
    if unit == "CM":
        return float(value)
    return float(value)


def _convert_weight(value, unit):
    """重量单位转 kg"""
    if value is None or unit is None:
        return None
    unit = unit.upper()
    if unit in ("LB", "LBS", "POUND", "POUNDS"):
        return float(value) * 0.453592
    if unit in ("KG", "KGS", "KILOGRAM", "KILOGRAMS"):
        return float(value)
    return float(value)


def _require_shop_id() -> int:
    """强制获取 shop_id，不传则抛异常"""
    shop_id = request.args.get('shop_id', '').strip() or None
    if not shop_id:
        raise ValueError("缺少必要参数: shop_id")
    try:
        return int(shop_id)
    except ValueError:
        raise ValueError("shop_id 必须是整数")


def _fetch_shipment_invoice_data(shop_id, shipment_id):
    """获取指定 shipment_id 的发票数据"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 1. 查 shipment 仓库代码和亚马逊参考号
            cursor.execute(
                "SELECT destination_warehouse_id, amazon_reference_id FROM amazon_inbound_shipments_detail WHERE shop_id = %s AND shipment_confirmation_id = %s",
                (shop_id, shipment_id),
            )
            shipment_row = cursor.fetchone()
            warehouse_id = (
                shipment_row["destination_warehouse_id"]
                if shipment_row
                else ""
            )
            amazon_reference_id = (
                shipment_row["amazon_reference_id"]
                if shipment_row
                else ""
            )

            # 2. 查箱子列表
            cursor.execute(
                """
                SELECT * FROM amazon_inbound_plan_boxes
                WHERE shop_id = %s AND shipment_id = %s
                ORDER BY id ASC
                """,
                (shop_id, shipment_id),
            )
            boxes = cursor.fetchall()

            if not boxes:
                return warehouse_id, []

            # 3. 收集所有 msku，批量查 products
            msku_set = set()
            box_items = []
            for box in boxes:
                items = json.loads(box["items_json"] or "[]")
                for item in items:
                    msku = item.get("msku")
                    if msku:
                        msku_set.add(msku)
                    box_items.append((box, item))

            product_map = {}
            if msku_set:
                placeholders = ",".join(["%s"] * len(msku_set))
                cursor.execute(
                    f"""
                    SELECT 
                        seller_sku,
                        declare_name_en,
                        declare_name_cn,
                        material_en,
                        material_cn,
                        purpose,
                        brand,
                        model,
                        declare_value,
                        currency,
                        hs_code,
                        is_electric,
                        is_magnetic,
                        amazon_internal_id,
                        vat_number,
                        eori_number,
                        sales_url,
                        weight_kg,
                        dimensions_cm,
                        asin,
                        image_url,
                        fnsku
                    FROM products
                    WHERE seller_sku IN ({placeholders})
                    """,
                    tuple(msku_set),
                )
                for prod in cursor.fetchall():
                    product_map[prod["seller_sku"]] = prod

            # 4. 组装数据
            results = []
            for box, item in box_items:
                msku = item.get("msku")
                prod = product_map.get(msku, {})
                box_qty = box.get("quantity") or 1
                item_qty = item.get("quantity") or 0
                total_qty = box_qty * item_qty
                unit_price = prod.get("declare_value") or Decimal("0")
                total_value = float(unit_price) * total_qty

                results.append(
                    {
                        "图片(不能超出单元格)": prod.get("image_url") or "",
                        "是否报关": "否",
                        "件数CTN": box_qty,
                        "英文品名": prod.get("declare_name_en") or "",
                        "中文品名": prod.get("declare_name_cn") or "",
                        "英文材质": prod.get("material_en") or "",
                        "中文材质": prod.get("material_cn") or "",
                        "用途": prod.get("purpose") or "",
                        "品牌": prod.get("brand") or "",
                        "型号": prod.get("model") or "",
                        "单箱申报数量(PCS)": item_qty,
                        "申报总数量PCS": total_qty,
                        "单个产品申报货值": float(unit_price) if unit_price else 0,
                        "总申报货值": total_value,
                        "币种": prod.get("currency") or "USD",
                        "单箱重量(kg)": (
                            round(
                                _convert_weight(
                                    box.get("weight_value"), box.get("weight_unit")
                                ),
                                3,
                            )
                            if _convert_weight(
                                box.get("weight_value"), box.get("weight_unit")
                            )
                            else ""
                        ),
                        "海关编码": prod.get("hs_code") or "",
                        "是否带电": "是" if prod.get("is_electric") else "否",
                        "是否带磁": "是" if prod.get("is_magnetic") else "否",
                        "FBA货箱编号": box.get("box_id") or "",
                        "亚马逊内部编码ID": amazon_reference_id or "",
                        "货箱长度(cm)": (
                            round(
                                _convert_length(
                                    box.get("dimensions_length"),
                                    box.get("dimensions_unit"),
                                ),
                                2,
                            )
                            if _convert_length(
                                box.get("dimensions_length"),
                                box.get("dimensions_unit"),
                            )
                            else ""
                        ),
                        "货箱宽度(cm)": (
                            round(
                                _convert_length(
                                    box.get("dimensions_width"),
                                    box.get("dimensions_unit"),
                                ),
                                2,
                            )
                            if _convert_length(
                                box.get("dimensions_width"),
                                box.get("dimensions_unit"),
                            )
                            else ""
                        ),
                        "货箱高度(cm)": (
                            round(
                                _convert_length(
                                    box.get("dimensions_height"),
                                    box.get("dimensions_unit"),
                                ),
                                2,
                            )
                            if _convert_length(
                                box.get("dimensions_height"),
                                box.get("dimensions_unit"),
                            )
                            else ""
                        ),
                        "仓库代码": warehouse_id,
                        "派送地址": box.get("destination_region_state") or "",
                        "国家": "US",
                        "VAT号码": prod.get("vat_number") or "",
                        "EORI号码": prod.get("eori_number") or "",
                        "销售链接": prod.get("sales_url") or "",
                        "产品重量(kg)": (
                            float(prod.get("weight_kg"))
                            if prod.get("weight_kg")
                            else ""
                        ),
                        "产品尺寸(长*宽*高cm)": prod.get("dimensions_cm") or "",
                        "ASIN": prod.get("asin") or "",
                        "FNSKU": prod.get("fnsku") or "",
                    }
                )

            return warehouse_id, results
    finally:
        conn.close()


def _build_excel(data):
    """基于模板构建 xlsx，返回 BytesIO"""
    wb = load_workbook(TEMPLATE_PATH)
    ws = wb.active

    header_row = TEMPLATE_HEADER_ROW
    data_start_row = TEMPLATE_DATA_START_ROW

    headers = []
    for col in range(1, ws.max_column + 1):
        cell_value = ws.cell(row=header_row, column=col).value
        if cell_value:
            headers.append(str(cell_value))
        else:
            break
    if not headers:
        headers = list(data[0].keys())

    img_col_idx = None
    for idx, h in enumerate(headers, 1):
        if h == "图片(不能超出单元格)":
            img_col_idx = idx
            break

    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    for row_offset, row_data in enumerate(data):
        row_idx = data_start_row + row_offset
        ws.row_dimensions[row_idx].height = 60
        for col_idx, h in enumerate(headers, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=row_data.get(h))
            cell.border = thin_border
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    if img_col_idx:
        for row_offset, row_data in enumerate(data):
            row_idx = data_start_row + row_offset
            img_url = row_data.get("图片(不能超出单元格)")
            if not img_url:
                continue
            try:
                pil_img = None
                parsed = urlparse(img_url)
                path = parsed.path
                if path.startswith("/static/") or path.startswith("static/"):
                    local_path = path.lstrip("/")
                    pil_img = PILImage.open(local_path)
                else:
                    req = urllib.request.Request(
                        img_url, headers={"User-Agent": "Mozilla/5.0"}
                    )
                    with urllib.request.urlopen(req, timeout=15) as response:
                        image_bytes = response.read()
                    pil_img = PILImage.open(io.BytesIO(image_bytes))

                if pil_img.mode in ("RGBA", "LA", "P"):
                    background = PILImage.new("RGB", pil_img.size, (255, 255, 255))
                    if pil_img.mode == "P":
                        pil_img = pil_img.convert("RGBA")
                    if pil_img.mode in ("RGBA", "LA"):
                        background.paste(
                            pil_img,
                            mask=pil_img.split()[-1],
                        )
                        pil_img = background
                    else:
                        pil_img = pil_img.convert("RGB")

                max_width = 60
                max_height = 50
                pil_img.thumbnail((max_width, max_height), PILImage.LANCZOS)

                img_buf = io.BytesIO()
                pil_img.save(img_buf, format="PNG")
                img_buf.seek(0)
                xl_img = XLImage(img_buf)

                cell_width_px = 84
                cell_height_px = 80
                offset_x = max(0, (cell_width_px - xl_img.width) // 2)
                offset_y = max(0, (cell_height_px - xl_img.height) // 2)

                marker = AnchorMarker(
                    col=img_col_idx - 1,
                    row=row_idx - 1,
                    colOff=pixels_to_EMU(offset_x),
                    rowOff=pixels_to_EMU(offset_y),
                )
                size = XDRPositiveSize2D(
                    cx=pixels_to_EMU(xl_img.width),
                    cy=pixels_to_EMU(xl_img.height),
                )
                xl_img.anchor = OneCellAnchor(_from=marker, ext=size)
                ws._images.append(xl_img)

                cell = ws.cell(row=row_idx, column=img_col_idx)
                cell.value = None
            except Exception as e:
                print(f"[Invoice Export] 图片插入失败 [{img_url}]: {e}")
                pass

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


@amazon_invoice_export_bp.route('/amazon/shipments/<shipment_id>/invoice/export', methods=['GET'])
@login_required
@permission_required('amazon_invoice_export:export')
def export_shipment_invoice(shipment_id):
    """
    根据货件编号导出发票模板 xlsx
    查询参数（必填）:
        shop_id  - 店铺ID
    """
    try:
        shop_id = _require_shop_id()
        warehouse_id, data = _fetch_shipment_invoice_data(shop_id=shop_id, shipment_id=shipment_id)
        if not data:
            return jsonify({"status": "error", "message": "该货件没有箱子数据"}), 404

        xlsx_io = _build_excel(data)
        filename = f"{shipment_id}.xlsx"

        return send_file(
            xlsx_io,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
        )

    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        print(f"[Invoice Export] 导出异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
