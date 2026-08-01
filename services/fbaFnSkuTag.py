import os
import re
import time
import uuid
from datetime import datetime
from io import BytesIO

import barcode
from barcode.writer import ImageWriter
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# --- 配置区 ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FONT_PATH = os.path.join(BASE_DIR, "static", "font", "SIMHEI.TTF")
OUTPUT_DIR = os.path.join(BASE_DIR, "static", "fbatag")

# --- 字体注册 ---
if os.path.exists(FONT_PATH):
    pdfmetrics.registerFont(TTFont('SimHei', FONT_PATH))
else:
    print(f"[FBA Label] warning: font not found {FONT_PATH}, Chinese text may fail")


def _safe_filename_part(value: str, max_len: int = 30) -> str:
    """过滤文件名中的危险字符，防止路径穿越和文件名非法"""
    value = value.strip()[:max_len]
    return re.sub(r'[^A-Za-z0-9_-]', '_', value)


def _cleanup_old_labels(directory: str, keep_days: int = 7):
    """清理指定目录下超过 keep_days 天的旧标签 PDF"""
    if not os.path.isdir(directory):
        return
    now = time.time()
    deadline = now - keep_days * 86400
    try:
        for fname in os.listdir(directory):
            if not fname.startswith("Label-") or not fname.endswith(".pdf"):
                continue
            fpath = os.path.join(directory, fname)
            try:
                if os.path.getmtime(fpath) < deadline:
                    os.remove(fpath)
                    print(f"[FBA Label] cleanup old label: {fname}")
            except OSError:
                pass
    except Exception as e:
        print(f"[FBA Label] cleanup old labels failed: {e}")


def _truncate_text(text: str, font_name: str, font_size: float, max_width: float, c: canvas.Canvas) -> str:
    """按最大宽度截断文本，超出加省略号"""
    if not text:
        return text
    text_width = c.stringWidth(text, font_name, font_size)
    if text_width <= max_width:
        return text
    ellipsis = "..."
    ellipsis_width = c.stringWidth(ellipsis, font_name, font_size)
    avail = max_width - ellipsis_width
    if avail <= 0:
        return text[:10]
    # 从后往前截断
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if c.stringWidth(text[:mid], font_name, font_size) <= avail:
            lo = mid
        else:
            hi = mid - 1
    return text[:lo] + ellipsis


def _make_code128_image(fnsku: str, target_width_mm: float, module_height_mm: float = 16.0, dpi: int = 600):
    """生成 Code128 条码位图，宽度自动撑满 target_width_mm，返回 (ImageReader, width_mm, height_mm)

    两遍生成：
      1. 先用参考条宽生成一次，测出条码实际模块数（python-barcode 会自动用 Code-C 压缩数字段，模块数不能硬算）；
      2. 按目标宽度反推条宽重新生成，让条码像原矢量版一样撑满标签，屏幕/打印都好扫。

    600 DPI 保证条边界量化误差 < 0.03mm，远小于条宽本身，热敏打印稳定。
    """
    def _build(module_width_mm: float, quiet_zone_mm: float):
        bc = barcode.get('code128', fnsku, writer=ImageWriter())
        buf = BytesIO()
        bc.write(buf, {
            'module_width': module_width_mm,
            'module_height': module_height_mm,
            'quiet_zone': quiet_zone_mm,
            'write_text': False,  # 不在图片里写字，FNSKU 单独画在 PDF 上
            'dpi': dpi,
        })
        buf.seek(0)
        img = Image.open(buf)
        img.load()
        return img, img.width / dpi * 25.4, img.height / dpi * 25.4

    if not fnsku or not re.fullmatch(r'[A-Za-z0-9]+', fnsku):
        raise ValueError("FNSKU 不能为空且只能包含字母和数字")

    # 第 1 遍：参考条宽 0.3mm，量出实际模块数
    ref_mw, ref_qz = 0.3, 3.0
    _, ref_w_mm, _ = _build(ref_mw, ref_qz)
    module_count = (ref_w_mm - 2 * ref_qz) / ref_mw
    if module_count <= 0:
        raise ValueError("FNSKU 条码模块数计算异常")

    # 第 2 遍：按目标宽度反推条宽（两侧安静区按 10 个模块预留，Code128 规范要求）
    module_width = target_width_mm / (module_count + 20)
    module_width = max(module_width, 0.18)  # 下限，防止超长 FNSKU 条宽过小
    img, w_mm, h_mm = _build(module_width, 10 * module_width)

    return ImageReader(img), w_mm, h_mm


def generate_amazon_label_v4(
    fnsku: str,
    product_name: str,
    extra_info: str,
    sku: str,
    width_mm: float = 70,
    height_mm: float = 40,
    output_dir: str = "static/fbatag"
):
    if not fnsku or not re.fullmatch(r'[A-Za-z0-9]+', fnsku):
        raise ValueError("fnsku 不能为空且只能包含字母和数字")
    if not sku:
        raise ValueError("sku 不能为空")

    # 尺寸下限：40x30mm，防止条码被裁切导致扫不出来
    try:
        width_mm = float(width_mm)
        height_mm = float(height_mm)
    except (TypeError, ValueError):
        raise ValueError("width_mm / height_mm 格式错误")
    if width_mm < 40 or height_mm < 30:
        raise ValueError("标签尺寸不能小于 40x30mm")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 清理 7 天前的旧标签，避免 static/fbatag 无限膨胀
    _cleanup_old_labels(output_dir, keep_days=7)

    # 文件名加短 UUID 防止 1 秒内并发冲突
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    uid = uuid.uuid4().hex[:8]
    file_name = f"Label-{_safe_filename_part(sku)}-{_safe_filename_part(fnsku)}-{ts}-{uid}.pdf"
    output_path = os.path.join(output_dir, file_name)

    c = None
    try:
        # --- 1. 画布设置 ---
        width = width_mm * mm
        height = height_mm * mm
        c = canvas.Canvas(output_path, pagesize=(width, height))

        # 预留 3mm 左右边距
        margin = 3 * mm
        available_width = width - (margin * 2)
        text_max_width = available_width - 1 * mm  # 文本左右再留 0.5mm 呼吸空间

        # --- 2. 条码参数（关键！）---
        bc_img, bc_w_mm, bc_h_mm = _make_code128_image(fnsku, target_width_mm=available_width / mm)
        natural_w = bc_w_mm * mm
        natural_h = bc_h_mm * mm

        # 兜底：条宽触及下限导致条码仍超出可用宽度时，按比例缩小
        if natural_w > available_width:
            scale = available_width / natural_w
            draw_w = available_width
            draw_h = natural_h * scale
            if scale < 0.999:
                print(f"[FBA Label] fnsku too long, barcode scaled to {scale:.1%}")
        else:
            draw_w = natural_w
            draw_h = natural_h

        # --- 3. 绘制条码（带留白背景）---
        bc_x = (width - draw_w) / 2  # 居中
        bc_y = height - 19 * mm

        quiet_zone = 3 * mm
        c.setFillColorRGB(1, 1, 1)
        c.rect(
            bc_x - quiet_zone,
            bc_y - 1 * mm,
            draw_w + (quiet_zone * 2),
            draw_h + 2 * mm,
            fill=1, stroke=0
        )
        c.setFillColorRGB(0, 0, 0)
        c.drawImage(bc_img, bc_x, bc_y, width=draw_w, height=draw_h)

        # --- 4. 动态布局：条码和底部固定，中间均分间距 ---
        bottom_baseline = 2 * mm
        top_of_bottom_row = 6 * mm
        barcode_bottom = bc_y

        is_tight = height_mm <= 30
        pn_font_size = 6 if is_tight else 8
        ei_font_size = 6 if is_tight else 7

        # 截断文本，防止超长产品名或附加信息超出标签右边界
        display_name = _truncate_text(product_name, "SimHei", pn_font_size, text_max_width, c)
        display_extra = _truncate_text(extra_info, "SimHei", ei_font_size, text_max_width, c)

        # 中间文本行（FNSKU 居中，产品名 / 附加信息左对齐）
        mid_lines = [("SimHei", pn_font_size, display_name)]
        if display_extra and (barcode_bottom - top_of_bottom_row) > 14 * mm:
            mid_lines.append(("SimHei", ei_font_size, display_extra))

        fnsku_baseline = barcode_bottom - 3.5 * mm
        below_fnsku = fnsku_baseline - 1 * mm - top_of_bottom_row
        n_other = len(mid_lines)
        line_h = 2.5 * mm if is_tight else 3.2 * mm
        other_block_h = n_other * line_h
        other_gap = (below_fnsku - other_block_h) / (n_other + 1)
        if other_gap < 0.5 * mm:
            other_gap = 0.5 * mm

        # 渲染 FNSKU（居中）
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(width / 2, fnsku_baseline, fnsku)

        # 渲染产品名 / 附加信息（左对齐）
        y = fnsku_baseline - 1 * mm - other_gap
        for font_name, font_size, text in mid_lines:
            y -= line_h
            baseline = y + 0.8 * mm
            c.setFont(font_name, font_size)
            c.drawString(3 * mm, baseline, text)
            y -= other_gap

        # --- 5. 绘制底部信息（SKU + Made In China）---
        bottom_font_size = 6 if is_tight else 8
        c.setFont("SimHei", bottom_font_size)
        mic_text = "Made In China"
        mic_width = c.stringWidth(mic_text, "SimHei", bottom_font_size)
        sku_text = f"SKU:{sku}"
        # SKU 从 3mm 处左对齐绘制，预留右侧 MIC 空间（加 1mm 间隙），避免重叠
        sku_max_width = width - 6 * mm - mic_width - 1 * mm
        sku_display = _truncate_text(sku_text, "SimHei", bottom_font_size, sku_max_width, c)
        c.drawString(3 * mm, bottom_baseline, sku_display)
        c.drawString(width - 3 * mm - mic_width, bottom_baseline, mic_text)

        c.save()
        print(f"[FBA Label] label generated: {output_path}")
        return output_path

    except Exception as e:
        # 生成失败时删除半成品 PDF，避免留下空文件
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except OSError:
                pass
        raise


# ==================== 测试运行 ====================
if __name__ == "__main__":
    generate_amazon_label_v4(
        fnsku="UTAG40895",
        product_name="迷你办公桌",
        extra_info="浅棕色",
        sku="BVQ9CE0002",
        output_dir="static/fbatag"
    )