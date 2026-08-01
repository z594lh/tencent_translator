import os
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
    print(f"[FBA Label] 警告：未找到字体文件 {FONT_PATH}，中文将无法显示！")


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

    # 第 1 遍：参考条宽 0.3mm，量出实际模块数
    ref_mw, ref_qz = 0.3, 3.0
    _, ref_w_mm, _ = _build(ref_mw, ref_qz)
    module_count = (ref_w_mm - 2 * ref_qz) / ref_mw

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
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    from datetime import datetime
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f"Label-{sku}-{fnsku}-{ts}.pdf"
    output_path = os.path.join(output_dir, file_name)

    # --- 1. 画布设置 ---
    width = width_mm * mm
    height = height_mm * mm
    c = canvas.Canvas(output_path, pagesize=(width, height))

    # --- 2. 条码参数（关键！）---
    margin = 3 * mm
    available_width = width - (margin * 2)

    # 用位图条码替代矢量条码，宽度自动撑满可用区域；热敏打印按像素渲染，扫不出来概率大幅降低
    bc_img, bc_w_mm, bc_h_mm = _make_code128_image(fnsku, target_width_mm=available_width / mm)
    natural_w = bc_w_mm * mm
    natural_h = bc_h_mm * mm

    # 兜底：条宽触及下限导致条码仍超出可用宽度时，按比例缩小（位图缩放比矢量小数条宽稳定）
    if natural_w > available_width:
        scale = available_width / natural_w
        draw_w = available_width
        draw_h = natural_h * scale
        if scale < 0.999:  # 像素取整导致的 0.0x mm 溢出不算缩小，不打印误导日志
            print(f"[FBA Label] FNSKU 过长，条码自动缩小至 {scale:.1%} 以适配标签")
    else:
        draw_w = natural_w
        draw_h = natural_h

    # --- 3. 绘制条码（带留白背景）---
    bc_x = (width - draw_w) / 2  # 居中
    bc_y = height - 19 * mm

    # 画白色背景块（强制留白，视觉上也有边距）
    quiet_zone = 3 * mm  # 两侧留白
    c.setFillColorRGB(1, 1, 1)
    c.rect(
        bc_x - quiet_zone,
        bc_y - 1 * mm,
        draw_w + (quiet_zone * 2),
        draw_h + 2 * mm,
        fill=1, stroke=0
    )
    c.setFillColorRGB(0, 0, 0)

    # 绘制条码位图
    c.drawImage(bc_img, bc_x, bc_y, width=draw_w, height=draw_h)

    # --- 4. 动态布局：条码和底部固定，中间均分间距 ---
    bottom_baseline = 2 * mm                       # 底部 SKU/MIC 基线（固定）
    top_of_bottom_row = 6 * mm                     # 底部行占 6mm
    barcode_bottom = bc_y                          # 条码底部（固定）

    # 30mm 及以下缩小产品名字号
    is_tight = height_mm <= 30
    pn_font_size = 6 if is_tight else 8

    # 中间文本行（从上到下：FNSKU → 产品名 → 附加信息 够空间才加）
    mid_lines = [
        ("Helvetica-Bold", 10, fnsku, True),          # FNSKU 居中
        ("SimHei", pn_font_size, product_name, False), # 产品名 左对齐
    ]
    if extra_info and (barcode_bottom - top_of_bottom_row) > 14 * mm:
        mid_lines.append(("SimHei", 6 if is_tight else 7, extra_info, False))

    # FNSKU 基线：升部 2.5mm + 间距 1mm = 条码底向下 3.5mm
    # 保证 FNSKU 文字和条码之间有可见间距
    fnsku_baseline = barcode_bottom - 3.5 * mm

    # FNSKU 以下可用空间
    below_fnsku = fnsku_baseline - 1 * mm - top_of_bottom_row  # 减去 FNSKU 降部 1mm
    other_lines = mid_lines[1:]                                 # FNSKU 之外的行
    n_other = len(other_lines)
    line_h = 2.5 * mm if is_tight else 3.2 * mm
    other_block_h = n_other * line_h
    other_gap = (below_fnsku - other_block_h) / (n_other + 1)
    if other_gap < 0.5 * mm:
        other_gap = 0.5 * mm

    # 渲染 FNSKU（居中）
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(width / 2, fnsku_baseline, fnsku)

    # 渲染其余行（从左到右从上往下）
    y = fnsku_baseline - 1 * mm - other_gap            # FNSKU 降部下方起始
    for font_name, font_size, text, centered in other_lines:
        y -= line_h
        baseline = y + 0.8 * mm
        c.setFont(font_name, font_size)
        c.drawString(3 * mm, baseline, text)
        y -= other_gap

    # --- 5. 绘制底部信息（SKU + Made In China，固定贴底）---
    bottom_font_size = 6 if is_tight else 8
    c.setFont("SimHei", bottom_font_size)
    c.drawString(3 * mm, bottom_baseline, f"SKU:{sku}")
    mic_text = "Made In China"
    mic_width = c.stringWidth(mic_text, "SimHei", bottom_font_size)
    c.drawString(width - 3 * mm - mic_width, bottom_baseline, mic_text)

    c.save()
    print(f"[FBA Label] 标签生成成功: {output_path}")
    return output_path

# ==================== 测试运行 ====================
if __name__ == "__main__":
    generate_amazon_label_v4(
        fnsku="UTAG40895",
        product_name="迷你办公桌",
        extra_info="浅棕色",
        sku="BVQ9CE0002",
        output_dir="static/fbatag"
    )