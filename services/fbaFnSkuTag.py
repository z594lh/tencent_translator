import os
from reportlab.pdfgen import canvas
from reportlab.graphics.barcode import code128
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# --- 配置区 ---
FONT_PATH = os.path.join("static", "font", "SimHei.TTF")
OUTPUT_DIR = "static/fbatag"

# --- 字体注册 ---
if os.path.exists(FONT_PATH):
    pdfmetrics.registerFont(TTFont('SimHei', FONT_PATH))
else:
    print(f"❌ 警告：未找到字体文件 {FONT_PATH}，中文将无法显示！")

def generate_amazon_label_v4(
    fnsku: str,
    product_name: str,
    extra_info: str,
    sku: str,
    width_mm: float = 50,
    height_mm: float = 30,
    output_dir: str = "static/fbatag"
):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_name = f"Label-{sku}-{fnsku}.pdf"
    output_path = os.path.join(output_dir, file_name)

    # --- 1. 画布设置 ---
    width = width_mm * mm
    height = height_mm * mm
    c = canvas.Canvas(output_path, pagesize=(width, height))

    # --- 2. 条码参数（关键！）---
    # 50mm 标签，左右各留 3mm 边距，条码可用宽度 44mm
    margin = 3 * mm
    available_width = width - (margin * 2)
    
    # 先尝试 0.5mm，如果太宽就自动缩小
    bar_width = 0.5 * mm
    
    barcode = code128.Code128(
        fnsku, 
        barHeight=16 * mm, 
        barWidth=bar_width,
        quiet=False,        # 我们自己控制留白
        humanReadable=False
    )
    
    # 如果条码超出可用宽度，按比例缩小
    if barcode.width > available_width:
        scale = available_width / barcode.width
        bar_width = bar_width * scale
        barcode = code128.Code128(
            fnsku, 
            barHeight=16 * mm, 
            barWidth=bar_width,
            quiet=False,
            humanReadable=False
        )
        print(f"⚠️ 条码自动缩小至 {bar_width/mm:.2f}mm 以适配标签")
    
    # --- 3. 绘制条码（带留白背景）---
    bc_x = (width - barcode.width) / 2  # 居中
    bc_y = height - 19 * mm
    
    # 画白色背景块（强制留白，视觉上也有边距）
    quiet_zone = 3 * mm  # 两侧留白
    c.setFillColorRGB(1, 1, 1)
    c.rect(
        bc_x - quiet_zone,
        bc_y - 1 * mm,
        barcode.width + (quiet_zone * 2),
        16 * mm + 2 * mm,
        fill=1, stroke=0
    )
    c.setFillColorRGB(0, 0, 0)
    
    # 绘制条码
    barcode.drawOn(c, bc_x, bc_y)

    # --- 4. 绘制 FNSKU 文字 ---
    fnsku_text_y = bc_y - 5 * mm
    c.setFont("Helvetica-Bold", 10)
    c.drawCentredString(width / 2, fnsku_text_y, fnsku)

    # --- 5. 绘制产品信息 ---
    text_start_y = fnsku_text_y - 6 * mm
    line_height = 3.5 * mm
    
    c.setFont("SimHei", 8)
    c.drawString(3 * mm, text_start_y, product_name)
    
    c.setFont("SimHei", 7)
    c.drawString(3 * mm, text_start_y - line_height, extra_info)

    # --- 6. 绘制底部信息 ---
    bottom_y = 2 * mm
    c.setFont("SimHei", 8)
    
    c.drawString(3 * mm, bottom_y, f"SKU:{sku}")
    
    mic_text = "Made In China"
    mic_width = c.stringWidth(mic_text, "SimHei", 8)
    c.drawString(width - 3 * mm - mic_width, bottom_y, mic_text)

    c.save()
    print(f"✅ 标签生成成功: {output_path}")
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