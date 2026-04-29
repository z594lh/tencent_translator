"""
PDF 裁剪与拆分服务 - 使用 PyMuPDF
"""
import fitz
import io
import zipfile


def _fabric_to_pdf_rect(left, top, width, height, page_rect, scale):
    """前端 Fabric.js 坐标(px, 左上角原点) → PyMuPDF Rect(pt, 左上角原点)"""
    x0 = page_rect.x0 + left / scale
    x1 = page_rect.x0 + (left + width) / scale
    y0 = page_rect.y0 + top / scale
    y1 = page_rect.y0 + (top + height) / scale
    return fitz.Rect(x0, y0, x1, y1)


def crop_pdf(file_stream, operations):
    """
    裁剪 PDF 页面
    :param file_stream: PDF 文件流 (bytes 或 file-like)
    :param operations: 操作列表 [{"type":"crop","page":0,"bbox":[left,top,width,height],"scale":1.5}, ...]
    :return: BytesIO 对象
    """
    doc = fitz.open(stream=file_stream, filetype="pdf")

    for op in operations:
        if op.get('type') != 'crop':
            continue
        page_num = op.get('page', 0)
        if page_num < 0 or page_num >= len(doc):
            continue

        page = doc[page_num]
        page_rect = page.rect
        scale = op.get('scale', 1.5)

        left, top, width, height = op['bbox']
        rect = _fabric_to_pdf_rect(left, top, width, height, page_rect, scale)
        # 限制在页面边界内
        rect.x0 = max(page_rect.x0, min(rect.x0, page_rect.x1 - 1))
        rect.y0 = max(page_rect.y0, min(rect.y0, page_rect.y1 - 1))
        rect.x1 = max(rect.x0 + 1, min(rect.x1, page_rect.x1))
        rect.y1 = max(rect.y0 + 1, min(rect.y1, page_rect.y1))
        page.set_cropbox(rect)

    output = io.BytesIO()
    doc.save(output)
    output.seek(0)
    doc.close()
    return output


def split_pdf(file_stream, pages):
    """
    拆分 PDF 页面
    :param file_stream: PDF 文件流
    :param pages: 页码列表 [0, 2, ...]
    :return: (output_bytes, is_zip, download_name)
    """
    doc = fitz.open(stream=file_stream, filetype="pdf")

    if len(pages) == 1:
        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=pages[0], to_page=pages[0])
        output = io.BytesIO()
        new_doc.save(output)
        output.seek(0)
        doc.close()
        new_doc.close()
        return output, False, f'page_{pages[0] + 1}.pdf'
    else:
        output = io.BytesIO()
        with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
            for page_num in pages:
                new_doc = fitz.open()
                new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
                page_bytes = io.BytesIO()
                new_doc.save(page_bytes)
                zf.writestr(f'page_{page_num + 1}.pdf', page_bytes.getvalue())
                new_doc.close()
        doc.close()
        output.seek(0)
        return output, True, 'split_pages.zip'
