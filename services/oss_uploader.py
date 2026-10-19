"""
阿里云 OSS 上传服务
封装文件/流上传到 OSS，返回可直接访问的 HTTPS URL

需要环境变量:
    ALI_ACCESS_KEY_ID     - 阿里云 AccessKey ID
    ALI_ACCESS_KEY_SECRET - 阿里云 AccessKey Secret
    ALI_ENDPOINT          - OSS 地域节点，如 oss-cn-shenzhen.aliyuncs.com
    ALI_BUCKET_NAME       - Bucket 名称
"""
import os
import uuid
import hashlib
import io
import re
import base64
from datetime import datetime
from urllib.parse import quote

from PIL import Image, ImageOps, UnidentifiedImageError
import oss2


def image_signature(image):
    """轻量图像特征只用于候选图相似比对，不作为Amazon官方处理状态。"""
    image = ImageOps.exif_transpose(image).convert('RGB')
    colors = base64.b64encode(image.resize((8, 8)).tobytes()).decode('ascii')
    values = list(image.convert('L').resize((17, 16)).tobytes())
    bits = ''.join('1' if values[y * 17 + x] > values[y * 17 + x + 1] else '0' for y in range(16) for x in range(16))
    return {'colors': colors, 'dhash': f'{int(bits, 2):064x}'}

# ==================== 读取环境变量 ====================
ALI_ACCESS_KEY_ID = os.getenv("ALI_ACCESS_KEY_ID", "")
ALI_ACCESS_KEY_SECRET = os.getenv("ALI_ACCESS_KEY_SECRET", "")
ALI_ENDPOINT = os.getenv("ALI_ENDPOINT", "")
ALI_BUCKET_NAME = os.getenv("ALI_BUCKET_NAME", "")


def _get_bucket():
    """获取 OSS Bucket 实例（延迟初始化）"""
    if not all([ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET, ALI_ENDPOINT, ALI_BUCKET_NAME]):
        missing = []
        if not ALI_ACCESS_KEY_ID:
            missing.append("ALI_ACCESS_KEY_ID")
        if not ALI_ACCESS_KEY_SECRET:
            missing.append("ALI_ACCESS_KEY_SECRET")
        if not ALI_ENDPOINT:
            missing.append("ALI_ENDPOINT")
        if not ALI_BUCKET_NAME:
            missing.append("ALI_BUCKET_NAME")
        raise ValueError(f"缺少阿里云 OSS 环境变量: {', '.join(missing)}")

    auth = oss2.Auth(ALI_ACCESS_KEY_ID, ALI_ACCESS_KEY_SECRET)
    bucket = oss2.Bucket(auth, ALI_ENDPOINT, ALI_BUCKET_NAME)
    return bucket


def _build_url(oss_key: str) -> str:
    """拼接 OSS 访问 URL"""
    endpoint = ALI_ENDPOINT
    # 如果 endpoint 已包含 http:// 或 https://，只取域名部分
    if endpoint.startswith("http://"):
        endpoint = endpoint[7:]
    elif endpoint.startswith("https://"):
        endpoint = endpoint[8:]
    return f"https://{ALI_BUCKET_NAME}.{endpoint}/{quote(oss_key, safe='/')}"


def generate_oss_key(filename: str, prefix: str = "amazon/listing/") -> str:
    """
    生成 OSS 对象键（带时间戳和 UUID，避免重名）
    :param filename: 原始文件名
    :param prefix: 路径前缀
    :return: 如 amazon/listing/20260509/xxxx-original.jpg
    """
    ext = os.path.splitext(filename)[1].lower()
    date_str = datetime.now().strftime("%Y%m%d")
    unique_id = uuid.uuid4().hex[:8]
    oss_name = f"{unique_id}{ext}"
    return f"{prefix}{date_str}/{oss_name}"


def upload_file(local_file_path: str, oss_key: str = None) -> dict:
    """
    上传本地文件到 OSS
    :param local_file_path: 本地文件绝对或相对路径
    :param oss_key: OSS 上的对象键（路径），为 None 时自动生成
    :return: {"success": True, "url": "https://...", "oss_key": "..."}
    """
    try:
        bucket = _get_bucket()
        filename = os.path.basename(local_file_path)
        if not oss_key:
            oss_key = generate_oss_key(filename)

        bucket.put_object_from_file(oss_key, local_file_path)
        url = _build_url(oss_key)

        return {
            "success": True,
            "url": url,
            "oss_key": oss_key,
            "filename": filename
        }
    except Exception as e:
        print(f"[OSS Upload] 上传失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "url": None,
            "oss_key": None
        }


def upload_stream(file_obj, oss_key: str = None, filename: str = "upload.bin") -> dict:
    """
    上传文件流到 OSS（适用于 Flask request.files）
    :param file_obj: 文件对象（如 FileStorage）
    :param oss_key: OSS 上的对象键，为 None 时自动生成
    :param filename: 原始文件名，用于生成默认 oss_key
    :return: {"success": True, "url": "https://...", "oss_key": "..."}
    """
    try:
        bucket = _get_bucket()
        if not oss_key:
            oss_key = generate_oss_key(filename)

        # 支持 Flask FileStorage 的 stream
        if hasattr(file_obj, "read"):
            data = file_obj.read()
            bucket.put_object(oss_key, data)
        else:
            bucket.put_object(oss_key, file_obj)

        url = _build_url(oss_key)

        return {
            "success": True,
            "url": url,
            "oss_key": oss_key,
            "filename": filename
        }
    except Exception as e:
        print(f"[OSS Upload] 流上传失败: {e}")
        return {
            "success": False,
            "error": str(e),
            "url": None,
            "oss_key": None
        }


def listing_image_prefix(shop_id, marketplace_id, sku, position):
    """SKU保持可辨识，特殊字符编码，避免多店或目录穿越碰撞。"""
    if not str(shop_id).isdigit() or int(shop_id) < 1:
        raise ValueError('上传图片必须指定有效店铺')
    if not isinstance(marketplace_id, str) or not re.fullmatch(r'[A-Z0-9]{8,30}', marketplace_id):
        raise ValueError('图片站点无效')
    if not isinstance(sku, str) or not sku.strip() or len(sku) > 200:
        raise ValueError('图片SKU无效')
    if position not in ('MAIN', 'SWCH', *[f'PT{i:02d}' for i in range(1, 9)]):
        raise ValueError('图片位置须为MAIN、PT01～PT08或SWCH')
    sku_folder = quote(sku, safe='-_').replace('.', '%2E')
    return f'amazon/listing/images/managed/{int(shop_id)}/{marketplace_id}/{sku_folder}/{position}/'


def upload_image_for_listing(file_obj, filename: str = None, *, shop_id=None, marketplace_id=None, sku=None, position=None) -> dict:
    """
    专用于亚马逊 Listing 图片上传的快捷方法
    自动添加 amazon/listing/images/ 前缀
    """
    if filename is None and hasattr(file_obj, "filename"):
        filename = file_obj.filename
    if not filename:
        filename = "image.jpg"
    prefix = listing_image_prefix(shop_id, marketplace_id, sku, position)
    data = file_obj.read(10 * 1024 * 1024 + 1)
    if not data or len(data) > 10 * 1024 * 1024:
        raise ValueError('图片须大于0且不超过10MB')
    try:
        with Image.open(io.BytesIO(data)) as image:
            image.verify()
        with Image.open(io.BytesIO(data)) as image:
            if image.format not in ('JPEG', 'PNG') or getattr(image, 'n_frames', 1) != 1:
                raise ValueError('本版仅支持静态JPEG和PNG图片')
            if image.width * image.height > 40000000 or min(image.size) < 100:
                raise ValueError('图片尺寸须至少100像素，且不超过4000万像素；平台类目规则另行校验')
            extension = '.jpg' if image.format == 'JPEG' else '.png'
            mime = 'image/jpeg' if extension == '.jpg' else 'image/png'
            width, height = image.size
            # 原图字节不转换；只为ERP生成独立小预览，避免反复下载原图。
            preview = ImageOps.exif_transpose(image).convert('RGB')
            signature = image_signature(preview)
            preview.thumbnail((320, 320))
            buffer = io.BytesIO()
            preview.save(buffer, 'JPEG', quality=72, optimize=True)
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError):
        raise ValueError('无法读取图片，文件损坏或不是有效图片') from None
    version = datetime.now().strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:12]
    directory = prefix + version + '/'
    key, preview_key = directory + 'original' + extension, directory + 'preview.jpg'
    bucket = _get_bucket()
    headers = {'Content-Type': mime, 'Cache-Control': 'public, max-age=31536000, immutable', 'x-oss-forbid-overwrite': 'true'}
    # 两对象任一失败不重试、不删除原图，保留可排查证据。
    bucket.put_object(key, data, headers=headers)
    bucket.put_object(preview_key, buffer.getvalue(), headers={**headers, 'Content-Type': 'image/jpeg'})
    return {'success': True, 'url': _build_url(key), 'oss_key': key, 'filename': filename,
            'preview_url': _build_url(preview_key), 'position': position, 'sha256': hashlib.sha256(data).hexdigest(),
            'width': width, 'height': height, 'size': len(data), 'preview_size': len(buffer.getvalue()), 'signature': signature}


def delete_object(oss_key: str) -> dict:
    """
    删除 OSS 上的单个对象
    :param oss_key: OSS 对象键，如 amazon/listing/images/20260509/abc123.jpg
    :return: {"success": True/False, "error": "..."}
    """
    try:
        if not oss_key:
            return {"success": False, "error": "oss_key 不能为空"}
        bucket = _get_bucket()
        bucket.delete_object(oss_key)
        return {"success": True, "error": None}
    except Exception as e:
        print(f"[OSS Delete] 删除失败: {e}")
        return {"success": False, "error": str(e)}


def delete_object_by_url(url: str) -> dict:
    """
    通过 HTTPS URL 删除 OSS 对象
    从 URL 中解析出 oss_key 后调用 delete_object
    """
    try:
        if not url:
            return {"success": False, "error": "url 不能为空"}
        # URL 格式: https://bucket.endpoint/oss_key
        # 需要去掉协议、bucket.endpoint/ 前缀
        endpoint_part = f"{ALI_BUCKET_NAME}."
        # 处理 endpoint 可能带协议的情况
        endpoint = ALI_ENDPOINT
        if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
        elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]

        # 期望的 URL 前缀
        prefix = f"https://{ALI_BUCKET_NAME}.{endpoint}/"
        if url.startswith(prefix):
            oss_key = url[len(prefix):]
        else:
            # 尝试用通用方式解析
            from urllib.parse import urlparse
            parsed = urlparse(url)
            oss_key = parsed.path.lstrip("/")
            # 如果 path 包含 bucket 名作为第一级，也去掉（某些 CDN 场景）
            if oss_key.startswith(ALI_BUCKET_NAME + "/"):
                oss_key = oss_key[len(ALI_BUCKET_NAME) + 1:]

        if not oss_key:
            return {"success": False, "error": "无法从 URL 解析 oss_key"}
        return delete_object(oss_key)
    except Exception as e:
        print(f"[OSS Delete] URL 解析失败: {e}")
        return {"success": False, "error": str(e)}


def cleanup_orphan_listing_images(dry_run: bool = True) -> dict:
    """
    清理亚马逊 Listing 图片目录中的孤儿文件
    扫描 OSS 的 amazon/listing/images/ 前缀，对比数据库 amazon_listing_images 表，
    删除没有被任何 Listing 引用的图片。

    :param dry_run: True 只统计不真正删除（试运行），False 真正删除
    :return: {"success": True/False, "scanned": N, "orphan": N, "deleted": N, "errors": [...]}
    """
    import time
    from services.mysql_service import get_db_connection

    prefix = "amazon/listing/images/"
    errors = []
    scanned = 0
    orphan = 0
    deleted = 0

    try:
        bucket = _get_bucket()

        # 1. 从数据库获取所有被引用的图片 URL
        conn = get_db_connection()
        referenced_urls = set()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT media_location FROM amazon_listing_images")
                for row in cursor.fetchall():
                    url = row.get("media_location")
                    if url:
                        referenced_urls.add(url)
        finally:
            conn.close()

        # 2. 遍历 OSS 前缀下的所有文件
        marker = ""
        while True:
            result = bucket.list_objects(prefix=prefix, marker=marker, max_keys=1000)
            if not result.object_list:
                break

            for obj in result.object_list:
                scanned += 1
                # 新管理目录保留处理中和历史版本，不能因平台改写地址而当作孤儿删除。
                if obj.key.startswith(prefix + 'managed/'):
                    continue
                url = _build_url(obj.key)

                if url not in referenced_urls:
                    orphan += 1
                    if not dry_run:
                        del_result = delete_object(obj.key)
                        if del_result["success"]:
                            deleted += 1
                        else:
                            errors.append({"key": obj.key, "error": del_result["error"]})
                    # 避免请求过快
                    time.sleep(0.05)

            if not result.is_truncated:
                break
            marker = result.next_marker

        return {
            "success": True,
            "dry_run": dry_run,
            "scanned": scanned,
            "orphan": orphan,
            "deleted": deleted,
            "errors": errors
        }

    except Exception as e:
        print(f"[OSS Cleanup] 清理异常: {e}")
        return {
            "success": False,
            "dry_run": dry_run,
            "scanned": scanned,
            "orphan": orphan,
            "deleted": deleted,
            "errors": errors + [str(e)]
        }
