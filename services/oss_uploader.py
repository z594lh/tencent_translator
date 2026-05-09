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
from datetime import datetime

import oss2


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
    return f"https://{ALI_BUCKET_NAME}.{endpoint}/{oss_key}"


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


def upload_image_for_listing(file_obj, filename: str = None) -> dict:
    """
    专用于亚马逊 Listing 图片上传的快捷方法
    自动添加 amazon/listing/images/ 前缀
    """
    if filename is None and hasattr(file_obj, "filename"):
        filename = file_obj.filename
    if not filename:
        filename = "image.jpg"
    oss_key = generate_oss_key(filename, prefix="amazon/listing/images/")
    return upload_stream(file_obj, oss_key=oss_key, filename=filename)


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
