import os
import uuid
import io
import base64
import json
import tempfile
import requests
import pymysql
from typing import List, Optional
from dotenv import load_dotenv
from volcenginesdkarkruntime import Ark


# ============== 配置和初始化 ==============

def get_doubao_client():
    """初始化豆包 (火山引擎 Ark) 客户端"""
    load_dotenv(override=True)

    api_key = os.environ.get("ARK_API_KEY")
    if not api_key:
        raise ValueError("未找到 ARK_API_KEY 环境变量，请在 .env 文件中设置")

    return Ark(
        base_url="https://ark.cn-beijing.volces.com/api/v3",
        api_key=api_key,
    )


# ============== 图片处理工具 ==============

def save_image_from_url(image_url: str, output_dir: str = "static/output") -> tuple:
    """从URL下载图片并保存到本地，返回 (本地URL, 本地文件路径)"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()

        filename = f"{uuid.uuid4()}.png"
        local_path = os.path.join(output_dir, filename)

        with open(local_path, "wb") as f:
            f.write(response.content)

        base_url = os.getenv("BASE_URL", "http://127.0.0.1:5000")
        url = f"{base_url.rstrip('/')}/static/output/{filename}"

        return url, local_path.replace("\\\\", "/")

    except Exception as e:
        print(f"❌ 保存图片失败: {e}")
        return None, None


def save_image_to_temp(image_b64_or_url: str) -> str:
    """将图片(base64或URL)保存为临时文件，返回本地路径"""
    temp_dir = os.path.join(tempfile.gettempdir(), "doubao_uploads")
    os.makedirs(temp_dir, exist_ok=True)

    temp_file = os.path.join(temp_dir, f"{uuid.uuid4()}.png")

    try:
        if image_b64_or_url.startswith("http"):
            response = requests.get(image_b64_or_url, timeout=30)
            response.raise_for_status()
            with open(temp_file, "wb") as f:
                f.write(response.content)
        elif image_b64_or_url.startswith("data:image"):
            base64_data = image_b64_or_url.split(",")[1]
            image_bytes = base64.b64decode(base64_data)
            with open(temp_file, "wb") as f:
                f.write(image_bytes)
        else:
            image_bytes = base64.b64decode(image_b64_or_url)
            with open(temp_file, "wb") as f:
                f.write(image_bytes)

        return temp_file
    except Exception as e:
        print(f"❌ 保存临时图片失败: {e}")
        return None


def upload_image_to_doubao(image_path: str) -> str:
    """使用 REST API 上传图片到豆包，获取URL"""
    try:
        load_dotenv(override=True)
        api_key = os.environ.get("ARK_API_KEY")
        if not api_key:
            raise ValueError("未找到 ARK_API_KEY")

        url = "https://ark.cn-beijing.volces.com/api/v3/files"
        headers = {
            "Authorization": f"Bearer {api_key}"
        }

        with open(image_path, "rb") as f:
            files = {
                "file": (os.path.basename(image_path), f, "image/png"),
                "purpose": (None, "user_data")  # 必须用 user_data
            }
            response = requests.post(url, headers=headers, files=files, timeout=60)

        if response.status_code == 200:
            result = response.json()
            # 提取上传后的文件URL
            if "url" in result:
                return result["url"]
            elif "data" in result and "url" in result["data"]:
                return result["data"]["url"]
            else:
                print(f"⚠️ 上传成功但未找到URL: {result}")
                return None
        else:
            print(f"❌ 上传失败: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"❌ 上传图片到豆包失败: {e}")
        return None


def get_image_url_from_input(img_data: str, temp_files: list) -> Optional[str]:
    """处理各种图片输入格式，返回可上传的URL"""
    if not img_data:
        return None

    if img_data.startswith("http"):
        return img_data

    if img_data.startswith("data:image") or len(img_data) > 1000:
        temp_path = save_image_to_temp(img_data)
        if temp_path:
            temp_files.append(temp_path)
            image_url = upload_image_to_doubao(temp_path)
            return image_url

    return None


def to_doubao_base64(image_input):
    """
    万能转换函数：
    支持 3 种输入 → 统一输出 豆包要求的 base64 格式
    格式：data:image/png;base64,xxxxxx

    输入支持：
    1. 本地图片路径（如：test.png）
    2. 纯 base64 字符串（无前缀）
    3. 带前缀的 base64（如 data:image/png;base64,xxx）
    """
    # ----------------------
    # 情况1：输入是本地文件路径
    # ----------------------
    if os.path.exists(image_input):
        ext = os.path.splitext(image_input)[-1].lower().replace(".", "")
        # 兼容 jpeg 格式
        if ext == "jpeg":
            ext = "jpg"
        with open(image_input, "rb") as f:
            base64_data = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/{ext};base64,{base64_data}"

    # ----------------------
    # 情况2：已经是带前缀的 base64
    # ----------------------
    if image_input.startswith("data:image/"):
        return image_input

    # ----------------------
    # 情况3：纯 base64（无前缀）
    # ----------------------
    return f"data:image/png;base64,{image_input}"

# ============== 核心服务：统一生图接口 ==============

def edit_ai_images_service(
    message: str,
    session_id: str = None,
    image_ids: list = None,
    image_b64_list: list = None,
    count: int = 1,
    model_name: str = "doubao-seedream-5-0-260128",
    aspect_ratio: str = "1:1",
    quality: str = "2K",
    **kwargs
) -> dict:
    """
    豆包AI统一生图服务 - 根据参数自动判断生图模式

    支持模式:
    1. 文生单图: 无图片, count=1
    2. 文生多图: 无图片, count>1 (使用sequential_image_generation)
    3. 图生图(单图): 有图片, count=1
    4. 单图生多图: 有1张图片, count>1 (使用sequential_image_generation)
    5. 多图生多图: 有多张图片, count>1 (使用sequential_image_generation)
    """
    load_dotenv(override=True)
    client = get_doubao_client()

    input_images = []
    temp_files = []
    mode = "unknown"

    try:
        # ===== 1. 收集所有输入图片 =====
        if image_ids:
            from geminiAI import get_image_relative_path_by_id
            for img_id in image_ids:
                local_path = get_image_relative_path_by_id(img_id)
                img_data_db = to_doubao_base64(local_path)
                # print(f": {img_data}")
                input_images.append(img_data_db)

        if image_b64_list:
            for img_data in image_b64_list:
                img_data_db = to_doubao_base64(img_data)
                input_images.append(img_data_db)

        # ===== 2. 判断生图模式 =====
        has_images = len(input_images) > 0
        is_multi_output = count > 1
        is_multi_input = len(input_images) > 1

        if not has_images and not is_multi_output:
            mode = "text2image_single"
        elif not has_images and is_multi_output:
            mode = "text2image_multi"
        elif has_images and not is_multi_output and not is_multi_input:
            mode = "image2image"
        elif has_images and is_multi_output and not is_multi_input:
            mode = "single2multi"
        elif has_images and is_multi_output and is_multi_input:
            mode = "multi2multi"
        elif has_images and is_multi_input and not is_multi_output:
            mode = "multi2single"

        print(f"🎨 生图模式: {mode} | 输入图片: {len(input_images)}张 | 输出: {count}张")

        # ===== 3. 构建请求参数 =====
        size_map = {"512": "512", "1K": "1K", "2K": "2K", "4K": "4K"}
        size = size_map.get(quality, quality)

        # 多图生成时，在提示词中添加数量说明
        final_prompt = message
        if is_multi_output:
            if f"生成{count}张" not in message and f"生成 {count} 张" not in message:
                final_prompt = f"生成{count}张图：{message}"

        request_params = {
            "model": model_name,
            "prompt": final_prompt,
            "size": size,
            "output_format": kwargs.get('output_format', 'png'),
            "response_format": "url",
            "watermark": kwargs.get('watermark', False)
        }

        # 添加图片参数
        if input_images:
            if len(input_images) == 1:
                request_params["image"] = input_images[0]
            else:
                request_params["image"] = input_images

        # 组图模式配置
        if is_multi_output:
            request_params["sequential_image_generation"] = "auto"
            try:
                from volcenginesdkarkruntime.types.images.images import SequentialImageGenerationOptions
                request_params["sequential_image_generation_options"] = SequentialImageGenerationOptions(max_images=count)
                print(f"🎨 启用组图模式，计划生成 {count} 张图片")
            except ImportError:
                print("⚠️ 无法导入 SequentialImageGenerationOptions")
                request_params["sequential_image_generation_options"] = {"max_images": count}

        final_session_id = session_id or str(uuid.uuid4())[:8]

        # ===== 4. 调用豆包API =====
        print(f"🚀 调用豆包API: model={model_name}, size={size}")
        response = client.images.generate(**request_params)
        print(f"DEBUG: Response -> {response}")

        # ===== 5. 处理返回结果 =====
        result_data = []

        if response.data:
            print(f"📦 返回 {len(response.data)} 张图片")
            for i, image in enumerate(response.data):
                new_img_id = str(uuid.uuid4())
                image_info = {
                    "image_id": new_img_id,
                    "prompt": message,
                    "model": model_name,
                    "quality": quality,
                    "mode": mode,
                    "index": i + 1
                }

                image_url = image.url if hasattr(image, 'url') else None

                if image_url:
                    local_url, local_path = save_image_from_url(image_url)
                    if local_url:
                        image_info["url"] = local_url
                        image_info["local_path"] = local_path
                    else:
                        image_info["url"] = image_url

                if hasattr(image, 'size') and image.size:
                    image_info["size"] = image.size

                if hasattr(image, 'revised_prompt') and image.revised_prompt:
                    image_info["revised_prompt"] = image.revised_prompt

                result_data.append(image_info)
                print(f"✅ 图片 {i+1} 生成成功: {image_info.get('url', 'N/A')[:60]}...")

                # 保存到数据库
                if image_info.get("local_path"):
                    history_record = {
                        "prompt": message,
                        "model": model_name,
                        "mode": mode,
                        "quality": quality,
                        "index": i + 1
                    }
                    save_image_to_db(
                        image_id=new_img_id,
                        session_id=final_session_id,
                        url=image_info["url"],
                        local_path=image_info["local_path"],
                        prompt=message,
                        history=history_record,
                        model=model_name
                    )

        return {
            "images": [item.get("url") for item in result_data if item.get("url")],
            "image_details": result_data,
            "session_id": final_session_id,
            "status": "success" if result_data else "error",
            "ai_text": "",
            "mode": mode,
            "input_count": len(input_images),
            "output_count": len(result_data)
        }

    except Exception as e:
        print(f"❌ 豆包生图错误: {e}")
        import traceback
        traceback.print_exc()
        return {
            "error": str(e),
            "status": "error",
            "mode": mode if 'mode' in locals() else "unknown"
        }

    finally:
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except:
                pass


# ============== 数据库功能 ==============

def get_db_connection():
    load_dotenv(override=True)
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""), 
        database=os.getenv("DB_NAME", "ai_image_project"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def ensure_session_exists(cursor, session_id):
    """确保父表 ai_sessions 中存在该 ID"""
    check_sql = "SELECT id FROM ai_sessions WHERE id = %s"
    cursor.execute(check_sql, (session_id,))
    if not cursor.fetchone():
        insert_session_sql = "INSERT INTO ai_sessions (id) VALUES (%s)"
        cursor.execute(insert_session_sql, (session_id,))


def save_image_to_db(image_id, session_id, url, local_path, prompt, history, model="doubao-seedream-5-0-260128"):
    """保存图片信息到数据库"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            ensure_session_exists(cursor, session_id)

            history_data = []
            if isinstance(history, list):
                for h in history:
                    if hasattr(h, 'to_json'):
                        history_data.append(json.loads(h.to_json()))
                    elif isinstance(h, dict):
                        history_data.append(h)
                    else:
                        try:
                            history_data.append(json.loads(json.dumps(h, default=lambda o: o.__dict__)))
                        except:
                            continue
            elif isinstance(history, dict):
                history_data = [history]

            history_json = json.dumps(history_data)

            sql = """INSERT INTO ai_images (id, session_id, image_url, local_path, prompt, history_snapshot)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (image_id, session_id, url, local_path, prompt, history_json))
        conn.commit()
        print(f"📖 数据库记录已同步: Image ID {image_id}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()


def get_image_relative_path_by_id(image_id):
    """根据 ID 从数据库获取图片的本地存储相对路径"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT local_path FROM ai_images WHERE id = %s"
            cursor.execute(sql, (image_id,))
            result = cursor.fetchone()
            if result:
                return result['local_path']
            return None
    finally:
        conn.close()


# ============== 快速调用函数 ==============

def quick_generate(prompt: str, image_url: str = None, count: int = 1, size: str = "2K") -> Optional[str]:
    """快速生成图片，返回URL"""
    image_b64_list = [image_url] if image_url else None

    result = edit_ai_images_service(
        message=prompt,
        image_b64_list=image_b64_list,
        count=count,
        quality=size
    )

    if result.get("status") == "success" and result.get("images"):
        return result["images"][0]
    return None


# ============== 测试代码 ==============

if __name__ == "__main__":
    print("\n" + "="*60)
    print("  豆包 (Seedream) AI 统一生图服务测试")
    print("="*60)

    # 测试1: 文生单图
    print("\n[测试1] 文生单图 (text2image_single)")
    result = edit_ai_images_service(
        message="一只可爱的柴犬在樱花树下微笑",
        count=1,
        quality="2K"
    )
    print(f"模式: {result.get('mode')}, 状态: {result['status']}, 生成: {result.get('output_count')}张")
    if result.get('images'):
        print(f"URL: {result['images'][0][:60]}...")

    # 测试2: 文生多图（组图模式）
    print("\n[测试2] 文生多图 (text2image_multi)")
    result = edit_ai_images_service(
        message="赛博朋克风格的城市夜景，霓虹灯光",
        count=3,
        quality="2K"
    )
    print(f"模式: {result.get('mode')}, 状态: {result['status']}, 生成: {result.get('output_count')}张")

    print("\n" + "="*60)
    print("测试完成！")
    print("="*60)
