import os
import uuid
import json
import io
import pymysql
from dotenv import load_dotenv
from google import genai
from google.genai import types
import base64
from flask import request


def get_translation_prompt(target='zh'):
    # 语言映射表
    language_map = {
        'zh': '中文:',
        'en': 'English:',
        'ja': '日本語:',
        'ko': '한국어:'
    }

    # 获取对应的语言名称
    language_name = language_map.get(target, '中文')

    # 根据目标语言生成不同的提示语
    if target == 'zh':
        return f"请帮我翻译成{language_name}"
    elif target == 'en':
        return f"Please help me translate it into {language_name}"
    elif target == 'ja':
        return f"{language_name}に翻訳してください"
    elif target == 'ko':
        return f"{language_name}로 번역해 주세요"
    else:
        return "Please help me translate it into a supported language"
    

def setup_proxy_from_env():
    """从 .env 文件中读取代理配置，有则设置，无则清除"""
    proxy_http = os.getenv("HTTP_PROXY")
    proxy_https = os.getenv("HTTPS_PROXY")

    if proxy_http:
        os.environ['http_proxy'] = proxy_http
        os.environ['HTTP_PROXY'] = proxy_http
    else:
        # 如果 .env 没写，确保不使用系统可能残留的代理设置
        os.environ.pop('http_proxy', None)
        os.environ.pop('HTTP_PROXY', None)

    if proxy_https:
        os.environ['https_proxy'] = proxy_https
        os.environ['HTTPS_PROXY'] = proxy_https
    else:
        os.environ.pop('https_proxy', None)
        os.environ.pop('HTTPS_PROXY', None)


def generate_ai_response(contents: str) -> str:
    """
    使用 Google GenAI 生成 AI 回答
    
    参数:
        contents (str): 用户输入的内容（可以是中文）

    返回:
        str: 模型生成的回答文本
    """
    # 加载环境变量（先加载，防止 Client 初始化时找不到 API KEY）
    load_dotenv(override=True)

    # 设置代理
    setup_proxy_from_env()

    # 初始化客户端
    client = genai.Client()

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=contents
        )
        return response.text
    except Exception as e:
        return f"发生错误：{e}"

def generate_ai_img_response(image_base64: str, prompt_text: str) -> str:
    """
    使用指定的图片和提示文本，通过 AI 模型生成相应的回答文本。
    
    参数:
        image_base64 (str): 用户输入的图片base64编码字符串。
        prompt_text (str): 提供给AI模型的提示文本，用于描述或请求特定的输出。
    
    返回:
        str: 根据提供的图片和提示文本，由AI模型生成的回答文本。
    """
    # 加载环境变量（先加载，防止 Client 初始化时找不到 API KEY）
    load_dotenv(override=True)

    # 设置代理
    setup_proxy_from_env()

    # 初始化客户端
    client = genai.Client()
    image_bytes = base64.b64decode(image_base64)

    try:
        # 调用模型
        response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=[
                    types.Part.from_bytes(
                        data=image_bytes,
                        mime_type='image/jpeg',  # 确保和图片实际类型一致，如 image/png
                    ),
                    types.Part.from_text(text=prompt_text)  # 添加你的文本提示
                ]
            )

        return response.text
    except Exception as e:
        return f"发生错误：{e}"
    
# --- AI生图部分 ---


def save_image_locally(image_bytes):
    """返回 (完整URL, 本地相对路径)"""
    output_dir = os.path.join('static', 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filename = f"{uuid.uuid4()}.jpg"
    relative_path = os.path.join(output_dir, filename) # static/output/xxx.jpg
    
    with open(relative_path, "wb") as f:
        f.write(image_bytes)
    
    # 构造 URL
    try:
        base_url = os.getenv("BASE_URL") 
        url = f"{base_url.rstrip('/')}/static/output/{filename}"
    except:
        url = f"/static/output/{filename}"
        
    return url, relative_path.replace("\\", "/")
    

def get_db_connection():
    load_dotenv(override=True)
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "remote_user"),
        password=os.getenv("DB_PASSWORD", "你的密码"), 
        database=os.getenv("DB_NAME", "ai_image_project"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def save_image_to_db(image_id, url, local_path, prompt, history, user_id=None):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 序列化历史记录 (保持你原来的强化版逻辑)
            history_data = []
            for h in history:
                if hasattr(h, 'to_json'):
                    history_data.append(json.loads(h.to_json()))
                elif isinstance(h, dict):
                    history_data.append(h)
                else:
                    try:
                        history_data.append(json.loads(json.dumps(h, default=lambda o: o.__dict__)))
                    except: continue

            history_json = json.dumps(history_data)

            # 增加 user_id 和 local_path 字段的插入
            sql = """INSERT INTO ai_images (id, user_id, image_url, local_path, prompt, history_snapshot)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (image_id, user_id, url, local_path, prompt, history_json))
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


def edit_ai_images_service(
    message: str,
    image_ids: list = None,
    image_b64_list: list = None,
    count: int = 1,
    model_name: str = "gemini-3.1-flash-image-preview",
    aspect_ratio: str = "1:1",
    quality: str = "2K",
    user_id: int = None
) -> dict:
    """
    使用 Gemini 模型基于多张输入图片进行编辑/修改，生成新图片。
    支持从数据库加载图片 (image_ids) 或直接传入 base64 图片 (image_b64_list)。

    参数:
        message (str): 提示词，描述如何修改/组合图片
        image_ids (list): 数据库中的图片ID列表，如 ["img-001", "img-002"]
        image_b64_list (list): base64编码的图片列表，如 ["base64str1", "base64str2"]
        count (int): 生成图片数量
        model_name (str): 使用的模型名称
        aspect_ratio (str): 宽高比，可选 "1:1","1:4","1:8","2:3","3:2","3:4","4:1","4:3","4:5","5:4","8:1","9:16","16:9","21:9"
        quality (str): 分辨率，可选 "512", "1K", "2K", "4K"

    返回:
        dict: 包含生成的图片URL等信息
    """
    load_dotenv(override=True)
    setup_proxy_from_env()
    client = genai.Client()

    # --- 2. 收集所有输入图片作为 Part ---
    image_parts = []

    # a. 优先从数据库加载图片 (image_ids)
    if image_ids:
        for img_id in image_ids:
            local_path = get_image_relative_path_by_id(img_id)
            if local_path and os.path.exists(local_path):
                try:
                    with open(local_path, "rb") as f:
                        image_parts.append(types.Part.from_bytes(
                            data=f.read(),
                            mime_type='image/jpeg'
                        ))
                    print(f"✅ 已加载历史图片: {local_path}")
                except Exception as e:
                    print(f"⚠️ 加载图片 {img_id} 失败: {e}")
            else:
                print(f"⚠️ 图片 {img_id} 不存在: {local_path}")

    # b. 其次处理直接传入的 base64 图片
    if image_b64_list:
        for b64_str in image_b64_list:
            try:
                img_bytes = base64.b64decode(b64_str)
                image_parts.append(types.Part.from_bytes(
                    data=img_bytes,
                    mime_type='image/jpeg'
                ))
                print(f"✅ 已加载 base64 图片")
            except Exception as e:
                print(f"⚠️ 解码 base64 图片失败: {e}")

    if not image_parts:
        return {"error": "没有可用的输入图片，请提供 image_ids 或 image_b64_list"}

    # --- 3. 构造配置 ---
    generation_config = types.GenerateContentConfig(
        response_modalities=['TEXT', 'IMAGE'],
        image_config=types.ImageConfig(
            aspect_ratio=aspect_ratio,
            image_size=quality
        )
    )

    result_data = []

    try:
        # --- 4. 构造请求内容 ---
        # 所有图片 + 提示词
        contents = image_parts + [types.Part.from_text(text=message)]

        # --- 5. 循环生成 ---
        for i in range(count):
            # 使用 client.models.generate_content 直接调用（非多轮对话模式）
            response = client.models.generate_content(
                model=model_name,
                contents=contents,
                config=generation_config
            )

            # --- 6. 构建历史记录（用于二次修改时参考）---
            history_record = {
                "prompt": message,
                "model_version": response.model_version if hasattr(response, 'model_version') else model_name,
                "generation_config": {
                    "aspect_ratio": aspect_ratio,
                    "quality": quality
                },
                "input_images_count": len(image_parts),
                "ai_response": {
                    "text": None,
                    "finish_reason": str(response.candidates[0].finish_reason) if response.candidates else None,
                },
                "usage_metadata": {
                    "prompt_token_count": response.usage_metadata.prompt_token_count if response.usage_metadata else 0,
                    "candidates_token_count": response.usage_metadata.candidates_token_count if response.usage_metadata else 0,
                    "total_token_count": response.usage_metadata.total_token_count if response.usage_metadata else 0,
                } if response.usage_metadata else None
            }

            # --- 7. 处理生成的图片并保存 ---
            image_found = False
            print(f"DEBUG: Response -> {response}")
            for part in response.parts:
                if part.text is not None:
                    print(part.text)
                    history_record["ai_response"]["text"] = part.text
                elif part.inline_data is not None:
                    img_data = part.inline_data.data
                    if img_data:
                        new_img_id = str(uuid.uuid4())

                        # 保存图片并获取URL和本地路径
                        url, local_rel_path = save_image_locally(img_data)

                        # 存入数据库（包含历史记录）
                        save_image_to_db(new_img_id, url, local_rel_path, message, [history_record], user_id=user_id)

                        result_data.append({
                            "image_id": new_img_id,
                            "url": url
                        })
                        image_found = True
                elif hasattr(part, 'as_image') and part.as_image():
                    image = part.as_image()
                    # 将 PIL Image 转为 bytes
                    img_bytes = io.BytesIO()
                    image.save(img_bytes, format='PNG')
                    img_data = img_bytes.getvalue()

                    new_img_id = str(uuid.uuid4())

                    # 保存图片并获取URL和本地路径
                    url, local_rel_path = save_image_locally(img_data)

                    # 存入数据库
                    save_image_to_db(new_img_id, url, local_rel_path, message, [], user_id=user_id)

                    result_data.append({
                        "image_id": new_img_id,
                        "url": url
                    })
                    image_found = True

            if not image_found:
                print(f"⚠️ AI 未生图，回复文字: {response.text if hasattr(response, 'text') else '无文字'}")

        return {
            "images": [item['url'] for item in result_data],
            "image_details": result_data,
            "status": "success" if result_data else "no_image",
            "ai_text": response.text if not image_found and hasattr(response, 'text') else ""
        }

    except Exception as e:
        print(f"Service Error: {str(e)}")
        return {"error": str(e)}


def text_to_image_service(
    message: str,
    count: int = 1,
    model_name: str = "gemini-3.1-flash-image-preview",
    aspect_ratio: str = "1:1",
    quality: str = "512",
    user_id: int = None
) -> dict:
    """
    文生图服务 - 使用 Gemini 模型根据提示词生成图片
    """
    load_dotenv(override=True)
    setup_proxy_from_env()
    client = genai.Client()

    # 构造配置
    generation_config = types.GenerateContentConfig(
        response_modalities=['TEXT', 'IMAGE'],
        image_config=types.ImageConfig(
            aspect_ratio=aspect_ratio,
            image_size=quality
        )
    )

    result_data = []

    try:
        # 循环生成
        for i in range(count):
            # 使用 client.models.generate_content 直接调用（文生图模式）
            response = client.models.generate_content(
                model=model_name,
                contents=[types.Part.from_text(text=message)],
                config=generation_config
            )

            # 构建历史记录
            history_record = {
                "prompt": message,
                "model_version": response.model_version if hasattr(response, 'model_version') else model_name,
                "generation_config": {
                    "aspect_ratio": aspect_ratio,
                    "quality": quality
                },
                "ai_response": {
                    "text": None,
                    "finish_reason": str(response.candidates[0].finish_reason) if response.candidates else None,
                },
                "usage_metadata": {
                    "prompt_token_count": response.usage_metadata.prompt_token_count if response.usage_metadata else 0,
                    "candidates_token_count": response.usage_metadata.candidates_token_count if response.usage_metadata else 0,
                    "total_token_count": response.usage_metadata.total_token_count if response.usage_metadata else 0,
                } if response.usage_metadata else None
            }

            # 处理生成的图片并保存
            image_found = False
            print(f"DEBUG: Response -> {response}")
            for part in response.parts:
                if part.text is not None:
                    print(part.text)
                    history_record["ai_response"]["text"] = part.text
                elif part.inline_data is not None:
                    img_data = part.inline_data.data
                    if img_data:
                        new_img_id = str(uuid.uuid4())

                        # 保存图片并获取URL和本地路径
                        url, local_rel_path = save_image_locally(img_data)

                        # 存入数据库
                        save_image_to_db(new_img_id, url, local_rel_path, message, [history_record], user_id=user_id)

                        result_data.append({
                            "image_id": new_img_id,
                            "url": url
                        })
                        image_found = True
                elif hasattr(part, 'as_image') and part.as_image():
                    image = part.as_image()
                    # 将 PIL Image 转为 bytes
                    img_bytes = io.BytesIO()
                    image.save(img_bytes, format='PNG')
                    img_data = img_bytes.getvalue()

                    new_img_id = str(uuid.uuid4())

                    # 保存图片并获取URL和本地路径
                    url, local_rel_path = save_image_locally(img_data)

                    # 存入数据库
                    save_image_to_db(new_img_id, url, local_rel_path, message, [history_record], user_id=user_id)

                    result_data.append({
                        "image_id": new_img_id,
                        "url": url
                    })
                    image_found = True

            if not image_found:
                print(f"⚠️ AI 未生图，回复文字: {response.text if hasattr(response, 'text') else '无文字'}")

        return {
            "images": [item['url'] for item in result_data],
            "image_details": result_data,
            "status": "success" if result_data else "no_image",
            "ai_text": response.text if not result_data else ""
        }

    except Exception as e:
        print(f"Service Error: {str(e)}")
        return {"error": str(e)}


if __name__ == "__main__":
    # 模拟一个一直运行的后端测试环境
    print("\n" + "="*50)
    print("  AI 生图系统数据库版测试启动 (Model: gemini-2.5-flash-image)")
    print("="*50)

    while True:
        print("\n[新任务]")
        u_input = input("请输入描述词 (输入 'q' 退出): ")
        if u_input.lower() == 'q': 
            break
        
        print("\n--- 上下文选择 ---")
        print("1. 全新生成 (直接回车)")
        print("2. 基于某张图进行修改 (输入该图片的 Image ID)")
        target_img_id = input("请输入 Image ID: ").strip()
        if target_img_id == "":
            target_img_id = None

        # 调用重构后的服务函数
        # 注意：这里我们手动指定 model_name 为 gemini-2.5-flash-image
        result = text_to_image_service(
            message=u_input,
            model_name="gemini-2.5-flash-image"
        )

        if "error" in result:
            print("\n❌ 运行失败:", result["error"])
        else:
            print("\n✅ 运行成功！")

            # 打印生成的图片及其 ID
            for item in result.get('image_details', []):
                print(f"---")
                print(f"图片 ID (Image ID): {item['image_id']}")
                print(f"存储路径 (URL): {item['url']}")
                print(f"提示：下次输入此 Image ID 即可针对这张图修改。")