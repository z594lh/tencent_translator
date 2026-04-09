import os
import uuid
from dotenv import load_dotenv
from google import genai
from google.genai import types
import base64
from flask import request


# 内存会话池：{ session_id: [history_list] }
SESSIONS_POOL = {}
# 限制最大存储量，防止内存占用过高
MAX_SESSIONS = 50



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

def save_image_locally(image_bytes):
    output_dir = os.path.join('static', 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filename = f"{uuid.uuid4()}.jpg"
    file_path = os.path.join(output_dir, filename)
    
    with open(file_path, "wb") as f:
        f.write(image_bytes)
    
    # --- 拼接完整 URL ---
    try:
        base_url = os.getenv("BASE_URL") 
        return f"{base_url}/static/output/{filename}"
    except Exception:
        return f"/static/output/{filename}"

def generate_ai_images_service(
    message: str, 
    session_id: str = None, 
    image_b64: str = None, 
    count: int = 1, 
    model_name: str = "gemini-3.1-flash-image-preview",
    aspect_ratio: str = "1:1",
    quality: str = "720"
) -> dict:
    load_dotenv(override=True)
    setup_proxy_from_env()
    client = genai.Client()
    
    # 1. 查找或创建会话历史
    if session_id and session_id in SESSIONS_POOL:
        current_history = SESSIONS_POOL[session_id]
    else:
        current_history = []
        session_id = str(uuid.uuid4())[:8]

    # --- 2. 提示词增强逻辑 (Prompt Engineering) ---
    # 映射前端传来的 720, 1080, 1440 参数
    quality_map = {
        "720": "标准高清 (720p resolution, clear details)",
        "1080": "全高清 (1080p Full HD, sharp textures, high-quality rendering)",
        "1440": "超清 (2K/1440p resolution, ultra-detailed, cinematic lighting, masterpiece)"
    }
    
    # 构造技术指令块
    tech_requirements = (
        f"\n\n---\n"
        f"[技术规格/Technical Requirements]:\n"
        f"- 图像比例 (Aspect Ratio): {aspect_ratio}\n"
        f"- 图像质量 (Quality): {quality_map.get(str(quality), 'Standard')}\n"
        f"- 请严格按照此比例和质量要求生成图像。"
    )
    
    # 拼接最终发送给 AI 的 Prompt
    final_message = f"{message}{tech_requirements}"
    # --------------------------------------------

    result_urls = []

    try:
        # 构造当前输入的 Parts
        current_parts = []
        if image_b64:
            img_bytes = base64.b64decode(image_b64)
            current_parts.append(types.Part.from_bytes(data=img_bytes, mime_type='image/jpeg'))
        
        # 使用增强后的 final_message
        current_parts.append(types.Part.from_text(text=final_message))

        # 循环生成指定数量的图片
        for i in range(count):
            chat = client.chats.create(
                model=model_name,
                history=current_history,
                config=types.GenerateContentConfig(response_modalities=['TEXT', 'IMAGE'])
            )
            response = chat.send_message(current_parts)
            
            # 只有第一张图的交互记录到历史中，避免历史记录过于臃肿
            if i == 0:
                user_content = types.Content(role="user", parts=current_parts)
                model_content = response.candidates[0].content
                current_history.append(user_content)
                current_history.append(model_content)

            # 提取生成的图片数据
            for part in response.candidates[0].content.parts:
                if part.inline_data:
                    url = save_image_locally(part.inline_data.data)
                    result_urls.append(url)

        # 回存到内存池
        if len(SESSIONS_POOL) >= MAX_SESSIONS:
            first_key = next(iter(SESSIONS_POOL))
            SESSIONS_POOL.pop(first_key)
        
        SESSIONS_POOL[session_id] = current_history

        return {
            "images": result_urls,
            "session_id": session_id,
            "status": "success"
        }

    except Exception as e:
        print(f"Service Error: {str(e)}") # 打印日志方便调试
        return {"error": str(e)}


if __name__ == "__main__":
    # 模拟一个一直运行的后端
    while True:
        print("\n--- AI 生图系统 (模拟后端运行) ---")
        u_input = input("请输入指令 (输入 'q' 退出): ")
        if u_input == 'q': break
        
        s_id = input("请输入 Session ID (首次生成请直接回车): ")
        if s_id.strip() == "": s_id = None

        # 调用函数
        result = generate_ai_images_service(message=u_input, session_id=s_id)
        
        if "error" in result:
            print("失败:", result["error"])
        else:
            print("成功！")
            print(f"Session ID: {result['session_id']}")
            print(f"图片路径: {result['images']}")