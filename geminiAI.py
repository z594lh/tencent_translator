import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
import base64

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
    """从 .env 文件中读取代理配置，并设置到 os.environ"""
    proxy_http = os.getenv("HTTP_PROXY")
    proxy_https = os.getenv("HTTPS_PROXY")

    if proxy_http:
        os.environ['http_proxy'] = proxy_http
    if proxy_https:
        os.environ['https_proxy'] = proxy_https


def generate_ai_response(contents: str) -> str:
    """
    使用 Google GenAI 生成 AI 回答
    
    参数:
        contents (str): 用户输入的内容（可以是中文）

    返回:
        str: 模型生成的回答文本
    """
    # 加载环境变量（先加载，防止 Client 初始化时找不到 API KEY）
    load_dotenv()

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
    load_dotenv()

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




if __name__ == "__main__":
    user_input = "翻译成中文：After arranging shipment (v2.logistics.ship_order) for the integrated channel, use this api to get the tracking_number, which is a required parameter for creating shipping labels. The api response can return tracking_number empty, since this info is dependent from the 3PL, due to this it is allowed to keep calling the api within 5 minutes interval, until the tracking_number is returned."
    result = generate_ai_response(user_input)
    print("AI 回答：", result)