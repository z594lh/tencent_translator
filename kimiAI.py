import os
import base64
from dotenv import load_dotenv
from openai import OpenAI


def get_kimi_client():
    """
    初始化 Kimi (Moonshot) 客户端

    返回:
        OpenAI: 配置好的 OpenAI 客户端实例（用于调用 Moonshot API）
    """
    load_dotenv(override=True)

    api_key = os.environ.get("MOONSHOT_API_KEY")
    if not api_key:
        raise ValueError("未找到 MOONSHOT_API_KEY 环境变量，请在 .env 文件中设置")

    return OpenAI(
        api_key=api_key,
        base_url="https://api.moonshot.cn/v1",
    )


def encode_image_to_base64(image_path: str) -> str:
    """
    将图片文件编码为 base64 格式的 data URL

    参数:
        image_path (str): 图片文件的本地路径

    返回:
        str: base64 编码的 data URL
    """
    with open(image_path, "rb") as f:
        image_data = f.read()

    # 获取文件扩展名并去掉点号
    ext = os.path.splitext(image_path)[1].lower().replace(".", "")
    # 默认使用 jpeg 如果扩展名不存在
    if not ext:
        ext = "jpeg"

    return f"data:image/{ext};base64,{base64.b64encode(image_data).decode('utf-8')}"


def understand_image(
    image_input: str,
    prompt: str = "请详细描述这张图片的内容。",
    model: str = "kimi-k2.5"
) -> str:
    """
    使用 Kimi K2.5 多模态模型理解图片内容

    参数:
        image_input (str): 图片路径（本地文件路径）或 base64 编码的图片数据
        prompt (str): 询问图片的问题或指令，默认是描述图片内容
        model (str): 使用的模型名称，默认是 kimi-k2.5

    返回:
        str: Kimi 对图片的理解/回答
    """
    load_dotenv(override=True)

    try:
        client = get_kimi_client()

        # 判断输入是文件路径还是 base64 数据
        if os.path.isfile(image_input):
            image_url = encode_image_to_base64(image_input)
        elif image_input.startswith("data:image"):
            # 已经是 base64 data URL 格式
            image_url = image_input
        else:
            # 尝试作为 base64 字符串处理，包装成 data URL
            image_url = f"data:image/jpeg;base64,{image_input}"

        completion = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "你是 Kimi，一个强大的多模态AI助手。你能够理解图片内容并提供详细的分析和描述。"
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                            },
                        },
                        {
                            "type": "text",
                            "text": prompt,
                        },
                    ],
                },
            ],
        )

        return completion.choices[0].message.content

    except Exception as e:
        return f"发生错误：{e}"


def understand_image_with_history(
    image_input: str,
    prompt: str,
    history: list = None,
    model: str = "kimi-k2.5"
) -> dict:
    """
    使用 Kimi K2.5 进行多轮对话式的图片理解

    参数:
        image_input (str): 图片路径或 base64 编码的图片数据
        prompt (str): 当前轮次的问题或指令
        history (list): 历史对话记录，格式为 OpenAI 消息格式列表
        model (str): 使用的模型名称

    返回:
        dict: 包含回答内容和更新后的历史记录
    """
    load_dotenv(override=True)

    if history is None:
        history = []

    try:
        client = get_kimi_client()

        # 判断输入是文件路径还是 base64 数据
        if os.path.isfile(image_input):
            image_url = encode_image_to_base64(image_input)
        elif image_input.startswith("data:image"):
            image_url = image_input
        else:
            image_url = f"data:image/jpeg;base64,{image_input}"

        # 构建当前用户消息
        current_message = {
            "role": "user",
            "content": [
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_url,
                    },
                },
                {
                    "type": "text",
                    "text": prompt,
                },
            ],
        }

        # 如果没有历史记录，添加系统消息
        messages = []
        if not history:
            messages.append({
                "role": "system",
                "content": "你是 Kimi，一个强大的多模态AI助手。你能够理解图片内容并提供详细的分析和描述。"
            })

        messages.extend(history)
        messages.append(current_message)

        completion = client.chat.completions.create(
            model=model,
            messages=messages,
        )

        response_content = completion.choices[0].message.content

        # 更新历史记录
        new_history = history.copy()
        new_history.append(current_message)
        new_history.append({
            "role": "assistant",
            "content": response_content
        })

        return {
            "response": response_content,
            "history": new_history,
            "status": "success"
        }

    except Exception as e:
        return {
            "response": f"发生错误：{e}",
            "history": history,
            "status": "error",
            "error": str(e)
        }


def enhance_prompt_text(prompt: str, model: str = "kimi-k2.5") -> str:
    """
    方法1：将简单的文本提示词转换为专业的 AI 生图提示词

    参数:
        prompt (str): 用户输入的简单提示词，如"一只猫在草地上"
        model (str): 使用的模型名称

    返回:
        str: 专业优化后的提示词（失败返回原提示词）
    """
    system_prompt = """You are an expert AI image prompt engineer specialized in creating professional prompts for image generation models (Midjourney, Stable Diffusion, DALL-E, Gemini).

Your task is to transform simple descriptions into highly detailed, professional prompts.

Rules:
1. Expand simple descriptions into rich, detailed scenes with specific elements
2. Add: lighting conditions, camera angles, atmosphere, textures, color palette
3. Include quality modifiers: "masterpiece", "best quality", "highly detailed", "8k", "professional"
4. Use vivid, descriptive adjectives and artistic terminology
5. Structure: [Main Subject], [Details], [Environment], [Lighting/Atmosphere], [Style], [Quality]
6. Output ONLY the optimized English prompt, no explanations or markdown
7. Keep under 150 words but information-dense

Example:
Input: "一只猫在草地上"
Output: "A fluffy orange tabby cat lounging on vibrant green grass, golden hour sunlight casting warm shadows, shallow depth of field, professional photography, highly detailed fur texture, bokeh background, masterpiece, best quality, 8k, DSLR camera, soft natural lighting, peaceful meadow atmosphere"""

    load_dotenv(override=True)

    try:
        client = get_kimi_client()

        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Enhance this prompt: {prompt}"}
            ],
        )

        return completion.choices[0].message.content.strip()

    except Exception as e:
        print(f"提示词增强失败: {e}")
        return prompt  # 失败返回原提示词


def enhance_prompt_with_image(prompt: str, image_input: str, model: str = "kimi-k2.5") -> str:
    """
    方法2：结合图片和提示词，生成专业的 AI 生图提示词

    参数:
        prompt (str): 用户输入的提示词/修改需求，如"让这只猫变成赛博朋克风格"
        image_input (str): 图片路径（本地文件路径）或 base64 编码的图片数据
        model (str): 使用的模型名称

    返回:
        str: 结合图片内容生成的专业提示词（失败返回原提示词）
    """
    system_prompt = """You are an expert AI image prompt engineer. Analyze the provided image and user's modification request, then create a professional prompt for AI image generation.

Your task:
1. Analyze the image: identify subject, composition, colors, style, lighting, and key elements
2. Understand user's modification intent from their prompt
3. Generate a detailed, professional prompt that:
   - Preserves important elements from the original image that user wants to keep
   - Applies the requested modifications precisely
   - Adds professional details: lighting, camera settings, textures, atmosphere
   - Includes quality modifiers: "masterpiece", "best quality", "highly detailed"

Rules:
1. Output ONLY the optimized English prompt, no explanations
2. Be specific about what to change and what to preserve
3. Keep under 200 words but information-dense
4. Use professional artistic terminology"""

    load_dotenv(override=True)

    try:
        client = get_kimi_client()

        # 处理图片输入
        if os.path.isfile(image_input):
            image_url = encode_image_to_base64(image_input)
        elif image_input.startswith("data:image"):
            image_url = image_input
        else:
            image_url = f"data:image/jpeg;base64,{image_input}"

        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": image_url},
                        },
                        {
                            "type": "text",
                            "text": f"Analyze this image and create a professional prompt based on this request: {prompt}",
                        },
                    ],
                },
            ],
        )

        return completion.choices[0].message.content.strip()

    except Exception as e:
        print(f"图文提示词增强失败: {e}")
        return prompt  # 失败返回原提示词


if __name__ == "__main__":
    # 测试代码
    print("\n" + "="*50)
    print("  Kimi K2.5 图片理解测试")
    print("="*50)

    test_image = "static/output/1c58ad7e-3041-4ee0-b11d-aa9d8bbfcb8b.jpg"  # 请替换为实际图片路径

    # 测试 3: 方法1 - 纯文本提示词增强
    print("\n" + "="*50)
    print("[测试 3] 方法1: 纯文本提示词增强")
    print("="*50)

    test_prompts = [
        "一只猫在草地上",
        "赛博朋克风格的城市夜景",
        "古装美女在樱花树下"
    ]

    for prompt in test_prompts:
        print(f"\n📝 原始: {prompt}")
        enhanced = enhance_prompt_text(prompt)
        print(f"✨ 增强: {enhanced[:150]}...")
        print("-" * 40)

