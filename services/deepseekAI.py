import os
from dotenv import load_dotenv
from openai import OpenAI


def get_deepseek_client():
    """初始化 DeepSeek 客户端"""
    load_dotenv(override=True)

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise ValueError("未找到 DEEPSEEK_API_KEY 环境变量，请在 .env 文件中设置")

    return OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com",
    )


SYSTEM_PROMPT = """你是一个专业的跨境电商产品申报信息生成助手。根据用户提供的产品 listing 描述和材质信息，生成以下申报信息：

1. 中文申报名称：准确描述产品类别和关键特征，符合海关申报规范
2. 英文申报名称：精简准确，必须在 35 个字符以内（含空格），符合海关申报规范
3. 中文材质：翻译或规范为中文材质名称
4. 英文材质：翻译为英文材质名称

规则：
- 名称应包含材质、用途等关键信息，便于海关分类
- 材质字段单独输出，如果用户未提供材质信息，则根据描述合理推断
- 只输出 JSON 格式，不要任何其他内容

输出格式示例：
{"name_cn": "塑料制厨房烹饪用具", "name_en": "Plastic Kitchen Cooking Utensil", "material_cn": "塑料", "material_en": "Plastic"}"""


def generate_declaration_info(listing_description: str, material_cn: str = "", model: str = "deepseek-v4-flash") -> dict:
    """
    根据 listing 描述和材质生成产品申报信息（中英文名称 + 中英文材质）

    参数:
        listing_description (str): 产品的 listing 描述文案
        material_cn (str): 中文材质（从 Amazon 属性中提取），可选
        model (str): 使用的模型名称，默认 deepseek-v4-flash

    返回:
        dict: {"name_cn": "", "name_en": "", "material_cn": "", "material_en": ""}
    """
    load_dotenv(override=True)

    user_parts = [f"产品描述：{listing_description}"]
    if material_cn:
        user_parts.append(f"材质：{material_cn}")

    try:
        client = get_deepseek_client()

        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": "\n".join(user_parts)},
            ],
            temperature=0.3,
            stream=False,
        )

        raw = completion.choices[0].message.content.strip()

        # 清理可能的 markdown 代码块标记
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(lines[1:]) if len(lines) > 1 else raw
            if raw.endswith("```"):
                raw = raw[:-3]

        import json
        result = json.loads(raw)
        return {
            "name_cn": result.get("name_cn", ""),
            "name_en": result.get("name_en", ""),
            "material_cn": result.get("material_cn", material_cn),
            "material_en": result.get("material_en", ""),
        }

    except Exception as e:
        print(f"[DeepSeek] 生成申报信息失败: {e}")
        return {"name_cn": "", "name_en": "", "material_cn": material_cn, "material_en": "", "error": str(e)}
