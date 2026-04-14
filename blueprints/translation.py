"""
翻译模块 - 文本翻译、图片翻译、AI翻译
"""
from flask import Blueprint, request, jsonify
from services.translator import translate, translate_image, translate_html_with_structure, tencent_client
from services.geminiAi import (
    generate_ai_response,
    get_translation_prompt,
    generate_ai_img_response,
)

# 创建 Blueprint
translation_bp = Blueprint('translation', __name__, url_prefix='/api')


@translation_bp.route('/translate', methods=['POST'])
def api_translate():
    """文本翻译接口"""
    data = request.get_json()
    text = data.get('text', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not text:
        return jsonify({"error": "No text provided"}), 400

    result = translate(text=text, from_lang=source, to_lang=target, client=tencent_client)
    return jsonify({
        "original": text,
        "translated": result,
        "source": source,
        "target": target
    })


@translation_bp.route('/translate-image', methods=['POST'])
def api_translate_image():
    """图片翻译接口"""
    data = request.get_json()
    image_b64 = data.get('image', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not image_b64:
        return jsonify({"error": "No image provided"}), 400

    try:
        result = translate_image(
            image_data=image_b64,
            from_lang=source,
            to_lang=target,
            client=tencent_client
        )
        return jsonify({
            "original": '',
            "translated": result,
            "source": source,
            "target": target
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@translation_bp.route('/translate-ai', methods=['POST'])
def api_translate_ai():
    """AI文本翻译接口"""
    data = request.get_json()
    text = data.get('text', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    additional_txt = get_translation_prompt(target)
    text = additional_txt + text

    if not text:
        return jsonify({"error": "No text provided"}), 400

    result = generate_ai_response(contents=text)
    return jsonify({
        "original": text,
        "translated": result,
        "source": source,
        "target": target
    })


@translation_bp.route('/translate-ai-image', methods=['POST'])
def api_translate_ai_image():
    """AI图片翻译接口"""
    data = request.get_json()
    image_b64 = data.get('image', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not image_b64:
        return jsonify({"error": "No image provided"}), 400

    prompt_text = get_translation_prompt(target)
    result = generate_ai_img_response(image_base64=image_b64, prompt_text=prompt_text)
    return jsonify({
        "original": '',
        "translated": result,
        "source": source,
        "target": target
    })
