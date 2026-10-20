"""
翻译模块 - 文本翻译、图片翻译、AI翻译
"""
from functools import wraps
from flask import Blueprint, request, jsonify, current_app
from blueprints.user_auth import login_required
from services.baidu_translator import translate, translate_image, TranslationError
from services.geminiAi import (
    generate_ai_response,
    get_translation_prompt,
    generate_ai_img_response,
)

# 创建 Blueprint
translation_bp = Blueprint('translation', __name__, url_prefix='/api')


def machine_response(function):
    """百度错误统一脱敏；成功响应保留原字段，不自动重试收费请求。"""
    @wraps(function)
    def wrapped(*args, **kwargs):
        try:
            response = function(*args, **kwargs)
        except TranslationError as exc:
            body = {'error':str(exc)}
            if exc.provider_code:
                body['error_code'] = exc.provider_code
            response = jsonify(body)
            response.status_code = exc.status
            if exc.retry_after:
                response.headers['Retry-After'] = str(exc.retry_after)
        except Exception:
            # 不记录上游异常文本，避免请求原文/签名/密钥出现在日志中。
            current_app.logger.error('百度翻译本地处理异常，请检查服务配置与实现')
            response = jsonify(error='翻译服务暂不可用，请联系管理员')
            response.status_code = 503
        response.headers['Cache-Control'] = 'no-store'
        return response
    return wrapped


def machine_payload(limit):
    if request.content_length and request.content_length > limit:
        raise TranslationError('翻译请求体过大', 413)
    data = request.get_json(silent=True) if request.is_json else None
    if not isinstance(data, dict):
        raise TranslationError('请提交JSON对象')
    return data


@translation_bp.route('/translate', methods=['POST'])
@login_required
@machine_response
def api_translate():
    """百度文本翻译接口；路径及成功字段保持不变。"""
    data = machine_payload(400000)
    text = data.get('text', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not isinstance(text, str) or not text.strip():
        raise TranslationError('请输入需要翻译的文本')

    result = translate(text=text, from_lang=source, to_lang=target)
    return jsonify({
        "original": text,
        "translated": result,
        "source": source,
        "target": target
    })


@translation_bp.route('/translate-image', methods=['POST'])
@login_required
@machine_response
def api_translate_image():
    """百度图片翻译接口；需要百度账号单独开通图片服务。"""
    data = machine_payload(6 * 1024 * 1024)
    image_b64 = data.get('image', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not isinstance(image_b64, str) or not image_b64:
        raise TranslationError('请提供需要翻译的图片')
    result = translate_image(image_data=image_b64, from_lang=source, to_lang=target)
    return jsonify({
        "original": '',
        "translated": result,
        "source": source,
        "target": target
    })


@translation_bp.route('/translate-ai', methods=['POST'])
@login_required
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
@login_required
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
