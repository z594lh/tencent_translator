# app.py
from flask import Flask, render_template, request, jsonify
import os
import configparser

# 导入你的翻译模块
from translator import translate, translate_image, translate_html_with_structure, tencent_client
from geminiAI import generate_ai_response,get_translation_prompt,generate_ai_img_response

app = Flask(__name__, template_folder='templates', static_folder='static')

@app.route('/')
def index():
    # 从配置中读取链接
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH, encoding="utf-8")
        author_link = config.get("TencentCloud", "url")
    except Exception as e:
        author_link = "#"  # 默认值

    return render_template('index.html', author_link=author_link)

@app.route('/translate', methods=['POST'])
def api_translate():
    data = request.get_json()
    text = data.get('text', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not text:
        return jsonify({"error": "No text provided"}), 400

    # 调用你的翻译函数
    result = translate(text=text, from_lang=source, to_lang=target, client=tencent_client)
    return jsonify({
        "original": text,
        "translated": result,
        "source": source,
        "target": target
    })

@app.route('/translate-image', methods=['POST'])
def api_translate_image():
    data = request.get_json()
    image_b64 = data.get('image', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not image_b64:
        return jsonify({"error": "No image provided"}), 400

    try:
        # 调用 translate_image，传入 base64 数据
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

@app.route('/translate-ai', methods=['POST'])
def api_translate_ai():
    data = request.get_json()
    text = data.get('text', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    additional_txt = get_translation_prompt(target)
    text = additional_txt + text;

    if not text:
        return jsonify({"error": "No text provided"}), 400

    # 调用你的翻译函数
    result = generate_ai_response(contents=text)
    return jsonify({
        "original": text,
        "translated": result,
        "source": source,
        "target": target
        })


@app.route('/translate-ai-image', methods=['POST'])
def api_translate_ai_image():
    data = request.get_json()
    image_b64 = data.get('image', '')
    source = data.get('source', 'auto')
    target = data.get('target', 'zh')

    if not image_b64:
        return jsonify({"error": "No image provided"}), 400

    prompt_text = get_translation_prompt(target)


    # 调用你的翻译函数
    result = generate_ai_img_response(image_base64=image_b64,prompt_text=prompt_text)
    return jsonify({
        "original": '',
        "translated": result,
        "source": source,
        "target": target
        })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)