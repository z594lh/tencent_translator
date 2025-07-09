# app.py
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from datetime import datetime
import os
import configparser
from video import video_bp

# 导入你的翻译模块
from translator import translate, translate_image, translate_html_with_structure, tencent_client
from geminiAI import generate_ai_response,get_translation_prompt,generate_ai_img_response

# 确保 log 目录存在，并生成当天的日志文件路径
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)



@app.before_request
def log_request():
    pass  # 只用于触发 after_request

@app.after_request
def log_response(response):
    ip = request.headers.get('X-Forwarded-For', request.access_route[0] if request.access_route else request.remote_addr)
    route = request.endpoint or 'N/A'
    method = request.method
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 按天生成日志文件名：log/access-2025-07-03.log
    log_file = os.path.join(LOG_DIR, f"access-{date_str}.log")

    log_line = f"{time_str} - {ip} - {method} {route}\n"

    # 写入日志文件
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_line)

    return response


def getConfigUrl():
    # 从配置中读取链接
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH, encoding="utf-8")
        author_link = config.get("TencentCloud", "url")
    except Exception as e:
        author_link = "#"  # 默认值
    
    return author_link;


@app.route('/')
def index():
    return render_template('index.html', author_link=getConfigUrl())


@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        'author_link': getConfigUrl()
    })



@app.route('/api/translate', methods=['POST'])
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

@app.route('/api/translate-image', methods=['POST'])
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

@app.route('/api/translate-ai', methods=['POST'])
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


@app.route('/api/translate-ai-image', methods=['POST'])
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


# 注册视频相关路由
app.register_blueprint(video_bp)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)