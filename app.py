# app.py
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from datetime import datetime
import os
import random
import uuid
from video_enhanced import video_bp
from jellyfin import get_stream_url
from config import config

# 导入你的翻译模块
from translator import translate, translate_image, translate_html_with_structure, tencent_client
# 导入你的 AI 模块，新增文生图和图生图函数
from geminiAI import (
    generate_ai_response, 
    get_translation_prompt, 
    generate_ai_img_response, 
    generate_ai_images_service, 
)

# 确保 log 目录存在，并生成当天的日志文件路径
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

OUTPUT_DIR = os.path.join(app.static_folder, 'output')

# 确保文件夹存在，不存在就建一个，省得报错
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)



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
    return config.get_tencent_url()


@app.route('/')
def index():
    return render_template('index.html', author_link=getConfigUrl())


@app.route('/upload')
def upload_page():
    """上传页面路由"""
    return render_template('upload.html', author_link=getConfigUrl())


@app.route('/jellyfin')
def jellyfin_page():
    video_url = request.args.get('url', '')
    return render_template('jellyfin.html', video_url=video_url)


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

@app.route('/api/ai/chat-image', methods=['POST'])
def chat_image_endpoint():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # --- 1. 参数提取与映射 ---
        # 对应前端 currentPrompt
        message = data.get('prompt') 
        # 对应前端 inputSessionId
        session_id = data.get('session_id') 
        # 对应前端上传的参考图
        image_b64 = data.get('image') 
        # 对应前端 selectedModel
        model_name = data.get('model', 'gemini-3.1-flash-image-preview') 
        
        # --- 2. 提取新增的配置参数 ---
        # 对应前端 config.num (注意转换类型为 int)
        count = int(data.get('number_of_images', 1))
        # 对应前端 config.ratio (如 "16:9")
        aspect_ratio = data.get('aspect_ratio', '1:1')
        # 对应前端 config.quality (如 "1080")
        quality = data.get('quality', '720')

        if not message:
            return jsonify({"error": "Message (prompt) is required"}), 400

        # --- 3. 调用 Service 方法 ---
        # 注意：这里传入了我们刚才在 Service 中新加的参数
        result = generate_ai_images_service(
            message=message,
            session_id=session_id,
            image_b64=image_b64,
            count=count,
            model_name=model_name,
            aspect_ratio=aspect_ratio,
            quality=quality
        )

        # 检查 Service 内部是否抛出异常
        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        # --- 4. 结构适配返回前端 ---
        res_images = result.get("images", [])
        return jsonify({
            "image": res_images[0] if res_images else None, # 兼容旧的单图逻辑
            "images": res_images,                          # 支持多图展示
            "session_id": result.get("session_id"),
            "status": "success"
        })

    except Exception as e:
        # 打印详细错误方便本地调试
        print(f"Server Error: {str(e)}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

@app.route('/api/ai/gallery', methods=['GET'])
def get_gallery():
    try:
        # 1. 获取分页参数
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))
        
        output_dir = os.path.join('static', 'output')
        base_url = os.getenv("BASE_URL")

        if not os.path.exists(output_dir):
            return jsonify({"images": [], "total": 0})

        # 2. 获取文件列表并按修改时间倒序 (新图在前)
        files = []
        for f in os.listdir(output_dir):
            if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
                file_path = os.path.join(output_dir, f)
                files.append({
                    "name": f,
                    "mtime": os.path.getmtime(file_path)
                })
        
        # 按 mtime 倒序排列
        files.sort(key=lambda x: x['mtime'], reverse=True)

        # 3. 执行分页截取
        total_count = len(files)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        paged_files = files[start_idx:end_idx]

        # 4. 构造完整 URL
        result_images = [f"{base_url}/static/output/{f['name']}" for f in paged_files]

        return jsonify({
            "status": "success",
            "images": result_images,
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "has_more": end_idx < total_count
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/ai/gallery/<path:filename>', methods=['DELETE']) # 使用 <path:filename> 更稳妥
def delete_gallery_image(filename):
    try:
        # 1. 提取纯文件名，防止路径注入
        safe_filename = os.path.basename(filename)
        # 2. 拼接绝对路径
        file_path = os.path.join(OUTPUT_DIR, safe_filename)
        
        # 打印一下，看看后台显示的路径对不对
        print(f"尝试删除文件: {file_path}")

        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"status": "success", "message": "删除成功"}), 200
        else:
            print(f"错误: 文件不存在于 {file_path}")
            return jsonify({"status": "error", "message": "找不到该文件: " + file_path}), 404

    except Exception as e:
        print(f"删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/ai/chat-image-test', methods=['POST'])
def chat_image_test_endpoint():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # --- 1. 参数提取 ---
        session_id = data.get('session_id') or str(uuid.uuid4())[:8]
        count = int(data.get('number_of_images', 1))

        # --- 2. 配置本地路径与域名 ---
        output_dir = os.path.join('static', 'output')
        # 💡 这里加上你的后端完整地址
        base_url = os.getenv("BASE_URL")
        
        if not os.path.exists(output_dir):
            return jsonify({"error": "static/output 文件夹不存在"}), 500

        all_files = [f for f in os.listdir(output_dir) 
                     if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]

        if not all_files:
            return jsonify({"error": "文件夹内没有图片"}), 500

        # 随机抽取
        if len(all_files) >= count:
            selected_files = random.sample(all_files, count)
        else:
            selected_files = [random.choice(all_files) for _ in range(count)]

        # --- 3. 拼接完整的 URL ---
        test_images = [f"{base_url}/static/output/{f}" for f in selected_files]

        return jsonify({
            "image": test_images[0], 
            "images": test_images,
            "session_id": session_id,
            "status": "success"
        })

    except Exception as e:
        print(f"Debug Error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/jellyfin/stream_url')
def api_jellyfin_stream_url():
    import re
    itemid_raw = request.args.get('itemid', '')
    # 支持直接传id或url，自动提取32位itemid
    m = re.search(r'([a-fA-F0-9]{32})', itemid_raw)
    itemid = m.group(1) if m else itemid_raw.strip()
    try:
        url = get_stream_url(itemid)
        return jsonify({'url': url})
    except Exception as e:
        return jsonify({'error': str(e)}), 400


# 注册视频相关路由
app.register_blueprint(video_bp)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
