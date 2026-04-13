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
from dotenv import load_dotenv

# 导入你的翻译模块
from translator import translate, translate_image, translate_html_with_structure, tencent_client
# 导入你的 AI 模块，新增文生图和图生图函数
from geminiAI import (
    generate_ai_response,
    get_translation_prompt,
    generate_ai_img_response,
    text_to_image_service as gemini_text_to_image,
    edit_ai_images_service as gemini_edit_images,
)
# 导入豆包AI模块
from doubaoAI import (
    text_to_image_service as doubao_text_to_image,
    edit_ai_images_service as doubao_edit_images,
   
)
# 导入数据库连接
from geminiAI import get_db_connection

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

# ============== AI模型路由分发器 ==============

def get_image_service_by_model(model_name: str):
    """
    根据模型名称获取对应的AI生图服务函数
    所有AI服务统一使用 edit_ai_images_service 接口

    参数:
        model_name (str): 模型名称

    返回:
        function: 统一的服务函数
    """
    # Gemini 模型列表
    gemini_models = [
        'gemini-3.1-flash-image-preview',
        'gemini-2.5-flash-image',
        'gemini-2.0-flash-exp-image-generation',
    ]

    # 豆包模型列表
    doubao_models = [
        'doubao-seedream-5-0-260128',
        'doubao-seedream-4-5-251128',
        'doubao-seedream-4-0-250828',
    ]

    if any(model in model_name.lower() for model in gemini_models):
        return gemini_edit_images
    elif any(model in model_name.lower() for model in doubao_models):
        return doubao_edit_images
    else:
        # 默认使用 Gemini
        print(f"⚠️ 未知模型 '{model_name}'，默认使用 Gemini")
        return gemini_edit_images


def get_text_image_service_by_model(model_name: str):
    """
    根据模型名称获取对应的AI文生图服务函数

    参数:
        model_name (str): 模型名称

    返回:
        function: 文生图服务函数
    """
    # Gemini 模型列表
    gemini_models = [
        'gemini-3.1-flash-image-preview',
        'gemini-2.5-flash-image',
        'gemini-2.0-flash-exp-image-generation',
    ]

    # 豆包模型列表
    doubao_models = [
        'doubao-seedream-5-0-260128',
        'doubao-seedream-4-5-251128',
        'doubao-seedream-4-0-250828',
    ]

    if any(model in model_name.lower() for model in gemini_models):
        return gemini_text_to_image
    elif any(model in model_name.lower() for model in doubao_models):
        return doubao_text_to_image
    else:
        # 默认使用 Gemini
        print(f"⚠️ 未知模型 '{model_name}'，默认使用 Gemini")
        return gemini_text_to_image


# ============== 新版统一AI接口文生图=============
@app.route('/api/ai/chat-image', methods=['POST'])
def chat_image_endpoint():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # --- 1. 参数提取与映射 ---
        message = data.get('prompt')
        session_id = data.get('session_id')

        model_name = data.get('model', 'gemini-3.1-flash-image-preview')

        # --- 2. 提取配置参数 ---
        count = int(data.get('number_of_images', 1))
        aspect_ratio = data.get('aspect_ratio', '1:1')
        quality = data.get('quality', '512')

        if not message:
            return jsonify({"error": "Message (prompt) is required"}), 400

        # --- 3. 根据模型选择对应的服务 ---
        text_image_service = get_text_image_service_by_model(model_name)

        print(f"🤖 [Chat-Image] 使用模型: {model_name}")
        result = text_image_service(
            message=message,
            session_id=session_id,
            count=count,
            model_name=model_name,
            aspect_ratio=aspect_ratio,
            quality=quality
        )

        # 检查错误
        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        # --- 4. 结构适配返回前端 ---
        res_images = result.get("images", [])
        image_details = result.get("image_details", []) # 包含新生成的 ID

        return jsonify({
            "image": res_images[0] if res_images else None,
            "images": res_images,
            "image_details": image_details, # 返回详情，包含 id 和 url
            "session_id": result.get("session_id"),
            "status": result.get("status", "success"),
            "ai_text": result.get("ai_text", "") # 万一 AI 只回了文字
        })

    except Exception as e:
        print(f"Server Error: {str(e)}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


# ============== 新版统一AI接口图生图=============

@app.route('/api/ai/edit-image-v2', methods=['POST'])
def unified_edit_image_endpoint():
    """
    [新版] 统一AI图生图/编辑接口 - 支持多模型路由
    - Gemini: 支持图生图编辑
    - 豆包: 暂不支持，会返回友好提示
    """
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # --- 1. 参数提取 ---
        message = data.get('prompt')
        session_id = data.get('session_id')
        model_name = data.get('model', 'gemini-3.1-flash-image-preview')

        # 多图支持
        image_ids = data.get('image_ids') or []
        images_b64 = data.get('images') or []

        # --- 2. 提取配置参数 ---
        count = int(data.get('number_of_images', 1))
        aspect_ratio = data.get('aspect_ratio', '1:1')
        quality = data.get('quality', '2K')

        if not message:
            return jsonify({"error": "Message (prompt) is required"}), 400

        # 至少要有图片ID或base64图片之一
        if not image_ids and not images_b64:
            return jsonify({"error": "At least one image (image_ids or images) is required"}), 400

        print(f"💡 [Unified Edit API] 收到参数:")
        print(f"   - Prompt: {message}")
        print(f"   - Image IDs: {image_ids}")
        print(f"   - Images (base64 count): {len(images_b64)}")
        print(f"   - Model: {model_name}")

        # --- 3. 根据模型选择对应的服务 ---
        image_service = get_image_service_by_model(model_name)

        print(f"🤖 [Unified API] 使用模型: {model_name}")
        result = image_service(
            message=message,
            session_id=session_id,
            image_ids=image_ids if image_ids else None,
            image_b64_list=images_b64 if images_b64 else None,
            count=count,
            model_name=model_name,
            aspect_ratio=aspect_ratio,
            quality=quality
        )

        # 检查错误
        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        # --- 4. 结构适配返回前端 ---
        res_images = result.get("images", [])
        image_details = result.get("image_details", [])

        return jsonify({
            "image": res_images[0] if res_images else None,
            "images": res_images,
            "image_details": image_details,
            "session_id": result.get("session_id"),
            "status": result.get("status", "success"),
            "ai_text": result.get("ai_text", ""),
            "model": model_name  # 返回实际使用的模型
        })

    except Exception as e:
        print(f"[Unified Edit API] Error: {str(e)}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500

@app.route('/api/ai/gallery', methods=['GET'])
def get_gallery():
    try:
        # 1. 获取分页参数
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        # 2. 从数据库查询图片，按 created_at 倒序排列
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 查询总数
                cursor.execute("SELECT COUNT(*) as total FROM ai_images")
                total_count = cursor.fetchone()['total']

                # 分页查询图片，按 created_at 倒序
                offset = (page - 1) * page_size
                sql = """
                    SELECT id, image_url FROM ai_images
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, (page_size, offset))
                results = cursor.fetchall()

                # 构建包含 image_id 和 url 的列表
                result_images = [
                    {
                        "image_id": row['id'],
                        "url": row['image_url'].encode('utf-8', 'ignore').decode('utf-8')  # 过滤非法字符
                    }
                    for row in results
                ]

                return jsonify({
                    "status": "success",
                    "images": result_images,
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "has_more": (offset + len(result_images)) < total_count
                })
        finally:
            conn.close()

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/ai/gallery/<image_id>', methods=['DELETE'])
def delete_gallery_image(image_id):
    try:
        # 1. 从数据库查询图片信息
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 查询图片的本地路径
                cursor.execute("SELECT local_path FROM ai_images WHERE id = %s", (image_id,))
                result = cursor.fetchone()

                if not result:
                    return jsonify({"status": "error", "message": "图片ID不存在"}), 404

                local_path = result['local_path']

                # 2. 删除本地文件
                if local_path and os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"✅ 已删除文件: {local_path}")
                else:
                    print(f"⚠️ 文件不存在或路径为空: {local_path}")

                # 3. 从数据库删除记录
                cursor.execute("DELETE FROM ai_images WHERE id = %s", (image_id,))
                conn.commit()

                return jsonify({"status": "success", "message": "删除成功"}), 200
        finally:
            conn.close()

    except Exception as e:
        print(f"删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/ai/edit-image-test', methods=['POST'])
def edit_image_test_endpoint():
    """
    编辑图片测试接口 - 不调用AI，返回图库随机图片
    用于前端测试 edit-image 接口
    """
    try:
        data = request.json
        
        # --- 调试核心：打印前端传来的参数 ---
        print("\n" + "="*30)
        print("🚀 [Edit Image Test - Frontend Request Data]")
        # 使用 json.dumps 让控制台输出带缩进的 JSON，方便观察
        import json
        print(json.dumps(data, indent=4, ensure_ascii=False))
        print("="*30 + "\n")

        if not data:
            return jsonify({"error": "No data provided"}), 400

        # --- 1. 参数提取 (edit-image 接口格式) ---
        session_id = data.get('session_id') or str(uuid.uuid4())[:8]
        count = int(data.get('number_of_images', 1))
        
        prompt = data.get('prompt', '')

        # 获取前端传来的图片参数（edit-image 接口格式）
        image_ids = data.get('image_ids') or []
        images = data.get('images') or []

        print(f"💡 测试模式 - 收到参数:")
        print(f"   - Prompt: {prompt}")
        print(f"   - Image IDs: {image_ids}")
        print(f"   - Images (base64 count): {len(images)}")
        # --- 2. 获取环境变量与路径 ---
        load_dotenv(override=True)
        base_url = os.getenv("BASE_URL", "http://127.0.0.1:5000")
        output_dir = os.path.join('static', 'output')
        
        if not os.path.exists(output_dir):
            return jsonify({"error": "static/output 文件夹不存在"}), 500

        all_files = [f for f in os.listdir(output_dir) 
                     if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]

        if not all_files:
            return jsonify({"error": "文件夹内没有图片"}), 500

        if len(all_files) >= count:
            selected_files = random.sample(all_files, count)
        else:
            selected_files = [random.choice(all_files) for _ in range(count)]

        # --- 3. 构造核心返回数据 ---
        image_details = []
        res_images = []
        
        for f in selected_files:
            url = f"{base_url}/static/output/{f}"
            fake_id = str(uuid.uuid4()) 
            
            res_images.append(url)
            image_details.append({
                "image_id": fake_id,
                "url": url
            })

        # --- 4. 返回结构 ---
        return jsonify({
            "image": res_images[0],
            "images": res_images,
            "image_details": image_details,
            "session_id": session_id,
            "status": "success",
            "ai_text": f"测试模式：Edit-Image 接口测试，收到 Prompt 为 '{data.get('prompt')}'，Image IDs: {data.get('image_ids', [])}"
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 400


# 注册视频相关路由
app.register_blueprint(video_bp)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
