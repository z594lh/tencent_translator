"""
AI生图模块 - 文生图、图生图、图库管理
"""
from flask import Blueprint, request, jsonify
import os
import random
import uuid
from dotenv import load_dotenv

# 导入登录验证装饰器
from blueprints.user_auth import login_required

# 导入AI服务
from services.geminiAi import (
    text_to_image_service as gemini_text_to_image,
    edit_ai_images_service as gemini_edit_images,
)
from services.mysql_service import get_db_connection
from services.doubaoAI import (
    text_to_image_service as doubao_text_to_image,
    edit_ai_images_service as doubao_edit_images,
)

# 创建 Blueprint
ai_image_bp = Blueprint('ai_image', __name__, url_prefix='/api/ai')


# ============== AI模型路由分发器 ==============

def get_image_service_by_model(model_name: str):
    """
    根据模型名称获取对应的AI生图服务函数
    所有AI服务统一使用 edit_ai_images_service 接口
    """
    gemini_models = [
        'gemini-3.1-flash-image-preview',
        'gemini-2.5-flash-image',
        'gemini-2.0-flash-exp-image-generation',
    ]

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
        print(f"⚠️ 未知模型 '{model_name}'，默认使用 Gemini")
        return gemini_edit_images


def get_text_image_service_by_model(model_name: str):
    """
    根据模型名称获取对应的AI文生图服务函数
    """
    gemini_models = [
        'gemini-3.1-flash-image-preview',
        'gemini-2.5-flash-image',
        'gemini-2.0-flash-exp-image-generation',
    ]

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
        print(f"⚠️ 未知模型 '{model_name}'，默认使用 Gemini")
        return gemini_text_to_image


# ============== 文生图接口 ==============
@ai_image_bp.route('/chat-image', methods=['POST'])
@login_required
def chat_image_endpoint():
    """新版统一AI接口文生图"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # 获取当前登录用户ID
        user_id = request.current_user['id']

        message = data.get('prompt')
        model_name = data.get('model', 'gemini-3.1-flash-image-preview')
        count = int(data.get('number_of_images', 1))
        aspect_ratio = data.get('aspect_ratio', '1:1')
        quality = data.get('quality', '512')

        if not message:
            return jsonify({"error": "Message (prompt) is required"}), 400

        text_image_service = get_text_image_service_by_model(model_name)

        print(f"🤖 [Chat-Image] 使用模型: {model_name}, 用户ID: {user_id}")
        result = text_image_service(
            message=message,
            count=count,
            model_name=model_name,
            aspect_ratio=aspect_ratio,
            quality=quality,
            user_id=user_id
        )

        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        res_images = result.get("images", [])
        image_details = result.get("image_details", [])

        return jsonify({
            "image": res_images[0] if res_images else None,
            "images": res_images,
            "image_details": image_details,
            "status": result.get("status", "success"),
            "ai_text": result.get("ai_text", "")
        })

    except Exception as e:
        print(f"Server Error: {str(e)}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


# ============== 图生图接口 ==============
@ai_image_bp.route('/edit-image-v2', methods=['POST'])
@login_required
def unified_edit_image_endpoint():
    """新版统一AI图生图/编辑接口"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # 获取当前登录用户ID
        user_id = request.current_user['id']

        message = data.get('prompt')
        model_name = data.get('model', 'gemini-3.1-flash-image-preview')
        image_ids = data.get('image_ids') or []
        images_b64 = data.get('images') or []
        count = int(data.get('number_of_images', 1))
        aspect_ratio = data.get('aspect_ratio', '1:1')
        quality = data.get('quality', '2K')

        if not message:
            return jsonify({"error": "Message (prompt) is required"}), 400

        if not image_ids and not images_b64:
            return jsonify({"error": "At least one image (image_ids or images) is required"}), 400

        print(f"💡 [Unified Edit API] 收到参数:")
        print(f"   - Prompt: {message}")
        print(f"   - Image IDs: {image_ids}")
        print(f"   - Images (base64 count): {len(images_b64)}")
        print(f"   - Model: {model_name}")
        print(f"   - 用户ID: {user_id}")

        image_service = get_image_service_by_model(model_name)

        print(f"🤖 [Unified API] 使用模型: {model_name}")
        result = image_service(
            message=message,
            image_ids=image_ids if image_ids else None,
            image_b64_list=images_b64 if images_b64 else None,
            count=count,
            model_name=model_name,
            aspect_ratio=aspect_ratio,
            quality=quality,
            user_id=user_id
        )

        if "error" in result:
            return jsonify({"error": result["error"]}), 500

        res_images = result.get("images", [])
        image_details = result.get("image_details", [])

        return jsonify({
            "image": res_images[0] if res_images else None,
            "images": res_images,
            "image_details": image_details,
            "status": result.get("status", "success"),
            "ai_text": result.get("ai_text", ""),
            "model": model_name
        })

    except Exception as e:
        print(f"[Unified Edit API] Error: {str(e)}")
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


# ============== 图库接口 ==============
@ai_image_bp.route('/gallery', methods=['GET'])
@login_required
def get_gallery():
    """获取图库列表 - 只展示当前用户或user_id为空的图片"""
    try:
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 20))

        # 获取当前登录用户ID
        user_id = request.current_user['id']

        try:
            conn = get_db_connection()
        except:
            return jsonify({"status": "success", "images": [], "total": 0, "page": page, "page_size": page_size, "has_more": False})

        try:
            with conn.cursor() as cursor:
                # 只统计当前用户或user_id为空的图片
                cursor.execute(
                    "SELECT COUNT(*) as total FROM ai_images WHERE user_id = %s OR user_id IS NULL",
                    (user_id,)
                )
                total_count = cursor.fetchone()['total']

                offset = (page - 1) * page_size
                # 只查询当前用户或user_id为空的图片
                sql = """
                    SELECT id, image_url FROM ai_images
                    WHERE user_id = %s OR user_id IS NULL
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, (user_id, page_size, offset))
                results = cursor.fetchall()

                result_images = []
                for row in results:
                    try:
                        img_id = str(row['id'])
                        url = str(row['image_url']).encode('utf-8', 'ignore').decode('utf-8')
                    except:
                        img_id = ""
                        url = ""
                    result_images.append({
                        "image_id": img_id,
                        "url": url
                    })

                return jsonify({
                    "status": "success",
                    "images": result_images,
                    "total": total_count,
                    "page": page,
                    "page_size": page_size,
                    "has_more": (offset + len(result_images)) < total_count
                })
        finally:
            try:
                conn.close()
            except:
                pass

    except Exception:
        return jsonify({"status": "success", "images": [], "total": 0, "page": page, "page_size": page_size, "has_more": False})


@ai_image_bp.route('/gallery/<image_id>', methods=['DELETE'])
@login_required
def delete_gallery_image(image_id):
    """删除图库图片"""
    try:
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT local_path FROM ai_images WHERE id = %s", (image_id,))
                result = cursor.fetchone()

                if not result:
                    return jsonify({"status": "error", "message": "图片ID不存在"}), 404

                local_path = result['local_path']

                if local_path and os.path.exists(local_path):
                    os.remove(local_path)
                    print(f"✅ 已删除文件: {local_path}")
                else:
                    print(f"⚠️ 文件不存在或路径为空: {local_path}")

                cursor.execute("DELETE FROM ai_images WHERE id = %s", (image_id,))
                conn.commit()

                return jsonify({"status": "success", "message": "删除成功"}), 200
        finally:
            conn.close()

    except Exception as e:
        print(f"删除异常: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============== 测试接口 ==============
@ai_image_bp.route('/edit-image-test', methods=['POST'])
@login_required
def edit_image_test_endpoint():
    """编辑图片测试接口 - 不调用AI，返回图库随机图片"""
    try:
        data = request.json

        print("\n" + "="*30)
        print("🚀 [Edit Image Test - Frontend Request Data]")
        import json
        print(json.dumps(data, indent=4, ensure_ascii=False))
        print("="*30 + "\n")

        if not data:
            return jsonify({"error": "No data provided"}), 400

        count = int(data.get('number_of_images', 1))
        prompt = data.get('prompt', '')
        image_ids = data.get('image_ids') or []
        images = data.get('images') or []

        print(f"💡 测试模式 - 收到参数:")
        print(f"   - Prompt: {prompt}")
        print(f"   - Image IDs: {image_ids}")
        print(f"   - Images (base64 count): {len(images)}")

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

        return jsonify({
            "image": res_images[0],
            "images": res_images,
            "image_details": image_details,
            "status": "success",
            "ai_text": f"测试模式：Edit-Image 接口测试，收到 Prompt 为 '{data.get('prompt')}'，Image IDs: {data.get('image_ids', [])}"
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 400
