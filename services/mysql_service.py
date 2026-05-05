"""
MySQL 数据库服务模块
统一的数据库连接管理和常用数据库操作
其他服务模块应从此模块导入 get_db_connection，而非从 geminiAi/doubaoAI
"""
import os
import json
import pymysql
from dotenv import load_dotenv


def get_db_connection():
    """获取 MySQL 数据库连接"""
    load_dotenv(override=True)
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "remote_user"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "ai_image_project"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


# ==================== ai_images 表操作 ====================

def save_image_to_db(image_id, url, local_path, prompt, history, user_id=None, model=None):
    """
    保存 AI 生成图片信息到数据库
    兼容 geminiAi 和 doubaoAI 的调用方式
    
    Args:
        model: 模型名称（可选，doubaoAI 会传入，目前暂存入 history_snapshot）
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 序列化历史记录
            history_data = []
            if isinstance(history, list):
                for h in history:
                    if hasattr(h, 'to_json'):
                        history_data.append(json.loads(h.to_json()))
                    elif isinstance(h, dict):
                        history_data.append(h)
                    else:
                        try:
                            history_data.append(json.loads(json.dumps(h, default=lambda o: o.__dict__)))
                        except:
                            continue
            elif isinstance(history, dict):
                history_data = [history]

            history_json = json.dumps(history_data)

            sql = """INSERT INTO ai_images (id, user_id, image_url, local_path, prompt, history_snapshot)
                     VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(sql, (image_id, user_id, url, local_path, prompt, history_json))
        conn.commit()
        print(f"📖 数据库记录已同步: Image ID {image_id}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()


def get_image_relative_path_by_id(image_id):
    """根据 ID 从数据库获取图片的本地存储相对路径"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT local_path FROM ai_images WHERE id = %s"
            cursor.execute(sql, (image_id,))
            result = cursor.fetchone()
            if result:
                return result['local_path']
            return None
    finally:
        conn.close()
