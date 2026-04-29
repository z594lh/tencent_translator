# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import os
from blueprints.video_enhanced import video_bp
from config import config

# 导入各个功能模块的 Blueprint
from blueprints.pages import pages_bp
from blueprints.user_auth import auth_bp
from blueprints.translation import translation_bp
from blueprints.ai_image import ai_image_bp
from blueprints.expenses import expenses_bp
from blueprints.fba_tools import fba_tools_bp
from blueprints.amazon_api import amazon_api_bp

def getConfigUrl():
    """从配置中读取链接"""
    return config.get_tencent_url()

# 确保 log 目录存在，并生成当天的日志文件路径
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

OUTPUT_DIR = os.path.join(app.static_folder, 'output')

# 确保文件夹存在，不存在就建一个，省得报错
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

try:
    # Flask 2.0+ / 3.0+
    app.json.ensure_ascii = False
except AttributeError:
    # Flask 1.x 老版本
    app.config['JSON_AS_ASCII'] = False


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


@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        'author_link': getConfigUrl()
    })


# 注册页面路由
app.register_blueprint(pages_bp)

# 注册用户认证路由
app.register_blueprint(auth_bp)

# 注册翻译路由
app.register_blueprint(translation_bp)

# 注册AI生图路由
app.register_blueprint(ai_image_bp)

# 注册视频相关路由
app.register_blueprint(video_bp)

# 注册记账路由
app.register_blueprint(expenses_bp)

# 注册 FBA 工具路由
app.register_blueprint(fba_tools_bp)

# 注册亚马逊 API 路由
app.register_blueprint(amazon_api_bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
