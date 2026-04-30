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
from blueprints.supplier import supplier_bp
from blueprints.products import products_bp

# APScheduler 定时任务
from apscheduler.schedulers.background import BackgroundScheduler
from services.amazon_db_sync import AmazonDbSyncService

def run_scheduled_sync():
    """每小时执行的 Amazon 数据同步任务"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Scheduler] 开始定时同步...")
    try:
        service = AmazonDbSyncService()
        results = service.sync_all()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Scheduler] 定时同步完成: {results}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [Scheduler] 定时同步异常: {e}")

# 创建并配置定时任务调度器
scheduler = BackgroundScheduler()
# 每小时执行一次（从整点开始，如 10:00, 11:00...）
scheduler.add_job(run_scheduled_sync, 'cron', minute=0, id='amazon_sync_hourly', replace_existing=True)
scheduler.start()

def getConfigUrl():
    """从配置中读取链接"""
    return config.get_tencent_url()

# 确保 log 目录存在，并生成当天的日志文件路径
LOG_DIR = 'log'
os.makedirs(LOG_DIR, exist_ok=True)


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)

# 自定义 JSON 序列化：datetime 统一返回 %Y-%m-%d %H:%M:%S
try:
    from flask.json.provider import DefaultJSONProvider
    class CustomJSONProvider(DefaultJSONProvider):
        def default(self, o):
            if isinstance(o, datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            return super().default(o)
    app.json = CustomJSONProvider(app)
except ImportError:
    from flask.json import JSONEncoder
    class CustomJSONEncoder(JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            return super().default(o)
    app.json_encoder = CustomJSONEncoder

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

# 注册供应商管理路由
app.register_blueprint(supplier_bp)

# 注册产品管理路由
app.register_blueprint(products_bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
