# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime, date
import os

from config import config

# 导入各个功能模块的 Blueprint
from blueprints.pages import pages_bp
from blueprints.user_auth import auth_bp
from blueprints.translation import translation_bp
from blueprints.ai_image import ai_image_bp
from blueprints.transactions import transactions_bp
from blueprints.fba_tools import fba_tools_bp
from blueprints.amazon.inventory import amazon_inventory_bp
from blueprints.amazon.shipments import amazon_shipments_bp
from blueprints.amazon.inbound_plans import amazon_inbound_plans_bp
from blueprints.amazon.invoice_export import amazon_invoice_export_bp
from blueprints.amazon.orders import amazon_orders_bp
from blueprints.amazon.listing import amazon_listing_bp
from blueprints.amazon.listing_ownership import listing_ownership_bp
from blueprints.performance import performance_bp
from blueprints.daily_reports import daily_reports_bp
from blueprints.amazon.finances import amazon_finances_bp
from blueprints.amazon.advertising import advertising_bp
from blueprints.amazon.advertising_manage import advertising_manage_bp
from blueprints.shops import shops_bp
from blueprints.supplier import supplier_bp
from blueprints.products import products_bp
from blueprints.logistics import logistics_bp
from blueprints.pricing import pricing_bp
from blueprints.product_board import product_board_bp
from blueprints.reports import reports_bp
from blueprints.cron import cron_bp
from blueprints.permissions import permissions_bp
from blueprints.options import options_bp
from blueprints.asin_bsr import asin_bsr_bp

import services.notification_handlers

# 定时任务已迁移到 Ubuntu crontab，通过 scripts/cron/ 子模块执行
# 不再使用 APScheduler，避免与系统计划任务重复运行
# 如需查看原 APScheduler 代码，参考 services/scheduler.py

def getConfigUrl():
    """从配置中读取链接"""
    return config.get_tencent_url()

# 访问日志目录（相对于 WorkingDirectory，保持现有部署行为）
LOG_DIR = 'log'

# 自定义 JSON 序列化：datetime 统一返回 %Y-%m-%d %H:%M:%S
try:
    from flask.json.provider import DefaultJSONProvider

    class CustomJSONProvider(DefaultJSONProvider):
        def default(self, o):
            if isinstance(o, datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(o, date):
                return o.strftime('%Y-%m-%d')
            return super().default(o)
except ImportError:
    from flask.json import JSONEncoder

    class CustomJSONEncoder(JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime):
                return o.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(o, date):
                return o.strftime('%Y-%m-%d')
            return super().default(o)


def log_request():
    print(f"[Request] {request.method} {request.path} - Content-Length: {request.content_length}", flush=True)


def log_response(response):
    ip = request.headers.get('X-Forwarded-For', request.access_route[0] if request.access_route else request.remote_addr)
    route = request.endpoint or 'N/A'
    method = request.method
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 按天生成日志文件名：log/access-2025-07-03.log
    log_file = os.path.join(LOG_DIR, f"access-{date_str}.log")

    user = getattr(request, 'current_user', None)
    username = user['username'] if user else '-'
    log_line = f"{time_str} - {ip} - {username} - {method} {route}\n"

    # 写入日志文件
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(log_line)

    return response


def get_config():
    return jsonify({
        'author_link': getConfigUrl()
    })


BLUEPRINTS = (
    pages_bp,
    auth_bp,
    translation_bp,
    ai_image_bp,
    transactions_bp,
    fba_tools_bp,
    amazon_inventory_bp,
    amazon_shipments_bp,
    amazon_inbound_plans_bp,
    amazon_invoice_export_bp,
    amazon_orders_bp,
    amazon_listing_bp,
    listing_ownership_bp,
    performance_bp,
    daily_reports_bp,
    amazon_finances_bp,
    advertising_bp,
    advertising_manage_bp,
    shops_bp,
    supplier_bp,
    products_bp,
    logistics_bp,
    pricing_bp,
    product_board_bp,
    reports_bp,
    cron_bp,
    permissions_bp,
    options_bp,
    asin_bsr_bp,
)


def create_app(test_config=None):
    """创建 Flask 应用。

    这里只组装 Web 应用，不启动箱唛或发票后台线程。后台任务由
    ``worker.py`` 的独立单实例进程负责，避免 Gunicorn 多 worker
    重复消费同一任务。
    """
    flask_app = Flask(__name__, template_folder='templates', static_folder='static')
    if test_config:
        flask_app.config.update(test_config)

    # 保留现有跨域行为。前端既可能经 Cloudflare Pages Function 代理，
    # 也可能直接访问 api.zhongyuwen.cn；下载接口需要暴露文件名响应头。
    CORS(flask_app, expose_headers=['Content-Disposition'], max_age=3600)

    try:
        flask_app.json = CustomJSONProvider(flask_app)
    except NameError:
        flask_app.json_encoder = CustomJSONEncoder

    try:
        flask_app.json.ensure_ascii = False
    except AttributeError:
        flask_app.config['JSON_AS_ASCII'] = False

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(flask_app.static_folder, 'output'), exist_ok=True)

    flask_app.before_request(log_request)
    flask_app.after_request(log_response)
    flask_app.add_url_rule('/api/config', 'get_config', get_config, methods=['GET'])

    for blueprint in BLUEPRINTS:
        flask_app.register_blueprint(blueprint)

    return flask_app


# 保留 Gunicorn 的 ``app:app`` 入口兼容性。
app = create_app()
OUTPUT_DIR = os.path.join(app.static_folder, 'output')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
