"""
页面路由模块 - 网站页面渲染
"""
from flask import Blueprint, render_template, request
from config import config

# 创建 Blueprint
pages_bp = Blueprint('pages', __name__)


def getConfigUrl():
    """从配置中读取链接"""
    return config.get_tencent_url()


@pages_bp.route('/')
def index():
    """首页"""
    return render_template('index.html', author_link=getConfigUrl())


@pages_bp.route('/upload')
def upload_page():
    """上传页面路由"""
    print(111)
    return render_template('upload.html', author_link=getConfigUrl())


@pages_bp.route('/jellyfin')
def jellyfin_page():
    """Jellyfin视频页面"""
    video_url = request.args.get('url', '')
    return render_template('jellyfin.html', video_url=video_url)
