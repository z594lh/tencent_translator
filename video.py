from flask import Blueprint, jsonify, request, send_from_directory, abort, Response
import os
import uuid
import mimetypes
from datetime import datetime

video_bp = Blueprint('video', __name__)

# 视频文件夹路径
VIDEO_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'videos')
# 确保视频文件夹存在
os.makedirs(VIDEO_FOLDER, exist_ok=True)

# 支持的视频格式
SUPPORTED_VIDEO_EXTENSIONS = {'mp4', 'webm', 'mov', 'avi', 'mkv'}

def is_supported_video(filename):
    """检查文件是否为支持的视频格式"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in SUPPORTED_VIDEO_EXTENSIONS

def get_file_metadata(file_path):
    """获取文件元数据"""
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'modified': int(stat.st_mtime)
        }
    except Exception as e:
        print(f"获取文件元数据失败: {e}")
        return {
            'size': 0,
            'modified': 0
        }

@video_bp.route('/api/upload-video', methods=['POST'])
def upload_video():
    """上传视频文件"""
    if 'file' not in request.files:
        return jsonify({"error": "未上传文件"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "文件名为空"}), 400

    if not is_supported_video(file.filename):
        return jsonify({"error": "不支持的视频格式"}), 400

    # 生成唯一文件名
    file_ext = os.path.splitext(file.filename)[1]
    unique_filename = f"{uuid.uuid4().hex}{file_ext}"
    file_path = os.path.join(VIDEO_FOLDER, unique_filename)

    try:
        file.save(file_path)
        return jsonify({
            "message": "上传成功",
            "filename": unique_filename
        })
    except Exception as e:
        print(f"上传文件失败: {e}")
        return jsonify({"error": "上传文件失败，请重试"}), 500

@video_bp.route('/api/videos', methods=['GET'])
def list_videos():
    """获取视频列表"""
    try:
        if not os.path.exists(VIDEO_FOLDER):
            os.makedirs(VIDEO_FOLDER, exist_ok=True)
            return jsonify([])

        videos = []
        for filename in os.listdir(VIDEO_FOLDER):
            file_path = os.path.join(VIDEO_FOLDER, filename)
            if os.path.isfile(file_path) and is_supported_video(filename):
                metadata = get_file_metadata(file_path)
                videos.append({
                    'filename': filename,
                    'url': f'/api/video-detail/{filename}',  # 使用/api/前缀的路径
                    **metadata
                })

        # 按修改时间排序，最新的在前
        videos.sort(key=lambda x: x['modified'], reverse=True)
        return jsonify(videos)
    
    except Exception as e:
        print(f"列出视频时发生错误: {e}")
        return jsonify({"error": "无法读取视频列表，请检查日志"}), 500

@video_bp.route('/api/video-detail/<path:filename>')
def serve_video(filename):
    """流式传输视频文件（优化大文件处理）"""
    file_path = os.path.join(VIDEO_FOLDER, filename)
    
    # 基础校验
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        abort(404, description="视频文件不存在")
    
    if not is_supported_video(filename):
        abort(403, description="不支持的视频格式")
    
    # 获取文件信息
    file_size = os.path.getsize(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)
    mime_type = mime_type or 'video/mp4'  # 默认使用MP4类型
    
    # 处理Range请求（关键优化：分块读取文件，避免内存溢出）
    range_header = request.headers.get('Range', None)
    
    try:
        if range_header:
            # 解析Range头（格式：bytes=start-end）
            try:
                range_part = range_header.split('=')[1]
                start_str, end_str = range_part.split('-')
                start = int(start_str) if start_str else 0
                end = int(end_str) if end_str else file_size - 1
            except:
                # 解析失败时返回整个文件
                start = 0
                end = file_size - 1
            
            # 确保范围在有效范围内
            start = max(0, min(start, file_size - 1))
            end = max(start, min(end, file_size - 1))
            chunk_size = end - start + 1
            
            # 生成器函数：分块读取文件（每次8KB）
            def generate_chunk():
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    remaining = chunk_size
                    while remaining > 0:
                        chunk = f.read(min(8192, remaining))  # 每次读取8KB
                        if not chunk:
                            break
                        remaining -= len(chunk)
                        yield chunk
            
            # 构建206响应（部分内容）
            response = Response(
                generate_chunk(),
                status=206,
                mimetype=mime_type
            )
            response.headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
            response.headers['Content-Length'] = str(chunk_size)
            
        else:
            # 无Range请求时，也分块传输
            def generate_full():
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(8192)  # 每次读取8KB
                        if not chunk:
                            break
                        yield chunk
            
            response = Response(
                generate_full(),
                status=200,
                mimetype=mime_type
            )
            response.headers['Content-Length'] = str(file_size)
        
        # 通用响应头配置
        response.headers['Accept-Ranges'] = 'bytes'  # 关键：告诉浏览器支持Range请求
        response.headers['Cache-Control'] = 'no-cache'  # 避免缓存导致的问题
        response.headers['Pragma'] = 'no-cache'
        
        return response
    
    except Exception as e:
        # 捕获所有异常，返回具体错误信息
        print(f"处理视频时出错：{str(e)}")
        abort(500, description=f"服务器处理视频失败：{str(e)}")