"""
增强版视频处理模块
支持硬件加速转码和断点续传
"""

import os
import uuid
import json
import hashlib
import threading
import subprocess
import mimetypes
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, List
import logging
import shutil

import ffmpeg
import av
from flask import Blueprint, jsonify, request, send_from_directory, abort, Response
from werkzeug.utils import secure_filename


# 配置日志，按日期写入log/xxx.log
from logging.handlers import TimedRotatingFileHandler
LOG_DIR = Path(__file__).parent / 'log'
LOG_DIR.mkdir(exist_ok=True)
# 日志文件名格式为 video_YYYY-MM-DD.log
today_str = datetime.now().strftime('%Y-%m-%d')
log_file_path = LOG_DIR / f"video_{today_str}.log"
file_handler = logging.FileHandler(str(log_file_path), encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
file_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# 避免重复添加handler
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == str(log_file_path) for h in logger.handlers):
    logger.addHandler(file_handler)

video_bp = Blueprint('video', __name__)

# 配置路径
VIDEO_FOLDER = Path(__file__).parent / 'videos'
TEMP_FOLDER = VIDEO_FOLDER / 'temp'
TRANSCODED_FOLDER = VIDEO_FOLDER / 'transcoded'

# 确保目录存在
for folder in [VIDEO_FOLDER, TEMP_FOLDER, TRANSCODED_FOLDER]:
    folder.mkdir(parents=True, exist_ok=True)

# 支持的视频格式
SUPPORTED_VIDEO_EXTENSIONS = {
    'mp4', 'webm', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'm4v'
}

# 转码配置
TRANSCODE_CONFIG = {
    'h264': {
        'codec': 'h264_qsv',  # Intel QSV硬件加速
        'preset': 'fast',
        'crf': 23
    },
    'h265': {
        'codec': 'hevc_qsv',  # Intel QSV硬件加速
        'preset': 'fast',
        'crf': 28
    }
}

# 上传状态存储
upload_status: Dict[str, Dict] = {}
status_lock = threading.Lock()


# 兼容虚拟环境下ffmpeg/ffprobe找不到的问题
FFMPEG_PATH = shutil.which('ffmpeg')
FFPROBE_PATH = shutil.which('ffprobe')

# ffmpeg-python的input/output/probe都不要传cmd参数，只在run时传cmd

class VideoProcessor:
    """视频处理类，支持硬件加速"""
    
    @staticmethod
    def check_hardware_support() -> Dict[str, bool]:
        """检查硬件加速支持"""
        support = {
            'qsv': False,
            'nvenc': False,
            'amf': False,
            'reason': ''
        }
        
        try:
            # 检查系统信息
            import platform
            system = platform.system()
            support['system'] = system
            
            # 使用subprocess直接检查FFmpeg支持的编码器
            result = subprocess.run(['ffmpeg', '-codecs'], capture_output=True, text=True)
            if result.returncode == 0:
                codecs_output = result.stdout.lower()
                
                # 检查各种硬件加速编码器
                if 'h264_qsv' in codecs_output:
                    support['qsv'] = True
                if 'h264_nvenc' in codecs_output:
                    support['nvenc'] = True
                if 'h264_amf' in codecs_output:
                    support['amf'] = True
                
                # 添加详细原因
                if system == 'Windows':
                    support['reason'] = 'Windows系统需要Intel Media SDK驱动支持'
                elif system == 'Linux':
                    support['reason'] = 'Linux系统需要VA-API支持'
                else:
                    support['reason'] = 'macOS系统需要VideoToolbox支持'
                    
        except Exception as e:
            logger.error(f"检查硬件加速支持失败: {e}")
            support['reason'] = str(e)
            
        return support
    
    @staticmethod
    def get_video_info(file_path: Path) -> Dict:
        """获取视频信息"""
        try:
            probe = ffmpeg.probe(str(file_path))
            video_stream = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'video'),
                None
            )
            
            if not video_stream:
                return {}
                
            return {
                'duration': float(video_stream.get('duration', 0)),
                'width': int(video_stream.get('width', 0)),
                'height': int(video_stream.get('height', 0)),
                'bitrate': int(video_stream.get('bit_rate', 0)),
                'codec': video_stream.get('codec_name', ''),
                'fps': eval(video_stream.get('r_frame_rate', '0/1'))
            }
        except Exception as e:
            logger.error(f"获取视频信息失败: {e}")
            return {}
    
    @staticmethod
    def transcode_video(
        input_path: Path,
        output_path: Path,
        codec: str = 'h264',
        quality: str = 'medium',
        hw_accel: bool = True
    ) -> bool:
        """转码视频，支持硬件加速"""
        try:
            # 确保输出目录存在
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 检查输入文件是否存在
            if not input_path.exists():
                logger.error(f"输入文件不存在: {input_path}")
                return False
            
            # 获取视频信息以确定合适的参数
            video_info = VideoProcessor.get_video_info(input_path)
            if not video_info:
                logger.error(f"无法获取视频信息: {input_path}")
                return False
            
            # 尝试硬件加速，失败则回退到软件编码
            support = VideoProcessor.check_hardware_support()
            encoder = None
            
            if hw_accel and support['qsv']:
                try:
                    # 尝试QSV硬件加速
                    encoder = TRANSCODE_CONFIG[codec]['codec']
                    logger.info(f"尝试使用硬件加速编码器: {encoder}")
                    stream = ffmpeg.input(str(input_path))
                    stream = ffmpeg.output(
                        stream,
                        str(output_path),
                        vcodec=encoder,
                        preset='fast',
                        movflags='faststart',
                        **{'threads': 0}
                    )
                    ffmpeg.run(stream, overwrite_output=True, quiet=True)
                    
                    if output_path.exists() and output_path.stat().st_size > 0:
                        logger.info(f"硬件加速转码成功: {input_path} -> {output_path}")
                        return True
                        
                except Exception as e:
                    logger.warning(f"硬件加速转码失败: {e}，回退到软件编码")
                    if output_path.exists():
                        output_path.unlink()
            
            # 使用软件编码作为回退
            encoder = 'libx264' if codec == 'h264' else 'libx265'
            logger.info(f"使用软件编码器: {encoder}")
            
            # 设置质量参数
            crf_map = {'low': 18, 'medium': 23, 'high': 28}
            crf = crf_map.get(quality, 23)
            
            # 构建FFmpeg命令（软件编码）
            stream = ffmpeg.input(str(input_path))
            stream = ffmpeg.output(
                stream,
                str(output_path),
                vcodec=encoder,
                crf=crf,
                preset='fast',
                movflags='faststart',
                **{'threads': 0}
            )
            # 执行转码
            try:
                if FFMPEG_PATH:
                    ffmpeg.run(stream, overwrite_output=True, quiet=True, cmd=FFMPEG_PATH)
                else:
                    ffmpeg.run(stream, overwrite_output=True, quiet=True)
            except ffmpeg.Error as e:
                logger.error(f"ffmpeg stderr: {e.stderr.decode('utf-8', errors='ignore') if hasattr(e, 'stderr') and e.stderr else e}")
                raise
            
            # 检查输出文件
            if output_path.exists() and output_path.stat().st_size > 0:
                logger.info(f"软件转码完成: {input_path} -> {output_path} ({output_path.stat().st_size} bytes)")
                return True
            else:
                logger.error(f"软件转码失败: 输出文件为空")
                return False
            
        except Exception as e:
            logger.error(f"转码失败: {e}")
            # 清理失败的输出文件
            if output_path.exists():
                output_path.unlink()
            return False


class UploadManager:
    """断点续传管理器"""
    
    @staticmethod
    def generate_file_id(filename: str, file_size: int) -> str:
        """生成文件唯一ID"""
        content = f"{filename}_{file_size}_{datetime.now().isoformat()}"
        return hashlib.md5(content.encode()).hexdigest()
    
    @staticmethod
    def init_upload(file_id: str, filename: str, file_size: int, chunk_size: int = 1024*1024) -> Dict:
        """初始化上传会话"""
        with status_lock:
            upload_status[file_id] = {
                'filename': filename,
                'file_size': file_size,
                'chunk_size': chunk_size,
                'uploaded_chunks': [],
                'total_chunks': (file_size + chunk_size - 1) // chunk_size,
                'status': 'uploading',
                'created_at': datetime.now().isoformat()
            }
        
        return upload_status[file_id]
    
    @staticmethod
    def save_chunk(file_id: str, chunk_index: int, chunk_data: bytes) -> bool:
        """保存文件分片"""
        try:
            temp_file = TEMP_FOLDER / f"{file_id}_{chunk_index}.part"
            with open(temp_file, 'wb') as f:
                f.write(chunk_data)
            
            with status_lock:
                if file_id in upload_status:
                    if chunk_index not in upload_status[file_id]['uploaded_chunks']:
                        upload_status[file_id]['uploaded_chunks'].append(chunk_index)
            
            return True
        except Exception as e:
            logger.error(f"保存分片失败: {e}")
            return False
    
    @staticmethod
    def merge_chunks(file_id: str, final_filename: str) -> Optional[Path]:
        """合并分片文件"""
        try:
            if file_id not in upload_status:
                return None
                
            status = upload_status[file_id]
            total_chunks = status['total_chunks']
            uploaded_chunks = len(status['uploaded_chunks'])
            
            if uploaded_chunks != total_chunks:
                logger.error(f"分片不完整: {uploaded_chunks}/{total_chunks}")
                return None
            
            # 合并文件
            final_path = VIDEO_FOLDER / final_filename
            with open(final_path, 'wb') as final_file:
                for i in range(total_chunks):
                    chunk_file = TEMP_FOLDER / f"{file_id}_{i}.part"
                    if chunk_file.exists():
                        with open(chunk_file, 'rb') as cf:
                            final_file.write(cf.read())
                        chunk_file.unlink()  # 删除分片
            
            # 清理状态
            with status_lock:
                del upload_status[file_id]
            
            logger.info(f"文件合并完成: {final_path}")
            return final_path
            
        except Exception as e:
            logger.error(f"合并文件失败: {e}")
            return None
    
    @staticmethod
    def get_upload_status(file_id: str) -> Optional[Dict]:
        """获取上传状态"""
        with status_lock:
            return upload_status.get(file_id)


# API路由

@video_bp.route('/api/upload/init', methods=['POST'])
def init_upload():
    """初始化断点续传"""
    try:
        data = request.json
        filename = secure_filename(data.get('filename', ''))
        file_size = int(data.get('fileSize', 0))
        chunk_size = int(data.get('chunkSize', 1024*1024))
        
        if not filename or file_size <= 0:
            return jsonify({'error': '参数无效'}), 400
        
        file_id = UploadManager.generate_file_id(filename, file_size)
        status = UploadManager.init_upload(file_id, filename, file_size, chunk_size)
        
        return jsonify({
            'fileId': file_id,
            'status': status
        })
        
    except Exception as e:
        logger.error(f"初始化上传失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/upload/chunk', methods=['POST'])
def upload_chunk():
    """上传文件分片"""
    try:
        file_id = request.form.get('fileId')
        chunk_index = int(request.form.get('chunkIndex', 0))
        chunk_data = request.files.get('chunk')
        
        if not file_id or not chunk_data:
            return jsonify({'error': '参数缺失'}), 400
        
        success = UploadManager.save_chunk(
            file_id, 
            chunk_index, 
            chunk_data.read()
        )
        
        if success:
            status = UploadManager.get_upload_status(file_id)
            return jsonify({
                'success': True,
                'progress': len(status['uploaded_chunks']) / status['total_chunks'] * 100
            })
        else:
            return jsonify({'error': '保存分片失败'}), 500
            
    except Exception as e:
        logger.error(f"上传分片失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/upload/complete', methods=['POST'])
def complete_upload():
    """完成上传并合并文件"""
    try:
        data = request.json
        file_id = data.get('fileId')
        final_filename = secure_filename(data.get('filename', ''))
        
        if not file_id or not final_filename:
            return jsonify({'error': '参数缺失'}), 400
        
        final_path = UploadManager.merge_chunks(file_id, final_filename)
        
        if final_path and final_path.exists():
            # 获取视频信息
            video_info = VideoProcessor.get_video_info(final_path)
            
            # 异步转码（可选）
            transcoded_path = TRANSCODED_FOLDER / f"{final_path.stem}_h264.mp4"
            threading.Thread(
                target=VideoProcessor.transcode_video,
                args=(final_path, transcoded_path),
                daemon=True
            ).start()
            
            return jsonify({
                'success': True,
                'filename': final_filename,
                'videoInfo': video_info,
                'url': f'/api/video-detail/{final_filename}'
            })
        else:
            return jsonify({'error': '合并文件失败'}), 500
            
    except Exception as e:
        logger.error(f"完成上传失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/upload/status/<file_id>')
def get_upload_status(file_id):
    """获取上传状态"""
    try:
        status = UploadManager.get_upload_status(file_id)
        if status:
            return jsonify(status)
        else:
            return jsonify({'error': '上传会话不存在'}), 404
    except Exception as e:
        logger.error(f"获取上传状态失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/transcode/<filename>', methods=['POST'])
def transcode_video_endpoint(filename):
    """转码视频端点"""
    try:
        data = request.json
        codec = data.get('codec', 'h264')
        quality = data.get('quality', 'medium')
        
        input_path = VIDEO_FOLDER / secure_filename(filename)
        if not input_path.exists():
            return jsonify({'error': '文件不存在'}), 404
        
        output_filename = f"{input_path.stem}_{codec}.mp4"
        output_path = TRANSCODED_FOLDER / output_filename
        
        # 异步转码
        threading.Thread(
            target=VideoProcessor.transcode_video,
            args=(input_path, output_path, codec, quality),
            daemon=True
        ).start()
        
        return jsonify({
            'success': True,
            'outputFile': output_filename,
            'message': '转码任务已启动'
        })
        
    except Exception as e:
        logger.error(f"转码请求失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/hardware-info')
def get_hardware_info():
    """获取硬件加速信息"""
    try:
        support = VideoProcessor.check_hardware_support()
        return jsonify({
            'hardwareSupport': support,
            'transcodeConfig': TRANSCODE_CONFIG
        })
    except Exception as e:
        logger.error(f"获取硬件信息失败: {e}")
        return jsonify({'error': str(e)}), 500


# 保持原有的视频列表和播放接口
@video_bp.route('/api/videos', methods=['GET'])
def list_videos():
    """获取视频列表（包含转码文件）"""
    try:
        videos = []
        
        # 原始视频
        for file_path in VIDEO_FOLDER.glob('*'):
            if file_path.suffix.lower()[1:] in SUPPORTED_VIDEO_EXTENSIONS:
                stat = file_path.stat()
                videos.append({
                    'filename': file_path.name,
                    'url': f'/api/video-detail/{file_path.name}',
                    'size': stat.st_size,
                    'modified': int(stat.st_mtime),
                    'type': 'original',
                    'transcoded': False
                })
        
        # 转码视频
        for file_path in TRANSCODED_FOLDER.glob('*'):
            if file_path.suffix.lower()[1:] in SUPPORTED_VIDEO_EXTENSIONS:
                stat = file_path.stat()
                videos.append({
                    'filename': file_path.name,
                    'url': f'/api/video-detail/transcoded/{file_path.name}',
                    'size': stat.st_size,
                    'modified': int(stat.st_mtime),
                    'type': 'transcoded',
                    'transcoded': True
                })
        
        videos.sort(key=lambda x: x['modified'], reverse=True)
        return jsonify(videos)
        
    except Exception as e:
        logger.error(f"列出视频失败: {e}")
        return jsonify({'error': str(e)}), 500


@video_bp.route('/api/video-detail/<path:filename>')
@video_bp.route('/api/video-detail/transcoded/<path:filename>')
def serve_video(filename):
    """流式传输视频文件（支持HTTP Range请求）"""
    try:
        # 确定文件路径
        if 'transcoded' in request.path:
            file_path = TRANSCODED_FOLDER / filename
        else:
            file_path = VIDEO_FOLDER / filename
        
        if not file_path.exists():
            abort(404, description="视频文件不存在")
        
        # 获取文件信息
        file_size = file_path.stat().st_size
        
        # 处理Range请求（支持分片播放）
        range_header = request.headers.get('Range', None)
        
        if range_header:
            # 解析Range头
            byte_range = range_header.replace('bytes=', '').split('-')
            start = int(byte_range[0]) if byte_range[0] else 0
            end = int(byte_range[1]) if byte_range[1] and byte_range[1] != '' else file_size - 1
            
            if start >= file_size or end >= file_size or start > end:
                abort(416, description="请求的范围无效")
            
            # 计算范围
            length = end - start + 1
            
            # 读取指定范围的数据
            def generate():
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    remaining = length
                    chunk_size = 8192
                    while remaining > 0:
                        chunk = f.read(min(chunk_size, remaining))
                        if not chunk:
                            break
                        yield chunk
                        remaining -= len(chunk)
            
            # 设置响应头
            response = Response(
                generate(),
                206,
                mimetype=mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream',
                direct_passthrough=True
            )
            response.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
            response.headers.add('Accept-Ranges', 'bytes')
            response.headers.add('Content-Length', str(length))
            
            return response
        else:
            # 正常发送整个文件
            return send_from_directory(
                str(file_path.parent), 
                file_path.name,
                as_attachment=False,
                conditional=True
            )
        
    except Exception as e:
        logger.error(f"视频服务错误: {e}")
        return jsonify({'error': str(e)}), 500


# 清理工具
@video_bp.route('/api/cleanup', methods=['POST'])
def cleanup_temp():
    """清理临时文件"""
    try:
        # 清理超过24小时的临时文件
        cutoff = datetime.now().timestamp() - 24 * 3600
        
        for temp_file in TEMP_FOLDER.glob('*.part'):
            if temp_file.stat().st_mtime < cutoff:
                temp_file.unlink()
        
        return jsonify({'success': True, 'message': '清理完成'})
    except Exception as e:
        logger.error(f"清理失败: {e}")
        return jsonify({'error': str(e)}), 500
