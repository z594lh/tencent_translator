/**
 * 断点续传视频上传器
 * 支持硬件加速转码
 */

class VideoUploader {
    constructor(options = {}) {
        this.chunkSize = options.chunkSize || 1024 * 1024; // 1MB
        this.maxRetries = options.maxRetries || 3;
        this.retryDelay = options.retryDelay || 1000;
        this.onProgress = options.onProgress || (() => {});
        this.onComplete = options.onComplete || (() => {});
        this.onError = options.onError || (() => {});
        
        this.fileId = null;
        this.uploadedChunks = [];
        this.isPaused = false;
    }

    async uploadFile(file) {
        try {
            // 检查文件类型
            if (!this.isValidVideoFile(file)) {
                throw new Error('不支持的文件格式');
            }

            // 初始化上传
            const initResult = await this.initUpload(file);
            this.fileId = initResult.fileId;
            
            // 获取已上传的分片
            const uploadedChunks = initResult.status.uploadedChunks || [];
            this.uploadedChunks = new Set(uploadedChunks);

            // 开始上传
            await this.uploadChunks(file);
            
            // 完成上传
            const result = await this.completeUpload(file.name);
            this.onComplete(result);
            
            return result;
            
        } catch (error) {
            this.onError(error);
            throw error;
        }
    }

    isValidVideoFile(file) {
        const validExtensions = ['mp4', 'webm', 'mov', 'avi', 'mkv', 'flv', 'wmv', 'm4v'];
        const extension = file.name.split('.').pop().toLowerCase();
        return validExtensions.includes(extension);
    }

    async initUpload(file) {
        const response = await fetch('/api/upload/init', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                filename: file.name,
                fileSize: file.size,
                chunkSize: this.chunkSize
            })
        });

        if (!response.ok) {
            throw new Error('初始化上传失败');
        }

        return await response.json();
    }

    async uploadChunks(file) {
        const totalChunks = Math.ceil(file.size / this.chunkSize);
        
        for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
            if (this.isPaused) {
                throw new Error('上传已暂停');
            }

            if (this.uploadedChunks.has(chunkIndex)) {
                continue; // 跳过已上传的分片
            }

            const start = chunkIndex * this.chunkSize;
            const end = Math.min(start + this.chunkSize, file.size);
            const chunk = file.slice(start, end);

            await this.uploadChunk(chunkIndex, chunk);
            
            const progress = ((chunkIndex + 1) / totalChunks) * 100;
            this.onProgress(progress, chunkIndex, totalChunks);
        }
    }

    async uploadChunk(chunkIndex, chunk, retries = 0) {
        try {
            const formData = new FormData();
            formData.append('fileId', this.fileId);
            formData.append('chunkIndex', chunkIndex);
            formData.append('chunk', chunk);

            const response = await fetch('/api/upload/chunk', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                throw new Error(`上传分片 ${chunkIndex} 失败`);
            }

            const result = await response.json();
            this.uploadedChunks.add(chunkIndex);
            
            return result;
            
        } catch (error) {
            if (retries < this.maxRetries) {
                console.warn(`重试分片 ${chunkIndex} (${retries + 1}/${this.maxRetries})`);
                await this.delay(this.retryDelay * (retries + 1));
                return this.uploadChunk(chunkIndex, chunk, retries + 1);
            }
            throw error;
        }
    }

    async completeUpload(filename) {
        const response = await fetch('/api/upload/complete', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                fileId: this.fileId,
                filename: filename
            })
        });

        if (!response.ok) {
            throw new Error('完成上传失败');
        }

        return await response.json();
    }

    async getUploadStatus() {
        if (!this.fileId) return null;

        const response = await fetch(`/api/upload/status/${this.fileId}`);
        return await response.json();
    }

    pause() {
        this.isPaused = true;
    }

    resume() {
        this.isPaused = false;
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// 转码管理器
class TranscodeManager {
    async transcodeVideo(filename, options = {}) {
        const response = await fetch(`/api/transcode/${filename}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                codec: options.codec || 'h264',
                quality: options.quality || 'medium'
            })
        });

        if (!response.ok) {
            throw new Error('转码请求失败');
        }

        return await response.json();
    }

    async getHardwareInfo() {
        const response = await fetch('/api/hardware-info');
        return await response.json();
    }
}

// 上传界面控制器
class UploadUI {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.uploader = null;
        this.transcodeManager = new TranscodeManager();
        
        this.initUI();
        this.loadHardwareInfo();
    }

    initUI() {
        this.container.innerHTML = `
            <div class="upload-container">
                <div class="upload-area" id="uploadArea">
                    <div class="upload-icon">📁</div>
                    <p>拖拽视频文件到此处或点击选择</p>
                    <input type="file" id="fileInput" accept="video/*" style="display: none;">
                    <button onclick="document.getElementById('fileInput').click()">选择文件</button>
                </div>
                
                <div class="upload-progress" id="uploadProgress" style="display: none;">
                    <div class="progress-bar">
                        <div class="progress-fill" id="progressFill"></div>
                    </div>
                    <div class="progress-info">
                        <span id="progressText">0%</span>
                        <span id="chunkInfo">0/0 分片</span>
                    </div>
                    <div class="upload-controls">
                        <button id="pauseBtn" onclick="uploadUI.pauseUpload()">暂停</button>
                        <button id="resumeBtn" onclick="uploadUI.resumeUpload()" style="display: none;">继续</button>
                        <button id="cancelBtn" onclick="uploadUI.cancelUpload()">取消</button>
                    </div>
                </div>
                
                <div class="hardware-info" id="hardwareInfo">
                    <h4>硬件加速信息</h4>
                    <div id="hardwareSupport"></div>
                </div>
                
                <div class="uploaded-files" id="uploadedFiles">
                    <h4>已上传文件</h4>
                    <div id="filesList"></div>
                </div>
            </div>
        `;

        this.setupEventListeners();
    }

    setupEventListeners() {
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');

        // 拖拽上传
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('drag-over');
        });

        uploadArea.addEventListener('dragleave', () => {
            uploadArea.classList.remove('drag-over');
        });

        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('drag-over');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                this.handleFile(files[0]);
            }
        });

        // 文件选择
        fileInput.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                this.handleFile(e.target.files[0]);
            }
        });
    }

    async loadHardwareInfo() {
        try {
            const info = await this.transcodeManager.getHardwareInfo();
            const supportDiv = document.getElementById('hardwareSupport');
            
            let html = '<ul>';
            for (const [key, value] of Object.entries(info.hardwareSupport)) {
                html += `<li>${key.toUpperCase()}: ${value ? '✅ 支持' : '❌ 不支持'}</li>`;
            }
            html += '</ul>';
            
            supportDiv.innerHTML = html;
        } catch (error) {
            console.error('加载硬件信息失败:', error);
        }
    }

    async handleFile(file) {
        this.uploader = new VideoUploader({
            chunkSize: 2 * 1024 * 1024, // 2MB
            onProgress: (progress, chunkIndex, totalChunks) => {
                this.updateProgress(progress, chunkIndex, totalChunks);
            },
            onComplete: (result) => {
                this.onUploadComplete(result);
            },
            onError: (error) => {
                this.onUploadError(error);
            }
        });

        this.showProgress();
        
        try {
            await this.uploader.uploadFile(file);
        } catch (error) {
            console.error('上传失败:', error);
        }
    }

    showProgress() {
        document.getElementById('uploadArea').style.display = 'none';
        document.getElementById('uploadProgress').style.display = 'block';
    }

    updateProgress(progress, chunkIndex, totalChunks) {
        document.getElementById('progressFill').style.width = `${progress}%`;
        document.getElementById('progressText').textContent = `${Math.round(progress)}%`;
        document.getElementById('chunkInfo').textContent = `${chunkIndex + 1}/${totalChunks} 分片`;
    }

    onUploadComplete(result) {
        document.getElementById('uploadProgress').style.display = 'none';
        document.getElementById('uploadArea').style.display = 'block';
        
        // 添加到已上传文件列表
        this.addToFilesList(result);
        
        // 可选：自动转码
        if (confirm('上传完成！是否立即转码为H.264格式？')) {
            this.transcodeVideo(result.filename);
        }
    }

    onUploadError(error) {
        alert(`上传失败: ${error.message}`);
        document.getElementById('uploadProgress').style.display = 'none';
        document.getElementById('uploadArea').style.display = 'block';
    }

    pauseUpload() {
        if (this.uploader) {
            this.uploader.pause();
            document.getElementById('pauseBtn').style.display = 'none';
            document.getElementById('resumeBtn').style.display = 'inline-block';
        }
    }

    resumeUpload() {
        if (this.uploader) {
            this.uploader.resume();
            document.getElementById('pauseBtn').style.display = 'inline-block';
            document.getElementById('resumeBtn').style.display = 'none';
        }
    }

    cancelUpload() {
        if (this.uploader) {
            this.uploader.pause();
            this.uploader = null;
        }
        
        document.getElementById('uploadProgress').style.display = 'none';
        document.getElementById('uploadArea').style.display = 'block';
    }

    async transcodeVideo(filename) {
        try {
            const result = await this.transcodeManager.transcodeVideo(filename);
            alert(result.message);
            this.loadFilesList(); // 刷新文件列表
        } catch (error) {
            alert(`转码失败: ${error.message}`);
        }
    }

    addToFilesList(file) {
        const filesList = document.getElementById('filesList');
        const div = document.createElement('div');
        div.className = 'file-item';
        div.innerHTML = `
            <span>${file.filename}</span>
            <button onclick="uploadUI.playVideo('${file.filename}')">播放</button>
            <button onclick="uploadUI.transcodeVideo('${file.filename}')">转码</button>
        `;
        filesList.appendChild(div);
    }

    playVideo(filename) {
        window.open(`/api/video-detail/${filename}`, '_blank');
    }

    async loadFilesList() {
        try {
            const response = await fetch('/api/videos');
            const files = await response.json();
            
            const filesList = document.getElementById('filesList');
            filesList.innerHTML = '';
            
            // 分组显示
            const originalFiles = files.filter(f => f.type === 'original');
            const transcodedFiles = files.filter(f => f.type === 'transcoded');
            
            if (originalFiles.length > 0) {
                const originalDiv = document.createElement('div');
                originalDiv.innerHTML = '<h5>原始视频</h5>';
                filesList.appendChild(originalDiv);
                
                originalFiles.forEach(file => {
                    this.addFileItem(file, '原始');
                });
            }
            
            if (transcodedFiles.length > 0) {
                const transcodedDiv = document.createElement('div');
                transcodedDiv.innerHTML = '<h5>转码视频</h5>';
                filesList.appendChild(transcodedDiv);
                
                transcodedFiles.forEach(file => {
                    this.addFileItem(file, '转码');
                });
            }
            
            if (files.length === 0) {
                filesList.innerHTML = '<p>暂无视频文件</p>';
            }
        } catch (error) {
            console.error('加载文件列表失败:', error);
        }
    }

    addFileItem(file, typeLabel) {
        const filesList = document.getElementById('filesList');
        const div = document.createElement('div');
        div.className = 'file-item';
        
        const sizeMB = (file.size / 1024 / 1024).toFixed(1);
        const date = new Date(file.modified * 1000).toLocaleString();
        
        div.innerHTML = `
            <div>
                <strong>${file.filename}</strong>
                <small>(${typeLabel})</small>
                <br>
                <small>${sizeMB}MB • ${date}</small>
            </div>
            <div>
                <button onclick="uploadUI.playVideo('${file.filename}', ${file.transcoded})">播放</button>
                ${!file.transcoded ? `<button onclick="uploadUI.transcodeVideo('${file.filename}')">转码</button>` : ''}
            </div>
        `;
        filesList.appendChild(div);
    }

    playVideo(filename, isTranscoded) {
        const url = isTranscoded 
            ? `/api/video-detail/transcoded/${filename}`
            : `/api/video-detail/${filename}`;
        window.open(url, '_blank');
    }
}

// 初始化
let uploadUI = null;

document.addEventListener('DOMContentLoaded', () => {
    uploadUI = new UploadUI('uploadContainer');
    uploadUI.loadFilesList();
});

// CSS样式
const style = document.createElement('style');
style.textContent = `
    .upload-container {
        max-width: 800px;
        margin: 0 auto;
        padding: 20px;
    }

    .upload-area {
        border: 2px dashed #ccc;
        border-radius: 8px;
        padding: 40px;
        text-align: center;
        transition: all 0.3s;
        cursor: pointer;
    }

    .upload-area:hover, .upload-area.drag-over {
        border-color: #007bff;
        background-color: #f8f9fa;
    }

    .upload-icon {
        font-size: 48px;
        margin-bottom: 10px;
    }

    .progress-bar {
        width: 100%;
        height: 20px;
        background-color: #e9ecef;
        border-radius: 10px;
        overflow: hidden;
        margin: 10px 0;
    }

    .progress-fill {
        height: 100%;
        background-color: #007bff;
        transition: width 0.3s;
    }

    .progress-info {
        display: flex;
        justify-content: space-between;
        margin-bottom: 10px;
    }

    .upload-controls button {
        margin: 0 5px;
        padding: 5px 15px;
    }

    .hardware-info, .uploaded-files {
        margin-top: 30px;
        padding: 15px;
        background-color: #f8f9fa;
        border-radius: 5px;
    }

    .file-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 10px;
        margin: 5px 0;
        background: white;
        border-radius: 5px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }

    .file-item h5 {
        margin: 10px 0 5px 0;
        color: #495057;
    }

    .file-item button {
        margin-left: 5px;
        padding: 5px 10px;
        font-size: 12px;
    }

    .file-section {
        margin-bottom: 20px;
    }
`;

document.head.appendChild(style);
