#!/usr/bin/env python3
"""
视频模块功能测试脚本
"""

import os
import sys
from pathlib import Path
import requests
import json

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from video_enhanced import VideoProcessor

def test_hardware_support():
    """测试硬件加速支持"""
    print("=== 测试硬件加速支持 ===")
    support = VideoProcessor.check_hardware_support()
    print("硬件加速支持:")
    for key, value in support.items():
        print(f"  {key.upper()}: {'✅ 支持' if value else '❌ 不支持'}")
    return support

def test_video_info():
    """测试视频信息获取"""
    print("\n=== 测试视频信息获取 ===")
    # 创建一个测试视频文件（如果存在）
    test_video = Path("videos/test.mp4")
    if test_video.exists():
        info = VideoProcessor.get_video_info(test_video)
        print("视频信息:")
        for key, value in info.items():
            print(f"  {key}: {value}")
    else:
        print("  未找到测试视频文件")

def test_api_endpoints():
    """测试API端点"""
    print("\n=== 测试API端点 ===")
    base_url = "http://localhost:5000"
    
    try:
        # 测试硬件信息端点
        response = requests.get(f"{base_url}/api/hardware-info")
        if response.status_code == 200:
            print("  ✅ /api/hardware-info 正常")
        else:
            print(f"  ❌ /api/hardware-info 错误: {response.status_code}")
            
        # 测试视频列表端点
        response = requests.get(f"{base_url}/api/videos")
        if response.status_code == 200:
            print("  ✅ /api/videos 正常")
            videos = response.json()
            print(f"  当前视频数量: {len(videos)}")
        else:
            print(f"  ❌ /api/videos 错误: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("  ❌ 无法连接到服务器，请确保Flask应用正在运行")

def main():
    """主测试函数"""
    print("视频模块功能测试")
    print("=" * 50)
    
    # 测试硬件支持
    support = test_hardware_support()
    
    # 测试视频信息
    test_video_info()
    
    # 测试API端点
    test_api_endpoints()
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()
