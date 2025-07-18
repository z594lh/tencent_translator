import os
import configparser
from typing import Dict, Any

class Config:
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        """加载配置文件"""
        if self._config is None:
            BASE_DIR = os.path.dirname(os.path.abspath(__file__))
            CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
            
            self._config = configparser.ConfigParser()
            try:
                self._config.read(CONFIG_PATH, encoding="utf-8")
            except Exception as e:
                print(f"警告：无法读取配置文件 {CONFIG_PATH}: {e}")
                self._config = configparser.ConfigParser()
    
    def get(self, section: str, key: str, default: str = None) -> str:
        """获取配置值"""
        try:
            return self._config.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
    
    def get_tencent_url(self) -> str:
        """获取腾讯云URL"""
        return self.get("TencentCloud", "url", "#")
    
    def get_jellyfin_config(self) -> Dict[str, str]:
        """获取Jellyfin配置"""
        return {
            'url': self.get("JellyFin", "jellyfin_url", ""),
            'username': self.get("JellyFin", "username", ""),
            'password': self.get("JellyFin", "password", "")
        }

config = Config()