"""
系统配置

负责：
1. 配置加载
2. 配置验证
3. 配置管理
"""

from typing import Dict, Any
import yaml
import os
import logging

logger = logging.getLogger(__name__)


class Settings:
    """系统配置类"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self.load_config()
        
    def load_config(self) -> Dict[str, Any]:
        """加载配置"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info("配置加载成功")
            return config
        except Exception as e:
            logger.error(f"配置加载失败: {e}")
            return {}
            
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
        
    def set(self, key: str, value: Any):
        """设置配置值"""
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
            
        config[keys[-1]] = value
        
    def save_config(self):
        """保存配置"""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
            logger.info("配置保存成功")
        except Exception as e:
            logger.error(f"配置保存失败: {e}")


# 全局配置实例
settings = Settings()
