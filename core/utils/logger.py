"""
日志配置模块 - 适配 quant_v2 架构

统一配置系统日志，支持：
1. 多级别日志记录
2. 文件轮转
3. 格式化输出
4. 性能监控
"""

import os
import sys
from typing import Optional
from loguru import logger
from pathlib import Path


class Logger:
    """日志管理器类"""
    
    def __init__(self, name: str = "quant_system"):
        self.name = name
        self.logger = logger.bind(name=name)
        
    def setup_logger(
        self,
        log_file: Optional[str] = None,
        level: str = "INFO",
        log_dir: str = "logs"
    ) -> None:
        """
        设置日志配置
        
        Args:
            log_file: 日志文件路径 (可选)
            level: 日志级别
            log_dir: 日志目录
        """
        # 移除默认handler
        logger.remove()
        
        # 控制台输出配置
        logger.add(
            sys.stdout,
            level=level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                   "<level>{message}</level>",
            colorize=True,
            backtrace=True,
            diagnose=True
        )
        
        # 文件输出配置
        if log_file or log_dir:
            log_path = Path(log_dir)
            log_path.mkdir(exist_ok=True)
            
            if not log_file:
                log_file = log_path / "quant_system.log"
            else:
                log_file = log_path / log_file
            
            # 主日志文件
            logger.add(
                str(log_file),
                level=level,
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
                rotation="10 MB",
                retention="30 days",
                compression="zip",
                backtrace=True,
                diagnose=True,
                enqueue=True  # 多线程安全
            )
            
            # 错误日志文件
            error_log_file = log_path / "error.log"
            logger.add(
                str(error_log_file),
                level="ERROR",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
                rotation="10 MB",
                retention="60 days",
                compression="zip",
                backtrace=True,
                diagnose=True,
                enqueue=True
            )
    
    def info(self, message: str):
        """记录信息日志"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """记录警告日志"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """记录错误日志"""
        self.logger.error(message)
    
    def debug(self, message: str):
        """记录调试日志"""
        self.logger.debug(message)


def setup_logger(
    name: str = "quant_system",
    log_file: Optional[str] = None,
    level: str = "INFO",
    log_dir: str = "logs"
) -> None:
    """
    设置日志配置
    
    Args:
        name: 日志器名称
        log_file: 日志文件路径 (可选)
        level: 日志级别
        log_dir: 日志目录
    """
    logger_instance = Logger(name)
    logger_instance.setup_logger(log_file, level, log_dir)


def get_logger(name: str = "quant_system") -> Logger:
    """
    获取指定名称的日志器
    
    Args:
        name: 日志器名称
        
    Returns:
        配置好的日志器实例
    """
    return Logger(name)


# 性能监控装饰器
def log_execution_time(func_name: str = ""):
    """
    记录函数执行时间的装饰器
    
    Args:
        func_name: 函数名称 (可选)
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            
            func_logger = get_logger(f"performance.{func.__module__}")
            function_name = func_name or f"{func.__name__}"
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                func_logger.info(f"{function_name} 执行完成，耗时: {execution_time:.3f}秒")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                func_logger.error(f"{function_name} 执行失败，耗时: {execution_time:.3f}秒，错误: {e}")
                raise
                
        return wrapper
    return decorator


# 数据更新日志记录器
class DataUpdateLogger:
    """数据更新专用日志记录器"""
    
    def __init__(self, table_name: str, update_type: str = "incremental"):
        self.table_name = table_name
        self.update_type = update_type
        self.logger = get_logger(f"data_update.{table_name}")
        self.start_time = None
        self.records_count = 0
    
    def start(self, message: str = ""):
        """开始记录"""
        import time
        self.start_time = time.time()
        self.logger.info(f"开始更新 {self.table_name} 数据 ({self.update_type}) {message}")
    
    def progress(self, current: int, total: int, message: str = ""):
        """进度记录"""
        percentage = (current / total * 100) if total > 0 else 0
        self.logger.info(f"{self.table_name} 更新进度: {current}/{total} ({percentage:.1f}%) {message}")
    
    def success(self, records_count: int, message: str = ""):
        """成功完成"""
        import time
        if self.start_time:
            elapsed_time = time.time() - self.start_time
            self.logger.info(f"{self.table_name} 更新成功完成，共处理 {records_count} 条记录，耗时: {elapsed_time:.3f}秒 {message}")
        else:
            self.logger.info(f"{self.table_name} 更新成功完成，共处理 {records_count} 条记录 {message}")
        self.records_count = records_count
    
    def error(self, error_message: str):
        """错误记录"""
        import time
        if self.start_time:
            elapsed_time = time.time() - self.start_time
            self.logger.error(f"{self.table_name} 更新失败，耗时: {elapsed_time:.3f}秒，错误: {error_message}")
        else:
            self.logger.error(f"{self.table_name} 更新失败，错误: {error_message}")


# 导出主要接口
__all__ = [
    'Logger', 'setup_logger', 'get_logger', 'log_execution_time', 'DataUpdateLogger'
]
