"""
通用工具函数 - 适配 quant_v2 架构

提供系统常用的辅助函数
"""

import re
import time
import functools
from typing import List, Optional, Callable, Any, Union
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from .logger import get_logger

logger = get_logger("utils.helpers")


class StockCodeHelper:
    """股票代码处理工具类"""
    
    @staticmethod
    def format_stock_code(code: str, market: Optional[str] = None) -> str:
        """
        格式化股票代码
        
        Args:
            code: 原始股票代码
            market: 市场代码 ('SH', 'SZ')
            
        Returns:
            格式化后的股票代码 (如: '600519.SH')
        """
        # 去除空格和特殊字符
        code = re.sub(r'[^\d]', '', str(code))
        
        # 补齐6位
        if len(code) < 6:
            code = code.zfill(6)
        elif len(code) > 6:
            code = code[:6]
        
        # 如果没有指定市场，根据代码判断
        if market is None:
            if code.startswith(('60', '68', '11', '50', '51')):
                market = 'SH'
            elif code.startswith(('00', '30', '12', '15')):
                market = 'SZ'
            else:
                # 默认深圳
                market = 'SZ'
        
        return f"{code}.{market.upper()}"
    
    @staticmethod
    def parse_stock_code(code: str) -> tuple:
        """
        解析股票代码
        
        Args:
            code: 股票代码 (如: '600519.SH')
            
        Returns:
            (股票代码, 市场) 元组
        """
        if '.' in code:
            stock_code, market = code.split('.')
            return stock_code, market.upper()
        else:
            # 如果没有市场后缀，根据代码判断
            if code.startswith(('60', '68', '11', '50', '51')):
                return code, 'SH'
            elif code.startswith(('00', '30', '12', '15')):
                return code, 'SZ'
            else:
                return code, 'SZ'
    
    @staticmethod
    def is_valid_stock_code(code: str) -> bool:
        """
        验证股票代码是否有效
        
        Args:
            code: 股票代码
            
        Returns:
            是否有效
        """
        if not code:
            return False
        
        # 去除市场后缀
        if '.' in code:
            code = code.split('.')[0]
        
        # 检查是否为6位数字
        if not re.match(r'^\d{6}$', code):
            return False
        
        # 检查是否在有效范围内
        if code.startswith(('60', '68', '11', '50', '51', '00', '30', '12', '15')):
            return True
        
        return False


class DateHelper:
    """日期处理工具类"""
    
    @staticmethod
    def validate_date_format(date_str: str, format_str: str = "%Y-%m-%d") -> bool:
        """
        验证日期格式
        
        Args:
            date_str: 日期字符串
            format_str: 期望的格式
            
        Returns:
            是否有效
        """
        try:
            datetime.strptime(date_str, format_str)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def get_trading_dates(start_date: str, end_date: str) -> List[str]:
        """
        获取交易日列表
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        # 这里应该连接实际的交易日历数据
        # 暂时返回简单的日期范围
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        trading_dates = []
        current = start
        while current <= end:
            # 简单过滤周末，实际应该使用交易日历
            if current.weekday() < 5:  # 0-4 表示周一到周五
                trading_dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        
        return trading_dates
    
    @staticmethod
    def is_trading_day(date_str: str) -> bool:
        """
        判断是否为交易日
        
        Args:
            date_str: 日期字符串
            
        Returns:
            是否为交易日
        """
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            # 简单判断，实际应该使用交易日历
            return date_obj.weekday() < 5
        except ValueError:
            return False


class DataHelper:
    """数据处理工具类"""
    
    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗DataFrame数据
        
        Args:
            df: 原始DataFrame
            
        Returns:
            清洗后的DataFrame
        """
        if df.empty:
            return df
        
        # 去除重复行
        df = df.drop_duplicates()
        
        # 处理缺失值
        df = df.dropna()
        
        # 重置索引
        df = df.reset_index(drop=True)
        
        return df
    
    @staticmethod
    def validate_price_data(df: pd.DataFrame) -> bool:
        """
        验证价格数据是否有效
        
        Args:
            df: 价格数据DataFrame
            
        Returns:
            是否有效
        """
        if df.empty:
            return False
        
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        
        # 检查必需列
        for col in required_columns:
            if col not in df.columns:
                logger.warning(f"缺少必需列: {col}")
                return False
        
        # 检查价格数据合理性
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if df[col].min() <= 0:
                logger.warning(f"价格数据包含非正值: {col}")
                return False
        
        # 检查高低价关系
        if (df['high'] < df['low']).any():
            logger.warning("存在高价小于低价的数据")
            return False
        
        if (df['high'] < df['open']).any() or (df['high'] < df['close']).any():
            logger.warning("存在高价小于开盘价或收盘价的数据")
            return False
        
        if (df['low'] > df['open']).any() or (df['low'] > df['close']).any():
            logger.warning("存在低价大于开盘价或收盘价的数据")
            return False
        
        return True
    
    @staticmethod
    def calculate_returns(prices: pd.Series) -> pd.Series:
        """
        计算收益率
        
        Args:
            prices: 价格序列
            
        Returns:
            收益率序列
        """
        return prices.pct_change().dropna()
    
    @staticmethod
    def calculate_volatility(returns: pd.Series, window: int = 20) -> pd.Series:
        """
        计算波动率
        
        Args:
            returns: 收益率序列
            window: 滚动窗口大小
            
        Returns:
            波动率序列
        """
        return returns.rolling(window=window).std() * np.sqrt(252)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    重试装饰器
    
    Args:
        max_retries: 最大重试次数
        delay: 重试间隔（秒）
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"函数 {func.__name__} 执行失败，第 {attempt + 1} 次重试: {e}")
                        time.sleep(delay)
                    else:
                        logger.error(f"函数 {func.__name__} 执行失败，已达到最大重试次数: {e}")
            
            raise last_exception
        return wrapper
    return decorator


def validate_data_quality(df: pd.DataFrame, data_type: str = "price") -> dict:
    """
    验证数据质量
    
    Args:
        df: 数据DataFrame
        data_type: 数据类型 ('price', 'volume', 'indicator')
        
    Returns:
        质量报告字典
    """
    report = {
        "is_valid": True,
        "issues": [],
        "statistics": {}
    }
    
    if df.empty:
        report["is_valid"] = False
        report["issues"].append("数据为空")
        return report
    
    # 基本统计
    report["statistics"] = {
        "rows": len(df),
        "columns": len(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
        "duplicates": df.duplicated().sum()
    }
    
    # 数据类型特定检查
    if data_type == "price":
        if not DataHelper.validate_price_data(df):
            report["is_valid"] = False
            report["issues"].append("价格数据验证失败")
    
    # 检查缺失值
    missing_ratio = df.isnull().sum().sum() / (len(df) * len(df.columns))
    if missing_ratio > 0.1:  # 缺失值超过10%
        report["issues"].append(f"缺失值比例过高: {missing_ratio:.2%}")
    
    # 检查重复值
    if report["statistics"]["duplicates"] > 0:
        report["issues"].append(f"存在 {report['statistics']['duplicates']} 行重复数据")
    
    return report


# 导出主要接口
__all__ = [
    'StockCodeHelper', 'DateHelper', 'DataHelper',
    'retry_on_failure', 'validate_data_quality'
]
