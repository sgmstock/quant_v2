"""
Utils 模块测试

测试迁移的 utils 模块功能
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from core.utils.logger import Logger, setup_logger, get_logger
from core.utils.helpers import StockCodeHelper, DateHelper, DataHelper
from core.utils.indicators import TechnicalIndicators


class TestLogger:
    """日志模块测试"""
    
    def test_logger_creation(self):
        """测试日志器创建"""
        logger = Logger("test_logger")
        assert logger.name == "test_logger"
        
    def test_logger_methods(self):
        """测试日志方法"""
        logger = Logger("test_logger")
        # 这些方法应该不抛出异常
        logger.info("测试信息")
        logger.warning("测试警告")
        logger.error("测试错误")
        logger.debug("测试调试")


class TestStockCodeHelper:
    """股票代码处理测试"""
    
    def test_format_stock_code(self):
        """测试股票代码格式化"""
        # 测试基本格式化
        assert StockCodeHelper.format_stock_code("600519") == "600519.SH"
        assert StockCodeHelper.format_stock_code("000001") == "000001.SZ"
        
        # 测试指定市场
        assert StockCodeHelper.format_stock_code("600519", "SH") == "600519.SH"
        assert StockCodeHelper.format_stock_code("000001", "SZ") == "000001.SZ"
        
        # 测试补齐
        assert StockCodeHelper.format_stock_code("1") == "000001.SZ"
        assert StockCodeHelper.format_stock_code("1234567") == "123456.SH"
    
    def test_parse_stock_code(self):
        """测试股票代码解析"""
        code, market = StockCodeHelper.parse_stock_code("600519.SH")
        assert code == "600519"
        assert market == "SH"
        
        code, market = StockCodeHelper.parse_stock_code("000001.SZ")
        assert code == "000001"
        assert market == "SZ"
    
    def test_is_valid_stock_code(self):
        """测试股票代码验证"""
        assert StockCodeHelper.is_valid_stock_code("600519") == True
        assert StockCodeHelper.is_valid_stock_code("000001") == True
        assert StockCodeHelper.is_valid_stock_code("600519.SH") == True
        assert StockCodeHelper.is_valid_stock_code("123") == False
        assert StockCodeHelper.is_valid_stock_code("") == False


class TestDateHelper:
    """日期处理测试"""
    
    def test_validate_date_format(self):
        """测试日期格式验证"""
        assert DateHelper.validate_date_format("2024-01-01") == True
        assert DateHelper.validate_date_format("2024/01/01") == False
        assert DateHelper.validate_date_format("invalid") == False
    
    def test_get_trading_dates(self):
        """测试交易日获取"""
        dates = DateHelper.get_trading_dates("2024-01-01", "2024-01-05")
        assert len(dates) > 0
        assert "2024-01-01" in dates
    
    def test_is_trading_day(self):
        """测试交易日判断"""
        assert DateHelper.is_trading_day("2024-01-01") == True  # 周一
        assert DateHelper.is_trading_day("2024-01-06") == False  # 周六


class TestDataHelper:
    """数据处理测试"""
    
    def test_clean_dataframe(self):
        """测试DataFrame清洗"""
        # 创建测试数据
        df = pd.DataFrame({
            'A': [1, 2, 2, 3, np.nan],
            'B': [1, 1, 2, 3, 4]
        })
        
        cleaned_df = DataHelper.clean_dataframe(df)
        assert len(cleaned_df) == 3  # 去除重复和缺失值后
        assert cleaned_df.index.tolist() == [0, 1, 2]  # 重置索引
    
    def test_validate_price_data(self):
        """测试价格数据验证"""
        # 有效数据
        valid_df = pd.DataFrame({
            'open': [10, 11, 12],
            'high': [11, 12, 13],
            'low': [9, 10, 11],
            'close': [10.5, 11.5, 12.5],
            'volume': [1000, 1100, 1200]
        })
        assert DataHelper.validate_price_data(valid_df) == True
        
        # 无效数据 - 高价小于低价
        invalid_df = pd.DataFrame({
            'open': [10, 11, 12],
            'high': [9, 10, 11],  # 高价小于低价
            'low': [11, 12, 13],
            'close': [10.5, 11.5, 12.5],
            'volume': [1000, 1100, 1200]
        })
        assert DataHelper.validate_price_data(invalid_df) == False
    
    def test_calculate_returns(self):
        """测试收益率计算"""
        prices = pd.Series([100, 105, 110, 108, 112])
        returns = DataHelper.calculate_returns(prices)
        assert len(returns) == 4  # 第一个值为NaN
        assert abs(returns.iloc[0] - 0.05) < 0.001  # 5%收益率


class TestTechnicalIndicators:
    """技术指标测试"""
    
    def setup_method(self):
        """测试前准备"""
        self.indicators = TechnicalIndicators()
        
        # 创建测试数据
        self.test_data = pd.DataFrame({
            'open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            'high': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 108.5, 109.5],
            'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
        })
    
    def test_ma_calculation(self):
        """测试移动平均计算"""
        ma5 = self.indicators.MA(self.test_data['close'], 5)
        assert len(ma5) == len(self.test_data['close'])
        assert not np.isnan(ma5[-1])  # 最后一个值应该不是NaN
    
    def test_ema_calculation(self):
        """测试指数移动平均计算"""
        ema12 = self.indicators.EMA(self.test_data['close'], 12)
        assert len(ema12) == len(self.test_data['close'])
    
    def test_macd_calculation(self):
        """测试MACD计算"""
        dif, dea, macd = self.indicators.MACD(self.test_data['close'])
        assert len(dif) == len(self.test_data['close'])
        assert len(dea) == len(self.test_data['close'])
        assert len(macd) == len(self.test_data['close'])
    
    def test_kdj_calculation(self):
        """测试KDJ计算"""
        k, d, j = self.indicators.KDJ(
            self.test_data['high'], 
            self.test_data['low'], 
            self.test_data['close']
        )
        assert len(k) == len(self.test_data['close'])
        assert len(d) == len(self.test_data['close'])
        assert len(j) == len(self.test_data['close'])
    
    def test_calculate_all_indicators(self):
        """测试计算所有指标"""
        result_df = self.indicators.calculate_all_indicators(self.test_data)
        
        # 检查是否包含预期的指标列
        expected_columns = ['MA_5', 'MA_10', 'MA_20', 'MA_60', 'EMA_12', 'EMA_26',
                          'MACD_DIF', 'MACD_DEA', 'MACD', 'KDJ_K', 'KDJ_D', 'KDJ_J',
                          'RSI', 'BOLL_UPPER', 'BOLL_MID', 'BOLL_LOWER', 'ATR']
        
        for col in expected_columns:
            assert col in result_df.columns, f"缺少指标列: {col}"
    
    def test_get_signal_strength(self):
        """测试信号强度计算"""
        # 先计算指标
        df_with_indicators = self.indicators.calculate_all_indicators(self.test_data)
        
        # 获取信号强度
        signals = self.indicators.get_signal_strength(df_with_indicators)
        
        # 检查信号字典结构
        assert isinstance(signals, dict)
        assert 'total_strength' in signals
        assert 'signal_count' in signals


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v"])
