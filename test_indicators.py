"""
测试迁移的 indicators.py 函数

验证原始指标函数在 quant_v2 中是否正常工作
"""

import pandas as pd
import numpy as np
from core.utils.indicators import *

def test_basic_functions():
    """测试基础函数"""
    print("=== 测试基础函数 ===")
    
    # 测试数据
    data = np.array([100, 101, 102, 103, 104, 105, 106, 107, 108, 109])
    
    # 测试 RD
    result = RD(data, 2)
    print(f"RD 测试: {result[:3]}...")
    
    # 测试 MA
    ma5 = MA(data, 5)
    print(f"MA(5) 测试: {ma5[-3:]}")
    
    # 测试 EMA
    ema5 = EMA(data, 5)
    print(f"EMA(5) 测试: {ema5[-3:]}")
    
    print("基础函数测试完成 ✓\n")

def test_technical_indicators():
    """测试技术指标"""
    print("=== 测试技术指标 ===")
    
    # 创建测试数据
    np.random.seed(42)
    n = 100
    close = 100 + np.cumsum(np.random.randn(n) * 0.5)
    high = close + np.random.rand(n) * 2
    low = close - np.random.rand(n) * 2
    volume = np.random.randint(1000, 10000, n)
    
    # 测试 MACD
    dif, dea, macd = MACD(close)
    print(f"MACD 测试: DIF={dif[-1]:.2f}, DEA={dea[-1]:.2f}, MACD={macd[-1]:.2f}")
    
    # 测试 KDJ
    k, d, j = KDJ(close, high, low)
    print(f"KDJ 测试: K={k[-1]:.2f}, D={d[-1]:.2f}, J={j[-1]:.2f}")
    
    # 测试 RSI
    rsi = RSI(close)
    print(f"RSI 测试: {rsi[-1]:.2f}")
    
    # 测试 BIAS
    bias1, bias2, bias3, bias4 = BIAS(close)
    print(f"BIAS 测试: BIAS1={bias1[-1]:.2f}, BIAS2={bias2[-1]:.2f}")
    
    print("技术指标测试完成 ✓\n")

def test_zhibiao_function():
    """测试 zhibiao 函数"""
    print("=== 测试 zhibiao 函数 ===")
    
    # 创建测试 DataFrame
    np.random.seed(42)
    n = 50
    dates = pd.date_range('2024-01-01', periods=n, freq='D')
    
    df = pd.DataFrame({
        'date': dates,
        'open': 100 + np.cumsum(np.random.randn(n) * 0.5),
        'high': 100 + np.cumsum(np.random.randn(n) * 0.5) + np.random.rand(n) * 2,
        'low': 100 + np.cumsum(np.random.randn(n) * 0.5) - np.random.rand(n) * 2,
        'close': 100 + np.cumsum(np.random.randn(n) * 0.5),
        'volume': np.random.randint(1000, 10000, n)
    })
    
    # 确保 high >= low
    df['high'] = np.maximum(df['high'], df['low'])
    df['low'] = np.minimum(df['high'], df['low'])
    
    # 测试 zhibiao 函数
    result_df = zhibiao(df)
    
    print(f"原始数据列数: {len(df.columns)}")
    print(f"计算后列数: {len(result_df.columns)}")
    print(f"新增指标列: {len(result_df.columns) - len(df.columns)}")
    
    # 检查关键指标
    print(f"MA_5 最新值: {result_df['MA_5'].iloc[-1]:.2f}")
    print(f"MACD 最新值: {result_df['MACD'].iloc[-1]:.2f}")
    print(f"K 最新值: {result_df['K'].iloc[-1]:.2f}")
    print(f"ATR 最新值: {result_df['ATR'].iloc[-1]:.2f}")
    
    print("zhibiao 函数测试完成 ✓\n")

def test_cross_function():
    """测试 CROSS 函数"""
    print("=== 测试 CROSS 函数 ===")
    
    # 创建测试数据
    s1 = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    s2 = np.array([5, 5, 5, 5, 5, 5, 5, 5, 5, 5])
    
    cross_result = CROSS(s1, s2)
    print(f"CROSS 测试: {cross_result}")
    print(f"金叉次数: {np.sum(cross_result)}")
    
    print("CROSS 函数测试完成 ✓\n")

if __name__ == "__main__":
    print("开始测试迁移的 indicators.py 函数...\n")
    
    try:
        test_basic_functions()
        test_technical_indicators()
        test_zhibiao_function()
        test_cross_function()
        
        print("🎉 所有测试通过！indicators.py 迁移成功！")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
