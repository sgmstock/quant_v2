#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块MACD和成交量条件分析脚本

分析旅游及景区II板块中符合特定技术条件的股票占比：
条件1：日MACD1>4 且最近20日内日VOL5>VOL30*2小于等于2个
条件2：周MACD1>4 且 周VOL5<VOL30

时间段：2025-08-15到2025-09-19的每个交易日
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

# 导入v2项目的模块
from core.utils.indicators import zhibiao, MACD, MA
from core.utils.stock_filter import get_bankuai_stocks, StockXihua
from data_management.database_manager import DatabaseManager
from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest, get_multiple_stocks_weekly_data_for_backtest


def get_stock_data_from_db(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    从数据库获取股票的日线数据
    
    Args:
        stock_code: 股票代码
        start_date: 开始日期 'YYYY-MM-DD'
        end_date: 结束日期 'YYYY-MM-DD'
    
    Returns:
        pd.DataFrame: 包含OHLCV数据的DataFrame
    """
    try:
        db_manager = DatabaseManager()
        query = """
        SELECT trade_date, open, high, low, close, volume
        FROM k_daily 
        WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
        """
        df = db_manager.execute_query(query, (stock_code, start_date, end_date))
        
        if df.empty:
            return pd.DataFrame()
            
        # 确保数据类型正确
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 删除包含NaN的行
        df = df.dropna()
        
        return df
        
    except Exception as e:
        print(f"获取股票 {stock_code} 数据失败: {e}")
        return pd.DataFrame()


def convert_daily_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    将日线数据转换为周线数据
    
    Args:
        df: 日线数据DataFrame，必须包含trade_date, open, high, low, close, volume列
    
    Returns:
        pd.DataFrame: 周线数据
    """
    if df.empty:
        return pd.DataFrame()
    
    try:
        # 确保有足够的数据
        if len(df) < 5:  # 至少需要5天数据
            print(f"数据不足，只有{len(df)}天数据")
            return pd.DataFrame()
        
        # 复制数据并确保日期格式正确
        daily_df = df.copy()
        
        # 确保trade_date是datetime类型
        if 'trade_date' in daily_df.columns:
            daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
        else:
            print("缺少trade_date列")
            return pd.DataFrame()
        
        # 排序并设置索引
        daily_df = daily_df.sort_values('trade_date')
        daily_df.set_index('trade_date', inplace=True)
        
        # 过滤掉成交量为0的数据
        daily_df = daily_df[daily_df['volume'] > 0]
        
        if daily_df.empty:
            print("过滤后无有效数据")
            return pd.DataFrame()
        
        # 定义聚合规则
        agg_rules = {
            'open': 'first',
            'high': 'max',
            'low': 'min', 
            'close': 'last',
            'volume': 'sum'
        }
        
        # 按周重采样（周五结束）
        weekly_df = daily_df.resample('W-FRI').agg(agg_rules)
        
        # 删除空行
        weekly_df = weekly_df.dropna(subset=['close'])
        
        if weekly_df.empty:
            print("重采样后无数据")
            return pd.DataFrame()
        
        # 重置索引
        weekly_df = weekly_df.reset_index()
        weekly_df = weekly_df.rename(columns={'trade_date': 'week_end'})
        
        return weekly_df
        
    except Exception as e:
        print(f"转换周线数据失败: {e}")
        return pd.DataFrame()


def calculate_technical_indicators(df: pd.DataFrame, is_weekly: bool = False) -> pd.DataFrame:
    """
    计算技术指标
    
    Args:
        df: OHLCV数据
        is_weekly: 是否为周线数据
    
    Returns:
        pd.DataFrame: 包含技术指标的数据
    """
    if df.empty:
        print("输入数据为空")
        return pd.DataFrame()
    
    # 根据是否周线数据调整最少数据要求
    min_periods = 30 if is_weekly else 50
    if len(df) < min_periods:
        print(f"数据不足: 需要至少{min_periods}个周期，实际{len(df)}个周期")
        return pd.DataFrame()
    
    try:
        result_df = df.copy()
        
        # 检查必要的列
        required_cols = ['close', 'volume']
        for col in required_cols:
            if col not in result_df.columns:
                print(f"缺少必要列: {col}")
                return pd.DataFrame()
        
        # 提取数据并检查有效性
        close = result_df['close'].values
        volume = result_df['volume'].values
        
        # 检查数据有效性
        if np.any(np.isnan(close)) or np.any(np.isnan(volume)):
            print("数据包含NaN值")
            return pd.DataFrame()
        
        if np.all(close == 0) or np.all(volume == 0):
            print("数据全为0")
            return pd.DataFrame()
        
        # 计算MACD指标
        try:
            dif, dea, macd = MACD(close, SHORT=12, LONG=26, M=9)
            result_df['DIF'] = dif
            result_df['DEA'] = dea
            result_df['MACD'] = macd
        except Exception as e:
            print(f"MACD计算失败: {e}")
            return pd.DataFrame()
        
        # 计算成交量移动平均
        try:
            result_df['VOL5'] = MA(volume, 5)
            result_df['VOL30'] = MA(volume, 30)
        except Exception as e:
            print(f"成交量均线计算失败: {e}")
            return pd.DataFrame()
        
        return result_df
        
    except Exception as e:
        print(f"计算技术指标失败: {e}")
        return pd.DataFrame()


def check_condition1(daily_df: pd.DataFrame, check_date: str) -> bool:
    """
    检查条件1：日线MACD.iloc[-1]>.iloc[-4] 且最近20日内日VOL5>VOL30*2小于等于2个
    
    Args:
        daily_df: 日线数据（包含技术指标）
        check_date: 检查日期
    
    Returns:
        bool: 是否满足条件1
    """
    try:
        # 找到检查日期的位置
        check_date_dt = pd.to_datetime(check_date)
        
        # 筛选到检查日期为止的数据
        mask = daily_df['trade_date'] <= check_date_dt
        filtered_df = daily_df[mask].copy()
        
        if filtered_df.empty or len(filtered_df) < 5:  # 需要至少5天数据才能比较[-1]和[-4]
            return False
        
        # 获取MACD值列表
        macd_values = filtered_df['MACD'].tolist()
        
        # 检查MACD最新值是否大于4天前的值
        latest_macd = macd_values[-1]  # 最新值
        macd_4_days_ago = macd_values[-5]  # 4天前的值（索引-5对应4天前）
        
        if latest_macd <= macd_4_days_ago:
            return False
        
        # 检查最近20日内VOL5 > VOL30*2的天数
        recent_20_days = filtered_df.tail(min(20, len(filtered_df)))
        vol_condition = recent_20_days['VOL5'] > (recent_20_days['VOL30'] * 2)
        vol_condition_count = vol_condition.sum()
        
        # 小于等于2个
        return vol_condition_count <= 2
        
    except Exception as e:
        print(f"检查条件1失败: {e}")
        return False


def check_condition2(weekly_df: pd.DataFrame, check_date: str) -> bool:
    """
    检查条件2：周MACD.iloc[-1]>.iloc[-4] 且 周VOL5<VOL30
    
    Args:
        weekly_df: 周线数据（包含技术指标）
        check_date: 检查日期
    
    Returns:
        bool: 是否满足条件2
    """
    try:
        # 找到检查日期对应的周
        check_date_dt = pd.to_datetime(check_date)
        
        # 确定日期列名（可能是trade_date或week_end）
        date_col = 'trade_date' if 'trade_date' in weekly_df.columns else 'week_end'
        
        # 筛选到检查日期为止的周数据
        mask = weekly_df[date_col] <= check_date_dt
        filtered_df = weekly_df[mask].copy()
        
        if filtered_df.empty or len(filtered_df) < 5:  # 需要至少5周数据才能比较[-1]和[-4]
            return False
        
        # 获取指标值列表
        macd_values = filtered_df['MACD'].tolist()
        vol5_values = filtered_df['VOL5'].tolist()
        vol30_values = filtered_df['VOL30'].tolist()
        
        # 检查MACD最新值是否大于4周前的值
        latest_macd = macd_values[-1]  # 最新值
        macd_4_weeks_ago = macd_values[-5]  # 4周前的值（索引-5对应4周前）
        
        if latest_macd <= macd_4_weeks_ago:
            return False
        
        # 检查VOL5 < VOL30
        latest_vol5 = vol5_values[-1]
        latest_vol30 = vol30_values[-1]
        
        return latest_vol5 < latest_vol30
        
    except Exception as e:
        print(f"检查条件2失败: {e}")
        return False


def get_trading_days(start_date: str, end_date: str) -> List[str]:
    """
    获取指定时间段内的所有交易日
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        List[str]: 交易日列表
    """
    try:
        db_manager = DatabaseManager()
        query = """
        SELECT trade_date FROM trade_calendar 
        WHERE trade_date >= ? AND trade_date <= ? AND trade_status = 1
        ORDER BY trade_date
        """
        df = db_manager.execute_query(query, (start_date, end_date))
        return df['trade_date'].tolist()
        
    except Exception as e:
        print(f"获取交易日失败: {e}")
        return []


def analyze_sector_conditions(bankuai_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    分析板块中股票符合条件的占比
    
    Args:
        bankuai_name: 板块名称
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        pd.DataFrame: 分析结果
    """
    print(f"开始分析板块: {bankuai_name}")
    print(f"分析期间: {start_date} 到 {end_date}")
    
    # 获取板块股票列表
    stock_list_raw = get_bankuai_stocks(bankuai_name)
    if not stock_list_raw:
        print(f"未找到板块 {bankuai_name} 的成分股")
        return pd.DataFrame()
    
    print(f"板块原始成分股数量: {len(stock_list_raw)}")
    
    # 使用StockXihua过滤股票
    try:
        stock_xihua = StockXihua()
        stock_list = stock_xihua.filter_basic_conditions(stock_list_raw)
        print(f"经过基础筛选后的成分股数量: {len(stock_list)}")
        
        if not stock_list:
            print("筛选后没有符合条件的股票")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"股票筛选失败: {e}")
        print("使用原始股票列表继续分析")
        stock_list = stock_list_raw
    
    # 获取交易日列表
    trading_days = get_trading_days(start_date, end_date)
    if not trading_days:
        print("未找到交易日数据")
        return pd.DataFrame()
    
    print(f"分析交易日数量: {len(trading_days)}")
    
    # 分析每个交易日的条件符合情况
    results = []
    print("\n开始分析每日条件符合情况...")
    
    for i, trade_date in enumerate(trading_days, 1):
        print(f"分析日期 {i}/{len(trading_days)}: {trade_date}")
        
        # 获取当日的日线数据
        daily_data_dict = get_multiple_stocks_daily_data_for_backtest(stock_list, trade_date)
        
        # 获取当日的周线数据  
        weekly_data_dict = get_multiple_stocks_weekly_data_for_backtest(stock_list, trade_date)
        
        condition1_count = 0  # 符合条件1的股票数
        condition2_count = 0  # 符合条件2的股票数
        condition_any_count = 0  # 符合条件1或2的股票数
        valid_stocks = 0  # 有效分析的股票数
        
        # 获取有效股票列表（同时有日线和周线数据）
        valid_stock_codes = set(daily_data_dict.keys()) & set(weekly_data_dict.keys())
        
        for stock_code in valid_stock_codes:
            daily_df = daily_data_dict[stock_code]
            weekly_df = weekly_data_dict[stock_code]
            
            # 检查数据有效性
            if daily_df.empty or weekly_df.empty:
                continue
            
            # 计算技术指标
            daily_with_indicators = calculate_technical_indicators(daily_df, is_weekly=False)
            weekly_with_indicators = calculate_technical_indicators(weekly_df, is_weekly=True)
            
            if daily_with_indicators.empty or weekly_with_indicators.empty:
                continue
                
            valid_stocks += 1
            
            # 检查条件1
            cond1_result = check_condition1(daily_with_indicators, trade_date)
            if cond1_result:
                condition1_count += 1
            
            # 检查条件2
            cond2_result = check_condition2(weekly_with_indicators, trade_date)
            if cond2_result:
                condition2_count += 1
            
            # 检查条件1或2
            if cond1_result or cond2_result:
                condition_any_count += 1
        
        if valid_stocks > 0:
            condition1_ratio = condition1_count / valid_stocks
            condition2_ratio = condition2_count / valid_stocks
            condition_any_ratio = condition_any_count / valid_stocks
            
            results.append({
                'trade_date': trade_date,
                'total_stocks': valid_stocks,
                'condition1_count': condition1_count,
                'condition1_ratio': condition1_ratio,
                'condition2_count': condition2_count,
                'condition2_ratio': condition2_ratio,
                'condition_any_count': condition_any_count,
                'condition_any_ratio': condition_any_ratio
            })
            
            print(f"  有效股票: {valid_stocks}, 条件1: {condition1_count}({condition1_ratio:.2%}), "
                  f"条件2: {condition2_count}({condition2_ratio:.2%}), "
                  f"条件1或2: {condition_any_count}({condition_any_ratio:.2%})")
        else:
            print(f"  当日无有效股票数据")
    
    # 转换为DataFrame
    result_df = pd.DataFrame(results)
    
    if not result_df.empty:
        print(f"\n分析完成！共分析了 {len(result_df)} 个交易日")
        print(f"平均符合条件1的股票占比: {result_df['condition1_ratio'].mean():.2%}")
        print(f"平均符合条件2的股票占比: {result_df['condition2_ratio'].mean():.2%}")
        print(f"平均符合条件1或2的股票占比: {result_df['condition_any_ratio'].mean():.2%}")
    
    return result_df


def save_results(result_df: pd.DataFrame, bankuai_name: str, start_date: str, end_date: str):
    """
    保存分析结果
    
    Args:
        result_df: 分析结果DataFrame
        bankuai_name: 板块名称
        start_date: 开始日期
        end_date: 结束日期
    """
    if result_df.empty:
        print("没有结果可保存")
        return
    
    # 生成文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"sector_macd_volume_analysis_{bankuai_name}_{start_date}_{end_date}_{timestamp}.csv"
    
    # 保存到backtests目录
    filepath = os.path.join(os.path.dirname(__file__), filename)
    
    try:
        # 格式化百分比列
        result_df_formatted = result_df.copy()
        for col in ['condition1_ratio', 'condition2_ratio', 'condition_any_ratio']:
            result_df_formatted[col] = result_df_formatted[col].apply(lambda x: f"{x:.4f}")
        
        result_df_formatted.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"\n结果已保存到: {filepath}")
        
        # 显示统计信息
        print("\n=== 统计摘要 ===")
        print(f"分析期间: {start_date} 到 {end_date}")
        print(f"板块名称: {bankuai_name}")
        print(f"分析交易日数: {len(result_df)}")
        print(f"平均有效股票数: {result_df['total_stocks'].mean():.0f}")
        print(f"条件1平均占比: {result_df['condition1_ratio'].mean():.2%}")
        print(f"条件2平均占比: {result_df['condition2_ratio'].mean():.2%}")
        print(f"条件1或2平均占比: {result_df['condition_any_ratio'].mean():.2%}")
        print(f"条件1或2最高占比: {result_df['condition_any_ratio'].max():.2%}")
        print(f"条件1或2最低占比: {result_df['condition_any_ratio'].min():.2%}")
        
    except Exception as e:
        print(f"保存结果失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("板块MACD和成交量条件分析")
    print("=" * 60)
    
    # 参数设置
    bankuai_name = '旅游及景区'
    start_date = '2025-08-15'
    end_date = '2025-09-19'
    
    print(f"分析板块: {bankuai_name}")
    print(f"分析期间: {start_date} 到 {end_date}")
    print()
    print("条件说明:")
    print("条件1: 日线MACD最新值>4天前值 且最近20日内日VOL5>VOL30*2小于等于2个")
    print("条件2: 周线MACD最新值>4周前值 且 周VOL5<VOL30")
    print("=" * 60)
    
    # 执行分析
    result_df = analyze_sector_conditions(bankuai_name, start_date, end_date)
    
    # 保存结果
    if not result_df.empty:
        save_results(result_df, bankuai_name, start_date, end_date)
    else:
        print("分析未产生有效结果")


if __name__ == "__main__":
    main()

