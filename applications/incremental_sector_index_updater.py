#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【修正版】增量更新所有板块（包括细化板块）指数的工作流

核心修正：
1. 修正增量计算逻辑：从"按天循环"改为"按指数循环"，每个指数处理其所有待更新日期
2. 修正表名处理：统一使用 index_k_daily 表存储所有指数数据（标准指数和细化指数）
3. 添加针对每个指数的日期检查：determine_update_dates_for_index 函数
4. 重构核心处理函数：process_single_standard_index_all_dates 和 process_single_refined_index_all_dates
5. 新增重构版主工作流：main_incremental_update_new (推荐使用)

工作流程：
1. 获取所有申万板块
2. 对每个标准板块：确定该指数需要更新的日期，逐日进行连续的增量计算
3. 对每个细化板块：确定各细化指数需要更新的日期，逐日进行连续的增量计算
4. 保存数据到统一的数据表：index_k_daily (标准指数和细化指数统一存储)
"""

# 移除 sqlite3 导入，使用 DatabaseManager
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import sys
import os

# 导入v2项目的模块
from data_management.sector_index_calculator import SectorIndexCalculator
from core.utils.stock_filter import StockXihua
from data_management.data_processor import get_last_trade_date
from data_management.database_manager import DatabaseManager

# 定义细分类型的映射关系
REFINEMENT_MAP = {
    'DSZ': {'name': '大市值', 'attribute': 'dsz'},
    'XSZ': {'name': '小市值', 'attribute': 'xsz'},
    'GBJ': {'name': '高价股', 'attribute': 'gbj'},
    'DBJ': {'name': '低价股', 'attribute': 'dbj'},
    'DG':  {'name': '大高股', 'attribute': 'dg'},
    'GQ':  {'name': '国企股', 'attribute': 'gq'},
    'CQ':  {'name': '超强股', 'attribute': 'cq'}
}

def get_latest_index_info() -> Optional[Tuple[str, float]]:
    """
    从数据库获取指定指数的最新日期和收盘价。
    
    Returns:
        Optional[Tuple[str, float]]: (trade_date, close) 或 None
    """
    try:
        db_manager = DatabaseManager()
        query = """
        SELECT trade_date, close 
        FROM index_k_daily 
        WHERE index_code = '801050.ZS'
        ORDER BY trade_date DESC 
        LIMIT 1
        """
        df = db_manager.execute_query(query)
        
        if not df.empty:
            return df.iloc[0]['trade_date'], df.iloc[0]['close'] # (trade_date, close)
        else:
            return None
    except Exception as e:
        print(f"查询最新指数信息失败 (801050.ZS): {e}")
        return None

def get_index_last_day_info(index_code: str, table_name: str) -> Optional[Tuple[str, float]]:
    """
    从数据库获取指定指数的最新日期和收盘价。
    
    Args:
        index_code (str): 指数代码
        table_name (str): 数据库表名
        
    Returns:
        Optional[Tuple[str, float]]: (trade_date, close_price) 或 None
    """
    try:
        db_manager = DatabaseManager()
        query = f"""
        SELECT trade_date, close 
        FROM {table_name} 
        WHERE index_code = :index_code
        ORDER BY trade_date DESC 
        LIMIT 1
        """
        df = db_manager.execute_query(query, {"index_code": index_code})
        
        if not df.empty:
            return df.iloc[0]['trade_date'], df.iloc[0]['close']  # (trade_date, close_price)
        else:
            return None
    except Exception as e:
        print(f"查询最新指数信息失败 ({index_code}): {e}")
        return None

def get_latest_stock_data_date() -> Optional[str]:
    """
    【优化版】获取股票日线数据表k_daily的最新日期
    
    Returns:
        Optional[str]: 最新日期，格式为 'YYYY-MM-DD' 或 None
    """
    try:
        db_manager = DatabaseManager()
        # 【性能优化】使用MAX()函数比ORDER BY ... LIMIT 1更快
        query = """
        SELECT MAX(trade_date) 
        FROM k_daily
        """
        df = db_manager.execute_query(query)
        
        if not df.empty and not df.iloc[0, 0] is None:
            return df.iloc[0, 0]
        else:
            return None
    except Exception as e:
        print(f"查询股票数据最新日期失败: {e}")
        return None

def get_latest_index_data_date() -> Optional[str]:
    """
    【优化版】获取板块指数表index_k_daily的最新日期
    
    Returns:
        Optional[str]: 最新日期，格式为 'YYYY-MM-DD' 或 None
    """
    try:
        db_manager = DatabaseManager()
        # 【性能优化】使用MAX()函数比ORDER BY ... LIMIT 1更快
        query = """
        SELECT MAX(trade_date) 
        FROM index_k_daily
        """
        df = db_manager.execute_query(query)
        
        if not df.empty and not df.iloc[0, 0] is None:
            return df.iloc[0, 0]
        else:
            return None
    except Exception as e:
        print(f"查询指数数据最新日期失败: {e}")
        return None

def get_latest_index_data_date_for_specific_index(index_code: str, table_name: str = 'index_k_daily') -> Optional[str]:
    """
    【优化版】获取特定指数的最新日期
    
    Args:
        index_code (str): 指数代码，如 '801050.ZS'
        table_name (str): 数据库表名，默认为 'index_k_daily'
    
    Returns:
        Optional[str]: 最新日期，格式为 'YYYY-MM-DD' 或 None
    """
    try:
        db_manager = DatabaseManager()
        # 【性能优化】使用MAX()函数比ORDER BY ... LIMIT 1更快
        query = f"""
        SELECT MAX(trade_date) 
        FROM {table_name} 
        WHERE index_code = :index_code
        """
        df = db_manager.execute_query(query, {"index_code": index_code})
        
        if not df.empty and not df.iloc[0, 0] is None:
            return df.iloc[0, 0]
        else:
            return None
    except Exception as e:
        print(f"查询指数 {index_code} 最新日期失败: {e}")
        return None

def determine_update_dates_for_index(index_code: str, table_name: str = 'index_k_daily') -> List[str]:
    """
    确定特定指数需要更新的日期列表
    
    Args:
        index_code (str): 指数代码
        table_name (str): 数据库表名
        
    Returns:
        List[str]: 需要更新的日期列表，格式为 ['YYYY-MM-DD', ...]
    """
    # 获取股票数据最新日期
    stock_latest_date = get_latest_stock_data_date()
    if stock_latest_date is None:
        return []
    
    # 获取该指数的最新日期
    index_latest_date = get_latest_index_data_date_for_specific_index(index_code, table_name)
    if index_latest_date is None:
        print(f"    ❌ 指数 {index_code} 无历史数据，请先进行全量计算")
        return []
    
    # 比较两个日期
    stock_date = datetime.strptime(stock_latest_date, '%Y-%m-%d')
    index_date = datetime.strptime(index_latest_date, '%Y-%m-%d')
    
    if stock_date > index_date:
        # 计算需要更新的日期范围
        start_update_day = (index_date + timedelta(days=1)).strftime('%Y-%m-%d')
        # 使用v2项目的交易日获取函数
        from data_management.data_processor import get_trade_dates_between
        dates_to_update = get_trade_dates_between(start_update_day, stock_latest_date)
        return dates_to_update if dates_to_update else []
    else:
        # 已是最新或指数数据超前
        return []

def determine_update_dates() -> List[str]:
    """
    确定需要更新的日期列表: 基于股票数据和指数数据的比较。
    
    逻辑：
    1. 获取股票日线数据表k_daily的最新日期
    2. 获取板块指数表index_k_daily的最新日期
    3. 如果股票数据更新，但指数数据未更新，则获取所有需要更新的交易日列表
    
    Returns:
        List[str]: 需要更新的日期列表，格式为 ['YYYY-MM-DD', ...]
    """
    print(">>> 步骤1：确定需要更新的日期")
    
    # 获取股票数据最新日期
    stock_latest_date = get_latest_stock_data_date()
    print(f"股票数据最新日期: {stock_latest_date}")
    
    # 获取指数数据最新日期
    index_latest_date = get_latest_index_data_date()
    print(f"指数数据最新日期: {index_latest_date}")
    
    if stock_latest_date is None:
        print("❌ 未找到股票数据，无法确定更新日期")
        return []
    
    if index_latest_date is None:
        print("❌ 未找到指数数据，请先运行全量计算脚本")
        return []
    
    # 比较两个日期
    stock_date = datetime.strptime(stock_latest_date, '%Y-%m-%d')
    index_date = datetime.strptime(index_latest_date, '%Y-%m-%d')
    
    if stock_date > index_date:
        print(f"✅ 股票数据更新({stock_latest_date})，指数数据滞后({index_latest_date})，需要增量更新")
        
        # 计算需要更新的日期范围：从指数数据的下一天开始，到股票数据的最新一天结束
        start_update_day = (index_date + timedelta(days=1)).strftime('%Y-%m-%d')
        # 使用v2项目的交易日获取函数
        from data_management.data_processor import get_trade_dates_between
        dates_to_update = get_trade_dates_between(start_update_day, stock_latest_date)
        
        if dates_to_update:
            print(f"检测到需要更新的交易日共 {len(dates_to_update)} 天: 从 {dates_to_update[0]} 到 {dates_to_update[-1]}")
            return dates_to_update
        else:
            print(f"在 {start_update_day} 和 {stock_latest_date} 之间没有需要更新的交易日")
            return []
            
    elif stock_date == index_date:
        print(f"ℹ️ 股票数据和指数数据同步({stock_latest_date})，无需更新。")
        return []
    else: # index_date > stock_date
        # 【逻辑修正】
        print(f"⚠️ 警告: 数据不一致！指数数据最新到 {index_latest_date}，但基础股票数据只到 {stock_latest_date}。")
        print("   -> 请先更新股票日线数据。本次更新终止。")
        return []

def get_all_sw_sectors() -> List[Tuple[str, str]]:
    """
    获取所有申万板块的代码和名称（包括L1、L2、L3级别）
    
    Returns:
        List[Tuple[str, str]]: [(板块代码, 板块名称), ...]
    """
    try:
        db_manager = DatabaseManager()
        
        # 查询所有申万板块（L1、L2、L3级别）
        query = """
        SELECT DISTINCT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
        FROM sw_cfg_hierarchy 
        WHERE (l1_code IS NOT NULL AND l1_name IS NOT NULL)
           OR (l2_code IS NOT NULL AND l2_name IS NOT NULL)
           OR (l3_code IS NOT NULL AND l3_name IS NOT NULL)
        ORDER BY l1_code, l2_code, l3_code
        """
        
        df = db_manager.execute_query(query)
        
        sectors = []
        
        if not df.empty:
            # 收集L1级别板块
            l1_mask = df['l1_code'].notna() & df['l1_name'].notna()
            l1_sectors_df: pd.DataFrame = df[l1_mask][['l1_code', 'l1_name']].drop_duplicates()
            for _, row in l1_sectors_df.iterrows():
                sectors.append((row['l1_code'], row['l1_name']))
            
            # 收集L2级别板块
            l2_mask = df['l2_code'].notna() & df['l2_name'].notna()
            l2_sectors_df: pd.DataFrame = df[l2_mask][['l2_code', 'l2_name']].drop_duplicates()
            for _, row in l2_sectors_df.iterrows():
                sectors.append((row['l2_code'], row['l2_name']))
            
            # 收集L3级别板块
            l3_mask = df['l3_code'].notna() & df['l3_name'].notna()
            l3_sectors_df: pd.DataFrame = df[l3_mask][['l3_code', 'l3_name']].drop_duplicates()
            for _, row in l3_sectors_df.iterrows():
                sectors.append((row['l3_code'], row['l3_name']))
            
            print(f"找到申万板块: L1级别 {len(l1_sectors_df)} 个, L2级别 {len(l2_sectors_df)} 个, L3级别 {len(l3_sectors_df)} 个")
            return sectors
        else:
            print("警告：未找到任何申万板块信息")
            return []
            
    except Exception as e:
        print(f"获取申万板块信息失败: {e}")
        return []

def get_sector_constituents(sector_code: str) -> List[str]:
    """
    从sw_cfg_hierarchy表获取板块成分股
    
    Args:
        sector_code (str): 板块代码（如801170.SI, 801011.SI, 850131.SI等）
        
    Returns:
        List[str]: 股票代码列表
    """
    try:
        db_manager = DatabaseManager()
        
        # 精确匹配板块代码查询成分股
        query = """
        SELECT DISTINCT stock_code
        FROM sw_cfg_hierarchy 
        WHERE (l1_code = :sector_code OR l2_code = :sector_code OR l3_code = :sector_code)
        AND stock_code IS NOT NULL
        """
        
        df = db_manager.execute_query(query, {"sector_code": sector_code})
        
        if not df.empty:
            return df['stock_code'].tolist()
        else:
            print(f"警告：未找到板块 {sector_code} 的成分股")
            return []
            
    except Exception as e:
        print(f"获取板块成分股失败: {e}")
        return []

def save_incremental_index_data(index_data: pd.DataFrame, index_code: str, index_name: str, table_name: str, update_date: str):
    """
    【修正版】将增量指数数据保存到数据库，支持不同的表名
    使用 REPLACE 语句确保操作的幂等性，可以安全地重复执行。
    """
    if not isinstance(index_data, pd.DataFrame) or index_data.empty:
        print(f"    ❌ 传入的指数数据为空或格式不正确: {index_code}")
        return

    try:
        # 使用 DatabaseManager 进行数据库操作
        db_manager = DatabaseManager()
        
        # 获取指定日期的数据行
        if update_date in index_data.index:
            row = index_data.loc[update_date]
            
            # 准备单条数据
            data_to_insert = {
                'index_code': index_code,
                'index_name': index_name,
                'trade_date': update_date,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': int(row['volume'])
            }
            
            # 转换为DataFrame进行批量插入
            df_to_insert = pd.DataFrame([data_to_insert])
            
            # 使用批量插入方法
            success = db_manager.batch_insert_dataframe(df_to_insert, table_name)
            
            if success:
                print(f"    ✅ 成功保存/更新增量数据到 {table_name}: {index_code} - {update_date}")
            else:
                print(f"    ❌ 保存增量数据失败: {index_code} - {update_date}")
        else:
            print(f"    ❌ 在计算结果中未找到日期 {update_date} 的数据: {index_code}")

    except Exception as e:
        print(f"    ❌ 保存增量数据到 {table_name} 失败: {e}")

def process_single_standard_index_all_dates(sector_code: str, sector_name: str) -> Tuple[int, List[str]]:
    """
    【性能优化版】处理单个标准申万板块的所有待更新日期
    
    核心优化：
    - 只创建一次SectorIndexCalculator实例，避免重复数据加载
    - 使用所有成分股而非仅前20只，确保指数准确性
    - 优化内存使用和计算效率
    
    Args:
        sector_code (str): 板块代码
        sector_name (str): 板块名称
        
    Returns:
        Tuple[int, List[str]]: (成功更新的日期数量, 失败的日期列表)
    """
    new_index_code = sector_code.replace('.SI', '.ZS')
    index_name = f"{sector_name}指数"
    
    print(f"  处理标准板块: {index_name} ({new_index_code})")
    
    # 1. 确定该指数需要更新的日期列表
    dates_to_update = determine_update_dates_for_index(new_index_code, 'index_k_daily')
    if not dates_to_update:
        print(f"    ℹ️ 指数数据已是最新，无需更新")
        return 0, []
    
    print(f"    需要更新 {len(dates_to_update)} 个日期: {dates_to_update[0]} 到 {dates_to_update[-1]}")
    
    # 2. 获取成分股
    all_stocks = get_sector_constituents(sector_code)
    if len(all_stocks) < 3:
        print(f"    ❌ 成分股数量不足 ({len(all_stocks)})")
        return 0, dates_to_update
    
    print(f"    使用 {len(all_stocks)} 只成分股进行计算")
    
    # 3. 【性能关键】只创建一次计算器实例，加载完整日期范围的数据
    try:
        # 获取第一个更新日期的前一天作为起始日期
        first_update_date = dates_to_update[0]
        last_update_date = dates_to_update[-1]
        
        # 获取起始日期前一天的信息（用于确定数据加载范围）
        start_info = get_index_last_day_info(new_index_code, 'index_k_daily')
        if not start_info:
            print(f"    ❌ 未找到历史数据，无法进行增量更新")
            return 0, dates_to_update
        
        start_date = start_info[0]  # 前一天日期
        
        print(f"    创建计算器实例: 数据范围 {start_date} 到 {last_update_date}")
        
        # 【关键优化】一次性创建计算器，加载所有需要的数据
        calculator = SectorIndexCalculator(
            stock_list=all_stocks,  # 使用所有成分股
            start_date=start_date,
            end_date=last_update_date
        )
        
        print(f"    ✅ 数据加载完成，开始逐日增量计算...")
        
    except Exception as e:
        print(f"    ❌ 创建计算器失败: {e}")
        return 0, dates_to_update
    
    # 4. 【高效循环】逐日进行增量计算，无需重复加载数据
    success_count = 0
    failed_dates = []
    current_last_info = start_info  # 使用初始的历史信息
    
    for i, update_date in enumerate(dates_to_update, 1):
        try:
            print(f"      [{i}/{len(dates_to_update)}] 计算 {update_date}...")
            
            # 执行增量计算（现在非常快，因为数据已在内存中）
            new_index_row = calculator.calculate_incremental(current_last_info)
            
            # 保存到数据库
            save_incremental_index_data(new_index_row, new_index_code, index_name, 'index_k_daily', update_date)
            
            # 更新当前最新信息，为下一次计算做准备
            if update_date in new_index_row.index:
                current_last_info = (update_date, new_index_row.loc[update_date]['close'])
                success_count += 1
                print(f"      ✅ 成功更新 {update_date}")
            else:
                failed_dates.append(update_date)
                print(f"      ❌ 计算结果中缺少 {update_date} 数据")
                
        except Exception as e:
            print(f"      ❌ 更新 {update_date} 失败: {e}")
            failed_dates.append(update_date)
            # 不要中断循环，继续处理下一个日期
            continue
    
    print(f"    标准板块更新完成: 成功 {success_count}/{len(dates_to_update)} 个日期")
    return success_count, failed_dates

def process_standard_sector_incremental(sector_code: str, sector_name: str, update_date: str) -> bool:
    """
    【兼容版】处理单个标准申万板块的增量更新 - 保持向后兼容
    """
    new_index_code = sector_code.replace('.SI', '.ZS')
    index_name = f"{sector_name}指数"
    
    print(f"  处理标准板块: {index_name} ({new_index_code})")
    
    # 1. 获取该指数昨天的信息
    last_day_info = get_index_last_day_info(new_index_code, 'index_k_daily')
    if not last_day_info:
        print(f"    ❌ 未找到历史数据，无法进行增量更新: {new_index_code}。请先进行全量计算。")
        return False
    
    last_trade_date, last_close_price = last_day_info
    print(f"    上一交易日: {last_trade_date}, 收盘价: {last_close_price:.2f}")

    # 检查是否需要更新：如果指数数据的最新日期 >= 更新日期，则无需更新
    if last_trade_date >= update_date:
        print(f"    指数数据已是最新({last_trade_date})，无需更新。")
        return True

    try:
        # 2. 获取成分股
        all_stocks = get_sector_constituents(sector_code)
        if len(all_stocks) < 3:
            print(f"    ❌ 成分股数量不足 ({len(all_stocks)})")
            return False
            
        demo_stocks = all_stocks[:20] # 演示逻辑保留

        # 3. 准备计算器，只需要获取两天的数据
        calculator = SectorIndexCalculator(
            stock_list=demo_stocks,
            start_date=last_trade_date, # 开始日期是上一个交易日
            end_date=update_date      # 结束日期是今天
        )
        
        # 4. 执行增量计算
        new_index_row = calculator.calculate_incremental(last_day_info)

        # 5. 保存新的单行数据到数据库
        save_incremental_index_data(new_index_row, new_index_code, index_name, 'index_k_daily', update_date)
        
        return True
        
    except Exception as e:
        print(f"    ❌ 处理标准板块 {sector_code} 失败: {e}")
        import traceback
        traceback.print_exc() # 打印详细错误信息
        return False

def process_single_refined_index_all_dates(sector_code: str, sector_name: str) -> Tuple[int, Dict[str, List[str]]]:
    """
    【性能优化版】处理单个申万板块的所有细化板块的所有待更新日期
    
    核心优化：
    - 为每个细化指数只创建一次SectorIndexCalculator实例
    - 使用所有成分股进行细化分类，确保指数准确性
    - 优化内存使用和计算效率
    
    Args:
        sector_code (str): 板块代码
        sector_name (str): 板块名称
        
    Returns:
        Tuple[int, Dict[str, List[str]]]: (成功更新的细化指数总数, 失败的日期字典)
    """
    print(f"  处理细化板块: {sector_name} ({sector_code})")
    
    try:
        # 获取成分股
        all_stocks = get_sector_constituents(sector_code)
        if len(all_stocks) == 0:
            print(f"    ❌ 未找到板块 {sector_code} 的成分股")
            return 0, {}
        
        print(f"    使用 {len(all_stocks)} 只成分股进行细化分类")
        
        # 执行细化筛选（使用所有成分股）
        xihua = StockXihua()
        stock_df = xihua.create_stock_dataframe(all_stocks)  # 使用所有成分股
        
        if not stock_df.empty:
            xihua.calculate_quantile_categories(stock_df)
        else:
            print("    ❌ 无法创建股票数据框，使用模拟数据")
            # 使用模拟数据（基于所有成分股）
            total_stocks = len(all_stocks)
            xihua.dsz = all_stocks[int(total_stocks*0.8):]  # 后20%大市值
            xihua.xsz = all_stocks[:int(total_stocks*0.2)]   # 前20%小市值
            xihua.gbj = all_stocks[int(total_stocks*0.5):int(total_stocks*0.8)]  # 中间30%高价股
            xihua.dbj = all_stocks[:int(total_stocks*0.3)]   # 前30%低价股
            xihua.dg = list(set(xihua.dsz) & set(xihua.gbj))  # 大高股交集
        
        # 循环处理所有细分板块的指数
        total_success_count = 0
        failed_dates_dict = {}
        
        for tag, info in REFINEMENT_MAP.items():
            # 获取对应的股票列表
            sub_stock_list = getattr(xihua, info['attribute'])
            
            # 检查是否有足够的股票来计算指数
            if not sub_stock_list or len(sub_stock_list) < 3:
                print(f"    ⚠️ 跳过 {info['name']} ({tag}): 成分股不足 ({len(sub_stock_list) if sub_stock_list else 0} < 3)")
                continue
            
            try:
                # 生成指数代码和名称
                clean_sector_code = sector_code.replace('.SI', '')
                sub_index_code = f"{clean_sector_code}.{tag}"
                sub_index_name = f"{sector_name}-{info['name']}指数"
                
                print(f"    处理细化指数: {sub_index_name} ({sub_index_code}), 成分股: {len(sub_stock_list)} 只")
                
                # 确定该细化指数需要更新的日期列表
                dates_to_update = determine_update_dates_for_index(sub_index_code, 'index_k_daily')
                if not dates_to_update:
                    print(f"      ℹ️ 细化指数数据已是最新，无需更新")
                    total_success_count += 1  # 算作成功
                    continue
                
                print(f"      需要更新 {len(dates_to_update)} 个日期: {dates_to_update[0]} 到 {dates_to_update[-1]}")
                
                # 【性能关键】只创建一次计算器实例
                try:
                    # 获取起始日期信息
                    start_info = get_index_last_day_info(sub_index_code, 'index_k_daily')
                    if not start_info:
                        print(f"      ❌ 未找到历史数据，跳过该细化指数")
                        failed_dates_dict[sub_index_code] = ["无历史数据"]
                        continue
                    
                    start_date = start_info[0]
                    last_update_date = dates_to_update[-1]
                    
                    print(f"      创建计算器: 数据范围 {start_date} 到 {last_update_date}")
                    
                    # 【关键优化】一次性创建计算器
                    calculator = SectorIndexCalculator(
                        stock_list=sub_stock_list,
                        start_date=start_date,
                        end_date=last_update_date
                    )
                    
                    print(f"      ✅ 数据加载完成，开始逐日计算...")
                    
                except Exception as e:
                    print(f"      ❌ 创建计算器失败: {e}")
                    failed_dates_dict[sub_index_code] = [f"创建计算器失败: {e}"]
                    continue
                
                # 【高效循环】逐日进行增量计算
                success_count = 0
                failed_dates = []
                current_last_info = start_info
                
                for i, update_date in enumerate(dates_to_update, 1):
                    try:
                        print(f"        [{i}/{len(dates_to_update)}] 计算 {update_date}...")
                        
                        # 执行增量计算（现在非常快）
                        new_index_row = calculator.calculate_incremental(current_last_info)
                        
                        # 保存到数据库
                        save_incremental_index_data(new_index_row, sub_index_code, sub_index_name, 'index_k_daily', update_date)
                        
                        # 更新当前最新信息
                        if update_date in new_index_row.index:
                            current_last_info = (update_date, new_index_row.loc[update_date]['close'])
                            success_count += 1
                            print(f"        ✅ 成功更新 {update_date}")
                        else:
                            failed_dates.append(update_date)
                            print(f"        ❌ 计算结果中缺少 {update_date} 数据")
                            
                    except Exception as e:
                        print(f"        ❌ 更新 {update_date} 失败: {e}")
                        failed_dates.append(update_date)
                        continue
                
                if success_count > 0:
                    total_success_count += 1
                
                if failed_dates:
                    failed_dates_dict[sub_index_code] = failed_dates
                
                print(f"      细化指数更新完成: 成功 {success_count}/{len(dates_to_update)} 个日期")
                
            except Exception as e:
                print(f"      ❌ 处理细化指数 {tag} 失败: {e}")
                failed_dates_dict[f"{clean_sector_code}.{tag}"] = [f"处理失败: {e}"]
                continue
        
        print(f"    细化板块处理完成: 成功更新 {total_success_count} 个细化指数")
        return total_success_count, failed_dates_dict
        
    except Exception as e:
        print(f"    ❌ 处理细化板块 {sector_code} 失败: {e}")
        return 0, {}

def process_refined_sector_incremental(sector_code: str, sector_name: str, update_date: str) -> int:
    """
    【兼容版】处理单个申万板块的细化板块的增量更新 - 保持向后兼容
    
    Args:
        sector_code (str): 板块代码
        sector_name (str): 板块名称
        update_date (str): 更新日期
        
    Returns:
        int: 成功生成的细化指数数量
    """
    print(f"  处理细化板块: {sector_name} ({sector_code})")
    
    try:
        # 获取成分股
        all_stocks = get_sector_constituents(sector_code)
        if len(all_stocks) == 0:
            print(f"    ❌ 未找到板块 {sector_code} 的成分股")
            return 0
        
        # 使用前20只股票进行细化演示
        demo_stocks = all_stocks[:20]
        
        # 执行细化筛选
        xihua = StockXihua()
        stock_df = xihua.create_stock_dataframe(demo_stocks)
        
        if not stock_df.empty:
            xihua.calculate_quantile_categories(stock_df)
        else:
            print("    ❌ 无法创建股票数据框，使用模拟数据")
            # 使用模拟数据
            xihua.dsz = demo_stocks[16:]  # 模拟大市值 (后20%)
            xihua.xsz = demo_stocks[:4]   # 模拟小市值 (前20%)
            xihua.gbj = demo_stocks[10:14] + demo_stocks[17:]  # 模拟高价股
            xihua.dbj = demo_stocks[:6]   # 模拟低价股
            xihua.dg = list(set(xihua.dsz) & set(xihua.gbj))  # 模拟大高股
        
        # 循环计算所有细分板块的指数
        success_count = 0
        
        for tag, info in REFINEMENT_MAP.items():
            # 获取对应的股票列表
            sub_stock_list = getattr(xihua, info['attribute'])
            
            # 检查是否有足够的股票来计算指数
            if not sub_stock_list or len(sub_stock_list) < 3:
                continue
            
            try:
                # 生成指数代码和名称
                clean_sector_code = sector_code.replace('.SI', '')
                sub_index_code = f"{clean_sector_code}.{tag}"
                sub_index_name = f"{sector_name}-{info['name']}指数"
                
                # 1. 获取该细化指数昨天的信息
                last_day_info = get_index_last_day_info(sub_index_code, 'index_k_daily')
                if not last_day_info:
                    print(f"      ❌ 未找到历史数据，无法进行增量更新: {sub_index_code}。请先进行全量计算。")
                    continue
                
                last_trade_date, last_close_price = last_day_info
                print(f"      上一交易日: {last_trade_date}, 收盘价: {last_close_price:.2f}")

                # 检查是否需要更新：如果指数数据的最新日期 >= 更新日期，则无需更新
                if last_trade_date >= update_date:
                    print(f"      指数数据已是最新({last_trade_date})，无需更新。")
                    success_count += 1
                    continue

                # 2. 准备计算器，只需要获取两天的数据
                calculator = SectorIndexCalculator(
                    stock_list=sub_stock_list,
                    start_date=last_trade_date, # 开始日期是上一个交易日
                    end_date=update_date      # 结束日期是今天
                )
                
                # 3. 执行增量计算
                new_index_row = calculator.calculate_incremental(last_day_info)
                
                # 4. 保存新的单行数据到数据库
                save_incremental_index_data(new_index_row, sub_index_code, sub_index_name, 'index_k_daily', update_date)
                success_count += 1
                
            except Exception as e:
                print(f"    ❌ 计算失败 {tag}: {e}")
                continue
        
        return success_count
        
    except Exception as e:
        print(f"    ❌ 处理细化板块 {sector_code} 失败: {e}")
        return 0

def main_incremental_update_new():
    """
    【重构版】主要的增量更新工作流 - 按指数循环，每个指数处理其所有待更新日期
    """
    print("="*80)
    print("【重构版】增量更新所有板块（包括细化板块）指数的工作流")
    print("="*80)
    
    # 1. 获取所有申万板块
    print(f"\n>>> 步骤1：获取所有申万板块")
    all_sectors = get_all_sw_sectors()
    print(f"找到 {len(all_sectors)} 个申万板块")
    
    if len(all_sectors) == 0:
        print("❌ 未找到任何申万板块，退出")
        return {
            'standard_count': 0,
            'refined_count': 0,
            'total_count': 0,
            'failed_indices': {}
        }
    
    # 2. 按指数循环处理标准板块
    print(f"\n>>> 步骤2：处理所有标准申万板块的增量更新")
    total_standard_success = 0
    total_standard_dates = 0
    standard_failed_indices = {}
    
    for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
        print(f"\n[{i}/{len(all_sectors)}] 处理标准板块: {sector_name} ({sector_code})")
        
        try:
            success_count, failed_dates = process_single_standard_index_all_dates(sector_code, sector_name)
            if success_count > 0:
                total_standard_success += 1
                total_standard_dates += success_count
            
            if failed_dates:
                new_index_code = sector_code.replace('.SI', '.ZS')
                standard_failed_indices[new_index_code] = failed_dates
                
        except Exception as e:
            print(f"❌ 处理标准板块 {sector_code} 时发生严重错误: {e}")
            new_index_code = sector_code.replace('.SI', '.ZS')
            standard_failed_indices[new_index_code] = [f"严重错误: {e}"]
    
    print(f"\n标准板块增量更新完成: 成功处理 {total_standard_success}/{len(all_sectors)} 个板块")
    print(f"总计更新 {total_standard_dates} 个标准指数日期数据")
    
    # 3. 按指数循环处理细化板块
    print(f"\n>>> 步骤3：处理所有申万板块的细化板块的增量更新")
    total_refined_success = 0
    total_refined_indices = 0
    refined_failed_indices = {}
    
    for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
        print(f"\n[{i}/{len(all_sectors)}] 处理细化板块: {sector_name} ({sector_code})")
        
        try:
            success_count, failed_dates_dict = process_single_refined_index_all_dates(sector_code, sector_name)
            if success_count > 0:
                total_refined_success += 1
                total_refined_indices += success_count
            
            if failed_dates_dict:
                refined_failed_indices.update(failed_dates_dict)
                
        except Exception as e:
            print(f"❌ 处理细化板块 {sector_code} 时发生严重错误: {e}")
            # 为该板块的所有可能的细化指数记录错误
            clean_sector_code = sector_code.replace('.SI', '')
            for tag in REFINEMENT_MAP.keys():
                sub_index_code = f"{clean_sector_code}.{tag}"
                refined_failed_indices[sub_index_code] = [f"严重错误: {e}"]
    
    print(f"\n细化板块增量更新完成: 成功处理 {total_refined_success}/{len(all_sectors)} 个板块")
    print(f"总计生成 {total_refined_indices} 个细化指数")
    
    # 4. 最终汇总
    print(f"\n{'='*80}")
    print("所有指数的增量更新完成！")
    print(f"{'='*80}")
    print(f"标准板块指数: 成功处理 {total_standard_success} 个板块")
    print(f"细化板块指数: 成功处理 {total_refined_success} 个板块，生成 {total_refined_indices} 个细化指数")
    print(f"总计指数更新: {total_standard_dates + total_refined_indices} 个指数日期数据")
    
    # 报告失败的指数
    all_failed_indices = {}
    all_failed_indices.update(standard_failed_indices)
    all_failed_indices.update(refined_failed_indices)
    
    if all_failed_indices:
        print("\n" + "="*80)
        print("⚠️ 以下指数在处理过程中失败：")
        for index_code, failed_info in all_failed_indices.items():
            print(f"  指数: {index_code}")
            if isinstance(failed_info, list) and len(failed_info) > 0:
                if isinstance(failed_info[0], str) and "严重错误" in failed_info[0]:
                    print(f"    - {failed_info[0]}")
                else:
                    print(f"    - 失败日期: {', '.join(failed_info)}")
            else:
                print(f"    - 未知错误")
        print("="*80)
    
    return {
        'standard_count': total_standard_success,
        'refined_count': total_refined_indices,
        'total_count': total_standard_dates + total_refined_indices,
        'failed_indices': all_failed_indices
    }

def main_incremental_update():
    """
    【兼容版】主要的增量更新工作流 - 支持多日增量更新，保持向后兼容
    """
    print("="*80)
    print("增量更新所有板块（包括细化板块）指数的工作流")
    print("="*80)
    
    # 1. 确定需要更新的日期列表
    dates_to_update = determine_update_dates()
    
    if not dates_to_update:
        print("ℹ️ 无需更新，所有数据已是最新")
        return {
            'update_dates': [],
            'standard_count': 0,
            'refined_count': 0,
            'total_count': 0
        }
    
    # 2. 获取所有申万板块
    print(f"\n>>> 步骤2：获取所有申万板块")
    all_sectors = get_all_sw_sectors()
    print(f"找到 {len(all_sectors)} 个申万板块")
    
    if len(all_sectors) == 0:
        print("❌ 未找到任何申万板块，退出")
        return
    
    # 3. 循环处理每一天的增量更新
    total_standard_success = 0
    total_refined_success = 0
    total_refined_indices = 0
    failed_sectors = {}  # 用于记录失败的板块和原因
    
    for day_idx, update_date in enumerate(dates_to_update, 1):
        print(f"\n{'='*20} 正在处理日期: {update_date} ({day_idx}/{len(dates_to_update)}) {'='*20}")
        
        # 3.1 处理所有标准申万板块的增量更新
        print(f"\n>>> 步骤3.{day_idx}：处理所有标准申万板块的增量更新")
        standard_success_count = 0
        
        for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
            print(f"\n[{i}/{len(all_sectors)}] 处理: {sector_name} ({sector_code})")
            
            try:
                # 【增加异常捕获】
                if process_standard_sector_incremental(sector_code, sector_name, update_date):
                    standard_success_count += 1
            except Exception as e:
                print(f"❌ 处理标准板块 {sector_code} 时发生严重错误: {e}")
                if update_date not in failed_sectors:
                    failed_sectors[update_date] = []
                failed_sectors[update_date].append(f"标准板块 {sector_code} - {e}")
        
        print(f"\n标准板块增量更新完成: 成功 {standard_success_count}/{len(all_sectors)}")
        total_standard_success += standard_success_count
        
        # 3.2 处理所有申万板块的细化板块的增量更新
        print(f"\n>>> 步骤4.{day_idx}：处理所有申万板块的细化板块的增量更新")
        refined_success_count = 0
        day_refined_indices = 0
        
        for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
            print(f"\n[{i}/{len(all_sectors)}] 处理细化板块: {sector_name} ({sector_code})")
            
            try:
                # 【增加异常捕获】
                count = process_refined_sector_incremental(sector_code, sector_name, update_date)
                if count > 0:
                    refined_success_count += 1
                    day_refined_indices += count
            except Exception as e:
                print(f"❌ 处理细化板块 {sector_code} 时发生严重错误: {e}")
                if update_date not in failed_sectors:
                    failed_sectors[update_date] = []
                failed_sectors[update_date].append(f"细化板块 {sector_code} - {e}")
        
        print(f"\n细化板块增量更新完成: 成功 {refined_success_count}/{len(all_sectors)} 个板块")
        print(f"当日生成 {day_refined_indices} 个细化指数增量")
        total_refined_success += refined_success_count
        total_refined_indices += day_refined_indices
    
    # 4. 最终汇总
    print(f"\n{'='*80}")
    print("所有日期的增量更新完成！")
    print(f"{'='*80}")
    print(f"更新日期范围: {dates_to_update[0]} 到 {dates_to_update[-1]} (共 {len(dates_to_update)} 天)")
    print(f"标准板块指数增量: {total_standard_success} 个")
    print(f"细化板块指数增量: {total_refined_indices} 个")
    print(f"总计指数增量: {total_standard_success + total_refined_indices} 个")
    
    # 报告失败的板块
    if failed_sectors:
        print("\n" + "="*80)
        print("⚠️ 以下板块在处理过程中失败：")
        for date, errors in failed_sectors.items():
            print(f"  日期: {date}")
            for error in errors:
                print(f"    - {error}")
        print("="*80)
    
    return {
        'update_dates': dates_to_update,
        'standard_count': total_standard_success,
        'refined_count': total_refined_indices,
        'total_count': total_standard_success + total_refined_indices,
        'failed_sectors': failed_sectors
    }

if __name__ == "__main__":
    # 使用重构版的主工作流（推荐）
    print("使用重构版增量更新工作流...")
    result = main_incremental_update_new()
    
    # 如果日线更新成功，自动更新周线和月线数据
    if result and result.get('total_count', 0) > 0:
        print(f"\n>>> 步骤4：同步更新周线和月线数据")
        try:
            # 暂时注释掉，需要确认正确的导入路径
            # from incremental_weekly_monthly_updater import main_incremental_weekly_monthly_update
            # 使用当前日期进行周线月线更新
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            # 暂时注释掉，需要确认正确的导入路径
            # main_incremental_weekly_monthly_update(today)
        except ImportError as e:
            print(f"❌ 导入周线月线更新模块失败: {e}")
            print("请确保 incremental_weekly_monthly_updater.py 文件存在")
        except Exception as e:
            print(f"❌ 周线月线更新失败: {e}")
    else:
        print("⚠️ 日线更新未成功，跳过周线月线更新")
    
    # 如果需要使用旧版本的兼容模式，可以取消下面的注释
    # print("\n" + "="*50)
    # print("如需使用兼容版，请取消注释并运行：")
    # print("result = main_incremental_update()")
    # print("="*50)
