#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的申万板块指数计算工作流
1. 处理所有标准申万板块，生成指数并存入index_k_daily表
2. 处理所有申万板块的细化板块，生成指数并存入index_k_daily表
3. 处理股票分类指数（国企、B股、H股、老股、大高、高价、低价、次新、超强）
"""

# 移除 sqlite3 导入，使用 DatabaseManager
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入v2项目的模块
from data_management.sector_index_calculator import ActiveStockScreener, SectorIndexCalculator
from data_management.stock_category_mapper import StockCategoryIndexMapper
from core.utils.stock_filter import StockXihua
from data_management.data_processor import get_last_trade_date



# 定义细分类型的映射关系
REFINEMENT_MAP = {
    'DSZ': {'name': '大市值', 'attribute': 'dsz'},
    'XSZ': {'name': '小市值', 'attribute': 'xsz'},
    'GBJ': {'name': '高价股', 'attribute': 'gbj'},
    'DBJ': {'name': '低价股', 'attribute': 'dbj'},
    'DG':  {'name': '大高股', 'attribute': 'dg'},
    'GQ':  {'name': '国企股', 'attribute': 'gq'},
    'CQ': {'name': '超强股', 'attribute': 'cq'}
}

def get_all_sw_sectors() -> List[Tuple[str, str]]:
    """
    获取所有申万板块的代码和名称（包括L1、L2、L3级别）
    
    Returns:
        List[Tuple[str, str]]: [(板块代码, 板块名称), ...]
    """
    try:
        from data_management.database_manager import DatabaseManager
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
            l1_sectors = df[l1_mask][['l1_code', 'l1_name']].drop_duplicates()
            for _, row in l1_sectors.iterrows():
                sectors.append((row['l1_code'], row['l1_name']))
            
            # 收集L2级别板块
            l2_mask = df['l2_code'].notna() & df['l2_name'].notna()
            l2_sectors = df[l2_mask][['l2_code', 'l2_name']].drop_duplicates()
            for _, row in l2_sectors.iterrows():
                sectors.append((row['l2_code'], row['l2_name']))
            
            # 收集L3级别板块
            l3_mask = df['l3_code'].notna() & df['l3_name'].notna()
            l3_sectors = df[l3_mask][['l3_code', 'l3_name']].drop_duplicates()
            for _, row in l3_sectors.iterrows():
                sectors.append((row['l3_code'], row['l3_name']))
            
            print(f"找到申万板块: L1级别 {len(l1_sectors)} 个, L2级别 {len(l2_sectors)} 个, L3级别 {len(l3_sectors)} 个")
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
        from data_management.database_manager import DatabaseManager
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

def create_index_k_daily_table():
    """
    创建index_k_daily表（如果不存在）
    """
    try:
        from data_management.database_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS index_k_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            index_code TEXT NOT NULL,
            index_name TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            UNIQUE(index_code, trade_date)
        )
        """
        
        success = db_manager.execute_ddl(create_table_sql)
        if success:
            print("✅ index_k_daily表创建成功或已存在")
        else:
            print("❌ 创建index_k_daily表失败")
        
    except Exception as e:
        print(f"❌ 创建index_k_daily表失败: {e}")

# def create_index_k_daily_table():
#     """
#     创建index_k_daily表（如果不存在）
#     """
#     try:
#         db_path = 'databases/quant_system.db'
#         conn = sqlite3.connect(db_path)
        
#         create_table_sql = """
#         CREATE TABLE IF NOT EXISTS index_k_daily (
#             id INTEGER PRIMARY KEY AUTOINCREMENT,
#             index_code TEXT NOT NULL,
#             index_name TEXT NOT NULL,
#             trade_date TEXT NOT NULL,
#             open REAL,
#             high REAL,
#             low REAL,
#             close REAL,
#             volume INTEGER,
#             UNIQUE(index_code, trade_date)
#         )
#         """
        
#         conn.execute(create_table_sql)
#         conn.commit()
#         conn.close()
#         print("✅ index_k_daily表创建成功或已存在")
        
#     except Exception as e:
#         print(f"❌ 创建index_k_daily表失败: {e}")

def save_index_data_to_db(index_data: pd.DataFrame, index_code: str, index_name: str, table_name: str):
    """
    将指数数据保存到数据库（使用批量插入优化性能）
    
    Args:
        index_data (pd.DataFrame): 指数数据
        index_code (str): 指数代码
        index_name (str): 指数名称
        table_name (str): 表名
    """
    try:
        from data_management.database_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # 准备批量数据
        batch_data = []
        for trade_date, row in index_data.iterrows():
            # 确保trade_date是datetime类型
            if hasattr(trade_date, 'strftime'):
                date_str = trade_date.strftime('%Y-%m-%d')
            else:
                date_str = str(trade_date)
            
            batch_data.append({
                'index_code': index_code,
                'index_name': index_name,
                'trade_date': date_str,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': int(row['volume'])
            })
        
        # 转换为DataFrame进行批量插入
        df_to_insert = pd.DataFrame(batch_data)
        
        # 使用批量插入方法
        success = db_manager.batch_insert_dataframe(df_to_insert, table_name)
        
        if success:
            print(f"✅ 成功批量保存 {len(batch_data)} 条记录到 {table_name} 表")
        else:
            print(f"❌ 批量保存数据到 {table_name} 表失败")
        
    except Exception as e:
        print(f"❌ 保存数据到 {table_name} 表失败: {e}")

def process_standard_sector(sector_code: str, sector_name: str, start_date: str, end_date: str) -> bool:
    """
    处理单个标准申万板块
    
    Args:
        sector_code (str): 板块代码
        sector_name (str): 板块名称
        start_date (str): 开始日期
        end_date (str): 结束日期
        
    Returns:
        bool: 是否成功
    """
    print(f"\n--- 处理标准板块: {sector_name} ({sector_code}) ---")
    
    try:
        # 获取成分股
        all_stocks = get_sector_constituents(sector_code)
        if len(all_stocks) == 0:
            print(f"❌ 未找到板块 {sector_code} 的成分股")
            return False
        
        print(f"成分股数量: {len(all_stocks)}")
        
        # 使用前20只股票进行计算（避免数据量过大）
        demo_stocks = all_stocks[:20]
        print(f"使用前{len(demo_stocks)}只股票进行指数计算")
        
        # 计算指数
        calculator = SectorIndexCalculator(
            stock_list=demo_stocks,
            start_date=start_date,
            end_date=end_date
        )
        
        base_date = '2020-02-03'
        index_df = calculator.calculate_index(base_date, base_value=1000)
        
        # 生成指数代码和名称
        new_index_code = sector_code.replace('.SI', '.ZS')
        index_name = f"{sector_name}指数"
        
        print(f"指数代码: {new_index_code}")
        print(f"指数名称: {index_name}")
        print(f"数据形状: {index_df.shape}")
        print(f"最新收盘价: {index_df['close'].iloc[-1]:.2f}")
        
        # 保存到数据库
        save_index_data_to_db(index_df, new_index_code, index_name, 'index_k_daily')
        
        return True
        
    except Exception as e:
        print(f"❌ 处理标准板块 {sector_code} 失败: {e}")
        return False

def process_refined_sector(sector_code: str, sector_name: str, start_date: str, end_date: str) -> int:
    """
    处理单个申万板块的细化板块
    
    Args:
        sector_code (str): 板块代码
        sector_name (str): 板块名称
        start_date (str): 开始日期
        end_date (str): 结束日期
        
    Returns:
        int: 成功生成的细化指数数量
    """
    print(f"\n--- 处理细化板块: {sector_name} ({sector_code}) ---")
    
    try:
        # 获取成分股
        all_stocks = get_sector_constituents(sector_code)
        if len(all_stocks) == 0:
            print(f"❌ 未找到板块 {sector_code} 的成分股")
            return 0
        
        print(f"成分股数量: {len(all_stocks)}")
        
        # 使用前20只股票进行细化演示
        demo_stocks = all_stocks[:20]
        print(f"使用前{len(demo_stocks)}只股票进行细化演示")
        
        # 执行细化筛选
        xihua = StockXihua()
        stock_df = xihua.create_stock_dataframe(demo_stocks)
        
        if not stock_df.empty:
            xihua.calculate_quantile_categories(stock_df)
            print(f"细化结果: 大市值{len(xihua.dsz)}只, 小市值{len(xihua.xsz)}只, 高价股{len(xihua.gbj)}只, 低价股{len(xihua.dbj)}只, 大高股{len(xihua.dg)}只")
        else:
            print("❌ 无法创建股票数据框，使用模拟数据")
            # 使用模拟数据
            xihua.dsz = demo_stocks[16:]  # 模拟大市值 (后20%)
            xihua.xsz = demo_stocks[:4]   # 模拟小市值 (前20%)
            xihua.gbj = demo_stocks[10:14] + demo_stocks[17:]  # 模拟高价股
            xihua.dbj = demo_stocks[:6]   # 模拟低价股
            xihua.dg = list(set(xihua.dsz) & set(xihua.gbj))  # 模拟大高股
        
        # 循环计算所有细分板块的指数
        success_count = 0
        
        for tag, info in REFINEMENT_MAP.items():
            print(f"\n  处理细分类型: {info['name']} ({tag})")
            
            # 获取对应的股票列表
            sub_stock_list = getattr(xihua, info['attribute'])
            
            # 检查是否有足够的股票来计算指数
            if not sub_stock_list or len(sub_stock_list) < 3:
                print(f"    成分股数量不足({len(sub_stock_list) if sub_stock_list else 0}只)，跳过计算")
                continue
            
            print(f"    成分股数量: {len(sub_stock_list)}")
            
            try:
                # 计算指数
                calculator = SectorIndexCalculator(
                    stock_list=sub_stock_list,
                    start_date=start_date,
                    end_date=end_date
                )
                
                base_date = '2020-02-03'
                index_df = calculator.calculate_index(base_date, base_value=1000)
                
                # 生成指数代码和名称
                clean_sector_code = sector_code.replace('.SI', '')
                sub_index_code = f"{clean_sector_code}.{tag}"
                sub_index_name = f"{sector_name}-{info['name']}指数"
                
                print(f"    指数代码: {sub_index_code}")
                print(f"    指数名称: {sub_index_name}")
                print(f"    最新收盘价: {index_df['close'].iloc[-1]:.2f}")
                
                # 保存到数据库
                save_index_data_to_db(index_df, sub_index_code, sub_index_name, 'index_k_daily')
                success_count += 1
                
            except Exception as e:
                print(f"    ❌ 计算失败: {e}")
                continue
        
        return success_count
        
    except Exception as e:
        print(f"❌ 处理细化板块 {sector_code} 失败: {e}")
        return 0

def main_complete_workflow():
    """
    完整的申万板块指数计算工作流
    包含：
    1. 处理所有标准申万板块，生成指数并存入index_k_daily表
    2. 处理所有申万板块的细化板块，生成指数并存入index_k_daily表
    3. 处理股票分类指数（国企、B股、H股、老股、大高、高价、低价、次新、超强）
    """
    print("="*80)
    print("完整的申万板块指数计算工作流")
    print("="*80)
    
    # 定义参数
    start_date = '2020-02-03'
    end_date = get_last_trade_date()  # 动态获取最后一个交易日
    
    # 1. 创建数据库表
    print("\n>>> 步骤1：创建数据库表")
    create_index_k_daily_table()
    
    # 2. 获取所有申万板块
    print("\n>>> 步骤2：获取所有申万板块")
    all_sectors = get_all_sw_sectors()
    print(f"找到 {len(all_sectors)} 个申万板块")
    
    if len(all_sectors) == 0:
        print("❌ 未找到任何申万板块，退出")
        return
    
    # 3. 处理所有标准申万板块
    print(f"\n>>> 步骤3：处理所有标准申万板块")
    standard_success_count = 0
    
    for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
        print(f"\n[{i}/{len(all_sectors)}] 处理标准板块: {sector_name} ({sector_code})")
        
        if process_standard_sector(sector_code, sector_name, start_date, end_date):
            standard_success_count += 1
    
    print(f"\n标准板块处理完成: 成功 {standard_success_count}/{len(all_sectors)}")
    
    # 4. 处理所有申万板块的细化板块
    print(f"\n>>> 步骤4：处理所有申万板块的细化板块")
    refined_success_count = 0
    total_refined_indices = 0
    
    for i, (sector_code, sector_name) in enumerate(all_sectors, 1):
        print(f"\n[{i}/{len(all_sectors)}] 处理细化板块: {sector_name} ({sector_code})")
        
        count = process_refined_sector(sector_code, sector_name, start_date, end_date)
        if count > 0:
            refined_success_count += 1
            total_refined_indices += count
    
    print(f"\n细化板块处理完成: 成功 {refined_success_count}/{len(all_sectors)} 个板块")
    print(f"总计生成 {total_refined_indices} 个细化指数")
    
    # 5. 处理股票分类指数
    print(f"\n>>> 步骤5：处理股票分类指数")
    category_result = process_stock_category_indices(
        start_date=start_date,
        end_date=end_date,
        replace_existing=True
    )
    
    # 6. 最终汇总
    print(f"\n{'='*80}")
    print("所有处理完成！")
    print(f"{'='*80}")
    print(f"标准板块指数: {standard_success_count} 个")
    print(f"细化板块指数: {total_refined_indices} 个")
    print(f"股票分类指数: {category_result.get('category_count', 0)} 个")
    print(f"总计指数: {standard_success_count + total_refined_indices + category_result.get('category_count', 0)} 个")
    
    return {
        'standard_count': standard_success_count,
        'refined_count': total_refined_indices,
        'category_count': category_result.get('category_count', 0),
        'total_count': standard_success_count + total_refined_indices + category_result.get('category_count', 0),
        'category_result': category_result
    }


def process_stock_category_indices(start_date=None, end_date=None, replace_existing=True):
    """
    处理股票分类指数
    
    Args:
        start_date: 开始日期，格式'YYYY-MM-DD'
        end_date: 结束日期，格式'YYYY-MM-DD'
        replace_existing: 是否替换已存在的数据
        
    Returns:
        dict: 处理结果统计
    """
    print("\n" + "="*60)
    print("📊 处理股票分类指数")
    print("="*60)
    
    try:
        # 创建股票分类指数映射器
        mapper = StockCategoryIndexMapper()
        
        # 显示分类统计信息
        print("\n1. 获取分类统计信息:")
        summary = mapper.get_all_category_summary()
        print()
        
        # 计算所有分类指数
        print("2. 计算股票分类指数:")
        all_index_data = mapper.calculate_all_category_indices(
            start_date=start_date,
            end_date=end_date,
            save_to_db=True,
            replace_existing=replace_existing
        )
        
        # 显示结果统计
        total_records = sum(len(data) for data in all_index_data.values())
        print(f"\n✅ 股票分类指数处理完成！")
        print(f"   共处理{len(all_index_data)}个分类指数")
        print(f"   总记录数: {total_records}")
        
        # 显示指数表现摘要
        if all_index_data:
            print(f"\n📈 指数表现摘要:")
            for category, index_info in mapper.index_mapping.items():
                if category in all_index_data:
                    latest_data = all_index_data[category].iloc[-1] if not all_index_data[category].empty else None
                    if latest_data is not None:
                        print(f"   {index_info['index_name']}: 收盘价={latest_data['close']:.2f}, "
                              f"日期={latest_data['trade_date']}")
        
        return {
            'category_count': len(all_index_data),
            'total_records': total_records,
            'success': True
        }
        
    except Exception as e:
        print(f"❌ 处理股票分类指数失败: {e}")
        return {
            'category_count': 0,
            'total_records': 0,
            'success': False,
            'error': str(e)
        }



def main_complete_workflow_with_categories(include_categories=True, start_date=None, end_date=None, replace_existing=True):
    """
    完整的指数计算工作流（包含股票分类指数）
    
    Args:
        include_categories: 是否包含股票分类指数
        start_date: 开始日期，格式'YYYY-MM-DD'
        end_date: 结束日期，格式'YYYY-MM-DD'
        replace_existing: 是否替换已存在的数据
        
    Returns:
        dict: 完整的处理结果
    """
    print("🚀 开始完整的指数计算工作流...")
    print(f"   包含股票分类指数: {'是' if include_categories else '否'}")
    if start_date and end_date:
        print(f"   日期范围: {start_date} 至 {end_date}")
    print(f"   替换已存在数据: {'是' if replace_existing else '否'}")
    print()
    
    # 1. 处理申万板块指数
    print("📊 第一步：处理申万板块指数")
    sw_result = main_complete_workflow()
    
    # 2. 处理股票分类指数（可选）
    category_result = None
    if include_categories:
        print("\n📊 第二步：处理股票分类指数")
        category_result = process_stock_category_indices(
            start_date=start_date,
            end_date=end_date,
            replace_existing=replace_existing
        )
    
    # 3. 汇总结果
    print("\n" + "="*80)
    print("📋 完整工作流结果汇总")
    print("="*80)
    print(f"申万标准板块指数: {sw_result['standard_count']} 个")
    print(f"申万细化板块指数: {sw_result['refined_count']} 个")
    print(f"申万板块指数总计: {sw_result['total_count']} 个")
    
    if category_result:
        print(f"股票分类指数: {category_result['category_count']} 个")
        print(f"股票分类指数记录: {category_result['total_records']} 条")
        print(f"所有指数总计: {sw_result['total_count'] + category_result['category_count']} 个")
    
    return {
        'sw_result': sw_result,
        'category_result': category_result,
        'total_indices': sw_result['total_count'] + (category_result['category_count'] if category_result else 0)
    }


if __name__ == "__main__":
    print("="*80)
    print("📊 完整的指数计算工作流")
    print("="*80)
    print("包含以下功能：")
    print("1. 处理所有标准申万板块，生成指数并存入index_k_daily表")
    print("2. 处理所有申万板块的细化板块，生成指数并存入index_k_daily表")
    print("3. 处理股票分类指数（国企、B股、H股、老股、大高、高价、低价、次新、超强）")
    print()
    
    # 设置计算参数
    start_date = '2020-02-03'
    end_date = get_last_trade_date()  # 动态获取最后一个交易日
    
    print(f"📅 计算参数:")
    print(f"   开始日期: {start_date}")
    print(f"   结束日期: {end_date}")
    print(f"   包含股票分类指数: 是")
    print(f"   替换已存在数据: 是")
    print()
    
    # 询问用户确认
    print("⚠️ 警告：此操作将计算大量指数数据！")
    confirm = input("是否继续？(y/N): ").strip().lower()
    
    if confirm == 'y':
        # 执行完整工作流
        result = main_complete_workflow_with_categories(
            include_categories=True,
            start_date=start_date,
            end_date=end_date,
            replace_existing=True
        )
        print(f"\n✅ 完整工作流执行完成！")
    else:
        print("❌ 操作已取消")



