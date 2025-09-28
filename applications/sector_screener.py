#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块筛选器 (quant_v2 版本)

功能：
1. 筛选符合条件的板块
2. 计算申万板块指数涨幅
3. 多时间段分析
"""
import pandas as pd
import numpy as np
import glob
import sqlite3
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)




def get_qualified_sectors(excel_path=None):
    if excel_path is None:
        # 使用相对路径指向 databases 目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(current_dir, '..', 'databases', 'sector_market_cap_analysis.xlsx')
        excel_path = os.path.abspath(excel_path)
    """
    筛选符合条件的板块，返回index_code和index_name
    
    筛选条件：
    1. (国企 > 0.4) & (超强 > 0.7)
    2. 或者 超超强 > 0.6
    3. 或者 大高 > 0.8
    
    Args:
        excel_path (str): Excel文件路径
        
    Returns:
        tuple: (index_codes, index_names) 两个列表
               - index_codes: 符合条件的index_code列表，后缀已从.SI改为.ZS
               - index_names: 对应的板块名称列表
    """
    # 读取Excel文件
    df = pd.read_excel(excel_path)
    
    # 筛选符合条件的板块
    # 修复原来的逻辑错误：使用 | 而不是 or，并正确使用括号
    df_zhuli = df[
        ((df['国企'] > 0.4) & (df['超强'] > 0.7)) | 
        (df['超超强'] > 0.6) | 
        (df['大高'] > 0.8)
    ]
    
    # 获取index_code并将后缀从.SI改为.ZS
    index_codes = df_zhuli['index_code'].str.replace('.SI', '.ZS', regex=False).tolist()
    
    # 获取对应的板块名称作为index_name
    index_names = df_zhuli['板块名称'].tolist()
    
    return index_codes, index_names

def get_qualified_sector_codes(excel_path=None):
    if excel_path is None:
        # 使用相对路径指向 databases 目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(current_dir, '..', 'databases', 'sector_market_cap_analysis.xlsx')
        excel_path = os.path.abspath(excel_path)
    """
    筛选符合条件的板块，只返回index_code列表
    
    Args:
        excel_path (str): Excel文件路径
        
    Returns:
        list: 符合条件的index_code列表，后缀已从.SI改为.ZS
    """
    index_codes, _ = get_qualified_sectors(excel_path)
    return index_codes

# #预设近期消息博弈基本面的板块：方便外部调用。
# jinqi_xiaoxi_bankuai = []
# #预设长线强趋势板块。调用数据表。
# cxqqs_bankuai = []

# #主力强的，直接计算比例（在板块评分里面有了）
# zhuli_bankuai = get_qualified_sector_codes()  # 只获取index_codes

#历史长线循环居前：直接外部调用函数
# changxian_bankuai = get_changxian_zf_bankuai()

#属于历史长线循环居前的板块
#databases\xunhuan_changxian_zf\*.csv
def get_changxian_zf_bankuai():
    """
    读取databases/xunhuan_changxian_zf/文件夹中的所有CSV文件，获取index_code列表
    返回所有长线涨幅居前板块的index_code
    """
    import os
    import glob
    
    # 读取文件夹下的所有CSV文件
    csv_folder = r"databases\xunhuan_changxian_zf"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return []
    
    all_index_codes = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            if 'index_code' in df.columns:
                # 获取该文件中的所有index_code
                index_codes = df['index_code'].tolist()
                all_index_codes.extend(index_codes)
        except Exception as e:
            continue
    
    # 去重并返回
    unique_index_codes = list(set(all_index_codes))
    return unique_index_codes






#属于最近3个波段循环居前的板块
#databases\xunhuan_boduan_zf\*.csv
def get_boduan_zf_bankuai():
    """
    读取databases/xunhuan_boduan_zf/文件夹中的所有CSV文件，获取index_code列表
    返回所有长线涨幅居前板块的index_code
    """
    import os
    import glob
    
    # 读取文件夹下的所有CSV文件
    csv_folder = r"databases\xunhuan_boduan_zf"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return []
    
    all_index_codes = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            if 'index_code' in df.columns:
                # 获取该文件中的所有index_code
                index_codes = df['index_code'].tolist()
                all_index_codes.extend(index_codes)
        except Exception as e:
            continue
    
    # 去重并返回
    unique_index_codes = list(set(all_index_codes))
    return unique_index_codes

#属于最近3个波段循环居前的板块
#databases\xunhuan_boduan_bias\*.csv
def get_boduan_bias_bankuai():
    """
    读取databases/xunhuan_boduan_bias/文件夹中的所有CSV文件，获取index_code列表
    返回所有长线涨幅居前板块的index_code
    """
    import os
    import glob
    
    # 读取文件夹下的所有CSV文件
    csv_folder = r"databases\xunhuan_boduan_bias"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return []
    
    all_index_codes = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            if 'index_code' in df.columns:
                # 获取该文件中的所有index_code
                index_codes = df['index_code'].tolist()
                all_index_codes.extend(index_codes)
        except Exception as e:
            continue
    
    # 去重并返回
    unique_index_codes = list(set(all_index_codes))
    return unique_index_codes


def get_db_connection():
    """获取数据库连接"""
    db_path = 'databases/quant_system.db'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")
    return sqlite3.connect(db_path)


def get_sw_index_codes():
    """
    获取申万一级二级板块指数代码
    从sw数据表获取Level==1,==2的指数代码，并将后缀.SI改为.ZS
    """
    conn = get_db_connection()
    
    # 查询申万一级二级指数代码
    query = """
    SELECT index_code, industry_name, level
    FROM sw 
    WHERE level IN ('L1', 'L2')
    ORDER BY level, industry_name
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("⚠️ 未找到申万一级二级指数数据")
        return pd.DataFrame()
    
    # 将后缀.SI改为.ZS
    df['index_code_zs'] = df['index_code'].str.replace('.SI', '.ZS')
    
    print(f"✅ 获取到申万指数数据: {len(df)} 条")
    print(f"📊 一级指数: {len(df[df['level'] == 'L1'])} 个")
    print(f"📊 二级指数: {len(df[df['level'] == 'L2'])} 个")
    
    return df


def get_index_price_data(index_codes, start_date, end_date):
    """
    从index_k_daily表获取申万指数行情数据
    
    Args:
        index_codes: 指数代码列表
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    
    Returns:
        pd.DataFrame: 指数行情数据
    """
    from data_management.database_manager import DatabaseManager
    db_manager = DatabaseManager()
    
    # 构建查询条件
    index_codes_str = "', '".join(index_codes)
    
    query = f"""
    SELECT index_code, trade_date, close
    FROM index_k_daily 
    WHERE index_code IN ('{index_codes_str}')
      AND trade_date >= '{start_date}'
      AND trade_date <= '{end_date}'
    ORDER BY index_code, trade_date
    """
    
    df = db_manager.execute_query(query)
    
    if df.empty:
        print("⚠️ 未找到指数行情数据")
        return pd.DataFrame()
    
    print(f"✅ 获取到指数行情数据: {len(df)} 条记录")
    return df


def calculate_index_returns(price_data, start_date, end_date):
    """
    计算板块指数涨幅
    
    Args:
        price_data: 指数行情数据
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        pd.DataFrame: 包含涨幅计算结果的DataFrame
    """
    if price_data.empty:
        return pd.DataFrame()
    
    # 转换为日期格式
    price_data['trade_date'] = pd.to_datetime(price_data['trade_date'])
    
    results = []
    
    for index_code in price_data['index_code'].unique():
        index_data = price_data[price_data['index_code'] == index_code].copy()
        index_data = index_data.sort_values(by='trade_date')
        
        if len(index_data) < 2:
            continue
        
        # 获取开始和结束价格
        start_price = index_data.iloc[0]['close']
        end_price = index_data.iloc[-1]['close']
        
        # 计算涨幅
        if start_price and start_price > 0:
            return_rate = (end_price - start_price) / start_price * 100
        else:
            return_rate = 0
        
        results.append({
            'index_code': index_code,
            'start_date': start_date,
            'end_date': end_date,
            'start_price': start_price,
            'end_price': end_price,
            'return_rate': return_rate,
            'data_points': len(index_data)
        })
    
    return pd.DataFrame(results)


def calculate_sw_sector_returns(start_date, end_date):
    """
    计算申万一级二级板块指数涨幅的主函数
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    
    Returns:
        pd.DataFrame: 包含涨幅计算结果的DataFrame
    """
    print(f"🚀 开始计算申万板块指数涨幅")
    print(f"📅 时间范围: {start_date} 至 {end_date}")
    
    # 1. 获取申万指数代码
    sw_codes = get_sw_index_codes()
    if sw_codes.empty:
        return pd.DataFrame()
    
    # 2. 获取指数行情数据
    index_codes_zs = sw_codes['index_code_zs'].tolist()
    price_data = get_index_price_data(index_codes_zs, start_date, end_date)
    
    if price_data.empty:
        return pd.DataFrame()
    
    # 3. 计算涨幅
    returns = calculate_index_returns(price_data, start_date, end_date)
    
    if returns.empty:
        return pd.DataFrame()
    
    # 4. 合并指数信息
    result = returns.merge(
        sw_codes[['index_code_zs', 'industry_name', 'level']], 
        left_on='index_code', 
        right_on='index_code_zs', 
        how='left'
    )
    
    # 5. 排序和格式化
    result = result.sort_values('return_rate', ascending=False)
    result['return_rate'] = result['return_rate'].round(2)
    
    print(f"✅ 计算完成，共 {len(result)} 个板块")
    
    return result


def calculate_sw_sector_returns_with_bias(start_date, end_date):
    """
    计算申万一级二级板块指数涨幅的主函数（增加120BIAS字段）
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    
    Returns:
        pd.DataFrame: 包含涨幅计算结果的DataFrame，增加120BIAS字段
    """
    print(f"🚀 开始计算申万板块指数涨幅（含120BIAS）")
    print(f"📅 时间范围: {start_date} 至 {end_date}")
    
    # 1. 获取申万指数代码
    sw_codes = get_sw_index_codes()
    if sw_codes.empty:
        return pd.DataFrame()
    
    # 2. 获取指数行情数据（需要更多历史数据来计算120BIAS）
    # 为了计算120BIAS，需要获取end_date前至少120个交易日的数据
    index_codes_zs = sw_codes['index_code_zs'].tolist()
    
    # 扩展查询范围，获取足够的历史数据
    extended_start_date = pd.to_datetime(end_date) - pd.Timedelta(days=200)  # 多获取一些数据确保有120个交易日
    extended_start_date = extended_start_date.strftime('%Y-%m-%d')
    
    price_data = get_index_price_data(index_codes_zs, extended_start_date, end_date)
    
    if price_data.empty:
        return pd.DataFrame()
    
    # 3. 计算涨幅
    returns = calculate_index_returns(price_data, start_date, end_date)
    
    if returns.empty:
        return pd.DataFrame()
    
    # 4. 计算120BIAS
    bias_results = []
    
    for index_code in price_data['index_code'].unique():
        index_data = price_data[price_data['index_code'] == index_code].copy()
        index_data = index_data.sort_values(by='trade_date')
        
        if len(index_data) < 120:  # 确保有足够的数据计算120BIAS
            continue
        
        # 获取end_date当天的收盘价
        end_date_data = index_data[index_data['trade_date'] == end_date]
        if end_date_data.empty:
            continue
        
        end_price = end_date_data.iloc[0]['close']
        
        # 计算120日移动平均
        if len(index_data) >= 120:
            # 获取最近120个交易日的数据
            recent_120_data = index_data.tail(120)
            ma_120 = recent_120_data['close'].mean()
            
            # 计算120BIAS
            bias_120 = (end_price - ma_120) / ma_120 * 100 if ma_120 > 0 else 0
            
            bias_results.append({
                'index_code': index_code,
                '120BIAS': round(bias_120, 2)
            })
    
    # 5. 合并涨幅和BIAS数据
    bias_df = pd.DataFrame(bias_results)
    if bias_df.empty:
        print("⚠️ 未计算出120BIAS数据")
        return pd.DataFrame()
    
    # 合并涨幅和BIAS数据
    result = returns.merge(bias_df, on='index_code', how='left')
    
    # 6. 合并指数信息
    result = result.merge(
        sw_codes[['index_code_zs', 'industry_name', 'level']], 
        left_on='index_code', 
        right_on='index_code_zs', 
        how='left'
    )
    
    # 7. 排序和格式化
    result = result.sort_values('return_rate', ascending=False)
    result['return_rate'] = result['return_rate'].round(2)
    
    print(f"✅ 计算完成，共 {len(result)} 个板块（含120BIAS）")
    
    return result


# def main():
#     """主函数 - 示例用法"""
#     # 设定起止时间
#     start_date = "2024-01-01"
#     end_date = "2024-12-31"
    
#     # 计算申万板块指数涨幅
#     results = calculate_sw_sector_returns(start_date, end_date)
    
#     if not results.empty:
#         print("\n📈 申万板块指数涨幅排行榜:")
#         print("=" * 80)
        
#         # 显示前20名
#         top_results = results.head(20)
#         for idx, row in top_results.iterrows():
#             print(f"{row['level']} | {row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {row['index_code']}")
        
#         # 保存结果到CSV
#         # output_file = f"申万板块涨幅_{start_date}_{end_date}.csv"
#         output_file = f"databases/申万板块涨幅_{start_date}_{end_date}.csv"
#         results.to_csv(output_file, index=False, encoding='utf-8-sig')
#         print(f"\n💾 结果已保存到: {output_file}")
        
#         # 统计信息
#         print(f"\n📊 统计信息:")
#         print(f"总板块数: {len(results)}")
#         print(f"一级板块: {len(results[results['level'] == 'L1'])}")
#         print(f"二级板块: {len(results[results['level'] == 'L2'])}")
#         print(f"平均涨幅: {results['return_rate'].mean():.2f}%")
#         print(f"最大涨幅: {results['return_rate'].max():.2f}%")
#         print(f"最小涨幅: {results['return_rate'].min():.2f}%")
#     else:
#         print("❌ 未获取到计算结果")


def run_multiple_periods_analysis_changxian():
    """运行多时间段申万板块指数涨幅分析"""
    # 设定多个起止时间段
    time_periods = {
        "2025年1-4月": ("2025-01-10", "2025-04-11"),
        "2024年9月-2025年1月": ("2024-09-13", "2025-01-03"),
        "2024年2-6月": ("2024-02-02", "2024-06-07")
    }
    
    # 存储所有结果
    all_results = {}
    
    # 循环计算每个时间段的申万板块指数涨幅
    for period_name, (start_date, end_date) in time_periods.items():
        print(f"\n{'='*60}")
        print(f"📊 正在计算时间段: {period_name}")
        print(f"📅 时间范围: {start_date} 至 {end_date}")
        print(f"{'='*60}")
        
        # 计算申万板块指数涨幅（含120BIAS）
        results = calculate_sw_sector_returns_with_bias(start_date, end_date)
        
        if not results.empty:
            # 保存结果
            all_results[period_name] = results
            
            # 显示前10名（含120BIAS）
            print(f"\n📈 {period_name} 申万板块指数涨幅前10名（含120BIAS）:")
            print("-" * 120)
            print(f"{'级别':<4} | {'板块名称':<20} | {'涨幅(%)':>8} | {'120BIAS(%)':>10} | {'指数代码'}")
            print("-" * 120)
            top_results = results.head(10)
            for idx, row in top_results.iterrows():
                bias_120 = row.get('120BIAS', 0)
                print(f"{row['level']:<4} | {row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {bias_120:>10.2f}% | {row['index_code']}")
            
            # 保存结果到CSV
            output_file = f"databases/xunhuan_changxian/申万板块长线涨幅_{period_name}_{start_date}_{end_date}.csv"
            results.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 结果已保存到: {output_file}")
            
            # 统计信息
            print(f"\n📊 {period_name} 统计信息:")
            print(f"总板块数: {len(results)}")
            print(f"一级板块: {len(results[results['level'] == 'L1'])}")
            print(f"二级板块: {len(results[results['level'] == 'L2'])}")
            print(f"平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"最大涨幅: {results['return_rate'].max():.2f}%")
            print(f"最小涨幅: {results['return_rate'].min():.2f}%")
            
            # 120BIAS统计信息
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"最大120BIAS: {bias_data.max():.2f}%")
                    print(f"最小120BIAS: {bias_data.min():.2f}%")
                    
                    # 显示120BIAS极值板块
                    print(f"\n📊 120BIAS极值板块:")
                    max_bias = results.loc[results['120BIAS'].idxmax()]
                    min_bias = results.loc[results['120BIAS'].idxmin()]
                    print(f"最大120BIAS: {max_bias['industry_name']} ({max_bias['120BIAS']:.2f}%)")
                    print(f"最小120BIAS: {min_bias['industry_name']} ({min_bias['120BIAS']:.2f}%)")
        else:
            print(f"❌ {period_name} 未获取到计算结果")
    
    # 汇总所有结果
    if all_results:
        print(f"\n{'='*60}")
        print("📊 所有时间段汇总统计:")
        print(f"{'='*60}")
        
        for period_name, results in all_results.items():
            print(f"{period_name}:")
            print(f"  平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"  最大涨幅: {results['return_rate'].max():.2f}%")
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"  平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"  最大120BIAS: {bias_data.max():.2f}%")
        
        # 保存汇总结果
        summary_file = "databases/申万板块长线涨幅汇总.csv"
        with pd.ExcelWriter(summary_file.replace('.csv', '.xlsx'), engine='openpyxl') as writer:
            for period_name, results in all_results.items():
                # 清理sheet名称中的特殊字符
                sheet_name = period_name.replace('/', '-').replace('年', '').replace('月', '')
                results.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"\n💾 汇总结果已保存到: {summary_file.replace('.csv', '.xlsx')}")
    
    return all_results




def run_multiple_periods_analysis_zhongji():
    """运行多时间段申万板块指数涨幅分析"""
    # 设定多个起止时间段
    time_periods = {
        "2025年6-7月": ("2025-06-20", "2025-07-31"),  # 修改结束日期为数据库最新日期
        "2025年8月": ("2025-08-01", "2025-09-04"),
    }
    
    # 存储所有结果
    all_results = {}
    
    # 循环计算每个时间段的申万板块指数涨幅
    for period_name, (start_date, end_date) in time_periods.items():
        print(f"\n{'='*60}")
        print(f"📊 正在计算时间段: {period_name}")
        print(f"📅 时间范围: {start_date} 至 {end_date}")
        print(f"{'='*60}")
        
        # 计算申万板块指数涨幅（含120BIAS）
        results = calculate_sw_sector_returns_with_bias(start_date, end_date)
        
        if not results.empty:
            # 保存结果
            all_results[period_name] = results
            
            # 显示前10名（含120BIAS）
            print(f"\n📈 {period_name} 申万板块指数涨幅前10名（含120BIAS）:")
            print("-" * 120)
            print(f"{'级别':<4} | {'板块名称':<20} | {'涨幅(%)':>8} | {'120BIAS(%)':>10} | {'指数代码'}")
            print("-" * 120)
            top_results = results.head(10)
            for idx, row in top_results.iterrows():
                bias_120 = row.get('120BIAS', 0)
                print(f"{row['level']:<4} | {row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {bias_120:>10.2f}% | {row['index_code']}")
            
            # 保存结果到CSV
            output_dir = "databases/xunhuan_zhongji"
            os.makedirs(output_dir, exist_ok=True)  # 确保目录存在
            output_file = f"{output_dir}/申万板块中级涨幅_{period_name}_{start_date}_{end_date}.csv"
            results.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 结果已保存到: {output_file}")
            
            # 统计信息
            print(f"\n📊 {period_name} 统计信息:")
            print(f"总板块数: {len(results)}")
            print(f"一级板块: {len(results[results['level'] == 'L1'])}")
            print(f"二级板块: {len(results[results['level'] == 'L2'])}")
            print(f"平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"最大涨幅: {results['return_rate'].max():.2f}%")
            print(f"最小涨幅: {results['return_rate'].min():.2f}%")
            
            # 120BIAS统计信息
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"最大120BIAS: {bias_data.max():.2f}%")
                    print(f"最小120BIAS: {bias_data.min():.2f}%")
                    
                    # 显示120BIAS极值板块
                    print(f"\n📊 120BIAS极值板块:")
                    max_bias = results.loc[results['120BIAS'].idxmax()]
                    min_bias = results.loc[results['120BIAS'].idxmin()]
                    print(f"最大120BIAS: {max_bias['industry_name']} ({max_bias['120BIAS']:.2f}%)")
                    print(f"最小120BIAS: {min_bias['industry_name']} ({min_bias['120BIAS']:.2f}%)")
        else:
            print(f"❌ {period_name} 未获取到计算结果")
    
    # 汇总所有结果
    if all_results:
        print(f"\n{'='*60}")
        print("📊 所有时间段汇总统计:")
        print(f"{'='*60}")
        
        for period_name, results in all_results.items():
            print(f"{period_name}:")
            print(f"  平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"  最大涨幅: {results['return_rate'].max():.2f}%")
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"  平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"  最大120BIAS: {bias_data.max():.2f}%")
        
        # 保存汇总结果
        summary_file = "databases/申万板块中级涨幅汇总.csv"
        with pd.ExcelWriter(summary_file.replace('.csv', '.xlsx'), engine='openpyxl') as writer:
            for period_name, results in all_results.items():
                # 清理sheet名称中的特殊字符
                sheet_name = period_name.replace('/', '-').replace('年', '').replace('月', '')
                results.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"\n💾 汇总结果已保存到: {summary_file.replace('.csv', '.xlsx')}")
    
    return all_results




def run_multiple_periods_analysis_boduan():
    """运行多时间段申万板块指数涨幅分析"""
    # 设定多个起止时间段
    time_periods = {
        "2025年9月": ("2025-09-04", "2025-09-18"),  # 修改结束日期为数据库最新日期
        "2025年8月": ("2025-08-14", "2025-09-04"),
        "2025年7-8月": ("2025-07-31", "2025-08-14")
    }
    
    # 存储所有结果
    all_results = {}
    
    # 循环计算每个时间段的申万板块指数涨幅
    for period_name, (start_date, end_date) in time_periods.items():
        print(f"\n{'='*60}")
        print(f"📊 正在计算时间段: {period_name}")
        print(f"📅 时间范围: {start_date} 至 {end_date}")
        print(f"{'='*60}")
        
        # 计算申万板块指数涨幅（含120BIAS）
        results = calculate_sw_sector_returns_with_bias(start_date, end_date)
        
        if not results.empty:
            # 保存结果
            all_results[period_name] = results
            
            # 显示前10名（含120BIAS）
            print(f"\n📈 {period_name} 申万板块指数涨幅前10名（含120BIAS）:")
            print("-" * 120)
            print(f"{'级别':<4} | {'板块名称':<20} | {'涨幅(%)':>8} | {'120BIAS(%)':>10} | {'指数代码'}")
            print("-" * 120)
            top_results = results.head(10)
            for idx, row in top_results.iterrows():
                bias_120 = row.get('120BIAS', 0)
                print(f"{row['level']:<4} | {row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {bias_120:>10.2f}% | {row['index_code']}")
            
            # 保存结果到CSV
            output_file = f"databases/xunhuan_boduan/申万板块波段涨幅_{period_name}_{start_date}_{end_date}.csv"
            results.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n💾 结果已保存到: {output_file}")
            
            # 统计信息
            print(f"\n📊 {period_name} 统计信息:")
            print(f"总板块数: {len(results)}")
            print(f"一级板块: {len(results[results['level'] == 'L1'])}")
            print(f"二级板块: {len(results[results['level'] == 'L2'])}")
            print(f"平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"最大涨幅: {results['return_rate'].max():.2f}%")
            print(f"最小涨幅: {results['return_rate'].min():.2f}%")
            
            # 120BIAS统计信息
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"最大120BIAS: {bias_data.max():.2f}%")
                    print(f"最小120BIAS: {bias_data.min():.2f}%")
                    
                    # 显示120BIAS极值板块
                    print(f"\n📊 120BIAS极值板块:")
                    max_bias = results.loc[results['120BIAS'].idxmax()]
                    min_bias = results.loc[results['120BIAS'].idxmin()]
                    print(f"最大120BIAS: {max_bias['industry_name']} ({max_bias['120BIAS']:.2f}%)")
                    print(f"最小120BIAS: {min_bias['industry_name']} ({min_bias['120BIAS']:.2f}%)")
        else:
            print(f"❌ {period_name} 未获取到计算结果")
    
    # 汇总所有结果
    if all_results:
        print(f"\n{'='*60}")
        print("📊 所有时间段汇总统计:")
        print(f"{'='*60}")
        
        for period_name, results in all_results.items():
            print(f"{period_name}:")
            print(f"  平均涨幅: {results['return_rate'].mean():.2f}%")
            print(f"  最大涨幅: {results['return_rate'].max():.2f}%")
            if '120BIAS' in results.columns:
                bias_data = results['120BIAS'].dropna()
                if not bias_data.empty:
                    print(f"  平均120BIAS: {bias_data.mean():.2f}%")
                    print(f"  最大120BIAS: {bias_data.max():.2f}%")
        
        # 保存汇总结果
        summary_file = "databases/申万板块波段涨幅汇总.csv"
        with pd.ExcelWriter(summary_file.replace('.csv', '.xlsx'), engine='openpyxl') as writer:
            for period_name, results in all_results.items():
                # 清理sheet名称中的特殊字符
                sheet_name = period_name.replace('/', '-').replace('年', '').replace('月', '')
                results.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"\n💾 汇总结果已保存到: {summary_file.replace('.csv', '.xlsx')}")
    
    return all_results




def analyze_sector_performance():
    """
    分析板块指数表现
    1. 读取板块指数的涨幅CSV文件
    2. 获取涨幅前10名的板块名称
    3. 获取120BIAS在quantile(0.5)以下且涨幅在quantile(0.5)以上的板块名称
    """
    
    # 读取CSV文件
    csv_file = r"C:\quant_v2\databases\xunhuan_boduan\申万板块波段涨幅_2025年7-8月_2025-07-31_2025-08-14.csv"
    
    try:
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        print(f"✅ 成功读取CSV文件: {csv_file}")
        print(f"📊 数据包含 {len(df)} 条记录")
        print(f"📋 字段列表: {list(df.columns)}")
        
    except FileNotFoundError:
        print(f"❌ 文件不存在: {csv_file}")
        return
    except Exception as e:
        print(f"❌ 读取文件失败: {e}")
        return
    
    # 检查必要的列是否存在
    required_columns = ['industry_name', 'return_rate', '120BIAS']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ 缺少必要的列: {missing_columns}")
        return
    
    print(f"\n{'='*80}")
    print("📈 板块指数表现分析")
    print(f"{'='*80}")
    
    # 1. 获取涨幅前10名的板块名称
    print(f"\n🏆 涨幅前10名的板块:")
    print("-" * 100)
    print(f"{'板块名称':<20} | {'涨幅(%)':>8} | {'120BIAS(%)':>10} | {'级别':<4} | {'指数代码'}")
    print("-" * 100)
    
    # 按涨幅降序排列，取前10名
    top_10_by_return = df.nlargest(15, 'return_rate')
    
    for idx, row in top_10_by_return.iterrows():
        level = row.get('level', 'N/A')
        print(f"{row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {row['120BIAS']:>10.2f}% | {level:<4} | {row['index_code']}")
    
    # 2. 计算分位数
    return_median = df['return_rate'].quantile(0.6)
    bias_median = df['120BIAS'].quantile(0.4)
    
    print(f"\n📊 分位数统计:")
    print(f"涨幅中位数 (50%分位数): {return_median:.2f}%")
    print(f"120BIAS中位数 (50%分位数): {bias_median:.2f}%")
    
    # 3. 获取120BIAS在quantile(0.5)以下且涨幅在quantile(0.5)以上的板块
    print(f"\n🎯 筛选条件: 120BIAS < {bias_median:.2f}% 且 涨幅 > {return_median:.2f}%")
    print("-" * 80)
    
    # 筛选条件：120BIAS < 中位数 且 涨幅 > 中位数
    filtered_sectors = df[
        (df['120BIAS'] < bias_median) & 
        (df['return_rate'] > return_median)
    ].copy()
    
    if not filtered_sectors.empty:
        # 按涨幅降序排列
        filtered_sectors = filtered_sectors.sort_values('return_rate', ascending=False)
        
        print(f"✅ 找到 {len(filtered_sectors)} 个符合条件的板块:")
        print(f"{'板块名称':<20} | {'涨幅(%)':>8} | {'120BIAS(%)':>10} | {'级别':<4} | {'指数代码'}")
        print("-" * 100)
        
        for idx, row in filtered_sectors.iterrows():
            level = row.get('level', 'N/A')
            print(f"{row['industry_name']:<20} | {row['return_rate']:>8.2f}% | {row['120BIAS']:>10.2f}% | {level:<4} | {row['index_code']}")
        
        # 保存筛选结果
        output_file = "databases/筛选板块_低120BIAS高涨幅.csv"
        filtered_sectors.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n💾 筛选结果已保存到: {output_file}")
        
    else:
        print("❌ 没有找到符合条件的板块")
    
    # 4. 统计信息
    print(f"\n📊 整体统计信息:")
    print(f"总板块数: {len(df)}")
    print(f"涨幅范围: {df['return_rate'].min():.2f}% ~ {df['return_rate'].max():.2f}%")
    print(f"120BIAS范围: {df['120BIAS'].min():.2f}% ~ {df['120BIAS'].max():.2f}%")
    print(f"平均涨幅: {df['return_rate'].mean():.2f}%")
    print(f"平均120BIAS: {df['120BIAS'].mean():.2f}%")
    
    return {
        'top_10_by_return': top_10_by_return,
        'filtered_sectors': filtered_sectors,
        'return_median': return_median,
        'bias_median': bias_median,
        'all_data': df  # 返回完整数据供后续使用
    }

def analyze_sector_performance_boduan():
    """
    分析板块指数表现
    1. 读取文件夹下的所有板块指数的涨幅CSV文件
    2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称（最多30个）
    3. 获取120BIAS在quantile(0.4)以下且涨幅在quantile(0.55)以上的板块名称
    4，分别按照上面2，3项来保存筛选结果。用dataframe来保存，用2张表。
    """
    import os
    import glob
    
    # 1. 读取文件夹下的所有板块指数的涨幅CSV文件
    csv_folder = r"C:\quant_v2\databases\xunhuan_boduan"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return None
    
    results = {}
    
    for csv_file in csv_files:
        # 获取文件名（不含路径和扩展名）
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
    
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
        except Exception as e:
            continue
        
        # 检查必要的列是否存在
        required_columns = ['industry_name', 'return_rate', '120BIAS']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            continue
            
        # 2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称，但最多不超过30个
        # 涨幅排序前15%
        top_15_percent_count = max(1, int(len(df) * 0.15))  # 至少1个
        top_15_percent_by_return = df.nlargest(top_15_percent_count, 'return_rate').copy()
        
        # 涨幅20%以上的板块
        high_return_sectors = df[df['return_rate'] > 20].copy()
        
        # 合并两个条件（取并集，去重）
        table1_sectors = pd.concat([top_15_percent_by_return, high_return_sectors]).drop_duplicates(subset=['industry_name']).copy()
        table1_sectors = table1_sectors.sort_values('return_rate', ascending=False)
        
        # 限制最多不超过30个
        if len(table1_sectors) > 30:
            table1_sectors = table1_sectors.head(30)
        
        # 3. 获取120BIAS在quantile(0.4)以下且涨幅在quantile(0.55)以上的板块名称
        bias_threshold = df['120BIAS'].quantile(0.4)  # 120BIAS的40%分位数
        return_threshold = df['return_rate'].quantile(0.55)  # 涨幅的55%分位数
        
        table2_sectors = df[
            (df['120BIAS'] < bias_threshold) & 
            (df['return_rate'] > return_threshold)
        ].copy()
        table2_sectors = table2_sectors.sort_values('return_rate', ascending=False)
        
        # 4. 分别保存筛选结果到2张表
        # 保存表1：涨幅排序前15%或涨幅20%以上的板块（最多30个）
        output_file1 = rf"databases\xunhuan_boduan_zf\{file_name}_涨幅筛选.csv"
        table1_sectors.to_csv(output_file1, index=False, encoding='utf-8-sig')
        
        # 保存表2：低120BIAS高涨幅板块
        output_file2 = rf"databases\xunhuan_boduan_bias\{file_name}_bias筛选.csv"
        table2_sectors.to_csv(output_file2, index=False, encoding='utf-8-sig')
        
        # 保存每个文件的结果
        results[file_name] = {
            'table1_sectors': table1_sectors,  # 涨幅前15%或20%以上
            'table2_sectors': table2_sectors,  # 低120BIAS高涨幅
            'bias_threshold': bias_threshold,
            'return_threshold': return_threshold,
            'all_data': df
        }
    
    return results


def analyze_sector_performance_zhongji():
    """
    分析板块指数表现
    1. 读取文件夹下的所有板块指数的涨幅CSV文件
    2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称（最多30个）
    3. 获取120BIAS在quantile(0.4)以下且涨幅在quantile(0.55)以上的板块名称
    4，分别按照上面2，3项来保存筛选结果。用dataframe来保存，用2张表。
    """
    import os
    import glob
    
    # 1. 读取文件夹下的所有板块指数的涨幅CSV文件
    csv_folder = r"C:\quant_v2\databases\xunhuan_zhongji"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return None
    
    results = {}
    
    for csv_file in csv_files:
        # 获取文件名（不含路径和扩展名）
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
    
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
        except Exception as e:
            continue
        
        # 检查必要的列是否存在
        required_columns = ['industry_name', 'return_rate', '120BIAS']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            continue
            
        # 2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称，但最多不超过30个
        # 涨幅排序前15%
        top_15_percent_count = max(1, int(len(df) * 0.15))  # 至少1个
        top_15_percent_by_return = df.nlargest(top_15_percent_count, 'return_rate').copy()
        
        # 涨幅20%以上的板块
        high_return_sectors = df[df['return_rate'] > 20].copy()
        
        # 合并两个条件（取并集，去重）
        table1_sectors = pd.concat([top_15_percent_by_return, high_return_sectors]).drop_duplicates(subset=['industry_name']).copy()
        table1_sectors = table1_sectors.sort_values('return_rate', ascending=False)
        
        # 限制最多不超过30个
        if len(table1_sectors) > 30:
            table1_sectors = table1_sectors.head(30)
        
        # 3. 获取120BIAS在quantile(0.4)以下且涨幅在quantile(0.55)以上的板块名称
        bias_threshold = df['120BIAS'].quantile(0.4)  # 120BIAS的40%分位数
        return_threshold = df['return_rate'].quantile(0.55)  # 涨幅的55%分位数
        
        table2_sectors = df[
            (df['120BIAS'] < bias_threshold) & 
            (df['return_rate'] > return_threshold)
        ].copy()
        table2_sectors = table2_sectors.sort_values('return_rate', ascending=False)
        
        # 4. 分别保存筛选结果到2张表
        # 确保目录存在
        os.makedirs("databases/xunhuan_zhongji_zf", exist_ok=True)
        os.makedirs("databases/xunhuan_zhongji_bias", exist_ok=True)
        
        # 保存表1：涨幅排序前15%或涨幅20%以上的板块（最多30个）
        output_file1 = rf"databases\xunhuan_zhongji_zf\{file_name}_涨幅筛选.csv"
        table1_sectors.to_csv(output_file1, index=False, encoding='utf-8-sig')
        
        # 保存表2：低120BIAS高涨幅板块
        output_file2 = rf"databases\xunhuan_zhongji_bias\{file_name}_bias筛选.csv"
        table2_sectors.to_csv(output_file2, index=False, encoding='utf-8-sig')
        
        # 保存每个文件的结果
        results[file_name] = {
            'table1_sectors': table1_sectors,  # 涨幅前15%或20%以上
            'table2_sectors': table2_sectors,  # 低120BIAS高涨幅
            'bias_threshold': bias_threshold,
            'return_threshold': return_threshold,
            'all_data': df
        }
    
    return results


def analyze_sector_performance_changxian():
    """
    分析板块指数表现
    1. 读取文件夹下的所有板块指数的涨幅CSV文件
    2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称（最多30个）
    3，分别按照上面2项来保存筛选结果。用dataframe来保存。
    """
    import os
    import glob
    
    # 1. 读取文件夹下的所有板块指数的涨幅CSV文件
    csv_folder = r"C:\quant_v2\databases\xunhuan_changxian"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    
    if not csv_files:
        return None
    
    results = {}
    
    for csv_file in csv_files:
        # 获取文件名（不含路径和扩展名）
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
    
        try:
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
        except Exception as e:
            continue
        
        # 检查必要的列是否存在
        required_columns = ['industry_name', 'return_rate', '120BIAS']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            continue
            
        # 2. 获取涨幅排序前15%的板块名称或涨幅20%以上的板块名称，但最多不超过30个
        # 涨幅排序前15%
        top_15_percent_count = max(1, int(len(df) * 0.15))  # 至少1个
        top_15_percent_by_return = df.nlargest(top_15_percent_count, 'return_rate').copy()
        
        # 涨幅20%以上的板块
        high_return_sectors = df[df['return_rate'] > 20].copy()
        
        # 合并两个条件（取并集，去重）
        table1_sectors = pd.concat([top_15_percent_by_return, high_return_sectors]).drop_duplicates(subset=['industry_name']).copy()
        table1_sectors = table1_sectors.sort_values('return_rate', ascending=False)
        
        # 限制最多不超过30个
        if len(table1_sectors) > 30:
            table1_sectors = table1_sectors.head(30)
        
        # 3. 获取120BIAS在quantile(0.4)以下且涨幅在quantile(0.55)以上的板块名称
        bias_threshold = df['120BIAS'].quantile(0.4)  # 120BIAS的40%分位数
        return_threshold = df['return_rate'].quantile(0.55)  # 涨幅的55%分位数
        
        table2_sectors = df[
            (df['120BIAS'] < bias_threshold) & 
            (df['return_rate'] > return_threshold)
        ].copy()
        table2_sectors = table2_sectors.sort_values('return_rate', ascending=False)
        
        # 4. 分别保存筛选结果到2张表
        # 保存表1：涨幅排序前15%或涨幅20%以上的板块（最多30个）
        output_file1 = os.path.join("databases", "xunhuan_changxian_zf", f"{file_name}_涨幅筛选.csv")
        table1_sectors.to_csv(output_file1, index=False, encoding='utf-8-sig')
        
        # # 保存表2：低120BIAS高涨幅板块
        # output_file2 = rf"databases\xunhuan_changxian_bias\{file_name}_bias筛选.csv"
        # table2_sectors.to_csv(output_file2, index=False, encoding='utf-8-sig')
        
        # 保存每个文件的结果
        results[file_name] = {
            'table1_sectors': table1_sectors,  # 涨幅前15%或20%以上
            'table2_sectors': table2_sectors,  # 低120BIAS高涨幅
            'bias_threshold': bias_threshold,
            'return_threshold': return_threshold,
            'all_data': df
        }
    
    return results



def get_sector_index_codes(result_data, sector_names=None):
    """
    获取指定板块的index_code
    
    Args:
        result_data: analyze_sector_performance()的返回结果
        sector_names: 板块名称列表，如果为None则返回所有板块
    
    Returns:
        dict: {板块名称: index_code}
    """
    if result_data is None or 'all_data' not in result_data:
        print("❌ 无效的结果数据")
        return {}
    
    df = result_data['all_data']
    
    if sector_names is None:
        # 返回所有板块的index_code
        sector_codes = dict(zip(df['industry_name'], df['index_code']))
    else:
        # 返回指定板块的index_code
        sector_codes = {}
        for sector_name in sector_names:
            matching_rows = df[df['industry_name'] == sector_name]
            if not matching_rows.empty:
                sector_codes[sector_name] = matching_rows.iloc[0]['index_code']
            else:
                print(f"⚠️  未找到板块: {sector_name}")
    
    return sector_codes


if __name__ == "__main__":
    # 运行多时间段申万板块指数涨幅分析（含120BIAS）
    # run_multiple_periods_analysis_changxian()
    # run_multiple_periods_analysis_zhongji()
    # run_multiple_periods_analysis_boduan()
    #==============================================


    # result = analyze_sector_performance()
    
    # # 示例：获取涨幅前10名板块的index_code
    # if result:
    #     print(f"\n{'='*80}")
    #     print("🔗 涨幅前10名板块的index_code:")
    #     print(f"{'='*80}")
        
    #     top_10_codes = get_sector_index_codes(result, result['top_10_by_return']['industry_name'].tolist())
    #     for sector_name, index_code in top_10_codes.items():
    #         print(f"{sector_name:<20} | {index_code}")
        
    #     # 示例：获取筛选后板块的index_code
    #     if not result['filtered_sectors'].empty:
    #         print(f"\n🔗 筛选板块的index_code:")
    #         print("-" * 50)
    #         filtered_codes = get_sector_index_codes(result, result['filtered_sectors']['industry_name'].tolist())
    #         for sector_name, index_code in filtered_codes.items():
    #             print(f"{sector_name:<20} | {index_code}")
    #========================================
    result = analyze_sector_performance_boduan()
    if result:
        print("波段分析结果:")
        for file_name, data in result.items():
            print(f"\n文件: {file_name}")
            print(f"涨幅前15%或20%以上板块数量: {len(data['table1_sectors'])}")
            print(f"低120BIAS高涨幅板块数量: {len(data['table2_sectors'])}")
            if not data['table1_sectors'].empty:
                print("涨幅前15%或20%以上板块:")
                print(data['table1_sectors'][['industry_name', 'return_rate', '120BIAS']].head())
            if not data['table2_sectors'].empty:
                print("低120BIAS高涨幅板块:")
                print(data['table2_sectors'][['industry_name', 'return_rate', '120BIAS']].head())
    else:
        print("❌ 波段分析失败或没有数据")
    result1 = analyze_sector_performance_changxian()
    #========================================
    # 获取长线涨幅居前的板块index_code列表
    longxian_changxian_bankuai = get_boduan_bias_bankuai()
    print(longxian_changxian_bankuai)
    aa = analyze_sector_performance_zhongji()
    print(aa)
    
    # print(zhuli_bankuai)  # 注释掉未定义的变量


    #自设新成立的板块。
    #设定起止时间，来计算申万一级二级板块指数的涨幅。（申万指数行情数据在index_k_daily表中。获取申万一级二级指数代码的方式，通过sw数据表，直接看Level==1,==2，但需要把对应的index_code把后缀.SI改为.ZS）