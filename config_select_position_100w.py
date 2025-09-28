#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多板块操作股选择配置文件 - 100万账户版本
用户可以在这里修改配置参数，针对不同板块设定不同的操作参数
"""

from datetime import datetime

# =============================================================================
# 全局配置
# =============================================================================

# 分析日期（可以修改为具体日期或使用当前日期）
END_DATE = '2025-09-19'  # 或者使用: datetime.now().strftime('%Y-%m-%d')

# 基础账户配置
ACCOUNT_CONFIG = {
    'starting_cash': 1000000.0,  # 100万起始资金
    'account_name': 'account_100w'  # 账户名称
}

# 评分板块名称
pingfen_bankuai_names = ['低价']

# 候选股池配置（从板块获取：一般不用）
pools_to_load = {
    # 'houxuan_yangzhiye': '养殖业',
    # 'houxuan_lvyou': '旅游及景区',
    # 'houxuan_keji': '科技股',
    # 'houxuan_yiliao': '医疗器械',
    # 'houxuan_xinnengyuan': '新能源',
    # 'houxuan_jinrong': '金融服务',
    # 'houxuan_fangdichan': '房地产',
}

# CSV候选股池配置（兼容旧版本）
# 注意：csv_pools_to_load 将在 CSV_POOLS_CONFIG 定义后赋值

# =============================================================================
# 板块操作参数配置
# =============================================================================

# 板块操作参数配置字典
# 每个板块可以独立设置以下参数：
# - sector_initial_cap: 板块初始仓位比例
# - min_stocks: 最少股票数量
# - max_stocks: 最多股票数量  
# - top_percentage: 前百分比
# - pool_name: 数据库中的板块名称
# SECTOR_CONFIGS = {
#     # 养殖业板块配置
#     'yangzhiye': {
#         'pool_name': '养殖业',
#         'sector_initial_cap': 0.20,      # 20%仓位
#         'min_stocks': 3,                 # 最少3只股票
#         'max_stocks': 5,                 # 最多5只股票
#         'top_percentage': 0.25,          # 前25%
#         'description': '养殖业板块操作配置'
#     },
    
#     # 旅游及景区板块配置
#     'lvyou': {
#         'pool_name': '旅游及景区',
#         'sector_initial_cap': 0.15,      # 15%仓位
#         'min_stocks': 2,                 # 最少2只股票
#         'max_stocks': 4,                 # 最多4只股票
#         'top_percentage': 0.30,          # 前30%
#         'description': '旅游及景区板块操作配置'
#     },
    
#     # 科技股板块配置
#     'keji': {
#         'pool_name': '科技股',
#         'sector_initial_cap': 0.25,      # 25%仓位
#         'min_stocks': 4,                 # 最少4只股票
#         'max_stocks': 6,                 # 最多6只股票
#         'top_percentage': 0.20,          # 前20%
#         'description': '科技股板块操作配置'
#     },
    
#     # 医疗器械板块配置
#     'yiliao': {
#         'pool_name': '医疗器械',
#         'sector_initial_cap': 0.18,      # 18%仓位
#         'min_stocks': 3,                 # 最少3只股票
#         'max_stocks': 5,                 # 最多5只股票
#         'top_percentage': 0.22,          # 前22%
#         'description': '医疗器械板块操作配置'
#     },
    
#     # 新能源板块配置
#     'xinnengyuan': {
#         'pool_name': '新能源',
#         'sector_initial_cap': 0.22,      # 22%仓位
#         'min_stocks': 3,                 # 最少3只股票
#         'max_stocks': 5,                 # 最多5只股票
#         'top_percentage': 0.20,          # 前20%
#         'description': '新能源板块操作配置'
#     },
    
#     # 金融服务板块配置
#     'jinrong': {
#         'pool_name': '金融服务',
#         'sector_initial_cap': 0.30,      # 30%仓位
#         'min_stocks': 4,                 # 最少4只股票
#         'max_stocks': 7,                 # 最多7只股票
#         'top_percentage': 0.15,          # 前15%
#         'description': '金融服务板块操作配置'
#     },
    
#     # 房地产板块配置
#     'fangdichan': {
#         'pool_name': '房地产',
#         'sector_initial_cap': 0.12,      # 12%仓位
#         'min_stocks': 2,                 # 最少2只股票
#         'max_stocks': 4,                 # 最多4只股票
#         'top_percentage': 0.35,          # 前35%
#         'description': '房地产板块操作配置'
#     }
# }

# =============================================================================
# CSV文件配置（从CSV文件获取候选股池）
# =============================================================================

# CSV文件配置字典
# 每个CSV配置包含文件路径、列名映射和操作参数
CSV_POOLS_CONFIG = {
    'houxuan_yangzhiye': {
        'file_path': 'databases/操作板块/2025-09-26/合并_养殖业II_2025-09-26.csv',
        'stock_code_column': 'stock_code',
        'stock_name_column': 'stock_name',
        'sector_initial_cap': 0.12,      # 12%总资金
        'min_stocks': 2,                 # 最少2只股票
        'max_stocks': 4,                 # 最多4只股票
        'top_percentage': 0.15,          # 评分前15%
    },
    # 'houxuan_lvyou': {
    #     'file_path': 'databases/操作板块/2025-09-26/旅游及景区_stocks.csv',
    #     'stock_code_column': 'stock_code',
    #     'stock_name_column': 'stock_name',
    #     'sector_initial_cap': 0.10,
    #     'min_stocks': 2,
    #     'max_stocks': 3,
    #     'top_percentage': 0.40,
    #     'description': 'CSV旅游及景区板块操作配置'
    # },
    # 'houxuan_keji': {
    #     'file_path': 'databases/操作板块/2025-09-26/科技股_stocks.csv',
    #     'stock_code_column': 'code',
    #     'stock_name_column': 'name',
    #     'sector_initial_cap': 0.15,
    #     'min_stocks': 3,
    #     'max_stocks': 5,
    #     'top_percentage': 0.25,
    #     'description': 'CSV科技股板块操作配置'
    # },
}

# CSV候选股池配置（兼容旧版本）
csv_pools_to_load = CSV_POOLS_CONFIG

# =============================================================================
# 择时参数配置
# =============================================================================

# 长线股择时阈值
CX_THRESHOLD = 0.1

# 选股择时阈值  
XUANGU_THRESHOLD = 0.32

# =============================================================================
# 长线股选择条件
# =============================================================================

# 长线股技术分析得分阈值
CX_TECH_SCORE_THRESHOLD = 4

# 长线股总得分阈值
CX_TOTAL_SCORE_THRESHOLD = 9

# =============================================================================
# 风险控制参数
# =============================================================================

# 是否启用ATR风险调整
USE_RISK_ADJUSTMENT = True

# 最大板块数量限制（防止过度分散）
MAX_SECTORS = 6

# 总仓位上限（所有板块仓位总和不能超过此比例）
MAX_TOTAL_POSITION = 1.30  # 130%

# =============================================================================
# 辅助函数
# =============================================================================

def get_sector_config(sector_key):
    """
    获取指定板块的配置参数
    
    参数:
    sector_key (str): 板块键名
    
    返回:
    dict: 板块配置字典，如果不存在返回None
    """
    # 由于SECTOR_CONFIGS被注释，返回None
    return None

def get_all_sector_keys():
    """
    获取所有板块键名列表
    
    返回:
    list: 所有板块键名列表
    """
    # 由于SECTOR_CONFIGS被注释，返回空列表
    return []

def validate_sector_config(sector_key):
    """
    验证板块配置的完整性
    
    参数:
    sector_key (str): 板块键名
    
    返回:
    bool: 配置是否有效
    """
    config = get_sector_config(sector_key)
    if not config:
        return False
    
    required_keys = ['pool_name', 'sector_initial_cap', 'min_stocks', 'max_stocks', 'top_percentage']
    return all(key in config for key in required_keys)

def get_total_allocated_cap():
    """
    计算所有板块的总仓位分配
    
    返回:
    float: 总仓位比例
    """
    # 由于SECTOR_CONFIGS被注释，返回0
    return 0.0

def get_csv_pool_config(csv_key):
    """
    获取指定CSV池的配置参数
    
    参数:
    csv_key (str): CSV池键名
    
    返回:
    dict: CSV池配置字典，如果不存在返回None
    """
    return CSV_POOLS_CONFIG.get(csv_key)

def get_all_csv_keys():
    """
    获取所有CSV池键名列表
    
    返回:
    list: 所有CSV池键名列表
    """
    return list(CSV_POOLS_CONFIG.keys())

def validate_csv_config(csv_key):
    """
    验证CSV配置的完整性
    
    参数:
    csv_key (str): CSV池键名
    
    返回:
    bool: 配置是否有效
    """
    config = get_csv_pool_config(csv_key)
    if not config:
        return False
    
    required_keys = ['file_path', 'stock_code_column', 'stock_name_column', 
                    'sector_initial_cap', 'min_stocks', 'max_stocks', 'top_percentage']
    return all(key in config for key in required_keys)

def get_all_pool_keys():
    """
    获取所有池键名列表（包括数据库池和CSV池）
    
    返回:
    dict: 包含数据库池和CSV池键名的字典
    """
    return {
        'database_pools': get_all_sector_keys(),
        'csv_pools': get_all_csv_keys(),
        'all_pools': get_all_sector_keys() + get_all_csv_keys()
    }

def get_pool_config(pool_key):
    """
    获取池配置（自动判断是数据库池还是CSV池）
    
    参数:
    pool_key (str): 池键名
    
    返回:
    dict: 池配置字典，如果不存在返回None
    """
    # 先尝试从数据库池获取
    config = get_sector_config(pool_key)
    if config:
        config['pool_type'] = 'database'
        return config
    
    # 再尝试从CSV池获取
    config = get_csv_pool_config(pool_key)
    if config:
        config['pool_type'] = 'csv'
        return config
    
    return None

def get_pool_operation_params(pool_key):
    """
    获取池的操作参数（统一接口）
    
    参数:
    pool_key (str): 池键名
    
    返回:
    dict: 操作参数字典，包含sector_initial_cap, min_stocks, max_stocks, top_percentage
    """
    config = get_pool_config(pool_key)
    if not config:
        return None
    
    return {
        'sector_initial_cap': config['sector_initial_cap'],
        'min_stocks': config['min_stocks'],
        'max_stocks': config['max_stocks'],
        'top_percentage': config['top_percentage']
    }

def get_pool_name(pool_key):
    """
    获取池的名称（数据库名称或CSV文件路径）
    
    参数:
    pool_key (str): 池键名
    
    返回:
    str: 池名称
    """
    config = get_pool_config(pool_key)
    if not config:
        return None
    
    if config['pool_type'] == 'database':
        return config['pool_name']
    else:
        return config['file_path']

# =============================================================================
# 配置验证和显示
# =============================================================================

def print_config_summary():
    """打印配置摘要信息"""
    print("="*80)
    print("多板块操作股选择配置摘要")
    print("="*80)
    print(f"分析日期: {END_DATE}")
    print(f"账户起始资金: {ACCOUNT_CONFIG['starting_cash']:,.0f}")
    print(f"账户名称: {ACCOUNT_CONFIG['account_name']}")
    print(f"总仓位分配: {get_total_allocated_cap()*100:.1f}%")
    print(f"最大总仓位限制: {MAX_TOTAL_POSITION*100:.1f}%")
    print(f"最大板块数量: {MAX_SECTORS}")
    print(f"启用风险调整: {USE_RISK_ADJUSTMENT}")
    print()
    
    # 显示数据库池配置
    print("数据库池配置详情:")
    print("-"*80)
    print("  注意：数据库池配置已被注释，当前使用CSV池配置")
    print()
    
    # 显示CSV池配置
    if CSV_POOLS_CONFIG:
        print("CSV池配置详情:")
        print("-"*80)
        for csv_key, config in CSV_POOLS_CONFIG.items():
            print(f"CSV池: {csv_key}")
            print(f"  文件路径: {config['file_path']}")
            print(f"  代码列: {config['stock_code_column']}")
            print(f"  名称列: {config['stock_name_column']}")
        print(f"  仓位比例: {config['sector_initial_cap']*100:.1f}%")
        print(f"  股票数量: {config['min_stocks']}-{config['max_stocks']}只")
        print(f"  前百分比: {config['top_percentage']*100:.1f}%")
        print()
    
    # 显示所有池的汇总
    all_pools = get_all_pool_keys()
    print("池配置汇总:")
    print("-"*40)
    print(f"数据库池数量: {len(all_pools['database_pools'])}")
    print(f"CSV池数量: {len(all_pools['csv_pools'])}")
    print(f"总池数量: {len(all_pools['all_pools'])}")
    print(f"数据库池: {all_pools['database_pools']}")
    print(f"CSV池: {all_pools['csv_pools']}")

if __name__ == "__main__":
    # 运行配置摘要
    print_config_summary()
    
    # 验证配置
    print("\n配置验证:")
    print("-"*40)
    
    # 验证数据库池
    print("数据库池验证:")
    for sector_key in get_all_sector_keys():
        is_valid = validate_sector_config(sector_key)
        status = "✓" if is_valid else "✗"
        print(f"  {status} {sector_key}: {'有效' if is_valid else '无效'}")
    
    # 验证CSV池
    if CSV_POOLS_CONFIG:
        print("\nCSV池验证:")
        for csv_key in get_all_csv_keys():
            is_valid = validate_csv_config(csv_key)
            status = "✓" if is_valid else "✗"
            print(f"  {status} {csv_key}: {'有效' if is_valid else '无效'}")
    
    # 检查总仓位
    total_cap = get_total_allocated_cap()
    if total_cap > MAX_TOTAL_POSITION:
        print(f"\n⚠️  警告: 总仓位分配 {total_cap*100:.1f}% 超过限制 {MAX_TOTAL_POSITION*100:.1f}%")
    else:
        print(f"\n✓ 总仓位分配 {total_cap*100:.1f}% 在合理范围内")
    
    # 演示便捷调用方式
    print(f"\n便捷调用演示:")
    print("-"*40)
    
    # 演示获取所有池键名
    all_pools = get_all_pool_keys()
    print(f"所有池键名: {all_pools['all_pools']}")
    
    # 演示获取池配置
    if all_pools['all_pools']:
        sample_pool = all_pools['all_pools'][0]
        config = get_pool_config(sample_pool)
        print(f"示例池 '{sample_pool}' 配置: {config}")
        
        # 演示获取操作参数
        params = get_pool_operation_params(sample_pool)
        print(f"示例池 '{sample_pool}' 操作参数: {params}")
        
        # 演示获取池名称
        pool_name = get_pool_name(sample_pool)
        print(f"示例池 '{sample_pool}' 名称: {pool_name}")
