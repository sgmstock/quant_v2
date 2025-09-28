#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主执行文件 - 选股系统
从根目录运行，方便使用
"""

import os
import sys
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__)))

# 导入配置
from config_select_position_100w import (
    END_DATE, pingfen_bankuai_names, pools_to_load, csv_pools_to_load,
    CX_THRESHOLD, XUANGU_THRESHOLD, CX_TECH_SCORE_THRESHOLD, CX_TOTAL_SCORE_THRESHOLD
)

# 导入选股模块
from applications.select_stocks_pro import (
    load_candidate_pool_from_csv,
    load_candidate_pool_from_sector,
    initialize_sector_data,
    select_1bzl,
    update_1bzl_in_db,
    select_and_mark_cx_stock,
    update_scores_in_db
)
from data_management.sector_signal_analyzer import SectorSignalAnalyzer

def main():
    """主执行函数"""
    print("=" * 80)
    print("选股系统主执行程序")
    print("=" * 80)
    
    try:
        # --- 1. 全局配置 ---
        print(f"设定分析时间: {END_DATE}")
        print(f"评分板块: {pingfen_bankuai_names}")
        print(f"择时参数: CX阈值={CX_THRESHOLD}, XUANGU阈值={XUANGU_THRESHOLD}")
        
        # --- 2. 定义候选股池 ---
        CANDIDATE_POOLS = {}
        
        print("初始化候选股池...")
        
        # 处理板块方式
        for name, sector in pools_to_load.items():
            try:
                print(f"正在处理板块: {name} -> {sector}")
                stocks = load_candidate_pool_from_sector(sector)
                CANDIDATE_POOLS[name] = stocks
                print(f"  - {name}: {len(stocks)} 只股票")
            except Exception as e:
                print(f"处理板块 {name} 时出错: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # 处理CSV文件方式
        for name, config in csv_pools_to_load.items():
            try:
                print(f"正在处理CSV文件: {name} -> {config['file_path']}")
                stocks = load_candidate_pool_from_csv(
                    csv_file_path=config['file_path'],
                    pool_name=name,
                    stock_code_column=config.get('stock_code_column', 'stock_code'),
                    stock_name_column=config.get('stock_name_column', 'stock_name')
                )
                CANDIDATE_POOLS[name] = stocks
                print(f"  - {name}: {len(stocks)} 只股票")
            except Exception as e:
                print(f"处理CSV文件 {name} 时出错: {e}")
                import traceback
                traceback.print_exc()
                continue
                
    except Exception as e:
        print(f"候选股池初始化阶段出错: {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"\n{'='*80}\n开始执行全流程选股、标记与评分 (事件驱动模式)\n{'='*80}")
    
    for pool_name, stock_list in CANDIDATE_POOLS.items():
        print(f"\n{'*'*25} 处理板块: {pool_name.upper()} {'*'*25}")
        
        if not stock_list:
            print(f"板块 {pool_name} 股票列表为空，跳过。")
            continue

        # --- 步骤A: 先初始化数据库记录 ---
        print("--- A. 初始化数据库记录 ---")
        if not initialize_sector_data(pool_name, stock_list, END_DATE):
            print(f"✗ 板块 {pool_name} 初始化失败，跳过后续流程。")
            continue

        # --- 步骤B: 在内存中进行择时判断 ---
        print("--- B. 进行择时判断 ---")
        
        # 为了择时，需要先在内存中选出1bzl股
        sel_1bzl_stocks_mem = select_1bzl(stock_list, END_DATE)
        if not sel_1bzl_stocks_mem:
            print("✗ 未筛选出1波增量股，该板块今日无任何操作。")
            continue

        analyzer = SectorSignalAnalyzer(sel_1bzl_stocks_mem, 'backtest', END_DATE)
        cx_date = analyzer.get_cx_date(threshold=CX_THRESHOLD)
        xuangu_date = analyzer.get_xuangu_date(threshold=XUANGU_THRESHOLD)

        # 检查是否有任何事件发生
        if cx_date is None and xuangu_date is None:
            print(f"✗ 今日既不是 cx_date 也不是 xuangu_date，板块 {pool_name} 无需后续操作。")
            continue

        # --- 步骤C: 确认有事件发生，执行后续操作 ---
        print("\n✓ 发现操作信号，开始执行后续流程...") 

        # 步骤B.2: 更新1bzl标记（调用稳定函数）
        # 此函数会自己找到1bzl股并更新，虽然有微小重复计算，但保证了逻辑的稳定和正确
        if not update_1bzl_in_db(pool_name, stock_list, END_DATE):
            print(f"✗ 标记1bzl失败，终止板块 {pool_name} 的后续操作。")
            continue

        # 步骤B.3: 执行长线股标记（如果今天是cx_date）
        if cx_date:
            print("\n--- [独立流程1] 执行长线股选择与标记 ---")
            select_and_mark_cx_stock(pool_name, stock_list, END_DATE, pingfen_bankuai_names)
        
        # 步骤B.4: 执行评分更新（如果今天是xuangu_date）
        if xuangu_date:
            print("\n--- [独立流程2] 执行xuangu评分更新 ---")
            update_scores_in_db(pool_name, stock_list, END_DATE, pingfen_bankuai_names)

    print(f"\n{'='*80}\n所有板块分析流程执行完毕。\n{'='*80}")

if __name__ == "__main__":
    main()
