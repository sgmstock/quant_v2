# =============================================================================
# select_stocks_pro.py
# 职责：每日盘后运行，为所有预设板块在中央数据库中准备好当日的选股、标记和评分数据。
# =============================================================================
import pandas as pd
from datetime import datetime
import numpy as np
import os
import sys
import warnings

# 添加项目根目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# --- 核心模块导入 ---
from core.utils.stock_filter import StockXihua, get_bankuai_stocks, total_scores
from applications.sel_1bzl import SectorMomentumAnalyzer
from data_management.sector_signal_analyzer import SectorSignalAnalyzer
from data_management.database_manager import DatabaseManager

warnings.filterwarnings('ignore')

# 使用v2项目的数据库管理器
print("正在初始化数据库管理器...")
try:
    db_manager = DatabaseManager()
    print("数据库管理器初始化成功")
except Exception as e:
    print(f"数据库管理器初始化失败: {e}")
    raise

# =============================================================================
# ===== 核心数据库操作函数 =====
# =============================================================================

def initialize_sector_data(pool_name, stock_list, end_date):
    """步骤一：将板块的基础股票信息初始化到数据库。"""
    print(f"\n--- 1.1 初始化板块数据: {pool_name} ---")
    if not stock_list: return False
    
    try:
        # 首先确保daily_selections表存在
        create_table_query = """
            CREATE TABLE IF NOT EXISTS daily_selections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_name TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                name TEXT,
                trade_date TEXT NOT NULL,
                is_1bzl INTEGER DEFAULT 0,
                is_cx INTEGER DEFAULT 0,
                总得分 REAL DEFAULT 0.0,
                技术得分 REAL DEFAULT 0.0,
                主力得分 REAL DEFAULT 0.0,
                板块得分 REAL DEFAULT 0.0,
                低BIAS得分 REAL DEFAULT 0.0,
                UNIQUE(pool_name, stock_code, trade_date)
            )
        """
        db_manager.execute_ddl(create_table_query)
        
        # 为了幂等性，先删除当天该板块的旧数据
        delete_query = "DELETE FROM daily_selections WHERE pool_name = ? AND trade_date = ?"
        db_manager.execute_dml(delete_query, (pool_name, end_date))
        
        # 从 stock_basic 表获取名称
        placeholders = ','.join(['?' for _ in stock_list])
        query = f"SELECT stock_code, stock_name AS name FROM stock_basic WHERE stock_code IN ({placeholders})"
        df = db_manager.execute_query(query, tuple([str(s).zfill(6) for s in stock_list]))

        if df.empty:
            print(f"✗ 数据库中未找到 {pool_name} 的股票信息。")
            return False
            
        # 添加额外字段并存入数据库
        df['pool_name'] = pool_name
        df['trade_date'] = end_date
        df['is_1bzl'] = 0
        df['is_cx'] = 0  # 新增长线股标记字段
        score_columns = ['总得分', '技术得分', '主力得分', '板块得分', '低BIAS得分']
        for col in score_columns: df[col] = 0.0
        
        # 使用批量插入保存数据
        insert_query = """
            INSERT INTO daily_selections 
            (pool_name, stock_code, name, trade_date, is_1bzl, is_cx, 总得分, 技术得分, 主力得分, 板块得分, 低BIAS得分)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # 准备数据
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                row['pool_name'],
                row['stock_code'], 
                row['name'],
                row['trade_date'],
                row['is_1bzl'],
                row['is_cx'],
                row['总得分'],
                row['技术得分'],
                row['主力得分'],
                row['板块得分'],
                row['低BIAS得分']
            ))
        
        # 批量插入
        for data_row in data_to_insert:
            db_manager.execute_dml(insert_query, data_row)
        
        print(f"✓ 成功初始化 {len(df)} 条记录到数据库。")
        return True
    except Exception as e:
        print(f"✗ 初始化板块数据失败: {e}")
        return False

def select_1bzl(stock_list, end_date):
    """挑选1波中级增量股。"""
    if not stock_list: return []
    try:
        analyzer = SectorMomentumAnalyzer(stock_list, end_date)
        analyzer.run_analysis()
        results = analyzer.get_results()
        if (isinstance(results, dict) and 
            'final_selection' in results):
            final_selection = results['final_selection']
            if (hasattr(final_selection, 'columns') and 
                hasattr(final_selection, '__getitem__') and
                '股票代码' in final_selection.columns):
                return final_selection['股票代码'].tolist()
        return []
    except Exception as e:
        print(f"✗ 1bzl筛选失败: {e}")
        return []

def update_1bzl_in_db(pool_name, stock_list, end_date):
    """步骤二：筛选1波增量股，并更新数据库中的 is_1bzl 标记。"""
    print(f"--- 1.2 更新 'is_1bzl' 标记 for {pool_name} ---")
    
    sel_1bzl_stocks = select_1bzl(stock_list, end_date)
    if not sel_1bzl_stocks:
        print("✗ 未筛选出1波增量股，不作修改。")
        return False
        
    try:
        placeholders = ','.join(['?' for _ in sel_1bzl_stocks])
        
        # 使用DatabaseManager执行更新
        update_query = f"""
            UPDATE daily_selections 
            SET is_1bzl = 1 
            WHERE pool_name = ? AND trade_date = ? AND stock_code IN ({placeholders})
        """
        params = [pool_name, end_date] + [str(s).zfill(6) for s in sel_1bzl_stocks]
        
        # 执行更新查询
        db_manager.execute_query(update_query, tuple(params))
        print(f"✓ 成功标记 {len(sel_1bzl_stocks)} 只1bzl股票。")
        return True
    except Exception as e:
        print(f"✗ 更新is_1bzl失败: {e}")
        return False


def update_scores_in_db(pool_name, stock_list, end_date, pingfen_bankuai_names):
    """
    (业务逻辑修正版)
    只有xuangu_date的评分才能写入数据库。
    cx_date只用于内存中的长线股选择，不写入数据库。
    """
    print(f"\n--- 1.3 更新评分 for {pool_name} ---")
    
    try:
        # 1. 从数据库读取当天该板块的基础数据
        query = "SELECT stock_code, is_1bzl FROM daily_selections WHERE pool_name = ? AND trade_date = ?"
        df = db_manager.execute_query(query, (pool_name, end_date))
        
        if df.empty:
            print(f"✗ 数据库中未找到板块 {pool_name} 在 {end_date} 的数据。")
            return False
        
        sel_1bzl_stocks = df[df['is_1bzl'] == 1]['stock_code'].tolist()
        
        print(f"候选股池数量: {len(stock_list)}")
        print(f"1波增量股数量: {len(sel_1bzl_stocks)}")

        # 2. 核心安全阀：无1bzl股则不进行后续操作
        if not sel_1bzl_stocks:
            print("✗ 未筛选出1波增量股，根据策略终止评分。")
            return False
            
        # 3. 创建板块分析器
        analyzer = SectorSignalAnalyzer(sel_1bzl_stocks, 'backtest', end_date)
        
        # 4. xuangu择时和评分（唯一写入数据库的评分）
        print("\n=== xuangu择时评分 ===")
        xuangu_date = analyzer.get_xuangu_date(threshold=0.32)
        
        if xuangu_date is None:
            print("✗ 未找到合适的xuangu选股时间，评分保持为0。")
            return False
        
        print(f"✓ xuangu选股时间确定为: {xuangu_date}")
        
        # 5. 对整个板块在xuangu_date进行评分
        xuangu_scores_df = total_scores(stock_list, *pingfen_bankuai_names, date=xuangu_date)
        if xuangu_scores_df.empty:
            print("✗ xuangu评分函数未返回任何评分数据。")
            return False
            
        print(f"✓ xuangu评分函数成功计算了 {len(xuangu_scores_df)} 只股票的分数。")
        print(f"  > 评分结果列名: {xuangu_scores_df.columns.tolist()}")
        print(f"  > 评分数据样本:\n{xuangu_scores_df.head().to_string()}")

        # 6. 更新xuangu评分到数据库（保留is_cx值）
        xuangu_update_count = 0
        for _, row in xuangu_scores_df.iterrows():
            # 注意：这里不更新is_cx字段，保持其原有值
            update_query = """
                UPDATE daily_selections 
                SET 总得分 = ?, 技术得分 = ?, 主力得分 = ?, 板块得分 = ?, 低BIAS得分 = ?
                WHERE pool_name = ? AND trade_date = ? AND stock_code = ?
            """
            params = (
                row.get('总得分', 0.0), 
                row.get('技术得分', 0.0), 
                row.get('主力得分', 0.0),
                row.get('板块得分', 0.0), 
                row.get('低BIAS得分', 0.0),
                pool_name, 
                end_date, 
                str(row['stock_code']).zfill(6)
            )
            db_manager.execute_query(update_query, params)
            xuangu_update_count += 1
        
        if xuangu_update_count > 0:
            print(f"✓ xuangu评分：成功更新数据库，共更新了 {xuangu_update_count} 条记录的评分。")
            return True
        else:
            print("⚠️ xuangu评分已计算，但数据库中没有匹配的记录被更新。")
            return False

    except Exception as e:
        print(f"✗ 更新评分时发生严重错误: {e}")
        import traceback
        traceback.print_exc()
        return False


def select_and_mark_cx_stock(pool_name, stock_list, end_date, pingfen_bankuai_names):
    """
    选择并标记长线股(cx股)的函数
    
    业务逻辑修正版：
    1. 在内存中完成所有cx选股计算，不依赖数据库中的评分数据
    2. 只有最终的is_cx=1标记写入数据库，cx评分数据绝不写入数据库
    3. 完全独立于xuangu_date的评分流程
    """
    print(f"\n--- 长线股选择与标记 for {pool_name} ---")
    
    try:
        # 1. 从数据库读取当天该板块的基础数据（仅读取is_1bzl标记）
        query = "SELECT stock_code, is_1bzl FROM daily_selections WHERE pool_name = ? AND trade_date = ?"
        df = db_manager.execute_query(query, (pool_name, end_date))
        
        if df.empty:
            print(f"✗ 数据库中未找到板块 {pool_name} 在 {end_date} 的数据。")
            return False
        
        # 获取1波增量股用于择时判断
        sel_1bzl_stocks = df[df['is_1bzl'] == 1]['stock_code'].tolist()
        
        print(f"候选股池数量: {len(stock_list)}")
        print(f"1波增量股数量: {len(sel_1bzl_stocks)}")

        # 2. 核心安全阀：无1bzl股则不进行后续操作
        if not sel_1bzl_stocks:
            print("✗ 未筛选出1波增量股，根据策略终止长线股选择。")
            return False
        
        # 3. 进行板块择时（获取cx_date）
        analyzer = SectorSignalAnalyzer(sel_1bzl_stocks, 'backtest', end_date)
        cx_date = analyzer.get_cx_date(threshold=0.1)
        
        if cx_date is None:
            print("✗ cx择时条件不满足，终止长线股选择。")
            return False
        print(f"✓ cx择时条件满足，cx_date: {cx_date}")
        
        # 4. 在内存中计算cx_date的综合评分（绝不写入数据库）
        print("正在内存中计算cx_date的综合评分...")
        cx_total_scores_df = total_scores(stock_list, *pingfen_bankuai_names, date=cx_date)
        
        if cx_total_scores_df.empty:
            print("✗ cx综合评分计算失败。")
            return False
        print(f"✓ cx综合评分计算成功，共 {len(cx_total_scores_df)} 只股票。")
        
        # 5. 在内存中计算cx_date的技术分析cx_score
        print("正在内存中计算cx_date的技术分析...")
        from core.technical_analyzer.stock_technical_analyzer import StockTechnicalAnalyzer
        tech_analyzer = StockTechnicalAnalyzer()
        tech_scores_df = tech_analyzer.get_jishu_scores(stock_list, cx_date)
        
        if tech_scores_df.empty:
            print("✗ cx技术分析计算失败。")
            return False
        print(f"✓ cx技术分析计算成功，共 {len(tech_scores_df)} 只股票。")
        
        # 6. 在内存中合并评分数据
        combined_df = cx_total_scores_df.merge(
            tech_scores_df[['stock_code', 'cx_score']], 
            on='stock_code', 
            how='left'
        )
        
        # 填充缺失的cx_score
        combined_df['cx_score'] = combined_df['cx_score'].fillna(0)
        
        # 7. 在内存中筛选符合条件的股票：cx_score>=4, total_score>=9
        qualified_stocks = combined_df[
            (combined_df['cx_score'] >= 4) & 
            (combined_df['总得分'] >= 9)
        ]
        
        if qualified_stocks.empty:
            print("✗ 没有股票满足长线股条件（cx_score>=4 且 总得分>=9）。")
            print("  当前cx评分情况:")
            for _, row in combined_df.iterrows():
                print(f"    {row['stock_code']}: cx_score={row['cx_score']:.1f}, 总得分={row['总得分']:.1f}")
            return False
        
        print(f"✓ 找到 {len(qualified_stocks)} 只符合长线股条件的股票。")
        
        # 8. 在内存中找出总得分最高的股票
        best_cx_stock = qualified_stocks.loc[qualified_stocks['总得分'].idxmax()]
        best_stock_code = str(best_cx_stock['stock_code']).zfill(6)
        
        print(f"✓ 选出最佳长线股: {best_stock_code}，cx_score={best_cx_stock['cx_score']:.1f}, 总得分={best_cx_stock['总得分']:.2f}")
        
        # 9. 仅将is_cx=1标记写入数据库（不写入任何评分数据）
        update_query = """
            UPDATE daily_selections 
            SET is_cx = 1 
            WHERE pool_name = ? AND trade_date = ? AND stock_code = ?
        """
        db_manager.execute_query(update_query, (pool_name, end_date, best_stock_code))
        
        print(f"✓ 成功标记长线股: {best_stock_code}")
        print("⚠️ 注意：cx评分数据未写入数据库，仅标记is_cx=1。")
        return True

    except Exception as e:
        print(f"✗ 长线股选择与标记时发生严重错误: {e}")
        import traceback
        traceback.print_exc()
        return False


# =============================================================================
# ===== 主执行流程 =====
# =============================================================================
if __name__ == "__main__":
    print("脚本开始执行...")
    try:
        # --- 1. 全局配置 ---
        END_DATE = '2025-09-15' #datetime.now().strftime('%Y-%m-%d')
        pingfen_bankuai_names = ['低价']
        print(f"设定分析时间: {END_DATE}")
    except Exception as e:
        print(f"配置阶段出错: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

    try:
        # --- 2. 定义候选股池 ---
        CANDIDATE_POOLS = {}
        pools_to_load = {'houxuan_lvyou': '旅游及景区'}
        print("初始化候选股池...")
        for name, sector in pools_to_load.items():
            try:
                print(f"正在处理板块: {name} -> {sector}")
                # 使用v2项目的技术分析器
                from core.technical_analyzer.stock_technical_analyzer import StockTechnicalAnalyzer 
                print("导入StockTechnicalAnalyzer成功")
                
                print("正在获取板块股票...")
                stocks = get_bankuai_stocks(sector)
                print(f"获取到 {len(stocks)} 只股票")
                
                print("正在筛选基础条件...")
                stocks = StockXihua().filter_basic_conditions(stocks)
                print(f"筛选后剩余 {len(stocks)} 只股票")
                
                CANDIDATE_POOLS[name] = stocks
                print(f"  - {name}: {len(stocks)} 只股票")
            except Exception as e:
                print(f"处理板块 {name} 时出错: {e}")
                import traceback
                traceback.print_exc()
                continue
    except Exception as e:
        print(f"候选股池初始化阶段出错: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

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
        cx_date = analyzer.get_cx_date(threshold=0.1)
        xuangu_date = analyzer.get_xuangu_date(threshold=0.32)

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