import pandas as pd
import sqlite3
import os
import sys
from datetime import datetime

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 导入v2项目的模块
from core.execution.account import Account
from data_management.data_processor import get_latest_price, get_daily_data_for_backtest
from core.utils.indicators import ATR


# --- 系统参数设定 ---
# 默认账户（可以支持多账户）
DEFAULT_ACCOUNT = Account(starting_cash=1000000.0)
SECTOR_INITIAL_CAP = 0.25                  # 板块初始仓位比例，默认0.25

def get_db_path():
    """获取v2项目的数据库路径"""
    return os.path.join(current_dir, 'databases', 'quant_system.db')


def get_atr_volatility(stock_code, current_date, period=14):
    """
    获取股票的ATR波动率指标
    
    参数:
    stock_code: 股票代码
    current_date: 当前日期（回测日期）
    period: ATR计算周期，默认14天
    
    返回:
    float: ATR值，如果获取失败返回默认值
    """
    try:
        # 获取历史数据
        df = get_daily_data_for_backtest(stock_code, current_date)
        
        if df.empty or len(df) < period:
            print(f"警告: {stock_code} 数据不足，使用默认波动率")
            return 0.02  # 默认2%的波动率
        
        # 确保列名正确
        df = df.rename(columns={
            'trade_date': 'date',
            'open_price': 'open',
            'high_price': 'high', 
            'low_price': 'low',
            'close_price': 'close'
        })
        
        # 计算ATR
        atr_values, tr_values = ATR(df['close'].values, df['high'].values, df['low'].values, N=period)
        
        # 获取最新的ATR值
        latest_atr = atr_values[-1] if len(atr_values) > 0 else 0.02
        
        # 将ATR转换为相对波动率（相对于当前价格）
        current_price = df['close'].iloc[-1]
        relative_volatility = latest_atr / current_price if current_price > 0 else 0.02
        
        return max(relative_volatility, 0.01)  # 最小波动率1%
        
    except Exception as e:
        print(f"获取 {stock_code} ATR失败: {e}")
        return 0.02  # 默认波动率


def calculate_risk_adjusted_allocation(stock_scores, current_date):
    """
    基于ATR指标计算风险调整后的投入比例
    
    参数:
    stock_scores: dict, {股票代码: 评分}
    current_date: 当前日期（回测日期）
    
    返回:
    dict: {股票代码: 风险调整后投入比例}
    """
    risk_adjusted_scores = {}
    total_adjusted_score = 0
    
    print(f"开始计算风险调整后的投入比例，当前日期: {current_date}")
    
    # 第一步：计算风险调整后分数
    for stock_code, score in stock_scores.items():
        volatility = get_atr_volatility(stock_code, current_date)
        adjusted_score = score / volatility  # 风险调整后分数
        risk_adjusted_scores[stock_code] = adjusted_score
        total_adjusted_score += adjusted_score
        
        print(f"{stock_code}: 原始评分={score:.2f}, ATR波动率={volatility:.4f}, 调整后评分={adjusted_score:.2f}")
    
    # 第二步：计算最终权重
    final_weights = {}
    for stock_code, adjusted_score in risk_adjusted_scores.items():
        weight = adjusted_score / total_adjusted_score if total_adjusted_score > 0 else 0
        final_weights[stock_code] = weight
    
    print(f"风险调整完成，总调整后评分: {total_adjusted_score:.2f}")
    return final_weights







def is_cx_stock_by_holdings_table(stock_code, db_path=None):
    """
    【新方案】通过CX持仓记录表判断股票是否为长线股
    
    使用独立的cx_strategy_holdings表来准确识别CX持仓股票
    这是最稳健和解耦的方法
    
    参数:
    stock_code (str): 股票代码
    db_path (str): 数据库路径
    
    返回:
    tuple: (是否为长线股, 来源板块, 买入日期, 详细原因)
    """
    if db_path is None:
        db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 查询CX持仓记录表
        cx_holdings_query = """
            SELECT stock_code, stock_name, source_pool, buy_date, buy_price, buy_quantity
            FROM cx_strategy_holdings 
            WHERE stock_code = ?
        """
        cx_df = pd.read_sql_query(cx_holdings_query, conn, params=[stock_code])
        
        if not cx_df.empty:
            record = cx_df.iloc[0]
            conn.close()
            return (
                True, 
                record['source_pool'], 
                record['buy_date'], 
                f"CX持仓股票 (买入价格: {record['buy_price']:.2f}, 数量: {record['buy_quantity']})"
            )
        
        conn.close()
        return False, None, None, "不在CX持仓记录中"
        
    except Exception as e:
        print(f"查询CX持仓记录时发生错误: {e}")
        if 'conn' in locals():
            conn.close()
        return False, None, None, f"查询错误: {e}"


def calculate_sector_used_amount(pool_name, account, db_path=None):
    """
    计算板块已用额度 - 关键的第二步
    
    逻辑：
    1. 获取所有当前持仓股票
    2. 对每只持仓股票，检查是否属于该板块：
       a) 检查历史上是否通过该pool_name买入过（在daily_selections表中）
       b) 检查是否标记为is_cx=1的长线股
       c) 可扩展：检查股票的板块归属（通过get_sector_name等方法）
    3. 计算所有属于该板块的持仓股票的市值总和
    
    参数:
    pool_name (str): 板块名称
    account (Account): 账户对象
    db_path (str): 数据库路径
    
    返回:
    tuple: (板块已用额度, 板块持仓股票列表, 详细信息字典)
    """
    if db_path is None:
        db_path = get_db_path()
    
    current_positions = account.positions
    print(f"\n--- 计算板块 {pool_name} 已用额度 ---")
    print(f"当前总持仓股票: {list(current_positions.keys())}")
    
    sector_used_amount = 0
    sector_stocks = []
    stock_details = []
    
    try:
        conn = sqlite3.connect(db_path)
        
        for stock_code, position in current_positions.items():
            belongs_to_sector = False
            reason = ""
            
            # 方法1：检查历史上是否通过该pool_name买入过
            history_query = """
                SELECT COUNT(*) as count, MAX(trade_date) as latest_date
                FROM daily_selections 
                WHERE pool_name = ? AND stock_code = ?
            """
            history_df = pd.read_sql_query(history_query, conn, params=[pool_name, stock_code])
            
            if not history_df.empty and history_df['count'].iloc[0] > 0:
                belongs_to_sector = True
                reason = f"历史属于板块{pool_name} (最新日期: {history_df['latest_date'].iloc[0]})"
            
            # 方法2：检查是否为CX长线股（使用CX持仓记录表）
            if not belongs_to_sector:
                is_cx, cx_pool, cx_date, cx_reason = is_cx_stock_by_holdings_table(stock_code, db_path)
                if is_cx:
                    belongs_to_sector = True
                    reason = f"CX长线股 来源板块: {cx_pool} (买入日期: {cx_date}) - {cx_reason}"
            
            # 如果属于该板块，计算仓位
            if belongs_to_sector:
                # 计算持仓市值 = 持仓数量 * 当前市价（或平均成本价）
                # 这里使用平均成本价，也可以改为当前市价
                stock_position_value = position.total_amount * position.avg_cost
                sector_used_amount += stock_position_value
                sector_stocks.append(stock_code)
                
                stock_detail = {
                    'stock_code': stock_code,
                    'quantity': position.total_amount,
                    'avg_cost': position.avg_cost,
                    'position_value': stock_position_value,
                    'reason': reason
                }
                stock_details.append(stock_detail)
                
                print(f"  ✓ {stock_code}: 数量={position.total_amount}, 成本价={position.avg_cost:.2f}, "
                      f"仓位={stock_position_value:,.2f} ({reason})")
            else:
                print(f"  - {stock_code}: 不属于板块{pool_name}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 计算板块已用额度时发生错误: {e}")
        if 'conn' in locals():
            conn.close()
        return 0, [], []
    
    print(f"--- 板块 {pool_name} 已用额度计算完成 ---")
    print(f"板块持仓股票: {sector_stocks}")
    print(f"板块已用额度: {sector_used_amount:,.2f}")
    
    return sector_used_amount, sector_stocks, stock_details


def check_sector_position_availability(pool_name, end_date, sector_initial_cap=None, account=None, db_path=None):
    """
    判断板块仓位还有多少剩余可操作资金，确定是否板块还有加仓机会
    
    【修正版】使用正确的板块已用额度计算方法
    
    参数:
    pool_name (str): 板块名称
    end_date (str): 交易日期
    sector_initial_cap (float): 板块初始仓位比例，默认使用全局设置
    account (Account): 账户对象，默认使用默认账户
    db_path (str): 数据库路径
    
    返回:
    dict: 包含仓位信息的字典
    """
    if db_path is None:
        db_path = get_db_path()
    
    if sector_initial_cap is None:
        sector_initial_cap = SECTOR_INITIAL_CAP
    
    if account is None:
        account = DEFAULT_ACCOUNT
    
    # 实时获取账户总权益
    total_equity = account.get_total_equity()
    
    print(f"\n{'='*60}")
    print("板块仓位管理分析 【修正版】")
    print(f"{'='*60}")
    print(f"使用账户: {account.table_name}")
    
    # 第一步：计算板块总额度
    initial_sector_position = total_equity * sector_initial_cap
    print(f"账户总权益: {total_equity:,.2f}")
    print(f"板块初始仓位比例: {sector_initial_cap*100}%")
    print(f"板块总额度: {initial_sector_position:,.2f}")
    
    # 第二步：计算板块已用额度（关键修正）
    current_sector_position, sector_stocks, stock_details = calculate_sector_used_amount(pool_name, account, db_path)
    
    # 2. 从数据库读取候选股数据（用于显示信息）
    try:
        conn = sqlite3.connect(db_path)
        query = """
            SELECT stock_code
            FROM daily_selections 
            WHERE pool_name = ? AND trade_date = ?
        """
        df = pd.read_sql_query(query, conn, params=[pool_name, end_date])
        conn.close()
        candidate_stocks = df['stock_code'].astype(str).str.zfill(6).tolist()
        print(f"\n当前候选股数量: {len(candidate_stocks)}")
        print(f"当前候选股列表: {candidate_stocks}")
    except Exception as e:
        print(f"从数据库读取候选股数据失败: {e}")
        candidate_stocks = []
    
    print(f"\n板块已用额度: {current_sector_position:,.2f}")
    print(f"板块持仓股票: {sector_stocks}")
    
    # 第三步：计算本次可用资金
    if current_sector_position >= initial_sector_position:
        print(f"❌ 板块仓位已达到或超过总额度，不加仓")
        return {
            'can_add_position': False,
            'reason': '板块仓位已达到或超过总额度',
            'current_sector_position': current_sector_position,
            'initial_sector_position': initial_sector_position,
            'available_cash': account.available_cash,
            'sector_available_cash': 0,
            'sector_stocks': sector_stocks,
            'candidate_stocks': candidate_stocks,
            'stock_details': stock_details
        }
    
    # 计算板块可用资金
    sector_available_cash = initial_sector_position - current_sector_position
    print(f"本次可用资金: {sector_available_cash:,.2f}")
    
    # 获取账户剩余资金
    available_cash = account.available_cash
    print(f"账户剩余资金: {available_cash:,.2f}")
    
    # 判断是否可以继续买进
    can_add_position = available_cash >= sector_available_cash
    
    if can_add_position:
        print(f"✅ 板块可继续买进，剩余可操作资金: {sector_available_cash:,.2f}")
    else:
        print(f"❌ 账户资金不足，需要资金: {sector_available_cash:,.2f}，可用资金: {available_cash:,.2f}")
    
    return {
        'can_add_position': can_add_position,
        'reason': '资金充足' if can_add_position else '资金不足',
        'current_sector_position': current_sector_position,
        'initial_sector_position': initial_sector_position,
        'sector_available_cash': sector_available_cash,
        'available_cash': available_cash,
        'sector_stocks': sector_stocks,
        'candidate_stocks': candidate_stocks,
        'stock_details': stock_details
    }

# 使用示例（已移至文件末尾的完整main函数）



# 从数据库读取板块数据，来获取板块的操作个股===========================================
def get_buy_stocks_from_db(pool_name, end_date, min_stocks=3, max_stocks=5, top_percentage=0.2, db_path=None):
    """
    从数据库获取操作个股
    
    参数:
    pool_name (str): 板块名称
    end_date (str): 交易日期
    min_stocks (int): 最少股票数量，默认3只
    max_stocks (int): 最多股票数量，默认5只
    top_percentage (float): 前百分比，默认0.2（20%）
    db_path (str): 数据库路径
    
    返回:
    tuple: (选中的股票代码列表, 筛选后的DataFrame)
    """
    if db_path is None:
        db_path = get_db_path()
    
    try:
        # 1. 从数据库读取数据
        conn = sqlite3.connect(db_path)
        query = """
            SELECT stock_code, name, is_1bzl, 总得分, 技术得分, 主力得分, 板块得分, 低BIAS得分
            FROM daily_selections 
            WHERE pool_name = ? AND trade_date = ?
        """
        df = pd.read_sql_query(query, conn, params=[pool_name, end_date])
        conn.close()
        
        print(f"从数据库读取板块 {pool_name} 在 {end_date} 的数据:")
        print(f"原始数据行数: {len(df)}")
        
        if len(df) == 0:
            print("数据库中没有该板块的数据，返回空列表")
            return [], pd.DataFrame()
        
        # 2. 去掉创业板(300开头)和科创板(688开头)
        # 先将stock_code转换为字符串类型，并补齐6位数字
        df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
        df_filtered = df[~df['stock_code'].str.startswith(('30', '68'))]
        print(f"去掉创业板和科创板后剩余 {len(df_filtered)} 只股票")
        
        if len(df_filtered) == 0:
            print("筛选后无股票，返回空列表")
            return [], pd.DataFrame()
        
        # 3. 计算前百分比的股票数量
        n = int(len(df_filtered) * top_percentage)
        if n < min_stocks:
            n = min_stocks
        elif n > max_stocks:
            n = max_stocks
        
        print(f"选取前{n}只股票（前{top_percentage*100}%，最少{min_stocks}只，最多{max_stocks}只）")
        
        # 4. 选取总得分前n的个股
        top_n_stocks = df_filtered.nlargest(n, '总得分')  # type: ignore
        print(f"\n总得分前{n}的个股:")
        print(top_n_stocks[['stock_code', '总得分']].to_string(index=False))  # type: ignore
        
        # 5. 提取操作个股股票代码列表
        buy_stocks = top_n_stocks['stock_code'].tolist()
        print(f"\n从数据库选中的股票代码: {buy_stocks}")
        
        return buy_stocks, top_n_stocks
        
    except Exception as e:
        print(f"从数据库读取数据时发生错误: {e}")
        return [], pd.DataFrame()




def calculate_individual_stock_allocation(pool_name, end_date, sector_initial_cap=None, account=None, use_risk_adjustment=True, db_path=None):
    """
    计算个股资金分配和买入判断
    
    参数:
    pool_name (str): 板块名称
    end_date (str): 交易日期
    sector_initial_cap (float): 板块初始仓位比例，默认使用全局设置
    account (Account): 账户对象，默认使用默认账户
    use_risk_adjustment (bool): 是否使用ATR风险调整，默认True
    db_path (str): 数据库路径
    
    返回:
    dict: 包含个股分配信息的字典
    """
    if db_path is None:
        db_path = get_db_path()
    
    if sector_initial_cap is None:
        sector_initial_cap = SECTOR_INITIAL_CAP
    
    if account is None:
        account = DEFAULT_ACCOUNT
    
    # 实时获取账户总权益
    total_equity = account.get_total_equity()
    
    print(f"\n{'='*60}")
    print("个股资金分配分析")
    print(f"{'='*60}")
    print(f"使用账户: {account.table_name}")
    
    # 1. 获取板块操作个股和筛选后的DataFrame（避免重复读取数据库）
    buy_stocks, operation_stocks_df = get_buy_stocks_from_db(pool_name, end_date, db_path=db_path)
    
    if not buy_stocks or operation_stocks_df.empty:
        print("❌ 无操作个股，无法进行资金分配")
        return {
            'can_buy': False,
            'reason': '无操作个股',
            'buy_stocks': buy_stocks,
            'stock_allocations': []
        }
    
    # 3. 计算板块初始仓位
    initial_sector_position = total_equity * sector_initial_cap
    print(f"板块初始仓位: {initial_sector_position:,.2f}")
    
    # 4. 计算个股投入比例和资金
    total_score = operation_stocks_df['总得分'].sum()
    print(f"操作个股总得分: {total_score:.2f}")
    
    stock_allocations = []
    
    if use_risk_adjustment:
        # 使用ATR风险调整的分配方式
        print("\n使用ATR风险调整分配方式")
        
        # 构建股票评分字典
        stock_scores_dict = {}
        for _, row in operation_stocks_df.iterrows():  # type: ignore
            stock_scores_dict[row['stock_code']] = row['总得分']
        
        # 获取当前日期（用于ATR计算）
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 计算风险调整后的投入比例
        risk_adjusted_weights = calculate_risk_adjusted_allocation(stock_scores_dict, current_date)
        
        for _, row in operation_stocks_df.iterrows():  # type: ignore
            stock_code = row['stock_code']
            stock_score = row['总得分']
            
            # 使用风险调整后的投入比例
            allocation_ratio = risk_adjusted_weights.get(stock_code, 0)
            # 个股投入资金 = 板块初始仓位 * 投入比例
            stock_investment = initial_sector_position * allocation_ratio
            
            stock_info = {
                'stock_code': stock_code,
                'stock_score': stock_score,
                'allocation_ratio': allocation_ratio,
                'investment_amount': stock_investment
            }
            stock_allocations.append(stock_info)
            
            print(f"  {stock_code}: 得分={stock_score:.2f}, 风险调整后占比={allocation_ratio*100:.1f}%, 投入资金={stock_investment:,.2f}")
    else:
        # 使用传统的简单比例分配方式
        print("\n使用传统比例分配方式")
        
        for _, row in operation_stocks_df.iterrows():  # type: ignore
            stock_code = row['stock_code']
            stock_score = row['总得分']
            
            # 个股投入比例 = 个股评分 / 总分
            allocation_ratio = stock_score / total_score
            # 个股投入资金 = 板块初始仓位 * 投入比例
            stock_investment = initial_sector_position * allocation_ratio
            
            stock_info = {
                'stock_code': stock_code,
                'stock_score': stock_score,
                'allocation_ratio': allocation_ratio,
                'investment_amount': stock_investment
            }
            stock_allocations.append(stock_info)
            
            print(f"  {stock_code}: 得分={stock_score:.2f}, 占比={allocation_ratio*100:.1f}%, 投入资金={stock_investment:,.2f}")
    
    # 5. 检查板块可用资金
    position_result = check_sector_position_availability(pool_name, end_date, sector_initial_cap, account, db_path)
    sector_available_cash = position_result['sector_available_cash']
    
    print(f"\n板块可用资金: {sector_available_cash:,.2f}")
    
    # 6. 按优先级购买逻辑：按分配比例从高到低排序，直到钱不够为止
    # 按分配比例从高到低排序（优先购买权重高的股票）
    stock_allocations_sorted = sorted(stock_allocations, key=lambda x: x['allocation_ratio'], reverse=True)
    
    buyable_stocks = []
    total_required_cash = 0
    remaining_cash = sector_available_cash
    
    print(f"\n按优先级购买分析（可用资金: {remaining_cash:,.2f}）:")
    print("-" * 60)
    
    for stock_info in stock_allocations_sorted:
        required_cash = stock_info['investment_amount']
        
        # 判断是否有足够资金购买这只股票
        can_buy_individual = remaining_cash >= required_cash
        
        if can_buy_individual:
            # 可以购买，扣除资金
            remaining_cash -= required_cash
            total_required_cash += required_cash
            buyable_stocks.append(stock_info['stock_code'])
            stock_info['can_buy'] = True
            print(f"✅ {stock_info['stock_code']}: 可买入，需要资金 {required_cash:,.2f}，剩余资金 {remaining_cash:,.2f}")
        else:
            # 资金不足，无法购买
            stock_info['can_buy'] = False
            print(f"❌ {stock_info['stock_code']}: 资金不足，需要资金 {required_cash:,.2f}，剩余资金 {remaining_cash:,.2f}")
    
    # 7. 总体判断：只要有可买入的股票就算成功
    can_buy_overall = len(buyable_stocks) > 0
    
    print(f"\n{'='*40}")
    print("资金分配汇总")
    print(f"{'='*40}")
    print(f"操作个股总数: {len(buy_stocks)}")
    print(f"可买入个股数: {len(buyable_stocks)}")
    print(f"可买入个股: {buyable_stocks}")
    print(f"实际投入资金: {total_required_cash:,.2f}")
    print(f"板块可用资金: {sector_available_cash:,.2f}")
    print(f"剩余可用资金: {remaining_cash:,.2f}")
    print(f"资金利用率: {(total_required_cash/sector_available_cash*100):.1f}%")
    print(f"总体可买入: {'是' if can_buy_overall else '否'}")
    
    return {
        'can_buy': can_buy_overall,
        'reason': f'可买入{len(buyable_stocks)}只个股' if can_buy_overall else '资金不足或无操作个股',
        'buy_stocks': buy_stocks,
        'buyable_stocks': buyable_stocks,
        'stock_allocations': stock_allocations,
        'total_required_cash': total_required_cash,
        'sector_available_cash': sector_available_cash,
        'remaining_cash': remaining_cash,
        'cash_utilization_rate': total_required_cash/sector_available_cash if sector_available_cash > 0 else 0,
        'position_result': position_result
    }

def get_individual_stock_buy_signals(pool_name, end_date, sector_initial_cap=None, account=None, db_path=None):
    """
    获取个股待买入信号
    
    参数:
    pool_name (str): 板块名称
    end_date (str): 交易日期
    sector_initial_cap (float): 板块初始仓位比例
    account (Account): 账户对象，默认使用默认账户
    db_path (str): 数据库路径
    
    返回:
    list: 买入信号列表，每个信号包含股票代码、买入数量、买入价格等信息
    """
    if db_path is None:
        db_path = get_db_path()
    allocation_result = calculate_individual_stock_allocation(pool_name, end_date, sector_initial_cap, account, use_risk_adjustment=True, db_path=db_path)
    
    if not allocation_result['can_buy']:
        print(f"❌ 无法生成买入信号: {allocation_result['reason']}")
        return []
    
    buy_signals = []
    
    for stock_info in allocation_result['stock_allocations']:
        if stock_info['can_buy']:
            stock_code = stock_info['stock_code']
            investment_amount = stock_info['investment_amount']
            
            # 获取当前价格（这里需要实现获取实时价格的函数）
            current_price = get_latest_price(stock_code)
            
            if current_price and current_price > 0:
                # 计算买入数量（按手计算，1手=100股）
                shares_needed = int(investment_amount / current_price)
                shares_needed = (shares_needed // 100) * 100  # 调整为整手
                
                if shares_needed >= 100:  # 至少买入1手
                    buy_signal = {
                        'stock_code': stock_code,
                        'action': 'BUY',
                        'quantity': shares_needed,
                        'price': current_price,
                        'amount': shares_needed * current_price,
                        'allocation_ratio': stock_info['allocation_ratio'],
                        'stock_score': stock_info['stock_score']
                    }
                    buy_signals.append(buy_signal)
                    
                    print(f"📈 买入信号: {stock_code}, 数量: {shares_needed}, 价格: {current_price:.2f}, 金额: {buy_signal['amount']:,.2f}")
                else:
                    print(f"⚠️  {stock_code}: 计算买入数量不足1手，跳过")
            else:
                print(f"❌ {stock_code}: 无法获取当前价格")
    
    print(f"\n✅ 共生成 {len(buy_signals)} 个买入信号")
    return buy_signals

# (在 cangwei.py 文件中新增以下函数)

def get_cx_stock_buy_decision(stock_code, account):
    """
    【新增】为is_cx=1的股票专门制定买入决策。
    
    Args:
        stock_code (str): 待买入的股票代码。
        account (Account): 全局账户对象，用于获取可用资金。
        
    Returns:
        dict: 包含买入股票代码和数量的字典，如 {'stock_code': '600519', 'quantity': 100}。
              如果资金不足或不满足其他条件，则返回 None。
    """
    print(f"--- [仓位决策] 开始为CX股票 {stock_code} 制定仓位 ---")
    
    # =======================================================================
    # === 在这里定义您对CX股票的仓位管理规则 ===
    # 规则: 按总资金的10%买入，且不低于100股
    percentage_of_total = 0.10
    total_assets = account.available_cash + account.total_market_value  # 总资产 = 可用现金 + 持仓市值
    amount_to_buy = total_assets * percentage_of_total
    
    print(f"📊 总资产: {total_assets:,.2f} 元, 按10%计算买入金额: {amount_to_buy:,.2f} 元")
    # =======================================================================

    if account.available_cash < amount_to_buy:
        print(f"✗ 资金不足，无法为 {stock_code} 分配 {amount_to_buy:,.2f} 元 (可用: {account.available_cash:,.2f})")
        return None
        
    # 获取最新价格以计算股数
    latest_price = get_latest_price(stock_code)
    
    if latest_price is None or latest_price == 0:
        print(f"✗ 获取 {stock_code} 最新价格失败，无法计算股数。")
        return None
    
    # 计算股数并向下取整到100股的倍数，但确保不低于100股
    quantity = int(amount_to_buy / latest_price)
    quantity = (quantity // 100) * 100
    
    # 确保至少买入100股
    if quantity < 100:
        quantity = 100
        print(f"⚠️ 按10%资金计算股数不足100股，调整为最小买入量100股")
    
    # 检查调整后的资金需求
    required_amount = quantity * latest_price
    if account.available_cash < required_amount:
        print(f"✗ 资金不足，购买 {quantity} 股需要 {required_amount:,.2f} 元 (可用: {account.available_cash:,.2f})")
        return None
        
    print(f"✓ [仓位决策] CX股票 {stock_code} 决定买入 {quantity} 股。")
    return {
        'stock_code': stock_code,
        'quantity': quantity
    }

# 更新主函数
def main(pool_name=None, end_date=None, account=None):
    """主函数：演示完整的板块仓位管理和个股分配功能"""
    # 使用默认参数
    if pool_name is None:
        pool_name = 'houxuan_youse_xiaojinshu'  # 默认板块
    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime('%Y-%m-%d')  # 默认今天
    
    # 使用指定账户或默认账户
    if account is None:
        account = DEFAULT_ACCOUNT
    
    print("="*80)
    print("完整板块仓位管理和个股分配分析")
    print("="*80)
    print(f"分析板块: {pool_name}")
    print(f"分析日期: {end_date}")
    print(f"使用账户: {account.table_name}")
    
    # 检查数据库中是否有该板块的数据
    try:
        conn = sqlite3.connect(get_db_path())
        query = """
            SELECT COUNT(*) as count
            FROM daily_selections 
            WHERE pool_name = ? AND trade_date = ?
        """
        result = pd.read_sql_query(query, conn, params=[pool_name, end_date])
        conn.close()
        
        if result['count'].iloc[0] == 0:
            print(f"❌ 数据库中未找到板块 {pool_name} 在 {end_date} 的数据")
            return {'error': '数据库中没有该板块的数据'}
    except Exception as e:
        print(f"❌ 检查数据库时发生错误: {e}")
        return {'error': f'数据库检查失败: {e}'}
    
    # 1. 板块仓位分析
    position_result = check_sector_position_availability(pool_name, end_date, account=account)
    
    # 2. 个股资金分配分析
    allocation_result = calculate_individual_stock_allocation(pool_name, end_date, account=account)
    
    # 3. 生成买入信号
    buy_signals = get_individual_stock_buy_signals(pool_name, end_date, account=account)
    
    # 4. 最终汇总
    print(f"\n{'='*80}")
    print("最终分析结果汇总")
    print(f"{'='*80}")
    print(f"板块是否可以加仓: {'是' if position_result['can_add_position'] else '否'}")
    print(f"个股是否可以买入: {'是' if allocation_result['can_buy'] else '否'}")
    print(f"生成买入信号数量: {len(buy_signals)}")
    
    if buy_signals:
        print("\n买入信号详情:")
        for signal in buy_signals:
            print(f"  {signal['stock_code']}: {signal['quantity']}股 @ {signal['price']:.2f}元 = {signal['amount']:,.2f}元")
    
    print(f"\n账户状态:")
    print(f"  - 总权益: {account.get_total_equity():,.2f}")
    print(f"  - 可用现金: {account.available_cash:,.2f}")
    print(f"  - 持仓数量: {len(account.positions)}")
    
    return {
        'position_result': position_result,
        'allocation_result': allocation_result,
        'buy_signals': buy_signals
    }

def multi_account_analysis(pool_name=None, end_date=None):
    """多账户分析示例"""
    print("="*80)
    print("多账户板块仓位管理分析示例")
    print("="*80)
    
    # 使用默认参数
    if pool_name is None:
        pool_name = 'houxuan_youse_xiaojinshu'  # 默认板块
    if end_date is None:
        from datetime import datetime
        end_date = datetime.now().strftime('%Y-%m-%d')  # 默认今天
    
    # 创建多个账户
    accounts = {
        'account_1m': Account(starting_cash=1000000.0),  # 100万账户
        'account_500k': Account(starting_cash=500000.0),  # 50万账户
        'account_200k': Account(starting_cash=200000.0),  # 20万账户
    }
    
    # 检查数据库中是否有该板块的数据
    try:
        conn = sqlite3.connect(get_db_path())
        query = """
            SELECT COUNT(*) as count
            FROM daily_selections 
            WHERE pool_name = ? AND trade_date = ?
        """
        result = pd.read_sql_query(query, conn, params=[pool_name, end_date])
        conn.close()
        
        if result['count'].iloc[0] == 0:
            print(f"❌ 数据库中未找到板块 {pool_name} 在 {end_date} 的数据")
            return {'error': '数据库中没有该板块的数据'}
    except Exception as e:
        print(f"❌ 检查数据库时发生错误: {e}")
        return {'error': f'数据库检查失败: {e}'}
    
    results = {}
    
    # 对每个账户进行分析
    for account_name, account in accounts.items():
        print(f"\n{'='*60}")
        print(f"分析账户: {account_name}")
        print(f"分析板块: {pool_name}")
        print(f"分析日期: {end_date}")
        print(f"{'='*60}")
        
        try:
            # 1. 板块仓位分析
            position_result = check_sector_position_availability(pool_name, end_date, account=account)
            
            # 2. 个股资金分配分析
            allocation_result = calculate_individual_stock_allocation(pool_name, end_date, account=account)
            
            # 3. 生成买入信号
            buy_signals = get_individual_stock_buy_signals(pool_name, end_date, account=account)
            
            results[account_name] = {
                'position_result': position_result,
                'allocation_result': allocation_result,
                'buy_signals': buy_signals
            }
            
        except Exception as e:
            print(f"❌ 账户 {account_name} 分析失败: {e}")
            results[account_name] = None
    
    # 汇总所有账户的结果
    print(f"\n{'='*80}")
    print("多账户分析结果汇总")
    print(f"{'='*80}")
    
    for account_name, result in results.items():
        if result and 'error' not in result:
            print(f"\n📊 {account_name}:")
            print(f"  板块可加仓: {'是' if result['position_result']['can_add_position'] else '否'}")
            print(f"  个股可买入: {'是' if result['allocation_result']['can_buy'] else '否'}")
            print(f"  买入信号数: {len(result['buy_signals'])}")
            # 获取账户对象来计算权益
            account = accounts[account_name]
            total_equity = account.get_total_equity()
            print(f"  账户权益: {total_equity:,.2f}")
        else:
            print(f"\n❌ {account_name}: 分析失败")
    
    return results

def get_available_pools_and_dates(db_path=None):
    """
    查询数据库中可用的板块和日期
    
    参数:
    db_path (str): 数据库路径
    
    返回:
    dict: 包含可用板块和日期的字典
    """
    if db_path is None:
        db_path = get_db_path()
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 查询所有可用的板块
        pools_query = """
            SELECT DISTINCT pool_name, COUNT(*) as stock_count
            FROM daily_selections 
            GROUP BY pool_name
            ORDER BY pool_name
        """
        pools_df = pd.read_sql_query(pools_query, conn)
        
        # 查询所有可用的日期
        dates_query = """
            SELECT trade_date, COUNT(DISTINCT pool_name) as pool_count
            FROM daily_selections 
            GROUP BY trade_date
            ORDER BY trade_date DESC
        """
        dates_df = pd.read_sql_query(dates_query, conn)
        
        conn.close()
        
        return {
            'pools': pools_df.to_dict('records'),
            'dates': dates_df.to_dict('records')
        }
    except Exception as e:
        print(f"❌ 查询数据库信息失败: {e}")
        return {'pools': [], 'dates': []}

def show_database_info():
    """显示数据库中的板块和日期信息"""
    print("="*80)
    print("数据库板块和日期信息")
    print("="*80)
    
    info = get_available_pools_and_dates()
    
    print("\n📊 可用板块:")
    if info['pools']:
        for pool in info['pools']:
            print(f"  - {pool['pool_name']}: {pool['stock_count']} 只股票")
    else:
        print("  无可用板块数据")
    
    print("\n📅 可用日期:")
    if info['dates']:
        for date_info in info['dates'][:10]:  # 只显示最近10个日期
            print(f"  - {date_info['trade_date']}: {date_info['pool_count']} 个板块")
        if len(info['dates']) > 10:
            print(f"  ... 还有 {len(info['dates']) - 10} 个日期")
    else:
        print("  无可用日期数据")

# 如果直接运行此文件，执行主函数
if __name__ == "__main__":
    # 显示数据库信息
    show_database_info()
    
    print("\n" + "="*80)
    print("单账户分析:")
    # 单账户分析 - 使用默认参数
    main()
    
    print("\n" + "="*80)
    print("多账户分析:")
    # 多账户分析 - 使用默认参数
    multi_account_analysis()
    
