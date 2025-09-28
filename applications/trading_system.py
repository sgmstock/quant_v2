# =============================================================================
# trading_system.py
# 职责：作为系统的实时指挥中心，常驻运行。
# 1. 定时（11:00, 14:40）触发日线级别的买入决策流程。
# 2. 秒级（每10秒）监控持仓股的实时盈亏，执行止盈止损卖出策略。
# =============================================================================
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import schedule
import warnings
import traceback
import logging
import sqlite3
import redis

# --- 核心模块导入 ---
import os
import sys

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 账户与订单执行核心
from core.execution.account import Account 
# 板块信号分析
from data_management.sector_signal_analyzer import SectorSignalAnalyzer
# 仓位与资金管理
from core.execution.portfolio_manager import get_buy_stocks_from_db, get_individual_stock_buy_signals, get_cx_stock_buy_decision
# 个股技术指标分析
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer
from core.technical_analyzer.technical_analyzer import prepare_data_for_live
# 价格获取
from data_management.data_processor import get_latest_price

warnings.filterwarnings('ignore')

# --- 配置日志记录 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# ===== 给QMT发送交易信号 =====
# =============================================================================
#给QMT发送交易信号
def push_redis(action,stock,amount): 
    rs = redis.Redis(host='127.0.0.1', port=6379, db=0)
    qmt_order = {}
    qmt_order['strategy'] = 'shipan'#波段，中级，短线，新股的各类策略
    qmt_order['action'] = action # buy 或者 sell
    #qmt_order['stock'] = stock[:7] + ('SH' if stock[-1]=='G' else 'SZ')
    qmt_order['stock'] = stock + ('.SH' if stock.startswith('6') else '.SZ' if stock.startswith(('0', '3')) else '.BJ')

    qmt_order['amount'] = amount
    rs.xadd('myredis', qmt_order)
    rs.connection_pool.disconnect()
    time.sleep(2)
# # ===== 全局配置与对象初始化 =====
# REDIS_POOL = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)

# # ===== 给QMT发送交易信号 =====
# def push_redis(action, stock, amount): 
#     rs = redis.Redis(connection_pool=REDIS_POOL)
#     qmt_order = {
#         'strategy': 'shipan',
#         'action': action,
#         'stock': stock + ('.SH' if stock.startswith('6') else '.SZ' if stock.startswith(('0', '3')) else '.BJ'),
#         'amount': str(amount) # 建议将所有值转为字符串，这是Redis Stream的最佳实践
#     }
#     rs.xadd('myredis', qmt_order)
#     # 不再需要 connection_pool.disconnect()
#     time.sleep(1) # 可以适当缩短等待时间

# =============================================================================
# ===== 全局配置与对象初始化 =====
# =============================================================================
# --- 交易时间点 ---
# DAILY_ANALYSIS_TIMES = ["11:00", "14:40"]
DAILY_ANALYSIS_TIMES = ["10:00", "10:30","11:00", "11:25", "13:30", "14:00", "14:30", "14:50"]


# --- 止盈止损参数 (秒级监控使用) ---（我暂时不使用预设的止盈止损）
PROFIT_TARGET = 0.20    # 止盈目标：盈利20%
STOP_LOSS_TARGET = -0.15  # 止损目标：亏损10%

# --- 数据库路径配置 ---
def get_db_path():
    """获取v2项目的数据库路径"""
    return os.path.join(current_dir, 'databases', 'quant_system.db')

# --- 初始化全局账户对象 ---
# 整个交易脚本生命周期中，只使用这一个account实例，确保状态一致
print("正在初始化全局账户...")
ACCOUNT = Account(starting_cash=1000000.0)
print(f"账户初始化完成。初始现金: {ACCOUNT.available_cash:,.2f}, 初始持仓: {list(ACCOUNT.positions.keys())}")

# *** 核心修正 1: 创建一个全局集合，用于存储所有在daily_selections中存在的股票 ***
OPERATING_STOCKS_TODAY = set()
print(f"✓ OPERATING_STOCKS_TODAY 已初始化: {list(OPERATING_STOCKS_TODAY)} (数量: {len(OPERATING_STOCKS_TODAY)})")

# *** 添加全局停止标志 ***
SHOULD_STOP = False
print(f"✓ 全局停止标志已初始化: {SHOULD_STOP}")


# =============================================================================
# ===== 核心任务函数 =====
# =============================================================================

def update_operating_stocks_list():
    """
    【修正】更新操作股票列表 - 包含所有在daily_selections中存在的个股
    """
    global OPERATING_STOCKS_TODAY
    print("\n--- 正在更新操作股票列表 ---")
    
    OPERATING_STOCKS_TODAY.clear()
    
    try:
        conn = sqlite3.connect(get_db_path())
        # 获取所有在daily_selections表中存在的股票（不限日期）
        query = "SELECT DISTINCT stock_code FROM daily_selections"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            OPERATING_STOCKS_TODAY.update(df['stock_code'].astype(str).str.zfill(6).tolist())
        
        print(f"✓ 操作股票列表已更新，共 {len(OPERATING_STOCKS_TODAY)} 只股票。")
        if OPERATING_STOCKS_TODAY:
            print(f"  股票列表: {sorted(list(OPERATING_STOCKS_TODAY))}")
            
            # 计算持仓股与操作股票的交集
            current_positions = list(ACCOUNT.positions.keys())
            intersection = set(current_positions) & OPERATING_STOCKS_TODAY
            print(f"  当前持仓: {current_positions}")
            print(f"  交集 (需要监控的股票): {sorted(list(intersection))} (数量: {len(intersection)})")
            
            if intersection:
                print(f"  ✓ 有 {len(intersection)} 只持仓股需要秒级监控")
            else:
                print(f"  ⚠️  当前持仓与操作股票无交集，秒级监控将被跳过")
        else:
            print("  ⚠️  数据库中无股票数据，秒级监控将被跳过")
    except Exception as e:
        print(f"✗ 更新操作股票列表失败: {e}")

# =============================================================================
# ===== 核心任务函数 =====
# =============================================================================
def add_cx_holding_record(stock_code, stock_name, source_pool, buy_date, buy_price, buy_quantity):
    """
    【新增】添加CX策略持仓记录
    
    当CX策略买入股票成功后调用此函数，将持仓信息记录到cx_strategy_holdings表中
    
    Args:
        stock_code (str): 股票代码
        stock_name (str): 股票名称
        source_pool (str): 来源板块名称
        buy_date (str): 买入日期
        buy_price (float): 买入价格
        buy_quantity (int): 买入数量
    """
    conn = None
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 插入CX持仓记录（使用INSERT OR REPLACE防止重复）
        insert_query = """
            INSERT OR REPLACE INTO cx_strategy_holdings 
            (stock_code, stock_name, source_pool, buy_date, buy_price, buy_quantity, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """
        
        cursor.execute(insert_query, (stock_code, stock_name, source_pool, buy_date, buy_price, buy_quantity))
        conn.commit()
        
        logger.info(f"CX持仓记录已添加: {stock_code} ({stock_name}) 来源板块: {source_pool}")
        print(f"   -> [CX持仓] 记录已添加: {stock_code} 来源板块: {source_pool}")

    except sqlite3.Error as e:
        logger.error(f"添加CX持仓记录失败 {stock_code}: {e}")
        print(f"   -> [CX持仓] ❌ 添加记录失败: {e}")
    finally:
        if conn:
            conn.close()


def get_cx_strategy_holdings():
    """
    【新增】获取CX策略持仓股票列表
    
    从cx_strategy_holdings表中获取所有当前持有的CX长线股代码
    
    Returns:
        set: CX长线股代码集合
    """
    conn = None
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        
        # 查询所有CX持仓记录
        query = "SELECT DISTINCT stock_code FROM cx_strategy_holdings"
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 转换为集合并确保6位代码格式
        cx_holdings = {str(row[0]).zfill(6) for row in results}
        
        return cx_holdings

    except sqlite3.Error as e:
        logger.error(f"获取CX持仓记录失败: {e}")
        print(f"   -> [CX持仓] ❌ 获取记录失败: {e}")
        return set()
    finally:
        if conn:
            conn.close()


def remove_cx_holding_record(stock_code):
    """
    【新增】删除CX策略持仓记录
    
    当卖出股票时调用此函数，如果该股票在cx_strategy_holdings表中，则删除记录
    
    Args:
        stock_code (str): 股票代码
        
    Returns:
        bool: 是否删除了记录
    """
    conn = None
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 删除CX持仓记录
        delete_query = "DELETE FROM cx_strategy_holdings WHERE stock_code = ?"
        
        cursor.execute(delete_query, (stock_code,))
        rows_deleted = cursor.rowcount
        conn.commit()
        
        if rows_deleted > 0:
            logger.info(f"CX持仓记录已删除: {stock_code}")
            print(f"   -> [CX持仓] 记录已删除: {stock_code}")
            return True
        else:
            # 不是CX股票，正常情况
            return False

    except sqlite3.Error as e:
        logger.error(f"删除CX持仓记录失败 {stock_code}: {e}")
        print(f"   -> [CX持仓] ❌ 删除记录失败: {e}")
        return False
    finally:
        if conn:
            conn.close()


def sell_stock_with_cx_cleanup(account, stock_code, amount):
    """
    【新增】带CX持仓记录清理的卖出函数
    
    这是一个包装函数，在执行卖出操作后自动清理CX持仓记录
    
    Args:
        account: 账户对象
        stock_code (str): 股票代码
        amount (int): 卖出数量
        
    Returns:
        Order: 卖出订单，如果失败返回None
    """
    try:
        # 执行卖出操作
        order = account.order_sell(stock_code, amount)
        
        if order:
            # 卖出成功，检查并清理CX持仓记录
            was_cx_stock = remove_cx_holding_record(stock_code)
            
            if was_cx_stock:
                logger.info(f"卖出CX股票 {stock_code} 完成，已清理持仓记录")
                print(f"   -> [CX清理] 卖出CX股票 {stock_code} 完成")
            
            return order
        else:
            logger.warning(f"卖出 {stock_code} 失败")
            return None
            
    except Exception as e:
        logger.error(f"卖出股票 {stock_code} 时发生错误: {e}")
        print(f"   -> [卖出] ❌ 卖出 {stock_code} 失败: {e}")
        return None


def mark_cx_signal_as_used(stock_code):
    """
    【信号消费】将指定股票在 daily_selections 表中所有 is_cx=1 的记录更新为 0。
    这是为了确保一旦根据CX信号买入，该信号就不会被重复触发。

    Args:
        stock_code (str): 需要更新的股票代码。
    """
    conn = None
    try:
        db_path = get_db_path()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # SQL UPDATE 语句：只更新特定股票的is_cx=1的记录
        query = "UPDATE daily_selections SET is_cx = 0 WHERE stock_code = ? AND is_cx = 1"
        
        cursor.execute(query, (stock_code,))
        rows_updated = cursor.rowcount  # 获取受影响的行数
        conn.commit()  # 提交事务，使更改生效
        
        if rows_updated > 0:
            logger.info(f"CX信号已消费: 将股票 {stock_code} 的 {rows_updated} 条 is_cx=1 记录更新为 0。")
            print(f"   -> [数据库] CX信号已消费, {stock_code} 的 is_cx 标志已更新为 0。")
        else:
            # 这种情况很少见，但为了日志完整性加上
            logger.warning(f"尝试消费CX信号 {stock_code}，但在数据库中未找到 is_cx=1 的记录。")
            print(f"   -> [数据库] 尝试消费CX信号 {stock_code}，但未找到匹配记录。")

    except sqlite3.Error as e:
        logger.error(f"更新股票 {stock_code} 的 is_cx 标志失败: {e}")
        print(f"   -> [数据库] ❌ 更新 {stock_code} 的 is_cx 标志失败: {e}")
    finally:
        if conn:
            conn.close()


def run_cx_buy_analysis():
    """
    【新增】【逻辑修正版】专门用于处理 is_cx=1 股票的买入决策函数。
    高优先级，独立于常规买入流程。
    新逻辑：查找历史上所有被标记为is_cx=1的股票。
    """
    logger.info("="*50)
    logger.info(f"开始执行CX长线股买入分析 @ {datetime.now().strftime('%H:%M:%S')}")
    print(f"\n{'='*30} 触发CX长线股买入分析 @ {datetime.now().strftime('%H:%M:%S')} {'='*30}")

    try:
        # 1. 从数据库中找出历史上所有被标记为 is_cx=1 的股票
        conn = sqlite3.connect(get_db_path())

        ### MODIFIED QUERY ###
        # 新的查询逻辑：
        # - 从 daily_selections 表中选择所有 is_cx = 1 的记录。
        # - 使用 GROUP BY stock_code 来确保每只股票只出现一次。
        # - 使用 MAX(name) 和 MAX(pool_name) 来为每个股票代码获取一个关联的名称和池名。
        query = """
            SELECT
                stock_code,
                MAX(name) as name,
                MAX(pool_name) as pool_name
            FROM daily_selections
            WHERE is_cx = 1
            GROUP BY stock_code
        """
        cx_stocks_df = pd.read_sql_query(query, conn)
        conn.close()

        if cx_stocks_df.empty:
            logger.info("数据库中未发现历史上任何is_cx=1的买入信号。")
            print("数据库中未发现历史上任何is_cx=1的买入信号。")
            return

        logger.info(f"发现 {len(cx_stocks_df)} 个历史CX买入信号: {cx_stocks_df['stock_code'].tolist()}")
        print(f"发现 {len(cx_stocks_df)} 个历史CX买入信号，准备执行...")

        # 2. 逐一处理这些高优先级信号
        for _, row in cx_stocks_df.iterrows():
            stock_code = str(row['stock_code']).zfill(6)
            stock_name = row['name']
            
            logger.info(f"--- 处理CX信号: {stock_code} ({stock_name}) ---")
            print(f"\n--- 处理CX信号: {stock_code} ({stock_name}) ---")

            # 3. 检查条件：当前是否未持仓

            if (stock_code not in ACCOUNT.positions):
                logger.info(f"{stock_code} 当前未持仓，满足买入条件。")
                print(f"✓ {stock_code} 当前未持仓，满足买入条件。")
                
                # 4. 调用cangwei.py获取买入数量
                buy_decision = get_cx_stock_buy_decision(stock_code, ACCOUNT)
                
                if buy_decision and buy_decision.get('quantity', 0) > 0:
                    quantity = buy_decision['quantity']
                    logger.info(f"仓位决策完成，准备市价买入 {stock_code}, 数量: {quantity}")
                    print(f"  => [市价买入] {stock_code}, 数量: {quantity}")

                    try:
                        # 5. 执行市价买入
                        # push_redis('buy', stock_code, quantity)
                        ACCOUNT.order_buy(stock_code, quantity)
                        logger.info(f"CX股票 {stock_code} 买入执行成功。")
                        print("  => 买入执行完毕。")

                        # ==================== 新增逻辑 ====================
                        # 6. 买入成功后，添加CX持仓记录
                        
                        current_date = datetime.now().strftime('%Y-%m-%d')
                        current_price = get_latest_price(stock_code) or 0
                        source_pool = row['pool_name']  # 从查询结果中获取板块名称
                        
                        add_cx_holding_record(
                            stock_code=stock_code,
                            stock_name=stock_name,
                            source_pool=source_pool,
                            buy_date=current_date,
                            buy_price=current_price,
                            buy_quantity=quantity
                        )
                        
                        # 7. 消费信号，防止重复买入
                        mark_cx_signal_as_used(stock_code)
                        # ================================================

                    except Exception as e:
                        logger.error(f"CX股票 {stock_code} 买入执行失败: {e}")
                        print(f"  ❌ 买入执行失败: {e}")
                else:
                    logger.warning(f"{stock_code} 仓位决策返回无效，跳过买入。")
                    print(f"  - {stock_code} 仓位决策返回无效，跳过买入。")
            else:
                logger.info(f"{stock_code} 已有持仓，跳过本次CX买入。")
                print(f"✗ {stock_code} 已有持仓，跳过本次CX买入。")

    except Exception as e:
        logger.error(f"执行CX长线股买入分析时发生严重错误: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        print(f"❌ 执行CX长线股买入分析时发生严重错误: {e}")

def run_daily_buy_analysis():
    """
    日线级别的买入决策主函数。
    由调度器在每日的11:00和14:40触发。
    """
    try:
        logger.info("="*50)
        logger.info(f"开始执行日线买入分析 @ {datetime.now().strftime('%H:%M:%S')}")
        print(f"\n{'='*30} 触发日线买入分析 @ {datetime.now().strftime('%H:%M:%S')} {'='*30}")
        
        # 1. 确定分析日期和要操作的板块数据
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"分析日期: {end_date}")
        logger.info(f"从数据库读取板块数据")
        
        # 从数据库获取所有板块数据（去重）
        try:
            conn = sqlite3.connect(get_db_path())
            # 获取所有板块列表（去重），并获取每个板块的最新数据日期
            query = """
                SELECT pool_name, MAX(trade_date) as latest_date
                FROM daily_selections 
                GROUP BY pool_name
                ORDER BY pool_name
            """
            pool_data_df = pd.read_sql_query(query, conn)
            conn.close()
            
            if pool_data_df.empty:
                logger.warning("数据库中未找到任何板块数据，跳过本次买入分析。")
                print("数据库中未找到任何板块数据，跳过本次买入分析。")
                return
            
            # 创建板块名称到最新日期的映射
            pool_latest_dates = dict(zip(pool_data_df['pool_name'], pool_data_df['latest_date']))
            pool_names = pool_data_df['pool_name'].tolist()
            
            logger.info(f"发现 {len(pool_names)} 个板块: {pool_names}")
            logger.info(f"各板块最新数据日期: {pool_latest_dates}")
            print(f"发现 {len(pool_names)} 个板块，将使用各板块的最新数据进行分析。")
        except Exception as e:
            logger.error(f"从数据库读取板块列表失败: {e}")
            print(f"❌ 从数据库读取板块列表失败: {e}")
            return
    except Exception as e:
        logger.error(f"买入分析初始化阶段发生错误: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        print(f"❌ 买入分析初始化失败: {e}")
        return

    # 2. 对每个板块独立进行买入决策
    for pool_name in pool_names:
        # 获取该板块的最新数据日期
        pool_latest_date = pool_latest_dates[pool_name]
        
        logger.info(f"开始处理板块: {pool_name} (使用数据日期: {pool_latest_date})")
        print(f"\n{'*'*25} 正在决策板块: {pool_name.upper()} (数据日期: {pool_latest_date}) {'*'*25}")

        try:
            # --- 宏观分析：从数据库获取当前板块专属信号 ---
            logger.info(f"从数据库读取板块数据: {pool_name} (日期: {pool_latest_date})")
            conn = sqlite3.connect(get_db_path())
            query = """
                SELECT stock_code, name, is_1bzl, 总得分, 技术得分, 主力得分, 板块得分, 低BIAS得分
                FROM daily_selections 
                WHERE pool_name = ? AND trade_date = ?
            """
            df = pd.read_sql_query(query, conn, params=[pool_name, pool_latest_date])
            conn.close()
            logger.info(f"数据库读取成功，共 {len(df)} 行数据")
            
            # 安全阀 1: 检查总得分，如果全为0则跳过
            if '总得分' in df.columns and (df['总得分'].fillna(0) == 0).all():
                logger.warning(f"板块 {pool_name} 总得分全为0，未到选股日，跳过。")
                print("✗ 该板块总得分全为0，未到选股日，跳过。")
                continue
            
            # 安全阀 2: 检查1波增量股，如果没有则跳过
            try:
                sel_1bzl_stocks = df[df['is_1bzl'] == 1]['stock_code'].astype(str).str.zfill(6).tolist()
                logger.info(f"板块 {pool_name} 1波增量股数量: {len(sel_1bzl_stocks)}")
            except Exception as e:
                logger.error(f"处理1波增量股时发生错误: {e}")
                print(f"❌ 处理1波增量股时发生错误: {e}")
                continue
                
            if not sel_1bzl_stocks:
                logger.warning(f"板块 {pool_name} 无1波增量股，无法判断板块信号，跳过。")
                print("✗ 该板块无1波增量股，无法判断板块信号，跳过。")
                continue

            logger.info(f"开始板块信号分析，股票列表: {sel_1bzl_stocks}")
            sector_analyzer = SectorSignalAnalyzer(sel_1bzl_stocks, 'realtime')
            is_confirmed_bottom = sector_analyzer.get_bankuai_db()
            is_approaching_bottom = sector_analyzer.get_bankuai_jjdb()
            logger.info(f"板块信号结果 - 明确底部: {is_confirmed_bottom}, 接近底部: {is_approaching_bottom}")


            # --- 仓位与资金分析 ---
            # 从cangwei.py获取经过资金和仓位过滤后的买入建议
            logger.info("开始仓位与资金分析...")
            try:
                buy_signals_from_cangwei = get_individual_stock_buy_signals(pool_name, pool_latest_date, account=ACCOUNT)
                logger.info(f"仓位分析完成，获得 {len(buy_signals_from_cangwei)} 个买入信号")
            except Exception as e:
                logger.error(f"仓位分析失败: {e}")
                print(f"❌ 仓位分析失败: {e}")
                continue
                
            if not buy_signals_from_cangwei:
                logger.warning("仓位/资金分析后，无买入信号，跳过后续技术分析。")
                print("仓位/资金分析后，无买入信号，跳过后续技术分析。")
                continue

            #------------------------
            logger.info("开始根据板块信号，逐一进行个股技术信号过滤...")
            print("开始根据板块信号，逐一进行个股技术信号过滤...")
            
            # 记录板块信号状态到日志
            logger.info(f"=== 板块信号状态 ===")
            logger.info(f"板块名称: {pool_name}")
            logger.info(f"数据日期: {pool_latest_date}")
            logger.info(f"明确底部信号: {is_confirmed_bottom}")
            logger.info(f"接近底部信号: {is_approaching_bottom}")
            logger.info(f"买入条件类型: {'宽松条件(dazhi_buy)' if is_confirmed_bottom else '严格条件(mingque_buy)' if is_approaching_bottom else '无板块信号'}")
            logger.info(f"待分析股票数量: {len(buy_signals_from_cangwei)}")
        
            for signal in buy_signals_from_cangwei:
                stock_code = signal['stock_code']
                quantity = signal['quantity']
                price = signal.get('price', 0)
                amount = signal.get('amount', 0)
                
                logger.info(f"=== 开始分析股票: {stock_code} ===")
                logger.info(f"建议买入数量: {quantity}")
                logger.info(f"建议买入价格: {price:.2f}")
                logger.info(f"建议买入金额: {amount:,.2f}")
                print(f"分析股票: {stock_code}, 建议买入数量: {quantity}")

                try:
                    # 获取实时数据并进行最终的技术信号过滤
                    logger.info(f"为股票 {stock_code} 准备实时数据...")
                    data = prepare_data_for_live(stock_code)
                    
                    if not data or data.get('daily') is None:
                        logger.warning(f"股票 {stock_code} 数据获取失败，跳过")
                        logger.info(f"=== 股票 {stock_code} 分析结果: 数据获取失败 ===")
                        print(f"  - {stock_code}: 数据获取失败，跳过")
                        continue
                    
                    logger.info(f"股票 {stock_code} 数据获取成功，日线数据行数: {len(data['daily'])}")
                    analyzer = TechnicalAnalyzer(data)
                    individual_buy_signal = False # 初始化为False

                    # --- 应用您的分层买入逻辑 ---
                    if is_confirmed_bottom:
                        # 板块明确底部，应用"宽松"买入条件
                        logger.info(f"板块明确底部，应用宽松买入条件检查股票 {stock_code}")
                        try:
                            dazhi_buy = analyzer.dazhi_buy()
                            zjqs_ding = analyzer.zjqs_ding()
                            zjtzz = analyzer.zjtzz()
                            bdqs_ding = analyzer.bdqs_ding()
                            
                            logger.info(f"=== 股票 {stock_code} 技术指标分析 ===")
                            logger.info(f"dazhi_buy (大智买入): {dazhi_buy}")
                            logger.info(f"zjqs_ding (资金趋势顶): {zjqs_ding}")
                            logger.info(f"zjtzz (资金调整中): {zjqs_ding}")
                            logger.info(f"bdqs_ding (波段趋势顶): {bdqs_ding}")
                            
                            # 宽松买入条件：dazhi_buy 且 非全部卖出信号
                            condition_result = dazhi_buy and ((not zjqs_ding) or (not bdqs_ding))
                            logger.info(f"宽松买入条件判断: dazhi_buy({dazhi_buy}) AND 非全部卖出信号 = {condition_result}")
                            
                            if condition_result:
                                individual_buy_signal = True
                                logger.info(f"=== 股票 {stock_code} 分析结果: 通过宽松买入条件 ===")
                                print(f"✓ {stock_code}: 板块[明确底部] + 个股[dazhi_buy宽松条件]信号确认！")
                            else:
                                logger.info(f"=== 股票 {stock_code} 分析结果: 未通过宽松买入条件 ===")
                        except Exception as e:
                            logger.error(f"检查股票 {stock_code} 宽松买入条件时发生错误: {e}")
                            logger.info(f"=== 股票 {stock_code} 分析结果: 技术指标检查异常 ===")
                            print(f"  - {stock_code}: 技术指标检查失败: {e}")
                            continue
                    
                    elif is_approaching_bottom:
                        # 板块接近底部，应用"严格"买入条件
                        logger.info(f"板块接近底部，应用严格买入条件检查股票 {stock_code}")
                        try:
                            mingque_buy = analyzer.mingque_buy()
                            zjqs_ding = analyzer.zjqs_ding()
                            zjtzz = analyzer.zjtzz()
                            bdqs_ding = analyzer.bdqs_ding()
                            
                            logger.info(f"=== 股票 {stock_code} 技术指标分析 ===")
                            logger.info(f"mingque_buy (明确买入): {mingque_buy}")
                            logger.info(f"zjqs_ding (资金趋势顶): {zjqs_ding}")
                            logger.info(f"zjtzz (资金调整中): {zjtzz}")
                            logger.info(f"bdqs_ding (波段趋势顶): {bdqs_ding}")
                            
                            # 严格买入条件：mingque_buy 且 非全部卖出信号
                            condition_result = mingque_buy and ((not zjqs_ding) or (not zjtzz) or (not bdqs_ding))
                            logger.info(f"严格买入条件判断: mingque_buy({mingque_buy}) AND 非全部卖出信号 = {condition_result}")
                            
                            if condition_result:
                                individual_buy_signal = True
                                logger.info(f"=== 股票 {stock_code} 分析结果: 通过严格买入条件 ===")
                                print(f"✓ {stock_code}: 板块[接近底部] + 个股[mingque_buy严格条件]信号确认！")
                            else:
                                logger.info(f"=== 股票 {stock_code} 分析结果: 未通过严格买入条件 ===")
                        except Exception as e:
                            logger.error(f"检查股票 {stock_code} 严格买入条件时发生错误: {e}")
                            logger.info(f"=== 股票 {stock_code} 分析结果: 技术指标检查异常 ===")
                            print(f"  - {stock_code}: 技术指标检查失败: {e}")
                            continue
                    
                    # --- 交易执行 ---
                    logger.info(f"=== 股票 {stock_code} 交易执行判断 ===")
                    logger.info(f"技术信号通过: {individual_buy_signal}")
                    logger.info(f"当前持仓检查: {stock_code} {'在' if stock_code in ACCOUNT.positions else '不在'} 持仓中")
                    
                    if individual_buy_signal and stock_code not in ACCOUNT.positions:
                        logger.info(f"=== 执行买入操作 ===")
                        logger.info(f"买入股票: {stock_code}")
                        logger.info(f"买入数量: {quantity}")
                        logger.info(f"买入价格: {price:.2f}")
                        logger.info(f"买入金额: {amount:,.2f}")
                        print(f"  => 准备执行买入: {quantity} 股 {stock_code}")
                        try:
                            # push_redis('buy', stock_code, quantity)
                            ACCOUNT.order_buy(stock_code, quantity)
                            logger.info(f"=== 买入执行成功 ===")
                            logger.info(f"股票: {stock_code}, 数量: {quantity}, 价格: {price:.2f}")
                            print("  => 买入执行完毕。")
                        except Exception as e:
                            logger.error(f"=== 买入执行失败 ===")
                            logger.error(f"股票: {stock_code}, 错误: {e}")
                            print(f"  ❌ 买入执行失败: {e}")
                    elif individual_buy_signal:
                        logger.info(f"=== 股票 {stock_code} 已有持仓，跳过买入 ===")
                        print(f"  - {stock_code}: 已有持仓，本次不重复买入。")
                    else:
                        logger.info(f"=== 股票 {stock_code} 未通过技术信号过滤 ===")
                        print(f"  - {stock_code}: 未通过最终的技术信号过滤。")
                
                except Exception as e_inner:
                    logger.error(f"分析股票 {stock_code} 时发生错误: {e_inner}")
                    logger.error(f"错误详情: {traceback.format_exc()}")
                    print(f"  - 分析股票 {stock_code} 时发生错误: {e_inner}")

        except Exception as e_outer:
            logger.error(f"处理板块 {pool_name} 时发生严重错误: {e_outer}")
            logger.error(f"错误详情: {traceback.format_exc()}")
            print(f"✗ 处理板块 {pool_name} 时发生严重错误: {e_outer}")



# =============================================================================
# ===== 卖出决策核心任务函数 (最终修正版) =====
# =============================================================================

def run_daily_sell_analysis():
    """
    日线级别的卖出决策主函数。
    简化版本，直接从CSV文件获取板块信息。
    """
    try:
        logger.info("="*50)
        logger.info(f"开始执行日线卖出分析 @ {datetime.now().strftime('%H:%M:%S')}")
        print(f"\n{'='*30} 触发日线卖出分析 @ {datetime.now().strftime('%H:%M:%S')} {'='*30}")
        
        # --- 全局账户状态 ---
        # 直接使用全局唯一的ACCOUNT对象，不再重复创建
        all_held_stocks = list(ACCOUNT.positions.keys())
        if not all_held_stocks:
            logger.info("当前无任何持仓，跳过卖出分析。")
            print("当前无任何持仓，跳过卖出分析。")
            return

        logger.info(f"当前总持仓股票: {all_held_stocks}")
        print(f"当前总持仓股票: {all_held_stocks}")
    except Exception as e:
        logger.error(f"卖出分析初始化阶段发生错误: {e}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        print(f"❌ 卖出分析初始化失败: {e}")
        return

    # --- 获取所有板块数据（去重） ---
    try:
        conn = sqlite3.connect(get_db_path())
        # 获取所有板块列表（去重），并获取每个板块的最新数据日期
        query = """
            SELECT pool_name, MAX(trade_date) as latest_date
            FROM daily_selections 
            GROUP BY pool_name
            ORDER BY pool_name
        """
        pool_data_df = pd.read_sql_query(query, conn)
        conn.close()
        
        if pool_data_df.empty:
            print("数据库中未找到任何板块数据，跳过卖出分析。")
            return
        
        # 创建板块名称到最新日期的映射
        pool_latest_dates = dict(zip(pool_data_df['pool_name'], pool_data_df['latest_date']))
        pool_names = pool_data_df['pool_name'].tolist()
        
        print(f"发现 {len(pool_names)} 个板块，将使用各板块的最新数据进行卖出分析。")
    except Exception as e:
        print(f"❌ 从数据库读取板块列表失败: {e}")
        return

    # --- 对每个板块独立进行卖出决策 ---
    for pool_name in pool_names:
        # 获取该板块的最新数据日期
        pool_latest_date = pool_latest_dates[pool_name]
        
        print(f"\n{'*'*25} 正在决策板块: {pool_name.upper()} (数据日期: {pool_latest_date}) {'*'*25}")

        try:
            # --- 宏观分析：从数据库获取当前板块专属信号 ---
            conn = sqlite3.connect(get_db_path())
            query = """
                SELECT stock_code, is_1bzl
                FROM daily_selections 
                WHERE pool_name = ? AND trade_date = ?
            """
            df = pd.read_sql_query(query, conn, params=[pool_name, pool_latest_date])
            conn.close()
            
            sel_1bzl_stocks = df[df['is_1bzl'] == 1]['stock_code'].astype(str).str.zfill(6).tolist()
            if not sel_1bzl_stocks:
                logger.warning(f"板块 {pool_name} 无信号股，跳过卖出分析。")
                print(f"✗ 板块 {pool_name} 无信号股，跳过卖出分析。")
                continue
            
            # 记录卖出分析开始
            logger.info(f"=== 开始卖出分析 ===")
            logger.info(f"板块名称: {pool_name}")
            logger.info(f"数据日期: {pool_latest_date}")
            logger.info(f"1波增量股数量: {len(sel_1bzl_stocks)}")
            logger.info(f"1波增量股列表: {sel_1bzl_stocks}")

            # 获取板块顶部信号
            sector_analyzer = SectorSignalAnalyzer(sel_1bzl_stocks, 'realtime')
            is_sector_confirmed_top = sector_analyzer.get_bankuai_ding()
            is_sector_approaching_top = sector_analyzer.get_bankuai_jjding()
            
            logger.info(f"=== 板块顶部信号分析 ===")
            logger.info(f"明确顶部信号: {is_sector_confirmed_top}")
            logger.info(f"接近顶部信号: {is_sector_approaching_top}")
            logger.info(f"卖出条件类型: {'宽松条件(盈利>5%)' if is_sector_confirmed_top else '严格条件(盈利>8%)' if is_sector_approaching_top else '个股技术/盈利双重标准'}")
            print(f"板块顶部信号: 明确顶部={is_sector_confirmed_top}, 接近顶部={is_sector_approaching_top}")

            # --- 筛选持仓：只处理属于当前板块的持仓股 ---
            # 从数据库中获取该板块的所有股票代码
            all_stocks_in_sector = df['stock_code'].astype(str).str.zfill(6).tolist()
            stocks_to_check_in_this_sector = [
                stock for stock in all_held_stocks if stock in all_stocks_in_sector
            ]
            if not stocks_to_check_in_this_sector:
                print("本板块无持仓股，跳过。")
                continue
            
            print(f"检查本板块持仓: {stocks_to_check_in_this_sector}")

            # --- 交易决策：严格遵循您的原始逻辑 ---
            final_sell_list_for_sector = []
            logger.info(f"=== 开始卖出决策分析 ===")
            logger.info(f"待检查持仓股票: {stocks_to_check_in_this_sector}")
            
            # 分层决策
            if is_sector_confirmed_top:
                logger.info(f"=== 应用宽松卖出条件 (盈利 > 5%) ===")
                print("板块[明确顶部]，应用'盈利 > 5%'宽松卖出条件...")
                for stock_code in stocks_to_check_in_this_sector:
                    pnl_data = ACCOUNT.get_position_pnl(stock_code)
                    if pnl_data and pnl_data['pnl_ratio'] > 0.05:
                        final_sell_list_for_sector.append(stock_code)
                        logger.info(f"=== 股票 {stock_code} 触发宽松卖出条件 ===")
                        logger.info(f"盈利比例: {pnl_data['pnl_ratio']:.2%}")
                        print(f"  ✓ {stock_code}: 触发卖出 (盈利 {pnl_data['pnl_ratio']:.2%})")
                    else:
                        logger.info(f"股票 {stock_code} 未触发宽松卖出条件，盈利比例: {pnl_data['pnl_ratio']:.2% if pnl_data else 'N/A'}")

            elif is_sector_approaching_top:
                logger.info(f"=== 应用严格卖出条件 (盈利 > 8%) ===")
                print("板块[接近顶部]，应用'盈利 > 8%'严格卖出条件...")
                for stock_code in stocks_to_check_in_this_sector:
                    pnl_data = ACCOUNT.get_position_pnl(stock_code)
                    if pnl_data and pnl_data['pnl_ratio'] > 0.08:
                        final_sell_list_for_sector.append(stock_code)
                        logger.info(f"=== 股票 {stock_code} 触发严格卖出条件 ===")
                        logger.info(f"盈利比例: {pnl_data['pnl_ratio']:.2%}")
                        print(f"  ✓ {stock_code}: 触发卖出 (盈利 {pnl_data['pnl_ratio']:.2%})")
                    else:
                        logger.info(f"股票 {stock_code} 未触发严格卖出条件，盈利比例: {pnl_data['pnl_ratio']:.2% if pnl_data else 'N/A'}")

            else:
                logger.info(f"=== 应用个股技术/盈利双重标准 ===")
                print("板块无顶部信号，应用个股技术/盈利双重标准...")
                for stock_code in stocks_to_check_in_this_sector:
                    logger.info(f"=== 分析股票 {stock_code} 卖出条件 ===")
                    data = prepare_data_for_live(stock_code)
                    analyzer = TechnicalAnalyzer(data)
                    pnl_data = ACCOUNT.get_position_pnl(stock_code)
                    
                    if not pnl_data: 
                        logger.info(f"股票 {stock_code} 无持仓数据，跳过")
                        continue

                    # 您的复杂判断逻辑
                    tech_sell_signal = analyzer.zjqs_ding() or analyzer.zjtzz() or analyzer.bdqs_ding()
                    profit_sell_signal = pnl_data['pnl_ratio'] > 0.20 # 大幅盈利直接卖出
                    combo_sell_signal = tech_sell_signal and pnl_data['pnl_ratio'] > 0.08 # 技术信号+盈利卖出
                    
                    logger.info(f"技术卖出信号: {tech_sell_signal}")
                    logger.info(f"大幅盈利信号: {profit_sell_signal} (盈利: {pnl_data['pnl_ratio']:.2%})")
                    logger.info(f"组合卖出信号: {combo_sell_signal}")
                    
                    if profit_sell_signal or combo_sell_signal:
                        final_sell_list_for_sector.append(stock_code)
                        logger.info(f"=== 股票 {stock_code} 触发双重标准卖出条件 ===")
                        print(f"  ✓ {stock_code}: 触发卖出 (技术信号: {tech_sell_signal}, 盈利: {pnl_data['pnl_ratio']:.2%})")
                    else:
                        logger.info(f"股票 {stock_code} 未触发双重标准卖出条件")

            # --- 交易执行 ---
            logger.info(f"=== 卖出执行阶段 ===")
            logger.info(f"最终卖出清单: {final_sell_list_for_sector}")
            
            if final_sell_list_for_sector:
                print(f"\n本板块最终卖出清单: {final_sell_list_for_sector}")
                for stock_code in final_sell_list_for_sector:
                    sell_quantity = ACCOUNT.get_closeable_amount(stock_code)
                    logger.info(f"=== 准备卖出股票 {stock_code} ===")
                    logger.info(f"可卖出数量: {sell_quantity}")
                    
                    if sell_quantity > 0:
                        logger.info(f"=== 执行卖出操作 ===")
                        logger.info(f"卖出股票: {stock_code}")
                        logger.info(f"卖出数量: {sell_quantity}")
                        print(f"  => 准备执行卖出: {sell_quantity} 股 {stock_code}")
                        try:
                            # push_redis('sell', stock_code, sell_quantity)
                            # 【修正】使用带CX持仓记录清理的卖出函数
                            order = sell_stock_with_cx_cleanup(ACCOUNT, stock_code, sell_quantity)
                            if order:
                                logger.info(f"=== 卖出执行成功 ===")
                                logger.info(f"股票: {stock_code}, 数量: {sell_quantity}")
                                print("  => 卖出执行完毕。")
                            else:
                                logger.error(f"=== 卖出执行失败 ===")
                                logger.error(f"股票: {stock_code}, 卖出函数返回None")
                                print(f"  ❌ 卖出执行失败: 卖出函数返回None")
                        except Exception as e:
                            logger.error(f"=== 卖出执行失败 ===")
                            logger.error(f"股票: {stock_code}, 错误: {e}")
                            print(f"  ❌ 卖出执行失败: {e}")
                    else:
                        logger.info(f"股票 {stock_code} 无可卖出数量，跳过")
            else:
                logger.info(f"本板块无最终卖出信号")
                print("本板块无最终卖出信号。")
                
        except Exception as e:
            print(f"✗ 处理板块 {pool_name} 卖出决策时发生错误: {e}")

    print(f"\n{'='*30} 日线卖出分析完成 @ {datetime.now().strftime('%H:%M:%S')} {'='*30}")
    generate_report("每日卖出分析报告")


def monitor_positions_for_sell():
    """
    【已修正】秒级持仓监控函数。
    对所有在daily_selections中存在的持仓股进行止盈止损监控。
    对于长线持仓股(cx_strategy_holdings)，应用更高的止盈标准。
    """
    current_positions = list(ACCOUNT.positions.keys())
    if not current_positions:
        return
    
    if not OPERATING_STOCKS_TODAY:
        # 只在第一次显示警告，避免刷屏
        if not hasattr(monitor_positions_for_sell, '_warning_shown'):
            print(f"\n⚠️  [秒级监控] OPERATING_STOCKS_TODAY 为空，跳过监控")
            print(f"   当前持仓: {current_positions}")
            print(f"   今日操作股票: {list(OPERATING_STOCKS_TODAY)}")
            monitor_positions_for_sell._warning_shown = True
        return

    print(f".", end='', flush=True)

    # 获取CX长线股列表（动态获取，确保实时性）
    cx_strategy_holdings = get_cx_strategy_holdings()

    for stock_code in current_positions:
        # *** 核心修正 2: 增加安全检查 ***
        # 如果当前持仓的股票不在daily_selections中，则直接跳过
        if stock_code not in OPERATING_STOCKS_TODAY:
            continue
            
        pnl_data = ACCOUNT.get_position_pnl(stock_code)
        #print(pnl_data['stock_code'],pnl_data['pnl_ratio'])
        
        if pnl_data:
            pnl_ratio = pnl_data['pnl_ratio']
            should_sell = False
            reason = ""

            # ===================== 差异化止盈逻辑开始 =====================
            # 检查当前股票是否为长线股，并为其设定特定的止盈目标
            if stock_code in cx_strategy_holdings:
                # 如果是长线股，止盈目标在原基础上增加10个百分点
                current_profit_target = PROFIT_TARGET + 0.10  # 20% + 10% = 30%
                profit_reason_prefix = "秒级长线止盈"
            else:
                # 否则，使用默认的止盈目标
                current_profit_target = PROFIT_TARGET
                profit_reason_prefix = "秒级止盈"
            # ===================== 差异化止盈逻辑结束 =====================

            # 使用动态设定的止盈目标进行判断
            if pnl_ratio >= current_profit_target:
                should_sell = True
                reason = f"{profit_reason_prefix}(收益率 {pnl_ratio:.2%}, 目标 {current_profit_target:.2%})"
            elif pnl_ratio <= STOP_LOSS_TARGET:
                should_sell = True
                reason = f"秒级止损(收益率 {pnl_ratio:.2%})"
            
            if should_sell:
                sell_quantity = ACCOUNT.get_closeable_amount(stock_code)
                if sell_quantity > 0:
                    print(f"\n🚨 [秒级监控卖出] {stock_code}: 触发 {reason} 条件！")
                    # ... 执行卖出 ...
                    # push_redis('sell', stock_code, sell_quantity)
                    # 【修正】使用带CX持仓记录清理的卖出函数
                    order = sell_stock_with_cx_cleanup(ACCOUNT, stock_code, sell_quantity)
                    if order:
                        print(f"✅ [秒级监控] {stock_code} 卖出成功")
                    else:
                        print(f"❌ [秒级监控] {stock_code} 卖出失败")


def stop_trading_system():
    """
    15:10分自动停止交易系统的函数。
    """
    global SHOULD_STOP
    SHOULD_STOP = True
    print(f"\n{'='*50}")
    print(f"🛑 交易系统将在15:10分自动停止 @ {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*50}")
    logger.info("交易系统收到15:10分停止信号，准备停止运行")
    generate_report("15:10分自动停止报告")

def generate_report(report_title):
    """
    生成并打印交易报告。
    """
    print(f"\n{'#'*30} {report_title} @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {'#'*30}")
    ACCOUNT.display_positions_with_pnl() # 直接调用account的强大报告功能
    print(f"{'#'*80}")


# =============================================================================
# ===== 主调度器与执行入口 =====
# =============================================================================
if __name__ == "__main__":
    # 1. 启动时，先更新一次今天要操作的股票列表
    update_operating_stocks_list()

    # 2. 设置日线分析任务
    for t in DAILY_ANALYSIS_TIMES:
        # 在执行买卖分析前，先更新一下操作列表，以防盘中有变动
        schedule.every().day.at(t).do(update_operating_stocks_list) 

        # 优先执行高优先级的CX买入分析
        schedule.every().day.at(t).do(run_cx_buy_analysis)

        schedule.every().day.at(t).do(run_daily_buy_analysis)
        schedule.every().day.at(t).do(run_daily_sell_analysis)
    
    # 3. 设置15:10分自动停止任务
    schedule.every().day.at("15:10").do(stop_trading_system)
    
    # 4. 设置秒级持仓监控任务
    schedule.every(10).seconds.do(monitor_positions_for_sell)
    
    print("--- 交易调度器已启动 ---")
    print(f"日线任务执行时间: {DAILY_ANALYSIS_TIMES}")
    print(f"自动停止时间: 15:10")
    print(f"秒级监控频率: 每10秒")
    print(f"当前 OPERATING_STOCKS_TODAY: {list(OPERATING_STOCKS_TODAY)} (数量: {len(OPERATING_STOCKS_TODAY)})")
    print(f"当前持仓: {list(ACCOUNT.positions.keys())}")
    
    # 显示交集信息
    current_positions = list(ACCOUNT.positions.keys())
    intersection = set(current_positions) & OPERATING_STOCKS_TODAY
    print(f"交集 (需要监控的股票): {sorted(list(intersection))} (数量: {len(intersection)})")
    if intersection:
        print(f"✓ 有 {len(intersection)} 只持仓股需要秒级监控")
    else:
        print(f"⚠️  当前持仓与操作股票无交集，秒级监控将被跳过")
    
    print("\n--- 等待预定任务时间... 按 Ctrl+C 手动停止，或等待15:10自动停止 ---")
    
    # 5. 启动主循环
    try:
        while not SHOULD_STOP:
            schedule.run_pending()
            time.sleep(1)
        
        # 如果是因为15:10分自动停止
        if SHOULD_STOP:
            print("\n--- 交易系统已按计划在15:10分自动停止 ---")
            logger.info("交易系统已按计划在15:10分自动停止")
            
    except KeyboardInterrupt:
        print("\n--- 调度器已手动停止 ---")
        generate_report("手动停止最终账户状态报告")