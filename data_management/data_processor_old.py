"""
数据处理器

负责数据清洗、转换和标准化
基于 quant_v2/data/data_handle.py 的数据处理功能
"""
import pandas as pd
from datetime import datetime, date, time, timedelta
from sqlalchemy import create_engine, text
import adata
import akshare as ak
from .Ashare import get_price

# 导入 quant_v2 项目的数据管理模块
try:
    from .database_manager import DatabaseManager
    # 创建数据库管理器实例
    db_manager = DatabaseManager()
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from data_management.database_manager import DatabaseManager
    db_manager = DatabaseManager()


def convert_to_jq_format(stock_code):
    """
    将股票代码转换为聚宽格式
    6开头加.XSHG，其他加.XSHE
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        
    Returns:
        str: 聚宽格式股票代码，如 '000001.XSHE'
    """
    # 移除可能存在的后缀
    code = stock_code.replace('.XSHG', '').replace('.XSHE', '')
    
    # 判断是否以6开头
    if code.startswith('6'):
        return f"{code}.XSHG"
    else:
        return f"{code}.XSHE"

def get_latest_price(stock_code):
    """
    获取股票最新价格
    使用多种数据源确保获取成功
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        
    Returns:
        float: 最新价格，如果获取失败返回 None
    """
    # 方案1：使用Ashare获取实时价格
    try:
        # 转换股票代码为聚宽格式
        jq_stock_code = convert_to_jq_format(stock_code)
        daily_data = get_price(jq_stock_code, count=2, frequency='1m')
        if not daily_data.empty:
            latest_price = daily_data['close'].iloc[-1]
            return float(latest_price)
    except Exception as e:
        print(f"获取 {stock_code} 最新价时出错: {str(e)}")
    
    # 方案2：使用adata获取最近交易日收盘价
    try:
        # 使用动态日期，获取最近30天的数据
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        daily_df = adata.stock.market.get_market(stock_code, start_date=start_date, end_date=end_date, k_type=1, adjust_type=1)
        if not daily_df.empty:
            latest_close = daily_df['close'].iloc[-1]
            print(f"使用备用方案获取到 {stock_code} 最近收盘价: {latest_close}")
            return float(latest_close)
        else:
            print(f"备用方案也无法获取 {stock_code} 的价格数据")
    except Exception as e:
        print(f'备用方案获取昨日收盘价失败: {str(e)}')

    # 方案3：使用akshare获取实时价格
    try:
        stock_bid_ask_df = ak.stock_bid_ask_em(symbol=stock_code)  # 获取股票的买卖报价
        
        # 提取最新价格
        latest_price_row = stock_bid_ask_df[stock_bid_ask_df['item'] == '最新']
        if not latest_price_row.empty:
            latest_price = latest_price_row['value'].values[0]  # 获取最新价格
            # 检查是否为无效值
            if latest_price == '-' or latest_price == '' or latest_price is None:
                print(f"股票 {stock_code} 最新价格为无效值: {latest_price}，尝试备用方案")
                raise ValueError(f"无效的价格数据: {latest_price}")
            # 处理科学计数法，转为标准格式
            latest_price = float(latest_price)
            return latest_price
        else:
            print("返回的数据中没有 '最新' 行")
            raise ValueError("数据中没有最新价格信息")
    except Exception as e:
        print(f"获取 {stock_code} 最新价时出错: {str(e)}")
        # 继续尝试备用方案

    # 双重保险：返回None表示价格获取失败
    print(f"警告：无法获取股票 {stock_code} 的价格")
    return None


def get_last_trade_date_for_stock(stock_code, db_manager: DatabaseManager = None):
    """
    获取指定股票在本地数据库中的最后交易日期
    
    Args:
        stock_code (str): 股票代码
        db_manager (DatabaseManager, optional): 数据库管理器实例
        
    Returns:
        datetime.date: 最后交易日期，如果未找到则返回None
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        query = "SELECT MAX(trade_date) FROM k_daily WHERE stock_code = ?"
        result_df = db_manager.execute_query(query, (stock_code,))
        
        if not result_df.empty and result_df.iloc[0, 0] is not None:
            # 将字符串日期转换为date对象
            return datetime.strptime(result_df.iloc[0, 0], '%Y-%m-%d').date()
        else:
            return None
    except Exception as e:
        print(f"❌ 获取股票 {stock_code} 最后交易日期失败: {e}")
        return None


#计算“最近一个已结束的完整交易周”
#这是整个流程的基石。一个“完整交易周”我们通常定义为到周五结束。所以，我们需要一个函数来精确计算出相对于今天，上一个完整周的周五是哪一天。
def get_last_complete_week_friday() -> date:
    """
    计算并返回最近一个已完整结束的交易周的周五日期。
    - 周一的 weekday() 是 0, 周日是 6。周五是 4。
    """
    today = date.today()
    # 计算今天距离上个周五有多少天。
    # 如果今天是周六(5)，那上周五是1天前。
    # 如果今天是周日(6)，那上周五是2天前。
    # 如果今天是周五(4)，本周还未结束，上个完整周的周五是7天前。
    days_since_friday = (today.weekday() - 4 + 7) % 7
    if days_since_friday == 0:
        # 如果今天是周五，则上一个完整周的周五是7天前
        last_friday = today - timedelta(days=7)
    else:
        # 否则，就是本周的周五已经过去了
        last_friday = today - timedelta(days=days_since_friday)
        
    return last_friday

#
def get_last_weekly_date(stock_code, db_manager: DatabaseManager = None):
    """
    获取指定股票在本地周线数据库中的最后交易日期
    
    Args:
        stock_code (str): 股票代码
        db_manager (DatabaseManager, optional): 数据库管理器实例
        
    Returns:
        str: 最后周线交易日期，如果未找到则返回None
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        query = "SELECT MAX(trade_date) FROM k_weekly WHERE stock_code = ?"
        result_df = db_manager.execute_query(query, (stock_code,))
        
        if not result_df.empty and result_df.iloc[0, 0] is not None:
            return result_df.iloc[0, 0]
        else:
            return None
    except Exception as e:
        print(f"❌ 获取股票 {stock_code} 最后周线交易日期失败: {e}")
        return None

#获取本地历史数据k_monthly的最后交易日期
def get_last_complete_month_end() -> date:
    """
    计算并返回最近一个已完整结束的交易月的月末日期。
    如果当前月还未结束，返回上个月的月末。
    """
    today = date.today()
    
    # 获取当前月的最后一天
    if today.month == 12:
        next_month = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month = today.replace(month=today.month + 1, day=1)
    
    current_month_end = next_month - timedelta(days=1)
    
    # 如果今天是当前月的最后一天，且已收盘，则当前月已完整
    current_time = datetime.now().time()
    is_after_market_close = current_time > time(15, 0)
    
    if today == current_month_end and is_after_market_close:
        # 当前月已完整结束
        return current_month_end
    else:
        # 当前月未结束，返回上个月的月末
        if today.month == 1:
            last_month_end = today.replace(year=today.year - 1, month=12, day=31)
        else:
            last_month_end = today.replace(month=today.month - 1, day=1) - timedelta(days=1)
        return last_month_end

def get_last_monthly_date(stock_code, db_manager: DatabaseManager = None):
    """
    获取指定股票在本地月线数据库中的最后交易日期
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        db_manager (DatabaseManager, optional): 数据库管理器实例
        
    Returns:
        date: 最后月线交易日期，如果未找到则返回None
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        query = "SELECT MAX(trade_date) FROM k_monthly WHERE stock_code = ?"
        result_df = db_manager.execute_query(query, (stock_code,))
        
        if not result_df.empty and result_df.iloc[0, 0] is not None:
            return datetime.strptime(result_df.iloc[0, 0], '%Y-%m-%d').date()
        else:
            return None
    except Exception as e:
        print(f"❌ 获取股票 {stock_code} 月线最后日期失败: {e}")
        return None


def clean_market_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    对行情数据进行标准化的清洗流程。

    Args:
        df (pd.DataFrame): 原始行情数据，需要包含 'open', 'high', 'low', 'close', 'volume' 列。

    Returns:
        pd.DataFrame: 清洗后的高质量行情数据。
    """
    print("--- 开始数据清洗 ---")
    initial_rows = len(df)
    print(f"原始数据行数: {initial_rows}")

    # 如果数据为空，直接返回
    if df.empty:
        print("数据为空，无需清洗")
        print("--- 数据清洗完成 ---\n")
        return df.copy()

    # 检查必要的列是否存在
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"警告：缺少必要的列 {missing_columns}，跳过数据清洗")
        print("--- 数据清洗完成 ---\n")
        return df.copy()

    # 步骤1: 检查并处理缺失值 (NaN)
    # 策略：如果一行中的关键价格数据有一个是NaN，就删除该行
    nan_rows = df[required_columns].isnull().any(axis=1).sum()
    if nan_rows > 0:
        print(f"发现 {nan_rows} 行存在缺失值 (NaN)，将予以删除。")
        df = df.dropna(subset=required_columns)
    
    # 步骤2: 删除成交量为0的无效交易日 (处理您遇到的问题)
    zero_volume_rows = len(df[df['volume'] <= 0])
    if zero_volume_rows > 0:
        print(f"发现 {zero_volume_rows} 行成交量为0或负数，将予以删除。")
        df = df[df['volume'] > 0]
        
    # 步骤3: 检查并处理价格逻辑错误 (例如 low > high)
    # 策略：删除不符合逻辑的数据
    logical_errors = df[(df['low'] > df['high']) | (df['open'] > df['high']) | (df['close'] > df['high']) | \
                        (df['low'] > df['open']) | (df['low'] > df['close'])]
    if not logical_errors.empty:
        print(f"发现 {len(logical_errors)} 行存在价格逻辑错误 (如 low > high)，将予以删除。")
        df = df.drop(logical_errors.index)

    # 清洗总结
    final_rows = len(df)
    print(f"清洗后数据行数: {final_rows}")
    print(f"总计删除 {initial_rows - final_rows} 条问题数据行。")
    print("--- 数据清洗完成 ---\n")
    
    # 返回一个干净的副本
    return df.copy()


#将日线数据转换为周线数据
def convert_daily_to_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    将日线行情数据转换为周线行情数据。
    """
    if daily_df.empty:
        return pd.DataFrame()
        
    # 确保 trade_date 是 datetime 类型并设为索引
    df = daily_df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    
    # 定义聚合规则
    aggregation_rules = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    # 如果有 amount 列，添加到聚合规则中
    if 'amount' in df.columns:
        aggregation_rules['amount'] = 'sum'
    
    # 如果有 stock_code 列，添加到聚合规则中
    if 'stock_code' in df.columns:
        aggregation_rules['stock_code'] = 'first'
    
    # 使用 resample 方法，'W-FRI' 表示按周重采样，且每周的结束点是周五
    weekly_df = df.resample('W-FRI').agg(aggregation_rules)
    
    # 清理数据：删除重采样后产生的全为空值的行
    weekly_df.dropna(how='all', inplace=True)
    weekly_df.reset_index(inplace=True)

    return weekly_df

def convert_daily_to_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    将日线行情数据转换为月线行情数据。
    """
    if daily_df.empty:
        return pd.DataFrame()
        
    # 确保 trade_date 是 datetime 类型并设为索引
    df = daily_df.copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df.set_index('trade_date', inplace=True)
    
    # 定义聚合规则
    aggregation_rules = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    
    # 如果有 amount 列，添加到聚合规则中
    if 'amount' in df.columns:
        aggregation_rules['amount'] = 'sum'
    
    # 如果有 stock_code 列，添加到聚合规则中
    if 'stock_code' in df.columns:
        aggregation_rules['stock_code'] = 'first'
    
    # 使用 resample 方法，'M' 表示按月重采样，且每月的结束点是月末
    monthly_df = df.resample('M').agg(aggregation_rules)
    
    # 清理数据：删除重采样后产生的全为空值的行
    monthly_df.dropna(how='all', inplace=True)
    monthly_df.reset_index(inplace=True)
    
    return monthly_df




def fetch_data_from_api(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    【职责一：只从API获取数据】
    这是一个纯粹的获取函数，不与数据库交互。
    支持多个数据源：adata、Ashare 和 akshare
    按优先级顺序尝试：adata -> Ashare -> akshare
    """
    print(f"正在从API获取 {stock_code} 从 {start_date} 到 {end_date} 的数据...")
    
    # 方法1：尝试使用 adata 获取数据
    try:
        print("尝试使用 adata 获取数据...")
        data = adata.stock.market.get_market(stock_code, k_type=1, start_date=start_date, end_date=end_date)
        if data is not None and len(data) > 0:
            df = pd.DataFrame(data, columns=['trade_date', 'open', 'high', 'low', 'close', 'volume'])
            df['stock_code'] = stock_code
            # 规范化日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']]
            print(f"✅ adata 成功获取 {len(df)} 条数据")
            return df
        else:
            print("adata 未返回数据，尝试 akshare...")
    except Exception as e:
        print(f"adata 获取失败: {e}，尝试 akshare...")


    # 方法2：使用 akshare 获取数据
    try:
        print("尝试使用 akshare 获取数据...")
        # akshare 需要完整的股票代码格式
        if stock_code.startswith('6'):
            ak_code = f"{stock_code}.SH"
        else:
            ak_code = f"{stock_code}.SZ"
        
        # 使用 akshare 获取数据
        data = ak.stock_zh_a_hist(symbol=ak_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        
        if data is not None and len(data) > 0:
            # akshare 返回的列名需要映射
            df = data.copy()
            # 重命名列以匹配标准格式
            df = df.rename(columns={
                '日期': 'trade_date',
                '开盘': 'open', 
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume'
            })
            
            # 添加股票代码列
            df['stock_code'] = stock_code
            
            # 确保日期格式正确
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 选择需要的列
            df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']]
            
            print(f"✅ akshare 成功获取 {len(df)} 条数据")
            return df
        else:
            print("akshare 未返回数据")
    except Exception as e:
        print(f"akshare 获取失败: {e}")

    # 方法3：使用 Ashare 获取数据
    try:
        print("尝试使用 Ashare 获取数据...")
        # 转换股票代码格式为 Ashare 需要的格式
        # Ashare 需要的是带市场前缀的格式，如 sz000001 或 sh600000
        if stock_code.startswith('6'):
            ashare_code = f"sh{stock_code}"
        else:
            ashare_code = f"sz{stock_code}"
        
        # 计算需要获取的数据条数
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        days_diff = (end_dt - start_dt).days
        count = max(days_diff + 10, 100)  # 多获取一些数据确保覆盖
        
        print(f"使用 Ashare 代码: {ashare_code}, 获取 {count} 条数据")
        
        # 使用 Ashare 获取数据
        ashare_df = get_price(ashare_code, end_date=end_date, count=count, frequency='1d')
        
        if not ashare_df.empty:
            # 转换 Ashare 数据格式为标准格式
            df = ashare_df.reset_index()
            df.columns = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
            df['stock_code'] = stock_code
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 过滤日期范围
            df = df[(df['trade_date'] >= pd.to_datetime(start_date).date()) & 
                   (df['trade_date'] <= pd.to_datetime(end_date).date())]
            
            df = df[['stock_code', 'trade_date', 'open', 'high', 'low', 'close', 'volume']]
            print(f"✅ Ashare 成功获取 {len(df)} 条数据")
            return df
        else:
            print("Ashare 未返回数据")
    except Exception as e:
        print(f"Ashare 获取失败: {e}，尝试 akshare...")
    

    
    # 如果三个数据源都失败，返回空DataFrame
    print("❌ 所有数据源都获取失败")
    return pd.DataFrame()


def load_daily_data_from_db(stock_code: str, start_date: str = None, end_date: str = None, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    从本地数据库加载指定股票在指定日期范围内的日线数据
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
        end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
        
    Returns:
        pd.DataFrame: 包含日线数据的DataFrame，列包括 stock_code, trade_date, open, high, low, close, volume
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        # 构建SQL查询
        if start_date and end_date:
            query = """
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_daily 
                WHERE stock_code = :stock_code 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date
            """
            params = {
                'stock_code': stock_code,
                'start_date': start_date,
                'end_date': end_date
            }
        else:
            query = """
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_daily 
                WHERE stock_code = :stock_code
                ORDER BY trade_date
            """
            params = {'stock_code': stock_code}
        
        # 执行查询
        df = db_manager.execute_query(query, params)
        
        # 确保日期格式正确
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        print(f"从数据库加载 {stock_code} 日线数据: {len(df)} 条记录")
        return df
        
    except Exception as e:
        print(f"❌ 从数据库加载 {stock_code} 日线数据失败: {e}")
        return pd.DataFrame()


def load_weekly_data_from_db(stock_code: str, start_date: str = None, end_date: str = None, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    从本地数据库加载指定股票在指定日期范围内的周线数据
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
        end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
        
    Returns:
        pd.DataFrame: 包含周线数据的DataFrame
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        # 构建SQL查询
        if start_date and end_date:
            query = text("""
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_weekly 
                WHERE stock_code = :stock_code 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date
            """)
            params = {
                'stock_code': stock_code,
                'start_date': start_date,
                'end_date': end_date
            }
        else:
            query = text("""
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_weekly 
                WHERE stock_code = :stock_code
                ORDER BY trade_date
            """)
            params = {'stock_code': stock_code}
        
        # 执行查询 - 将text对象转换为字符串
        df = db_manager.execute_query(str(query), params)
        
        # 确保日期格式正确
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        print(f"从数据库加载 {stock_code} 周线数据: {len(df)} 条记录")
        return df
        
    except Exception as e:
        print(f"❌ 从数据库加载 {stock_code} 周线数据失败: {e}")
        return pd.DataFrame()


def load_monthly_data_from_db(stock_code: str, start_date: str = None, end_date: str = None, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    从本地数据库加载指定股票在指定日期范围内的月线数据
    
    Args:
        stock_code (str): 股票代码，如 '000001'
        start_date (str, optional): 开始日期，格式为'YYYY-MM-DD'
        end_date (str, optional): 结束日期，格式为'YYYY-MM-DD'
        
    Returns:
        pd.DataFrame: 包含月线数据的DataFrame
    """
    try:
        # 使用传入的数据库管理器或创建新的
        if db_manager is None:
            db_manager = DatabaseManager()
        
        # 构建SQL查询
        if start_date and end_date:
            query = text("""
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_monthly 
                WHERE stock_code = :stock_code 
                AND trade_date >= :start_date 
                AND trade_date <= :end_date
                ORDER BY trade_date
            """)
            params = {
                'stock_code': stock_code,
                'start_date': start_date,
                'end_date': end_date
            }
        else:
            query = text("""
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_monthly 
                WHERE stock_code = :stock_code
                ORDER BY trade_date
            """)
            params = {'stock_code': stock_code}
        
        # 执行查询 - 将text对象转换为字符串
        df = db_manager.execute_query(str(query), params)
        
        # 确保日期格式正确
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        print(f"从数据库加载 {stock_code} 月线数据: {len(df)} 条记录")
        return df
        
    except Exception as e:
        print(f"❌ 从数据库加载 {stock_code} 月线数据失败: {e}")
        return pd.DataFrame()


def update_and_load_data_daily(symbol: str) -> pd.DataFrame:
    """
    获取完整的最新行情数据，包括实时且未结束的行情数据
    
    主要功能：
    a. 判断最后1个交易日是那天
    b. 从本地的历史行情数据表中获取的最后1天的trade_date
    c. 对比a项和b项，如果本地历史行情数据表有缺失，就通过API接口来获取缺失数据
    d. 拼接本地历史数据+API获取的数据，形成full_df
    
    Args:
        symbol (str): 股票代码，如 '000001'
        
    Returns:
        pd.DataFrame: 完整的行情数据，包括本地历史数据和实时数据
    """
    print(f"\n--- 开始为 {symbol} 获取完整行情数据 ---")
    
    # a. 判断最后1个交易日是否那天
    last_trade_date = db_manager.get_last_trade_date()  # 获取最新交易日
    today = datetime.now().date()
    
    print(f"最新交易日: {last_trade_date}")
    print(f"今天日期: {today}")
    
    # b. 从本地的历史行情数据表中获取的最后1天的trade_date
    last_local_date = get_last_trade_date_for_stock(symbol)
    
    if last_local_date:
        print(f"本地最后交易日: {last_local_date}")
    else:
        print("本地无历史数据")
        last_local_date = None
    
    # c. 对比a项和b项，检查是否有缺失
    missing_data_df = pd.DataFrame()
    
    if last_local_date is None:
        # 本地无数据，需要获取所有数据
        print("本地无数据，需要获取所有历史数据...")
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')  # 获取一年数据
        end_date = today.strftime('%Y-%m-%d')
        missing_data_df = fetch_data_from_api(symbol, start_date, end_date)
        
    elif last_local_date < pd.to_datetime(last_trade_date).date():
        # 本地数据有缺失，需要补充
        print(f"本地数据缺失，从 {last_local_date} 到 {last_trade_date}")
        start_date = (last_local_date + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
        missing_data_df = fetch_data_from_api(symbol, start_date, end_date)
    
    # d. 拼接本地历史数据+API获取的数据，形成full_df
    print("正在拼接完整数据...")
    
    # 从数据库加载所有本地数据
    local_df = load_daily_data_from_db(symbol, '2024-01-01', today.strftime('%Y-%m-%d'))
    
    # 如果有实时数据，进行拼接
    if not missing_data_df.empty:
        print(f"拼接 {len(missing_data_df)} 条实时数据")
        # 确保列顺序一致
        missing_data_df = missing_data_df[local_df.columns]
        full_df = pd.concat([local_df, missing_data_df], ignore_index=True)
        # 确保 trade_date 列数据类型一致
        full_df['trade_date'] = pd.to_datetime(full_df['trade_date'])
        # 去重（按日期）
        full_df = full_df.drop_duplicates(subset=['trade_date'], keep='last')
        full_df = full_df.sort_values('trade_date').reset_index(drop=True)
    else:
        full_df = local_df
    
    print(f"最终获取到 {len(full_df)} 条完整数据")
    print(f"数据日期范围: {full_df['trade_date'].min()} 到 {full_df['trade_date'].max()}")
    print(f"--- {symbol} 完整行情数据获取完成 ---\n")
    
    return full_df

def update_and_load_data_weekly(symbol: str) -> pd.DataFrame:
    """
    获取完整的最新周线行情数据，包括实时且未结束的周线行情数据。
    从最新的日线数据动态生成周线数据。
    """
    print(f"\n--- 开始为 {symbol} 获取完整周线行情数据 ---")
    
    # 1. 调用日线函数，获取包括实时日线在内的所有数据
    full_daily_df = update_and_load_data_daily(symbol)
    
    # 2. 调用转换函数，将日线数据转换为周线数据
    full_weekly_df = convert_daily_to_weekly(full_daily_df)
    
    print(f"最终获取到 {len(full_weekly_df)} 条完整周线数据")
    if not full_weekly_df.empty:
        print(f"数据日期范围: {full_weekly_df['trade_date'].min()} 到 {full_weekly_df['trade_date'].max()}")
    print(f"--- {symbol} 完整周线行情数据获取完成 ---\n")
    
    return full_weekly_df

def update_and_load_data_monthly(symbol: str) -> pd.DataFrame:
    """
    获取完整的最新月线行情数据，包括实时且未结束的月线行情数据。
    从最新的日线数据动态生成月线数据。
    """
    print(f"\n--- 开始为 {symbol} 获取完整月线行情数据 ---")
    
    # 1. 调用日线函数，获取包括实时日线在内的所有数据
    full_daily_df = update_and_load_data_daily(symbol)
    
    # 2. 调用转换函数，将日线数据转换为月线数据
    full_monthly_df = convert_daily_to_monthly(full_daily_df)
    
    print(f"最终获取到 {len(full_monthly_df)} 条完整月线数据")
    if not full_monthly_df.empty:
        print(f"数据日期范围: {full_monthly_df['trade_date'].min()} 到 {full_monthly_df['trade_date'].max()}")
    print(f"--- {symbol} 完整月线行情数据获取完成 ---\n")
    
    return full_monthly_df

#设计专门用于回测用的获取日线行情数据：
def get_daily_data_for_backtest(stock_code: str, current_date: str, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    # 1. 从本地加载该股票的【全部】历史数据
    #    (优化：可以在回测开始时一次性加载所有股票到内存)
    full_local_df = load_daily_data_from_db(stock_code, db_manager=db_manager) 

    # 2. 严格截取截至 current_date 的数据，防止未来函数
    # 确保 trade_date 列是 datetime 类型
    if not full_local_df.empty:
        full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'])
        df_snapshot = full_local_df[full_local_df['trade_date'] <= pd.to_datetime(current_date)].copy()
    else:
        df_snapshot = pd.DataFrame()
    
    return df_snapshot

#设计专门用于回测用的获取周线行情数据：
def get_weekly_data_for_backtest(stock_code: str, current_date: str, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    # 获取所有周线数据，然后按日期过滤
    full_local_df = load_weekly_data_from_db(stock_code, db_manager=db_manager)
    # 确保 trade_date 列是 datetime 类型
    if not full_local_df.empty:
        full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'])
        df_snapshot = full_local_df[full_local_df['trade_date'] <= pd.to_datetime(current_date)].copy()
    else:
        df_snapshot = pd.DataFrame()
    return df_snapshot

#设计专门用于回测用的获取月线行情数据：
def get_monthly_data_for_backtest(stock_code: str, current_date: str, db_manager: DatabaseManager = None) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    full_local_df = load_monthly_data_from_db(stock_code, db_manager=db_manager)
    # 确保 trade_date 列是 datetime 类型
    if not full_local_df.empty:
        full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'])
        df_snapshot = full_local_df[full_local_df['trade_date'] <= pd.to_datetime(current_date)].copy()
    else:
        df_snapshot = pd.DataFrame()
    return df_snapshot

#设计专门用于回测用的批量获取多只股票日线行情数据：
def get_multiple_stocks_daily_data_for_backtest(stock_codes: list, current_date: str) -> dict:
    """
    为策略提供在特定日期所需的多只股票数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        current_date (str): 当前日期，格式为 'YYYY-MM-DD'
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
              例如: {'000001': DataFrame, '000002': DataFrame, ...}
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票在 {current_date} 的日线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的数据获取函数
            stock_data = get_daily_data_for_backtest(stock_code, current_date)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 数据失败: {str(e)}")
    
    print(f"\n批量获取完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#设计专门用于回测用的批量获取多只股票周线行情数据：
def get_multiple_stocks_weekly_data_for_backtest(stock_codes: list, current_date: str) -> dict:
    """
    为策略提供在特定日期所需的多只股票周线数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        current_date (str): 当前日期，格式为 'YYYY-MM-DD'
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票在 {current_date} 的周线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的周线数据获取函数
            stock_data = get_weekly_data_for_backtest(stock_code, current_date)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 周线数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 周线数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 周线数据失败: {str(e)}")
    
    print(f"\n批量获取完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#设计专门用于回测用的批量获取多只股票月线行情数据：
def get_multiple_stocks_monthly_data_for_backtest(stock_codes: list, current_date: str) -> dict:
    """
    为策略提供在特定日期所需的多只股票月线数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        current_date (str): 当前日期，格式为 'YYYY-MM-DD'
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票在 {current_date} 的月线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的月线数据获取函数
            stock_data = get_monthly_data_for_backtest(stock_code, current_date)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 月线数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 月线数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 月线数据失败: {str(e)}")
    
    print(f"\n批量获取完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#设计专门用于实盘交易的批量获取多只股票日线行情数据：
def update_and_load_multiple_stocks_daily_data(stock_codes: list) -> dict:
    """
    为实盘交易批量获取多只股票的完整最新行情数据，包括实时且未结束的行情数据。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
              例如: {'000001': DataFrame, '000002': DataFrame, ...}
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票的实时日线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的实时数据获取函数
            stock_data = update_and_load_data_daily(stock_code)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 实时数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 实时数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 实时数据失败: {str(e)}")
    
    print(f"\n批量获取实时数据完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#设计专门用于实盘交易的批量获取多只股票周线行情数据：
def update_and_load_multiple_stocks_weekly_data(stock_codes: list) -> dict:
    """
    为实盘交易批量获取多只股票的完整最新周线行情数据，包括实时且未结束的周线行情数据。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票的实时周线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的周线数据获取函数
            stock_data = update_and_load_data_weekly(stock_code)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 实时周线数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 实时周线数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 实时周线数据失败: {str(e)}")
    
    print(f"\n批量获取实时周线数据完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#设计专门用于实盘交易的批量获取多只股票月线行情数据：
def update_and_load_multiple_stocks_monthly_data(stock_codes: list) -> dict:
    """
    为实盘交易批量获取多只股票的完整最新月线行情数据，包括实时且未结束的月线行情数据。
    
    Args:
        stock_codes (list): 股票代码列表，如 ['000001', '000002', '000858']
        
    Returns:
        dict: 以股票代码为键，DataFrame为值的字典
    """
    print(f"开始批量获取 {len(stock_codes)} 只股票的实时月线数据...")
    
    result_dict = {}
    failed_stocks = []
    
    for i, stock_code in enumerate(stock_codes, 1):
        try:
            print(f"正在处理第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 调用单只股票的月线数据获取函数
            stock_data = update_and_load_data_monthly(stock_code)
            
            if not stock_data.empty:
                result_dict[stock_code] = stock_data
                print(f"  ✓ 成功获取 {stock_code} 实时月线数据，共 {len(stock_data)} 条记录")
            else:
                failed_stocks.append(stock_code)
                print(f"  ✗ {stock_code} 实时月线数据为空")
                
        except Exception as e:
            failed_stocks.append(stock_code)
            print(f"  ✗ 获取 {stock_code} 实时月线数据失败: {str(e)}")
    
    print(f"\n批量获取实时月线数据完成:")
    print(f"  成功: {len(result_dict)} 只股票")
    print(f"  失败: {len(failed_stocks)} 只股票")
    if failed_stocks:
        print(f"  失败的股票代码: {failed_stocks}")
    
    return result_dict

#实盘交易辅助函数：数据验证和监控
def validate_realtime_data(stock_data_dict: dict) -> dict:
    """
    验证实盘交易数据的完整性和质量
    
    Args:
        stock_data_dict (dict): 股票数据字典
        
    Returns:
        dict: 验证结果，包含每个股票的验证状态
    """
    print("开始验证实盘交易数据...")
    
    validation_results = {}
    
    for stock_code, df in stock_data_dict.items():
        result = {
            'stock_code': stock_code,
            'is_valid': True,
            'issues': [],
            'data_count': len(df),
            'latest_date': None,
            'latest_price': None
        }
        
        if df.empty:
            result['is_valid'] = False
            result['issues'].append('数据为空')
        else:
            # 检查必要列是否存在
            required_columns = ['open', 'high', 'low', 'close', 'volume', 'trade_date']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                result['is_valid'] = False
                result['issues'].append(f'缺少列: {missing_columns}')
            
            # 检查数据完整性
            if not df.empty:
                # 检查缺失值
                required_price_columns = ['open', 'high', 'low', 'close', 'volume']
                available_columns = [col for col in required_price_columns if col in df.columns]
                
                if available_columns:
                    null_counts = df[available_columns].isnull().sum()
                    if null_counts.sum() > 0:
                        result['issues'].append(f'存在缺失值: {null_counts.to_dict()}')
                        result['is_valid'] = False
                
                # 检查价格逻辑（只有在所有价格列都存在时）
                if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
                    price_errors = df[(df['low'] > df['high']) | 
                                     (df['open'] > df['high']) | 
                                     (df['close'] > df['high']) |
                                     (df['low'] > df['open']) | 
                                     (df['low'] > df['close'])]
                    if not price_errors.empty:
                        result['issues'].append(f'价格逻辑错误: {len(price_errors)} 条记录')
                        result['is_valid'] = False
                
                # 检查成交量
                if 'volume' in df.columns:
                    zero_volume = len(df[df['volume'] <= 0])
                    if zero_volume > 0:
                        result['issues'].append(f'零成交量记录: {zero_volume} 条')
                        result['is_valid'] = False
                
                # 获取最新数据信息
                if 'trade_date' in df.columns:
                    result['latest_date'] = df['trade_date'].max()
                if 'close' in df.columns:
                    result['latest_price'] = df['close'].iloc[-1]
        
        validation_results[stock_code] = result
        
        # 打印验证结果
        if result['is_valid']:
            print(f"  ✓ {stock_code}: 数据验证通过 ({result['data_count']} 条记录)")
        else:
            print(f"  ✗ {stock_code}: 数据验证失败 - {', '.join(result['issues'])}")
    
    print("数据验证完成")
    return validation_results


def get_last_trade_date(today_date=None):
    """
    获取最后一个交易日
    
    Args:
        today_date: 指定日期，如果为None则使用当前日期
        
    Returns:
        str: 最后一个交易日，格式为 'YYYY-MM-DD'
    """
    try:
        if today_date is None:
            today_date = datetime.now().strftime('%Y-%m-%d')
        
        # 从数据库获取最后一个交易日
        query = """
        SELECT MAX(trade_date) as last_trade_date
        FROM k_daily
        WHERE trade_date <= :today_date
        """
        
        result = db_manager.execute_query(query, {"today_date": today_date})
        
        if not result.empty and result.iloc[0]['last_trade_date'] is not None:
            return result.iloc[0]['last_trade_date']
        else:
            # 如果数据库中没有数据，返回当前日期
            return today_date
            
    except Exception as e:
        print(f"获取最后交易日失败: {e}")
        # 如果出错，返回当前日期
        return datetime.now().strftime('%Y-%m-%d')


# if __name__ == "__main__":
#     ashare_df = get_price("600048.XSHG", end_date="20250929", count=2, frequency='1d')
#     print(ashare_df)
#     # 测试单只股票数据获取
#     dd = db_manager.get_last_trade_date(today_date=None)

#     print(dd)
    
    # 测试单只股票回测数据获取
    # aa = get_daily_data_for_backtest('000029', '2025-09-04')
    # print(aa)
    
    # # 测试批量获取多只股票数据（回测用）
    # stock_list = ['000001', '000002', '000858', '000029']
    # current_date = '2025-09-04'
    
    # # # 测试批量获取日线数据（回测用）
    # print("=== 测试批量获取日线数据（回测用）===")
    # daily_data_dict = get_multiple_stocks_daily_data_for_backtest(stock_list, current_date)
    # print(f"获取到 {len(daily_data_dict)} 只股票的日线数据")
    # print(daily_data_dict)
    
    # # 测试批量获取实时数据（实盘用）
    # print("\n=== 测试批量获取实时数据（实盘用）===")
    # realtime_daily_data = update_and_load_multiple_stocks_daily_data(stock_list)
    # print(f"获取到 {len(realtime_daily_data)} 只股票的实时日线数据")
    
    # # 测试批量获取周线数据
    # print("\n=== 测试批量获取周线数据 ===")
    # weekly_data_dict = get_multiple_stocks_weekly_data_for_backtest(stock_list, current_date)
    # print(f"获取到 {len(weekly_data_dict)} 只股票的周线数据")
    
    # # 测试批量获取月线数据
    # print("\n=== 测试批量获取月线数据 ===")
    # monthly_data_dict = get_multiple_stocks_monthly_data_for_backtest(stock_list, current_date)
    # print(f"获取到 {len(monthly_data_dict)} 只股票的月线数据")

