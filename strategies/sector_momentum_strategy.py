import pandas as pd
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core.utils.indicators import zhibiao
from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest

def get_sector_stocks(sector_code: str = None, sector_name: str = None) -> list:
    """
    获取板块成分股
    
    Args:
        sector_code (str, optional): 板块代码 (index_code)
        sector_name (str, optional): 板块名称 (industry_name)
    
    Returns:
        list: 板块成分股代码列表
    
    Note:
        sector_code 和 sector_name 至少需要提供一个
    """
    import sqlite3
    
    if not sector_code and not sector_name:
        raise ValueError("必须提供 sector_code 或 sector_name 参数")
    
    try:
        # 连接数据库
        db_path = 'databases/quant_system.db'
        conn = sqlite3.connect(db_path)
        
        # 首先尝试从申万配置表获取
        try:
            # 根据提供的参数构建查询条件
            if sector_code:
                # 通过板块代码查找 (申万表中有 index_code 列)
                query = "SELECT DISTINCT stock_code FROM sw_cfg WHERE index_code = ?"
                params = (sector_code,)
            else:
                # 通过板块名称查找 (申万表中有 industry_name 列)
                query = "SELECT DISTINCT stock_code FROM sw_cfg WHERE industry_name = ?"
                params = (sector_name,)
            
            result_df = pd.read_sql_query(query, conn, params=params)
            
            if not result_df.empty:
                stock_list = result_df['stock_code'].tolist()
                print(f"在申万配置表中找到 {len(stock_list)} 只成分股")
                conn.close()
                return stock_list
                        
        except Exception as e:
            print(f"读取申万配置表时出错: {e}")
        
        # 如果申万配置表没有找到，尝试通达信配置表
        try:
            # 根据提供的参数构建查询条件
            if sector_code:
                # 通过板块代码查找
                query = "SELECT DISTINCT stock_code FROM tdx_cfg WHERE index_code = ?"
                params = (sector_code,)
            else:
                # 通过板块名称查找
                query = "SELECT DISTINCT stock_code FROM tdx_cfg WHERE industry_name = ?"
                params = (sector_name,)
            
            result_df = pd.read_sql_query(query, conn, params=params)
            
            if not result_df.empty:
                stock_list = result_df['stock_code'].tolist()
                print(f"在通达信配置表中找到 {len(stock_list)} 只成分股")
                conn.close()
                return stock_list
                        
        except Exception as e:
            print(f"读取通达信配置表时出错: {e}")
        
        conn.close()
        print(f"未找到匹配的板块: {sector_code or sector_name}")
        return []
        
    except Exception as e:
        print(f"获取板块成分股时出错: {e}")
        return []


def list_available_sectors() -> dict:
    """
    列出所有可用的板块信息
    
    Returns:
        dict: 包含申万和通达信板块信息的字典
    """
    import sqlite3
    
    try:
        db_path = 'databases/quant_system.db'
        conn = sqlite3.connect(db_path)
        
        result = {}
        
        # 获取申万板块信息
        try:
            sw_df = pd.read_sql_query("SELECT * FROM sw_cfg", conn)
            if not sw_df.empty:
                result['申万板块'] = sw_df
                print(f"申万板块数量: {len(sw_df)}")
        except Exception as e:
            print(f"读取申万配置表时出错: {e}")
            result['申万板块'] = pd.DataFrame()
        
        # 获取通达信板块信息
        try:
            tdx_df = pd.read_sql_query("SELECT * FROM tdx_cfg", conn)
            if not tdx_df.empty:
                result['通达信板块'] = tdx_df
                print(f"通达信板块数量: {len(tdx_df)}")
        except Exception as e:
            print(f"读取通达信配置表时出错: {e}")
            result['通达信板块'] = pd.DataFrame()
        
        conn.close()
        return result
        
    except Exception as e:
        print(f"列出可用板块时出错: {e}")
        return {'申万板块': pd.DataFrame(), '通达信板块': pd.DataFrame()}
    



class MomentumStockSelector:
    def __init__(self, sector_code: str, trade_date: str):
        self.sector_code = sector_code
        self.trade_date = pd.to_datetime(trade_date)
        self.stock_list = get_sector_stocks(sector_code=sector_code)
        self.data_cache = {}
        
        self._prepare_data()

    def _prepare_data(self):
        """
        一次性获取所有成分股的数据，并计算所需指标。
        """
        print("\n开始准备所有股票数据并计算指标...")
        
        # 使用项目中的批量数据获取函数
        from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest
        
        # 批量获取所有股票数据
        stock_data_dict = get_multiple_stocks_daily_data_for_backtest(
            self.stock_list, 
            self.trade_date.strftime('%Y-%m-%d')
        )
        
        for code, df in stock_data_dict.items():
            if df is not None and len(df) >= 30: # 确保有足够数据计算指标
                # 使用项目中的 zhibiao 函数计算所有技术指标
                df_with_indicators = zhibiao(df)
                
                # 计算换手率（如果数据中没有的话）
                if 'turnover_rate' not in df_with_indicators.columns:
                    # 简单的换手率计算：成交量/流通股本（这里用成交量/10000作为近似）
                    df_with_indicators['turnover_rate'] = df_with_indicators['volume'] / 10000
                
                self.data_cache[code] = df_with_indicators
                
        print(f"数据准备完毕！成功处理 {len(self.data_cache)} 只股票\n")

    def _filter_by_new_high(self) -> list:
        """
        步骤2：筛选最近8日内创20日新高的股票。
        这是实现您思路的核心步骤。
        """
        print("步骤2：正在执行“近期创20日新高”初步筛选...")
        
        preliminary_list = []
        for code, df in self.data_cache.items():
            if len(df) < 20:
                continue

            # 计算20日滚动最高价
            rolling_max_20d = df['close'].rolling(window=20).max()
            
            # 检查最近8天的收盘价是否 >= 同期的20日滚动最高价
            # .any() 的意思是，只要最近8天内有任何一天满足条件，就返回True
            is_recent_high = (df['close'].iloc[-8:] >= rolling_max_20d.iloc[-8:]).any()
            
            if is_recent_high:
                preliminary_list.append(code)
                
        print(f"初步筛选出 {len(preliminary_list)} 只股票: {preliminary_list}")
        return preliminary_list

    def run_selection(self, top_n: int = 5) -> list:
        """
        执行完整的选股逻辑。
        """
        # 步骤 1 & 2: 获取成分股并进行初步筛选
        preliminary_list = self._filter_by_new_high()
        
        if not preliminary_list:
            print("没有股票通过初步筛选，无法进行排序。")
            return []

        # 步骤 3: 计算排序指标
        print("\n步骤3：为初选股票计算排序指标（换手率均值，K值均值）...")
        ranking_data = []
        for code in preliminary_list:
            df = self.data_cache[code]
            if len(df) < 12:
                continue
            
            # ========== 计算加权平均换手率 ==========

            # 1. 分别获取最近6日 和 前7-12日的数据
            recent_6_days_turnover = df['turnover_rate'].iloc[-6:]
            older_6_days_turnover = df['turnover_rate'].iloc[-12:-6]

            # 2. 分别计算这两段时间的算术平均值
            avg_turnover_recent = recent_6_days_turnover.mean()
            avg_turnover_older = older_6_days_turnover.mean()

            # 3. 根据您设定的权重（0.6 和 0.4）计算最终的加权平均值
            avg_turnover = (avg_turnover_recent * 0.6) + (avg_turnover_older * 0.4)


            # ========== 计算加权平均K值 (逻辑完全相同) ==========

            # 1. 分别获取最近6日 和 前7-12日的数据
            recent_6_days_k = df['K'].iloc[-6:]
            older_6_days_k = df['K'].iloc[-12:-6]

            # 2. 分别计算这两段时间的算术平均值
            avg_k_recent = recent_6_days_k.mean()
            avg_k_older = older_6_days_k.mean()

            # 3. 根据您设定的权重（0.6 和 0.4）计算最终的加权平均值
            avg_k_value = (avg_k_recent * 0.6) + (avg_k_older * 0.4)
            
            ranking_data.append({
                'code': code,
                'avg_turnover': avg_turnover,
                'avg_k': avg_k_value
            })

        if not ranking_data:
            print("计算排序指标时出错或数据不足。")
            return []
            
        # 将排序数据转为DataFrame
        rank_df = pd.DataFrame(ranking_data)
        
        # 步骤 4: 排序与最终筛选
        print("\n步骤4：执行双重排序并选出最终结果...")
        
        # 分别计算排名，数值越大排名越靠前 (ascending=False)
        # .rank() 会返回名次，例如第一名是1.0，第二名是2.0
        rank_df['turnover_rank'] = rank_df['avg_turnover'].rank(ascending=False, method='min')
        rank_df['k_rank'] = rank_df['avg_k'].rank(ascending=False, method='min')
        
        # 将两个排名相加，综合排名越小越好
        rank_df['combined_rank'] = rank_df['turnover_rank'] + rank_df['k_rank']
        
        # 按综合排名升序排序
        final_df = rank_df.sort_values(by='combined_rank', ascending=True)
        
        print("排序后的完整列表：")
        print(final_df)
        
        # 选出排名前 top_n 的股票
        final_selection = final_df.head(top_n)['code'].tolist()
        
        return final_selection



#挑选板块的波段1波增量个股：
# 3. 核心：重构后的 StockScreener 类
# ==============================================================================
class StockScreener:
    """
    一个用于筛选股票的类，整合了数据获取、指标计算和条件判断。
    """
    def __init__(self, stock_list, trade_date):
        """
        初始化股票筛选器。

        Args:
            stock_list (list): 待筛选的股票代码列表。
            trade_date (str or pd.Timestamp): 交易日期（筛选基于此日期的数据）。
        """
        self.stock_list = stock_list
        self.trade_date = pd.to_datetime(trade_date)
        self.data_cache = {} # 用于缓存已计算指标的DataFrame

        # 初始化时，一次性准备好所有股票的数据和指标
        self._prepare_all_data()

    def _prepare_all_data(self):
        """
        为股票池中的所有股票获取数据并计算技术指标。
        这是整个流程效率提升的关键。
        """
        print("开始为所有股票准备数据和计算指标...")
        
        # 使用项目中的批量数据获取函数
        from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest
        
        # 批量获取所有股票数据
        stock_data_dict = get_multiple_stocks_daily_data_for_backtest(
            self.stock_list, 
            self.trade_date.strftime('%Y-%m-%d')
        )
        
        for code, df in stock_data_dict.items():
            if df is not None and not df.empty and len(df) >= 20: # 确保有足够数据计算指标
                # 使用项目中的 zhibiao 函数计算所有指标
                self.data_cache[code] = zhibiao(df)
                
        print(f"所有股票数据准备完毕！成功处理 {len(self.data_cache)} 只股票\n")

    # --- 以下是根据旧逻辑改写的条件检查方法 ---
    
    def _check_new_high_10_20(self, df):
        """条件1: 最近10日创出20日新高"""
        if len(df) < 20:
            return False
        return df['close'].iloc[-10:].max() == df['close'].iloc[-20:].max()

    def _check_macd_positive(self, df):
        """条件2: MACD > 0"""
        if 'MACD' not in df.columns or len(df) < 1:
            return False
        # 使用 .iloc[-1] 获取最后一天的值
        return df['MACD'].iloc[-1] > 0

    def _check_j_oversold(self, df):
        """条件3: KDJ的J值在最近9日内有至少2天大于85"""
        if 'J' not in df.columns or len(df) < 9:
            return False
        return (df['J'].iloc[-9:] > 85).sum() >= 2
        
    def _calculate_gain_10_12(self, df):
        """计算涨幅: 最近10日最高价 / 最近12日最低价"""
        if len(df) < 12:
            return None # 无法计算
        return df['close'].iloc[-10:].max() / df['close'].iloc[-12:].min()

    def run_screening(self):
        """
        执行完整的筛选流程。
        """
        print(f"在 {self.trade_date.strftime('%Y-%m-%d')} 执行筛选策略...")
        
        final_results = []
        for code in self.stock_list:
            df = self.data_cache.get(code)
            
            # 如果没有数据或数据不足，则跳过
            if df is None or df.empty:
                continue

            # --- 应用所有筛选条件 ---
            # 1. 判断硬性条件
            cond_new_high = self._check_new_high_10_20(df)
            cond_macd = self._check_macd_positive(df)
            cond_j = self._check_j_oversold(df)

            # 只有当所有硬性条件都满足时，才进行下一步计算
            if cond_new_high and cond_macd and cond_j:
                # 2. 计算涨幅并判断
                gain = self._calculate_gain_10_12(df)
                
                # 注意：您原始的筛选条件 (df['涨幅']<1.18) & (df['涨幅']<1.05)
                # 等价于 df['涨幅'] < 1.05。这里我将遵循这个逻辑。
                # 如果您意在筛选一个区间，比如 1.05 < gain < 1.18，可以修改下面的逻辑。
                if gain is not None and gain < 1.05:
                    final_results.append({
                        'code': code,
                        'gain': gain
                    })
        
        # 如果没有符合条件的股票，返回空列表
        if not final_results:
            print("没有找到符合所有条件的股票。")
            return []
            
        # 使用DataFrame进行排序
        results_df = pd.DataFrame(final_results)
        results_df = results_df.sort_values(by='gain', ascending=False)
        
        print("\n筛选结果:")
        print(results_df)
        
        return results_df['code'].tolist()


class StockScreener_zj:#挑选1波中级增量股
    """
    一个用于筛选股票的类，整合了数据获取、指标计算和条件判断。
    (已按新需求改造)
    """
    def __init__(self, stock_list, trade_date):
        self.stock_list = stock_list
        self.trade_date = pd.to_datetime(trade_date)
        self.data_cache = {}
        self._prepare_all_data()

    def _prepare_all_data(self):
        """为所有股票准备数据和指标 (保持不变)"""
        print("开始为所有股票准备数据和计算指标...")
        stock_data_dict = get_multiple_stocks_daily_data_for_backtest(
            self.stock_list, self.trade_date.strftime('%Y-%m-%d')
        )
        for code, df in stock_data_dict.items():
            if df is not None and not df.empty and len(df) >= 40: # 确保数据足够
                self.data_cache[code] = zhibiao(df)
        print(f"所有股票数据准备完毕！成功处理 {len(self.data_cache)} 只股票\n")

    # --- 新增的、与新逻辑对应的私有方法 ---
    
    def _check_new_high_15_40(self, df: pd.DataFrame) -> bool:
        """条件2: 最近15日创40日新高"""
        if len(df) < 40:
            return False
        # 获取最近40天和最近15天的数据
        recent_40_days = df.iloc[-40:]
        recent_15_days = df.iloc[-15:]
        # 判断最近15日的最高价是否等于最近40日的最高价
        return recent_15_days['high'].max() == recent_40_days['high'].max()

    def _calculate_weighted_turnover(self, df: pd.DataFrame) -> float:
        """计算加权平均换手率"""
        if 'turnover' not in df.columns or len(df) < 30:
            return 0.0 # 数据不足则返回0
            
        recent_30_days_turnover = df['turnover'].iloc[-30:]
        
        # 切片获取最近10天和11-30天的数据
        last_10_days = recent_30_days_turnover.iloc[-10:]
        prev_20_days = recent_30_days_turnover.iloc[:20]
        
        # 计算加权平均
        weighted_avg = (last_10_days.mean() * 0.6) + (prev_20_days.mean() * 0.4)
        return weighted_avg

    # --- 全面重写 run_screening 方法 ---
    
    def run_screening(self) -> list:
        """
        执行完整的多阶段筛选流程。
        """
        print(f"--- 在 {self.trade_date.strftime('%Y-%m-%d')} 执行筛选 ---")
        
        # 步骤 1: 30BIAS高低排序获取排名前40%
        print("\n[步骤 1] 按 BIAS_30 进行初步筛选...")
        bias_values = {}
        for code, df in self.data_cache.items():
            if 'BIAS_30' in df.columns and not pd.isna(df['BIAS_30'].iloc[-1]):
                bias_values[code] = df['BIAS_30'].iloc[-1]
        
        if not bias_values:
            print("没有足够的数据计算BIAS，筛选终止。")
            return []
            
        bias_series = pd.Series(bias_values).sort_values(ascending=False)
        top_40_percent_count = int(len(bias_series) * 0.4)
        bias_pool = bias_series.head(top_40_percent_count).index.tolist()
        print(f"共 {len(bias_series)} 只股票参与BIAS排序，选出前40%共 {len(bias_pool)} 只。")

        # 步骤 2: 在BIAS池中，筛选“最近15日创40日新高”的股票
        print("\n[步骤 2] 在BIAS池中筛选'新高'股票...")
        stage2_pool = []
        for code in bias_pool:
            df = self.data_cache.get(code)
            if self._check_new_high_15_40(df):
                stage2_pool.append(code)
        print(f"满足'新高'条件的股票共 {len(stage2_pool)} 只: {stage2_pool}")

        # 步骤 3: 根据数量进行判断
        print("\n[步骤 3] 根据候选股数量进行决策...")
        if len(stage2_pool) <= 6:
            print(f"数量 ({len(stage2_pool)}) <= 6，直接选入所有候选股。")
            final_selection = stage2_pool
        else:
            print(f"数量 ({len(stage2_pool)}) > 6，需要进行加权换手率排序。")
            
            # 步骤 4: 计算加权换手率并排序，选出前70%
            turnover_values = {}
            for code in stage2_pool:
                df = self.data_cache.get(code)
                turnover_values[code] = self._calculate_weighted_turnover(df)
            
            turnover_series = pd.Series(turnover_values).sort_values(ascending=False)
            top_70_percent_count = int(len(turnover_series) * 0.7)
            
            print("加权换手率排序结果:")
            print(turnover_series)
            
            final_selection = turnover_series.head(top_70_percent_count).index.tolist()
            print(f"按换手率选出前70%共 {len(final_selection)} 只股票。")

        print("\n--- 筛选流程结束 ---")
        print(f"最终选出的股票列表: {final_selection}")
        return final_selection




def convert_dict_to_long_format(stock_data_dict: dict) -> pd.DataFrame:
    """
    将股票数据字典转换为长格式DataFrame
    
    Args:
        stock_data_dict (dict): 以股票代码为键，DataFrame为值的字典
        
    Returns:
        pd.DataFrame: 包含所有股票数据的长格式DataFrame，包含stock_code列
    """
    print("正在将字典格式转换为长格式...")
    
    all_dataframes = []
    
    for stock_code, df in stock_data_dict.items():
        if not df.empty:
            # 为每个DataFrame添加stock_code列
            df_copy = df.copy()
            df_copy['stock_code'] = stock_code
            all_dataframes.append(df_copy)
            print(f"  添加股票 {stock_code}: {len(df_copy)} 条记录")
    
    if all_dataframes:
        # 合并所有DataFrame
        long_format_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"转换完成，总共 {len(long_format_df)} 条记录")
        return long_format_df
    else:
        print("警告：没有有效的数据可以转换")
        return pd.DataFrame()


#我们需要一个预处理步骤，将您的"长格式"数据转换成策略类需要的"面板格式"。这通常通过一个独立的辅助函数来完成，而不是在类里面做，这样可以保持类的职责纯粹。
def prepare_panel_data(long_format_df: pd.DataFrame, stock_list: list) -> dict:
    """
    将您的标准长格式数据，转换为策略类所需的多维面板数据格式。

    Args:
        long_format_df (pd.DataFrame): 包含 stock_code, trade_date 等列的原始数据。
        stock_list (list): 需要处理的股票列表。

    Returns:
        dict: 包含 'open', 'high', 'low', 'close' 等键的字典，
              每个键对应一个以日期为索引、股票代码为列的DataFrame。
    """
    print("--- 正在进行数据预处理：将长格式转换为面板格式 ---")
    # 1. 筛选我们关心的股票和列
    df = long_format_df[long_format_df['stock_code'].isin(stock_list)].copy()
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    panel_dict = {}
    ohlc_cols = ['open', 'high', 'low', 'close', 'volume'] # 您可以按需增减

    for col in ohlc_cols:
        # 2. 使用 pivot 操作将数据从长格式转换为宽格式
        panel = df.pivot(index='trade_date', columns='stock_code', values=col)
        panel_dict[col] = panel
    
    print("--- 数据预处理完成 ---")
    return {'daily': panel_dict}


class MyBankuaiStrategy:
    """
    板块策略类（根据您的需求和数据格式量身定制）。
    """
    def __init__(self, price_data_dict: dict, stock_list: list, **kwargs):
        """
        初始化方法：接收已经预处理好的面板数据字典。
        """
        print("1. 板块策略对象初始化...")
        self.stock_list = stock_list
        
        # price_data_dict 结构: {'daily': {'open': df, 'high': df, ...}}
        if 'daily' not in price_data_dict:
            raise ValueError("price_data_dict 必须包含 'daily' 数据。")
            
        # 这里的 price_data_dict['daily'] 本身就是一个字典，所以直接赋值
        self.daily_df_dict = price_data_dict['daily']
        # 您也可以在这里处理周线、月线数据
        
        self.params = kwargs
        self.sector_stats = pd.DataFrame(index=self.daily_df_dict['close'].index)
        self.indicator_panels = {} # 用于存储所有股票的指标面板

    def calculate_indicators(self):
        """
        指标计算：
        为板块内的每一只股票调用您的zhibiao函数，并将结果重新组合成指标面板。
        """
        print("2. 正在为板块内所有股票计算指标...")
        all_indicators_list = []
        
        for stock in self.stock_list:
            # 检查股票是否存在于数据中
            if stock not in self.daily_df_dict['close'].columns:
                print(f"警告：股票 {stock} 不存在于数据中，跳过")
                continue
                
            # a. 为单只股票准备一个符合zhibiao格式的DataFrame
            try:
                stock_df = pd.DataFrame({
                    'open': self.daily_df_dict['open'][stock],
                    'high': self.daily_df_dict['high'][stock],
                    'low': self.daily_df_dict['low'][stock],
                    'close': self.daily_df_dict['close'][stock],
                    'volume': self.daily_df_dict['volume'][stock],
                })
                # 添加 trade_date 列（从索引中获取）
                # 确保索引是datetime类型，然后转换为字符串格式
                if isinstance(stock_df.index, pd.DatetimeIndex):
                    stock_df['trade_date'] = stock_df.index
                else:
                    stock_df['trade_date'] = pd.to_datetime(stock_df.index)
                stock_df['stock_code'] = stock # 添加股票代码列，方便后续处理
                
                # b. 调用您熟悉的zhibiao函数
                stock_indicators = zhibiao(stock_df)
                all_indicators_list.append(stock_indicators)
            except Exception as e:
                print(f"警告：处理股票 {stock} 时出错: {e}")
                continue

        # c. 将所有股票的指标结果合并回一个大的DataFrame
        if not all_indicators_list:
            print("错误：没有成功计算任何股票的指标")
            return
            
        full_indicator_df = pd.concat(all_indicators_list)
        
        # 确保 trade_date 列存在且为 datetime 类型
        if 'trade_date' not in full_indicator_df.columns:
            print("错误：合并后的指标DataFrame缺少trade_date列")
            return
        
        full_indicator_df['trade_date'] = pd.to_datetime(full_indicator_df['trade_date'])
        
        # d. 将这个长格式的指标表，再次转换为我们需要的面板格式
        #    这样 self.indicator_panels['MACD'] 就会是一个 (日期 x 股票) 的DataFrame
        for col in ['DIF', 'DEA', 'MACD', 'K', 'D', 'J', 'MA_7','MA_26', 'VOL_5', 'VOL_30']: # 按需添加您关心的指标
            if col in full_indicator_df.columns:
                panel = full_indicator_df.pivot(index='trade_date', columns='stock_code', values=col)
                self.indicator_panels[col] = panel
            else:
                print(f"警告：指标列 '{col}' 不存在于数据中")
            
        print("   所有股票的指标已计算并重组为面板格式。")
    
    def chixu_genzj(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        例如：计算板块内MACD < 0的股票占比。
        """
        print("3. 正在执行板块统计 'chixu_genzj'...")
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
            
        # 1. 从存储中获取需要分析的指标面板 (例如MACD)
        #    这是一个以日期为索引，股票代码为列的DataFrame
        macd_panel = self.indicator_panels['MACD']
        
        # 2. 应用您的技术条件 (例如 MACD < 0)
        #    这会返回一个同样大小的布尔型DataFrame (True/False)
        condition_met_panel = macd_panel < 0
        
        # 3. 统计每日满足条件的股票数量
        #    .sum(axis=1) 会横向（沿着列）求和。True计为1，False计为0。
        count_met = condition_met_panel.sum(axis=1)
        
        # 4. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 5. 将这个新的统计序列存入 sector_stats
        self.sector_stats['MACD_neg_proportion'] = proportion
        
        print("   'MACD<0股票占比' 已计算并存储。")
        return proportion # 也可以直接返回结果

    def chixu_genzj7x26(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        例如：计算板块内MA7 < MA26的股票占比。
        """
        print("3. 正在执行板块统计 'chixu_macd_proportion'...")
        if not self.indicator_panels:
            self.calculate_indicators()
        ma7_panel = self.indicator_panels['MA_7']
        ma26_panel = self.indicator_panels['MA_26']
        condition_met_panel = ma7_panel < ma26_panel
        count_met = condition_met_panel.sum(axis=1)
        proportion = count_met / len(self.stock_list)
        self.sector_stats['MA7_lt_MA26_proportion'] = proportion
        print("   'MA7<MA26股票占比' 已计算并存储。")
        return proportion
    
    def chixu_genzjding(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. MACD > 0 且 最近2天中有1天以上 VOL_5 > VOL_30
        2. 或者 最近4天连续 MACD > 0
        """
        print("4. 正在执行板块统计 'chixu_genzjding'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['MACD', 'VOL_5', 'VOL_30']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        macd_panel = self.indicator_panels['MACD']
        vol5_panel = self.indicator_panels['VOL_5'] 
        vol30_panel = self.indicator_panels['VOL_30']
        
        # 2. 创建条件面板 - 条件1: MACD > 0
        macd_positive_panel = macd_panel > 0
        
        # 3. 创建条件面板 - 条件1: 最近2天中有1天以上 VOL_5 > VOL_30
        vol_condition_panel = pd.DataFrame(index=macd_panel.index, columns=macd_panel.columns)
        for stock in self.stock_list:
            if stock not in vol5_panel.columns or stock not in vol30_panel.columns:
                print(f"警告：股票 {stock} 缺少成交量数据")
                continue
                
            vol5_series = vol5_panel[stock]
            vol30_series = vol30_panel[stock]
            
            # 计算最近2天中VOL_5 > VOL_30的天数
            for i in range(len(vol5_series)):
                if i >= 1:  # 需要至少2天数据
                    recent_vol5 = vol5_series.iloc[i-1:i+1]  # 最近2天
                    recent_vol30 = vol30_series.iloc[i-1:i+1]
                    vol_condition_panel.iloc[i, vol_condition_panel.columns.get_loc(stock)] = (recent_vol5 > recent_vol30).sum() >= 1
                else:
                    vol_condition_panel.iloc[i, vol_condition_panel.columns.get_loc(stock)] = False
        
        # 4. 创建条件面板 - 条件2: 最近4天连续 MACD > 0
        macd_4days_positive_panel = pd.DataFrame(index=macd_panel.index, columns=macd_panel.columns)
        for stock in self.stock_list:
            if stock not in macd_panel.columns:
                print(f"警告：股票 {stock} 缺少MACD数据")
                continue
                
            macd_series = macd_panel[stock]
            
            # 计算最近4天是否连续MACD > 0
            for i in range(len(macd_series)):
                if i >= 3:  # 需要至少4天数据
                    recent_macd = macd_series.iloc[i-3:i+1]  # 最近4天
                    macd_4days_positive_panel.iloc[i, macd_4days_positive_panel.columns.get_loc(stock)] = (recent_macd > 0).sum() == 4
                else:
                    macd_4days_positive_panel.iloc[i, macd_4days_positive_panel.columns.get_loc(stock)] = False
        
        # 5. 组合条件：条件1 或 条件2
        # 条件1: MACD > 0 且 最近2天中有1天以上 VOL_5 > VOL_30
        condition1_panel = macd_positive_panel & vol_condition_panel
        
        # 条件2: 最近4天连续 MACD > 0
        condition2_panel = macd_4days_positive_panel
        
        # 最终条件：条件1 或 条件2
        final_condition_panel = condition1_panel | condition2_panel
        
        # 6. 统计每日满足条件的股票数量
        count_met = final_condition_panel.sum(axis=1)
        
        # 7. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 8. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genzjding_proportion'] = proportion
        
        print("   '持续跟踪技术状态股票占比' 已计算并存储。")
        return proportion
    def chixu_genzj7d26(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. MA_7 > MA_26 且 MACD > 0
        2. 或者 最近9天连续 MACD > 0
        """
        print("5. 正在执行板块统计 'chixu_genzj7d26'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['MACD', 'MA_7', 'MA_26']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        macd_panel = self.indicator_panels['MACD']
        ma7_panel = self.indicator_panels['MA_7']
        ma26_panel = self.indicator_panels['MA_26']
        
        # 2. 创建条件面板 - 条件1: MA_7 > MA_26
        ma7_gt_ma26_panel = ma7_panel > ma26_panel
        
        # 3. 创建条件面板 - 条件1: MACD > 0
        macd_positive_panel = macd_panel > 0
        
        # 4. 创建条件面板 - 条件2: 最近9天连续 MACD > 0
        macd_9days_positive_panel = pd.DataFrame(index=macd_panel.index, columns=macd_panel.columns)
        for stock in self.stock_list:
            macd_series = macd_panel[stock]
            
            # 计算最近9天是否连续MACD > 0
            for i in range(len(macd_series)):
                if i >= 8:  # 需要至少9天数据
                    recent_macd = macd_series.iloc[i-8:i+1]  # 最近9天
                    macd_9days_positive_panel.iloc[i, macd_9days_positive_panel.columns.get_loc(stock)] = (recent_macd > 0).sum() == 9
                else:
                    macd_9days_positive_panel.iloc[i, macd_9days_positive_panel.columns.get_loc(stock)] = False
        
        # 5. 组合条件：条件1 或 条件2
        # 条件1: MA_7 > MA_26 且 MACD > 0
        condition1_panel = ma7_gt_ma26_panel & macd_positive_panel
        
        # 条件2: 最近9天连续 MACD > 0
        condition2_panel = macd_9days_positive_panel
        
        # 最终条件：条件1 或 条件2
        final_condition_panel = condition1_panel | condition2_panel
        
        # 6. 统计每日满足条件的股票数量
        count_met = final_condition_panel.sum(axis=1)
        
        # 7. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 8. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genzj7d26_proportion'] = proportion
        
        print("   '持续跟踪MA7>MA26且MACD>0技术状态股票占比' 已计算并存储。")
        return proportion
    
    def chixu_genbd(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. K < D
        2. 或者 最近2天连续 K < D
        3. 或者 最近2天连续 J值下降
        """
        print("6. 正在执行板块统计 'chixu_genbd'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['K', 'D', 'J']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        k_panel = self.indicator_panels['K']
        d_panel = self.indicator_panels['D']
        j_panel = self.indicator_panels['J']
        
        # 2. 创建条件面板 - 条件1: K < D
        k_lt_d_panel = k_panel < d_panel
        
        # 3. 创建条件面板 - 条件2: 最近2天连续 K < D
        k_2days_lt_d_panel = pd.DataFrame(index=k_panel.index, columns=k_panel.columns)
        for stock in self.stock_list:
            k_series = k_panel[stock]
            d_series = d_panel[stock]
            
            # 计算最近2天是否连续K < D
            for i in range(len(k_series)):
                if i >= 1:  # 需要至少2天数据
                    recent_k = k_series.iloc[i-1:i+1]  # 最近2天
                    recent_d = d_series.iloc[i-1:i+1]
                    k_2days_lt_d_panel.iloc[i, k_2days_lt_d_panel.columns.get_loc(stock)] = (recent_k < recent_d).sum() == 2
                else:
                    k_2days_lt_d_panel.iloc[i, k_2days_lt_d_panel.columns.get_loc(stock)] = False
        
        # 4. 创建条件面板 - 条件3: 最近2天连续 J值下降
        j_2days_decline_panel = pd.DataFrame(index=j_panel.index, columns=j_panel.columns)
        for stock in self.stock_list:
            j_series = j_panel[stock]
            
            # 计算最近2天是否连续J值下降
            for i in range(len(j_series)):
                if i >= 1:  # 需要至少2天数据
                    recent_j = j_series.iloc[i-1:i+1]  # 最近2天
                    # 检查是否连续下降：J[i] < J[i-1]
                    j_decline = (recent_j.iloc[1] < recent_j.iloc[0])
                    j_2days_decline_panel.iloc[i, j_2days_decline_panel.columns.get_loc(stock)] = j_decline
                else:
                    j_2days_decline_panel.iloc[i, j_2days_decline_panel.columns.get_loc(stock)] = False
        
        # 5. 组合条件：条件1 或 条件2 或 条件3
        # 条件1: K < D
        condition1_panel = k_lt_d_panel
        
        # 条件2: 最近2天连续 K < D
        condition2_panel = k_2days_lt_d_panel
        
        # 条件3: 最近2天连续 J值下降
        condition3_panel = j_2days_decline_panel
        
        # 最终条件：条件1 或 条件2 或 条件3
        final_condition_panel = condition1_panel | condition2_panel | condition3_panel
        
        # 6. 统计每日满足条件的股票数量
        count_met = final_condition_panel.sum(axis=1)
        
        # 7. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 8. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genbd_proportion'] = proportion
        
        print("   '持续跟踪KDJ技术状态股票占比' 已计算并存储。")
        return proportion
    
    def chixu_genbdqsdi(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. 最近4天连续 K < D
        """
        print("7. 正在执行板块统计 'chixu_genbdqsdi'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['K', 'D']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        k_panel = self.indicator_panels['K']
        d_panel = self.indicator_panels['D']
        
        # 2. 创建条件面板 - 最近4天连续 K < D
        k_4days_lt_d_panel = pd.DataFrame(index=k_panel.index, columns=k_panel.columns)
        for stock in self.stock_list:
            k_series = k_panel[stock]
            d_series = d_panel[stock]
            
            # 计算最近4天是否连续K < D
            for i in range(len(k_series)):
                if i >= 3:  # 需要至少4天数据
                    recent_k = k_series.iloc[i-3:i+1]  # 最近4天
                    recent_d = d_series.iloc[i-3:i+1]
                    k_4days_lt_d_panel.iloc[i, k_4days_lt_d_panel.columns.get_loc(stock)] = (recent_k < recent_d).sum() == 4
                else:
                    k_4days_lt_d_panel.iloc[i, k_4days_lt_d_panel.columns.get_loc(stock)] = False
        
        # 3. 统计每日满足条件的股票数量
        count_met = k_4days_lt_d_panel.sum(axis=1)
        
        # 4. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 5. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genbdqsdi_proportion'] = proportion
        
        print("   '持续跟踪4天连续K<D技术状态股票占比' 已计算并存储。")
        return proportion
    
    def chixu_genbdding(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. 最近2天中有1天以上 K > D 且 MACD > 0
        2. 或者 最近2天连续 K > D
        """
        print("8. 正在执行板块统计 'chixu_genbdding'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['K', 'D', 'MACD']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        k_panel = self.indicator_panels['K']
        d_panel = self.indicator_panels['D']
        macd_panel = self.indicator_panels['MACD']
        
        # 2. 创建条件面板 - 条件1: 最近2天中有1天以上 K > D 且 MACD > 0
        condition1_panel = pd.DataFrame(index=k_panel.index, columns=k_panel.columns)
        for stock in self.stock_list:
            k_series = k_panel[stock]
            d_series = d_panel[stock]
            macd_series = macd_panel[stock]
            
            # 计算最近2天中有1天以上 K > D 且 MACD > 0
            for i in range(len(k_series)):
                if i >= 1:  # 需要至少2天数据
                    recent_k = k_series.iloc[i-1:i+1]  # 最近2天
                    recent_d = d_series.iloc[i-1:i+1]
                    recent_macd = macd_series.iloc[i-1:i+1]
                    
                    # 检查最近2天中是否有1天以上满足 K > D 且 MACD > 0
                    k_gt_d = recent_k > recent_d
                    macd_positive = recent_macd > 0
                    combined_condition = k_gt_d & macd_positive
                    condition1_panel.iloc[i, condition1_panel.columns.get_loc(stock)] = combined_condition.sum() >= 1
                else:
                    condition1_panel.iloc[i, condition1_panel.columns.get_loc(stock)] = False
        
        # 3. 创建条件面板 - 条件2: 最近2天连续 K > D
        condition2_panel = pd.DataFrame(index=k_panel.index, columns=k_panel.columns)
        for stock in self.stock_list:
            k_series = k_panel[stock]
            d_series = d_panel[stock]
            
            # 计算最近2天是否连续K > D
            for i in range(len(k_series)):
                if i >= 1:  # 需要至少2天数据
                    recent_k = k_series.iloc[i-1:i+1]  # 最近2天
                    recent_d = d_series.iloc[i-1:i+1]
                    condition2_panel.iloc[i, condition2_panel.columns.get_loc(stock)] = (recent_k > recent_d).sum() == 2
                else:
                    condition2_panel.iloc[i, condition2_panel.columns.get_loc(stock)] = False
        
        # 4. 组合条件：条件1 或 条件2
        final_condition_panel = condition1_panel | condition2_panel
        
        # 5. 统计每日满足条件的股票数量
        count_met = final_condition_panel.sum(axis=1)
        
        # 6. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 7. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genbdding_proportion'] = proportion
        
        print("   '持续跟踪波段折返顶技术状态股票占比' 已计算并存储。")
        return proportion
    
    def chixu_genbdqsding(self):
        """
        板块统计方法：跟踪特定技术状态的股票比例。
        技术条件：
        1. 最近4天连续 K > D 且 MACD > 0
        """
        print("9. 正在执行板块统计 'chixu_genbdqsding'...")
        
        # 0. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()
        
        # 1. 获取需要的指标面板
        required_indicators = ['K', 'D', 'MACD']
        for indicator in required_indicators:
            if indicator not in self.indicator_panels:
                print(f"错误：缺少必要的指标面板 '{indicator}'")
                return pd.Series()
        
        k_panel = self.indicator_panels['K']
        d_panel = self.indicator_panels['D']
        macd_panel = self.indicator_panels['MACD']
        
        # 2. 创建条件面板 - 最近4天连续 K > D 且 MACD > 0
        k_4days_gt_d_and_macd_positive_panel = pd.DataFrame(index=k_panel.index, columns=k_panel.columns)
        for stock in self.stock_list:
            k_series = k_panel[stock]
            d_series = d_panel[stock]
            macd_series = macd_panel[stock]
            
            # 计算最近4天是否连续K > D 且 MACD > 0
            for i in range(len(k_series)):
                if i >= 3:  # 需要至少4天数据
                    recent_k = k_series.iloc[i-3:i+1]  # 最近4天
                    recent_d = d_series.iloc[i-3:i+1]
                    recent_macd = macd_series.iloc[i-3:i+1]
                    
                    # 检查最近4天是否连续满足 K > D 且 MACD > 0
                    k_gt_d = recent_k > recent_d
                    macd_positive = recent_macd > 0
                    combined_condition = k_gt_d & macd_positive
                    k_4days_gt_d_and_macd_positive_panel.iloc[i, k_4days_gt_d_and_macd_positive_panel.columns.get_loc(stock)] = combined_condition.sum() == 4
                else:
                    k_4days_gt_d_and_macd_positive_panel.iloc[i, k_4days_gt_d_and_macd_positive_panel.columns.get_loc(stock)] = False
        
        # 3. 统计每日满足条件的股票数量
        count_met = k_4days_gt_d_and_macd_positive_panel.sum(axis=1)
        
        # 4. 计算每日满足条件的股票比例
        proportion = count_met / len(self.stock_list)
        
        # 5. 将这个新的统计序列存入 sector_stats
        self.sector_stats['genbdqsding_proportion'] = proportion
        
        print("   '持续跟踪4天连续K>D且MACD>0技术状态股票占比' 已计算并存储。")
        return proportion
    
    def generate_signals(self):
        """
        生成买卖信号：
        基于板块统计指标，决定何时买入或卖出。
        """
        print("4. 正在生成买卖信号...")
        
        # 1. 确保指标已计算
        if not self.indicator_panels:
            self.calculate_indicators()

        # 2. 生成买入信号
        # 首先确保必要的统计指标已计算
        if 'MACD_neg_proportion' not in self.sector_stats.columns:
            print("   警告：MACD_neg_proportion 未计算，正在计算...")
            self.chixu_genzj()
        
        # proportion 是 chixu_genzj 计算出的完整的历史占比序列
        proportion = self.sector_stats['MACD_neg_proportion'] 

        # 条件1：占比 >= 0.4
        condition1 = proportion >= 0.4

        # 条件2：占比 > 前一日的占比
        # .shift(1) 可以获取前一日的数据，这是pandas的强大功能
        condition2 = proportion > proportion.shift(1)

        # 最终的买入信号是两个条件同时满足
        entries = condition1 & condition2

        # 简单定义一个退出逻辑：当占比低于0.1时退出
        exits = proportion < 0.1
        
        print("   信号已生成，正在进行清理...")
        # 返回清理后的信号
        return entries, exits










# # 定义要分析的板块和股票列表
# my_stock_list =['601696', '002736', '600958', '000776', '600864', '601555', '600095', '000712']


# # 1. 创建模拟的原始数据
# print(">>> 第一步：创建模拟原始数据...")
# stock_data_dict = get_multiple_stocks_daily_data_for_backtest(my_stock_list, current_date='2025-09-04')

# print(f"获取到的股票数据字典包含 {len(stock_data_dict)} 只股票")
# for stock_code, df in stock_data_dict.items():
#     print(f"  {stock_code}: {len(df)} 条记录")

# # 将字典格式转换为长格式DataFrame
# print(">>> 第一步.5：将字典格式转换为长格式...")
# long_format_df = convert_dict_to_long_format(stock_data_dict)

# if long_format_df.empty:
#     print("错误：转换后的长格式DataFrame为空，无法继续执行")
#     exit(1)

# # 2. 预处理数据
# print(">>> 第二步：预处理数据...")
# price_data_dict = prepare_panel_data(long_format_df, my_stock_list)
# print("预处理后的数据结构 (以close为例):")
# print(price_data_dict['daily']['close'].head())
# print("\n" + "="*50 + "\n")
# # 3. 创建策略对象
# print(">>> 第三步：创建策略对象...")
# strategy = MyBankuaiStrategy(price_data_dict=price_data_dict, stock_list=my_stock_list)

# # 4. 计算指标
# print(">>> 第四步：计算指标...")
# strategy.calculate_indicators()

# # 5. 调用核心方法并查看中间结果
# print(">>> 第五步：调用核心方法...")
# # 调用 chixu_genzj, 它会自动触发 calculate_indicators
# proportion_series = strategy.chixu_genzj()
# print("\n板块统计结果 (最近10天):")
# print(proportion_series.tail(10))
# print("\n" + "="*50 + "\n")

# # 5.5. 调用新的 chixu_genzjding 方法
# print(">>> 第五步.5：调用持续跟踪技术状态方法...")
# genzjding_result = strategy.chixu_genzjding()
# print("\n持续跟踪技术状态结果 (最近10天):")
# print(genzjding_result.tail(10))
# print("\n" + "="*50 + "\n")

# # 5.6. 调用新的 chixu_genzj7d26 方法
# print(">>> 第五步.6：调用持续跟踪MA7>MA26且MACD>0技术状态方法...")
# genzj7d26_result = strategy.chixu_genzj7d26()
# print("\n持续跟踪MA7>MA26且MACD>0技术状态结果 (最近10天):")
# print(genzj7d26_result.tail(10))
# print("\n" + "="*50 + "\n")

# # 5.7. 调用新的 chixu_genbd 方法
# print(">>> 第五步.7：调用持续跟踪KDJ技术状态方法...")
# genbd_result = strategy.chixu_genbd()
# print("\n持续跟踪KDJ技术状态结果 (最近10天):")
# print(genbd_result.tail(10))
# print("\n" + "="*50 + "\n")

# # 5.8. 调用新的 chixu_genbdqsdi 方法
# print(">>> 第五步.8：调用持续跟踪4天连续K<D技术状态方法...")
# genbdqsdi_result = strategy.chixu_genbdqsdi()
# print("\n持续跟踪4天连续K<D技术状态结果 (最近10天):")
# print(genbdqsdi_result.tail(10))
# print("\n" + "="*50 + "\n")

# 5.9. 调用新的 chixu_genbdding 方法
# print(">>> 第五步.9：调用持续跟踪波段折返顶技术状态方法...")
# genbdding_result = strategy.chixu_genbdding()
# print("\n持续跟踪波段折返顶技术状态结果 (最近10天):")
# print(genbdding_result.tail(10))
# print("\n" + "="*50 + "\n")

# 5.10. 调用新的 chixu_genbdqsding 方法
# print(">>> 第五步.10：调用持续跟踪4天连续K>D且MACD>0技术状态方法...")
# genbdqsding_result = strategy.chixu_genbdqsding()
# print("\n持续跟踪4天连续K>D且MACD>0技术状态结果 (最近10天):")
# print(genbdqsding_result.tail(10))
# print("\n" + "="*50 + "\n")
if __name__ == "__main__":
    pass
# else:
# # 测试板块成分股获取函数
#     print(">>> 测试板块成分股获取函数...")
#     print("\n1. 列出所有可用板块:")
#     available_sectors = list_available_sectors()
#     for sector_type, df in available_sectors.items():
#         if not df.empty:
#             print(f"\n{sector_type}:")
#             print(f"  列名: {df.columns.tolist()}")
#             print(f"  前5条记录:")
#             print(df.head())

#     print("\n2. 测试获取板块成分股:")
#     # 使用数据库中实际存在的板块代码和名称进行测试
#     if not available_sectors['申万板块'].empty:
#         # 从申万板块中获取一些实际的板块代码和名称
#         sw_df = available_sectors['申万板块']
#         test_codes = sw_df['index_code'].unique()[:3]  # 取前3个不同的板块代码
#         test_names = sw_df['industry_name'].unique()[:3]  # 取前3个不同的行业名称
        
#         print(f"使用申万板块进行测试:")
#         for code in test_codes:
#             print(f"\n尝试通过代码 '{code}' 获取成分股:")
#             stocks = get_sector_stocks(sector_code=code)
#             if stocks:
#                 print(f"  找到 {len(stocks)} 只成分股: {stocks[:5]}...")  # 只显示前5只
#             else:
#                 print("  未找到成分股")

#         for name in test_names:
#             print(f"\n尝试通过名称 '{name}' 获取成分股:")
#             stocks = get_sector_stocks(sector_name=name)
#             if stocks:
#                 print(f"  找到 {len(stocks)} 只成分股: {stocks[:5]}...")  # 只显示前5只
#             else:
#                 print("  未找到成分股")

#         if not available_sectors['通达信板块'].empty:
#             # 从通达信板块中获取一些实际的板块代码和名称
#             tdx_df = available_sectors['通达信板块']
#             test_codes = tdx_df['index_code'].unique()[:3]  # 取前3个不同的板块代码
#             test_names = tdx_df['industry_name'].unique()[:3]  # 取前3个不同的行业名称
            
#             print(f"\n使用通达信板块进行测试:")
#             for code in test_codes:
#                 print(f"\n尝试通过代码 '{code}' 获取成分股:")
#                 stocks = get_sector_stocks(sector_code=code)
#                 if stocks:
#                     print(f"  找到 {len(stocks)} 只成分股: {stocks[:5]}...")  # 只显示前5只
#                 else:
#                     print("  未找到成分股")

#             for name in test_names:
#                 print(f"\n尝试通过名称 '{name}' 获取成分股:")
#                 stocks = get_sector_stocks(sector_name=name)
#                 if stocks:
#                     print(f"  找到 {len(stocks)} 只成分股: {stocks[:5]}...")  # 只显示前5只
#                 else:
#                     print("  未找到成分股")

#         print("\n" + "="*50 + "\n")

#         # 测试修改后的类
#         print(">>> 测试修改后的 MomentumStockSelector 和 StockScreener 类...")

#         # 测试 MomentumStockSelector
#         print("\n1. 测试 MomentumStockSelector 类:")
#         try:
#             # 使用一个实际的板块代码进行测试
#             test_sector_code = '801170.SI'  # 交通运输
#             test_date = '2024-01-15'
            
#             print(f"使用板块代码: {test_sector_code}, 日期: {test_date}")
#             momentum_selector = MomentumStockSelector(test_sector_code, test_date)
            
#             # 运行选股逻辑
#             selected_stocks = momentum_selector.run_selection(top_n=3)
#             print(f"选出的股票: {selected_stocks}")
            
#         except Exception as e:
#             print(f"MomentumStockSelector 测试失败: {e}")

#         # 测试 StockScreener
#         print("\n2. 测试 StockScreener 类:")
#         try:
#             # 使用一些测试股票代码
#             test_stock_list = ['000001', '000002', '000858']
#             test_date = '2024-01-15'
            
#             print(f"使用股票列表: {test_stock_list}, 日期: {test_date}")
#             stock_screener = StockScreener(test_stock_list, test_date)
            
#             # 运行筛选逻辑
#             screened_stocks = stock_screener.run_screening()
#             print(f"筛选出的股票: {screened_stocks}")
            
#         except Exception as e:
#             print(f"StockScreener 测试失败: {e}")

#         print("\n" + "="*50 + "\n")



# # 5.5. 调用新的 chixu_genzjding 方法
# print(">>> 第五步.5：调用持续跟踪技术状态方法...")
# genzjding_result = strategy.chixu_genzjding()
# print("\n持续跟踪技术状态结果 (最近10天):")
# print(genzjding_result.tail(10))
# print("\n" + "="*50 + "\n")

# # 5.6. 调用新的 chixu_genzj7d26 方法
# print(">>> 第五步.6：调用持续跟踪MA7>MA26且MACD>0技术状态方法...")
# genzj7d26_result = strategy.chixu_genzj7d26()
# print("\n持续跟踪MA7>MA26且MACD>0技术状态结果 (最近10天):")
# print(genzj7d26_result.tail(10))
# print("\n" + "="*50 + "\n")

# # 6. 生成信号
# print(">>> 第六步：生成信号...")
# print(">>> 第五步：生成最终交易信号...")
# entries, exits = strategy.generate_signals()
# print("\n买入信号 (最近出现的几次):")
# print(entries[entries].tail())
# print("\n卖出信号 (最近出现的几次):")
# print(exits[exits].tail())