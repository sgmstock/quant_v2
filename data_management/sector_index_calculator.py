import pandas as pd
import numpy as np
import sqlite3
from typing import Tuple
import sys
import os

# 导入v2项目的模块
from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest
from core.utils.indicators import zhibiao



class ActiveStockScreener:
    """
    一个根据活跃度标准来筛选板块成分股的工具类。

    它实现了以下功能：
    1. 从数据库获取流通A股数据
    2. 计算平均换手率和成交量爆发天数
    3. 挑选20%的个股，且不低于3只
    4. 如果板块成分股少于等于3只就不挑选，直接使用所有成分股
    """
    def __init__(self, sector_code: str, trade_date: str):
        """
        初始化筛选器。

        Args:
            sector_code (str): 板块代码，如 '801193.SI'
            trade_date (str): 交易日期，格式 'YYYY-MM-DD'
        """
        print(f"1. 活跃股筛选器初始化...")
        print(f"   板块代码: {sector_code}")
        print(f"   交易日期: {trade_date}")
        
        self.sector_code = sector_code
        self.trade_date = trade_date
        
        # 获取板块成分股
        try:
            from core.utils.stock_filter import get_bankuai_stocks
            self.stock_list = get_bankuai_stocks(sector_code)
        except ImportError:
            # 如果策略模块不存在，使用默认的股票列表
            print(f"   警告: 无法导入get_bankuai_stocks，使用默认股票列表")
            self.stock_list = ['000001', '000002', '000858']  # 默认股票列表
        
        if len(self.stock_list) <= 3:
            print(f"   板块成分股数量({len(self.stock_list)}) <= 3，将直接使用所有成分股")
            self.selected_stocks = self.stock_list
            return
        
        print(f"   获取到 {len(self.stock_list)} 只成分股")
        
        # 获取股票数据和流通股数据
        self._prepare_data()
        
        # 计算指标
        self._calculate_metrics()
        
        # 执行筛选
        self._screen_stocks()

    def _prepare_data(self):
        """
        准备股票数据和流通股数据
        """
        print("2. 准备股票数据和流通股数据...")
        
        # 获取股票行情数据
        stock_data_dict = get_multiple_stocks_daily_data_for_backtest(
            self.stock_list, 
            self.trade_date
        )
        
        # 获取流通股数据
        self._get_circulating_shares()
        
        # 【优化】使用字典收集数据，避免DataFrame碎片化
        volume_data = {}
        shares_data = {}
        
        for stock_code, df in stock_data_dict.items():
            if stock_code in self.circulating_shares and not df.empty:
                # 确保数据有足够的长度
                if len(df) >= 30:
                    # 设置日期为索引
                    df_indexed = df.set_index('trade_date')
                    
                    # 【优化】使用字典收集数据，避免DataFrame碎片化
                    volume_data[stock_code] = df_indexed['volume']
                    
                    # 使用流通股数据
                    shares_value = self.circulating_shares[stock_code]
                    # 创建流通股面板（所有日期使用相同的流通股数据）
                    shares_series = pd.Series([shares_value] * len(df_indexed), 
                                            index=df_indexed.index, 
                                            name=stock_code)
                    shares_data[stock_code] = shares_series
        
        # 【优化】一次性创建所有面板，避免DataFrame碎片化
        print("   创建数据面板...")
        self.volume_panel = pd.DataFrame(volume_data)
        self.shares_panel = pd.DataFrame(shares_data)
        
        # 【新增】处理停牌数据，确保数据完整性
        self._handle_suspension_data()
        
        print(f"   成功处理 {len(self.volume_panel.columns)} 只股票的数据")

    def _handle_suspension_data(self):
        """
        处理停牌数据，确保数据完整性
        参考 SectorIndexCalculator 的处理逻辑
        """
        print("   处理停牌数据...")
        
        # 1. 对流通股数据进行向前填充
        print("   - 填充流通股数据 (ffill)...")
        if not self.shares_panel.empty:
            self.shares_panel = self.shares_panel.fillna(method='ffill')
            self.shares_panel = self.shares_panel.fillna(method='bfill')  # 处理开头就停牌的情况
        
        # 2. 对成交量数据填充为 0（停牌期间成交量为0）
        print("   - 填充成交量数据为 0...")
        if not self.volume_panel.empty:
            self.volume_panel = self.volume_panel.fillna(0)
        
        print("   停牌数据处理完成")

    def _get_circulating_shares(self):
        """
        从数据库获取流通A股数据
        """
        print("   从数据库获取流通A股数据...")
        
        try:
            # 连接数据库
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases', 'quant_system.db')
            conn = sqlite3.connect(db_path)
            
            # 【安全修复】使用参数化查询，避免SQL注入风险
            placeholders = ','.join(['?' for _ in self.stock_list])
            query = f"""
            SELECT stock_code, 流通A股 
            FROM stock_basic_pro 
            WHERE stock_code IN ({placeholders})
            """
            
            result_df = pd.read_sql_query(query, conn, params=self.stock_list)
            conn.close()
            
            # 转换为字典
            self.circulating_shares = dict(zip(result_df['stock_code'], result_df['流通A股']))
            
            print(f"   成功获取 {len(self.circulating_shares)} 只股票的流通股数据")
            
        except Exception as e:
            print(f"   获取流通股数据失败: {e}")
            # 使用默认值
            self.circulating_shares = {code: 100000000 for code in self.stock_list}  # 1亿股

    def _calculate_metrics(self):
        """
        计算活跃度指标
        """
        print("3. 计算活跃度指标...")
        
        # 计算换手率
        self._calculate_turnover_rate()
        
        # 计算成交量爆发天数
        self._calculate_volume_bursts()

    def _calculate_turnover_rate(self, avg_window=30):
        """
        【指标1】计算每日换手率，并计算其移动平均值。
        """
        print(f"   正在计算 {avg_window} 日平均换手率...")
        # 向量化计算每日换手率
        daily_turnover = self.volume_panel / self.shares_panel
        # 计算移动平均，得到更平滑、更可靠的指标
        self.metrics = {}
        self.metrics['avg_turnover'] = daily_turnover.rolling(window=avg_window).mean()

    def _calculate_volume_bursts(self, lookback_period=126, ma_window=30, multiplier=1.5):
        """
        【指标2】计算最近N天内，成交量爆发的天数。
        """
        print(f"   正在计算过去 {lookback_period} 日内的成交量爆发天数...")
        # 1. 计算30日移动平均成交量
        avg_vol_30d = self.volume_panel.rolling(window=ma_window).mean()
        
        # 2. 判断每日成交量是否超过 30日均量 * 1.5 (得到布尔值面板)
        is_burst = self.volume_panel > (avg_vol_30d * multiplier)
        
        # 3. 在过去6个月(126天)的滚动窗口内，对布尔值求和，得到爆发天数
        #    .sum() 在布尔值上操作时，会将 True 计为 1, False 计为 0
        self.metrics['burst_days_count'] = is_burst.rolling(window=lookback_period).sum()

    def _load_stock_attributes(self):
        """
        从数据库加载股票属性信息（国企信息、收盘价等）
        """
        try:
            # 连接数据库
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases', 'quant_system.db')
            conn = sqlite3.connect(db_path)
            
            # 【安全修复】使用参数化查询，避免SQL注入风险
            placeholders = ','.join(['?' for _ in self.stock_list])
            query = f"""
            SELECT stock_code, 国企, 收盘价 
            FROM stock_basic_pro 
            WHERE stock_code IN ({placeholders})
            """
            
            result_df = pd.read_sql_query(query, conn, params=self.stock_list)
            conn.close()
            
            if not result_df.empty:
                # 设置股票代码为索引
                result_df.set_index('stock_code', inplace=True)
                self.stock_attributes = result_df
                print(f"   成功加载 {len(self.stock_attributes)} 只股票的属性信息")
            else:
                self.stock_attributes = pd.DataFrame()
                print("   未找到股票属性信息")
                
        except Exception as e:
            print(f"   加载股票属性信息失败: {e}")
            self.stock_attributes = pd.DataFrame()

    def _screen_stocks(self):
        """
        执行股票筛选，并根据加分项调整得分。
        """
        print("4. 执行股票筛选...")
        
        screen_date = pd.to_datetime(self.trade_date)
        
        try:
            turnover_on_date = self.metrics['avg_turnover'].loc[screen_date]
            bursts_on_date = self.metrics['burst_days_count'].loc[screen_date]
        except KeyError:
            print(f"   错误：筛选日期 '{self.trade_date}' 不在数据范围内")
            self.selected_stocks = self.stock_list[:3] if len(self.stock_list) >= 3 else self.stock_list
            return

        results_df = pd.DataFrame({
            'avg_turnover': turnover_on_date,
            'burst_days_count': bursts_on_date
        }).dropna()
        
        if results_df.empty:
            print("   没有有效的指标数据，使用所有成分股")
            self.selected_stocks = self.stock_list
            return
        
        # ------------------- 核心修改部分开始 -------------------

        # 1. 计算基础得分（和原来一样）
        #    我们将列名改为 'base_score' 以示区分
        print("   正在计算基础得分...")
        results_df['base_score'] = (results_df['avg_turnover'] * 0.6 + 
                                    results_df['burst_days_count'] / 100 * 0.4)
        
        # 2. 初始化 final_score，初始值等于 base_score
        results_df['final_score'] = results_df['base_score']

        # 3. 【加分项1】对国企股进行加分
        print("   正在为国企股进行加分...")
        self._load_stock_attributes()
        if hasattr(self, 'stock_attributes') and not self.stock_attributes.empty:
            # 从stock_basic_pro表获取国企信息
            is_soe = self.stock_attributes['国企'] == True
            soe_codes = self.stock_attributes[is_soe].index
            
            # 找出结果中属于国企的股票
            results_is_soe_mask = results_df.index.isin(soe_codes)
            
            # 对这些股票的 final_score 乘以 1.2
            results_df.loc[results_is_soe_mask, 'final_score'] *= 1.2
            print(f"   找到 {results_is_soe_mask.sum()} 只国企股并已加分")

        # 4. 【加分项2】对高价股进行加分
        print("   正在为高价股进行加分...")
        if hasattr(self, 'stock_attributes') and not self.stock_attributes.empty:
            # 从stock_basic_pro表获取收盘价信息
            close_prices = self.stock_attributes['收盘价'].dropna()
            if not close_prices.empty:
                price_threshold = close_prices.median()
                print(f"   价格中位数为: {price_threshold:.2f}")

                # 将收盘价合并到结果中，方便筛选
                close_prices_renamed = close_prices.copy()
                close_prices_renamed.name = 'close_price'
                results_df = results_df.join(close_prices_renamed)
                
                # 找出价格高于中位数的股票
                is_high_price_mask = results_df['close_price'] > price_threshold
                
                # 对这些股票的 final_score 乘以 1.2
                results_df.loc[is_high_price_mask, 'final_score'] *= 1.2
                print(f"   找到 {is_high_price_mask.sum()} 只高价股并已加分")

        # 5. 按最终得分(final_score)进行降序排序
        results_df = results_df.sort_values('final_score', ascending=False)
        
        # ------------------- 核心修改部分结束 -------------------
        
        # 后续挑选逻辑基于新的排序结果，保持不变
        total_stocks = len(results_df)
        select_count = max(3, int(total_stocks * 0.2))
        
        self.selected_stocks = results_df.head(select_count).index.tolist()
        
        print("\n   筛选及加分后的最终排名预览:")
        # 打印包含所有得分的表格，方便您检查和调试
        display_columns = ['base_score', 'final_score']
        if 'close_price' in results_df.columns:
            display_columns.append('close_price')
        print(results_df[display_columns].head(10))

        print(f"\n   从 {total_stocks} 只股票中挑选出 {len(self.selected_stocks)} 只活跃股票")
        print(f"   选中的股票: {self.selected_stocks}")

    def get_selected_stocks(self):
        """
        获取筛选出的股票列表
        """
        return self.selected_stocks






class SectorIndexCalculator:
    """
    一个用于计算自定义板块指数（流通市值加权）的工具类。
    可以计算完整的指数 OHLCV 数据。
    """
    def __init__(self, stock_list: list, start_date: str, end_date: str):
        """
        初始化指数计算器。

        Args:
            stock_list (list): 股票代码列表
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
        """
        print("1. 板块指数计算器初始化...")
        print(f"   股票数量: {len(stock_list)}")
        print(f"   日期范围: {start_date} 到 {end_date}")
        
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        
        # 初始化面板属性
        self.open_panel = None
        self.high_panel = None
        self.low_panel = None
        self.close_panel = None
        self.volume_panel = None
        self.shares_panel = None
        
        # 初始化计算结果属性
        self.market_cap_panel = None
        self.total_market_cap = None
        self.sector_index_df = None  # 最终结果将是一个DataFrame
        
        # 准备数据
        self._prepare_data()
        print("   数据准备完成。")

    def _prepare_data(self):
        """
        【修改】准备股票价格(OHLCV)和流通股数据。
        优化为一次性获取所有股票数据，效率更高。
        """
        print("2. 准备股票价格(OHLCV)和流通股数据...")
        
        # 获取股票行情数据
        from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest
        
        # 【优化】一次性获取所有股票数据，而不是在循环中逐个获取
        all_stock_data = get_multiple_stocks_daily_data_for_backtest(
            self.stock_list,
            self.end_date
        )
        
        # 获取流通股数据
        self._get_circulating_shares()
        
        # 【修改】初始化所有需要的面板 - 使用字典收集数据，最后一次性合并
        self.open_data = {}
        self.high_data = {}
        self.low_data = {}
        self.close_data = {}
        self.volume_data = {}
        self.shares_data = {}
        
        # 使用 all_stock_data 填充面板
        for stock_code, df in all_stock_data.items():
            if stock_code in self.circulating_shares and not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df_filtered = df[(df['trade_date'] >= self.start_date) & 
                               (df['trade_date'] <= self.end_date)]
                
                if not df_filtered.empty:
                    df_indexed = df_filtered.set_index('trade_date')
                    
                    # 【优化】使用字典收集数据，避免DataFrame碎片化
                    self.open_data[stock_code] = df_indexed['open']
                    self.high_data[stock_code] = df_indexed['high']
                    self.low_data[stock_code] = df_indexed['low']
                    self.close_data[stock_code] = df_indexed['close']
                    self.volume_data[stock_code] = df_indexed['volume']
                    
                    shares_value = self.circulating_shares[stock_code]
                    shares_series = pd.Series([shares_value] * len(df_indexed), 
                                            index=df_indexed.index, 
                                            name=stock_code)
                    self.shares_data[stock_code] = shares_series
        
        # 【优化】一次性创建所有面板，避免DataFrame碎片化
        print("   创建数据面板...")
        self.open_panel = pd.DataFrame(self.open_data)
        self.high_panel = pd.DataFrame(self.high_data)
        self.low_panel = pd.DataFrame(self.low_data)
        self.close_panel = pd.DataFrame(self.close_data)
        self.volume_panel = pd.DataFrame(self.volume_data)
        self.shares_panel = pd.DataFrame(self.shares_data)
        
        # 清理临时数据字典
        del self.open_data, self.high_data, self.low_data, self.close_data, self.volume_data, self.shares_data
        
        # 【关键修正】处理停牌数据
        print("   处理停牌数据...")

        # 1. 对价格和股本数据进行向前填充
        print("   - 填充价格和股本数据 (ffill)...")
        price_share_cols = ['open_panel', 'high_panel', 'low_panel', 'close_panel', 'shares_panel']
        for col_name in price_share_cols:
            panel = getattr(self, col_name)
            if panel is not None and not panel.empty:
                panel_filled = panel.fillna(method='ffill')
                panel_filled = panel_filled.fillna(method='bfill')  # 处理开头就停牌的情况
                setattr(self, col_name, panel_filled)

        # 2. 【重要微调】对成交量数据填充为 0
        print("   - 填充成交量数据为 0...")
        if self.volume_panel is not None and not self.volume_panel.empty:
            self.volume_panel = self.volume_panel.fillna(0)
        
        print(f"   成功处理 {len(self.close_panel.columns)} 只股票的OHLCV数据")

    def _get_circulating_shares(self):
        """
        【安全版】从数据库获取流通A股数据
        """
        print("   从数据库获取流通A股数据...")
        
        try:
            # 连接数据库
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases', 'quant_system.db')
            conn = sqlite3.connect(db_path)
            
            # 【安全修正】使用参数化查询防止SQL注入
            placeholders = ','.join('?' for _ in self.stock_list)
            query = f"SELECT stock_code, 流通A股 FROM stock_basic_pro WHERE stock_code IN ({placeholders})"
            
            result_df = pd.read_sql_query(query, conn, params=self.stock_list)
            conn.close()
            
            # 转换为字典
            self.circulating_shares = dict(zip(result_df['stock_code'], result_df['流通A股']))
            
            print(f"   成功获取 {len(self.circulating_shares)} 只股票的流通股数据")
            
        except Exception as e:
            print(f"   获取流通股数据失败: {e}")
            # 使用默认值
            self.circulating_shares = {code: 100000000 for code in self.stock_list}  # 1亿股


    def calculate_index(self, base_date: str, base_value: int = 1000) -> pd.DataFrame:
        """
        【全新】计算完整的板块指数 (OHLCV)。

        指数计算标准方法:
        - Index_Open/High/Low/Close: 使用前一交易日(T-1)的市值作为权重，对当日(T)的
          个股Open/High/Low/Close进行加权平均。
        - Index_Volume: 板块内所有成分股的成交量总和。
        - Index_Points: 将计算出的 Index_Close 序列根据基期和基点进行标准化。

        Args:
            base_date (str): 基准日期, 格式 'YYYY-MM-DD'。
            base_value (int, optional): 基点, 默认为 1000.

        Returns:
            pd.DataFrame: 包含 'open', 'high', 'low', 'close', 'volume' 列的指数DataFrame。
        """
        print("3. 正在计算完整的板块指数(OHLCV)...")
        
        # 检查面板数据是否已准备
        if self.close_panel is None or self.shares_panel is None:
            raise ValueError("面板数据未准备，请先调用 _prepare_data() 方法")
        
        # 1. 计算每日收盘市值和总市值 (用于计算权重)
        self.market_cap_panel = self.close_panel * self.shares_panel
        self.total_market_cap = self.market_cap_panel.sum(axis=1)
        
        # 2. 计算权重：使用前一天的市值占比作为当天的权重
        #    .shift(1) 表示使用上一行的数据
        weights = self.market_cap_panel.shift(1).div(self.total_market_cap.shift(1), axis=0)

        # 3. 计算指数的OHLC (市值加权平均)
        #    (self.open_panel * weights) 对每个元素进行乘法
        #    .sum(axis=1) 将每日所有加权后的价格相加，得到指数的模拟价格
        index_open_price = (self.open_panel * weights).sum(axis=1)
        index_high_price = (self.high_panel * weights).sum(axis=1)
        index_low_price = (self.low_panel * weights).sum(axis=1)
        index_close_price = (self.close_panel * weights).sum(axis=1)
        
        # 4. 计算指数的成交量 (直接求和)
        if self.volume_panel is None:
            raise ValueError("成交量面板数据未准备")
        index_volume = self.volume_panel.sum(axis=1)

        # 5. 将指数的OHLCV合并到DataFrame
        index_df = pd.DataFrame({
            'open_price': index_open_price,
            'high_price': index_high_price,
            'low_price': index_low_price,
            'close_price': index_close_price,
            'volume': index_volume
        }).dropna()  # 删除权重计算可能产生的NaN行 (如第一行)
        
        # 6. 计算最终的指数点位 (标准化)
        try:
            # 将 base_date 转换为 datetime 类型以匹配索引
            base_date_dt = pd.to_datetime(base_date)
            base_price = index_df.loc[base_date_dt, 'close_price']
        except KeyError:
            raise ValueError(f"错误：基准日期 '{base_date}' 不在数据范围内。")
        
        if base_price == 0 or pd.isna(base_price):
            print(f"   警告：基准日期 '{base_date}' 的指数价格为 {base_price}")
            print(f"   可用的日期范围: {index_df.index.min()} 到 {index_df.index.max()}")
            # 尝试使用第一个有效日期作为基准
            valid_dates = index_df[index_df['close_price'] > 0].index
            if len(valid_dates) > 0:
                base_date = str(valid_dates[0])
                base_price = index_df.loc[valid_dates[0], 'close_price']
                print(f"   使用第一个有效日期 '{base_date}' 作为基准，价格为 {base_price}")
            else:
                raise ValueError("错误：没有找到有效的基准价格。")
            
        # 根据基准价格，将模拟价格序列转换为指数点位序列
        index_df['open'] = base_value * (index_df['open_price'] / base_price)
        index_df['high'] = base_value * (index_df['high_price'] / base_price)
        index_df['low'] = base_value * (index_df['low_price'] / base_price)
        index_df['close'] = base_value * (index_df['close_price'] / base_price)
        
        # 只保留最终需要的列并保留2位小数
        final_df = pd.DataFrame(index_df[['open', 'high', 'low', 'close', 'volume']].copy())
        
        # 对价格列保留2位小数，成交量保持整数
        final_df['open'] = final_df['open'].round(2)
        final_df['high'] = final_df['high'].round(2)
        final_df['low'] = final_df['low'].round(2)
        final_df['close'] = final_df['close'].round(2)
        final_df['volume'] = final_df['volume'].round(0).astype(int)
        
        self.sector_index_df = final_df
        
        print("4. 板块指数计算完成！")
        return final_df

    def calculate_incremental(self, last_day_info: Tuple[str, float]) -> pd.DataFrame:
        """
        【修正版】基于前一日的指数信息和成分股OHLC数据，进行增量计算。
        
        核心修正：
        - 分别基于成分股的开盘价、最高价、最低价、收盘价计算指数的OHLC
        - 使用前一天的市值作为权重，对当天各个价格进行加权平均
        - 然后基于昨日指数收盘价进行标准化，得到有意义的OHLC差异

        Args:
            last_day_info (Tuple[str, float]): (上一个交易日日期, 上一个交易日收盘价)
        
        Returns:
            pd.DataFrame: 包含当天开高收低成交量的新数据行
        """
        last_date_str, last_close_price = last_day_info
        
        print(f"   执行增量计算: 基准日期={last_date_str}, 基准点位={last_close_price:.2f}")
        
        # 确保市值已计算
        if self.market_cap_panel is None or self.total_market_cap is None:
            if self.close_panel is None or self.shares_panel is None:
                raise ValueError("错误：价格面板或股本面板数据未准备。")
            self.market_cap_panel = self.close_panel * self.shares_panel
            self.total_market_cap = self.market_cap_panel.sum(axis=1)

        # 获取今日日期和数据
        try:
            today_date = self.total_market_cap.index[-1]  # 最后一天（当前更新日）
            last_day_cap = self.total_market_cap.loc[last_date_str]
            today_cap = self.total_market_cap.loc[today_date]
        except KeyError:
            raise ValueError(f"错误：数据中不包含上一个交易日 '{last_date_str}' 的市值。")
        
        if last_day_cap == 0:
            raise ValueError(f"错误：上一个交易日 '{last_date_str}' 的总市值为0。")

        print(f"   昨日总市值: {last_day_cap:.2f}, 今日总市值: {today_cap:.2f}")

        # 检查必要的面板数据
        if (self.market_cap_panel is None or self.open_panel is None or 
            self.high_panel is None or self.low_panel is None or 
            self.close_panel is None or self.volume_panel is None):
            raise ValueError("错误：必要的面板数据未准备。")

        # 【关键修正】计算权重：使用昨日的市值占比作为今日的权重
        last_day_market_cap = self.market_cap_panel.loc[last_date_str]
        last_day_weights = last_day_market_cap / last_day_cap
        
        # 【核心改进】分别计算今日指数的OHLC价格（基于成分股OHLC的加权平均）
        today_open_weighted = (self.open_panel.loc[today_date] * last_day_weights).sum()
        today_high_weighted = (self.high_panel.loc[today_date] * last_day_weights).sum()
        today_low_weighted = (self.low_panel.loc[today_date] * last_day_weights).sum()
        today_close_weighted = (self.close_panel.loc[today_date] * last_day_weights).sum()
        
        # 计算昨日指数的收盘价格（用于标准化）
        last_day_close_weighted = (self.close_panel.loc[last_date_str] * last_day_weights).sum()
        
        if last_day_close_weighted <= 0:
            raise ValueError(f"错误：昨日加权收盘价为 {last_day_close_weighted}，无法进行标准化计算。")
        
        # 【核心修正】基于昨日指数收盘价，分别计算今日指数的OHLC点位
        today_open_index = last_close_price * (today_open_weighted / last_day_close_weighted)
        today_high_index = last_close_price * (today_high_weighted / last_day_close_weighted)
        today_low_index = last_close_price * (today_low_weighted / last_day_close_weighted)
        today_close_index = last_close_price * (today_close_weighted / last_day_close_weighted)
        
        # 计算今日成交量（直接求和）
        today_volume = self.volume_panel.loc[today_date].sum()

        # 【数据合理性检查】确保 low <= open,close <= high
        today_low_index = min(today_low_index, today_open_index, today_close_index)
        today_high_index = max(today_high_index, today_open_index, today_close_index)

        # 创建新的数据行
        new_row = pd.DataFrame([{
            'open': round(today_open_index, 2), 
            'high': round(today_high_index, 2),
            'low': round(today_low_index, 2),
            'close': round(today_close_index, 2),
            'volume': int(today_volume)
        }], index=[today_date])
        
        print(f"   增量计算结果: 开={today_open_index:.2f}, 高={today_high_index:.2f}, 低={today_low_index:.2f}, 收={today_close_index:.2f}")
        
        return new_row

    @classmethod
    def from_active_stocks(cls, screener: 'ActiveStockScreener', start_date: str, end_date: str):
        """
        从 ActiveStockScreener 筛选结果创建指数计算器
        
        Args:
            screener (ActiveStockScreener): 活跃股票筛选器实例
            start_date (str): 开始日期，格式 'YYYY-MM-DD'
            end_date (str): 结束日期，格式 'YYYY-MM-DD'
            
        Returns:
            SectorIndexCalculator: 指数计算器实例
        """
        selected_stocks = screener.get_selected_stocks()
        print(f"使用筛选出的 {len(selected_stocks)} 只股票计算板块指数")
        return cls(selected_stocks, start_date, end_date)





# # 测试代码
# if __name__ == "__main__":
#     print("="*60)
#     print("测试 ActiveStockScreener 和 SectorIndexCalculator 类")
#     print("="*60)
    
#     # 使用交通运输板块进行测试（因为证券板块不存在）
#     sector_code = '801170.SI'  # 交通运输板块
#     trade_date = '2024-01-15'
    
#     try:
#         # 1. 测试 ActiveStockScreener
#         print("\n>>> 测试 ActiveStockScreener 类:")
#         screener = ActiveStockScreener(sector_code, trade_date)
#         selected_stocks = screener.get_selected_stocks()
        
#         print(f"\n筛选结果:")
#         print(f"板块代码: {sector_code}")
#         print(f"交易日期: {trade_date}")
#         print(f"筛选出的股票数量: {len(selected_stocks)}")
#         print(f"筛选出的股票: {selected_stocks}")
        
#         # 2. 测试 SectorIndexCalculator
#         if len(selected_stocks) > 0:
#             print(f"\n>>> 测试 SectorIndexCalculator 类:")
            
#             # 使用筛选出的股票计算指数
#             start_date = '2024-01-01'
#             end_date = '2024-01-15'
            
#             # 方法1：直接使用股票列表
#             print(f"\n方法1：直接使用股票列表")
#             calculator = SectorIndexCalculator(selected_stocks[:5], start_date, end_date)  # 只使用前5只股票
            
#             # 计算指数
#             base_date = '2024-01-02'  # 使用第二个交易日作为基准
#             index_series = calculator.calculate_index(base_date, base_value=1000)
            
#             print(f"\n指数计算结果:")
#             print(f"基准日期: {base_date}")
#             print(f"基准值: 1000")
#             print(f"指数数据点数: {len(index_series)}")
#             print(f"最新指数值: {index_series.iloc[-1]:.2f}")
#             print(f"指数范围: {index_series.min():.2f} - {index_series.max():.2f}")
            
#             # 方法2：使用工厂方法
#             print(f"\n方法2：使用工厂方法")
#             calculator2 = SectorIndexCalculator.from_active_stocks(screener, start_date, end_date)
#             index_series2 = calculator2.calculate_index(base_date, base_value=1000)
            
#             print(f"\n工厂方法指数计算结果:")
#             print(f"指数数据点数: {len(index_series2)}")
#             print(f"最新指数值: {index_series2.iloc[-1]:.2f}")
            
#         else:
#             print("没有筛选出股票，跳过指数计算测试")
        
#     except Exception as e:
#         print(f"测试失败: {e}")
#         import traceback
#         traceback.print_exc()
    
#     print("\n" + "="*60)