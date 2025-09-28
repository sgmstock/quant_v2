"""
板块信号分析器类
改造自原有的get_xuangu_date函数，支持不同数据源和多种比例阈值分析
"""

import pandas as pd
import numpy as np
from datetime import datetime
from strategies.sector_momentum_strategy import MyBankuaiStrategy
from data_management.data_processor import get_multiple_stocks_daily_data_for_backtest, update_and_load_multiple_stocks_daily_data




class SectorSignalAnalyzer:
    """
    板块信号分析器
    
    功能：
    1. 支持回测数据和实盘数据两种数据源
    2. 计算MACD<0股票占比
    3. 提供多种阈值分析方法
    """
    
    def __init__(self, stock_list, data_source='backtest', end_date=None):
        """
        初始化板块信号分析器（优化版）
        
        参数:
        stock_list (list): 股票代码列表
        data_source (str): 数据源类型，'backtest'(回测数据) 或 'realtime'(实盘数据)
        end_date (str): 数据截止日期，仅在回测模式下使用，格式为'YYYY-MM-DD'
        """
        self.stock_list = stock_list
        self.data_source = data_source
        self.end_date = end_date
        
        # --- 状态变量 ---
        self.stock_data_dict = None
        self.strategy = None  # 策略对象只初始化一次
        self._strategy_indicators_calculated = False  # 标记基础指标是否已计算
        
        # --- 结果缓存：为每个计算结果创建一个独立的缓存变量 ---
        self.results_genzj = None
        self.results_genzj7x26 = None
        self.results_genzjding = None
        self.results_genzj7d26 = None
        
        # 保持向后兼容性
        self.proportion_results = None
        
        print(f"初始化板块信号分析器:")
        print(f"  股票列表: {len(stock_list)} 只股票")
        print(f"  数据源: {'回测数据' if data_source == 'backtest' else '实盘数据'}")
        if data_source == 'backtest' and end_date:
            print(f"  截止日期: {end_date}")
    
    def load_data(self):
        """
        根据数据源类型加载股票数据
        """
        print("正在加载股票数据...")
        
        if self.data_source == 'backtest':
            if not self.end_date:
                raise ValueError("回测模式下必须提供end_date参数")
            self.stock_data_dict = get_multiple_stocks_daily_data_for_backtest(self.stock_list, self.end_date)
        elif self.data_source == 'realtime':
            self.stock_data_dict = update_and_load_multiple_stocks_daily_data(self.stock_list)
        else:
            raise ValueError("data_source必须是'backtest'或'realtime'")
        
        if self.stock_data_dict:
            print(f"成功获取到 {len(self.stock_data_dict)} 只股票的数据")
        return self.stock_data_dict
    
    def _prepare_panel_data(self):
        """
        将股票数据转换为面板数据格式
        """
        if not self.stock_data_dict:
            self.load_data()
        
        print("正在准备面板数据...")
        
        # 提取所有股票的数据并重新组织
        all_dates = None
        ohlcv_data = {'open': {}, 'high': {}, 'low': {}, 'close': {}, 'volume': {}}
        
        if self.stock_data_dict:
            for stock_code, df in self.stock_data_dict.items():
                if df is not None and not df.empty:
                    # 确保数据有正确的日期索引
                    if 'trade_date' in df.columns:
                        df = df.set_index('trade_date')
                    
                    # 确保索引是datetime类型
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index)
                    
                    if all_dates is None:
                        all_dates = df.index
                    else:
                        all_dates = all_dates.intersection(df.index)
                    
                    ohlcv_data['open'][stock_code] = df['open']
                    ohlcv_data['high'][stock_code] = df['high']
                    ohlcv_data['low'][stock_code] = df['low']
                    ohlcv_data['close'][stock_code] = df['close']
                    ohlcv_data['volume'][stock_code] = df['volume']
        
        # 创建面板数据
        daily_panel = {}
        if all_dates is not None:
            for key in ['open', 'high', 'low', 'close', 'volume']:
                daily_panel[key] = pd.DataFrame(ohlcv_data[key]).loc[all_dates]
        
        price_data_dict = {'daily': daily_panel}
        return price_data_dict
    
    def _ensure_strategy_ready(self):
        """
        确保策略对象和基础指标已准备就绪。
        这是所有计算方法的公共前置步骤，只执行一次。
        """
        if self.strategy and self._strategy_indicators_calculated:
            return  # 如果已经准备好，直接返回

        print("首次计算，正在准备基础数据和指标...")
        
        # 1. 准备面板数据
        price_data_dict = self._prepare_panel_data()
        
        # 2. 创建策略对象
        if self.strategy is None:
            self.strategy = MyBankuaiStrategy(price_data_dict, self.stock_list, end_date=self.end_date)
        
        # 3. 计算所有股票的基础指标（如MACD），这一步是公共的，无法避免
        if not self._strategy_indicators_calculated:
            self.strategy.calculate_indicators()
            self._strategy_indicators_calculated = True
            print("基础指标计算完成。")
    
    def calculate_proportion(self):
        """
        计算MACD<0股票占比（保持向后兼容性）
        
        返回:
        pd.Series: 每日MACD<0股票占比的时间序列
        """
        # 使用新的独立计算方法
        results = self.calculate_genzj_proportion()
        
        print("MACD<0股票占比计算完成")
        print(f"数据范围: {results.index[0]} 至 {results.index[-1]}")
        print(f"最新占比: {results.iloc[-1]:.2%}")
        
        return results
    
    # ==================================================================
    # === 独立的计算方法 (每个方法负责一种计算并缓存结果) ===
    # ==================================================================
    
    def calculate_genzj_proportion(self):
        """
        计算 MACD<0 股票占比 (chixu_genzj)
        
        返回:
        pd.Series: 每日MACD<0股票占比的时间序列
        """
        if self.results_genzj is not None:
            return self.results_genzj  # 如果已缓存，直接返回

        self._ensure_strategy_ready()  # 确保基础数据已准备好
        
        print("正在计算 MACD<0 股票占比 (chixu_genzj)...")
        if hasattr(self.strategy, 'chixu_genzj'):
            self.results_genzj = self.strategy.chixu_genzj()
        else:
            raise AttributeError("策略对象缺少 chixu_genzj 方法")
        print("计算完成。")
        
        # 保持向后兼容性
        self.proportion_results = self.results_genzj
        
        return self.results_genzj

    def calculate_genzj7x26_proportion(self):
        """
        计算 MA7<MA26 股票占比 (chixu_genzj7x26)
        
        返回:
        pd.Series: 每日MA7<MA26股票占比的时间序列
        """
        if self.results_genzj7x26 is not None:
            return self.results_genzj7x26

        self._ensure_strategy_ready()
        
        print("正在计算 MA7<MA26 股票占比 (chixu_genzj7x26)...")
        if hasattr(self.strategy, 'chixu_genzj7x26'):
            self.results_genzj7x26 = self.strategy.chixu_genzj7x26()
        else:
            raise AttributeError("策略对象缺少 chixu_genzj7x26 方法")
        print("计算完成。")
        return self.results_genzj7x26
        
    def calculate_genzjding_proportion(self):
        """
        计算持续跟踪技术状态股票占比 (chixu_genzjding)
        
        返回:
        pd.Series: 每日持续跟踪技术状态股票占比的时间序列
        """
        if self.results_genzjding is not None:
            return self.results_genzjding

        self._ensure_strategy_ready()
        
        print("正在计算持续跟踪技术状态股票占比 (chixu_genzjding)...")
        if hasattr(self.strategy, 'chixu_genzjding'):
            self.results_genzjding = self.strategy.chixu_genzjding()
        else:
            raise AttributeError("策略对象缺少 chixu_genzjding 方法")
        print("计算完成。")
        return self.results_genzjding

    def calculate_genzj7d26_proportion(self):
        """
        计算 MA7>MA26且MACD>0 或 连续9天MACD>0 股票占比 (chixu_genzj7d26)
        
        返回:
        pd.Series: 每日满足条件的股票占比的时间序列
        """
        if self.results_genzj7d26 is not None:
            return self.results_genzj7d26

        self._ensure_strategy_ready()
        
        print("正在计算 MA7>MA26且MACD>0 或 连续9天MACD>0 股票占比 (chixu_genzj7d26)...")
        if hasattr(self.strategy, 'chixu_genzj7d26'):
            self.results_genzj7d26 = self.strategy.chixu_genzj7d26()
        else:
            raise AttributeError("策略对象缺少 chixu_genzj7d26 方法")
        print("计算完成。")
        return self.results_genzj7d26
    
    def get_xuangu_date(self, threshold=0.32):
        """
        获取选股日期，基于MACD<0股票占比信号
        
        参数:
        threshold (float): 占比阈值，默认0.32（32%）
        
        返回:
        str or None: 选股日期字符串，如果信号未成立则返回None
        """
        print(f"正在检查选股信号（阈值: {threshold:.0%}）...")
        
        # 确保已计算占比
        results = self.calculate_genzj_proportion()
        
        print("MACD<0股票占比 (最近15天):")
        if results is not None:
            print(results.tail(15).apply(lambda x: f"{x:.2%}"))
        
        # 检查是否有足够的数据来进行比较
        if results is None or len(results) < 3:
            print("数据不足，无法进行对比分析。")
            return None
        
        # 获取最新数据
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 条件判断
        condition_met = last_value > threshold and last_value > prev_value and prev_value <= threshold
        #condition_met2 = last_value > threshold and (last_value == prev_value and last_value > prev_value_3)
        
        # 打印结果
        if condition_met:# or condition_met2:
            print(f"✓ 信号成立日期: {last_date.strftime('%Y-%m-%d')}")
            print(f"  触发条件: MACD<0股票占比为 {last_value:.2%}，大于{threshold:.0%}且高于前一日的 {prev_value:.2%}")
            return last_date.strftime('%Y-%m-%d')
        else:
            print(f"✗ 信号未成立日期: {last_date.strftime('%Y-%m-%d')}")
            print(f"  当前占比为 {last_value:.2%}，未满足大于{threshold:.0%}且持续上升的条件。")
            return None
    
    def get_cx_date(self, threshold=0.1):
        print(f"正在检查选股信号（阈值: {threshold:.0%}）...")
        
        # 确保已计算占比
        results = self.calculate_genzj_proportion()
        
        print("MACD<0股票占比 (最近15天):")
        if results is not None:
            print(results.tail(15).apply(lambda x: f"{x:.2%}"))
        
        # 检查是否有足够的数据来进行比较
        if results is None or len(results) < 3:
            print("数据不足，无法进行对比分析。")
            return None
        
        # 获取最新数据
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 条件判断
        condition_met = last_value > threshold and last_value > prev_value and prev_value < threshold
        #condition_met2 = last_value > threshold and (last_value == prev_value and last_value > prev_value_3)
        
        # 打印结果
        if condition_met:# or condition_met2:
            print(f"✓ 信号成立日期: {last_date.strftime('%Y-%m-%d')}")
            print(f"  触发条件: MACD<0股票占比为 {last_value:.2%}，大于{threshold:.0%}且高于前一日的 {prev_value:.2%}")
            return last_date.strftime('%Y-%m-%d')
        else:
            print(f"✗ 信号未成立日期: {last_date.strftime('%Y-%m-%d')}")
            print(f"  当前占比为 {last_value:.2%}，未满足大于{threshold:.0%}且持续上升的条件。")
            return None



    
    def get_bankuai_jjdb(self):
        """
        获取板块是否接近底部信号：比例值>0.4且<0.7
        
        返回:
        dict: 包含信号状态、当前值、日期等信息的字典
        """
        print("正在检查板块是否接近底部信号（0.4 < 占比 < 0.7）...")
        
        # 确保已计算占比
        results = self.calculate_genzj_proportion()
        
        if results is None or len(results) == 0:
            return {"signal": False, "reason": "无数据"}
        
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 判断是否在合理区间
        signal_met = 0.32 < last_value < 0.7 
        
        result = {
            "signal": signal_met,
            "date": last_date.strftime('%Y-%m-%d'),
            "proportion": last_value,
            "threshold_range": "0.4 < 占比 < 0.7...."
        }
        
        if signal_met:
            print(f"✓ 板块是否接近底部信号成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块处于接近底部")
        else:
            print(f"✗ 板块是否接近底部信号未成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            if last_value <= 0.32:
                print(f"  状态: 板块不够接近底部（<32%）")
            else:
                print(f"  状态: 板块观察")
        
        return signal_met
    
    def get_bankuai_db(self):
        """
        获取板块明确底部信号：比例值>0.7
        
        返回:
        dict: 包含信号状态、当前值、日期等信息的字典
        """
        print("正在检查板块明确底部信号（占比 > 0.7）...")
        
        # 确保已计算占比
        results = self.calculate_genzj_proportion()
        
        if results is None or len(results) == 0:
            return {"signal": False, "reason": "无数据"}
        
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 判断是否明确底部
        signal_met = last_value >= 0.7 and last_value >= prev_value and last_value > prev_value_3
        
        result = {
            "signal": signal_met,
            "date": last_date.strftime('%Y-%m-%d'),
            "proportion": last_value,
            "threshold": "> 0.7"
        }
        
        if signal_met:
            print(f"⚠️  板块明确底部信号成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块明确底部，需要积极操作")
        else:
            print(f"✓ 板块明确底部信号未成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块调整中或接近底部（<70%）")
        
        return signal_met
    
    def get_bankuai_jjding(self):
        """
        获取板块接近顶部信号：比例值>0.7
        
        返回:
        dict: 包含信号状态、当前值、日期等信息的字典
        """
        print("正在检查板块接近顶部信号（占比 > 0.7）...")
        
        # 确保已计算占比
        results = self.calculate_genzjding_proportion()
        
        if results is None or len(results) == 0:
            return {"signal": False, "reason": "无数据"}
        
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 判断是否接近顶部
        signal_met = last_value > 0.7 
        
        result = {
            "signal": signal_met,
            "date": last_date.strftime('%Y-%m-%d'),
            "proportion": last_value,
            "threshold": "> 0.7"
        }
        
        if signal_met:
            print(f"⚠️  板块接近顶部信号成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块接近顶部，需要准备卖出操作")
        else:
            print(f"✓ 板块明确顶部信号未成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块未接近顶部（≤70%）")
        
        return signal_met
    def get_bankuai_ding(self):
        """
        获取板块明确顶部信号：比例值>0.7
        
        返回:
        dict: 包含信号状态、当前值、日期等信息的字典
        """
        print("正在检查板块明确顶部信号（占比 > 0.7）...")
        
        # 确保已计算占比
        results = self.calculate_genzjding_proportion()
        
        if results is None or len(results) == 0:
            return {"signal": False, "reason": "无数据"}
        
        last_date = results.index[-1]
        last_value = results.iloc[-1]
        prev_value = results.iloc[-2]
        prev_value_3 = results.iloc[-3] if len(results) >= 3 else 0
        
        # 判断是否明确顶部
        signal_met = last_value > 0.7 and last_value < prev_value
        
        result = {
            "signal": signal_met,
            "date": last_date.strftime('%Y-%m-%d'),
            "proportion": last_value,
            "threshold": "> 0.7"
        }
        
        if signal_met:
            print(f"⚠️  板块明确顶部信号成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块明确顶部，需要卖出操作")
        else:
            print(f"✓ 板块明确顶部信号未成立")
            print(f"  日期: {result['date']}")
            print(f"  当前占比: {last_value:.2%}")
            print(f"  状态: 板块未明确顶部（≤70%），需要卖出操作 ")
        
        return signal_met

    
    def print_recent_data(self, days=15):
        """
        打印最近几天的数据
        
        参数:
        days (int): 要显示的天数
        """
        results = self.calculate_genzj_proportion()
        
        print(f"\n最近{days}天MACD<0股票占比:")
        print("-" * 40)
        if results is not None:
            recent_data = results.tail(days)
            for date, proportion in recent_data.items():
                print(f"{date.strftime('%Y-%m-%d')}: {proportion:.2%}")
        else:
            print("无数据可显示")
    
    def print_recent_allstocks_data(self, days=15):
        """
        打印所有个股的近期MACD<0状态（True/False）
        
        参数:
        days (int): 要显示的天数，默认15天
        """
        print(f"\n近{days}天各股票MACD<0状态:")
        print("="*80)
        
        # 确保已计算指标
        self.calculate_genzj_proportion()
        
        # 检查strategy是否存在且有indicator_panels
        if self.strategy is None or not hasattr(self.strategy, 'indicator_panels'):
            print("错误：策略对象未正确初始化")
            return
            
        # 获取MACD指标面板
        if 'MACD' not in self.strategy.indicator_panels:
            print("错误：MACD指标未计算")
            return
        
        macd_panel = self.strategy.indicator_panels['MACD']
        
        # 创建MACD<0的布尔面板
        condition_panel = macd_panel < 0
        
        # 获取最近N天的数据
        recent_data = condition_panel.tail(days)
        
        if recent_data.empty:
            print("无数据可显示")
            return
        
        # 获取日期列表
        dates = recent_data.index.strftime('%Y-%m-%d').tolist()
        
        # 打印表头
        print(f"{'股票代码':<10}", end="")
        for date in dates:
            print(f"{date:<12}", end="")
        print()  # 换行
        
        print("-" * (10 + 12 * len(dates)))
        
        # 打印每只股票的数据
        for stock in self.stock_list:
            if stock in recent_data.columns:
                print(f"{stock:<10}", end="")
                for date in recent_data.index:
                    value = recent_data.loc[date, stock]
                    status = "True" if value else "False"
                    print(f"{status:<12}", end="")
                print()  # 换行
        
        # 计算并打印每日的True占比
        print("-" * (10 + 12 * len(dates)))
        print(f"{'True占比':<10}", end="")
        for date in recent_data.index:
            true_count = recent_data.loc[date].sum()
            total_count = len(self.stock_list)
            ratio = true_count / total_count if total_count > 0 else 0
            print(f"{ratio:.1f}{'':8}", end="")
        print()  # 换行
        
        print("="*80)
    
    # ==================================================================
    # === 便捷方法：支持一次性获取多种计算结果 ===
    # ==================================================================
    
    def get_all_proportions(self):
        """
        一次性获取所有类型的股票占比计算结果
        
        返回:
        dict: 包含所有计算结果的字典
        """
        print("正在计算所有类型的股票占比...")
        
        results = {
            'genzj': self.calculate_genzj_proportion(),
            'genzj7x26': self.calculate_genzj7x26_proportion(),
            'genzjding': self.calculate_genzjding_proportion(),
            'genzj7d26': self.calculate_genzj7d26_proportion()
        }
        
        print("所有计算完成。")
        return results
    
    # def get_comprehensive_analysis(self, thresholds=[0.3, 0.4, 0.5, 0.6, 0.7]):
    #     """
    #     获取综合分析结果，包含多种计算和多个阈值的信号状态
        
    #     参数:
    #     thresholds (list): 要分析的阈值列表
        
    #     返回:
    #     dict: 综合分析结果
    #     """
    #     print("正在进行综合分析...")
        
    #     # 获取所有计算结果
    #     all_results = self.get_all_proportions()
        
    #     if not all_results['genzj'] or len(all_results['genzj']) == 0:
    #         return {"error": "无数据"}
        
    #     last_date = all_results['genzj'].index[-1]
        
    #     # 分析各个阈值（基于MACD<0占比）
    #     threshold_analysis = {}
    #     for threshold in thresholds:
    #         threshold_analysis[f"{threshold:.0%}"] = {
    #             "met": all_results['genzj'].iloc[-1] > threshold,
    #             "xuangu_signal": self.get_xuangu_date(threshold) is not None
    #         }
        
    #     # 板块状态判断（基于MACD<0占比）
    #     last_value = all_results['genzj'].iloc[-1]
    #     if last_value > 0.7:
    #         status = "过热"
    #         risk_level = "高"
    #     elif last_value > 0.4:
    #         status = "积极"
    #         risk_level = "中"
    #     elif last_value > 0.2:
    #         status = "温和"
    #         risk_level = "低"
    #     else:
    #         status = "冷淡"
    #         risk_level = "很低"
        
    #     comprehensive_result = {
    #         "date": last_date.strftime('%Y-%m-%d'),
    #         "current_proportions": {
    #             "MACD<0": last_value,
    #             "MA7<MA26": all_results['genzj7x26'].iloc[-1] if len(all_results['genzj7x26']) > 0 else 0,
    #             "持续跟踪": all_results['genzjding'].iloc[-1] if len(all_results['genzjding']) > 0 else 0,
    #             "MA7>MA26且MACD>0": all_results['genzj7d26'].iloc[-1] if len(all_results['genzj7d26']) > 0 else 0
    #         },
    #         "status": status,
    #         "risk_level": risk_level,
    #         "threshold_analysis": threshold_analysis,
    #         "moderate_signal": self.get_bankuai_jjdb(),
    #         "hot_signal": self.get_bankuai_db(),
    #         "recent_trend": self._analyze_trend()
    #     }
        
    #     print(f"\n=== 综合分析结果 ===")
    #     print(f"日期: {comprehensive_result['date']}")
    #     print(f"当前MACD<0占比: {last_value:.2%}")
    #     print(f"板块状态: {status}")
    #     print(f"风险级别: {risk_level}")
        
    #     return comprehensive_result
    
    # def _analyze_trend(self, days=5):
    #     """
    #     分析最近几天的趋势
        
    #     参数:
    #     days (int): 分析的天数
        
    #     返回:
    #     dict: 趋势分析结果
    #     """
    #     results = self.calculate_genzj_proportion()
    #     if results is None or len(results) < days:
    #         return {"trend": "数据不足", "change": 0}
        
    #     recent_data = results.tail(days)
    #     first_value = recent_data.iloc[0]
    #     last_value = recent_data.iloc[-1]
    #     change = last_value - first_value
        
    #     if change > 0.05:
    #         trend = "快速上升"
    #     elif change > 0.02:
    #         trend = "上升"
    #     elif change > -0.02:
    #         trend = "平稳"
    #     elif change > -0.05:
    #         trend = "下降"
    #     else:
    #         trend = "快速下降"
        
    #     return {
    #         "trend": trend,
    #         "change": change,
    #         "days": days
    #     }
    
    def print_all_recent_data(self, days=15):
        """
        打印所有计算结果的最近几天数据
        
        参数:
        days (int): 要显示的天数
        """
        print(f"\n最近{days}天所有计算结果:")
        print("="*80)
        
        all_results = self.get_all_proportions()
        
        for name, results in all_results.items():
            if results is not None and len(results) > 0:
                print(f"\n{name} 最近{days}天数据:")
                print("-" * 40)
                recent_data = results.tail(days)
                for date, proportion in recent_data.items():
                    print(f"{date.strftime('%Y-%m-%d')}: {proportion:.2%}")
            else:
                print(f"\n{name}: 无数据")


# 使用示例
if __name__ == "__main__":
    # 示例股票列表
    test_stocks = ['000001', '000002', '000858', '600036', '600519']
    
    print("=== 回测数据模式示例 ===")
    analyzer_backtest = SectorSignalAnalyzer(
        stock_list=test_stocks,
        data_source='backtest',
        end_date='2025-09-11'
    )
    
    # 获取选股日期
    xuangu_date = analyzer_backtest.get_xuangu_date(threshold=0.4)
    print(f"选股日期: {xuangu_date}")
    
    # 检查板块接近底部信号
    moderate_signal = analyzer_backtest.get_bankuai_jjdb()
    print(f"板块接近底部信号: {moderate_signal}")
    
    # 检查板块明确底部信号
    hot_signal = analyzer_backtest.get_bankuai_db()
    print(f"板块明确底部信号: {hot_signal}")
    
    # 获取所有计算结果
    all_results = analyzer_backtest.get_all_proportions()
    print(f"所有计算结果: {len(all_results)} 种")
    
    # # 综合分析
    # comprehensive = analyzer_backtest.get_comprehensive_analysis()
    
    # 打印所有最近数据
    analyzer_backtest.print_all_recent_data(days=10)
    
    print("\n=== 实盘数据模式示例 ===")
    analyzer_realtime = SectorSignalAnalyzer(
        stock_list=test_stocks,
        data_source='realtime'
    )
    
    xuangu_date_realtime = analyzer_realtime.get_xuangu_date(threshold=0.4)
    print(f"实盘选股日期: {xuangu_date_realtime}")
