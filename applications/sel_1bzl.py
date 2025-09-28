#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多阶段事件识别与时序聚类分析 (quant_v2 版本)

功能：
1. 识别特定市场板块内的技术指标共振
2. 群体性价格突破行为的时间集中性分析
3. 筛选高动量领涨股
"""

import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
from tqdm import tqdm

# 导入v2项目的模块
from core.utils.stock_filter import get_bankuai_stocks, StockXihua
from data_management.data_processor import get_daily_data_for_backtest
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer

class SectorMomentumAnalyzer:
    """
    一个用于执行多阶段事件识别与时序聚类分析的类。

    该策略旨在识别特定市场板块内，由技术指标共振所引发的
    群体性价格突破行为的时间集中性，并筛选出其中的高动量领涨股。
    
    最终版特性 (全自动):
    - T0日的识别逻辑完全自动化，自动寻找近期市场宽度动量从峰值回落的转折点，
      无需人工指定基- 准日或回看窗口。
    """
    def __init__(self, stock_list: list, end_date: str or pd.Timestamp = None):
        """
        初始化分析器。

        参数:
            stock_list (list): 需要分析的股票代码列表。
            end_date (str or pd.Timestamp, optional): 分析的基准日。
                                                     如果为None，则自动使用当前日期，用于实盘自动化。
                                                     默认为 None。
        """
        self.stock_list = stock_list
        if end_date is None:
            self.end_date = pd.Timestamp.now().normalize()
            print(f"未指定end_date，自动使用当前日期: {self.end_date.strftime('%Y-%m-%d')}")
        else:
            self.end_date = pd.to_datetime(end_date)
        
        self.required_history_days = 90
        self.W = 10
        
        self.results = {"status": "Not Started"}
        self.close_prices_df = None
        self.macd_df = None
        self.ma20_df = None
        self.breakthrough_signals = None

    def _load_and_prepare_data(self):
        """私有方法：加载并准备所有股票的收盘价数据。"""
        print("\n--- [步骤 1/5] 批量加载并准备数据 ---")
        all_close_prices = {}
        # 确保数据加载到指定的end_date
        date_str_for_loading = self.end_date.strftime('%Y-%m-%d')
        for stock_code in tqdm(self.stock_list, desc="加载行情数据"):
            try:
                df_daily = get_daily_data_for_backtest(stock_code, date_str_for_loading)
                if df_daily.empty or len(df_daily) < self.required_history_days:
                    continue
                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
                df_daily = df_daily.set_index('trade_date')
                if 'close' in df_daily.columns:
                    all_close_prices[stock_code] = df_daily['close']
            except Exception as e:
                print(f"处理 {stock_code} 时出错: {e}")
        
        if not all_close_prices:
            print("错误：未能加载任何有效的股票数据。")
            return False

        self.close_prices_df = pd.concat(all_close_prices, axis=1)
        # 确保数据截取到end_date
        self.close_prices_df = self.close_prices_df[self.close_prices_df.index <= self.end_date]
        self.close_prices_df.dropna(axis=1, thresh=len(self.close_prices_df) - 10, inplace=True)
        self.close_prices_df.ffill(inplace=True)
        print(f"成功合并 {self.close_prices_df.shape[1]} 只股票的数据。")
        return True

    def _calculate_indicators(self):
        """私有方法：为所有股票计算所需的技术指标。"""
        print("\n--- [步骤 2/5] 批量计算技术指标 ---")
        stock_indicators = {}
        date_str_for_loading = self.end_date.strftime('%Y-%m-%d')
        for stock_code in tqdm(self.close_prices_df.columns, desc="计算技术指标"):
            try:
                df_daily = get_daily_data_for_backtest(stock_code, date_str_for_loading)
                if df_daily.empty: continue
                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
                df_daily = df_daily.set_index('trade_date')
                analyzer = TechnicalAnalyzer({'daily': df_daily})
                # 截取到end_date
                stock_indicators[stock_code] = analyzer.df_daily[analyzer.df_daily.index <= self.end_date]
            except Exception:
                continue
        
        if not stock_indicators: return False

        self.macd_df = pd.concat([df['MACD'].rename(code) for code, df in stock_indicators.items()], axis=1)
        self.ma20_df = pd.concat([df['MA_20'].rename(code) for code, df in stock_indicators.items()], axis=1)
        self.close_prices_df = self.close_prices_df[self.macd_df.columns]
        return True

    def _find_t0_date(self):
        """
        私有方法：自动识别事件触发日(T0)。
        逻辑：T0是最近一个满足以下条件的日期：
              1. 当日MACD>0的股票比例 >= 20%
              2. 次日的该比例出现下降 (即当日为局部峰值)
        """
        print("\n--- [步骤 3/5] 自动识别事件触发日(T0) ---")
        
        if self.macd_df is None or self.macd_df.empty:
             print("错误: MACD数据未准备好。")
             return False

        macd_positive_ratio = (self.macd_df > 0).mean(axis=1)
        
        # 条件1: 比例必须达到20%的阈值
        above_threshold = macd_positive_ratio >= 0.20
        
        # 条件2: 第二天的比例比当天低 (标志着当天是峰值)
        is_peak = macd_positive_ratio > macd_positive_ratio.shift(-1)
        
        # 组合两个条件，找到所有有效的峰值日期
        t0_candidates = macd_positive_ratio[above_threshold & is_peak]

        if t0_candidates.empty:
            print("❌ 在历史数据中未找到符合'宽度见顶回落'条件的事件触发日(T0)。")
            self.results.update({"status": "Failure: No T0 Peak Found"})
            return False
        
        # T0是所有候选者中最近的一个
        self.results['t0_date'] = t0_candidates.index[-1]
        self.results['t0_ratio'] = t0_candidates.iloc[-1]
        
        print(f"✅ 自动识别T0 (宽度峰值日): {self.results['t0_date'].strftime('%Y-%m-%d')}")
        print(f"   峰值日MACD>0比例: {self.results['t0_ratio']:.2%}")
        return True

    def _analyze_temporal_distribution(self):
        """私有方法：进行时间分布的中心趋势与聚类分析。"""
        print("\n--- [步骤 4/5] 分析突破信号的时间分布 ---")
        is_20_day_high = self.close_prices_df == self.close_prices_df.rolling(window=20, min_periods=20).max()
        is_ma20_rising = self.ma20_df > self.ma20_df.shift(1)
        self.breakthrough_signals = is_20_day_high & is_ma20_rising
        
        try:
            t0_loc = self.breakthrough_signals.index.get_loc(self.results['t0_date'])
            # 确保观测窗口不会超出数据范围的开头
            start_loc = max(0, t0_loc - self.W)
            observation_window_signals = self.breakthrough_signals.iloc[start_loc : t0_loc]
            
            if observation_window_signals.empty:
                print("❌ 观测窗口为空，可能是T0日过于靠前。")
                return False

            self.results['window_start_date'] = observation_window_signals.index[0]
            self.results['window_end_date'] = observation_window_signals.index[-1]
        except (KeyError, IndexError):
            print(f"❌ 无法定位T0日期或定义观测窗口。")
            return False
        
        print(f"观测窗口: {self.results['window_start_date'].strftime('%Y-%m-%d')} 至 {self.results['window_end_date'].strftime('%Y-%m-%d')}")

        breakthrough_events = observation_window_signals.stack()[lambda x: x]

        if breakthrough_events.empty:
            print("❌ 在观测窗口内未发现任何突破事件。")
            self.results.update({"status": "Failure: No Breakthroughs Found"})
            return False
            
        breakthrough_dates = breakthrough_events.index.get_level_values('trade_date').to_series()
        daily_counts = breakthrough_dates.value_counts().sort_index()

        self.results.update({
            'total_breakthroughs': len(breakthrough_dates),
            'daily_counts': daily_counts,
            'most_active_day': daily_counts.idxmax(),
            'max_daily_count': daily_counts.max(),
            'median_date': pd.to_datetime(breakthrough_dates.apply(lambda x: x.timestamp()).median(), unit='s')
        })
        
        print(f"✅ 窗口内共发现 {self.results['total_breakthroughs']} 个突破事件。")
        print(f"   爆发最集中日期: {self.results['most_active_day'].strftime('%Y-%m-%d')}")
        return True

    def _screen_momentum_leaders(self):
        """私有方法：基于动量筛选核心股票池。"""
        print("\n--- [步骤 5/5] 基于动量筛选核心股票池 ---")
        most_active_day = self.results['most_active_day']
        
        try:
            base_loc = self.breakthrough_signals.index.get_loc(most_active_day)
            end_loc = min(base_loc + 3, len(self.breakthrough_signals))
            three_day_window = self.breakthrough_signals.iloc[base_loc:end_loc]
            
            target_stocks = three_day_window.any(axis=0)[lambda x: x].index.tolist()

            if not target_stocks:
                self.results['final_selection'] = pd.DataFrame()
                return True

            if len(target_stocks) <=3:
                self.results['final_selection'] = pd.DataFrame({'股票代码': target_stocks})
                return True

            perf_end_date = three_day_window.index[-1]
            perf_end_loc = self.close_prices_df.index.get_loc(perf_end_date)
            perf_start_loc = perf_end_loc - 9
            
            if perf_start_loc < 0:
                self.results['final_selection'] = pd.DataFrame()
                return True

            start = self.close_prices_df.iloc[perf_start_loc][target_stocks]
            end = self.close_prices_df.iloc[perf_end_loc][target_stocks]
            
            returns = ((end / start) - 1).replace([np.inf, -np.inf], np.nan).dropna()
            returns.name = "10日涨幅"
            
            final_selection = returns.sort_values(ascending=False).head(int(np.ceil(len(returns) * 0.6)))
            
            self.results['final_selection'] = final_selection.reset_index().rename(columns={'index': '股票代码'})
            self.results['status'] = 'Success'
            return True

        except (KeyError, IndexError):
            return False

    def run_analysis(self):
        """公开方法：执行完整的分析流程。"""
        if not self._load_and_prepare_data(): return
        if not self._calculate_indicators(): return
        if not self._find_t0_date(): return
        if not self._analyze_temporal_distribution(): return
        if not self._screen_momentum_leaders(): return
        print("\n--- ✅ 策略分析全部完成 ---")

    def get_results(self):
        """公开方法：获取分析结果。"""
        return self.results

import pandas as pd
import numpy as np
from tqdm import tqdm
from data_management.data_processor import get_daily_data_for_backtest
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer


class SectorHighBreakoutAnalyzer:
    """
    板块成分股40日新高突破分析器
    
    该类用于分析板块成分股在最近40日内每5交易日中股价创出40日新高的个股比例，
    并挑选出比例最高的前2个5日周期。
    
    主要功能：
    - 自动加载板块成分股的历史价格数据
    - 计算每只股票的40日新高信号
    - 按5交易日为单位进行分组统计
    - 计算每个5日周期内创新高的股票比例
    - 识别并返回比例最高的前2个5日周期
    """
    
    def __init__(self, stock_list: list, end_date=None):
        """
        初始化分析器。
        
        参数:
            stock_list (list): 需要分析的股票代码列表（板块成分股）。
            end_date (str or pd.Timestamp, optional): 分析的截止日期。
                                                     如果为None，则自动使用当前日期。
                                                     默认为 None。
        """
        self.stock_list = stock_list
        if end_date is None:
            self.end_date = pd.Timestamp.now().normalize()
            print(f"未指定end_date，自动使用当前日期: {self.end_date.strftime('%Y-%m-%d')}")
        else:
            self.end_date = pd.to_datetime(end_date)
        
        # 分析参数
        self.lookback_days = 40  # 新高回看天数
        self.group_days = 5      # 分组天数
        self.required_history_days = 60  # 需要的最少历史数据天数
        
        # 结果存储
        self.results = {"status": "Not Started"}
        self.close_prices_df = None
        self.high_prices_df = None
        self.new_high_signals = None
        self.period_analysis = None

    def _load_and_prepare_data(self):
        """私有方法：加载并准备所有股票的价格数据。"""
        print("\n--- [步骤 1/4] 批量加载股票价格数据 ---")
        all_close_prices = {}
        all_high_prices = {}
        
        # 确保数据加载到指定的end_date
        date_str_for_loading = self.end_date.strftime('%Y-%m-%d')
        
        for stock_code in tqdm(self.stock_list, desc="加载行情数据"):
            try:
                df_daily = get_daily_data_for_backtest(stock_code, date_str_for_loading)
                if df_daily.empty or len(df_daily) < self.required_history_days:
                    continue
                
                df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
                df_daily = df_daily.set_index('trade_date')
                
                if 'close' in df_daily.columns and 'high' in df_daily.columns:
                    all_close_prices[stock_code] = df_daily['close']
                    all_high_prices[stock_code] = df_daily['high']
                    
            except Exception as e:
                print(f"处理 {stock_code} 时出错: {e}")
        
        if not all_close_prices:
            print("错误：未能加载任何有效的股票数据。")
            return False

        # 合并所有股票数据
        self.close_prices_df = pd.concat(all_close_prices, axis=1)
        self.high_prices_df = pd.concat(all_high_prices, axis=1)
        
        # 确保数据截取到end_date
        self.close_prices_df = self.close_prices_df[self.close_prices_df.index <= self.end_date]
        self.high_prices_df = self.high_prices_df[self.high_prices_df.index <= self.end_date]
        
        # 数据清洗：去除数据不足的股票
        min_valid_data = len(self.close_prices_df) - 10
        self.close_prices_df = self.close_prices_df.dropna(axis=1, thresh=min_valid_data)
        self.high_prices_df = self.high_prices_df[self.close_prices_df.columns]
        
        # 前向填充缺失值
        self.close_prices_df = self.close_prices_df.ffill()
        self.high_prices_df = self.high_prices_df.ffill()
        
        print(f"成功合并 {self.close_prices_df.shape[1]} 只股票的数据。")
        print(f"数据时间范围: {self.close_prices_df.index[0].strftime('%Y-%m-%d')} 至 {self.close_prices_df.index[-1].strftime('%Y-%m-%d')}")
        return True

    def _calculate_new_high_signals(self):
        """私有方法：计算每只股票的40日新高信号。"""
        print("\n--- [步骤 2/4] 计算40日新高信号 ---")
        
        if self.high_prices_df is None or self.high_prices_df.empty:
            print("错误: 最高价数据未准备好。")
            return False
        
        # 计算40日滚动最高价
        rolling_max_40 = self.high_prices_df.rolling(window=self.lookback_days, min_periods=self.lookback_days).max()
        
        # 判断当日最高价是否等于40日最高价（即创出40日新高）
        self.new_high_signals = (self.high_prices_df == rolling_max_40)
        
        # 统计每日创新高的股票数量
        daily_new_high_counts = self.new_high_signals.sum(axis=1)
        total_stocks = self.new_high_signals.shape[1]
        daily_new_high_ratios = daily_new_high_counts / total_stocks
        
        print(f"✅ 已计算40日新高信号。")
        print(f"   分析股票数量: {total_stocks}")
        print(f"   最近10日新高比例统计:")
        
        recent_10_days = daily_new_high_ratios.tail(10)
        for date, ratio in recent_10_days.items():
            print(f"   {date.strftime('%Y-%m-%d')}: {ratio:.2%} ({int(ratio * total_stocks)}/{total_stocks})")
        
        return True

    def _analyze_5day_periods(self):
        """私有方法：按5交易日分组分析新高比例。"""
        print("\n--- [步骤 3/4] 按5日周期分析新高比例 ---")
        
        if self.new_high_signals is None:
            print("错误: 新高信号数据未准备好。")
            return False
        
        # 获取最近40个交易日的数据进行分析
        analysis_data = self.new_high_signals.tail(self.lookback_days)
        
        if len(analysis_data) < self.lookback_days:
            print(f"警告: 可用数据不足40日，实际数据: {len(analysis_data)}日")
        
        # 按5日分组
        periods = []
        period_stats = []
        
        # 从最新日期往前，每5个交易日为一组
        for i in range(0, len(analysis_data), self.group_days):
            period_end_idx = len(analysis_data) - i - 1
            period_start_idx = max(0, period_end_idx - self.group_days + 1)
            
            if period_start_idx < 0:
                break
            
            # 获取该周期的数据
            period_data = analysis_data.iloc[period_start_idx:period_end_idx + 1]
            
            if len(period_data) < self.group_days:
                continue
            
            # 计算该周期内的新高情况
            period_start_date = period_data.index[0]
            period_end_date = period_data.index[-1]
            
            # 统计该周期内至少创出一次新高的股票数量
            stocks_with_new_high = period_data.any(axis=0).sum()
            total_stocks = period_data.shape[1]
            new_high_ratio = stocks_with_new_high / total_stocks if total_stocks > 0 else 0
            
            # 统计该周期内新高事件的总次数
            total_new_high_events = period_data.sum().sum()
            
            period_info = {
                'period_id': len(periods) + 1,
                'start_date': period_start_date,
                'end_date': period_end_date,
                'stocks_with_new_high': stocks_with_new_high,
                'total_stocks': total_stocks,
                'new_high_ratio': new_high_ratio,
                'total_new_high_events': total_new_high_events,
                'avg_events_per_stock': total_new_high_events / total_stocks if total_stocks > 0 else 0
            }
            
            periods.append(period_info)
            period_stats.append({
                'period': f"{period_start_date.strftime('%m-%d')}~{period_end_date.strftime('%m-%d')}",
                'ratio': new_high_ratio,
                'stocks_count': stocks_with_new_high,
                'total_events': total_new_high_events
            })
        
        if not periods:
            print("错误: 未能创建任何有效的5日周期。")
            return False
        
        # 按新高比例排序，选出前2个周期
        periods_sorted = sorted(periods, key=lambda x: x['new_high_ratio'], reverse=True)
        top_2_periods = periods_sorted[:2]
        
        self.period_analysis = periods
        self.results['total_periods_analyzed'] = len(periods)
        self.results['top_2_periods'] = top_2_periods
        self.results['all_periods'] = periods_sorted
        
        print(f"✅ 共分析了 {len(periods)} 个5日周期。")
        print(f"\n📊 所有周期新高比例排名:")
        for i, period in enumerate(periods_sorted, 1):
            print(f"   {i:2d}. {period['start_date'].strftime('%m-%d')}~{period['end_date'].strftime('%m-%d')}: "
                  f"{period['new_high_ratio']:.2%} ({period['stocks_with_new_high']}/{period['total_stocks']}) "
                  f"总事件:{period['total_new_high_events']}")
        
        return True

    def _generate_final_results(self):
        """私有方法：生成最终分析结果。"""
        print("\n--- [步骤 4/4] 生成最终分析结果 ---")
        
        if 'top_2_periods' not in self.results:
            print("错误: 周期分析结果未准备好。")
            return False
        
        top_2_periods = self.results['top_2_periods']
        
        if len(top_2_periods) == 0:
            print("未找到任何有效的高比例周期。")
            self.results['status'] = 'No Valid Periods Found'
            return True
        
        # 生成详细的结果报告
        final_report = {
            'analysis_date': self.end_date.strftime('%Y-%m-%d'),
            'sector_stock_count': len(self.stock_list),
            'valid_stock_count': self.close_prices_df.shape[1] if self.close_prices_df is not None else 0,
            'analysis_period_days': self.lookback_days,
            'group_period_days': self.group_days,
            'top_periods': []
        }
        
        print(f"\n🎯 比例最高的前2个5日周期:")
        for i, period in enumerate(top_2_periods, 1):
            period_detail = {
                'rank': i,
                'period_range': f"{period['start_date'].strftime('%Y-%m-%d')} ~ {period['end_date'].strftime('%Y-%m-%d')}",
                'new_high_ratio': period['new_high_ratio'],
                'stocks_with_new_high': period['stocks_with_new_high'],
                'total_stocks': period['total_stocks'],
                'total_new_high_events': period['total_new_high_events'],
                'avg_events_per_stock': period['avg_events_per_stock']
            }
            final_report['top_periods'].append(period_detail)
            
            print(f"   第{i}名: {period_detail['period_range']}")
            print(f"         新高比例: {period_detail['new_high_ratio']:.2%}")
            print(f"         创新高股票: {period_detail['stocks_with_new_high']}/{period_detail['total_stocks']}")
            print(f"         总新高事件: {period_detail['total_new_high_events']}")
            print(f"         平均每股事件: {period_detail['avg_events_per_stock']:.2f}")
        
        self.results['status'] = 'Success'
        self.results['final_report'] = final_report
        
        return True

    def run_analysis(self):
        """公开方法：执行完整的分析流程。"""
        print(f"\n🚀 开始板块成分股40日新高突破分析")
        print(f"分析股票数量: {len(self.stock_list)}")
        print(f"截止日期: {self.end_date.strftime('%Y-%m-%d')}")
        print(f"分析参数: {self.lookback_days}日新高, 按{self.group_days}日分组")
        
        if not self._load_and_prepare_data():
            self.results['status'] = 'Data Loading Failed'
            return
        
        if not self._calculate_new_high_signals():
            self.results['status'] = 'Signal Calculation Failed'
            return
        
        if not self._analyze_5day_periods():
            self.results['status'] = 'Period Analysis Failed'
            return
        
        if not self._generate_final_results():
            self.results['status'] = 'Result Generation Failed'
            return
        
        print("\n--- ✅ 板块新高突破分析全部完成 ---")

    def get_results(self):
        """公开方法：获取分析结果。"""
        return self.results

    def get_top_periods_summary(self):
        """公开方法：获取前2个周期的简要汇总。"""
        if self.results.get('status') != 'Success':
            return pd.DataFrame()
        
        if 'final_report' not in self.results:
            return pd.DataFrame()
        
        top_periods = self.results['final_report']['top_periods']
        
        summary_data = []
        for period in top_periods:
            summary_data.append({
                '排名': period['rank'],
                '时间范围': period['period_range'],
                '新高比例': f"{period['new_high_ratio']:.2%}",
                '创新高股票数': f"{period['stocks_with_new_high']}/{period['total_stocks']}",
                '总新高事件数': period['total_new_high_events'],
                '平均每股事件数': f"{period['avg_events_per_stock']:.2f}"
            })
        
        return pd.DataFrame(summary_data)

    def get_detailed_signals_for_period(self, period_rank: int = 1):
        """
        公开方法：获取指定排名周期内的详细新高信号。
        
        参数:
            period_rank (int): 周期排名，1表示第一名，2表示第二名
            
        返回:
            pd.DataFrame: 该周期内每日每股的新高信号详情
        """
        if self.results.get('status') != 'Success' or 'top_2_periods' not in self.results:
            return pd.DataFrame()
        
        if period_rank < 1 or period_rank > len(self.results['top_2_periods']):
            print(f"错误: 周期排名 {period_rank} 超出范围")
            return pd.DataFrame()
        
        target_period = self.results['top_2_periods'][period_rank - 1]
        start_date = target_period['start_date']
        end_date = target_period['end_date']
        
        # 提取该周期的新高信号数据
        period_signals = self.new_high_signals.loc[start_date:end_date]
        
        return period_signals


# =============================================================================
# --- 示例用法 (Example Usage) ---
# =============================================================================
if __name__ == "__main__":
    
    try:
        raw_stocks = get_bankuai_stocks("小金属")
        stock_filter = StockXihua()
        filtered_stocks = stock_filter.filter_basic_conditions(raw_stocks)
    except Exception as e:
        print(f"获取股票列表失败: {e}")
        filtered_stocks = []

    if filtered_stocks:
        # --- 场景1: 实盘自动化运行 (不传入日期) ---
        print("\n\n场景1: 实盘自动化运行...")
        # 实例化时不传入 end_date，将自动使用当前日期
        auto_analyzer = SectorMomentumAnalyzer(stock_list=filtered_stocks)
        auto_analyzer.run_analysis()
        auto_results = auto_analyzer.get_results()

        # --- 场景2: 指定历史日期进行回测 ---
        print("\n\n场景2: 指定历史日期回测...")
        backtest_date = '2025-08-07'
        print(f"回测日期: {backtest_date}")
        backtest_analyzer = SectorMomentumAnalyzer(stock_list=filtered_stocks, end_date=backtest_date)
        backtest_analyzer.run_analysis()
        backtest_results = backtest_analyzer.get_results()

        # --- 打印回测报告 ---
        print("\n\n================= 回测分析报告 ==================")
        print(f"分析状态: {backtest_results.get('status', 'N/A')}")
        
        if backtest_results.get('status') == 'Success':
            print(f"事件触发日 (T0): {backtest_results.get('t0_date', pd.NaT).strftime('%Y-%m-%d')}")
            final_selection_df = backtest_results.get('final_selection')
            if final_selection_df is not None and not final_selection_df.empty:
                print("\n--- 核心动量股池 (按10日涨幅排序) ---")
                if '10日涨幅' in final_selection_df.columns:
                    final_selection_df['10日涨幅'] = pd.to_numeric(final_selection_df['10日涨幅'], errors='coerce').map('{:.2%}'.format)
                print(final_selection_df.to_string(index=False))
        print("==================================================")