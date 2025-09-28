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

