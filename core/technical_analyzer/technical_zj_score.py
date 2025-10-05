"""
中级技术分析评分系统 - 整理版
支持不同市场环境下的动态权重调整
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
import warnings

# #市场环境的判断，暂时不需要，因为技术指标的评分已经包括了这样的判断
# class MarketEnvironmentAnalyzer:
#     """
#     市场环境分析器
#     用于判断当前市场环境
#     """
    
#     def __init__(self):
#         self.environment_names = {
#             'bull': '牛市',
#             'bear': '熊市',
#             'sideways': '震荡市'
#         }
    
#     def analyze_sector_environment(self, sector_name: str, 
#                                  sector_data: pd.DataFrame) -> str:
#         """
#         分析板块环境
        
#         Args:
#             sector_name: 板块名称
#             sector_data: 板块数据
            
#         Returns:
#             str: 环境类型
#         """
#         try:
#             # 基于板块指数判断
#             current_price = sector_data['close'].iloc[-1]
#             price_5d_ago = sector_data['close'].iloc[-6]
#             ma_60 = sector_data['MA_60'].iloc[-1]
            
#             # 强势上涨判断
#             if (current_price > price_5d_ago and current_price > ma_60):
#                 return 'bull'
#             elif (current_price < price_5d_ago and current_price < ma_60):
#                 return 'bear'
#             else:
#                 return 'sideways'
                
#         except Exception as e:
#             warnings.warn(f"板块环境分析失败: {e}")
#             return 'sideways'
    
#     def analyze_market_environment(self, market_data: pd.DataFrame) -> str:
#         """
#         分析整体市场环境
        
#         Args:
#             market_data: 市场指数数据
            
#         Returns:
#             str: 环境类型
#         """
#         try:
#             # 基于主要指数判断
#             ma_20 = market_data['MA_20'].iloc[-1]
#             ma_60 = market_data['MA_60'].iloc[-1]
#             ma_120 = market_data.get('MA_120', market_data['MA_60']).iloc[-1]
            
#             if ma_20 > ma_60 > ma_120:
#                 return 'bull'
#             elif ma_20 < ma_60 < ma_120:
#                 return 'bear'
#             else:
#                 return 'sideways'
                
#         except Exception as e:
#             warnings.warn(f"市场环境分析失败: {e}")
#             return 'sideways'



class TechnicalScoringSystem:
    """
    技术分析评分系统
    
    功能：
    1. 根据市场环境动态调整指标权重
    2. 计算个股技术指标综合得分
    3. 支持多种市场环境判断
    """
    
    def __init__(self):
        """初始化评分系统"""
        # 基础权重配置 (总和为1.0)
        self.base_weights = {
            'trend': 0.30,      # 趋势指标 (MACD, ADX, MA等)
            'momentum': 0.25,   # 动量指标 (KDJ, RSI等)
            'volume': 0.20,     # 成交量指标
            'volatility': 0.15, # 波动率指标 (BOLL, ATR等)
            'oscillator': 0.10  # 震荡指标
        }
        
        # # 不同市场环境下的权重调整系数
        # 不需要市场环境权重调整，直接使用基础权重配置
        # 因为技术评分已经包含了对市场环境的判断
        
        # # 指标阈值配置
        # self.thresholds = {
        #     'macd_positive': 0,
        #     'adx_strong': 20,
        #     'kdj_golden_cross': True,
        #     'volume_above_avg': True,
        #     'boll_width_percentile': 0.8
        # }
    
    # def detect_market_environment(self, df: pd.DataFrame) -> str:
    #     """
    #     检测市场环境
        
    #     Args:
    #         df: 包含技术指标的DataFrame
            
    #     Returns:
    #         str: 市场环境类型 ('bull', 'bear', 'sideways')
    #     """
    #     try:
    #         if df.empty:
    #             return 'sideways'
                
    #         # 获取最新数据
    #         latest = df.iloc[-1]
            
    #         # 简单的市场环境判断
    #         if 'MACD' in df.columns and 'RSI' in df.columns:
    #             macd = latest.get('MACD', 0)
    #             rsi = latest.get('RSI', 50)
                
    #             if macd > 0 and rsi > 50:
    #                 return 'bull'
    #             elif macd < 0 and rsi < 50:
    #                 return 'bear'
    #             else:
    #                 return 'sideways'
    #         else:
    #             return 'sideways'
                
    #     except Exception as e:
    #         return 'sideways'
            
    #     Returns:
    #         str: 市场环境 ('bull', 'bear', 'sideways')
    #     """
    #     try:
    #         # 基于MA趋势判断
    #         ma_20 = df['MA_20'].iloc[-1]
    #         ma_60 = df['MA_60'].iloc[-1] 
    #         ma_120 = df['MA_120'].iloc[-1] if 'MA_120' in df.columns else ma_60
            
    #         # 趋势强度判断
    #         if ma_20 > ma_60 > ma_120:
    #             return 'bull'
    #         elif ma_20 < ma_60 < ma_120:
    #             return 'bear'
    #         else:
    #             return 'sideways'
                
    #     except Exception as e:
    #         warnings.warn(f"市场环境检测失败: {e}")
    #         return 'sideways'
    
    def _calculate_trend_score(self, df: pd.DataFrame) -> float:
        """
        仅基于DMI和ADX计算趋势得分 (0-100)。
        使用平滑后的“多空力量差”作为核心前提条件。
        """
        try:
            # 1. 获取所需指标的最新值
            adx = df['ADX'].iloc[-1]
            dmi_spread_ma3_now = df['DMI_SPREAD_MA3'].iloc[-1]
            dmi_spread_ma3_prev = df['DMI_SPREAD_MA3'].iloc[-2]

            # 检查数据是否有效
            if pd.isna(dmi_spread_ma3_now) or pd.isna(dmi_spread_ma3_prev):
                return 20.0 # 数据不足，给予中性偏低分

            # 2. 【新核心前提条件】
            # 要求：平滑后的净多头动能为正，且仍在扩大
            is_bullish_momentum_accelerating = \
                (dmi_spread_ma3_now > 0) and (dmi_spread_ma3_now > dmi_spread_ma3_prev)

            # 3. 如果前提满足，则根据ADX的绝对值进行分层打分 (此部分逻辑不变)
            if is_bullish_momentum_accelerating:
                if adx > 80:
                    return 10.0   # 衰竭区
                elif adx > 60:
                    return 30.0  # 主升浪区但偏高
                elif adx > 48:
                    return 70.0  # 主升浪区但偏低
                elif adx > 30:  
                    return 100.0  # 主升浪区但偏高
                elif adx > 20:
                    return 80.0   # 最佳潜伏区且趋势确立
                else:
                    return 50.0   # 弱多头趋势 (动能虽强，但整体趋势强度ADX未跟上)

        except Exception as e:
            warnings.warn(f"DMI/ADX得分计算失败: {e}")
            return 10.0 # 异常时给予一个中性分数
    
    def _calculate_volatility_score(self, df: pd.DataFrame) -> float:
        """
        计算波动率得分 (0-100) - 2025/10/04 短期优化版

        核心逻辑:
        1. 使用20日滚动窗口计算带宽百分位，聚焦短期。
        2. 优先奖励“盘整蓄力”(MACD弱势/反转 + BOLL收缩)模式。
        """
        try:
            # # 0. 数据预处理：已经预处理，这里不用单独计算
            # # 1.1 计算标准化的布林带宽
            # df['BOLL_WIDTH'] = (df['UPPER'] - df['LOWER']) / df['MID']
            # # 1.2 【新】使用20日窗口计算带宽百分位
            # df['BOLL_WIDTH_PCT_20'] = df['BOLL_WIDTH'].rolling(window=20).apply(
            #     lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
            # )

            # 1. 获取最新值
            width_pct_now = df['BOLL_WIDTH_PCT_20'].iloc[-1]
            
            # 检查数据有效性
            if pd.isna(width_pct_now):
                return 30.0

            # 获取 MACD 相关状态
            macd_now = df['MACD'].iloc[-1]
            is_macd_bullish = macd_now > 0
            # 判断MACD是否处于“拐点”状态 (0轴下方但开始回升)
            is_macd_reversal = (macd_now <= 0) and (macd_now > df['MACD'].iloc[-4])

            # 获取近期是否处于极度收缩状态 (相对于过去20天)
            # 我们定义近期最低的15%为极度收缩
            was_in_squeeze = df['BOLL_WIDTH_PCT_20'].iloc[-6:-1].min() < 0.15

            # 2. 【新核心评分逻辑】

            # 状态一：“空中加油” - 最高分
            # 条件：近期曾极致收缩 + MACD已处于多头趋势
            if was_in_squeeze and is_macd_bullish:
                return 100.0   # 分数也很高

            
            # 状态二：“盘整蓄力” - 次高分
            # 条件：近期曾极致收缩 + MACD处于反转拐点状态
            elif was_in_squeeze and is_macd_reversal:
                return 85.0  # 给予次高分，捕捉高潜力爆发点
            
                
            # 状态三：“极致收缩”的预备信号 (不看MACD)
            # 条件：当前带宽处于历史最低10%
            elif width_pct_now < 0.10:
                return 60.0   # 潜力股，给予较高关注分
                
            # # 状态四：“持续高波动”的趋势行情（由于是做短期，所以不考虑），以后可考虑<0.50+>0.15给与40分
            # # 条件：当前带宽处于历史最高90%
            # elif width_pct_now > 0.90:
            #     return 70.0   # 趋势延续，分数也不错
                
            # 状态五：常规波动
            else:
                return 30.0   # 中性偏低分

        except Exception as e:
            warnings.warn(f"波动率得分计算失败: {e}")
            return 30.0
    
    def _calculate_momentum_score(self, df: pd.DataFrame) -> float:
        """
        计算动量得分 (0-100)，主要基于KDJ指标。
        """
        try:
            # 1. 为了代码清晰，先获取所有需要用到的KDJ值
            k_now = df['K'].iloc[-1]
            d_now = df['D'].iloc[-1]
            j_now = df['J'].iloc[-1]
            
            k_prev = df['K'].iloc[-2]
            d_prev = df['D'].iloc[-2]
            j_prev = df['J'].iloc[-2]
            
            k_max_2d = df['K'].iloc[-2:].max() # 最近2日的K值最大值
            j_max_2d = df['J'].iloc[-2:].max() # 最近2日的J值最大值
            
            j_min_2d = df['J'].iloc[-2:].min() # 最近2日的J值最小值

            # 2. 【核心评分逻辑 - 按优先级判断】

            # 规则5：超买后拐头 (次高优先级负向信号)
            # 条件：最近2日曾严重超买，且J值今日 < 昨日
            if (k_max_2d > 75 or j_max_2d > 95) and (j_now < j_prev):
                return -50.0  # 明确的风险信号，给予极低分

            # 规则3：刚启动 (次高优先级正向信号)
            # 条件：K线抬头向上，且K值处于低位(<40)
            elif (k_now > k_prev) and (k_now < 40):
                return 100.0 # 您定义的“加分最高”信号
            
            # 规则6：低位金叉 (最高优先级正向信号)
            # 条件：K线抬头向上，且K值处于低位(<50)
            is_golden_cross = (k_now > d_now) and (k_prev <= d_prev)
            if is_golden_cross and k_now < 50: # 低位金叉
                return 95.0 


            # 规则2：严重超卖 (第三优先级正向信号)
            # 条件：最近2日J值都小于10
            elif j_min_2d < 10:
                # 这里的 j_now < 10 and j_prev < 10 也可以，j_min_2d < 10更简洁
                return 90.0  # 强烈的反转预期，给予很高分

            # 规则1：普通超卖
            # 条件：仅今日J值小于10 (且不满足规则2)
            elif j_now < 10:
                return 75.0  # 不错的反转预期，分数略低于严重超卖

            # 规则4：仅处于超买状态，但未拐头 (潜在风险)
            # 条件：超买，但不满足规则5的拐头条件
            elif (k_max_2d > 75 or j_max_2d > 95):
                return -30.0  # 趋势虽强但风险已高，给予谨慎低分

            # 其他常规状态：K在D之上，健康的多头排列
            elif k_now > d_now:
                return 50.0 # 正常的多头状态，给予中性偏高分
                
            # 默认状态：空头排列
            else:
                return 20.0 # 空头状态，给予低分

        except Exception as e:
            warnings.warn(f"动量得分计算失败: {e}")
            return 50.0 # 异常时给予中性分

    # =================================================================
    # =============== 【最终版】成交量评分模块 (2025/10/04) ===============
    # =================================================================

    def _calculate_volume_score(self, df: pd.DataFrame) -> float:
        """
        【主函数】计算最终的成交量得分 (0-100)
        
        通过调用三个独立的评分组件，并将它们的结果合成为一个总分：
        1. OBV健康分 (Base Health): 基础资金流是否健康？ (满分25)
        2. 动能爆发分 (Burst Signal): 是否存在多维共振的强力启动信号？ (满分65)
        3. MFI背离分 (Reversal Risk): 是否存在隐藏的顶背离风险？ (惩罚项, -50到0)
        """
        try:
            # 1. 计算各组件得分
            health_score = self._score_obv_health(df)
            burst_score = self._score_momentum_burst(df)
            divergence_score = self._score_mfi_divergence(df) # 这是一个负分或0分

            # 2. 合成总分
            # 基础分 + 爆发分 + 风险惩罚
            final_score = health_score + burst_score + divergence_score
            
            # 3. 标准化到 0-100 的范围
            # max(0, ...) 确保了即使出现强烈的顶背离，最低分也只是0
            # min(..., 100) 确保了最高分不超过100
            final_score_normalized = max(0, min(final_score, 100))
            
            return final_score_normalized

        except Exception as e:
            warnings.warn(f"成交量总分计算失败: {e}")
            return 30.0 # 异常时给予中性分

    # ==================== 组件1: OBV健康分 ====================
    def _score_obv_health(self,df: pd.DataFrame) -> float:
        """组件1：计算OBV健康分"""
        try:
            if 'OBV_MA30' not in df.columns:
                # 假设zhibiao函数中已计算'OBV'
                df['OBV_MA30'] = df['OBV'].rolling(window=30,min_periods=20).mean()
            
            obv_now = df['OBV'].iloc[-1]
            obv_ma30 = df['OBV_MA30'].iloc[-1]
            
            if obv_now > obv_ma30:
                return 25.0  # 资金流健康，给予基础分
            else:
                return 0.0
        except Exception:
            return 1.0 # 异常时给一个1分

    # ==================== 组件2: 动能爆发分 ====================
    def _score_momentum_burst(self,df: pd.DataFrame) -> float:
        """组件2：计算动能爆发分"""
        try:
            # 条件1：最近2日收盘价只有1日或0日站上布林带上轨
            recent_2d_close = df['close'].iloc[-2:]  # 最近2日收盘价
            recent_2d_upper = df['UPPER'].iloc[-2:]  # 最近2日布林带上轨
            breakout_days = (recent_2d_close > recent_2d_upper).sum()  # 站上上轨的天数
            price_breakout = breakout_days <= 1  # 只有1日或0日站上上轨

            # 条件2：MACD动量条件 (MACD1>MACD4 或 MACD>0)
            macd1 = df['MACD'].iloc[-1]
            macd4 = df['MACD'].iloc[-4]
            macd_positive = macd1 > 0
            macd_burst = (macd1 > macd4) or macd_positive
            
            # 条件3：资金流创20日新高 (MFI或OBV或VR任一创20日新高)
            # MFI资金流指标
            mfi_now = df['MFI'].iloc[-1]
            mfi_high_20d = df['MFI'].iloc[-21:-1].max()
            mfi_breakout = mfi_now > mfi_high_20d
            
            # OBV能量潮指标
            obv_now = df['OBV'].iloc[-1]
            obv_high_20d = df['OBV'].iloc[-21:-1].max()
            obv_breakout = obv_now > obv_high_20d
            
            # VR容量比率指标
            vr_now = df['VR'].iloc[-1]
            vr_high_20d = df['VR'].iloc[-21:-1].max()
            vr_breakout = vr_now > vr_high_20d
            
            # 资金流综合条件：任一指标创20日新高
            fund_flow_breakout = mfi_breakout or obv_breakout or vr_breakout

            # 根据满足的条件数量来打分
            conditions_met = sum([price_breakout, macd_burst, fund_flow_breakout])
            
            if conditions_met == 3:
                return 65.0  # 三维共振，最强信号
            elif conditions_met == 2:
                return 15.0  # 强信号
            elif conditions_met == 1:
                return 10.0  # 有启动迹象
            else:
                return 0.0
        except Exception:
            return 10.0


    # ==================== 组件3: MFI背离风险分 ====================
    def _score_mfi_divergence(self, df: pd.DataFrame, lookback: int = 60, min_swings: int = 2) -> float:
        """
        计算MFI背离分（改进版，更精准的顶背离检测）
        
        Args:
            df: 包含价格和MFI指标的数据框
            lookback: 回溯周期
            min_swings: 最小波动次数要求，用于过滤假信号
        
        Returns:
            float: 背离评分，负值表示顶背离风险
        """
        try:
            # 数据验证
            if len(df) < lookback + 10:
                return 0.0
                
            # 获取分析窗口数据
            window = df.iloc[-lookback:].copy()
            price_series = window['close'].values
            mfi_series = window['MFI'].values
            
            # 1. 寻找显著的价格和MFI高点
            price_peaks = self._find_significant_peaks(price_series, prominence_ratio=0.02)
            mfi_peaks = self._find_significant_peaks(mfi_series, prominence_ratio=0.05)
            
            # 波动不足检查
            if len(price_peaks) < min_swings or len(mfi_peaks) < min_swings:
                return 0.0
            
            # 2. 获取关键数据点
            recent_price_peak_idx = price_peaks[-1]
            recent_mfi_peak_idx = mfi_peaks[-1]
            
            price_peak_val = price_series[recent_price_peak_idx]
            mfi_peak_val = mfi_series[recent_mfi_peak_idx]
            current_price = price_series[-1]
            current_mfi = mfi_series[-1]
            
            # 3. 多重条件验证顶背离
            divergence_score = 0.0
            
            # 条件1: 价格强度 vs MFI衰减
            price_strength = current_price / price_peak_val
            mfi_weakness = current_mfi / mfi_peak_val if mfi_peak_val > 0 else 1.0
            
            if price_strength > 0.98 and mfi_weakness < 0.85:
                divergence_score -= 20.0
                
                # 条件2: 时间序列验证
                if recent_mfi_peak_idx > recent_price_peak_idx:
                    # MFI高点晚于价格高点，可能不是真正的背离
                    divergence_score += 10.0
                else:
                    # 真正的背离：价格后高点，MFI前高点
                    divergence_score -= 10.0
            
            # 条件3: 趋势确认
            price_trend = self._calculate_trend(price_series[-20:])
            mfi_trend = self._calculate_trend(mfi_series[-20:])
            
            if price_trend > 0 and mfi_trend < -0.5:
                divergence_score -= 15.0
            
            # 条件4: 超买区域验证
            if mfi_peak_val > 80 and current_mfi < 70:
                divergence_score -= 10.0
            
            # 条件5: 成交量确认
            if 'volume' in df.columns:
                volume_trend = self._calculate_trend(window['volume'].values[-10:])
                if volume_trend < 0:
                    divergence_score -= 5.0
            
            # 限制分数范围并返回
            return max(-50.0, min(0.0, divergence_score))
            
        except Exception as e:
            return 0.0

    # ==================== 辅助函数 ====================

    def _find_significant_peaks(self, series: np.ndarray, prominence_ratio: float = 0.02) -> list:
        """
        寻找显著的高点位置
        
        Args:
            series: 时间序列数据
            prominence_ratio: 显著度比例阈值
            
        Returns:
            list: 峰值位置索引列表
        """
        try:
            from scipy.signal import find_peaks
            
            # 计算要求的显著度
            prominence = np.std(series) * prominence_ratio
            
            # 寻找峰值
            peaks, properties = find_peaks(series, prominence=prominence)
            
            # 返回按时间顺序排列的峰值位置
            return sorted(peaks.tolist())
        
        except ImportError:
            # 备用方案：如果没有scipy，使用简单方法
            peaks = []
            for i in range(1, len(series) - 1):
                if series[i] > series[i-1] and series[i] > series[i+1]:
                    # 简单峰值检测
                    if i > 0 and len(peaks) > 0:
                        # 检查与上一个峰值的差异是否显著
                        if series[i] > series[peaks[-1]] * (1 + prominence_ratio):
                            peaks.append(i)
                    else:
                        peaks.append(i)
            return peaks

    def _calculate_trend(self, series: np.ndarray) -> float:
        """
        计算序列的趋势强度
        
        Args:
            series: 时间序列数据
            
        Returns:
            float: 标准化后的趋势强度
        """
        if len(series) < 2:
            return 0.0
        
        x = np.arange(len(series))
        slope = np.polyfit(x, series, 1)[0]  # 线性回归的斜率
        
        # 标准化趋势强度
        trend_strength = slope / (np.std(series) + 1e-8)
        return trend_strength
    
    
    def _calculate_oscillator_score(self, df: pd.DataFrame) -> float:
        """
        【最终版】计算震荡指标得分 (0-100)
        
        核心任务：在被严格定义的“战术牛市”中，寻找“健康回调买入点”。
        """
        try:
            # 0. 数据验证与获取
            # 确保所有需要的指标都存在
            required_cols = ['RSI_24', 'UPPER', 'ADX', 'PDI', 'MDI','MID']
            if not all(col in df.columns for col in required_cols):
                return 30.0

            rsi = df['RSI_24'].iloc[-1]
            mid = df['MID'].iloc[-1]
            mid_prev = df['MID'].iloc[-2]
            # 1. 【核心前提条件】构建三层牛市过滤器
            
            # 第一层：战略过滤 - 中线趋势是否向上？
            is_mid_term_bullish =  (mid > mid_prev)

            # 第二层：战术确认 - 近期是否表现出强势？
            # 条件A: 近期是否有过爆发性突破？
            has_recent_boll_breakout = (df['close'].iloc[-10:] > df['UPPER'].iloc[-10:]).any()
            # 条件B: 当前趋势状态是否强劲？
            is_adx_strong = (df['ADX'].iloc[-1] > 25) and (df['PDI'].iloc[-1] > df['MDI'].iloc[-1])
            
            # 组合成最终的“战术牛市”判断
            is_in_tactical_bull_phase = is_mid_term_bullish and (has_recent_boll_breakout or is_adx_strong)

            # 如果前提不满足，则此模块不提供高分
            if not is_in_tactical_bull_phase:
                return 20.0

            # 2. 【核心评分逻辑】在满足“战术牛市”前提下，为回调深度打分
            
            # 状态一：最佳回调买入区
            if 40 <= rsi < 55:
                return 100.0 # 强势回调结束，即将再次上涨
                
            # 状态二：回调稍深区
            elif 30 <= rsi < 40:
                return 75.0
                
            # 状态三：正常强势运行区
            elif 55 <= rsi < 70:
                return 60.0  # 趋势健康，但不是最佳入场点
                
            # 状态四：超买区
            elif rsi >= 70:
                return 10.0  # 风险积聚，等待回调
                
            # 状态五：极度超卖区
            else: # rsi < 30
                return 40.0 # 趋势可能转弱，谨慎对待

        except Exception as e:
            warnings.warn(f"震荡指标得分计算失败: {e}")
            return 40.0
    
# ==================== 主要接口方法 ====================
    
    def _calculate_category_scores(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        计算各类指标得分
        
        Args:
            df: 包含技术指标的DataFrame
            
        Returns:
            Dict[str, float]: 各类指标得分
        """
        return {
            'trend': self._calculate_trend_score(df),
            'momentum': self._calculate_momentum_score(df),
            'volume': self._calculate_volume_score(df),
            'volatility': self._calculate_volatility_score(df),
            'oscillator': self._calculate_oscillator_score(df)
        }
    
    def get_final_score(self, stock_df: pd.DataFrame) -> Tuple[float, Dict[str, float], Dict[str, float]]:
        """
        计算最终加权得分
        
        Args:
            stock_df: 个股技术指标DataFrame
            market_environment: 市场环境，如果为None则自动检测
            
        Returns:
            Tuple[float, Dict[str, float], Dict[str, float]]: 
            (最终得分, 归一化权重, 各类得分)
        """
        # 1. 计算各类指标得分
        category_scores = self._calculate_category_scores(stock_df)
        
        # 2. 直接使用基础权重计算最终得分
        final_score = sum(
            category_scores[category] * self.base_weights[category]
            for category in self.base_weights.keys()
        )
        
        return final_score, self.base_weights, category_scores

# =================================================
# ============= 【推荐的】使用流程 ==================
# =================================================
if __name__ == '__main__':
    # 0. 准备数据和指标 (省略)
    # 假设 stock_df 是已经计算好所有指标的DataFrame
    
    # 1. 实例化你的工具
    scoring_system = TechnicalScoringSystem()
    env_analyzer = EnvironmentAnalyzer()

    # 2. 【第一步】调用分析器，独立地判断环境
    current_env = env_analyzer.analyze_stock_micro_environment(stock_df)

    # 3. 【第二步】将数据和已知的环境，送入评分引擎
    score, weights, cat_scores = scoring_system.get_final_score(
        stock_df,
        market_environment=current_env
    )
    
    # 4. 打印结果
    print(f"========= 最终技术评分报告 (时间: {pd.Timestamp.now()}) ==========")
    print(f"判断出的微观环境: {current_env.upper()}")
    print("-" * 50)
    print("各维度裸分:")
    for cat, s in cat_scores.items():
        print(f"  - {cat.capitalize():<12}: {s:.2f}")
    print("-" * 50)
    print(f"基于 '{current_env}' 环境的最终权重:")
    for cat, w in weights.items():
        print(f"  - {cat.capitalize():<12}: {w:.2%}")
    print("-" * 50)
    print(f"最终加权总分: {score:.2f}")
    print("=" * 50)


# 使用示例
def example_usage():
    """使用示例"""
    # 创建评分系统
    scoring_system = TechnicalScoringSystem()
    
    # 模拟数据
    dates = pd.date_range('2024-01-01', periods=200)
    stock_data = pd.DataFrame({
        'date': dates,
        'close': np.random.randn(200).cumsum() + 100,
        'volume': np.random.randint(10000, 50000, 200)
    })
    
    # 计算技术指标 (这里需要您的zhibiao函数)
    # stock_data = zhibiao(stock_data)
    

    
    # 计算最终得分
    final_score, weights, category_scores = scoring_system.get_final_score(
        stock_data, market_env
    )
    
    print(f"市场环境: {market_env}")
    print(f"最终得分: {final_score:.2f}")
    print(f"权重分配: {weights}")
    print(f"各类得分: {category_scores}")


if __name__ == '__main__':
    example_usage()