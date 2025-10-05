import pandas as pd
import numpy as np
import warnings

class LongTermTechnicalScorer:
    """
    一个专门用于评估股票长线技术状态的评分器。
    
    核心理念：
    - 趋势为王：最重视长周期均线和趋势强度指标。
    - 牛市回调：在确认的上升趋势中，寻找理想的增持/买入点。
    - 量价确认：要求成交量指标支撑价格趋势。
    - 容忍超买：在强势市场中，超买不是卖出信号，而是趋势强劲的体现。
    """
    
    def __init__(self):
        """
        初始化基础权重。这些权重明确体现了长线策略的侧重点。
        """
        self.base_weights = {
            'trend': 0.45,       # 趋势是核心，权重最高
            'oscillator': 0.25,  # 寻找牛市回调买点，权重次之
            'volume': 0.15,      # 量能确认趋势，重要
            'volatility': 0.10,  # 偏好稳定上涨，而非爆炸性波动
            'momentum': 0.05     # 动量作为次要参考，权重最低
        }

    # ==================== 核心评分组件 ====================

    def _calculate_long_term_trend_score(self, df: pd.DataFrame) -> float:
        """
        组件1：计算长线趋势得分 (0-100)
        - 重点考察长期均线排列和ADX趋势强度。
        """
        try:
            # 1. 获取所需指标的最新值（对于长线可以不计算MA3）
            close = df['close'].iloc[-1]
            middle = df['MID'].iloc[-1]
            middle_prev = df['MID'].iloc[-2]
            pdi = df['PDI'].iloc[-1]
            mdi = df['MDI'].iloc[-1]
            adx = df['ADX'].iloc[-1]
            dmi_spread_ma3_now = df['DMI_SPREAD_MA3'].iloc[-1]
            dmi_spread_ma3_prev = df['DMI_SPREAD_MA3'].iloc[-2]
            bearish_spread_ma3_now = df['BEARISH_SPREAD_MA3'].iloc[-1]
            bearish_spread_ma3_prev = df['BEARISH_SPREAD_MA3'].iloc[-2]
            bearish_spread_ma3_prev2 = df['BEARISH_SPREAD_MA3'].iloc[-3]

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
                    return 80.0  # 主升浪区但偏低
                elif adx > 30:  
                    return 100.0  # 主升浪区但偏高
                elif adx > 20:
                    return 90.0   # 最佳潜伏区且趋势确立
                else:
                    return 55.0   # 弱多头趋势 (动能虽强，但整体趋势强度ADX未跟上)

            # --- 逻辑2: 探底回升模式一 (中等分: 40) ---
            # 描述: 空头市场中，价格在中轨下，但中轨已开始抬头，显示下跌趋缓，有反转迹象。
            is_bottom_reversal_type1 = \
                (middle > middle_prev) and (mdi > pdi) and (close < middle)

            if is_bottom_reversal_type1:
                return 40.0  # 明确的左侧信号，给予一个不错的分数

            # --- 逻辑3: 探底回升模式二 (中等偏低分: 35) ---
            # 描述: 空头市场中，中轨仍在下降，但空头力量(BEARISH_SPREAD)已连续2日收缩，是更早期的衰竭信号。
            is_bearish_power_exhausting = \
                (bearish_spread_ma3_now < bearish_spread_ma3_prev) and \
                (bearish_spread_ma3_prev < bearish_spread_ma3_prev2)
                
            is_bottom_reversal_type2 = \
                (middle < middle_prev) and (mdi > pdi) and \
                is_bearish_power_exhausting and (close < middle)

            if is_bottom_reversal_type2:
                return 35.0 # 更早期的左侧信号，风险更高，分数稍低

            # --- 其他情况 ---
            return 10.0 # 不满足任何看多或反转条件，给予低分

        except Exception as e:
            warnings.warn(f"长线趋势得分计算失败: {e}")
            return 10.0 # 异常时给一个较低分


    def _calculate_long_term_oscillator_score(self, df: pd.DataFrame) -> float:
        """
        组件2 v3：计算震荡指标得分 - 聚焦于周线级别的"健康趋势中的回调买点"
        - 核心逻辑: 以周线MID(MA20)为趋势核心，结合OBV和价格位置，寻找RSI回调后的启动点。
        - 该版本完全基于您的实战经验进行调整。
        """
        try:
            # --- 1. 数据准备与有效性检查 ---
            required_cols = ['close', 'OBV', 'OBV_MA30', 'MID', 'RSI_24']
            if not all(col in df.columns for col in required_cols) or df[required_cols].iloc[-2:].isnull().values.any():
                warnings.warn("震荡指标所需数据不足或存在NaN值")
                return 10.0 # 数据不全，给予偏低分

            # --- 2. 定义核心前提条件的“铁三角” ---
            # 2.1. 量能配合: OBV在其30周期均线之上
            is_obv_healthy = df['OBV'].iloc[-1] > df['OBV_MA30'].iloc[-1]
            
            # 2.2. 趋势向上: MID(中轨)方向朝上
            is_mid_rising = df['MID'].iloc[-1] > df['MID'].iloc[-2]

            # 2.3. 价格强势: 收盘价在中轨之上
            is_close_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]

            # 如果OBV或MID条件和股价位置条件都满足，降低评分
            if is_obv_healthy and is_mid_rising and is_close_above_mid:
                # 使用较长周期的RSI(24)来判断
                rsi_now = df['RSI_24'].iloc[-1]
                rsi_prev = df['RSI_24'].iloc[-2]
                if 40 < rsi_prev < 62 and rsi_now > rsi_prev:# 黄金回调区 (90-100分)
                    return 100.0
                elif rsi_prev < 40 and rsi_now > rsi_prev:# 深度回调区 (60分)
                    return 80.0
                else:
                    return 30.0# 其他情况，中性分  
            else:    
                return 10.0 # 其他情况，低分
        except Exception as e:
            warnings.warn(f"长线震荡指标得分计算失败: {e}")
            return 10.0



    def _calculate_long_term_volume_score(self, df: pd.DataFrame) -> float:
        """
        组件: 计算长线（周线）成交量得分 (0-100)
        - 核心思路: 结合OBV趋势、VR安全区和量价突破信号，综合评估周线级别的量能健康度。
        - 此版本不再考虑顶背离信号。
        """
        try:
            # --- 1. 数据准备与有效性检查 ---
            # 为确保稳健，长线分析至少需要一年的周线数据（约52周）
            if len(df) < 52:
                warnings.warn("周线数据不足一年，成交量评分可能不准确")
                return 20.0

            # --- 2. 评分逻辑实现 ---
            score = 0.0

            # 规则1: OBV趋势健康度，提供基础分 (满分40分)
            # 使用30周均线来判断OBV的长期趋势
            if 'OBV_MA30' not in df.columns:
                df['OBV_MA30'] = df['OBV'].rolling(window=30, min_periods=20).mean()
            
            if df['OBV'].iloc[-1] > df['OBV_MA30'].iloc[-1]:
                score += 40.0 # OBV在长期均线之上，资金处于流入趋势，给予坚实的基础分

            # 规则2: VR指标判断市场热度，调节分数 (得分范围: -20 至 +30分)
            vr_now = df['VR'].iloc[-1]
            if vr_now < 80:
                score += 20.0 # 低位区，有潜在启动可能，加分
            elif vr_now < 160:
                score += 40.0 # 安全区/温和放量区，最健康的状态，加分最多
            elif vr_now < 200:
                score += 0.0 # 获利区，略有热度，少量加分
            else: # vr_now >= 250
                score -= 30.0 # 警戒区，市场过热，明确扣分以控制风险

            # 规则3: 近期量价突破信号，提供爆发力加分 (满分30分)
            # 3.1 价格突破条件: 最近2周内，收盘价站上布林线上轨的天数 <= 1
            recent_2w_close = df['close'].iloc[-2:]
            recent_2w_upper = df['UPPER'].iloc[-2:]
            breakout_weeks = (recent_2w_close > recent_2w_upper).sum()
            price_breakout_condition = (breakout_weeks <= 1)

            # 3.2 条件2：MACD动量条件 (MACD1>MACD4 或 MACD>0)
            macd1 = df['MACD'].iloc[-1]
            macd4 = df['MACD'].iloc[-4]
            macd_positive = macd1 > 0
            macd_burst = (macd1 > macd4) or macd_positive
            
            # 3.3 条件3：资金流创20日新高 (MFI或OBV或VR任一创20日新高)
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
            conditions_met = sum([price_breakout_condition, macd_burst, fund_flow_breakout])
            
            # 基础分数 + 条件满足的额外分数
            if conditions_met == 3:
                score += 65.0  # 三维共振，最强信号
            elif conditions_met == 2:
                score += 15.0  # 强信号
            elif conditions_met == 1:
                score += 10.0  # 有启动迹象
            # conditions_met == 0 时，score保持原值

            # 返回最终得分 (确保分数在0-100之间)
            return max(0, min(100, score))

        except Exception as e:
            warnings.warn(f"长线成交量得分计算失败: {e}")
            return 10.0 # 异常时给一个中性偏低分

    def _calculate_long_term_volatility_score(self, df: pd.DataFrame) -> float:
        """
        组件4：计算波动率得分 (0-100)
        - 偏好：长期低波动率的稳定攀升。
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

    def _calculate_long_term_momentum_score(self, df: pd.DataFrame) -> float:
        """
        组件5 v3 最终版：计算动量得分 (0-100)
        - 完全基于您的实战经验逻辑：结合MACD柱的状态、加速度和顶背离进行评分。
        - 此版本直接读取您在 zhibiao 函数中定义的 'MACD' 列。
        """
        try:
            # --- 1. 数据准备与有效性检查 ---
            if len(df) < 5 or df['MACD'].iloc[-4:].isnull().any():
                warnings.warn("计算动量得分所需MACD数据不足")
                return 50.0  # 数据不足，给中性分

            # 直接从 'MACD' 列获取柱状体的值
            macd_hist_now = df['MACD'].iloc[-1]
            macd_hist_prev3 = df['MACD'].iloc[-4]  # 3天前的值

            # --- 2. 评分逻辑实现 ---
            score = 0.0

            # 规则1: MACD > 0，给予一个不错的基础分
            if macd_hist_now > 0:
                score = 50.0  # 处于多头动能区，基础分较高
            else:
                score = 30.0  # 处于空头动能区，基础分较低

            # 规则2: 动量在加速，增加分数
            if macd_hist_now > macd_hist_prev3:
                score += 40.0  # 动能增强或空头动能减弱，都是积极信号

            # 规则3: 出现顶背离迹象，明确减分
            # 3.1 检查价格是否强势突破布林线上轨
            price_hits_upper = (df['high'].iloc[-1] > df['UPPER'].iloc[-1]) or \
                            (df['high'].iloc[-2] > df['UPPER'].iloc[-2])
            
            # 3.2 检查动能是否在减弱
            momentum_is_waning = macd_hist_now < macd_hist_prev3

            # 如果价格新高但动能衰竭，则是明确的风险信号
            if price_hits_upper and momentum_is_waning:
                score -= 50.0  # 出现顶背离风险，扣除较多分数

            # --- 3. 返回最终得分 (确保分数在0-100之间) ---
            return max(0, min(100, score))

        except Exception as e:
            warnings.warn(f"长线动量得分计算失败: {e}")
            return 50.0  # 异常时给一个中性分

    # ==================== 总分计算方法 ====================

    def get_long_term_final_score(self, df: pd.DataFrame) -> tuple[float, dict, dict]:
        """
        计算最终的加权总分。
        
        Args:
            df: 包含所有必需技术指标的DataFrame。
            
        Returns:
            - final_score: 最终得分 (0-100)。
            - weights: 使用的权重。
            - category_scores: 各个分类的原始得分。
        """
        category_scores = {
            'trend': self._calculate_long_term_trend_score(df),
            'oscillator': self._calculate_long_term_oscillator_score(df),
            'volume': self._calculate_long_term_volume_score(df),
            'volatility': self._calculate_long_term_volatility_score(df),
            'momentum': self._calculate_long_term_momentum_score(df)
        }
        
        # 确保所有分类得分都不是None，如果是None则使用默认值
        for category in category_scores:
            if category_scores[category] is None:
                category_scores[category] = 30.0  # 默认中性分
                warnings.warn(f"分类 {category} 得分为None，使用默认值30.0")
        
        final_score = sum(
            category_scores[category] * self.base_weights[category]
            for category in self.base_weights
        )
        
        return final_score, self.base_weights, category_scores