import pandas as pd
import numpy as np
import warnings

class LongLongTermTechnicalScorer:
    """
    一个专门用于评估股票超长线技术状态的评分器。
    
    核心理念：
    - 趋势为王：最重视长周期均线和趋势强度指标。
    - 牛市回调：在确认的上升趋势中，寻找理想的增持/买入点。
    - 量价确认：要求成交量指标支撑价格趋势。
    - 容忍超买：在强势市场中，超买不是卖出信号，而是趋势强劲的体现。
    """
    
    def __init__(self):
        """
        初始化基础权重。这些权重明确体现了超长线策略的侧重点。
        """
        self.base_weights = {
            'trend': 0.35,       # 趋势是核心，权重最高
            'oscillator': 0.20,  # 寻找牛市回调买点，权重次之
            'volume': 0.05,      # 量能确认趋势，次要
            'volatility': 0.20,  # 偏好稳定上涨，而非爆炸性波动
            'momentum': 0.20     # 动量作为次要参考，权重最低
        }

    # ==================== 核心评分组件 ====================

    def _calculate_long_long_term_trend_score(self, df: pd.DataFrame) -> float:
        """
        组件1：计算超长线趋势得分 (0-100)
        - 基于MACD1、MID方向、股价位置、PDI/MDI关系的综合评分
        """
        try:
            # 获取所需指标的最新值
            close = df['close'].iloc[-1]
            close_prev = df['close'].iloc[-2]
            middle = df['MID'].iloc[-1]
            middle_prev = df['MID'].iloc[-2]
            upper = df['UPPER'].iloc[-1]
            upper_prev = df['UPPER'].iloc[-2]
            pdi = df['PDI'].iloc[-1]
            pdi_prev = df['PDI'].iloc[-2]
            mdi = df['MDI'].iloc[-1]
            adx = df['ADX'].iloc[-1]
            macd1 = df['MACD'].iloc[-1]  # MACD1等于MACD.iloc[-1]
            macd4 = df['MACD'].iloc[-4]

            # 检查数据有效性
            if pd.isna(macd1) or pd.isna(middle) or pd.isna(close):
                return 20.0

            # 基础变量
            is_mid_down = middle < middle_prev
            is_mid_up = middle > middle_prev
            is_price_below_mid = close < middle
            is_price_above_mid = close > middle
            is_pdi_stronger = pdi > mdi
            is_macd1_strong = macd1 > macd4
            is_macd_positive = macd1 > 0

            # 判断最近2周期股价是否都低于UPPER
            recent_close = df['close'].iloc[-2:]
            recent_upper = df['UPPER'].iloc[-2:]
            is_price_below_upper_2m = (recent_close < recent_upper).all()

            # 判断是否连续2周期（实际是月线）股价在upper以上
            is_price_above_upper_2m = (close > upper) and (close_prev > upper_prev)

            # a. MID向下 + 股价低于MID
            if is_mid_down and is_price_below_mid:
                score = 0.0
                if is_macd1_strong:
                    score += 40.0  # 基础分
                if is_macd_positive:
                    score += 20.0  # 加分
                return min(score, 100.0)

            # b. MID向下 + 股价高于MID + 最近2周期股价都低于UPPER
            elif is_mid_down and is_price_above_mid and is_price_below_upper_2m:
                score = 0.0
                if is_macd1_strong:
                    score += 40.0  # 基础分
                if is_macd_positive:
                    score += 20.0  # 加分
                if pdi * 1.01 > mdi:
                    score += 20.0  # PDI优势
                if pdi > pdi_prev:
                    score += 20.0  # PDI上升
                return min(score, 100.0)

            # c. MID向上 + PDI>MDI + MACD1>4
            elif is_mid_up and is_pdi_stronger and is_macd1_strong:
                if adx < 35 and not is_price_above_upper_2m:
                    return 100.0  # 最高分：大牛市，大牛股
                elif 35 <= adx < 60 and not is_price_above_upper_2m:
                    return 70.0   # 次高分
                else:
                    return 40.0   # 其他情况

            # d. MID向上 + PDI>MDI + MACD1<4
            elif is_mid_up and is_pdi_stronger and not is_macd1_strong:
                return 20.0  # 低分

            # 其他情况
            else:
                return 20.0

        except Exception as e:
            warnings.warn(f"超长线趋势得分计算失败: {e}")
            return 20.0


    def _calculate_long_long_term_oscillator_score(self, df: pd.DataFrame) -> float:
        """
        组件2 v3：计算震荡指标得分 - 聚焦于月线级别的"健康趋势中的回调买点"
        - 核心逻辑: 以月线MID(MA20)为趋势核心，结合OBV和价格位置，寻找RSI回调后的启动点。
        - 该版本完全基于您的实战经验进行调整。
        """
        try:
            # --- 1. 数据准备与有效性检查 ---
            required_cols = ['close', 'ADX', 'PDI', 'MDI', 'MACD', 'MID', 'RSI_24']
            if not all(col in df.columns for col in required_cols) or df[required_cols].iloc[-2:].isnull().values.any():
                warnings.warn("震荡指标所需数据不足或存在NaN值")
                return 10.0 # 数据不全，给予偏低分

            # 2.1. 趋势向上: MID(中轨)方向朝上
            is_mid_rising = df['MID'].iloc[-1] > df['MID'].iloc[-2]

            # 2.2. 价格强势: 收盘价在中轨之上
            is_close_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]
            #adx大于40且pdi>mdi,macd1>0
            is_adx_above_40 = df['ADX'].iloc[-1] > 40
            is_pdi_stronger = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]
            is_macd1_positive = df['MACD'].iloc[-1] > 0

            # 2.3. 价格弱势: 收盘价在中轨之下
            is_close_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]

            # 如果MID条件和股价位置条件都满足，降低评分
            # 使用较长周期的RSI(24)来判断
            rsi_now = df['RSI_24'].iloc[-1]
            rsi_prev = df['RSI_24'].iloc[-2]
            if is_mid_rising and is_close_above_mid:
                if 40 < rsi_prev < 62 and rsi_now > rsi_prev:# 黄金回调区 (90-100分)
                    return 100.0
                elif 40 < rsi_prev < 62:
                    return 60.0
                else:
                    return 30.0
            elif is_adx_above_40 and is_pdi_stronger and is_macd1_positive:
                if 40 < rsi_prev < 62 and rsi_now > rsi_prev:# 黄金回调区 (90-100分)
                    return 90.0
                elif 40 < rsi_prev < 62:
                    return 50.0
                else:
                    return 25.0
            elif is_close_below_mid and rsi_now <50:# 深度回调区 (60分)
                return 30.0
            else:    
                return 15.0 # 其他情况，低分
        except Exception as e:
            warnings.warn(f"长线震荡指标得分计算失败: {e}")
            return 10.0

    def _calculate_long_long_term_volume_score(self, df: pd.DataFrame) -> float:
        """
        组件3：计算超长线成交量得分 (0-100)
        - 基于MFI资金流指标和MACD动量的综合评分
        - 重点关注资金流的健康程度和动量配合
        """
        try:
            # 数据有效性检查
            required_cols = ['MFI', 'MACD']
            if not all(col in df.columns for col in required_cols):
                warnings.warn("成交量指标所需列不存在")
                return 20.0
            
            if df[required_cols].iloc[-2:].isnull().values.any():
                warnings.warn("成交量指标所需数据存在NaN值")
                return 20.0

            # 获取指标值
            mfi_now = df['MFI'].iloc[-1]
            mfi_prev = df['MFI'].iloc[-2]
            macd1 = df['MACD'].iloc[-1]
            macd4 = df['MACD'].iloc[-4]

            # 基础条件判断
            is_mfi_below_50 = mfi_now < 50
            is_mfi_below_70 = mfi_now < 70
            is_mfi_above_80 = mfi_now > 80
            is_mfi_rising = mfi_now > mfi_prev
            is_mfi_falling = mfi_now < mfi_prev
            is_macd1_strong = macd1 > macd4

            # 评分逻辑
            # 最高分：MFI在50以下 + MFI上升 + MACD1>4
            if is_mfi_below_50 and is_mfi_rising and is_macd1_strong:
                return 100.0

            # 高分：MFI在50以下 + MFI上升
            elif is_mfi_below_50 and is_mfi_rising:
                return 85.0

            # 中等分：MFI在70以下 + MFI上升 + MACD1>4
            elif is_mfi_below_70 and is_mfi_rising and is_macd1_strong:
                return 60.0

            # 低分：MFI在80以上 + MFI上升 + MACD1>4
            elif is_mfi_above_80 and is_mfi_rising and is_macd1_strong:
                return 20.0

            # 扣分：MFI下降
            elif is_mfi_falling:
                return 10.0

            # 其他情况：中性分
            else:
                return 20.0

        except Exception as e:
            warnings.warn(f"超长线成交量得分计算失败: {e}")
            return 20.0


    def _calculate_long_long_term_volatility_score(self, df: pd.DataFrame) -> float:
        """
        组件4：计算超长线波动率得分 (0-100)
        - 基于ATR(平均真实波幅)的波动性评分
        - 偏好：超长期低波动率的稳定攀升。
        """
        try:
            # 数据有效性检查
            required_cols = ['ATR', 'close']
            if not all(col in df.columns for col in required_cols):
                warnings.warn("波动率指标所需列不存在")
                return 30.0
            
            if df[required_cols].iloc[-10:].isnull().values.any():
                warnings.warn("波动率指标所需数据存在NaN值")
                return 30.0

            # 获取ATR相关数据
            atr_now = df['ATR'].iloc[-1]
            close_now = df['close'].iloc[-1]
            
            # 计算ATR相对价格的比例 (ATR百分比)
            atr_percentage = (atr_now / close_now) * 100
            
            # 计算ATR的历史分位数 (过去14个周期，适合月线数据)
            atr_14d = df['ATR'].iloc[-14:]
            atr_percentile = (atr_14d < atr_now).sum() / len(atr_14d) * 100
            
            # 计算ATR的短期趋势 (最近5个周期，保持不变)
            atr_5d = df['ATR'].iloc[-5:]
            atr_trend = atr_5d.iloc[-1] / atr_5d.iloc[0] - 1  # 5日变化率
            
            # 评分逻辑 - 强调低波动性
            score = 30.0  # 降低基础分，更严格
            
            # 1. ATR百分比评分 (权重50%) - 强调低波动
            if atr_percentage < 0.8:
                score += 40.0  # 极低波动，大幅加分
            elif atr_percentage < 1.2:
                score += 30.0  # 低波动，大幅加分
            elif atr_percentage < 1.8:
                score += 15.0  # 中等偏低波动
            elif atr_percentage < 2.5:
                score += 0.0   # 中等波动
            elif atr_percentage < 3.5:
                score -= 10.0  # 较高波动，扣分
            else:
                score -= 25.0  # 高波动，大幅扣分
            
            # 2. ATR历史分位数评分 (权重30%) - 基于14周期
            if atr_percentile < 25:
                score += 25.0  # 14周期内低位，大幅加分
            elif atr_percentile < 50:
                score += 10.0  # 14周期内中低位
            elif atr_percentile < 75:
                score += 0.0   # 14周期内中高位
            else:
                score -= 15.0  # 14周期内高位，扣分
            
            # 3. ATR趋势评分 (权重20%) - 短期趋势变化，保持不变
            if atr_trend < -0.1:
                score += 15.0  # ATR下降，波动收缩
            elif atr_trend < 0:
                score += 5.0   # ATR微降
            elif atr_trend < 0.1:
                score += 0.0   # ATR稳定
            elif atr_trend < 0.2:
                score -= 5.0   # ATR微升
            else:
                score -= 15.0  # ATR上升，波动扩张
            
            # 4. 特殊情况处理 - 更严格的低波动要求
            # 如果ATR百分比过高，直接给低分
            if atr_percentage > 4.0:
                score = 15.0
            
            # 如果ATR百分比极低且趋势稳定，给予额外奖励
            if atr_percentage < 0.6 and abs(atr_trend) < 0.05:
                score += 15.0  # 极低波动且稳定，额外加分
            
            # 确保分数在0-100范围内
            return max(0, min(100, score))

        except Exception as e:
            warnings.warn(f"超长线波动率得分计算失败: {e}")
            return 30.0

    def _calculate_long_long_term_momentum_score(self, df: pd.DataFrame) -> float:
        """
        组件5 v3 最终版：计算动量得分 (0-100)
        - 完全基于您的实战经验逻辑：结合MACD柱的状态、加速度和顶背离进行评分。
        - 此版本直接读取您在 zhibiao 函数中定义的 'MACD' 列。
        """
        try:
            # --- 1. 数据准备与有效性检查 ---
            required_cols = ['MACD', 'close', 'high', 'UPPER', 'MID']
            if not all(col in df.columns for col in required_cols):
                warnings.warn("动量指标所需列不存在")
                return 40.0
            
            if len(df) < 5 or df[required_cols].iloc[-4:].isnull().any():
                warnings.warn("计算动量得分所需数据不足")
                return 40.0

            # 直接从 'MACD' 列获取柱状体的值
            macd_hist_now = df['MACD'].iloc[-1]
            macd_hist_prev3 = df['MACD'].iloc[-4]  # 3天前的值

            # --- 2. 评分逻辑实现 ---
            score = 0.0

            # 规则1: MACD > 0，给予一个不错的基础分
            if macd_hist_now > 0:
                score = 30.0  # 处于多头动能区，基础分较高
            else:
                score = 20.0  # 处于空头动能区，基础分较低

            # 规则2: 动量在加速，增加分数
            if macd_hist_now > macd_hist_prev3:
                score += 40.0  # 动能增强或空头动能减弱，都是积极信号
            
            # 规则3: 检查股价低于MID
            is_close_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]
            if is_close_below_mid:
                score += 20.0  # 股价低于MID，加分

            # 规则4: 出现顶背离迹象，明确减分
            # 4.1 检查价格是否强势突破布林线上轨
            price_hits_upper = (df['high'].iloc[-1] > df['UPPER'].iloc[-1]) or \
                            (df['high'].iloc[-2] > df['UPPER'].iloc[-2])
            
            # 4.2 检查动能是否在减弱
            momentum_is_waning = macd_hist_now < macd_hist_prev3


            # 如果价格新高但动能衰竭，则是明确的风险信号
            if price_hits_upper and momentum_is_waning:
                score -= 50.0  # 2个条件都成立，扣除50分
            elif price_hits_upper or momentum_is_waning:
                score -= 25.0  # 只有1个条件成立，扣除25分
            

            # --- 3. 返回最终得分 (确保分数在0-100之间) ---
            return max(0, min(100, score))

        except Exception as e:
            warnings.warn(f"长线动量得分计算失败: {e}")
            return 40.0  # 异常时给一个中性分

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
            'trend': self._calculate_long_long_term_trend_score(df),
            'oscillator': self._calculate_long_long_term_oscillator_score(df),
            'volume': self._calculate_long_long_term_volume_score(df),
            'volatility': self._calculate_long_long_term_volatility_score(df),
            'momentum': self._calculate_long_long_term_momentum_score(df)
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