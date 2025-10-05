from ast import And
from pickle import FALSE
import adata
import os
import pandas as pd
import numpy as np
from pandas.core.nanops import F
from core.utils.indicators import *
import datetime
import time
from data_management.data_processor import get_monthly_data_for_backtest, get_weekly_data_for_backtest, get_daily_data_for_backtest, update_and_load_data_daily, update_and_load_data_weekly, update_and_load_data_monthly


class TechnicalAnalyzer:
    def __init__(self, data_dict: dict):
        """
        纯粹的分析器：在初始化时，直接接收一个包含所有周期DataFrame的字典。
        它不再关心数据是如何被加载的。
        """
        # print("--- 分析器已创建，接收到外部注入的数据 ---")
        
        # 从传入的字典中获取数据并计算指标
        self.df_monthly = zhibiao(data_dict.get('monthly', pd.DataFrame()))
        self.df_weekly = zhibiao(data_dict.get('weekly', pd.DataFrame()))
        self.df_daily = zhibiao(data_dict.get('daily', pd.DataFrame()))
        
        # # 也可以接收基本面数据
        # self.fundamentals = data_dict.get('fundamentals', None)
        
        # print("--- 指标计算完成，分析器准备就绪 ---")

    # ------------------------------------------------------------------
    # 所有的分析方法，如 is_in_long_term_uptrend, get_10m_return 等
    # ...都保持原样，完全不需要改动！...
    # ------------------------------------------------------------------
    #超长线技术状态。---------
    def ccx_jjdi(self) -> bool:
        """
        判断是否进入"超长线接近底部"的技术状态。
        该函数使用月线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ、MACD 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_monthly  # 使用月线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'K', 'MACD', 'J', 'ADX', 'PDI', 'MDI', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_monthly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ、MACD 和 DMI。")
            return False

        # --- 条件1：超长线趋势调整接近底部 ---
        # 描述: MID连续3周期下降 + boll带宽扩大[-1]>[-2] + 股价低于lower + K<20连续3个周期 + MACD<0+MACD[-1]<[-4] + J连续2周期<10
        cond1_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond1_price_below_lower = df['close'].iloc[-1] < df['LOWER'].iloc[-1]  # 股价低于lower
        cond1_k_oversold_3m = (df['K'] < 20).iloc[-3:].all()  # K<20连续3个周期
        cond1_macd_neg = df['MACD'].iloc[-1] < 0  # MACD<0
        cond1_macd_weak = df['MACD'].iloc[-1] < df['MACD'].iloc[-4]  # MACD[-1]<[-4]
        cond1_j_oversold_2m = (df['J'] < 10).iloc[-2:].all()  # J连续2周期<10
        
        condition1 = (cond1_mid_down_3m and cond1_bandwidth_expand and cond1_price_below_lower and 
                     cond1_k_oversold_3m and cond1_macd_neg and cond1_macd_weak and cond1_j_oversold_2m)

        # --- 条件2：超长线趋势调整接近底部（变体1） ---
        # 描述: MID连续3周期下降 + boll带宽扩大[-1]>[-2] + 股价低于MID + MACD<0+MACD[-1]>[-4]
        cond2_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond2_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond2_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价低于MID
        cond2_macd_neg = df['MACD'].iloc[-1] < 0  # MACD<0
        cond2_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        
        condition2 = (cond2_mid_down_3m and cond2_bandwidth_expand and cond2_price_below_mid and 
                     cond2_macd_neg and cond2_macd_turn_up)

        # --- 条件3：超长线趋势调整接近底部（变体2） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价低于MID + MACD>0
        cond3_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond3_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond3_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价低于MID
        cond3_macd_pos = df['MACD'].iloc[-1] > 0  # MACD>0
        
        condition3 = cond3_mid_down_3m and cond3_bandwidth_shrink and cond3_price_below_mid and cond3_macd_pos

        # --- 条件4：超长线趋势调整接近底部（变体3） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价低于MID + MACD<0+MACD[-1]>[-4]
        cond4_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond4_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond4_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价低于MID
        cond4_macd_neg = df['MACD'].iloc[-1] < 0  # MACD<0
        cond4_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        
        condition4 = (cond4_mid_down_3m and cond4_bandwidth_shrink and cond4_price_below_mid and 
                     cond4_macd_neg and cond4_macd_turn_up)

        # --- 条件5：长线调整收缩型调整接近底部 ---
        # 描述: MID连续3周上升 + BOLL带宽缩小[-1]<[-2] + 股价< MID
        cond5_mid_up_3m = (df['MID'].diff() > 0).iloc[-3:].all()  # MID连续3周上升
        cond5_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond5_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        
        condition5 = cond5_mid_up_3m and cond5_bandwidth_shrink and cond5_price_below_mid

        # --- 条件6：长线多头折返调整接近底部 ---
        # 描述: MID连续3周上升 + BOLL带宽缩小[-1]<[-2] + ADX[-1]<[-2] + MDI>PDI
        cond6_mid_up_3m = (df['MID'].diff() > 0).iloc[-3:].all()  # MID连续3周上升
        cond6_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond6_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<[-2]
        cond6_mdi_gt_pdi = df['MDI'].iloc[-1] > df['PDI'].iloc[-1]  # MDI>PDI
        
        condition6 = cond6_mid_up_3m and cond6_bandwidth_shrink and cond6_adx_down and cond6_mdi_gt_pdi

        # --- 最终判断：满足任一条件 ---
        is_ultra_long_term_bottom = (condition1 or condition2 or condition3 or condition4 or 
                                   condition5 or condition6)

        # (可选的调试信息)
        # if is_ultra_long_term_bottom:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("超长线趋势调整接近底部（完整版）")
        #     if condition2: triggered_conditions.append("超长线趋势调整接近底部（变体1）")
        #     if condition3: triggered_conditions.append("超长线趋势调整接近底部（变体2）")
        #     if condition4: triggered_conditions.append("超长线趋势调整接近底部（变体3）")
        #     if condition5: triggered_conditions.append("长线调整收缩型调整接近底部")
        #     if condition6: triggered_conditions.append("长线多头折返调整接近底部")
        #     print(f"触发了超长线接近底部条件: {', '.join(triggered_conditions)}")

        return is_ultra_long_term_bottom
    
    #超长线底部
    def ccx_di(self) -> bool:
        """
        判断是否进入"超长线底部"的技术状态。
        该函数使用月线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ、MACD 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_monthly  # 使用月线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'MACD', 'K', 'J', 'PDI', 'MDI', 'VOL_5', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_monthly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ、MACD 和 DMI。")
            return False

        # --- 条件1：超长线趋势调整底部 ---
        # 描述: MID连续3周期下降 + boll带宽扩大[-1]>[-2] + 股价< MID + MACD[-1]<0+MACD[-1]>MACD[-4]+vol_5[-1]>[-2]
        cond1_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond1_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond1_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        cond1_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        cond1_vol_up = df['VOL_5'].iloc[-1] > df['VOL_5'].iloc[-2]  # vol_5[-1]>[-2]
        
        condition1 = (cond1_mid_down_3m and cond1_bandwidth_expand and cond1_price_below_mid and 
                     cond1_macd_neg and cond1_macd_turn_up and cond1_vol_up)

        # --- 条件2：超长线调整收缩型调整底部（变体1） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价< MID + MACD[-1]<0 + MACD[-1]>MACD[-4]
        cond2_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond2_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond2_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond2_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        cond2_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition2 = (cond2_mid_down_3m and cond2_bandwidth_shrink and cond2_price_below_mid and 
                     cond2_macd_neg and cond2_macd_turn_up)

        # --- 条件3：超长线调整收缩型调整底部（变体2） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价< MID + PDI>MDI
        cond3_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond3_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond3_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond3_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        
        condition3 = cond3_mid_down_3m and cond3_bandwidth_shrink and cond3_price_below_mid and cond3_pdi_gt_mdi

        # --- 条件4：超长线调整收缩型调整底部（变体3） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价< MID + MACD[-1]>0 + MACD[-1]>MACD[-4]
        cond4_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond4_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond4_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond4_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond4_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition4 = (cond4_mid_down_3m and cond4_bandwidth_shrink and cond4_price_below_mid and 
                     cond4_macd_pos and cond4_macd_turn_up)

        # --- 条件5：超长线调整收缩型调整底部（变体4） ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价< MID + 最近3周期K<20 + K[-1]>K[-2]
        cond5_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond5_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond5_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond5_k_oversold_3m = (df['K'] < 20).iloc[-3:].sum() >= 1  # 最近3周期K<20
        cond5_k_turn_up = df['K'].iloc[-1] > df['K'].iloc[-2]  # K[-1]>K[-2]
        
        condition5 = (cond5_mid_down_3m and cond5_bandwidth_shrink and cond5_price_below_mid and 
                     cond5_k_oversold_3m and cond5_k_turn_up)

        # --- 条件6：超长线多头折返调整底部 ---
        # 描述: MID连续2周上升 + BOLL带宽缩小[-1]<[-2] + (MACD<0+MACD[-1]>[-4] or 最近3周期K<20+J[-1]>J[-2])
        cond6_mid_up_2m = (df['MID'].diff() > 0).iloc[-2:].all()  # MID连续2周上升
        cond6_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond6_macd_neg = df['MACD'].iloc[-1] < 0  # MACD<0
        cond6_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        cond6_macd_condition = cond6_macd_neg and cond6_macd_turn_up  # MACD<0+MACD[-1]>[-4]
        cond6_k_oversold_3m = (df['K'] < 20).iloc[-3:].sum() >= 1  # 最近3周期K<20
        cond6_j_turn_up = df['J'].iloc[-1] > df['J'].iloc[-2]  # J[-1]>J[-2]
        cond6_kdj_condition = cond6_k_oversold_3m and cond6_j_turn_up  # 最近3周期K<20+J[-1]>J[-2]
        
        condition6 = cond6_mid_up_2m and cond6_bandwidth_shrink and (cond6_macd_condition or cond6_kdj_condition)

        # --- 最终判断：满足任一条件 ---
        is_ultra_long_term_bottom = (condition1 or condition2 or condition3 or condition4 or 
                                   condition5 or condition6)

        # (可选的调试信息)
        # if is_ultra_long_term_bottom:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("超长线趋势调整底部")
        #     if condition2: triggered_conditions.append("超长线调整收缩型调整底部（变体1）")
        #     if condition3: triggered_conditions.append("超长线调整收缩型调整底部（变体2）")
        #     if condition4: triggered_conditions.append("超长线调整收缩型调整底部（变体3）")
        #     if condition5: triggered_conditions.append("超长线调整收缩型调整底部（变体4）")
        #     if condition6: triggered_conditions.append("超长线多头折返调整底部")
        #     print(f"触发了超长线底部条件: {', '.join(triggered_conditions)}")

        return is_ultra_long_term_bottom
    #超长线多头刚
    def ccxdtg(self) -> bool:
        """
        判断是否进入"超长线底部拐点"的技术状态。
        该函数使用月线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ、MACD 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_monthly  # 使用月线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'K', 'MACD', 'ADX', 'PDI', 'MDI', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_monthly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ、MACD 和 DMI。")
            return False

        # --- 条件1：超长线趋势调整后的折返拉抬多头刚（反弹） ---
        # 描述: MID连续3周期下降 + 股价< MID + K[-1]>K[-2]+K[-1]<35 + (MACD[-1]>0 OR MACD[-1]>MACD[-4])
        cond1_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond1_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond1_k_turn_up = df['K'].iloc[-1] > df['K'].iloc[-2]  # K[-1]>K[-2]
        cond1_k_low = df['K'].iloc[-1] < 35  # K[-1]<35
        cond1_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond1_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        cond1_macd_condition = cond1_macd_pos or cond1_macd_turn_up  # MACD[-1]>0 OR MACD[-1]>MACD[-4]
        
        condition1 = (cond1_mid_down_3m and cond1_price_below_mid and cond1_k_turn_up and 
                     cond1_k_low and cond1_macd_condition)

        # --- 条件2：超长线趋势调整后的折返拉抬多头刚（变体） ---
        # 描述: MID连续3周期下降 + 股价< MID + ADX[-1]>ADX[-2] + PDI>MDI
        cond2_mid_down_3m = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond2_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond2_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>ADX[-2]
        cond2_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        
        condition2 = cond2_mid_down_3m and cond2_price_below_mid and cond2_adx_up and cond2_pdi_gt_mdi

        # --- 条件3：超长线调整收缩型调整后多头刚 ---
        # 描述: MID连续2周期上升 + 股价<UPPER + ADX[-1]>ADX[-2] + PDI>MDI + MACD[-1]>MACD[-4]
        cond3_mid_up_2m = (df['MID'].diff() > 0).iloc[-2:].all()  # MID连续2周期上升
        cond3_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价<UPPER
        cond3_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>ADX[-2]
        cond3_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond3_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition3 = (cond3_mid_up_2m and cond3_price_below_upper and cond3_adx_up and 
                     cond3_pdi_gt_mdi and cond3_macd_turn_up)

        # --- 最终判断：满足任一条件 ---
        is_ultra_long_term_bottom_turning = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_ultra_long_term_bottom_turning:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("超长线趋势调整后折返拉抬多头刚（反弹）")
        #     if condition2: triggered_conditions.append("超长线趋势调整后折返拉抬多头刚（变体）")
        #     if condition3: triggered_conditions.append("超长线调整收缩型调整后多头刚")
        #     print(f"触发了超长线底部拐点条件: {', '.join(triggered_conditions)}")

        return is_ultra_long_term_bottom_turning
    #超长线多头中
    def ccxdtz(self) -> bool:
        """
        判断是否进入"超长线底部转折"的技术状态。
        该函数使用月线数据，依赖于 zhibiao 函数计算出的 BOLL、MACD、KDJ、DMI 和 ATR 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_monthly  # 使用月线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'MACD', 'ADX', 'PDI', 'MDI', 'K', 'D', 'J', 'ATR', 'TR', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_monthly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、MACD、KDJ、DMI 和 ATR。")
            return False

        # --- 条件1：超长线趋势调整后的折返拉抬多头中（均值回归） ---
        # 描述: MID在最近10周期下降的数量>上涨的数量+起码有1个是下降+ MID[-1]>[-2] + 股价> MID + 股价< UPPER + MACD[-1]>[-4] + ADX[-1]<[-2]
        # 计算最近10周期MID方向
        mid_direction_10m = (df['MID'].diff() > 0).iloc[-10:]  # 多头为True，空头为False
        cond1_mid_bearish_10m = mid_direction_10m.sum() < 5  # 下降数量>上涨数量（下降数量>5）
        cond1_mid_has_down = (df['MID'].diff() < 0).iloc[-10:].sum() >= 1  # 起码有1个是下降
        cond1_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        cond1_price_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]  # 股价> MID
        cond1_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价< UPPER
        cond1_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        cond1_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<[-2]
        
        condition1 = (cond1_mid_bearish_10m and cond1_mid_has_down and cond1_mid_up and 
                     cond1_price_above_mid and cond1_price_below_upper and cond1_macd_up and cond1_adx_down)

        # --- 条件2：超长线调整收缩型调整后多头中（变体1） ---
        # 描述: MID在最近8周期全是上升 + boll带宽扩大[-1]>[-2] + ADX[-1]>[-2] + PDI>MDI + MACD[-1]>[-4] + 最近3周期都是ATR<TR*1.6
        cond2_mid_up_8m = (df['MID'].diff() > 0).iloc[-8:].all()  # MID在最近8周期全是上升
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond2_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond2_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond2_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond2_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        cond2_atr_low = (df['ATR'] < df['TR'] * 1.6).iloc[-3:].all()  # 最近3周期都是ATR<TR*1.6
        
        condition2 = (cond2_mid_up_8m and cond2_bandwidth_expand and cond2_adx_up and 
                     cond2_pdi_gt_mdi and cond2_macd_up and cond2_atr_low)

        # --- 条件3：超长线调整收缩型调整后多头中（变体2） ---
        # 描述: MID在最近8周期全是上升 + boll带宽扩大[-1]>[-2] + ADX[-1]>[-2] + ADX[-1]<50 + PDI>MDI + MACD[-1]>[-4]
        cond3_mid_up_8m = (df['MID'].diff() > 0).iloc[-8:].all()  # MID在最近8周期全是上升
        cond3_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond3_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond3_adx_low = df['ADX'].iloc[-1] < 50  # ADX[-1]<50
        cond3_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond3_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        
        condition3 = (cond3_mid_up_8m and cond3_bandwidth_expand and cond3_adx_up and 
                     cond3_adx_low and cond3_pdi_gt_mdi and cond3_macd_up)

        # --- 条件4：超长线多头强势拉抬的中途回落 ---
        # 描述: MID[-1]>[-2] + boll带宽扩大[-1]>[-2] + ADX[-1]>[-2] + ADX>50 + PDI>MDI + MACD[-1]>0 + 最近6周期有1个K>80或J>100 + K[-1]<D[-1]
        cond4_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        cond4_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond4_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond4_adx_high = df['ADX'].iloc[-1] > 50  # ADX>50
        cond4_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond4_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond4_k_high = (df['K'] > 80).iloc[-6:].sum() >= 1  # 最近6周期有1个K>80
        cond4_j_high = (df['J'] > 100).iloc[-6:].sum() >= 1  # 最近6周期有1个J>100
        cond4_kdj_overbought = cond4_k_high or cond4_j_high  # K>80或J>100
        cond4_k_less_d = df['K'].iloc[-1] < df['D'].iloc[-1]  # K[-1]<D[-1]
        
        condition4 = (cond4_mid_up and cond4_bandwidth_expand and cond4_adx_up and cond4_adx_high and 
                     cond4_pdi_gt_mdi and cond4_macd_pos and cond4_kdj_overbought and cond4_k_less_d)

        # --- 最终判断：满足任一条件 ---
        is_ultra_long_term_bottom_turning = condition1 or condition2 or condition3 or condition4

        # (可选的调试信息)
        # if is_ultra_long_term_bottom_turning:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("超长线趋势调整后折返拉抬多头中（均值回归）")
        #     if condition2: triggered_conditions.append("超长线调整收缩型调整后多头中（变体1）")
        #     if condition3: triggered_conditions.append("超长线调整收缩型调整后多头中（变体2）")
        #     if condition4: triggered_conditions.append("超长线多头强势拉抬的中途回落")
        #     print(f"触发了超长线底部转折条件: {', '.join(triggered_conditions)}")

        return is_ultra_long_term_bottom_turning
    
    #长线接近底部
    def cx_jjdi(self) -> bool:
        """
        判断是否进入"长线接近底部"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ、MACD 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'K', 'MACD', 'J', 'ADX', 'PDI', 'MDI', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ、MACD 和 DMI。")
            return False

        # BOLL带宽计算（条件2和条件3需要）
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        
        # --- 条件1：长线趋势调整接近底部 ---
        # 描述: ADX[-1]<ADX[-2] + MDI>PDI + （K<20 or J <10 or K<D）
        cond1_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<ADX[-2]
        cond1_mdi_gt_pdi = df['MDI'].iloc[-1] > df['PDI'].iloc[-1]  # MDI>PDI
        cond1_k_oversold = df['K'].iloc[-1] < 20  # K<20
        cond1_j_oversold = df['J'].iloc[-1] < 10  # J<10
        cond1_k_lt_d = df['K'].iloc[-1] < df['D'].iloc[-1]  # K<D
        cond1_kdj_condition = cond1_k_oversold or cond1_j_oversold or cond1_k_lt_d  # （K<20 or J <10 or K<D）
        
        condition1 = cond1_adx_down and cond1_mdi_gt_pdi and cond1_kdj_condition

        # --- 条件2：长线调整收缩型调整接近底部 ---
        # 描述: MID连续3周上升 + BOLL带宽缩小[-1]<[-2] + 股价< MID
        cond2_mid_up_3w = (df['MID'].diff() > 0).iloc[-3:].all()  # MID连续3周上升
        cond2_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond2_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        
        condition2 = cond2_mid_up_3w and cond2_bandwidth_shrink and cond2_price_below_mid

        # --- 条件3：长线多头折返调整接近底部 ---
        # 描述: MID连续3周上升 + BOLL带宽缩小[-1]<[-2] + ADX[-1]<[-2] + MDI>PDI
        cond3_mid_up_3w = (df['MID'].diff() > 0).iloc[-3:].all()  # MID连续3周上升
        cond3_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond3_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<[-2]
        cond3_mdi_gt_pdi = df['MDI'].iloc[-1] > df['PDI'].iloc[-1]  # MDI>PDI
        
        condition3 = cond3_mid_up_3w and cond3_bandwidth_shrink and cond3_adx_down and cond3_mdi_gt_pdi

        # --- 最终判断：满足任一条件 ---
        is_long_term_bottom = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_long_term_bottom:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("长线趋势调整接近底部")
        #     if condition2: triggered_conditions.append("长线调整收缩型调整接近底部")
        #     if condition3: triggered_conditions.append("长线多头折返调整接近底部")
        #     print(f"触发了长线接近底部条件: {', '.join(triggered_conditions)}")

        return is_long_term_bottom


        #长线底部区域
    def cx_di(self) -> bool:
        """
        判断是否进入"长线底部"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ 和 MACD 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'K', 'MACD', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ 和 MACD。")
            return False

        # BOLL带宽计算（条件2和条件3需要）
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        
        # --- 条件1：长线趋势调整底部 ---
        # 描述: MID连续3周期下降 + ADX[-1]<ADX[-2] + MDI>PDI + 股价<MID+ ( K[-1]>K[-2] OR  (MACD[-1]>MACD[-4] + MACD[-1]<0)）
        cond1_mid_down_3w = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond1_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<ADX[-2]
        cond1_mdi_gt_pdi = df['MDI'].iloc[-1] > df['PDI'].iloc[-1]  # MDI>PDI
        cond1_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价<MID
        cond1_k_turn_up = df['K'].iloc[-1] > df['K'].iloc[-2]  # K[-1]>K[-2]
        cond1_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        cond1_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        cond1_k_or_macd = cond1_k_turn_up or (cond1_macd_turn_up and cond1_macd_neg)  # ( K[-1]>K[-2] OR  (MACD[-1]>MACD[-4] + MACD[-1]<0)）
        
        condition1 = (cond1_mid_down_3w and cond1_adx_down and cond1_mdi_gt_pdi and 
                      cond1_price_below_mid and cond1_k_or_macd)

        # --- 条件2：长线调整收缩型调整底部 ---
        # 描述: MID连续3周期下降 + boll带宽缩小[-1]<[-2] + 股价< MID + MACD[-1]<0 + MACD[-1]>MACD[-4]
        cond2_mid_down_3w = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond2_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond2_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond2_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        cond2_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition2 = (cond2_mid_down_3w and cond2_bandwidth_shrink and cond2_price_below_mid and 
                     cond2_macd_neg and cond2_macd_turn_up)

        # --- 条件3：长线多头折返调整底部 ---
        # 描述: MID连续2周上升 + BOLL带宽缩小[-1]<[-2] + MACD<0 + MACD[-1]>[-4]
        cond3_mid_up_2w = (df['MID'].diff() > 0).iloc[-2:].all()  # MID连续2周上升
        cond3_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond3_macd_neg = df['MACD'].iloc[-1] < 0  # MACD<0
        cond3_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        
        condition3 = cond3_mid_up_2w and cond3_bandwidth_shrink and cond3_macd_neg and cond3_macd_turn_up

        # --- 条件4：长线调整收缩型调整底部（增强版） ---
        # 描述: MID连续2周上升 + 最近20周期中有1个周期出现（ADX>50 + PDI>MDI） + ADX[-1]<ADX[-2] + MDI<PDI + MACD[-1]<0.+MACD[-1]>MACD[-4]
        cond4_mid_up_2w = (df['MID'].diff() > 0).iloc[-2:].all()  # MID连续2周上升
        # 最近20周期中有1个周期出现（ADX>50 + PDI>MDI）
        lookback_period = 20
        # 直接检查最近20个周期的数据，避免使用rolling函数
        recent_data = df.tail(lookback_period)
        cond4_historical_condition = ((recent_data['ADX'] > 50) & (recent_data['PDI'] > recent_data['MDI'])).any()
        cond4_mdi_gt_pdi = df['MDI'].iloc[-1] > df['PDI'].iloc[-1]  # MDI>PDI
        cond4_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        cond4_macd_turn_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4] or df['K'].iloc[-1] > df['K'].iloc[-2]  # MACD[-1]>MACD[-4] or K[-1]>K[-2]
        
        condition4 = (cond4_mid_up_2w and cond4_historical_condition and 
                     cond4_mdi_gt_pdi and cond4_macd_neg and cond4_macd_turn_up)

        # --- 条件5：长线趋势调整底部（增强版） ---
        # 描述: MID连续3周期下降 + ADX[-1]>ADX[-2] + PDI>MDI + ADX<25+ 股价< upper+ MACD[-1]<0
        cond5_mid_down_3w = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        cond5_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>ADX[-2]
        cond5_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond5_adx_low = df['ADX'].iloc[-1] < 25  # ADX<25
        cond5_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价< upper
        cond5_macd_neg = df['MACD'].iloc[-1] < 0  # MACD[-1]<0
        
        condition5 = (cond5_mid_down_3w and cond5_adx_up and cond5_pdi_gt_mdi and 
                     cond5_adx_low and cond5_price_below_upper and cond5_macd_neg)

        # --- 最终判断：满足任一条件 ---
        is_long_term_bottom = condition1 or condition2 or condition3 or condition4 or condition5

        # (可选的调试信息)
        # if is_long_term_bottom:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("长线趋势调整底部")
        #     if condition2: triggered_conditions.append("长线调整收缩型调整底部")
        #     if condition3: triggered_conditions.append("长线多头折返调整底部")
        #     if condition4: triggered_conditions.append("长线调整收缩型调整底部（增强版）")
        #     if condition5: triggered_conditions.append("长线趋势调整底部（增强版）")
        #     print(f"触发了长线底部条件: {', '.join(triggered_conditions)}")

        return is_long_term_bottom
        #长线多头刚
    def cxdtg(self) -> bool:
        """
        判断是否进入"长线底部拐点"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、MACD 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'MACD', 'ADX', 'PDI', 'MDI', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、MACD 和 DMI。")
            return False

        # --- 条件1：长线趋势调整后的折返拉抬多头反弹 ---
        # 描述: MID连续3周期下降 + boll带宽扩大[-1]>[-2] + 股价< MID + MACD[-1]>0 + MACD[-1]>MACD[-4]
        cond1_mid_down_3w = (df['MID'].diff() < 0).iloc[-3:].all()  # MID连续3周期下降
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond1_price_below_mid = df['close'].iloc[-1] < df['MID'].iloc[-1]  # 股价< MID
        cond1_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond1_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition1 = (cond1_mid_down_3w and cond1_bandwidth_expand and cond1_price_below_mid and 
                     cond1_macd_pos and cond1_macd_up)

        # --- 条件2：长线调整收缩型调整后多头刚 ---
        # 描述: 最近20周期MID空头数量>多头数量 + MID最近3周期有1周期下降 + boll带宽缩小[-1]<[-2] + 股价< UPPER + MACD[-1]>0 + MACD[-1]>MACD[-4]
        # 计算最近20周期MID多头/空头数量
        mid_direction_20w = (df['MID'].diff() > 0).iloc[-20:]  # 多头为True，空头为False
        cond2_mid_bearish_20w = mid_direction_20w.sum() < 10  # 空头数量>多头数量（空头数量>10）
        cond2_mid_down_1w = (df['MID'].diff() < 0).iloc[-3:].sum() >= 1  # MID最近3周期有1周期下降
        cond2_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond2_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价< UPPER
        cond2_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond2_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]
        
        condition2 = (cond2_mid_bearish_20w and cond2_mid_down_1w and cond2_bandwidth_shrink and 
                     cond2_price_below_upper and cond2_macd_pos and cond2_macd_up)

        # --- 条件3：长线多头折返调整后多头刚 ---
        # 描述: 最近20周期MID多头数量>空头数量 + boll带宽缩小[-1]<[-2] + 股价< UPPER + ADX[-1]>[-2] + ADX<30 + PDI>MDI
        cond3_mid_bullish_20w = mid_direction_20w.sum() > 12  # 多头数量>空头数量（多头数量>12）
        cond3_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽缩小
        cond3_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价< UPPER
        cond3_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond3_adx_low = df['ADX'].iloc[-1] < 30  # ADX<30
        cond3_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        
        condition3 = (cond3_mid_bullish_20w and cond3_bandwidth_shrink and cond3_price_below_upper and 
                     cond3_adx_up and cond3_adx_low and cond3_pdi_gt_mdi)

        # --- 最终判断：满足任一条件 ---
        is_long_term_bottom_turning = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_long_term_bottom_turning:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("长线趋势调整后折返拉抬多头反弹")
        #     if condition2: triggered_conditions.append("长线调整收缩型调整后多头刚")
        #     if condition3: triggered_conditions.append("长线多头折返调整后多头刚")
        #     print(f"触发了长线底部拐点条件: {', '.join(triggered_conditions)}")

        return is_long_term_bottom_turning
    #长线多头中
    def cxdtz(self) -> bool:
        """
        判断是否进入"长线底部转折"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、MACD、KDJ 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'MACD', 'ADX', 'PDI', 'MDI', 'K', 'D', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、MACD、KDJ 和 DMI。")
            return False

        # --- 条件1：长线趋势调整后的折返拉抬多头中（均值回归） ---
        # 描述: MID在最近10周期下降的数量>上涨的数量+起码有1个是下降+ MID[-1]>[-2] + 股价> MID + 股价< UPPER + MACD[-1]>[-4] + ADX[-1]<[-2]
        # 计算最近10周期MID方向
        mid_direction_10w = (df['MID'].diff() > 0).iloc[-10:]  # 多头为True，空头为False
        cond1_mid_bearish_10w = mid_direction_10w.sum() < 5  # 下降数量>上涨数量（下降数量>5）
        cond1_mid_has_down = (df['MID'].diff() < 0).iloc[-10:].sum() >= 1  # 起码有1个是下降
        cond1_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        cond1_price_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]  # 股价> MID
        cond1_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价< UPPER
        cond1_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>[-4]
        cond1_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<[-2]
        
        condition1 = (cond1_mid_bearish_10w and cond1_mid_has_down and cond1_mid_up and 
                     cond1_price_above_mid and cond1_price_below_upper and cond1_macd_up and cond1_adx_down)

        # --- 条件2：长线调整收缩型调整后多头中 ---
        # 描述: MID[-1]>[-2] + boll带宽扩大[-1]>[-2] + ADX[-1]>[-2] + ADX<50 + PDI>MDI
        cond2_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond2_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond2_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond2_adx_low = df['ADX'].iloc[-1] < 50  # ADX<50
        cond2_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        
        condition2 = cond2_mid_up and cond2_bandwidth_expand and cond2_adx_up and cond2_adx_low and cond2_pdi_gt_mdi

        # --- 条件3：长线多头强势拉抬的中途回落 ---
        # 描述: MID[-1]>[-2] + boll带宽扩大[-1]>[-2] + ADX[-1]>[-2] + ADX>50 + PDI>MDI + MACD[-1]>0 + K<D
        cond3_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        cond3_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond3_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[-1]>[-2]
        cond3_adx_high = df['ADX'].iloc[-1] > 50  # ADX>50
        cond3_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI>MDI
        cond3_macd_pos = df['MACD'].iloc[-1] > 0  # MACD[-1]>0
        cond3_k_less_d = df['K'].iloc[-1] < df['D'].iloc[-1]  # K<D
        
        condition3 = (cond3_mid_up and cond3_bandwidth_expand and cond3_adx_up and cond3_adx_high and 
                     cond3_pdi_gt_mdi and cond3_macd_pos and cond3_k_less_d)

        # --- 最终判断：满足任一条件 ---
        is_long_term_bottom_turning = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_long_term_bottom_turning:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("长线趋势调整后折返拉抬多头中（均值回归）")
        #     if condition2: triggered_conditions.append("长线调整收缩型调整后多头中")
        #     if condition3: triggered_conditions.append("长线多头强势拉抬的中途回落")
        #     print(f"触发了长线底部转折条件: {', '.join(triggered_conditions)}")

        return is_long_term_bottom_turning
    def cxqs_ding(self) -> bool:
        """
        判断是否进入"长线趋势顶部"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、KDJ、DMI 和 ATR 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'K', 'J', 'ADX', 'high', 'close', 'TR', 'ATR']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、KDJ、DMI 和 ATR。")
            return False

        # --- 条件1：长线趋势调整后的折返拉抬（均值回归）顶部 ---
        # 描述: MID在最近2周期都是下降 + 最高价 > UPPER在最近2周期有1个 + 股价> MID + 最近3周期K>75或J值大于95
        cond1_mid_down_2w = (df['MID'].diff() < 0).iloc[-2:].all()  # MID在最近2周期都是下降
        cond1_high_above_upper = (df['high'] > df['UPPER']).iloc[-2:].sum() >= 1  # 最高价 > UPPER在最近2周期有1个
        cond1_price_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]  # 股价> MID
        cond1_k_high = (df['K'] > 75).iloc[-3:].sum() >= 1  # 最近3周期K>75
        cond1_j_high = (df['J'] > 95).iloc[-3:].sum() >= 1  # 最近3周期J>95
        cond1_kdj_overbought = cond1_k_high or cond1_j_high  # K>75或J>95
        
        condition1 = (cond1_mid_down_2w and cond1_high_above_upper and cond1_price_above_mid and cond1_kdj_overbought)

        # --- 条件2：超高的ADX ---
        # 描述: MID[-1]>[-2] + boll带宽扩大[-1]>[-2] + ADX>75 + 股价> upper
        cond2_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond2_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond2_adx_very_high = df['ADX'].iloc[-1] > 75  # ADX>75
        cond2_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价> upper
        
        condition2 = cond2_mid_up and cond2_bandwidth_expand and cond2_adx_very_high and cond2_price_above_upper

        # --- 条件3：偏高ADX + 高波动 ---
        # 描述: MID[-1]>[-2] + boll带宽扩大[-1]>[-2] + ADX>55 + 最近2周期有1个是股价>UPPER + 最近3周期有1个TR>ATR*2
        cond3_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]  # MID[-1]>[-2]
        cond3_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽扩大
        cond3_adx_high = df['ADX'].iloc[-1] > 55  # ADX>55
        cond3_price_above_upper = (df['close'] > df['UPPER']).iloc[-2:].sum() >= 1  # 最近2周期有1个是股价>UPPER
        cond3_tr_high = (df['TR'] > df['ATR'] * 2).iloc[-3:].sum() >= 1  # 最近3周期有1个TR>ATR*2
        
        condition3 = (cond3_mid_up and cond3_bandwidth_expand and cond3_adx_high and 
                     cond3_price_above_upper and cond3_tr_high)

        # --- 最终判断：满足任一条件 ---
        is_long_term_trend_top = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_long_term_trend_top:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("长线趋势调整后折返拉抬（均值回归）顶部")
        #     if condition2: triggered_conditions.append("超高的ADX")
        #     if condition3: triggered_conditions.append("偏高ADX + 高波动")
        #     print(f"触发了长线趋势顶部条件: {', '.join(triggered_conditions)}")

        return is_long_term_trend_top

    def cxtzg(self) -> bool:
        """
        判断是否进入"长线趋势调整中"的技术状态。
        该函数使用周线数据，依赖于 zhibiao 函数计算出的 BOLL、MACD 和 DMI 指标。
        满足以下条件即返回True。
        """
        df = self.df_weekly  # 使用周线数据
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['UPPER', 'LOWER', 'MACD', 'ADX', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df_weekly 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、MACD 和 DMI。")
            return False

        # --- 条件：长线趋势拉抬见顶后的调整 ---
        # 描述: 股价低于upper + boll带宽开口扩大[-1]>[-2] + 最近3周期有1个是ADX>50 + MACD[1]<[4] + macd>0
        cond_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价低于upper
        
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        
        cond_adx_high = (df['ADX'] > 50).iloc[-3:].sum() >= 1  # 最近3周期有1个是ADX>50
        
        cond_macd_weak = df['MACD'].iloc[-1] < df['MACD'].iloc[-4]  # MACD[1]<[4]
        
        cond_macd_pos = df['MACD'].iloc[-1] > 0  # macd>0
        
        # --- 最终判断：所有条件都满足 ---
        is_long_term_adjusting = (cond_price_below_upper and cond_bandwidth_expand and 
                                 cond_adx_high and cond_macd_weak and cond_macd_pos)

        # (可选的调试信息)
        # if is_long_term_adjusting:
        #     print("触发了长线趋势调整中条件: 股价低于上轨+带宽扩大+ADX高位+MACD走弱但仍为正")

        return is_long_term_adjusting


    def zj_jjdi(self) -> bool:
        """
        判断是否进入"中级底部极度"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 MACD 和 KDJ 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MACD', 'J', 'K', 'D']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 MACD 和 KDJ。")
            return False

        # --- 条件1：MACD负值 ---
        # 描述: MACD[-1] < 0
        condition1 = df['MACD'].iloc[-1] < 0 and df['MACD'].iloc[-1] < df['MACD'].iloc[-4]

        # --- 条件2：KDJ超卖或深度死叉后反转 ---
        # 描述: J[-1] < 10 或者 (K<D最近4天连续 + J[-1]>[-2] + J[-1]<50)
        cond2_j_oversold = df['J'].iloc[-1] < 10  # J值超卖
        cond2_kd_dead_cross = (df['K'] < df['D']).iloc[-4:].sum() == 4  # K<D最近4天连续
        cond2_j_turn_up = df['J'].iloc[-1] > df['J'].iloc[-2]  # J值反转上涨
        cond2_j_low = df['J'].iloc[-1] < 50  # J值仍然较低
        
        condition2 = cond2_j_oversold 
        
        condition3 = cond2_kd_dead_cross and cond2_j_turn_up and cond2_j_low

        # --- 最终判断：满足任一条件 ---
        is_extreme_bottom = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_extreme_bottom:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("MACD负值")
        #     if condition2: triggered_conditions.append("KDJ超卖或深度死叉后反转")
        #     print(f"触发了中级底部极度条件: {', '.join(triggered_conditions)}")

        return is_extreme_bottom
    # def zj_jjdi_score(self) -> float:
    #     """
    #     计算"中级接近底部"状态的观测分值（0-1）。
    #     **V4版：** 引入基础分机制。一旦核心条件触发，评分直接从0.6开始，
    #     确保输出分值符合"高可能性"的初始判断。
    #     """
    #     df = self.df_daily
    #     if len(df) < 30: # 保证指标计算的有效性
    #         return 0.0

    #     # --- 1. 提取所有原子条件 ---
    #     conditions = {
    #         "macd_neg": df['MACD'].iloc[-1] < 0,
    #         "j_oversold": df['J'].iloc[-1] < 10,
    #         "k_less_d_4d": (df['K'] < df['D']).iloc[-4:].sum() == 4,
    #         "j_turn_up": df['J'].iloc[-1] > df['J'].iloc[-2],
    #         "j_low_pos": df['J'].iloc[-1] < 50,
    #     }

    #     # --- 2. 核心触发条件判断 ---
    #     # 核心特征是：J值严重超卖，或者KDJ已经连续死叉4天（表明深度回调）
    #     is_triggered = conditions["j_oversold"] or conditions["k_less_d_4d"]
                    
    #     if not is_triggered:
    #         # 如果连最基本的底部特征都没出现，说明未进入该状态，返回0分
    #         return 0.0

    #     # --- 3. 核心改进：一旦触发，直接给予0.6的基础分 ---
    #     score = 0.6
        
    #     # --- 4. 在基础分之上，进行额外加分 ---
    #     # 定义额外加分的权重，总和为 0.4 (使得满分为 0.6 + 0.4 = 1.0)
    #     bonus_weights = {
    #         "j_turn_up": 0.15,         # J值拐头是关键的反转确认信号
    #         "macd_neg_confirm": 0.05,  # MACD为负作为环境确认
    #         "combo_oversold_turn": 0.2, # 【强力组合】J值在超卖区拐头，给予最高奖励
    #     }

    #     # --- 应用加分项 ---
    #     if conditions["j_turn_up"]:
    #         score += bonus_weights["j_turn_up"]

    #     if conditions["macd_neg"]:
    #         score += bonus_weights["macd_neg_confirm"]

    #     # 应用最强的组合增强
    #     if conditions["j_oversold"] and conditions["j_turn_up"]:
    #         score += bonus_weights["combo_oversold_turn"]
            
    #     # --- 5. 确保分数不会超过1.0 ---
    #     final_score = min(score, 1.0)

    #     return round(final_score, 4)


    # ==================================================================
    # === 在此处添加修正后的"中级底部区域"函数 ===
    # ==================================================================
    def zj_db(self) -> bool:
        """
        判断是否进入"中级底部区域"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 BOLL 和 DMI 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'ADX', 'PDI', 'MDI', 'MACD', 'J', 'K', 'D', 'MA_7', 'MA_26']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL 和 DMI。")
            return False

        # --- 条件1：接近BOLL中轨 ---
        # 描述: 最近2天的最低价有1个是低于boll中轨 + upper最近3日连续下降 + mid最近3日连续上涨。
        cond1_boll_touch = (df['low'] < df['MID']).iloc[-2:].sum() >= 1
        cond1_upper_down = (df['UPPER'].diff() < 0).iloc[-3:].all()
        cond1_mid_up = (df['MID'].diff() > 0).iloc[-3:].all()
        
        condition1 = cond1_boll_touch and cond1_upper_down and cond1_mid_up

        # --- 条件2：ADX调整下来 ---
        # 描述: ADX.[-1]<ADX.[-2]且ADX.[-1]<ADX.[-4] 且 PDI[-1]<MDI[-1]
        cond2_adx_down = (df['ADX'].iloc[-1] < df['ADX'].iloc[-2]) and \
                         (df['ADX'].iloc[-1] < df['ADX'].iloc[-4])
        cond2_pdi_mdi = df['PDI'].iloc[-1] < df['MDI'].iloc[-1]
        
        condition2 = cond2_adx_down and cond2_pdi_mdi

        # --- 条件3：MACD为基础+J值 ---
        # 描述: MACD连续5交易日小于0 + J[-1]>[-2]+J[-1]<50
        cond3_macd_neg = (df['MACD'] < 0).iloc[-5:].all()
        cond3_j_turn = df['J'].iloc[-1] > df['J'].iloc[-2]
        cond3_j_low = df['J'].iloc[-1] < 50

        condition3 = cond3_macd_neg and cond3_j_turn and cond3_j_low
        
        # --- 条件4：MA_7 < MA_26 ---
        # 描述: MA_7 < MA26
        condition4 = df['MA_7'].iloc[-1] < df['MA_26'].iloc[-1] and df['MA_26'].iloc[-1] > df['MA_26'].iloc[-2]
        
        # --- 条件5：KDJ深度死叉或出现反转迹象 ---
        # 描述: KDJ的K<D最近7交易日连续都是 + J连续2个小于0 或者 K[-1]>[-2]
        cond5_k_less_d_7d = (df['K'] < df['D']).iloc[-7:].all()
        cond5_j_deep_oversold = (df['J'] < 0).iloc[-2:].all()
        cond5_k_turn_up = df['K'].iloc[-1] > df['K'].iloc[-2]

        condition5 = cond5_k_less_d_7d and (cond5_j_deep_oversold or cond5_k_turn_up)

        # --- 最终判断：满足任一条件 ---
        is_bottom_area = condition1 or condition2 or condition3 or condition4 or condition5

        # (可选的调试信息)
        # if is_bottom_area:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("接近BOLL中轨")
        #     if condition2: triggered_conditions.append("ADX调整下来")
        #     if condition3: triggered_conditions.append("MACD为基础+J值")
        #     if condition4: triggered_conditions.append("MA7<MA26")
        #     if condition5: triggered_conditions.append("KDJ深度死叉或反转")
        #     print(f"触发了中级底部区域条件: {', '.join(triggered_conditions)}")

        return is_bottom_area
    

    def zjdtg(self) -> bool:
        """
        判断是否进入"中级底部拐点"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 BOLL、DMI 和 MACD 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['MID', 'UPPER', 'LOWER', 'ADX', 'PDI', 'MDI', 'MACD', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、DMI 和 MACD。")
            return False

        # --- 条件1：BOLL中轨上涨 + 股价在中轨上运行 + 股价没有站上上轨 + BOLL带宽收缩 + MACD[-1]>MACD[-4] ---
        # 描述: MID[-1]>[-2] + 股价在中轨上运行 + 股价没有以收盘价站上上轨 + boll带宽[-1]<[-2] + MACD[-1]>MACD[-4]
        cond1_mid_up = df['MID'].iloc[-1] > df['MID'].iloc[-2]
        cond1_price_above_mid = df['close'].iloc[-1] > df['MID'].iloc[-1]  # 股价在中轨上运行
        cond1_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价没有站上上轨
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_shrink = boll_bandwidth_current < boll_bandwidth_prev  # 带宽收缩
        cond1_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD[-1]>MACD[-4]

        condition1 = cond1_mid_up and cond1_price_above_mid and cond1_price_below_upper and cond1_bandwidth_shrink and cond1_macd_up
        # --- 条件2：ADX上涨 + PDI > MDI + ADX < 35 + 股价没有站上上轨 ---
        # 描述: ADX[-1]>[-2] + PDI > MDI + ADX的值小于35 + 股价没有以收盘价站上上轨
        cond2_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]
        cond2_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]
        cond2_adx_low = df['ADX'].iloc[-1] < 35
        cond2_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价没有站上上轨
        
        condition2 = cond2_adx_up and cond2_pdi_gt_mdi and cond2_adx_low and cond2_price_below_upper

        # --- 条件3：MACD > 0 最近7天不超过2天 + MACD最近上涨 + 股价最近2日没有1天是站上UPPER ---
        # 描述: MACD>0最近7天<=2天 + MACD[-1]>[-4] + 股价最近2日没有1天是站上UPPER
        macd_positive_count = (df['MACD'] > 0).iloc[-7:].sum()  # 最近7天MACD>0的天数
        cond3_macd_limited_positive = macd_positive_count <= 2  # 最近7天不超过2天
        cond3_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]  # MACD最近上涨
        cond3_price_not_above_upper = (df['close'] > df['UPPER']).iloc[-2:].sum() == 0  # 股价最近2日没有1天是站上UPPER

        condition3 = cond3_macd_limited_positive and cond3_macd_up and cond3_price_not_above_upper

        # --- 最终判断：满足任一条件 ---
        is_bottom_turning_point = condition1 or condition2 or condition3

        # (可选的调试信息)
        # if is_bottom_turning_point:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("BOLL中轨上涨+股价在中轨上+未站上轨+带宽收缩")
        #     if condition2: triggered_conditions.append("ADX上涨+PDI>MDI+ADX<35+未站上轨")
        #     if condition3: triggered_conditions.append("MACD有限正数+MACD上涨")
        #     print(f"触发了中级底部拐点条件: {', '.join(triggered_conditions)}")

        return is_bottom_turning_point

    def zjdtz(self) -> bool:
        """
        判断是否进入"中级底部转折"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 BOLL、DMI 和 MACD 指标。
        满足以下任一条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['UPPER', 'LOWER', 'ADX', 'PDI', 'MDI', 'MACD', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、DMI 和 MACD。")
            return False

        # --- 条件1：股价站上上轨 + BOLL带宽开口扩大 + MACD上涨 ---
        # 描述: 股价收盘价站上upper + boll带宽开口扩大[-1]>[-2] + MACD[-1]>[-2]
        cond1_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价站上上轨
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        cond1_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-2]  # MACD上涨
        
        condition1 = cond1_price_above_upper and cond1_bandwidth_expand and cond1_macd_up

        # --- 条件2：ADX强势上涨 + PDI > MDI + ADX > 35 + 股价站上上轨 + MACD上涨 ---
        # 描述: ADX[-1]>[-2] + PDI > MDI + ADX的值大于35 + 以收盘价站上上轨 + MACD[-1]>[-2]
        cond2_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX上涨
        cond2_pdi_gt_mdi = df['PDI'].iloc[-1] > df['MDI'].iloc[-1]  # PDI > MDI
        cond2_adx_high = df['ADX'].iloc[-1] > 35  # ADX > 35 (强势状态)
        cond2_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价站上上轨
        cond2_macd_up = df['MACD'].iloc[-1] > df['MACD'].iloc[-2]  # MACD上涨
        
        condition2 = cond2_adx_up and cond2_pdi_gt_mdi and cond2_adx_high and cond2_price_above_upper and cond2_macd_up

        # --- 最终判断：满足任一条件 ---
        is_bottom_turning = condition1 or condition2

        # (可选的调试信息)
        # if is_bottom_turning:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("股价站上轨+带宽开口+MACD上涨")
        #     if condition2: triggered_conditions.append("ADX强势上涨+PDI>MDI+ADX>35+站上轨+MACD上涨")
        #     print(f"触发了中级底部转折条件: {', '.join(triggered_conditions)}")

        return is_bottom_turning
        



    def zjqs_ding(self) -> bool:
        """
        判断是否进入"中级趋势顶部"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 BOLL、MACD、KDJ、DMI 和成交量指标。
        满足以下任一条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['UPPER', 'LOWER', 'MACD', 'K', 'J', 'ADX', 'PDI', 'close', 'volume', 'VOL_30', 'VOL_3']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL、MACD、KDJ、DMI 和成交量指标。")
            return False

        # --- 条件1：股价站上上轨 + 带宽扩大 + MACD连续下降或大幅下降 ---
        # 描述: 最近3日有1天是股价收盘价站上upper + boll带宽开口扩大[-1]>[-2] + MACD.shift(0)<(1)连续2天或MACD[1]<[4]
        cond1_price_above_upper_3d = (df['close'] > df['UPPER']).iloc[-3:].sum() >= 1  # 最近3日有1天站上上轨
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        # MACD连续2天下降或MACD[1]<[4]
        cond1_macd_continuous_down = (df['MACD'].iloc[-2:] < df['MACD'].shift(1).iloc[-2:]).all()  # 连续2天下降
        cond1_macd_big_drop = df['MACD'].iloc[-1] < df['MACD'].iloc[-4]  # MACD[1]<[4]
        cond1_macd_weak = cond1_macd_continuous_down or cond1_macd_big_drop
        
        condition1 = cond1_price_above_upper_3d and cond1_bandwidth_expand and cond1_macd_weak

        # --- 条件2：股价站上上轨 + 带宽扩大 + KDJ超买后回落 ---
        # 描述: 最近3日有1天是股价收盘价站上upper + boll带宽开口扩大[-1]>[-2] + K[-2:].MAX()>80 或 J[-3:].MAX()>95 + J[-1]<[-2]
        cond2_price_above_upper_3d = (df['close'] > df['UPPER']).iloc[-3:].sum() >= 1  # 最近3日有1天站上上轨
        cond2_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        cond2_k_overbought = df['K'].iloc[-2:].max() > 80  # K[-2:].MAX()>80
        cond2_j_overbought = df['J'].iloc[-3:].max() > 95  # J[-3:].MAX()>95
        cond2_j_fall = df['J'].iloc[-1] < df['J'].iloc[-2]  # J[-1]<[-2]
        
        condition2 = cond2_price_above_upper_3d and cond2_bandwidth_expand and (cond2_k_overbought or cond2_j_overbought) and cond2_j_fall

        # --- 条件3：股价站上上轨 + 带宽扩大 + ADX强势但开始回落 ---
        # 描述: 股价收盘价站上upper + boll带宽开口扩大[-1]>[-2] + ADX>60 + ADX[1]>[2] + PDI[1]<[2]
        cond3_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价站上上轨
        cond3_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        cond3_adx_high = df['ADX'].iloc[-1] > 60  # ADX>60
        cond3_adx_up = df['ADX'].iloc[-1] > df['ADX'].iloc[-2]  # ADX[1]>[2]
        cond3_pdi_down = df['PDI'].iloc[-1] < df['PDI'].iloc[-2]  # PDI[1]<[2]
        
        condition3 = cond3_price_above_upper and cond3_bandwidth_expand and cond3_adx_high and cond3_adx_up and cond3_pdi_down

        # --- 条件4：股价站上上轨 + 带宽扩大 + 成交量异常放大后萎缩 ---
        # 描述: 股价收盘价站上upper + boll带宽开口扩大[-1]>[-2] + ADX>60 + 最近5日有1日的VOL>VOL_30*2.5 + vol_3[-1]<[-2]
        cond4_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价站上上轨
        cond4_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        cond4_adx_high = df['ADX'].iloc[-1] > 60  # ADX>60
        cond4_volume_surge = (df['volume'] > df['VOL_30'] * 2.5).iloc[-5:].sum() >= 1  # 最近5日有1日成交量异常放大
        cond4_vol3_shrink = df['VOL_3'].iloc[-1] < df['VOL_3'].iloc[-2]  # vol_3[-1]<[-2]
        
        condition4 = cond4_price_above_upper and cond4_bandwidth_expand and cond4_adx_high and cond4_volume_surge and cond4_vol3_shrink

        # --- 条件5：股价站上上轨 + ADX强势但开始回落 ---
        # 描述: 股价收盘价站上upper + adx>60 + adx[-1]<[-2]
        cond5_price_above_upper = df['close'].iloc[-1] > df['UPPER'].iloc[-1]  # 股价站上上轨
        cond5_adx_high = df['ADX'].iloc[-1] > 60  # ADX>60
        cond5_adx_down = df['ADX'].iloc[-1] < df['ADX'].iloc[-2]  # ADX[-1]<[-2]

        condition5 = cond5_price_above_upper and cond5_adx_high and cond5_adx_down

        # --- 最终判断：满足任一条件 ---
        is_trend_top = condition1 or condition2 or condition3 or condition4 or condition5

        # (可选的调试信息)
        # if is_trend_top:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("股价站上轨+带宽扩大+MACD连续下降")
        #     if condition2: triggered_conditions.append("股价站上轨+带宽扩大+KDJ超买回落")
        #     if condition3: triggered_conditions.append("股价站上轨+带宽扩大+ADX强势回落")
        #     if condition4: triggered_conditions.append("股价站上轨+带宽扩大+成交量异常后萎缩")
        #     if condition5: triggered_conditions.append("股价站上轨+ADX强势但开始回落")
        #     print(f"触发了中级趋势顶部条件: {', '.join(triggered_conditions)}")

        return is_trend_top

    def zjtzz(self) -> bool:
        """
        判断是否进入"中级调整中"的技术状态。
        该函数依赖于 zhibiao 函数计算出的 BOLL 和 MACD 指标。
        满足以下条件即返回True。
        """
        df = self.df_daily
        
        # --- 前置检查 ---
        # 确保数据量足够进行计算
        if len(df) < 30:
            return False
            
        # 确保 zhibiao 函数已经计算了必要的指标
        required_indicators = ['UPPER', 'LOWER', 'MACD', 'close']
        if not all(indicator in df.columns for indicator in required_indicators):
            print("错误: df 中缺少必要的指标列。请确保 zhibiao 函数已计算 BOLL 和 MACD。")
            return False

        # --- 条件1：股价低于上轨 + 带宽扩大 + MACD走弱 ---
        # 描述: 股价低于upper + boll带宽开口扩大[-1]>[-2] + MACD[1]<[4] + MACD>0
        cond1_price_below_upper = df['close'].iloc[-1] < df['UPPER'].iloc[-1]  # 股价低于上轨
        
        # BOLL带宽 = UPPER - LOWER
        boll_bandwidth_current = df['UPPER'].iloc[-1] - df['LOWER'].iloc[-1]
        boll_bandwidth_prev = df['UPPER'].iloc[-2] - df['LOWER'].iloc[-2]
        cond1_bandwidth_expand = boll_bandwidth_current > boll_bandwidth_prev  # 带宽开口扩大
        
        cond1_macd_weak = df['MACD'].iloc[-1] < df['MACD'].iloc[-4]  # MACD[1]<[4]
        cond1_macd_positive = df['MACD'].iloc[-1] > 0   # MACD>0
        
        condition1 = cond1_price_below_upper and cond1_bandwidth_expand and cond1_macd_weak and cond1_macd_positive


        # --- 条件2：股价在最近3天有1天是大于UPPER + MACD[1]<4 + MACD>0 ---
        # 描述: 股价在最近3天有1天是大于UPPER + MACD[1]<4 + MACD>0
        cond2_price_above_upper_3d = (df['close'] > df['UPPER']).iloc[-3:].sum() >= 1  # 最近3天有1天大于UPPER
        cond2_macd_weak = df['MACD'].iloc[-1] < df['MACD'].iloc[-4]  # MACD[1]<[4]
        cond2_macd_positive = df['MACD'].iloc[-1] > 0   # MACD>0
        
        condition2 = cond2_price_above_upper_3d and cond2_macd_weak and cond2_macd_positive

        # --- 最终判断：满足任一条件 ---
        is_adjusting = condition1 or condition2

        # (可选的调试信息)
        # if is_adjusting:
        #     triggered_conditions = []
        #     if condition1: triggered_conditions.append("股价低于上轨+带宽扩大+MACD走弱")
        #     if condition2: triggered_conditions.append("股价最近3天有1天大于上轨+MACD走弱")
        #     print(f"触发了中级调整中条件: {', '.join(triggered_conditions)}")


        return is_adjusting

    #========================
    def bdqs_di(self):
        df=self.df_daily
        # df15 = zhibiao(i, unit = '15m', today=date)
        # df30 = zhibiao(i, unit = '30m', today=date)    
        # close3ri = df['close'].iloc[-3:].max()
        # xianjia = get_latest_price(i)
        aa = False; ab = False;ac = False;ad = False;ae = False;af=False
        if ((df['J']<0).iloc[-3:].sum()>=2):# & (df['J'][-1]>df['J'][-2]):
            aa = True
            # print('bdqs_di_a,J小于0连续2个:{v}'.format(v=i))
        if ((df['K'] < df['D']).iloc[-4:].sum()==4) and (df['J'].iloc[-1] > df['J'].iloc[-2]) and (df['K'].iloc[-1]<35):
            ab = True
            # print('bdqs_di_b,K小于D连续4个,J[-1]>[-2],现价小于ma7或ma26:{v}'.format(v=i))  
        if ((df['close']<df['MA_26']).iloc[-3:].sum()>=1) and ((df['K']<df['D']).iloc[-4:].sum()==4) and (df['J'].iloc[-1]>df['J'].iloc[-2]):
            ac = True
            # print('bdqs_di_f,股价小于ma26,k小于d连续4日,j[-1]>[-2]:{v}'.format(v=i))
        if (df['close'].iloc[-3:].min()==df['close'].iloc[-20:].min()) and (df['K'].iloc[-1]>df['K'].iloc[-2]) and (df['K'].iloc[-1]<35):
            ad = True
            # print('bdqs_di_g,创出20日新低+K[-1]>[-2]:{v}'.format(v=i))

        if ((df['J']<10).iloc[-2:].sum()>=1) and (df['J'].iloc[-1]>df['J'].iloc[-2]):
            ae = True
            # print('bdqs_di_i,J[-1]<10连续1日,j1>2:{v}'.format(v=i)) 
        if df['MACD'].iloc[-1]<0 and (df['K']<0).iloc[-2:].sum()==2:
            af = True

        return aa or ab or ac or ad or ae or af
    def bdzf_di(self):
        df=self.df_daily
        aa = False; bb = False;cc = False;dd = False;ee = False;ff= False;gg=False;hh = False
        kxd = df['K'].iloc[-1]<df['D'].iloc[-1]
        kxd3 = (df['K']<df['D']).iloc[-3:].sum()==3
        cxma7 = df['close'].iloc[-1]<df['MA_7'].iloc[-1]
        cxma7102 = df['close'].iloc[-1]<df['MA_7'].iloc[-1]*1.02
        cxma26 = df['close'].iloc[-1]<df['MA_26'].iloc[-1]
        jx50 = df['J'].iloc[-1]<50
        macddt = df['MACD'].iloc[-1]>df['MACD'].iloc[-4]
        ma7ma26pd =min(df['MA_7'].iloc[-1],df['MA_26'].iloc[-1]) + abs(df['MA_7'].iloc[-1]-df['MA_26'].iloc[-1]) *0.15 
        cxma7ma26 = df['close'].iloc[-1]<ma7ma26pd
        vol5x30 = df['VOL_5'].iloc[-1]<df['VOL_30'].iloc[-1] and df['VOL_5'].iloc[-1]<df['VOL_5'].iloc[-2]
        j1x2 = (df['J'].shift(0)<df['J'].shift(1)).iloc[-2:].sum()==2
        j1x3 = (df['J'].shift(0)<df['J'].shift(1)).iloc[-3:].sum()==3
        # macdx0 = df['MACD'].iloc[-1]<0
        vol51x2 = df['VOL_5'].iloc[-1]<df['VOL_5'].iloc[-2]
        #趋势状态1
        # ma26dt = df['MA_26'].iloc[-1]>df['MA_26'].iloc[-2]
        # ma7d26 = df['MA_7'].iloc[-1]>df['MA_26'].iloc[-1]
        macdd0 = df['MACD'].iloc[-1]>0
        macdd0y4 = (df['MACD']>0).iloc[-9:].sum()>=4
        macdx0y1 = (df['MACD']<0).iloc[-9:].sum()>=1
        # vol5d30 = df['VOL_5'].iloc[-1]>df['VOL_30'].iloc[-1]
        # c7dc40 = df['close'].iloc[-7:].max() == df['close'].iloc[-20:].max()
        
        
        if (kxd and cxma7102) or (j1x3 and vol51x2 and cxma7102):
            aa = True
        if cxma7:
            bb = True
        if cxma7ma26:
            cc = True
        if jx50:
            dd = True
        if vol5x30 or j1x2:
            ee = True
        if kxd3:
            ff  = True
        if macdd0y4 and macdx0y1 and macdd0:
            gg = True
        # if macddt and vol51x2:
        #     hh = True
        return (aa or bb or cc or dd or ee or ff) and gg
        
    def bdqzf_di(self):
        df=self.df_daily
        aa = False; ab = False;ac = False;ad = False
        jx10 = df['J'].iloc[-1]<10
        kxd3 = (df['K']<df['D']).iloc[-3:].sum()==3
        kxd1 = df['K'].iloc[-1]<df['D'].iloc[-1]
        cxma73 = (df['close']<df['MA_7']).iloc[-3:].sum()==3 
        cxma26 = df['close'].iloc[-1]<df['MA_26'].iloc[-1]
        ma7ma26pd =min(df['MA_7'].iloc[-1],df['MA_26'].iloc[-1]) + abs(df['MA_7'].iloc[-1]-df['MA_26'].iloc[-1]) *0.15 
        cxma7ma26 = df['close'].iloc[-1]<ma7ma26pd
        kxd5 = (df['K']<df['D']).iloc[-5:].sum()==5
        jx50 = df['J'].iloc[-1]<50   
        macdd0 = df['MACD'].iloc[-1]>0
        if jx10:
            aa = True
        if kxd3 and (cxma73 or cxma7ma26 or jx50):
            ab = True
        if kxd5 and jx50:
            ac = True
        if kxd1 and macdd0 and cxma7ma26:
            ad = True
        return aa or ab or ac or ad
    

    def bdqs_ding(self):
        df=self.df_daily

        aa = False;bb = False;cc = False
        kddy4 = ((df['K']>df['D']).iloc[-4:].sum()==4)  
        c3dc40 = (df['close'].iloc[-3:].max()==df['close'].iloc[-40:].max())
        jd100y2 = (df['J']>100).iloc[-2:].sum()==2
        baoliang = (df['volume']>df['VOL_30']*4).iloc[-3:].sum()>=1 or (df['volume']>df['VOL_30']*3).iloc[-3:].sum()>=2
        j1x2 = df['J'].iloc[-1] < df['J'].iloc[-2]
        if kddy4 and c3dc40 and j1x2:
            aa = True
        if c3dc40 and baoliang:
            bb = True
        if jd100y2 and j1x2:
            cc = True
        return aa or bb or cc
    def ma26ruo(self):
        df=self.df_daily
        ma26ruo = df['MA_26'].iloc[-1] < df['MA_26'].iloc[-2]
        return ma26ruo


    def bias_120(self):
        df=self.df_daily
        bias120 = df['BIAS_120'].iloc[-1]
        return bias120
    
    def bd_kxd1(self):
        df=self.df_daily
        kxd1 = df['K'].iloc[-1] < df['D'].iloc[-1]
        return kxd1
    def bd_kxd2(self):
        df=self.df_daily
        kxd2 = (df['K']<df['D']).iloc[-2:].sum()==2
        return kxd2
    def bd_k1d2x40(self):
        df=self.df_daily
        k1d2 = df['K'].iloc[-1] > df['K'].iloc[-2]
        kxd4 = df['K'].iloc[-1] < 40
        return k1d2 and kxd4
    def bd_gao(self):
        df=self.df_daily
        gao = df['K'].iloc[-1] > 70 or df['J'].iloc[-1] > 92
        return gao
    def growth_rate(self):
        df=self.df_daily
        growth_rate = (df['close'].iloc[-1] - df['close'].iloc[-4]) / df['close'].iloc[-4]
        return growth_rate

    def mingque_buy(self):
        df=self.df_daily
        aa = False;bb = False;cc = False;dd = False
        macddt = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]
        macdx0 = (df['MACD']<0).iloc[-3:].sum()==3 
        kx50 =   df['K'].iloc[-1] < 55
        macdx0y8 = (df['MACD']<0).iloc[-9:].sum()>=8
        k1d2 = df['K'].iloc[-1] > df['K'].iloc[-2]
        vol5x30 = df['VOL_5'].iloc[-1] < df['VOL_30'].iloc[-1]
        if macddt and macdx0 and kx50:
            aa = True
        if macdx0y8 and k1d2 and kx50:
            bb = True
        if macddt and macdx0 and vol5x30:
            cc = True
        return aa or bb or cc

    def dazhi_buy(self):
        df=self.df_daily
        aa = False;bb = False;cc = False;dd = False
        macddt = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]
        c5xc20 = df['close'].iloc[-5:].max() < df['close'].iloc[-20:].max()
        macdx0 = (df['MACD']<0).iloc[-3:].sum()==3 
        kxd = df['K'].iloc[-1] < df['D'].iloc[-1]
        cxma7 = df['close'].iloc[-1] < df['MA_7'].iloc[-1]
        macdduogang = (df['MACD']>0).iloc[-3:].sum()<3
        if macddt and c5xc20:
            aa = True
        if macdx0:
            bb = True
        if kxd or cxma7: 
            cc = True
        if macddt and macdduogang:
            dd = True
        return aa or bb or cc or dd
    
    def mingque_sell(self):
        df=self.df_daily
        aa = False;bb = False;cc = False;dd = False

        return aa or bb or cc or dd
    def dazhi_sell(self):
        df=self.df_daily
        aa = False;bb = False;cc = False;dd = False

        return aa or bb or cc or dd







#==========================================       

def prepare_data_for_backtest(stock_code: str, date: str) -> dict:
    """
    回测数据提供者：快速从本地加载截至date的历史数据。
    """
    print(f"\n[数据准备-回测模式]: 为 {stock_code} 加载 {date} 的历史数据...")
    
    # 调用我们之前设计的快速加载函数
    df_M = get_monthly_data_for_backtest(stock_code, date)
    df_w = get_weekly_data_for_backtest(stock_code, date)
    df_d = get_daily_data_for_backtest(stock_code, date)
    # fundamentals = ...
    
    # 将所有数据打包成一个字典返回
    return {
        "monthly": df_M,
        "daily": df_d,
        "weekly": df_w,
        # "fundamentals": fundamentals
    }

def prepare_data_for_live(stock_code: str) -> dict:
    """
    实盘数据提供者：调用你强大的实时更新函数来获取最新数据。
    """
    print(f"\n[数据准备-实盘模式]: 为 {stock_code} 获取最新的实时数据...")
    
    # 只有在这里，我们才调用那个包含API请求和数据库更新的重量级函数
    df_d = update_and_load_data_daily(stock_code) # 获取日线
    df_w = update_and_load_data_weekly(stock_code) # 获取周线
    df_M = update_and_load_data_monthly(stock_code) # 获取月线
    # 你可能需要一些函数将日线数据转换成周线和月线
    # df_w = convert_daily_to_weekly(df_d)
    # df_m = convert_daily_to_monthly(df_d)
    
    return {
        "daily": df_d,
        "weekly": df_w,
        "monthly": df_M,
        # "monthly": df_m
    }

#==========================================
def create_analyzer(stock_code: str, date: str = None) -> TechnicalAnalyzer:
    """
    分析器工厂：根据是否提供了date参数，自动创建适用于回测或实盘的分析器实例。
    """
    data_for_analyzer = {}
    
    if date:
        # 如果提供了date，说明是回测模式
        data_for_analyzer = prepare_data_for_backtest(stock_code, date)
    else:
        # 如果没有提供date，说明是实盘模式
        data_for_analyzer = prepare_data_for_live(stock_code)
        
    # 将准备好的数据"注入"到分析器中，并返回实例
    return TechnicalAnalyzer(data_for_analyzer)


# 在 __main__ 块中进行测试是个好习惯
if __name__ == "__main__":
    print("--- 开始测试 TechnicalAnalyzer 类 ---")

    # --- 回测模式示例 ---
    print("\n=== 回测模式 ===")
    # 为股票 '000001' 在 '2024-01-01' 进行回测
    # 'date' 参数表示我们只使用截至该日期的数据，模拟历史回测
    backtest_analyzer = create_analyzer('000029', date='2024-06-29')

    # 访问数据属性并打印其形状
    print(f"回测分析器日线数据形状: {backtest_analyzer.df_daily.shape}")
    print(f"回测分析器周线数据形状: {backtest_analyzer.df_weekly.shape}")
    print(f"回测分析器月线数据形状: {backtest_analyzer.df_monthly.shape}")

    # 调用类的分析方法（例如，ccx_jjdi），它会根据内部数据进行计算
    is_bottom = backtest_analyzer.ccx_jjdi()
    print(f"回测模式下 '000001' 是否超长线接近底部: {is_bottom}")

    is_bottom = backtest_analyzer.ccx_di()
    print(f"回测模式下 '000001' 是否超长线底部: {is_bottom}")













    


    # # --- 实盘模式示例 ---
    # print("\n=== 实盘模式 ===")
    # # 为股票 '000001' 获取最新的实盘数据
    # # 不提供 'date' 参数，表示获取当前最新的数据
    # live_analyzer = create_analyzer('000001')

    # # 访问数据属性并打印其形状
    # print(f"实盘分析器日线数据形状: {live_analyzer.df_daily.shape}")
    # print(f"实盘分析器周线数据形状: {live_analyzer.df_weekly.shape}")
    # print(f"实盘分析器月线数据形状: {live_analyzer.df_monthly.shape}")

    # # 调用类的分析方法
    # is_bottom_live = live_analyzer.ccx_jjdi()
    # print(f"实盘模式下 '000001' 是否超长线接近底部: {is_bottom_live}")

    # print("\n--- TechnicalAnalyzer 类测试结束 ---")






# #没有超长线暴涨
# def no_ccxbz(df):
    
#     aa = False
#     # 获取最近48个月的数据
#     df_recent = df1M.tail(48)
    
#     # 遍历每个可能的3个月窗口
#     for start in range(len(df_recent)-2):
#         # 获取3个月的窗口数据
#         window = df_recent.iloc[start:start+3]
        
#         # 获取最后一个月的收盘价
#         last_close = window.iloc[-1]['close']
        
#         # 获取这3个月中的最低收盘价
#         min_close = window['close'].min()
#         #min_close = window.iloc[-3]['close']
#         # 如果最后一个月的收盘价是最低价的3倍或以上
#         if last_close >= min_close * 2.5:
#             #return aa == True
#             aa = True
#     return aa
#     #中级技术状态---------------------




        
  
    




# #超长线的最近10月涨幅
# def ccx_10zf(df):
#     df1M = zhibiao(df)
#     zf == df1M['close'].iloc[-1] / df1M['close'].iloc[-10]  
#     return zf

# #超长线的最近10月换手率
# def ccx_10hsl(df):
#     df1M = zhibiao(df)
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_data = jiben[jiben['code'] == i]
#     ltp = stock_data['流通A股'].values[0]
#     hsl == df1M['volume'].iloc[-10:].sum() / ltp
#     return hsl

# #长线技术---------------------
# #没有超长线暴涨
# def no_cxbz(df):
#     df1w = zhibiao(df)
#     aa = False
#     # 获取最近48个月的数据
#     df_recent = df1w.tail(25)
    
#     # 遍历每个可能的3个月窗口
#     for start in range(len(df_recent)-2):
#         # 获取3个月的窗口数据
#         window = df_recent.iloc[start:start+3]
        
#         # 获取最后一个月的收盘价
#         last_close = window.iloc[-1]['high']
        
#         # 获取这3个月中的最低收盘价
#         min_close = window['close'].min()
        
#         # 如果最后一个月的收盘价是最低价的1.9倍或以上
#         if last_close >= min_close * 1.9:
#             return aa == True
#     return aa






# #长线多头主升中
# def cxdtz_pd(i, date):
#     df1w = zhibiao(i, '1w', today=date)
#     #df1M = zhibiao(i, '1M', today=date)
#     aa = False;bb = False;cc = False;dd = False;ee= False
#     ma26kt  = df1w['MA_26'].iloc[-1] < df1w['MA_26'].iloc[-2]
#     ma26duogang = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2] and (df1w['MA_26'].shift(0) < df1w['MA_26'].shift(1)).iloc[-6:].sum()>=1
#     ma26dt  = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2]
#     ma7x26 = (df1w['MA_7'] < df1w['MA_26']).iloc[-2:].sum() ==2
#     macdd0 = (df1w['MACD']>0).iloc[-2:].sum()==2
#     c8dc20 = df1w['close'].iloc[-8:].max() == df1w['close'].iloc[-20:].max() and df1w['close'].iloc[-8:].max() < df1w['close'].iloc[-40:].max()
#     c6dc10 = df1w['close'].iloc[-6:].max() == df1w['close'].iloc[-10:].max() and df1w['close'].iloc[-6:].max() < df1w['close'].iloc[-20:].max() 
#     macdduo = (df1w['MACD'].shift(0)> df1w['MACD'].shift(3)).iloc[-4:].sum() >=3 
#     macdduogang = df1w['MACD'].iloc[-7]<0 and df1w['MACD'].iloc[-1]>0
#     meifangliang = (df1w['VOL_5'] < df1w['VOL_30']).iloc[-7:].sum()>=4 or (df1w['volume']>df1w['VOL_30']*2).iloc[-10:].sum()<=1
#     nofangliang = (df1w['volume'] >df1w['VOL_30']*2).iloc[-15:].sum() <=1
#     if (ma26kt or ma26duogang) and c6dc10 and macdd0 and macdduo:
#         aa = True
#     if (ma26kt or ma26duogang) and c8dc20 and macdd0 and macdduo and meifangliang:
#         bb = True
#     if ma26dt and macdd0 and macdduogang  and c6dc10:
#         cc = True
#     if ma7x26 and macdduo and macdd0:
#         dd = True
#     if macdd0 and macdduo and nofangliang:
#         ee = True
#     return aa or bb or cc or dd or ee




#放量扣分
# def cxdtz_fangliang(i, date=None):
    # df1w = zhibiao(i, '1w', today=date)
    # #df1M = zhibiao(i, '1M', today=date)
    # aa = False#;bb = False;cc = False;dd = False
    # fangliang = (df1w['VOL_5'] > df1w['VOL_30']).iloc[-3:].sum() ==3 and (df1w['volume']>df1w['VOL_30']*2).iloc[-10:].sum()<=3 and (df1w['volume']>df1w['VOL_30']*2).iloc[-10:].sum()>=1 and (df1w['volume']>df1w['VOL_30']*4).iloc[-10:].sum()==0
    # macdd0 = (df1w['MACD']>0).iloc[-5:].sum()==5
    # c8dc20 = df1w['close'].iloc[-8:].max() == df1w['close'].iloc[-20:].max() and df1w['close'].iloc[-8:].max() < df1w['close'].iloc[-40:].max()
    # macdduo = (df1w['MACD'].shift(0)> df1w['MACD'].shift(3)).iloc[-6:].sum() >=4
    # if macdd0 and c8dc20 and macdduo and fangliang:
    #     aa = True 
    # return aa
    
# def cx_ding_baoliang(df):
#     df1w = zhibiao(df)
#     aa = False
#     baoliang = (df1w['volume']>df1w['VOL_30']*4).iloc[-7:].sum()>=2 or (df1w['volume']>df1w['VOL_30']*3).iloc[-7:].sum()>=3
#     c4dc40 = df1w['close'].iloc[-7:].max() == df1w['close'].iloc[-40:].max() 

#     if baoliang and c4dc40:
#         aa = True
#     return aa 
    


# #长线的最近10月涨幅
# def cx_10zf(df1w):
#     df1w = zhibiao(df1w)
#     zf == df1w['close'].iloc[-1] / df1w['close'].iloc[-10]  
#     return zf

# #长线的最近10月换手率
# def cx_10hsl(df1w):
#     df1w = zhibiao(df1w)
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_data = jiben[jiben['code'] == i]
#     ltp = stock_data['流通A股'].values[0]
#     hsl == df1w['volume'].iloc[-10:].sum() / ltp
#     return hsl






        


# #长线强趋势多头中，针对长线强趋势股来判断
# def cxqqs_dtz(i, date):
#     df1w = zhibiao(i, '1w', today=date)
#     aa = False
#     c2xc40 = df1w['close'].iloc[-1] < df1w['close'].iloc[-50:].max()
#     macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
#     if c2xc40 and macddt:
#         aa = True
#     return aa
# #中线的最近20日涨幅
# def zx_10zf(i, date):
#     df = zhibiao(i, '1d', today=date)
#     zf == df['close'].iloc[-1] / df['close'].iloc[-20]  
#     return zf

# #长线的最近10月换手率
# def zx_10hsl(i, date):
#     df = zhibiao(i, '1d', today=date)
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_data = jiben[jiben['code'] == i]
#     ltp = stock_data['流通A股'].values[0]
#     hsl == df['volume'].iloc[-10:].sum() / ltp
#     return hsl

# def get_daily_512880():
#     #df = adata.fund.market.get_market_etf(fund_code='512880',k_type=1)
#     df = adata.stock.market.get_market_index()
#     print(df)
#     df.to_csv('daily/512880.csv')


# def hangqing(stocklist):
#     #get_daily_512880()
#     for i in stocklist:
#         get_daily_data(i)
#         get_week_data(i)
#         get_month_data(i)
#         time.sleep(0.5)


# def get_jishu_zj(stocklist, date):
#     """
#     计算股票列表的技术指标信号
    
#     Args:
#         stocklist: 股票代码列表
#         date: 日期字符串，格式'YYYY-MM-DD'
    
#     Returns:
#         DataFrame包含每只股票的代码、名称和技术指标结果
#     """
#     # 获取股票基本信息
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_info = jiben[['code', 'name']]
    
#     # 创建结果存储字典
#     results = {
#         'code': [],        # 股票代码
#         'zj_jjdi': [],      # 中级接近底
#         'zj_di': [],      # 中级底
#         'zjdtg': [],      # 中级多头刚
#         'zjdtz': [],      # 中级多头中
#     }
    
#     # 遍历股票列表计算信号
#     for stock in stocklist:
#         results['code'].append(stock)
#         results['zj_jjdi'].append(zj_jjdi(stock, date))
#         results['zj_di'].append(zj_di(stock, date))
#         results['zjdtg'].append(zjdtg(stock, date))
#         results['zjdtz'].append(zjdtz(stock, date))
    
#     # 转换为DataFrame
#     df = pd.DataFrame(results)
    
#     # 合并股票名称信息
#     df = df.merge(stock_info, on='code', how='left')
    
#     # 调整列顺序，将code和name放在最前面
#     cols = ['code', 'name'] + [col for col in df.columns if col not in ['code', 'name']]
#     df = df[cols]
    
#     return df

# def get_jishu_cx(stocklist, date):
#     """
#     计算股票列表的长线技术指标信号
    
#     Args:
#         stocklist: 股票代码列表
#         date: 日期字符串，格式'YYYY-MM-DD'
    
#     Returns:
#         DataFrame包含每只股票的代码、名称和长线技术指标结果
#     """
#     # 获取股票基本信息
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_info = jiben[['code', 'name']]
    
#     # 创建结果存储字典
#     results = {
#         'code': [],          # 股票代码
#         'no_cxbz': [],      # 长线有暴涨
#         'cx_jjdi': [],      # 长线接近底部
#         'cx_di': [],      # 长线底
#         'cxdtg': [],         # 长线多头刚
#         'cxdtz': [],   # 长线多头中
#         'cx_ding_tzz': [],       # 长线顶调整中
#         'cx_ding_baoliang': []    # 长线顶部爆量
#     }
    
#     # 遍历股票列表计算信号
#     for stock in stocklist:
#         results['code'].append(stock)
#         results['no_cxbz'].append(no_cxbz(stock, date))
#         results['cx_jjdi'].append(cx_jjdi(stock, date))
#         results['cx_di'].append(cx_di(stock, date))
#         results['cxdtg'].append(cxdtg(stock, date))
#         results['cxdtz'].append(cxdtz(stock, date))
#         results['cx_ding_tzz'].append(cx_ding_tzz(stock, date))
#         results['cx_ding_baoliang'].append(cx_ding_baoliang(stock, date))
    
#     # 转换为DataFrame
#     df = pd.DataFrame(results)
    
#     # 合并股票名称信息
#     df = df.merge(stock_info, on='code', how='left')
    
#     # 调整列顺序，将code和name放在最前面
#     cols = ['code', 'name'] + [col for col in df.columns if col not in ['code', 'name']]
#     df = df[cols]
    
#     return df


# def get_jishu_ccx(stocklist, date):
#     """
#     计算股票列表的超长线技术指标信号
    
#     Args:
#         stocklist: 股票代码列表
#         date: 日期字符串，格式'YYYY-MM-DD'
    
#     Returns:
#         DataFrame包含每只股票的代码、名称和超长线技术指标结果
#     """
#     # 获取股票基本信息
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_info = jiben[['code', 'name']]
    
#     # 创建结果存储字典
#     results = {
#         'code': [],          # 股票代码
#         'no_ccxbz': [],     # 超长线有暴涨
#         'ccx_jjdi': [],     # 超长线接近底部
#         'ccx_di': [],        # 超长线底部区域
#         'ccxdtg': [],   # 超长线多头刚
#         'ccxdtz': []   # 超长线多头中
#     }
    
#     # 遍历股票列表计算信号
#     for stock in stocklist:
#         results['code'].append(stock)
#         results['no_ccxbz'].append(no_ccxbz(stock, date))
#         results['ccx_jjdi'].append(ccx_jjdi(stock, date))
#         results['ccx_di'].append(ccx_di(stock, date))
#         results['ccxdtg'].append(ccxdtg(stock, date))
#         results['ccxdtz'].append(ccxdtz(stock, date))
    
#     # 转换为DataFrame
#     df = pd.DataFrame(results)
    
#     # 合并股票名称信息
#     df = df.merge(stock_info, on='code', how='left')
    
#     # 调整列顺序，将code和name放在最前面
#     df = df[['code', 'name', 'no_ccxbz', 'ccx_jjdi', 'ccx_di', 'ccxdtg', 'ccxdtz']]
    
#     return df


# def zjcxccx_scores(df_zj, df_cx, df_ccx):
#     """
#     根据三个技术指标DataFrame计算股票得分
    
#     Args:
#         df_zj: 中级技术指标DataFrame
#         df_cx: 长线技术指标DataFrame
#         df_ccx: 超长线技术指标DataFrame
    
#     Returns:
#         DataFrame包含每只股票的代码、名称和各项得分，按total_score降序排列
#     """
#     # 获取股票基本信息
#     jiben = pd.read_csv(r'E:\stock_data\jibenzhuli_new.csv', dtype={'code': str})
#     stock_info = jiben[['code', 'name']]
    
#     # 计算中级技术指标得分
#     zj_scores = pd.DataFrame()
#     zj_scores['code'] = df_zj['code']  # Keep code as column instead of index
#     zj_scores['zjjjdi_score'] = df_zj['zj_jjdi'] * 1.0
#     zj_scores['zjdi_score'] = df_zj['zj_di'] * 2.0
#     zj_scores['zjdtg_score'] = df_zj['zjdtg'] * 2.0
#     zj_scores['zjdtz_score'] = df_zj['zjdtz'] * 0
#     # 取最大值
#     zj_scores['zj_score'] = zj_scores[['zjjjdi_score','zjdi_score', 'zjdtg_score', 'zjdtz_score']].max(axis=1)
    
#     # 计算长线技术指标得分
#     cx_scores = pd.DataFrame()
#     cx_scores['code'] = df_cx['code']  # Keep code as column instead of index
#     cx_scores['cx_jjdi_score'] = df_cx['cx_jjdi'] * 0.5  # 这个保持独立
#     cx_scores['cx_di_score'] = df_cx['cx_di'] * 2
#     cx_scores['cxdtg_score'] = df_cx['cxdtg'] * 4
#     cx_scores['cxdtz_score'] = df_cx['cxdtz'] * 0.5

#     cx_scores['cx_ding_baoliang_score'] = df_cx['cx_ding_baoliang'] * -1
#     cx_scores['cx_ding_tzz_score'] = df_cx['cx_ding_tzz'] * -1
#     cx_scores['no_cxbz_score'] = df_cx['no_cxbz'] * -1
#     # 取最大值（除了no_cxbz）
#     cx_scores['cx_final_score'] = cx_scores[['cx_jjdi_score','cx_di_score', 'cxdtg_score', 'cxdtz_score']].max(axis=1)
#     cx_scores['cx_score'] = cx_scores['cx_final_score'] + cx_scores['cx_ding_baoliang_score'] + cx_scores['cx_ding_tzz_score'] + cx_scores['no_cxbz_score']
    
#     # 计算超长线技术指标得分
#     ccx_scores = pd.DataFrame()
#     ccx_scores['code'] = df_ccx['code']  # Keep code as column instead of index
#     # ccx_scores['no_ccxbz_score'] = df_ccx['no_ccxbz'] * -1  # 这个保持独立
#     ccx_scores['ccx_jjdi_score'] = df_ccx['ccx_jjdi'] * 1
#     ccx_scores['ccx_di_score'] = df_ccx['ccx_di'] * 3
#     ccx_scores['ccxdtg_score'] = df_ccx['ccxdtg'] * 5
#     ccx_scores['ccxdtz_score'] = df_ccx['ccxdtz'] * 1
    
#     ccx_scores['no_ccxbz_score'] = df_ccx['no_ccxbz'] * -2
    
#     # 取最大值（除了no_ccxbz）
#     ccx_scores['ccx_final_score'] = ccx_scores[['ccx_jjdi_score', 'ccx_di_score', 'ccxdtg_score', 'ccxdtz_score']].max(axis=1)
#     ccx_scores['ccx_score'] = ccx_scores['ccx_final_score'] + ccx_scores['no_ccxbz_score']
    
#     # 合并所有得分
#     final_scores = zj_scores[['code', 'zj_score']].merge(
#         cx_scores[['code', 'cx_score']], on='code', how='left'
#     ).merge(
#         ccx_scores[['code', 'ccx_score']], on='code', how='left'
#     )
    
#     # 合并股票基本信息
#     final_scores = final_scores.merge(stock_info, on='code', how='left')
    
#     # 计算总分
#     final_scores['total_score'] = final_scores['zj_score'] + final_scores['cx_score'] + final_scores['ccx_score']
    
#     # 调整列顺序
#     final_scores = final_scores[['code', 'name', 'zj_score', 'cx_score', 'ccx_score', 'total_score']]
    
#     # 按total_score降序排序
#     final_scores = final_scores.sort_values(by='total_score', ascending=False)
    
#     return final_scores


# def jishu_scores(stocklist, date):
#     df_zj = get_jishu_zj(stocklist, date)
#     df_cx = get_jishu_cx(stocklist, date)
#     df_ccx = get_jishu_ccx(stocklist, date)
#     jishu_scores = zjcxccx_scores(df_zj, df_cx, df_ccx)
#     print("\n股票技术指标得分:")
#     print(jishu_scores)
#     return jishu_scores
    
# #只是测试日线的中级信号。
# def test_signals_daily(stock_code, start_date, end_date):
#     # get_daily_512880()
#     # 获取完整的历史数据
#     df_full = zhibiao(stock_code, '1d')
#     print(f"获取到的历史数据范围: {df_full['date'].min()} 到 {df_full['date'].max()}")
    
#     # 转换日期格式
#     df_full['date'] = pd.to_datetime(df_full['date'])
#     start_date = pd.to_datetime(start_date)
#     end_date = pd.to_datetime(end_date)
    
#     # 获取交易日期列表
#     date_list = get_trade_days(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
#     print(f"分析的交易日期范围: {date_list[0]} 到 {date_list[-1]}")
    
#     # 存储所有信号数据
#     all_signals = []
    
#     # 遍历每个交易日
#     for date in date_list:
#         # 将date转换为字符串格式
#         date_str = date.strftime('%Y-%m-%d')
#         # aa = cx_di(stock_code, date=date_str)
#         # bb = cxdtg(stock_code, date=date_str)  # 使用date_str而不是date
#         # cc = cxdtz(stock_code, date=date_str)
#         ff = cx_ding_tzz(stock_code, date=date_str)
#         if ff:  
#             # 如果有信号，获取当天的数据
#             signal_data = df_full[df_full['date'].dt.strftime('%Y-%m-%d') == date_str][['date', 'close', 'K', 'D', 'J','MACD','MA_7','MA_26','volume','VOL_30','VOL_5']]
#             all_signals.append(signal_data)
#             # print(f"在 {date_str} 发现信号")
    
#     # 合并所有信号数据
#     if all_signals:
#         final_signals = pd.concat(all_signals, ignore_index=True)
        
#         # 打印结果
#         print(f"\n分析时间段: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
#         print(f"股票代码: {stock_code}")
#         print(f"\n共发现 {len(final_signals)} 个信号")
#         print("\n详细信号数据:")
#         print(final_signals.to_string())
        
#         return final_signals
#     else:
#         print(f"\n在时间段 {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')} 内未发现信号")
#         return pd.DataFrame()    
    
