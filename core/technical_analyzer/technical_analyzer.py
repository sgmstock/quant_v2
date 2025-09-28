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
    def ccx_jjdi(self):
        df1M = self.df_monthly
        df1w = self.df_weekly
        df1d = self.df_daily
        aa = False;bb = False; cc = False
        ma26kt = df1M['MA_26'].iloc[-1] < df1M['MA_26'].iloc[-2]
        ma7x26 = df1M['MA_7'].iloc[-1] < df1M['MA_26'].iloc[-1]
        ma26dt = df1M['MA_26'].iloc[-1] > df1M['MA_26'].iloc[-2]
        macdkt = df1M['MACD'].iloc[-1] < df1M['MACD'].iloc[-4]
        macddt = df1M['MACD'].iloc[-1] > df1M['MACD'].iloc[-4]
        macdx0  = df1M['MACD'].iloc[-1] < 0
        macdx03 = (df1M['MACD']< 0).iloc[-3:].sum() ==3
        kxd4 = (df1M['K']<df1M['D']).iloc[-4:].sum() ==4
        vol5d30y2 = (df1M['VOL_5']>df1M['VOL_30']).iloc[-2:].sum() == 2 
        jx10 =  df1M['J'].iloc[-1]<10
        macdd0 = df1M['MACD'].iloc[-1] > 0
        kxd7 = (df1M['K']<df1M['D']).iloc[-7:].sum() ==7
        if ma26kt and ma7x26 and macdkt and macdx0 and kxd4 and vol5d30y2:
            aa = True
        if ma26dt and macdkt and (macdx03 or jx10):
            bb = True
        if macdd0 and kxd7:
            cc = True
        return aa or bb or cc
    
    #超长线底部
    def ccx_di(self):
        df1M = self.df_monthly
        df1w = self.df_weekly
        df = self.df_daily
        aa = False;bb = False; cc = False;dd = False;ee = False
        #趋势状态1
        ma26kt = df1M['MA_26'].iloc[-1] < df1M['MA_26'].iloc[-2]
        ma7x26 = df1M['MA_7'].iloc[-1] < df1M['MA_26'].iloc[-1]
        #波动或成交1
        macddt = df1M['MACD'].iloc[-1] > df1M['MACD'].iloc[-3]
        macdx0  = df1M['MACD'].iloc[-1] < 0
        kx35 = df1M['K'].iloc[-1] < 35
        #趋势状态2
        ma26dt = df1M['MA_26'].iloc[-1] > df1M['MA_26'].iloc[-2]
        #ma7x26
        #波动或成交2
        jx10 = df1M['J'].iloc[-1] < 10
        # macddt ,macdx0,kx35
        #趋势状态3,
        #ma26kt 
        macdd0y6 = (df1M['MACD']> 0).iloc[-6:].sum() == 6
        #波动或成交3
        kxdy6 = (df1M['K']<df1M['D'] ).iloc[-6:].sum() == 6
        j1d2 = df1M['J'].iloc[-1] > df1M['J'].iloc[-2]
        #波动或成交4
        jx10y2 = (df1M['J']< 10).iloc[-2:].sum() == 2
        #macdd0y1
        macdd0y1 = df1M['MACD'].iloc[-1]>0 and df1M['MACD'].iloc[-2]< 0
        
        if ma26kt and ma7x26 and macddt and macdx0 and kx35:
            aa = True
        if ma26dt and ma7x26 and macdx0 and kx35 and (macddt or jx10):
            bb = True
        if ma26kt and macdd0y6 and kxdy6 and j1d2:
            cc = True
        if ma26kt and ma7x26 and jx10y2:
            dd = True
        if macddt and (macdx0 or macdd0y1):
            ee = True
        return aa or bb or cc or dd or ee   
    #超长线多头刚
    def ccxdtg(self):
        df1M = self.df_monthly
        df1w = self.df_weekly
        df = self.df_daily
        aa = False;bb = False;cc = False;dd = False
        #趋势状态1
        ma20kty4 = (df1M['MA_20'].shift(0) < df1M['MA_20'].shift(1)).iloc[-8:].sum() >=3
        ma20dty1 = (df1M['MA_20'].shift(0) > df1M['MA_20'].shift(1)).iloc[-3:].sum() >=1 and df1M['MA_20'].iloc[-1] > df1M['MA_20'].iloc[-2]
        macddt = df1M['MACD'].iloc[-1] > df1M['MACD'].iloc[-3]
        #波动成交1
        c2x30  = df1M['close'].iloc[-2:].max() < df1M['close'].iloc[-30:].max()
        #趋势状态2
        ma26kt = df1M['MA_26'].iloc[-1] < df1M['MA_26'].iloc[-2]
        ma7x26 = df1M['MA_7'].iloc[-1] < df1M['MA_26'].iloc[-1]
        macdd0  = df1M['MACD'].iloc[-1] > 0
        #macddt
        #波动成交2
        c3x20  = df1M['close'].iloc[-3:].max() < df1M['close'].iloc[-20:].max()
        vol5d30 = (df1M['VOL_5']> df1M['VOL_30']).iloc[-2:].sum()>=1
        vold30y2 = (df1M['volume']> df1M['VOL_30']*1.1).iloc[-6:].sum()>=2
        
        # 趋势状态3,macddt
        macdd0x4 = (df1M['MACD']> 0).iloc[-6:].sum() <= 5 and df1M['MACD'].iloc[-1] > 0
        #波动成交3
        j1x2_2 = (df1M['J'].shift(0) < df1M['J'].shift(1)).iloc[-2:].sum() == 2
        #趋势状态4,macddt,macdx0
        ma26dt = df1M['MA_26'].iloc[-1] > df1M['MA_26'].iloc[-2]
        #波动成交4
        kdd = df1M['K'].iloc[-1] > df1M['D'].iloc[-1] and df1M['K'].iloc[-1] < 50
        macdx0 = df1M['MACD'].iloc[-1] < 0
        
        if ma20kty4 and ma20dty1 and macddt and c3x20:
            aa = True
        if ma26kt and ma7x26 and macdd0 and macddt and c3x20 and (vol5d30 or vold30y2):
            bb = True
        if macdd0x4 and macddt and (j1x2_2 or c3x20):
            cc = True
        if ma26dt and macddt and macdx0 and kdd :
            dd = True
        return aa or bb or cc or dd  
    #超长线多头中
    def ccxdtz(self):
        df1M = self.df_monthly
        aa = False;bb = False
        ma26dt = df1M['MA_26'].iloc[-1] < df1M['MA_26'].iloc[-2]
        macdd0 = df1M['MACD'].iloc[-1] >0
        macddt = df1M['MACD'].iloc[-1] > df1M['MACD'].iloc[-4]
        kxd = df1M['K'].iloc[-1] < df1M['D'].iloc[-1]
        if ma26dt and macdd0 and kxd:
            aa = True
        if ma26dt and macdd0 and macddt:
            bb = True
        return aa or bb
    #长线接近底部
    def cx_jjdi(self):
        df1w = self.df_weekly
        aa = False;bb = False;cc = False;dd = False
        #趋势状态1,暴跌放量
        ma26kt =df1w['MA_26'].iloc[-1]<df1w['MA_26'].iloc[-2]
        ma7x26 = df1w['MA_7'].iloc[-1] < df1w['MA_26'].iloc[-1]
        #波动或成交1
        kxd7 = (df1w['K']< df1w['D']).iloc[-7:].sum()==7
        vol5d30y3 = (df1w['VOL_5']> df1w['VOL_30']).iloc[-3:].sum()==3
        #趋势状态2,
        macdx0 = df1w['MACD'].iloc[-1] <0
        ma26dt = df1w['MA_26'].iloc[-1]>df1w['MA_26'].iloc[-2]
        #波动或成交2
        jx10 = df1w['J'].iloc[-1] <10
        #趋势状态3,
        macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
        #波动或成交4
        vol5x30 = df1w['VOL_5'].iloc[-1] < df1w['VOL_30'].iloc[-1]
        if ma26kt and ma7x26 and kxd7 and vol5d30y3:
            aa = True
        if ma26dt and macdx0 and (jx10 or ma7x26):
            bb = True
        if macdx0 and macddt:
            cc = True
        if ma26dt and macdx0 and vol5x30 :
            dd = True
        return aa or bb or cc or dd
        #长线底部区域
    def cx_di(self):
        df1w = self.df_weekly
        aa = False;bb = False;cc = False;dd = False;ee = False;ff = False;gg = False;hh = False;ii =False;jj = False;kk = False
        #趋势状态1
        ma26dt = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2]
        ma7d26 = df1w['MA_7'].iloc[-1] > df1w['MA_26'].iloc[-1] 
        #波动成交1
        jx10y1 = (df1w['J']<10).iloc[-2:].sum() >= 1
        kxd4 = (df1w['K'] < df1w['D']).iloc[-4:].sum()==4
        k1d2 = df1w['K'].iloc[-1] > df1w['K'].iloc[-2]
        kx35 = df1w['K'].iloc[-1] < 35
        cxma26_09 = df1w['close'].iloc[-1] < df1w['MA_26'].iloc[-1]*0.9
        macdx0y3 = (df1w['MACD']<0).iloc[-3:].sum()==3
        kxd6 = (df1w['K'] < df1w['D']).iloc[-6:].sum()==6 
        j1d2 = df1w['J'].iloc[-1] > df1w['J'].iloc[-2]  
        jx10 = df1w['J'].iloc[-1] <10
        k1d2 = df1w['K'].iloc[-1] > df1w['K'].iloc[-2]
        macdd0 = df1w['MACD'].iloc[-1] >0 
        c2xc6 = df1w['close'].iloc[-2:].max() < df1w['close'].iloc[-6:].max()
        #趋势状态2
        #ma26dt
        ma7x26 = df1w['MA_7'].iloc[-1] < df1w['MA_26'].iloc[-1] 
        #波动成交2
        kxd2  = (df1w['K'] < df1w['D']).iloc[-2:].sum()==2
        #j1d2 ,jx10 
        macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
        macdx0 = df1w['MACD'].iloc[-1] <0
        #趋势状态3
        ma26kt = df1w['MA_26'].iloc[-1] < df1w['MA_26'].iloc[-2] 
        #ma7x26
        #波动成交3
        c2xc5 = df1w['close'].iloc[-2:].max() < df1w['close'].iloc[-5:].max()
        #macddt,,jx10 ;kxd2,k1d2
        kx20 = df1w['K'].iloc[-1] < 20
        kdd = df1w['K'].iloc[-1] > df1w['D'].iloc[-1]
        if ma26dt and ma7d26 and jx10y1 :
            aa = True
        if ma26dt and ma7d26 and kxd4 and k1d2 and kx35:
            bb = True
        if ma26dt and ma7d26 and cxma26_09:
            cc = True
        if ma26dt and ma7d26 and macdx0y3 and c2xc6:
            dd = True
        if ma26dt and ma7d26 and kxd6 and (j1d2 or jx10):#去掉c2xc6
            ee = True
        if ma26dt and ma7d26 and kxd6 and k1d2 and macdd0:
            ff = True
        #--------------------
        if ma26dt and ma7x26 and kxd2 and (j1d2 or jx10):
            gg = True
        if ma26dt and ma7x26 and macddt and macdx0:
            hh = True
        #--------------------
        if ma26kt and ma7x26 and c2xc5 and macddt and jx10:
            ii = True
        if ma26kt and ma7x26 and kxd2 and kx20 and k1d2:
            jj = True
        if ma26kt and ma7x26 and kdd and kx35:
            kk = True
        return aa or bb or cc or dd or ee or ff or gg or hh or ii or jj or kk
    def cxdtg(self):
        df1w = self.df_weekly
        #df1M = zhibiao(i, '1M', today=date)
        aa = False;bb = False;cc = False;dd = False;ee=False;ff = False
        #趋势状态1
        ma26dt = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2]
        c5xc20 = df1w['close'].iloc[-5:].max() < df1w['close'].iloc[-20:].max()
        #波动或成交1
        macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
        macdx0y1 = (df1w['MACD']<0).iloc[-7:].sum() >= 1 
        
        kx60 = df1w['K'].iloc[-1] < 60
        #波动成交2，趋势状态同1
        kxdy4 = (df1w['K']<df1w['D']).iloc[-4:].sum()==4
        k1d2 = df1w['K'].iloc[-1] > df1w['K'].iloc[-2]
        kx35 = df1w['K'].iloc[-1] < 35
        kxd = df1w['K'].iloc[-1] < df1w['D'].iloc[-1]
        #波动成交3，趋势状态同1
        #macddt,kx60
        vol5xvol30y2 = (df1w['VOL_5']<df1w['VOL_30']).iloc[-3:].sum() >= 2
        #趋势状态2
        ma26kt = df1w['MA_26'].iloc[-1] < df1w['MA_26'].iloc[-2]
        ma7x26 = df1w['MA_7'].iloc[-1] < df1w['MA_26'].iloc[-1]
        # c5xc10 = df1w['close'].iloc[-5:].max() < df1w['close'].iloc[-10:].max()
        #macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
        #波动或成交4
        kdd = df1w['K'].iloc[-1] > df1w['D'].iloc[-1]
        # kx35 
        #波动成5
        #macddt,kx60
        macdd01 = df1w['MACD'].iloc[-1] > -0.1
        #趋势状态3 ma26dt,ma7d26,macddt
        ma7d26 = df1w['MA_7'].iloc[-1] > df1w['MA_26'].iloc[-1]
        #波动成交6
        macdd0_x0y1 = df1w['MACD'].iloc[-1] > 0 and (df1w['MACD']<0).iloc[-6:].sum() >= 1   
        vol5xvol30_1d2 = (df1w['VOL_5']<df1w['VOL_30']).iloc[-2:].sum() == 2 and df1w['VOL_5'].iloc[-1]> df1w['VOL_5'].iloc[-2]

        if ma26dt and c5xc20 and macddt and  macdx0y1 and kx60 and k1d2:
            aa = True
        if ma26dt and c5xc20 and kxdy4 and k1d2 and (kx35 or kxd):
            bb = True
        if ma26dt and c5xc20 and vol5xvol30y2 and kx60 and k1d2:
            cc = True
        if ma26kt and ma7x26 and kdd and kx35:
            dd = True
        if ma26kt and ma7x26 and macddt and macdd01 and kx60 and k1d2:
            ee = True
        if ma26dt and ma7d26 and macdd0_x0y1 and macddt and vol5xvol30_1d2: 
            ff = True
        return aa or bb or cc or dd or ee or ff
    #长线多头中
    def cxdtz(self):
        df1w = self.df_weekly
        df = self.df_daily
        aa = False;bb = False;cc = False
        ma26dt  = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2]
        macdd0 = df1w['MACD'].iloc[-1]>0
        macddt = df1w['MACD'].iloc[-1] > df1w['MACD'].iloc[-4]
        d_ma26dt = df['MA_26'].iloc[-1] > df['MA_26'].iloc[-2]
        kxd = df1w['K'].iloc[-1] > df1w['D'].iloc[-1]
        if ma26dt and macdd0 and macddt:
            aa = True
        if ma26dt and d_ma26dt:
            bb = True
        if ma26dt and macdd0 and kxd:
            cc = True
        return aa or bb or cc
    def cx_ding_tzz(self):
        df1w = self.df_weekly
        df1M = self.df_monthly
        aa = False;bb = False;cc = False;dd = False
        #趋势状态1 月线KDJ偏高,放量.
        kd65 = df1M['K'].iloc[-1] > 65
        jd90 = df1M['J'].iloc[-1] > 90
        fangliang = (df1M['volume']>df1M['VOL_30']*3).iloc[-3:].sum()>=1 or (df1M['volume']>df1M['VOL_30']*2).iloc[-4:].sum()>=2
        #周线多头高位
        ma26dt = df1w['MA_26'].iloc[-1] > df1w['MA_26'].iloc[-2]
        macdd0 = df1w['MACD'].iloc[-1] > 0
        baoliang = (df1w['volume']>df1w['VOL_30']*5).iloc[-6:].sum()>=1 or (df1w['volume']>df1w['VOL_30']*3).iloc[-8:].sum()>=4
        w_cdc20 = df1w['close'].iloc[-3:].max() == df1w['close'].iloc[-20:].max()
        #趋势状态2 ma26dt 
        ma7d26 = df1w['MA_7'].iloc[-1] > df1w['MA_26'].iloc[-1]
        macdd0y6 = (df1w['MACD']>0).iloc[-6:].sum()==6
        #波动成交
        c4dc40 = df1w['close'].iloc[-4:].max() == df1w['close'].iloc[-40:].max() 
        kddy7 = (df1w['K'] > df1w['D']).iloc[-7:].sum()==7
        kd75y1 = (df1w['K'] > 75).iloc[-3:].sum()>=1
        macdkt = df1w['MACD'].iloc[-1]<df1w['MACD'].iloc[-4]

        if (kd65 or jd90 or fangliang) and w_cdc20 and macdd0:
            aa = True
        if ma26dt and macdd0 and baoliang and w_cdc20:
            bb = True
        if ma26dt and ma7d26 and macdd0y6 and c4dc40 and baoliang and macdd0:
            cc = True
        if ma26dt and ma7d26 and kddy7 and kd75y1 and baoliang and macdd0:
            dd = True
        return aa or bb or cc or dd
    def cx_ding_baoliang(self):
        df1w = self.df_weekly
        aa = False
        baoliang = (df1w['volume']>df1w['VOL_30']*4).iloc[-7:].sum()>=2 or (df1w['volume']>df1w['VOL_30']*3).iloc[-7:].sum()>=3
        c4dc40 = df1w['close'].iloc[-7:].max() == df1w['close'].iloc[-40:].max() 

        if baoliang and c4dc40:
            aa = True
        return aa 

    def zj_jjdi(self):
        df = self.df_daily
        aa = False
        macdx0 = df['MACD'].iloc[-1]<0
        bdqs_di = df['J'].iloc[-1]<10 or ((df['K']<df['D']).iloc[-4:].sum()==4 and (df['J'].iloc[-1]>df['J'].iloc[-2]) and (df['J'].iloc[-1]<50))
        if macdx0 or bdqs_di:
            aa = True
        return aa
    def zj_di(self):
        df = self.df_daily
        aa = False;bb = False;cc = False;dd = False;ee = False;ff = False;gg = False;hh = False;ii = False
        #趋势状态1
        ma26dt = df['MA_26'].iloc[-1] > df['MA_26'].iloc[-2]
        ma7d26 = df['MA_7'].iloc[-1] > df['MA_26'].iloc[-1]
        #波动成交1
        kxdy4 = (df['K'] < df['D']).iloc[-4:].sum()==4 
        j1d2x30 = df['J'].iloc[-1]>df['J'].iloc[-2] and df['J'].iloc[-1]<30
        jx10y2 = (df['J']<10).iloc[-2:].sum()==2
        #波动成交2
        kxd5y4 = (df['K'] < df['D']).iloc[-5:].sum()>=4
        k1d2 = df['K'].iloc[-1] > df['K'].iloc[-2]
        kx35 = df['K'].iloc[-1] < 35
        #波动成交3
        macdx0y3 = (df['MACD']<0).iloc[-3:].sum()==3
        ma7ma26pd =min(df['MA_7'].iloc[-1],df['MA_26'].iloc[-1]) + abs(df['MA_7'].iloc[-1]-df['MA_26'].iloc[-1]) *0.15
        cxma7ma26 = df['close'].iloc[-1]<ma7ma26pd 
        #趋势状态2
        #ma26dt
        ma7x26 = df['MA_7'].iloc[-1] < df['MA_26'].iloc[-1]
        #波动成交4
        #k1d2,kx35
        #趋势状态3,MA26dt
        #波动成交5
        kxdy7 = (df['K'] < df['D']).iloc[-7:].sum()==7
        #j1d2x30
        jx0y2 =(df['J']< 0).iloc[-2:].sum()==2
        #趋势状态4,ma7x26
        ma26kt = df['MA_26'].iloc[-1] < df['MA_26'].iloc[-2]
        #波动成交6
        macddt = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]
        macdx0 = (df['MACD']<0).iloc[-3:].sum()==3 
        kx50 =   df['K'].iloc[-1] < 50
        #波动成交7
        c3xc40 = df['close'].iloc[-3:].min() == df['close'].iloc[-40:].min()
        kxd8y7 = (df['K'] < df['D']).iloc[-8:].sum()>=7
        kdd = df['K'].iloc[-1] > df['D'].iloc[-1]
        #波动成交8
        j2x0 = df['J'].iloc[-2] < 0 
        if ma26dt and ma7d26 and kxdy4 and (j1d2x30 or jx10y2):
            aa = True
        if ma26dt and ma7d26 and kxd5y4 and k1d2 and kx35:
            bb = True
        if ma26dt and ma7d26 and macdx0y3 and cxma7ma26:
            cc = True
        if ma26dt and ma7x26 and k1d2 and kx35:
            dd = True
        if ma26dt and kxdy7 and j1d2x30:
            ee = True
        if ma26dt and jx0y2:
            ff = True
        if ma26kt and ma7x26 and macdx0 and macddt and kx50:
            gg = True
        if c3xc40 and kxd8y7 and kdd:
            hh = True
        if j2x0 and j1d2x30:
            ii = True
        return aa or bb or cc or dd or ee or ff or gg or hh or ii
    def zjdtg(self):
        df = self.df_daily
        df1w = self.df_weekly
        aa = False; bb = False;cc = False;dd = False;ee = False
        #汇总几个拐点时机
        ma20duogang = df['MA_20'].iloc[-1] > df['MA_20'].iloc[-2] and (df['MA_20'].shift(0) < df['MA_20'].shift(1)).iloc[-7:].sum()>=1
        macdduogang = df['MACD'].iloc[-1] > 0 and (df['MACD'] < 0).iloc[-7:].sum()>=1
        ma7d26gang = df['MA_7'].iloc[-1] > df['MA_26'].iloc[-1] and (df['MA_7'].shift(0) < df['MA_26'].shift(1)).iloc[-7:].sum()>=1
        duogang = ma20duogang or ma7d26gang or macdduogang
        #趋势状态1
        ma26kt = df['MA_26'].iloc[-1] < df['MA_26'].iloc[-2]
        #波动成交1
        macddt = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]
        macdd0 = df['MACD'].iloc[-1] > 0
        c5xc20 = df['close'].iloc[-5:].max() < df['close'].iloc[-20:].max() 
        k1d2 = df['K'].iloc[-1] > df['K'].iloc[-2]
        kx60 = df['K'].iloc[-1] < 60
        #趋势状态2
        ma26dt = df['MA_26'].iloc[-1] > df['MA_26'].iloc[-2] or df['MA_26'].iloc[-1] > df['MA_26'].iloc[-4]
        #波动成交2,macdd0,macddt
        c10xc40 = df['close'].iloc[-10:].max() < df['close'].iloc[-40:].max() 
        #趋势状态3,无
        #波动成交3，macdd0
        macddtduo=  (df['MACD'].shift(0) > df['MACD'].shift(3)).iloc[-7:].sum()>=4
        kxd = df['K'].iloc[-1]<df['D'].iloc[-1]
        cxma7 = df['close'].iloc[-1]<df['MA_7'].iloc[-1]
        kx70 = df['K'].iloc[-1] < 70
        #波动状态4
        ma7d = df['MA_7'].iloc[-1] > df['MA_7'].iloc[-2]
        vol5d = df['VOL_5'].iloc[-1] > df['VOL_5'].iloc[-2]
        vol5x30 = df['VOL_5'].iloc[-1] < df['VOL_30'].iloc[-1]*1.1
        duanduo = ma7d and vol5d and vol5x30
        #波动状态5，ma26dt,macddt,c10xc40,DUANDUO,
        jx100 = df['J'].iloc[-1]<100
    
        if ma26kt and duogang and macdd0 and macddt  and c5xc20 and kx70 and jx100: #kx60 and (c5xc20 or k1d2):
            aa = True
        if ma26dt and duogang and macdd0 and macddt and c10xc40 and kx60:
            bb = True
        if duogang and c10xc40 and macdd0 and macddtduo and kx70 and (kxd or cxma7):
            cc = True
        if ma26kt and macdd0 and macddt and c5xc20 and duanduo and kx70 and jx100:
            dd = True
        if ma26dt and macddt and c10xc40 and duanduo and kx70 and jx100:
            ee = True

        return aa or bb or cc or dd or ee

    def zjdtz(self):
        df = self.df_daily
        aa = False;bb = False#;cc = False
        #趋势状态1
        ma26dt = df['MA_26'].iloc[-1] > df['MA_26'].iloc[-2]
        ma26kt = df['MA_26'].iloc[-1] < df['MA_26'].iloc[-2]
        ma7d26 = df['MA_7'].iloc[-1] > df['MA_26'].iloc[-1]
        macdd0 = df['MACD'].iloc[-1] > 0
        fangliang = (df['VOL_5']>df['VOL_30']).iloc[-2:].sum()==2
        c5dcx20 = df['close'].iloc[-5:].max() == df['close'].iloc[-20:].max()
        c5xc60 = df['close'].iloc[-5:].max() < df['close'].iloc[-60:].max()
        macddt = df['MACD'].iloc[-1] > df['MACD'].iloc[-4]
        if ma7d26 and macdd0 and fangliang and c5dcx20 and macddt:
            aa = True
        if ma26kt and ma7d26 and macdd0 and fangliang and macddt:
            bb = True

        return aa or bb #or cc
        
    def bdcz_zjdt(self):
        df=self.df_daily
        df1w = self.df_weekly
        aa = False; bb = False;cc = False;dd = False
        #趋势状态1 ，
        macddt = df['MACD'].iloc[-1]>df['MACD'].iloc[-4]
        #波动成交1:macd大于0，只有2个以内的MACD>0，vol5大于30只有1个。
        macdd0 = df['MACD'].iloc[-1]>0
        macdx0y1 = (df['MACD']<0).iloc[-3:].sum()>=1
        vol5xvol30 = (df['VOL_5']<df['VOL_30']).iloc[-2:].sum()>=1 
        #波动成交2
        c3xc40 = df['close'].iloc[-3:].max() < df['close'].iloc[-40:].max()
        #趋势状态2，长线底部
        w_macddt = df1w['MACD'].iloc[-1]>df1w['MACD'].iloc[-4]
        w_macdx0 = (df1w['MACD']<0).iloc[-2:].sum()==2
        #限制条件，最近2日没有成交量大于vol30*2
        volxvol30 = (df['volume']>df['VOL_30']*2).iloc[-2:].sum()==0
        
        if macddt and macdd0 and macdx0y1 and vol5xvol30:
            aa = True
        if macddt and macdd0 and c3xc40 and vol5xvol30:
            bb = True
        if w_macddt and w_macdx0 and c3xc40:
            cc = True
        return  (aa or bb or cc) and volxvol30


    def zjqs_ding(self):
        df = self.df_daily
        aa = False;bb = False;cc = False
        #趋势状况
        cd40 = df['close'].iloc[-7:].max() == df['close'].iloc[-40:].max()
        macdd0y8 = (df['MACD']>0).iloc[-8:].sum() ==8 
        #小周期
        kd80_1x2 = df['K'].iloc[-2] > 80 and df['K'].iloc[-1] < df['K'].iloc[-2]
        jd100y3= (df['J'] > 100).iloc[-3:].sum()==3  and df['J'].iloc[-1] < df['J'].iloc[-2]
        macdgzk = (df['MACD']>0).iloc[-6:].sum()==6 and df['MACD'].iloc[-1]<df['MACD'].iloc[-4]
        #----
        baoliang = (df['volume']>df['VOL_30']*4).iloc[-4:].sum()>=1 or (df['volume']>df['VOL_30']*3).iloc[-5:].sum()>=2
        
        if cd40 and macdd0y8 and  (kd80_1x2 or jd100y3 or macdgzk):
            aa = True
        if cd40 and baoliang and kd80_1x2:
            bb = True

        return aa or bb

    def zjtzz(self):
        df=self.df_daily
        aa = False;bb = False
        ma26dt = df['MA_26'].iloc[-1]>df['MA_26'].iloc[-2]
        ma26kt = df['MA_26'].iloc[-1]<df['MA_26'].iloc[-2]
        if ma26dt and df['MACD'].iloc[-1] < 0 and df['MACD'].iloc[-1] < df['MACD'].iloc[-4]:
            aa = True
        if ma26kt and df['MACD'].iloc[-1] <0 and df['MA_7'].iloc[-1] < df['MA_26'].iloc[-1] and df['MACD'].iloc[-1] < df['MACD'].iloc[-4]:
            bb = True
        return aa or bb

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
    
