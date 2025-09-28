from ast import And
from pickle import FALSE
import adata
import os
import pandas as pd
import akshare as ak
import numpy as np
from pandas.core.nanops import F
from core.utils.indicators import *
import datetime
import time
from data_management.database_manager import DatabaseManager
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer
from data_management.data_processor import get_daily_data_for_backtest, get_weekly_data_for_backtest, get_monthly_data_for_backtest



class StockTechnicalAnalyzer:
    """股票技术指标分析器类"""
    
    def __init__(self):
        """初始化分析器"""
        self.db_conn = None
        self._shared_db_manager = None
    
    def _get_shared_db_manager(self):
        """获取共享的数据库管理器实例"""
        if self._shared_db_manager is None:
            self._shared_db_manager = DatabaseManager()
        return self._shared_db_manager
    
    def _get_stock_names(self, stocklist):
        """从数据库获取股票名称"""
        try:
            # 使用共享的数据库管理器
            db_manager = self._get_shared_db_manager()
            
            # 构建查询语句获取股票名称
            placeholders = ','.join(['?' for _ in stocklist])
            name_query = f"""
            SELECT stock_code, stock_name 
            FROM stock_basic 
            WHERE stock_code IN ({placeholders})
            """
            name_df = db_manager.execute_query(name_query, tuple(stocklist))
            
            if not name_df.empty:
                return dict(zip(name_df['stock_code'], name_df['stock_name']))
            else:
                return {}
        except Exception as e:
            print(f"获取股票名称失败: {e}")
            return {}
    
    def _prepare_technical_data_for_stock(self, stock_code, date):
        """
        为股票准备技术分析所需的所有周期数据
        
        Args:
            stock_code: 股票代码
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            dict: 包含日线、周线、月线数据的字典，如果失败返回None
        """
        try:
            # 获取各周期数据 - 使用与quant_cur相同的方式
            from data_management.data_processor import get_daily_data_for_backtest, get_weekly_data_for_backtest, get_monthly_data_for_backtest
            
            db_manager = self._get_shared_db_manager()
            df_daily = get_daily_data_for_backtest(stock_code, date, db_manager)
            df_weekly = get_weekly_data_for_backtest(stock_code, date, db_manager)
            df_monthly = get_monthly_data_for_backtest(stock_code, date, db_manager)
            
            # 检查数据是否足够
            if df_daily.empty or df_weekly.empty or df_monthly.empty:
                print(f"警告：股票 {stock_code} 的数据不足，跳过分析")
                return None
            
            # 返回数据字典
            return {
                'daily': df_daily,
                'weekly': df_weekly,
                'monthly': df_monthly
            }
            
        except Exception as e:
            print(f"为股票 {stock_code} 准备技术数据失败: {e}")
            return None
    
    def get_jishu_zj(self, stocklist, date):
        """
        计算股票列表的中级技术指标信号并评分
        
        Args:
            stocklist: 股票代码列表
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            DataFrame包含每只股票的代码、名称、技术指标结果和评分
        """
        try:
            # 获取股票名称
            stock_names = self._get_stock_names(stocklist)
            
            # 创建结果存储字典
            results = {
                'stock_code': [],        # 股票代码
                'stock_name': [],        # 股票名称
                'zj_jjdi': [],          # 中级接近底
                'zj_di': [],            # 中级底
                'zjdtg': [],            # 中级多头刚
                'zjdtz': [],            # 中级多头中
            }
            
            # 遍历股票列表计算信号
            for stock in stocklist:
                results['stock_code'].append(stock)
                results['stock_name'].append(stock_names.get(stock, ''))
                
                try:
                    # 准备技术数据
                    data_dict = self._prepare_technical_data_for_stock(stock, date)
                    
                    if data_dict is None:
                        # 数据不足，设置默认值
                        results['zj_jjdi'].append(False)
                        results['zj_di'].append(False)
                        results['zjdtg'].append(False)
                        results['zjdtz'].append(False)
                        continue
                    
                    # 创建技术分析器
                    analyzer = TechnicalAnalyzer(data_dict)
                    
                    # 计算中级技术指标
                    results['zj_jjdi'].append(analyzer.zj_jjdi())
                    results['zj_di'].append(analyzer.zj_di())
                    results['zjdtg'].append(analyzer.zjdtg())
                    results['zjdtz'].append(analyzer.zjdtz())
                    
                except Exception as e:
                    print(f"计算股票 {stock} 中级技术指标失败: {e}")
                    results['zj_jjdi'].append(False)
                    results['zj_di'].append(False)
                    results['zjdtg'].append(False)
                    results['zjdtz'].append(False)
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            # 计算评分
            df['zjjjdi_score'] = df['zj_jjdi'] * 1.0
            df['zjdi_score'] = df['zj_di'] * 2.0
            df['zjdtg_score'] = df['zjdtg'] * 2.0
            df['zjdtz_score'] = df['zjdtz'] * 0.0
            # 取最大值作为中级技术评分
            df['zj_score'] = df[['zjjjdi_score', 'zjdi_score', 'zjdtg_score', 'zjdtz_score']].max(axis=1)
            
            return df
            
        except Exception as e:
            print(f"计算中级技术指标失败: {e}")
            return pd.DataFrame()
    def get_ma26ruo(self, stocklist, date):
        """
        计算股票列表的MA26R技术指标信号并评分
        """
        try:
            # 获取股票名称
            stock_names = self._get_stock_names(stocklist)
            
            # 创建结果存储字典
            results = {
                'stock_code': [],        # 股票代码
                'stock_name': [],        # 股票名称
                'ma26ruo': [],            # MA26R弱
            }
            
            # 遍历股票列表计算信号
            for stock in stocklist:
                results['stock_code'].append(stock)
                results['stock_name'].append(stock_names.get(stock, ''))
                
                try:
                    # 准备技术数据
                    data_dict = self._prepare_technical_data_for_stock(stock, date)
                    
                    if data_dict is None:
                        # 数据不足，设置默认值
                        results['ma26ruo'].append(False)
                        continue
                    
                    # 创建技术分析器
                    analyzer = TechnicalAnalyzer(data_dict)
                    
                    # 计算MA26R技术指标
                    results['ma26ruo'].append(analyzer.ma26ruo())
                    
                except Exception as e:
                    print(f"计算股票 {stock} MA26R技术指标失败: {e}")
                    results['ma26ruo'].append(False)
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            # 计算评分
            df['ma26ruo_score'] = df['ma26ruo'] * (-3.5)
            
            return df
            
        except Exception as e:
            print(f"计算MA26R技术指标失败: {e}")
            return pd.DataFrame()
            



    def get_jishu_cx(self, stocklist, date):
        """
        计算股票列表的长线技术指标信号并评分
        
        Args:
            stocklist: 股票代码列表
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            DataFrame包含每只股票的代码、名称、技术指标结果和评分
        """
        try:
            # 获取股票名称
            stock_names = self._get_stock_names(stocklist)
            
            # 创建结果存储字典
            results = {
                'stock_code': [],        # 股票代码
                'stock_name': [],        # 股票名称
                'cx_jjdi': [],          # 长线接近底
                'cx_di': [],            # 长线底
                'cxdtg': [],            # 长线多头刚
                'cxdtz': [],            # 长线多头中
                'cx_ding_tzz': [],      # 长线顶特征
                'cx_ding_baoliang': [], # 长线顶爆量
            }
            
            # 遍历股票列表计算信号
            for stock in stocklist:
                results['stock_code'].append(stock)
                results['stock_name'].append(stock_names.get(stock, ''))
                
                try:
                    # 准备技术数据
                    data_dict = self._prepare_technical_data_for_stock(stock, date)
                    
                    if data_dict is None:
                        # 数据不足，设置默认值
                        results['cx_jjdi'].append(False)
                        results['cx_di'].append(False)
                        results['cxdtg'].append(False)
                        results['cxdtz'].append(False)
                        results['cx_ding_tzz'].append(False)
                        results['cx_ding_baoliang'].append(False)
                        continue
                    
                    # 创建技术分析器
                    analyzer = TechnicalAnalyzer(data_dict)
                    
                    # 计算长线技术指标
                    results['cx_jjdi'].append(analyzer.cx_jjdi())
                    results['cx_di'].append(analyzer.cx_di())
                    results['cxdtg'].append(analyzer.cxdtg())
                    results['cxdtz'].append(analyzer.cxdtz())
                    results['cx_ding_tzz'].append(analyzer.cx_ding_tzz())
                    results['cx_ding_baoliang'].append(analyzer.cx_ding_baoliang())
                    
                except Exception as e:
                    print(f"计算股票 {stock} 长线技术指标失败: {e}")
                    results['cx_jjdi'].append(False)
                    results['cx_di'].append(False)
                    results['cxdtg'].append(False)
                    results['cxdtz'].append(False)
                    results['cx_ding_tzz'].append(False)
                    results['cx_ding_baoliang'].append(False)
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            # 计算评分
            df['cx_jjdi_score'] = df['cx_jjdi'] * 1.0
            df['cxdi_score'] = df['cx_di'] * 3.0
            df['cxdtg_score'] = df['cxdtg'] * 4.5
            df['cxdtz_score'] = df['cxdtz'] * 1.0
            df['cx_ding_tzz_score'] = df['cx_ding_tzz'] * (-0.5)  # 负面信号扣分
            df['cx_ding_baoliang_score'] = df['cx_ding_baoliang'] * (-0.5)  # 负面信号扣分
            # 取最大值作为长线技术评分
            df['cx_final_score'] = df[['cx_jjdi_score','cxdi_score', 'cxdtg_score', 'cxdtz_score']].max(axis=1)
            df['cx_score'] = df['cx_final_score'] + df['cx_ding_baoliang_score'] + df['cx_ding_tzz_score']


            return df
            
        except Exception as e:
            print(f"计算长线技术指标失败: {e}")
            return pd.DataFrame()
    
    def get_jishu_ccx(self, stocklist, date):
        """
        计算股票列表的超长线技术指标信号并评分
        
        Args:
            stocklist: 股票代码列表
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            DataFrame包含每只股票的代码、名称、技术指标结果和评分
        """
        try:
            # 获取股票名称
            stock_names = self._get_stock_names(stocklist)
            
            # 创建结果存储字典
            results = {
                'stock_code': [],        # 股票代码
                'stock_name': [],        # 股票名称
                'ccx_jjdi': [],         # 超长线接近底
                'ccx_di': [],           # 超长线底
                'ccxdtg': [],           # 超长线多头刚
                'ccxdtz': [],           # 超长线多头中
            }
            
            # 遍历股票列表计算信号
            for stock in stocklist:
                results['stock_code'].append(stock)
                results['stock_name'].append(stock_names.get(stock, ''))
                
                try:
                    # 准备技术数据
                    data_dict = self._prepare_technical_data_for_stock(stock, date)
                    
                    if data_dict is None:
                        # 数据不足，设置默认值
                        results['ccx_jjdi'].append(False)
                        results['ccx_di'].append(False)
                        results['ccxdtg'].append(False)
                        results['ccxdtz'].append(False)
                        continue
                    
                    # 创建技术分析器
                    analyzer = TechnicalAnalyzer(data_dict)
                    
                    # 计算超长线技术指标
                    results['ccx_jjdi'].append(analyzer.ccx_jjdi())
                    results['ccx_di'].append(analyzer.ccx_di())
                    results['ccxdtg'].append(analyzer.ccxdtg())
                    results['ccxdtz'].append(analyzer.ccxdtz())
                    
                except Exception as e:
                    print(f"计算股票 {stock} 超长线技术指标失败: {e}")
                    results['ccx_jjdi'].append(False)
                    results['ccx_di'].append(False)
                    results['ccxdtg'].append(False)
                    results['ccxdtz'].append(False)
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            # 计算评分
            df['ccx_jjdi_score'] = df['ccx_jjdi'] * 1
            df['ccxdi_score'] = df['ccx_di'] * 2
            df['ccxdtg_score'] = df['ccxdtg'] * 4
            df['ccxdtz_score'] = df['ccxdtz'] * 1
            # 取最大值作为超长线技术评分
            df['ccx_score'] = df[['ccx_jjdi_score', 'ccxdi_score', 'ccxdtg_score', 'ccxdtz_score']].max(axis=1)
            
            return df
            
        except Exception as e:
            print(f"计算超长线技术指标失败: {e}")
            return pd.DataFrame()
    
    def get_atr(self, stock_code, date, N=20):
        """
        获取股票的ATR值
        通过调用indicators.py中的ATR函数来计算20天ATR
        
        参数:
        stock_code: 股票代码
        date: 计算日期
        N: ATR周期，默认20天
        
        返回:
        float: ATR值
        """
        try:
            # 准备技术数据
            data_dict = self._prepare_technical_data_for_stock(stock_code, date)
            
            if data_dict is None or data_dict['daily'].empty:
                return None
            
            df = data_dict['daily']
            
            if len(df) < N:
                return None
            
            # 从indicators模块导入ATR函数
            from core.utils.indicators import ATR
            
            # 准备数据：需要收盘价、最高价、最低价
            close = df['close'].values
            high = df['high'].values
            low = df['low'].values
            
            # 计算ATR (返回ATR序列和TR序列)
            atr_series, tr_series = ATR(close, high, low, N)
            
            # 返回最新的ATR值
            return atr_series[-1] if len(atr_series) > 0 else None
            
        except Exception as e:
            print(f"计算股票 {stock_code} ATR失败: {e}")
            return None
    
    def get_atr_score(self, stocklist, date):
        """
        获取股票列表的ATR评分
        计算所有股票的ATR值，然后根据分位数进行评分
        
        参数:
        stocklist: 股票代码列表
        date: 计算日期
        
        返回:
        dict: {stock_code: atr_score} 的字典，ATR在49%分位数以下得1分，否则得0分
        """
        atr_dict = {}
        atr_values = []
        
        # 先计算所有股票的ATR值
        for stock_code in stocklist:
            atr = self.get_atr(stock_code, date)
            if atr is not None:
                atr_dict[stock_code] = atr
                atr_values.append(atr)
        
        if not atr_values:
            return {code: 0 for code in stocklist}
        
        # 计算38%分位数
        import numpy as np
        atr_threshold = np.quantile(atr_values, 0.38)
        
        # 根据分位数评分
        atr_scores = {}
        for stock_code in stocklist:
            if stock_code in atr_dict:
                atr_scores[stock_code] = 0.8 if atr_dict[stock_code] < atr_threshold else 0
            else:
                atr_scores[stock_code] = 0
        
        return atr_scores
    
    def get_jishu_atr(self, stocklist, date):
        """
        计算股票列表的ATR技术指标并评分
        
        Args:
            stocklist: 股票代码列表
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            DataFrame包含每只股票的代码、名称、ATR值和评分
        """
        try:
            # 获取股票名称
            stock_names = self._get_stock_names(stocklist)
            
            # 获取ATR评分
            atr_scores_dict = self.get_atr_score(stocklist, date)
            
            # 创建结果存储字典
            results = {
                'stock_code': [],        # 股票代码
                'stock_name': [],        # 股票名称
                'atr_value': [],         # ATR值
                'atr_score': [],         # ATR评分
            }
            
            # 遍历股票列表
            for stock in stocklist:
                results['stock_code'].append(stock)
                results['stock_name'].append(stock_names.get(stock, ''))
                
                # 获取ATR值
                atr_value = self.get_atr(stock, date)
                results['atr_value'].append(atr_value if atr_value is not None else 0.0)
                
                # 获取ATR评分
                results['atr_score'].append(atr_scores_dict.get(stock, 0))
            
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            return df
            
        except Exception as e:
            print(f"计算ATR技术指标失败: {e}")
            return pd.DataFrame()

    


    
    def get_jishu_scores(self, stocklist, date):
        """
        汇总前面4个技术指标进行综合评分
        
        Args:
            stocklist: 股票代码列表
            date: 日期字符串，格式'YYYY-MM-DD'
        
        Returns:
            DataFrame包含每只股票的代码、名称、各项技术指标评分和总评分
        """
        try:
            # 分别计算四种技术指标
            df_ma26ruo = self.get_ma26ruo(stocklist, date)
            df_zj = self.get_jishu_zj(stocklist, date)
            df_cx = self.get_jishu_cx(stocklist, date)
            df_ccx = self.get_jishu_ccx(stocklist, date)
            df_atr = self.get_jishu_atr(stocklist, date)
            
            if df_ma26ruo.empty and df_zj.empty and df_cx.empty and df_ccx.empty and df_atr.empty:
                return pd.DataFrame()
            
            # 合并所有结果
            result_df = pd.DataFrame({'stock_code': stocklist})
            
            # 合并MA26R技术指标评分
            if not df_ma26ruo.empty:
                ma26ruo_scores = df_ma26ruo[['stock_code', 'ma26ruo_score']].copy()
                result_df = result_df.merge(ma26ruo_scores, on='stock_code', how='left')
            else:
                result_df['ma26ruo_score'] = 0.0

            # 合并中级技术指标评分
            if not df_zj.empty:
                zj_scores = df_zj[['stock_code', 'zj_score']].copy()
                result_df = result_df.merge(zj_scores, on='stock_code', how='left')
            else:
                result_df['zj_score'] = 0.0
            
            # 合并长线技术指标评分
            if not df_cx.empty:
                cx_scores = df_cx[['stock_code', 'cx_score']].copy()
                result_df = result_df.merge(cx_scores, on='stock_code', how='left')
            else:
                result_df['cx_score'] = 0.0
            
            # 合并超长线技术指标评分
            if not df_ccx.empty:
                ccx_scores = df_ccx[['stock_code', 'ccx_score']].copy()
                result_df = result_df.merge(ccx_scores, on='stock_code', how='left')
            else:
                result_df['ccx_score'] = 0.0
            
            # 合并ATR技术指标评分
            if not df_atr.empty:
                atr_scores = df_atr[['stock_code', 'atr_score']].copy()
                result_df = result_df.merge(atr_scores, on='stock_code', how='left')
            else:
                result_df['atr_score'] = 0.0
            
            # 填充缺失值
            result_df['ma26ruo_score'] = result_df['ma26ruo_score'].fillna(0.0)
            result_df['zj_score'] = result_df['zj_score'].fillna(0.0)
            result_df['cx_score'] = result_df['cx_score'].fillna(0.0)
            result_df['ccx_score'] = result_df['ccx_score'].fillna(0.0)
            result_df['atr_score'] = result_df['atr_score'].fillna(0.0)
            
            # 计算总评分（ATR评分权重为0.5）
            result_df['total_score'] = (result_df['ma26ruo_score'] + result_df['zj_score'] + result_df['cx_score'] + result_df['ccx_score'] + result_df['atr_score'] * 0.5) * 1.3
            
            # 添加股票名称
            stock_names = self._get_stock_names(stocklist)
            result_df['stock_name'] = result_df['stock_code'].map(stock_names).fillna('')
            
            # 调整列顺序
            cols = ['stock_code', 'stock_name', 'ma26ruo_score', 'zj_score', 'cx_score', 'ccx_score', 'atr_score', 'total_score']
            result_df = result_df[cols]
            
            # 按总评分降序排列
            result_df = result_df.sort_values(by='total_score', ascending=False).reset_index(drop=True)  # type: ignore
            
            return result_df
            
        except Exception as e:
            print(f"计算综合技术指标评分失败: {e}")
            return pd.DataFrame()
    
    
    
    def __del__(self):
        """析构函数，清理数据库连接"""
        if self.db_conn:
            pass  # 数据库连接会自动管理


