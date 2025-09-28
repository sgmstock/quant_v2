#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时K线数据提供器（主要是5分钟数据）

功能：
1. 获取实时日/周/月线数据（使用adata分别获取，不合成）
2. 与历史K线合并成60个周期的DataFrame
3. 不保存实时数据到数据库
4. 专为技术指标计算优化
"""

import pandas as pd
import sqlite3
from datetime import datetime, timedelta, time
from typing import List, Dict, Optional
import logging
import schedule
import threading

# 数据源导入
try:
    import adata
    ADATA_AVAILABLE = True
except ImportError:
    ADATA_AVAILABLE = False
    print("⚠️ adata未安装，将使用备用数据源")

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    print("⚠️ akshare未安装，实时报价功能将受限")


class RealtimeKlineProvider:
    """
    实时K线数据提供器
    专注于提供技术指标计算所需的K线数据
    """
    
    def __init__(self, db_path: str = "databases/quant_system.db"):
        self.db_path = db_path
        self.default_periods = 64  # 默认64个周期
        
        # 数据表名称
        self.daily_table = "k_daily"
        self.weekly_table = "k_weekly"
        self.monthly_table = "k_monthly"
        self.min5_table = "k_5min"  # 5分钟数据表
        
        # 交易时间设置
        self.trading_times = ['11:20', '14:40']  # 获取日线周线月线实时数据的时间点
        
        # 5分钟数据设置
        self.max_5min_periods = 1000  # 最大保存1000个周期
        
        # 监控股票列表
        self.watch_stocks = []
        
        # 设置日志
        self.setup_logging()
    
    def setup_logging(self):
        """
        设置日志系统
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # 初始化5分钟数据表
        self.init_5min_table()
    
    def init_5min_table(self):
        """
        初始化5分钟数据表
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建5分钟数据表
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.min5_table} (
                    stock_code TEXT,
                    trade_date TEXT,
                    trade_time TEXT,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    volume REAL,
                    PRIMARY KEY (stock_code, trade_time)
                )
            """)
            
            # 创建索引
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.min5_table}_trade_time ON {self.min5_table} (trade_time)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{self.min5_table}_stock_code ON {self.min5_table} (stock_code)")
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"5分钟数据表 {self.min5_table} 初始化完成")
            
        except Exception as e:
            self.logger.error(f"初始化5分钟数据表失败: {e}")
    
    def is_trading_time(self) -> bool:
        """
        检查当前是否为交易时间（11:20或14:40）
        
        Returns:
            bool: 是否为交易时间
        """
        now = datetime.now()
        current_time = now.strftime('%H:%M')
        
        # 检查是否为交易日（简单判断：周一到周五）
        if now.weekday() >= 5:  # 周六、周日
            return False
        
        # 检查是否为指定的交易时间
        return current_time in self.trading_times
    
    def is_5min_trading_time(self) -> bool:
        """
        检查当前是否为5分钟数据获取时间（交易日每5分钟）
        
        Returns:
            bool: 是否为5分钟交易时间
        """
        now = datetime.now()
        
        # 检查是否为交易日（周一到周五）
        if now.weekday() >= 5:  # 周六、周日
            return False
        
        # 检查是否在交易时间段内
        current_time = now.time()
        morning_start = time(9, 30)  # 9:30
        morning_end = time(11, 30)   # 11:30
        afternoon_start = time(13, 0)  # 13:00
        afternoon_end = time(15, 0)    # 15:00
        
        is_trading_session = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        # 检查是否为5分钟的整数倍
        if is_trading_session:
            minute = now.minute
            return minute % 5 == 0  # 每5分钟获取一次
        
        return False
    
    def get_historical_kline(self, stock_code: str, k_type: str, periods: int = 63) -> pd.DataFrame:
        """
        从数据库获取历史K线数据
        
        Args:
            stock_code: 股票代码
            k_type: K线类型 (daily/weekly/monthly)
            periods: 获取周期数（默认59，为实时数据留出1个位置）
        
        Returns:
            pd.DataFrame: 历史K线数据
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 根据K线类型选择表名
            table_mapping = {
                'daily': self.daily_table,
                'weekly': self.weekly_table,
                'monthly': self.monthly_table
            }
            
            if k_type not in table_mapping:
                self.logger.error(f"不支持的K线类型: {k_type}")
                return pd.DataFrame()
            
            table_name = table_mapping[k_type]
            
            query = f"""
            SELECT stock_code, trade_date, open, close, high, low, volume
            FROM {table_name}
            WHERE stock_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, conn, params=[stock_code, periods])
            conn.close()
            
            if not df.empty:
                # 按日期正序排列
                df = df.sort_values('trade_date').reset_index(drop=True)
                self.logger.info(f"获取{stock_code} {k_type}历史数据: {len(df)}条")
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取{stock_code} {k_type}历史数据失败: {e}")
            return pd.DataFrame()
    
    def get_5min_kline_data(self, stock_code: str, count: int = 5) -> pd.DataFrame:
        """
        获取5分钟K线数据
        
        Args:
            stock_code: 股票代码
            count: 获取数量（通过日期范围控制）
        
        Returns:
            pd.DataFrame: 5分钟K线数据
        """
        if not ADATA_AVAILABLE:
            self.logger.warning("adata不可用，无法获取5分钟K线数据")
            return pd.DataFrame()
        
        try:
            # 计算开始日期（获取最近几天的数据以确保有足够的5分钟数据）
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            
            # 使用adata获取5分钟K线数据
            df = adata.stock.market.get_market(
                stock_code=stock_code,
                k_type='5',  # 5分钟
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                # 按时间排序并取最新的count条记录
                df = df.sort_values('trade_time').tail(count).reset_index(drop=True)
                
                # 标准化列名（adata已经返回正确的列名）
                # 确保包含必要的列
                required_columns = ['stock_code', 'trade_date', 'trade_time', 'open', 'close', 'high', 'low', 'volume']
                
                # 检查并添加缺失的列
                for col in required_columns:
                    if col not in df.columns:
                        if col == 'stock_code':
                            df['stock_code'] = stock_code
                        elif col == 'trade_date':
                            # 从trade_time提取日期
                            df['trade_date'] = pd.to_datetime(df['trade_time']).dt.strftime('%Y-%m-%d')
                
                # 重新排列列顺序
                df = df[required_columns]
                
                self.logger.info(f"获取{stock_code} 5分钟数据成功: {len(df)}条")
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取{stock_code} 5分钟数据失败: {e}")
            return pd.DataFrame()
    
    def save_5min_data(self, stock_code: str, data_df: pd.DataFrame) -> bool:
        """
        保存5分钟数据到数据库，并维护1000个周期的限制
        
        Args:
            stock_code: 股票代码
            data_df: 5分钟数据
        
        Returns:
            bool: 保存是否成功
        """
        if data_df.empty:
            return True
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 保存新数据
            data_df.to_sql(self.min5_table, conn, if_exists='append', index=False)
            
            # 检查并清理超过1000个周期的数据
            self.cleanup_5min_data(stock_code, conn)
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"保存{stock_code} 5分钟数据成功: {len(data_df)}条")
            return True
            
        except Exception as e:
            self.logger.error(f"保存{stock_code} 5分钟数据失败: {e}")
            return False
    
    def cleanup_5min_data(self, stock_code: str, conn=None):
        """
        清理5分钟数据，保持最多1000个周期
        
        Args:
            stock_code: 股票代码
            conn: 数据库连接（可选）
        """
        should_close = False
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            should_close = True
        
        try:
            cursor = conn.cursor()
            
            # 获取当前数据量
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {self.min5_table} 
                WHERE stock_code = ?
            """, (stock_code,))
            
            count = cursor.fetchone()[0]
            
            if count > self.max_5min_periods:
                # 删除最旧的数据
                delete_count = count - self.max_5min_periods
                cursor.execute(f"""
                    DELETE FROM {self.min5_table} 
                    WHERE stock_code = ? 
                    AND trade_time IN (
                        SELECT trade_time 
                        FROM {self.min5_table} 
                        WHERE stock_code = ? 
                        ORDER BY trade_time 
                        LIMIT ?
                    )
                """, (stock_code, stock_code, delete_count))
                
                self.logger.info(f"清理{stock_code}旧5分钟数据: {delete_count}条，保持{self.max_5min_periods}个周期")
            
            if should_close:
                conn.commit()
                conn.close()
            
        except Exception as e:
            self.logger.error(f"清理{stock_code} 5分钟数据失败: {e}")
            if should_close and conn:
                conn.close()
    
    def update_5min_data_for_stock(self, stock_code: str):
        """
        更新单只股票的5分钟数据
        
        Args:
            stock_code: 股票代码
        """
        try:
            # 获取最新的5分钟数据（获取最近几条以确保有新数据）
            data_df = self.get_5min_kline_data(stock_code, 5)
            
            if not data_df.empty:
                # 保存到数据库
                self.save_5min_data(stock_code, data_df)
                self.logger.info(f"✅ {stock_code} 5分钟数据更新完成")
            else:
                self.logger.warning(f"⚠️ {stock_code} 5分钟数据获取为空")
        
        except Exception as e:
            self.logger.error(f"更新{stock_code} 5分钟数据异常: {e}")
            
            table_name = table_mapping[k_type]
            
            query = f"""
            SELECT stock_code, trade_date, open, close, high, low, volume
            FROM {table_name}
            WHERE stock_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, conn, params=[stock_code, periods])
            conn.close()
            
            if not df.empty:
                # 按日期正序排列
                df = df.sort_values('trade_date').reset_index(drop=True)
                self.logger.info(f"获取{stock_code} {k_type}历史数据: {len(df)}条")
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取{stock_code} {k_type}历史数据失败: {e}")
            return pd.DataFrame()
    
    def get_realtime_kline_data(self, stock_code: str, k_type: str, count: int = 3) -> pd.DataFrame:
        """
        使用adata分别获取实时K线数据（不通过合成）
        
        Args:
            stock_code: 股票代码
            k_type: K线类型 (daily/weekly/monthly)
            count: 获取数量
        
        Returns:
            pd.DataFrame: 实时K线数据
        """
        if not ADATA_AVAILABLE:
            self.logger.warning("adata不可用，无法获取实时K线数据")
            return pd.DataFrame()
        
        # K线类型映射到adata参数
        k_type_mapping = {
            'daily': '1',    # 日线
            'weekly': '2',   # 周线
            'monthly': '3'   # 月线
        }
        
        if k_type not in k_type_mapping:
            self.logger.error(f"不支持的K线类型: {k_type}")
            return pd.DataFrame()
        
        try:
            # 计算日期范围以获取足够的数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            if k_type == 'daily':
                start_date = (datetime.now() - timedelta(days=count + 5)).strftime('%Y-%m-%d')
            elif k_type == 'weekly':
                start_date = (datetime.now() - timedelta(weeks=count + 2)).strftime('%Y-%m-%d')
            else:  # monthly
                start_date = (datetime.now() - timedelta(days=(count + 2) * 30)).strftime('%Y-%m-%d')
            
            # 使用adata分别获取对应周期的K线数据
            df = adata.stock.market.get_market(
                stock_code=stock_code,
                k_type=k_type_mapping[k_type],
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                # 按日期排序并取最新的count条记录
                df = df.sort_values('trade_date').tail(count).reset_index(drop=True)
                
                # 确保包含必要的列
                required_columns = ['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
                
                # 检查并添加缺失的列
                for col in required_columns:
                    if col not in df.columns:
                        if col == 'stock_code':
                            df['stock_code'] = stock_code
                
                # 重新排列列顺序
                df = df[required_columns]
                
                self.logger.info(f"获取{stock_code} {k_type}实时数据成功: {len(df)}条")
            
            return df
            
        except Exception as e:
            self.logger.error(f"获取{stock_code} {k_type}实时数据失败: {e}")
            return pd.DataFrame()
    
    def merge_historical_and_realtime(self, historical_df: pd.DataFrame, realtime_df: pd.DataFrame, 
                                    periods: int = 62) -> pd.DataFrame:
        """
        合并历史数据和实时数据
        
        Args:
            historical_df: 历史K线数据
            realtime_df: 实时K线数据
            periods: 总周期数（默认60）
        
        Returns:
            pd.DataFrame: 合并后的K线数据
        """
        try:
            if historical_df.empty and realtime_df.empty:
                return pd.DataFrame()
            
            # 如果只有历史数据
            if realtime_df.empty:
                return historical_df.tail(periods).reset_index(drop=True)
            
            # 如果只有实时数据
            if historical_df.empty:
                return realtime_df.tail(periods).reset_index(drop=True)
            
            # 合并数据
            combined_df = pd.concat([historical_df, realtime_df], ignore_index=True)
            
            # 按日期排序并去重
            combined_df = combined_df.sort_values('trade_date')
            combined_df = combined_df.drop_duplicates(subset=['stock_code', 'trade_date'], keep='last')
            
            # 取最新的指定周期数
            result_df = combined_df.tail(periods).reset_index(drop=True)
            
            self.logger.info(f"合并K线数据完成: 历史{len(historical_df)}条 + 实时{len(realtime_df)}条 = 最终{len(result_df)}条")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"合并K线数据失败: {e}")
            return pd.DataFrame()
    
    def get_kline_for_analysis(self, stock_code: str, frequency: str = 'daily', periods: int = 64) -> pd.DataFrame:
        """
        获取用于技术指标分析的K线数据
        直接从adata获取最新的指定周期数据
        
        Args:
            stock_code: 股票代码
            frequency: 频率周期 ('daily'/'weekly'/'monthly')
            periods: 周期数（默认64）
        
        Returns:
            pd.DataFrame: 用于分析的K线数据
        """
        try:
            self.logger.info(f"从adata获取{stock_code} {frequency} K线数据，周期数: {periods}")
            
            if not ADATA_AVAILABLE:
                self.logger.error("adata不可用，无法获取数据")
                return pd.DataFrame()
            
            # 映射频率参数到adata的k_type参数
            k_type_map = {
                'daily': 1,      # 1.日; 
                'weekly': 2,     # 2.周; 
                'monthly': 3     # 3.月;
            }
            
            if frequency not in k_type_map:
                self.logger.error(f"不支持的频率: {frequency}，支持的频率: {list(k_type_map.keys())}")
                return pd.DataFrame()
            
            k_type = k_type_map[frequency]
            
            # 直接从adata获取指定周期的数据
            try:
                # 使用adata获取K线数据，根据API文档使用正确的参数
                df = adata.stock.market.get_market(
                    stock_code=stock_code,  # 股票代码
                    k_type=k_type,         # K线类型: 1.日; 2.周; 3.月; 4.季度; 5.5分钟; 15.15分钟; 30.30分钟; 60.60分钟
                    adjust_type=1           # K线复权类型: 0.不复权; 1.前复权; 2.后复权
                )
                
                if df is None or df.empty:
                    self.logger.warning(f"adata未返回{stock_code}的{frequency}数据")
                    return pd.DataFrame()
                
                # 获取最新的periods个周期
                if len(df) > periods:
                    df = df.tail(periods)
                
                # 只保留标准的OHLCV字段
                standard_columns =['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
                
                # 检查必要字段是否存在
                missing_columns = [col for col in standard_columns if col not in df.columns]
                if missing_columns:
                    self.logger.error(f"缺少必要的列: {missing_columns}")
                    self.logger.info(f"实际返回的列: {list(df.columns)}")
                    return pd.DataFrame()
                
                # 只选择标准字段
                df = df[standard_columns]
                
                self.logger.info(f"✅ 成功获取{stock_code} {frequency} K线数据: {len(df)}个周期")
                
                return df
                
            except Exception as e:
                self.logger.error(f"adata获取{stock_code} {frequency}数据失败: {e}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"获取{stock_code} {frequency} K线数据失败: {e}")
            return pd.DataFrame()
    
    def set_watch_stocks(self, stock_codes: List[str]):
        """
        设置监控股票列表
        
        Args:
            stock_codes: 股票代码列表
        """
        self.watch_stocks = stock_codes
        self.logger.info(f"设置监控股票: {', '.join(stock_codes)}")
    
    def get_multiple_stocks_kline(self, stock_codes: List[str], k_type: str = 'daily', 
                                periods: int = 62, force_realtime: bool = False) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票的K线数据
        
        Args:
            stock_codes: 股票代码列表
            k_type: K线类型
            periods: 周期数（默认60）
            force_realtime: 是否强制获取实时数据
        
        Returns:
            Dict[str, pd.DataFrame]: 股票代码到K线数据的映射
        """
        results = {}
        
        for stock_code in stock_codes:
            try:
                kline_df = self.get_kline_for_analysis(stock_code, k_type, periods, force_realtime)
                if not kline_df.empty:
                    results[stock_code] = kline_df
                else:
                    self.logger.warning(f"⚠️ {stock_code}数据获取失败")
            
            except Exception as e:
                self.logger.error(f"获取{stock_code}数据异常: {e}")
        
        self.logger.info(f"批量获取完成: {len(results)}/{len(stock_codes)}只股票")
        return results
    
    def scheduled_data_update(self):
        """
        定时数据更新任务（在11:20和14:40执行）
        """
        if not self.watch_stocks:
            self.logger.warning("未设置监控股票列表")
            return
        
        self.logger.info(f"开始定时数据更新: {datetime.now().strftime('%H:%M:%S')}")
        
        # 获取所有监控股票的各周期数据
        for k_type in ['daily', 'weekly', 'monthly']:
            try:
                results = self.get_multiple_stocks_kline(
                    self.watch_stocks, 
                    k_type, 
                    self.default_periods, 
                    force_realtime=True
                )
                
                self.logger.info(f"{k_type}数据更新完成: {len(results)}只股票")
                
            except Exception as e:
                self.logger.error(f"{k_type}数据更新失败: {e}")
        
        self.logger.info("定时数据更新完成")
    
    def scheduled_5min_update(self):
        """
        定时5分钟数据更新任务（交易日每5分钟执行）
        """
        if not self.watch_stocks:
            self.logger.warning("未设置监控股票列表")
            return
        
        self.logger.info(f"开始5分钟数据更新: {datetime.now().strftime('%H:%M:%S')}")
        
        # 更新所有监控股票的5分钟数据
        success_count = 0
        for stock_code in self.watch_stocks:
            try:
                self.update_5min_data_for_stock(stock_code)
                success_count += 1
            except Exception as e:
                self.logger.error(f"更新{stock_code} 5分钟数据失败: {e}")
        
        self.logger.info(f"5分钟数据更新完成: {success_count}/{len(self.watch_stocks)}只股票")
    
    def start_scheduled_monitoring(self):
        """
        启动定时监控
        """
        if not self.watch_stocks:
            self.logger.error("请先设置监控股票列表")
            return
        
        # 设置日/周/月线定时任务
        for trading_time in self.trading_times:
            schedule.every().monday.at(trading_time).do(self.scheduled_data_update)
            schedule.every().tuesday.at(trading_time).do(self.scheduled_data_update)
            schedule.every().wednesday.at(trading_time).do(self.scheduled_data_update)
            schedule.every().thursday.at(trading_time).do(self.scheduled_data_update)
            schedule.every().friday.at(trading_time).do(self.scheduled_data_update)
        
        # 设置5分钟数据定时任务（每5分钟检查一次）
        schedule.every(5).minutes.do(self.check_and_update_5min_data)
        
        self.logger.info(f"定时监控已启动")
        self.logger.info(f"日/周/月线更新时间: {', '.join(self.trading_times)}")
        self.logger.info(f"5分钟数据更新: 交易时间内每5分钟")
        
        # 在后台线程中运行调度器
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        
        self.logger.info("定时监控线程已启动")
    
    def check_and_update_5min_data(self):
        """
        检查并更新5分钟数据（只在交易时间内执行）
        """
        if self.is_5min_trading_time():
            self.scheduled_5min_update()
        else:
            # 非交易时间，跳过
            pass
    
    def get_realtime_quotes(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        获取实时报价数据
        
        Args:
            stock_codes: 股票代码列表
        
        Returns:
            Dict: 实时报价数据
        """
        quotes = {}
        
        for stock_code in stock_codes:
            try:
                # 优先使用akshare
                if AKSHARE_AVAILABLE:
                    quote_data = self.get_quote_from_akshare(stock_code)
                    if quote_data:
                        quotes[stock_code] = quote_data
                        continue
                
                # 备用adata
                if ADATA_AVAILABLE:
                    quote_data = self.get_quote_from_adata(stock_code)
                    if quote_data:
                        quotes[stock_code] = quote_data
                
            except Exception as e:
                self.logger.error(f"获取{stock_code}实时报价失败: {e}")
        
        return quotes
    
    def get_quote_from_akshare(self, stock_code: str) -> Optional[Dict]:
        """
        从akshare获取实时报价
        
        Args:
            stock_code: 股票代码
        
        Returns:
            Dict: 报价数据
        """
        try:
            df = ak.stock_bid_ask_em(symbol=stock_code)
            
            if not df.empty:
                latest_value = df[df['item'] == '最新']['value'].iloc[0] if not df[df['item'] == '最新'].empty else None
                
                return {
                    'stock_code': stock_code,
                    'current_price': latest_value,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        
        except Exception as e:
            self.logger.error(f"akshare获取{stock_code}报价失败: {e}")
        
        return None
    
    def get_quote_from_adata(self, stock_code: str) -> Optional[Dict]:
        """
        从adata获取实时报价（只获取s1价格）
        
        Args:
            stock_code: 股票代码
        
        Returns:
            Dict: 报价数据
        """
        try:
            df = adata.stock.market.get_market_five(stock_code=stock_code)
            
            if not df.empty and 's1' in df.columns:
                current_price = df['s1'].iloc[0]
                return {
                    'stock_code': stock_code,
                    'current_price': current_price,
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
        
        except Exception as e:
            self.logger.error(f"adata获取{stock_code}报价失败: {e}")
        
        return None
    
    def convert_5min_to_other_timeframes(self, stock_code: str, timeframes: List[str] = ['15min', '30min', '60min']) -> Dict[str, pd.DataFrame]:
        """
        将5分钟K线数据转换为其他时间周期（内存中转换，不存储到数据库）
        
        Args:
            stock_code: 股票代码
            timeframes: 要转换的时间周期列表
        
        Returns:
            Dict[str, pd.DataFrame]: 各时间周期的K线数据
        """
        try:
            # 从数据库获取5分钟数据
            conn = sqlite3.connect(self.db_path)
            query = f"""
            SELECT * FROM {self.min5_table}
            WHERE stock_code = ?
            ORDER BY trade_time
            """
            
            df_5min = pd.read_sql_query(query, conn, params=[stock_code])
            conn.close()
            
            if df_5min.empty:
                self.logger.warning(f"{stock_code} 没有5分钟数据")
                return {}
            
            # 转换时间格式
            df_5min['datetime'] = pd.to_datetime(df_5min['trade_time'])
            df_5min = df_5min.set_index('datetime')
            
            results = {}
            
            for timeframe in timeframes:
                try:
                    if timeframe == '15min':
                        # 转换为15分钟
                        df_converted = df_5min.resample('15T').agg({
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'volume': 'sum'
                        }).dropna()
                    elif timeframe == '30min':
                        # 转换为30分钟
                        df_converted = df_5min.resample('30T').agg({
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'volume': 'sum'
                        }).dropna()
                    elif timeframe == '60min':
                        # 转换为60分钟
                        df_converted = df_5min.resample('60T').agg({
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'volume': 'sum'
                        }).dropna()
                    else:
                        self.logger.warning(f"不支持的时间周期: {timeframe}")
                        continue
                    
                    # 重置索引并格式化
                    df_converted = df_converted.reset_index()
                    df_converted['stock_code'] = stock_code
                    df_converted['trade_date'] = df_converted['datetime'].dt.strftime('%Y-%m-%d')
                    df_converted['trade_time'] = df_converted['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 重新排列列
                    df_converted = df_converted[['stock_code', 'trade_date', 'trade_time', 'open', 'close', 'high', 'low', 'volume']]
                    
                    results[timeframe] = df_converted
                    self.logger.info(f"5分钟转{timeframe}完成: {len(df_converted)}条记录")
                    
                except Exception as e:
                    self.logger.error(f"转换{timeframe}失败: {e}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"转换时间周期失败: {e}")
            return {}
    
    def get_5min_data_from_db(self, stock_code: str, limit: int = None) -> pd.DataFrame:
        """
        从数据库获取5分钟K线数据
        
        Args:
            stock_code: 股票代码
            limit: 限制条数
        
        Returns:
            pd.DataFrame: 5分钟K线数据
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            if limit:
                query = f"""
                SELECT * FROM {self.min5_table}
                WHERE stock_code = ?
                ORDER BY trade_time DESC
                LIMIT ?
                """
                df = pd.read_sql_query(query, conn, params=[stock_code, limit])
                df = df.sort_values('trade_time').reset_index(drop=True)
            else:
                query = f"""
                SELECT * FROM {self.min5_table}
                WHERE stock_code = ?
                ORDER BY trade_time
                """
                df = pd.read_sql_query(query, conn, params=[stock_code])
            
            conn.close()
            
            if not df.empty:
                self.logger.info(f"从数据库获取{stock_code} 5分钟数据: {len(df)}条")
            
            return df
            
        except Exception as e:
            self.logger.error(f"从数据库获取{stock_code} 5分钟数据失败: {e}")
            return pd.DataFrame()
    
    def get_analysis_summary(self, stock_code: str) -> Dict:
        """
        获取股票分析数据汇总
        
        Args:
            stock_code: 股票代码
        
        Returns:
            Dict: 分析数据汇总
        """
        try:
            summary = {
                'stock_code': stock_code,
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'kline_data': {},
                'realtime_quote': None
            }
            
            # 获取不同周期的K线数据
            for k_type in ['daily', 'weekly', 'monthly']:
                kline_df = self.get_kline_for_analysis(stock_code, k_type, self.default_periods)
                if not kline_df.empty:
                    summary['kline_data'][k_type] = {
                        'periods': len(kline_df),
                        'latest_date': kline_df['trade_date'].iloc[-1],
                        'latest_close': kline_df['close'].iloc[-1],
                        'data': kline_df
                    }
            
            # 获取实时报价
            quotes = self.get_realtime_quotes([stock_code])
            if stock_code in quotes:
                summary['realtime_quote'] = quotes[stock_code]
            
            return summary
            
        except Exception as e:
            self.logger.error(f"获取{stock_code}分析汇总失败: {e}")
            return {}


def main():
    """
    主函数 - 演示用法
    """
    provider = RealtimeKlineProvider()
    
    print("🚀 实时K线数据提供器")
    print(f"⏰ 交易时间: {', '.join(provider.trading_times)}")
    print(f"📊 默认周期数: {provider.default_periods}")
    print(f"📈 5分钟数据: 最多保存{provider.max_5min_periods}个周期")
    print("="*50)
    print("1. 获取单只股票K线数据（60周期）")
    print("2. 批量获取多只股票K线数据")
    print("3. 设置监控股票列表")
    print("4. 启动定时监控（含5分钟数据）")
    print("5. 获取实时报价")
    print("6. 强制获取实时数据（忽略交易时间）")
    print("7. 获取5分钟K线数据")
    print("8. 手动更新5分钟数据")
    print("9. 退出")
    
    while True:
        try:
            choice = input("\n请选择操作 (1-9): ").strip()
            
            if choice == '1':
                stock_code = input("请输入股票代码: ").strip()
                k_type = input("请输入K线类型 (daily/weekly/monthly): ").strip() or 'daily'
                
                if stock_code:
                    kline_df = provider.get_kline_for_analysis(stock_code, k_type)
                    if not kline_df.empty:
                        print(f"\n📊 {stock_code} {k_type} K线数据:")
                        print(f"周期数: {len(kline_df)}")
                        print(f"日期范围: {kline_df['trade_date'].iloc[0]} 至 {kline_df['trade_date'].iloc[-1]}")
                        print(f"最新收盘价: {kline_df['close'].iloc[-1]:.2f}")
                        print(f"交易时间: {'是' if provider.is_trading_time() else '否'}")
                        print("\n最近5个周期:")
                        print(kline_df[['trade_date', 'open', 'close', 'high', 'low', 'volume']].tail().to_string(index=False))
                    else:
                        print(f"❌ 获取{stock_code}数据失败")
            
            elif choice == '2':
                stocks_input = input("请输入股票代码（用逗号分隔）: ").strip()
                k_type = input("请输入K线类型 (daily/weekly/monthly): ").strip() or 'daily'
                
                if stocks_input:
                    stock_codes = [code.strip() for code in stocks_input.split(',')]
                    results = provider.get_multiple_stocks_kline(stock_codes, k_type)
                    
                    print(f"\n📊 批量获取结果:")
                    for stock_code, kline_df in results.items():
                        print(f"{stock_code}: {len(kline_df)}个周期, 最新价格: {kline_df['close'].iloc[-1]:.2f}")
            
            elif choice == '3':
                stocks_input = input("请输入监控股票代码（用逗号分隔）: ").strip()
                
                if stocks_input:
                    stock_codes = [code.strip() for code in stocks_input.split(',')]
                    provider.set_watch_stocks(stock_codes)
                    print(f"✅ 已设置监控股票: {', '.join(stock_codes)}")
            
            elif choice == '4':
                if provider.watch_stocks:
                    provider.start_scheduled_monitoring()
                    print(f"✅ 定时监控已启动，监控股票: {', '.join(provider.watch_stocks)}")
                    print(f"⏰ 将在每个交易日的 {', '.join(provider.trading_times)} 自动更新数据")
                else:
                    print("❌ 请先设置监控股票列表")
            
            elif choice == '5':
                stocks_input = input("请输入股票代码（用逗号分隔）: ").strip()
                
                if stocks_input:
                    stock_codes = [code.strip() for code in stocks_input.split(',')]
                    quotes = provider.get_realtime_quotes(stock_codes)
                    
                    print(f"\n📈 实时报价:")
                    for stock_code, quote in quotes.items():
                        print(f"{stock_code}: 价格 {quote.get('current_price', 'N/A')}, "
                              f"涨跌 {quote.get('change_amount', 'N/A')}, "
                              f"涨跌幅 {quote.get('change_percent', 'N/A')}%")
            
            elif choice == '6':
                stock_code = input("请输入股票代码: ").strip()
                k_type = input("请输入K线类型 (daily/weekly/monthly): ").strip() or 'daily'
                
                if stock_code:
                    print("🔄 强制获取实时数据（忽略交易时间限制）...")
                    kline_df = provider.get_kline_for_analysis(stock_code, k_type, force_realtime=True)
                    if not kline_df.empty:
                        print(f"\n📊 {stock_code} {k_type} K线数据（含实时）:")
                        print(f"周期数: {len(kline_df)}")
                        print(f"日期范围: {kline_df['trade_date'].iloc[0]} 至 {kline_df['trade_date'].iloc[-1]}")
                        print(f"最新收盘价: {kline_df['close'].iloc[-1]:.2f}")
                        print("\n最近5个周期:")
                        print(kline_df[['trade_date', 'open', 'close', 'high', 'low', 'volume']].tail().to_string(index=False))
                    else:
                        print(f"❌ 获取{stock_code}数据失败")
            
            elif choice == '7':
                stock_code = input("请输入股票代码: ").strip()
                periods = int(input("请输入周期数 (默认100): ").strip() or 100)
                
                if stock_code:
                    try:
                        conn = sqlite3.connect(provider.db_path)
                        query = f"""
                        SELECT trade_date, trade_time, open, close, high, low, volume
                        FROM {provider.min5_table}
                        WHERE stock_code = ?
                        ORDER BY trade_time DESC
                        LIMIT ?
                        """
                        
                        df = pd.read_sql_query(query, conn, params=[stock_code, periods])
                        conn.close()
                        
                        if not df.empty:
                            df = df.sort_values('trade_time').reset_index(drop=True)
                            print(f"\n📊 {stock_code} 5分钟K线数据:")
                            print(f"周期数: {len(df)}")
                            print(f"时间范围: {df['trade_time'].iloc[0]} 至 {df['trade_time'].iloc[-1]}")
                            print(f"最新收盘价: {df['close'].iloc[-1]:.2f}")
                            print("\n最近5个周期:")
                            print(df[['trade_time', 'open', 'close', 'high', 'low', 'volume']].tail().to_string(index=False))
                        else:
                            print(f"❌ {stock_code}暂无5分钟数据")
                    
                    except Exception as e:
                        print(f"❌ 获取5分钟数据失败: {e}")
            
            elif choice == '8':
                stock_code = input("请输入股票代码: ").strip()
                
                if stock_code:
                    print(f"🔄 手动更新{stock_code}的5分钟数据...")
                    provider.update_5min_data_for_stock(stock_code)
                    print("✅ 更新完成")
            
            elif choice == '9':
                print("\n👋 再见！")
                break
            
            else:
                print("\n❌ 无效选择，请重新输入")
        
        except KeyboardInterrupt:
            print("\n\n👋 用户中断，再见！")
            break
        except Exception as e:
            print(f"\n❌ 操作失败: {e}")


if __name__ == "__main__":
    # main()
    # # 测试RealtimeKlineProvider类的主要方法
    provider = RealtimeKlineProvider()
    dd = provider.get_5min_kline_data('000029', 5)
    print(dd)

    # # 测试获取历史K线数据
    # hist_daily = provider.get_historical_kline('000029', 'daily', 60)
    # print("历史日线数据:\n", hist_daily)

    # #测试获取实时K线数据，可以用来添加日线历史数据。因为只获取历史数据没有的周期数据
    # realtime_daily = provider.get_realtime_kline_data('000029', 'daily', 3)
    # print("实时日线数据:\n", realtime_daily)

    # # 测试获取5分钟K线数据
    # min5_data = provider.get_5min_kline_data('000029', 5)
    # print("5分钟K线数据:\n", min5_data)

    # # 测试合并历史和实时数据
    # merged_data = provider.merge_historical_and_realtime(hist_daily, realtime_daily)
    # print("合并后的数据:\n", merged_data)

    # # 测试获取用于分析的完整K线数据
    # analysis_data = provider.get_kline_for_analysis('000029', 'daily', 64)
    # print("用于分析的K线数据:\n", analysis_data)
    # dd = provider.get_realtime_quotes()
    # print(dd)

    # 测试获取实时报价
    # get1_0029 = provider.get_quote_from_adata('000029')
    # print(get1_0029)
    # get_0029 = provider.get_quote_from_akshare('000029')
    # print(get_0029)

    # # 测试设置监控股票，以后可以用在可视化上，现在不需要用。
    # provider.set_watch_stocks(['000029', '600000'])


    #     def get_quote_from_akshare(self, stock_code: str) -> Optional[Dict]:
    #     """
    #     从akshare获取实时报价
        
    #     Args:
    #         stock_code: 股票代码
        
    #     Returns:
    #         Dict: 报价数据
    #     """
    #     try:
    #         # 尝试不同的akshare接口
    #         df = None
            
    #         # 方法1：尝试实时行情接口
    #         try:
    #             df = ak.stock_zh_a_spot_em()
    #             if not df.empty:
    #                 stock_data = df[df['代码'] == stock_code]
    #                 if not stock_data.empty:
    #                     row = stock_data.iloc[0]
    #                     return {
    #                         'stock_code': stock_code,
    #                         'current_price': row.get('最新价', None),
    #                         'change_amount': row.get('涨跌额', None),
    #                         'change_percent': row.get('涨跌幅', None),
    #                         'volume': row.get('成交量', None),
    #                         'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #                     }
    #         except:
    #             pass
            
    #         # 方法2：尝试个股实时行情
    #         try:
    #             df = ak.stock_individual_info_em(symbol=stock_code)
    #             if not df.empty:
    #                 return {
    #                     'stock_code': stock_code,
    #                     'current_price': df.get('最新价', None),
    #                     'change_amount': df.get('涨跌额', None),
    #                     'change_percent': df.get('涨跌幅', None),
    #                     'volume': df.get('成交量', None),
    #                     'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #                 }
    #         except:
    #             pass
        
    #     except Exception as e:
    #         self.logger.error(f"akshare获取{stock_code}报价失败: {e}")
        
    #     return None
    
    # def get_quote_from_adata(self, stock_code: str) -> Optional[Dict]:
    #     """
    #     从adata获取实时报价
        
    #     Args:
    #         stock_code: 股票代码
        
    #     Returns:
    #         Dict: 报价数据
    #     """
    #     try:
    #         df = adata.stock.market.list_market_current()
            
    #         if not df.empty:
    #             stock_data = df[df['stock_code'] == stock_code]
                
    #             if not stock_data.empty:
    #                 row = stock_data.iloc[0]
    #                 return {
    #                     'stock_code': stock_code,
    #                     'current_price': row.get('price', None),
    #                     'change_amount': row.get('change', None),
    #                     'change_percent': row.get('change_pct', None),
    #                     'volume': row.get('volume', None),
    #                     'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #                 }
        
    #     except Exception as e:
    #         self.logger.error(f"adata获取{stock_code}报价失败: {e}")
        
    #     return None
    
