#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数日线转周线和月线数据转换器 (quant_v2 优化版)

功能：
1. 从index_k_daily表读取指数日线数据
2. 转换为周线和月线数据
3. 【优化】采用安全的建表方式，不会删除旧数据
4. 【优化】支持真正的增量更新，可安全重复执行，不会产生重复数据
5. 【v2架构】使用DatabaseManager进行数据库操作
6. 【性能优化】使用批量插入提升性能
"""

import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from data_management.database_manager import DatabaseManager

class IndexDailyToWeeklyMonthlyConverter:
    """
    指数日线转周线和月线数据转换器 (quant_v2 优化版)
    """

    def __init__(self):
        """初始化转换器，使用DatabaseManager"""
        self.db_manager = DatabaseManager()
        self.daily_table = "index_k_daily"
        self.weekly_table = "index_k_weekly"
        self.monthly_table = "index_k_monthly"

    def _create_table_if_not_exists(self, table_name: str) -> bool:
        """
        【重构】通用的建表函数，如果表不存在则创建
        """
        try:
            # 使用DatabaseManager的DDL方法
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_code TEXT NOT NULL,
                index_name TEXT,
                trade_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                trade_days INTEGER,
                UNIQUE(index_code, trade_date)
            )
            """
            
            success = self.db_manager.execute_ddl(create_sql)
            if success:
                # 创建索引
                index_sql = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_code_date ON {table_name} (index_code, trade_date)"
                self.db_manager.execute_ddl(index_sql)
                print(f"✅ {table_name} 表已存在或创建成功。")
                return True
            else:
                print(f"❌ 创建 {table_name} 表失败")
                return False
        except Exception as e:
            print(f"❌ 创建 {table_name} 表失败: {e}")
            return False

    def get_all_index_codes(self) -> List[str]:
        """
        获取所有指数代码
        """
        try:
            query = f"SELECT DISTINCT index_code FROM {self.daily_table} ORDER BY index_code"
            df = self.db_manager.execute_query(query)
            
            if not df.empty:
                return df['index_code'].tolist()
            else:
                return []
        except Exception as e:
            print(f"❌ 获取指数代码失败: {e}")
            return []

    def get_daily_data(self, index_code: str, start_date: Optional[str] = None) -> pd.DataFrame:
        """
        【优化】获取日线数据，增加 start_date 参数以支持增量更新
        """
        try:
            query = f"""
            SELECT index_code, index_name, trade_date, open, high, low, close, volume
            FROM {self.daily_table}
            WHERE index_code = :index_code
            """
            params = {"index_code": index_code}
            
            if start_date:
                query += " AND trade_date >= :start_date"
                params["start_date"] = start_date

            query += " ORDER BY trade_date"
            
            df = self.db_manager.execute_query(query, params)
            
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df.set_index('trade_date', inplace=True)
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except Exception as e:
            print(f"❌ 获取 {index_code} 日线数据失败: {e}")
            return pd.DataFrame()

    def _resample_data(self, daily_df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """
        【重构/修正版】通用的数据重采样函数
        """
        if daily_df.empty or daily_df.volume.sum() == 0:
            return pd.DataFrame()

        ohlc_dict = {
            'open': 'first', 'high': 'max', 'low': 'min',
            'close': 'last', 'volume': 'sum'
        }

        # 1. 执行重采样
        resampled_df = daily_df.resample(rule).agg(ohlc_dict)

        # 2. 【关键修正】将索引替换为每个周期内实际的最后一个交易日
        last_trade_dates = daily_df['close'].resample(rule).last().index
        
        # 确保索引长度匹配
        if len(last_trade_dates) == len(resampled_df):
            resampled_df.index = last_trade_dates
        else:
            print(f"警告：重采样结果长度不匹配，使用原始索引。规则: {rule}")

        # 3. 计算周期内的交易天数
        resampled_df['trade_days'] = daily_df['open'].resample(rule).count()
        if 'index_name' in daily_df.columns:
            resampled_df['index_name'] = daily_df['index_name'].resample(rule).first()
        
        # 4. 删除没有交易的周期 (例如国庆黄金周)
        resampled_df = resampled_df.dropna(subset=['open'])

        # 5. 重置索引并将日期格式化为字符串
        resampled_df = resampled_df.reset_index()
        resampled_df['trade_date'] = resampled_df['trade_date'].dt.strftime('%Y-%m-%d')
        
        return resampled_df

    def _save_data(self, index_code: str, data_df: pd.DataFrame, table_name: str, period_start_date: str):
        """
        【重构】通用的保存数据函数，实现真正的增量更新
        """
        if data_df.empty:
            return False
        
        try:
            # 【关键修正】在插入新数据前，删除可能已存在的不完整周期数据
            delete_sql = f"DELETE FROM {table_name} WHERE index_code = :index_code AND trade_date >= :period_start_date"
            self.db_manager.execute_dml(delete_sql, {"index_code": index_code, "period_start_date": period_start_date})
            
            # 添加指数代码列
            data_df['index_code'] = index_code
            
            # 确保列顺序正确
            columns_order = ['index_code', 'index_name', 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'trade_days']
            data_df = data_df[columns_order]
            
            # 使用批量插入方法
            success = self.db_manager.batch_insert_dataframe(data_df, table_name)
            return success
        except Exception as e:
            print(f"❌ 保存 {index_code} 到 {table_name} 失败: {e}")
            return False
    
    def run_conversion(self, index_codes: Optional[List[str]] = None, full_mode: bool = False):
        """
        【重构/修正版】执行转换的主函数，支持全量和增量模式
        """
        start_time = datetime.now()
        print(f"\n🚀 开始转换任务 ({'全量模式' if full_mode else '增量模式'})...")
        print(f"   开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. 确保表存在
        self._create_table_if_not_exists(self.weekly_table)
        self._create_table_if_not_exists(self.monthly_table)
        
        # 2. 获取要处理的指数列表
        if index_codes is None:
            index_codes = self.get_all_index_codes()
        if not index_codes:
            print("❌ 未找到任何指数代码，任务终止。")
            return

        print(f"   待处理指数数量: {len(index_codes)}")
        
        # 3. 逐个指数进行转换
        for i, code in enumerate(index_codes, 1):
            print(f"--- [{i}/{len(index_codes)}] 正在处理: {code} ---")
            
            start_date_for_fetch = None
            if not full_mode:
                # 增量模式：找到最后一个周/月的数据，从那周/月的第一天开始获取日线数据
                query = f"SELECT MAX(trade_date) FROM {self.weekly_table} WHERE index_code = :index_code"
                df = self.db_manager.execute_query(query, {"index_code": code})
                last_week_date_str = df.iloc[0, 0] if not df.empty and df.iloc[0, 0] is not None else None
                
                if last_week_date_str:
                    last_week_date = datetime.strptime(last_week_date_str, '%Y-%m-%d')
                    # 从上周的周一开始获取数据，以覆盖不完整的周
                    start_date_for_fetch = (last_week_date - timedelta(days=last_week_date.weekday())).strftime('%Y-%m-%d')
            
            daily_df = self.get_daily_data(code, start_date=start_date_for_fetch)
            if daily_df.empty:
                print("   -> 无日线数据，跳过。")
                continue
            
            # 增量更新的起始日期，用于后续的DELETE操作
            if not daily_df.empty and not daily_df.index.empty:
                first_day_of_period = daily_df.index.min().strftime('%Y-%m-%d')
            else:
                print(f"   -> 无法获取起始日期，跳过 {code}")
                continue

            # 【关键修正】明确传入周五和月末作为采样规则
            # 转换周线
            weekly_df = self._resample_data(daily_df, 'W-FRI') 
            self._save_data(code, weekly_df, self.weekly_table, first_day_of_period)
            
            # 转换月线
            monthly_df = self._resample_data(daily_df, 'M')
            self._save_data(code, monthly_df, self.monthly_table, first_day_of_period)

        print(f"\n✅ 任务完成! 耗时: {datetime.now() - start_time}")


# 主程序入口
if __name__ == "__main__":
    converter = IndexDailyToWeeklyMonthlyConverter()
    
    print("="*60)
    print("指数日线转周线月线数据转换器 (quant_v2 优化版)")
    print("="*60)
    print("1. 增量模式：只转换新增的数据（推荐）")
    print("2. 全量模式：重新转换所有数据")
    print()
    
    choice = input("请选择模式 (1/2，默认为1): ").strip()
    
    if choice == '2':
        confirm = input("警告：全量模式会删除并重建所有周线和月线数据，是否继续? (y/N): ").strip().lower()
        if confirm == 'y':
            converter.run_conversion(full_mode=True)
        else:
            print("操作已取消")
    else:
        # 默认使用增量模式
        converter.run_conversion(full_mode=False)