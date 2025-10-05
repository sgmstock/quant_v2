"""
日线数据更新与周期转换脚本

功能：
1. 提供两种日线更新模式：
   - [首选] 从指定CSV文件进行增量更新。
   - [备用] 从Akshare获取实时数据进行更新。
2. 在日线数据更新成功后，自动对周线(k_weekly)和月线(k_monthly)数据进行增量更新。
3. 自动创建所有需要的数据库表。
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import akshare as ak
from pathlib import Path
import os

from .database_manager import DatabaseManager
from .timeframe_converter import TimeframeConverter
from core.utils.logger import get_logger
from core.utils.jqdata_converter import JQDataConverter

logger = get_logger("data_management.data_updater")


class DataUpdater:
    """
    一个完整的日线数据处理管道，支持从聚宽CSV或Akshare更新，并自动转换周线/月线。
    优先级：聚宽数据 > Akshare数据
    """
    def __init__(self, db_manager: DatabaseManager, jqdata_csv_path=None, jqdata_converted_path=None, akshare_cache_path=None):
        self.db_manager = db_manager
        self.jqdata_csv_path = jqdata_csv_path or "databases/daily_update_last.csv"
        self.jqdata_converted_path = jqdata_converted_path or "databases/daily_update_converted.csv"
        self.akshare_cache_path = akshare_cache_path or "databases/akshare_daily.csv"
        self.jqdata_converter = JQDataConverter()
        self.all_stock_codes = self._get_all_stock_codes()

    def _get_all_stock_codes(self):
        """从数据库获取所有股票代码"""
        try:
            # 使用 DatabaseManager 获取股票列表
            stock_codes = self.db_manager.get_stock_list("k_daily")
            logger.info(f"从数据库成功获取 {len(stock_codes)} 个股票代码。")
            return set(stock_codes)
        except Exception as e:
            logger.error(f"从数据库获取股票代码失败: {e}")
            return set()

    # --- 核心功能1: 从聚宽数据更新 ---
    def update_from_jqdata(self):
        """[首选] 从聚宽CSV文件增量更新日线数据"""
        logger.info(f"--- 模式1: 尝试从聚宽数据文件 '{self.jqdata_csv_path}' 更新 ---")
        if not os.path.exists(self.jqdata_csv_path):
            logger.warning(f"聚宽CSV文件不存在: {self.jqdata_csv_path}")
            return False, None

        try:
            # 步骤1: 转换聚宽数据格式
            logger.info("正在转换聚宽数据格式...")
            success, quality = self.jqdata_converter.convert_and_validate(
                self.jqdata_csv_path, 
                self.jqdata_converted_path
            )
            
            if not success:
                logger.error(f"聚宽数据转换失败: {quality}")
                return False, None
            
            logger.info(f"聚宽数据转换成功: {quality['total_records']} 条记录")
            
            # 步骤2: 读取转换后的数据
            df = pd.read_csv(self.jqdata_converted_path)
            
            # 数据清洗和格式化
            required_cols = ['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"转换后的CSV文件缺少必要列。需要: {required_cols}, 实际: {df.columns.tolist()}")
                return False, None

            df.rename(columns={'date': 'trade_date'}, inplace=True)
            df['stock_code'] = df['stock_code'].astype(str).str.zfill(6)
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            df.dropna(inplace=True)
            
            # 只保留数据库中存在的股票代码
            df = df[df['stock_code'].isin(self.all_stock_codes)]
            if df.empty:
                logger.warning("转换后的聚宽数据中没有需要更新的有效股票数据。")
                return True, None # 认为成功，但不触发周期转换

            # 使用 DatabaseManager 保存数据（使用 replace 策略处理冲突）
            success = self.db_manager.save_stock_data(df, "k_daily", conflict_resolution="replace")
            if success:
                min_date = df['trade_date'].min()
                logger.info(f"🎉 成功从聚宽数据更新了 {len(df)} 条日线数据。")
                return True, min_date
            else:
                logger.error("聚宽数据更新失败")
                return False, None

        except Exception as e:
            logger.error(f"从聚宽数据更新时出错: {e}")
            return False, None

    # --- 核心功能2: 从Akshare更新 ---
    def update_from_akshare(self):
        """[备用] 从Akshare获取数据并更新日线"""
        logger.info("--- 模式2: 尝试从 Akshare 更新 ---")
        try:
            logger.info("开始从akshare获取当日实时股票数据...")
            stock_df = ak.stock_zh_a_spot_em()
            
            # 验证Akshare返回的字段
            required_akshare_cols = ['代码', '今开', '最高', '最低', '最新价', '成交量']
            missing_cols = [col for col in required_akshare_cols if col not in stock_df.columns]
            if missing_cols:
                logger.error(f"Akshare返回数据缺少必要字段: {missing_cols}")
                logger.info(f"实际字段: {stock_df.columns.tolist()}")
                return False, None
            
            # 数据映射和清洗
            today = datetime.now().strftime('%Y-%m-%d')
            df = pd.DataFrame({
                'stock_code': stock_df['代码'],
                'trade_date': today,
                'open': stock_df['今开'], 'high': stock_df['最高'], 'low': stock_df['最低'],
                'close': stock_df['最新价'], 'volume': stock_df['成交量'] * 100
            })
            df.dropna(inplace=True)
            df = df[df['stock_code'].isin(self.all_stock_codes)]

            if df.empty:
                logger.warning("Akshare没有返回需要更新的有效股票数据。")
                return True, None

            # 使用 DatabaseManager 保存数据（使用 replace 策略处理冲突）
            success = self.db_manager.save_stock_data(df, "k_daily", conflict_resolution="replace")
            if success:
                logger.info(f"🎉 成功从Akshare更新了 {len(df)} 条日线数据。")
                return True, today
            else:
                logger.error("Akshare数据更新失败")
                return False, None

        except Exception as e:
            logger.error(f"从Akshare更新时出错: {e}")
            return False, None

    # --- 核心功能3: 周期数据转换 ---
    def _update_resampled_data(self, start_date):
        """在日线更新后，增量更新周线和月线数据"""
        if start_date is None:
            logger.info("没有新的日线数据，跳过周期转换。")
            return

        logger.info("--- 开始增量更新周线和月线数据 ---")
        
        # 只更新周线数据，月线数据已经完整
        self._resample_and_update('k_weekly', 'W-FRI', start_date)
        
        # 检查月线数据是否需要更新
        logger.info("检查月线数据状态...")
        try:
            # 获取最新的月线数据日期
            latest_monthly = self.db_manager.get_latest_data_date('k_monthly')
            if latest_monthly:
                logger.info(f"月线数据最新日期: {latest_monthly}")
                # 如果月线数据已经是最新的，跳过转换
                if latest_monthly >= start_date:
                    logger.info("月线数据已经是最新的，跳过月线转换。")
                    return
                else:
                    logger.info("月线数据需要更新，开始转换...")
                    self._resample_and_update('k_monthly', 'M', start_date)
            else:
                logger.info("没有找到月线数据，开始转换...")
                self._resample_and_update('k_monthly', 'M', start_date)
        except Exception as e:
            logger.warning(f"检查月线数据状态失败: {e}，跳过月线转换。")

    def _resample_and_update(self, table_name, period_code, start_date):
        """通用重采样和更新逻辑 - 最终修复版本"""
        try:
            from sqlalchemy import text
            
            # 确定重计算的真正起始点（周初或月初）
            start_dt = pd.to_datetime(start_date)
            if period_code == 'W-FRI':
                recalc_start = (start_dt - pd.to_timedelta(start_dt.weekday(), unit='d')).strftime('%Y-%m-%d')
            else: # 月线 ('M' or 'ME')
                recalc_start = start_dt.strftime('%Y-%m-01')

            logger.info(f"为 '{table_name}' 表重计算自 {recalc_start} 以来的数据...")

            # [核心修正] 恢复为原始的、更稳定的数据加载方式
            with self.db_manager.engine.connect() as conn:
                query = text("SELECT * FROM k_daily WHERE trade_date >= :recalc_start")
                result = conn.execute(query, {"recalc_start": recalc_start})
                daily_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            if daily_df.empty:
                logger.info(f"在 {recalc_start} 之后没有日线数据，跳过 {table_name} 更新。")
                return
                
            # 执行周期转换
            daily_df['trade_date'] = pd.to_datetime(daily_df['trade_date'])
            daily_df = daily_df.set_index('trade_date')
            
            # 修复 'M' 的 FutureWarning
            resample_code = 'ME' if period_code == 'M' else period_code
            
            agg_rules = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
            period_df = daily_df.groupby('stock_code').resample(resample_code).agg(agg_rules)
            period_df.dropna(inplace=True)
            period_df.reset_index(inplace=True)
            period_df['trade_date'] = period_df['trade_date'].dt.strftime('%Y-%m-%d')
            
            if period_df.empty:
                logger.warning(f"没有生成 {resample_code} 数据")
                return

            # 使用 engine.begin() 来管理事务，自动提交或回滚
            with self.db_manager.engine.begin() as conn:
                # 先删除旧数据
                delete_query = text(f"DELETE FROM {table_name} WHERE trade_date >= :recalc_start")
                result = conn.execute(delete_query, {"recalc_start": recalc_start})
                logger.info(f"从 {table_name} 删除了 {result.rowcount} 条旧数据。")

            # 使用您封装好的 DatabaseManager 保存新数据
            success = self.db_manager.save_stock_data(period_df, table_name, conflict_resolution="replace")
            if success:
                logger.info(f"✅ 成功更新了 {len(period_df)} 条数据到 {table_name} 表。")
            else:
                logger.error(f"保存数据到 {table_name} 表失败")
            
        except Exception as e:
            logger.error(f"更新 {table_name} 表时失败: {e}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")

    # --- 主流程 ---
    def run(self):
        """执行完整的数据更新流程"""
        logger.info("=============================================")
        logger.info("=== 开始执行日线数据更新流程 ===")
        logger.info("=== 优先级: 聚宽数据 > Akshare数据 ===")
        logger.info("=============================================")
        
        try:
            # 步骤1: 优先尝试从聚宽数据更新
            success, start_date = self.update_from_jqdata()

            # 步骤2: 如果聚宽数据更新失败或未执行，则回退到Akshare
            if not success:
                logger.warning("聚宽数据更新失败或未执行，回退到Akshare更新模式...")
                success, start_date = self.update_from_akshare()
            
            # 步骤3: 如果日线更新成功，则触发周期数据转换
            if success:
                logger.info("日线数据更新成功，开始更新周线和月线...")
                self._update_resampled_data(start_date)
                logger.info("🎉 所有更新流程执行完毕！")
            else:
                logger.error("❌ 所有日线更新方式均失败，流程终止。")
                
        except Exception as e:
            logger.error(f"❌ 更新流程执行过程中发生未预期的错误: {e}")
            logger.error("建议检查数据库连接和数据完整性")
    
    # --- 数据验证 ---
    def check_recent_data(self, stock_code='000029', days=5):
        """查询指定股票最近几日的行情数据以供验证"""
        logger.info(f"\n🔎 验证股票 {stock_code} 最近 {days} 日的数据...")
        
        # 为不同周期设置不同的查询策略
        table_configs = {
            'k_daily': {'limit': days, 'desc': f'最近{days}日'},
            'k_weekly': {'limit': days, 'desc': f'最近{days}周'},  # 周线获取最近N条记录
            'k_monthly': {'limit': days, 'desc': f'最近{days}个月'}  # 月线获取最近N条记录
        }
        
        for table, config in table_configs.items():
            try:
                if table == 'k_daily':
                    # 日线数据使用日期范围查询
                    end_date = datetime.now().strftime('%Y-%m-%d')
                    start_date = (datetime.now() - timedelta(days=config['limit'])).strftime('%Y-%m-%d')
                    df = self.db_manager.get_stock_data(stock_code, start_date, end_date, table)
                else:
                    # 周线和月线数据使用LIMIT查询，获取最新的N条记录
                    df = self.db_manager.get_latest_stock_data(stock_code, table, config['limit'])
                
                logger.info(f"--- {table} 表数据 ({config['desc']}) ---")
                if df.empty:
                    logger.info("未找到数据。")
                else:
                    print(df.to_string(index=False))
                    logger.info(f"成功获取 {len(df)} 条 {stock_code} 数据")
            except Exception as e:
                 logger.error(f"查询 {table} 表失败: {e}")
