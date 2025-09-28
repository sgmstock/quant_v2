"""
时间周期转换器

负责日线数据转换为周线和月线数据
基于 quant_v2/data/daily_to_weekly_converter.py 和 daily_to_monthly_converter.py
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from .database_manager import DatabaseManager
from core.utils.logger import get_logger

logger = get_logger("data_management.timeframe_converter")


class TimeframeConverter:
    """时间周期转换器"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        初始化时间周期转换器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
    
    def daily_to_weekly(self, stock_codes: Optional[List[str]] = None) -> bool:
        """
        日线转周线数据
        
        Args:
            stock_codes: 股票代码列表，如果为None则转换所有股票
            
        Returns:
            bool: 转换是否成功
        """
        try:
            logger.info("开始日线转周线数据转换...")
            
            # 获取需要转换的股票列表
            if stock_codes is None:
                stock_codes = self.db_manager.get_stock_list("k_daily")
            
            if not stock_codes:
                logger.warning("没有找到需要转换的股票")
                return False
            
            all_weekly_data = []
            
            for stock_code in stock_codes:
                logger.info(f"正在转换 {stock_code} 的周线数据...")
                
                try:
                    # 获取日线数据
                    daily_data = self.db_manager.get_stock_data(
                        stock_code, "2020-01-01", "2025-12-31", "k_daily"
                    )
                    
                    if daily_data.empty:
                        logger.warning(f"{stock_code} 没有日线数据")
                        continue
                    
                    # 转换为周线数据
                    weekly_data = self._convert_to_weekly(daily_data)
                    
                    if not weekly_data.empty:
                        all_weekly_data.append(weekly_data)
                        logger.info(f"成功转换 {stock_code} 的 {len(weekly_data)} 条周线数据")
                    else:
                        logger.warning(f"{stock_code} 周线数据为空")
                        
                except Exception as e:
                    logger.error(f"转换 {stock_code} 周线数据失败: {e}")
                    continue
            
            if all_weekly_data:
                # 合并所有周线数据
                combined_weekly = pd.concat(all_weekly_data, ignore_index=True)
                logger.info(f"总共生成 {len(combined_weekly)} 条周线数据")
                
                # 保存到数据库（使用 replace 策略处理冲突）
                success = self.db_manager.save_stock_data(combined_weekly, "k_weekly", conflict_resolution="replace")
                if success:
                    logger.info("周线数据转换成功")
                    return True
                else:
                    logger.error("周线数据保存失败")
                    return False
            else:
                logger.error("没有生成任何周线数据")
                return False
                
        except Exception as e:
            logger.error(f"日线转周线数据转换失败: {e}")
            return False
    
    def daily_to_monthly(self, stock_codes: Optional[List[str]] = None) -> bool:
        """
        日线转月线数据
        
        Args:
            stock_codes: 股票代码列表，如果为None则转换所有股票
            
        Returns:
            bool: 转换是否成功
        """
        try:
            logger.info("开始日线转月线数据转换...")
            
            # 获取需要转换的股票列表
            if stock_codes is None:
                stock_codes = self.db_manager.get_stock_list("k_daily")
            
            if not stock_codes:
                logger.warning("没有找到需要转换的股票")
                return False
            
            all_monthly_data = []
            
            for stock_code in stock_codes:
                logger.info(f"正在转换 {stock_code} 的月线数据...")
                
                try:
                    # 获取日线数据
                    daily_data = self.db_manager.get_stock_data(
                        stock_code, "2020-01-01", "2025-12-31", "k_daily"
                    )
                    
                    if daily_data.empty:
                        logger.warning(f"{stock_code} 没有日线数据")
                        continue
                    
                    # 转换为月线数据
                    monthly_data = self._convert_to_monthly(daily_data)
                    
                    if not monthly_data.empty:
                        all_monthly_data.append(monthly_data)
                        logger.info(f"成功转换 {stock_code} 的 {len(monthly_data)} 条月线数据")
                    else:
                        logger.warning(f"{stock_code} 月线数据为空")
                        
                except Exception as e:
                    logger.error(f"转换 {stock_code} 月线数据失败: {e}")
                    continue
            
            if all_monthly_data:
                # 合并所有月线数据
                combined_monthly = pd.concat(all_monthly_data, ignore_index=True)
                logger.info(f"总共生成 {len(combined_monthly)} 条月线数据")
                
                # 保存到数据库（使用 replace 策略处理冲突）
                success = self.db_manager.save_stock_data(combined_monthly, "k_monthly", conflict_resolution="replace")
                if success:
                    logger.info("月线数据转换成功")
                    return True
                else:
                    logger.error("月线数据保存失败")
                    return False
            else:
                logger.error("没有生成任何月线数据")
                return False
                
        except Exception as e:
            logger.error(f"日线转月线数据转换失败: {e}")
            return False
    
    def _convert_to_weekly(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """
        将日线数据转换为周线数据
        
        Args:
            daily_data: 日线数据DataFrame
            
        Returns:
            pd.DataFrame: 周线数据
        """
        try:
            # 确保数据按日期排序
            daily_data = daily_data.sort_values('trade_date')
            
            # 转换日期格式
            daily_data['date'] = pd.to_datetime(daily_data['trade_date'])
            
            # 按周分组
            weekly_data = []
            
            for stock_code in daily_data['stock_code'].unique():
                stock_data = daily_data[daily_data['stock_code'] == stock_code].copy()
                
                # 按周分组
                stock_data['week'] = stock_data['date'].dt.to_period('W')
                
                # 计算周线数据
                weekly = stock_data.groupby('week').agg({
                    'stock_code': 'first',
                    'open': 'first',
                    'close': 'last',
                    'high': 'max',
                    'low': 'min',
                    'volume': 'sum'
                }).reset_index()
                
                # 转换周线日期
                weekly['trade_date'] = weekly['week'].dt.end_time.dt.strftime('%Y-%m-%d')
                weekly = weekly.drop('week', axis=1)
                
                weekly_data.append(weekly)
            
            if weekly_data:
                return pd.concat(weekly_data, ignore_index=True)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"转换周线数据失败: {e}")
            return pd.DataFrame()
    
    def _convert_to_monthly(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """
        将日线数据转换为月线数据
        
        Args:
            daily_data: 日线数据DataFrame
            
        Returns:
            pd.DataFrame: 月线数据
        """
        try:
            # 确保数据按日期排序
            daily_data = daily_data.sort_values('trade_date')
            
            # 转换日期格式
            daily_data['date'] = pd.to_datetime(daily_data['trade_date'])
            
            # 按月分组
            monthly_data = []
            
            for stock_code in daily_data['stock_code'].unique():
                stock_data = daily_data[daily_data['stock_code'] == stock_code].copy()
                
                # 按月分组
                stock_data['month'] = stock_data['date'].dt.to_period('M')
                
                # 计算月线数据
                monthly = stock_data.groupby('month').agg({
                    'stock_code': 'first',
                    'open': 'first',
                    'close': 'last',
                    'high': 'max',
                    'low': 'min',
                    'volume': 'sum'
                }).reset_index()
                
                # 转换月线日期
                monthly['trade_date'] = monthly['month'].dt.end_time.dt.strftime('%Y-%m-%d')
                monthly = monthly.drop('month', axis=1)
                
                monthly_data.append(monthly)
            
            if monthly_data:
                return pd.concat(monthly_data, ignore_index=True)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"转换月线数据失败: {e}")
            return pd.DataFrame()
