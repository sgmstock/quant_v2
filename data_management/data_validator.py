"""
数据验证器

负责数据质量验证和完整性检查
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from core.utils.logger import get_logger

logger = get_logger("data_management.data_validator")


class DataValidator:
    """数据验证器"""
    
    def __init__(self):
        """初始化数据验证器"""
        pass
    
    def validate_stock_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证股票数据
        
        Args:
            df: 股票数据DataFrame
            
        Returns:
            Dict[str, Any]: 验证结果
        """
        try:
            validation_result = {
                "is_valid": True,
                "issues": [],
                "warnings": [],
                "statistics": {}
            }
            
            if df.empty:
                validation_result["is_valid"] = False
                validation_result["issues"].append("数据为空")
                return validation_result
            
            # 检查必需列
            required_columns = ['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                validation_result["is_valid"] = False
                validation_result["issues"].append(f"缺少必需列: {missing_columns}")
                return validation_result
            
            # 基本统计
            validation_result["statistics"] = {
                "total_rows": len(df),
                "unique_stocks": df['stock_code'].nunique(),
                "date_range": {
                    "start": df['trade_date'].min(),
                    "end": df['trade_date'].max()
                }
            }
            
            # 数据质量检查
            self._check_missing_values(df, validation_result)
            self._check_price_consistency(df, validation_result)
            self._check_date_consistency(df, validation_result)
            self._check_volume_consistency(df, validation_result)
            
            logger.info(f"数据验证完成: {'通过' if validation_result['is_valid'] else '失败'}")
            return validation_result
            
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            return {"is_valid": False, "issues": [f"验证过程出错: {e}"]}
    
    def _check_missing_values(self, df: pd.DataFrame, result: Dict[str, Any]):
        """检查缺失值"""
        try:
            missing_count = df.isnull().sum().sum()
            if missing_count > 0:
                result["warnings"].append(f"发现 {missing_count} 个缺失值")
                
                # 如果缺失值过多，标记为无效
                missing_ratio = missing_count / (len(df) * len(df.columns))
                if missing_ratio > 0.1:  # 缺失值超过10%
                    result["is_valid"] = False
                    result["issues"].append(f"缺失值比例过高: {missing_ratio:.2%}")
        except Exception as e:
            logger.error(f"检查缺失值失败: {e}")
    
    def _check_price_consistency(self, df: pd.DataFrame, result: Dict[str, Any]):
        """检查价格数据一致性"""
        try:
            # 检查价格 > 0
            price_columns = ['open', 'close', 'high', 'low']
            for col in price_columns:
                if col in df.columns:
                    invalid_count = (df[col] <= 0).sum()
                    if invalid_count > 0:
                        result["is_valid"] = False
                        result["issues"].append(f"{col} 列有 {invalid_count} 个非正值")
            
            # 检查高低价关系
            high_low_invalid = (df['high'] < df['low']).sum()
            if high_low_invalid > 0:
                result["is_valid"] = False
                result["issues"].append(f"发现 {high_low_invalid} 条高价小于低价的数据")
            
            # 检查开盘价和收盘价与高低价的关系
            open_invalid = ((df['open'] > df['high']) | (df['open'] < df['low'])).sum()
            close_invalid = ((df['close'] > df['high']) | (df['close'] < df['low'])).sum()
            
            if open_invalid > 0:
                result["is_valid"] = False
                result["issues"].append(f"发现 {open_invalid} 条开盘价超出高低价范围的数据")
            
            if close_invalid > 0:
                result["is_valid"] = False
                result["issues"].append(f"发现 {close_invalid} 条收盘价超出高低价范围的数据")
                
        except Exception as e:
            logger.error(f"检查价格一致性失败: {e}")
    
    def _check_date_consistency(self, df: pd.DataFrame, result: Dict[str, Any]):
        """检查日期数据一致性"""
        try:
            # 检查日期格式
            try:
                pd.to_datetime(df['trade_date'])
            except:
                result["is_valid"] = False
                result["issues"].append("日期格式不正确")
                return
            
            # 检查日期范围合理性
            dates = pd.to_datetime(df['trade_date'])
            min_date = dates.min()
            max_date = dates.max()
            
            # 检查是否在合理范围内（2000年至今）
            if min_date < pd.Timestamp('2000-01-01'):
                result["warnings"].append(f"发现过早期的日期: {min_date}")
            
            if max_date > pd.Timestamp.now():
                result["warnings"].append(f"发现未来日期: {max_date}")
            
            # 检查重复日期
            duplicate_dates = df.groupby('stock_code')['trade_date'].apply(lambda x: x.duplicated().sum()).sum()
            if duplicate_dates > 0:
                result["warnings"].append(f"发现 {duplicate_dates} 个重复日期")
                
        except Exception as e:
            logger.error(f"检查日期一致性失败: {e}")
    
    def _check_volume_consistency(self, df: pd.DataFrame, result: Dict[str, Any]):
        """检查成交量数据一致性"""
        try:
            # 检查成交量 >= 0
            negative_volume = (df['volume'] < 0).sum()
            if negative_volume > 0:
                result["is_valid"] = False
                result["issues"].append(f"发现 {negative_volume} 个负成交量")
            
            # 检查异常大的成交量
            volume_q99 = df['volume'].quantile(0.99)
            volume_q01 = df['volume'].quantile(0.01)
            outlier_volume = ((df['volume'] > volume_q99 * 10) | (df['volume'] < volume_q01 * 0.1)).sum()
            
            if outlier_volume > 0:
                result["warnings"].append(f"发现 {outlier_volume} 个异常成交量")
                
        except Exception as e:
            logger.error(f"检查成交量一致性失败: {e}")
    
    def validate_data_freshness(self, df: pd.DataFrame, max_days: int = 7) -> Dict[str, Any]:
        """
        验证数据新鲜度
        
        Args:
            df: 股票数据DataFrame
            max_days: 最大允许的天数
            
        Returns:
            Dict[str, Any]: 新鲜度验证结果
        """
        try:
            if df.empty:
                return {"is_fresh": False, "message": "数据为空"}
            
            # 获取最新日期
            latest_date = pd.to_datetime(df['trade_date']).max()
            days_old = (datetime.now() - latest_date).days
            
            is_fresh = days_old <= max_days
            
            result = {
                "is_fresh": is_fresh,
                "latest_date": latest_date.strftime('%Y-%m-%d'),
                "days_old": days_old,
                "max_allowed_days": max_days
            }
            
            if not is_fresh:
                result["message"] = f"数据过旧，最新日期为 {latest_date.strftime('%Y-%m-%d')}，已过去 {days_old} 天"
            else:
                result["message"] = f"数据新鲜，最新日期为 {latest_date.strftime('%Y-%m-%d')}"
            
            logger.info(f"数据新鲜度检查: {result['message']}")
            return result
            
        except Exception as e:
            logger.error(f"验证数据新鲜度失败: {e}")
            return {"is_fresh": False, "message": f"验证失败: {e}"}
    
    def get_validation_summary(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        获取验证结果摘要
        
        Args:
            validation_results: 验证结果列表
            
        Returns:
            Dict[str, Any]: 验证摘要
        """
        try:
            total_validations = len(validation_results)
            valid_count = sum(1 for result in validation_results if result.get("is_valid", False))
            
            all_issues = []
            all_warnings = []
            
            for result in validation_results:
                all_issues.extend(result.get("issues", []))
                all_warnings.extend(result.get("warnings", []))
            
            summary = {
                "total_validations": total_validations,
                "valid_count": valid_count,
                "invalid_count": total_validations - valid_count,
                "success_rate": valid_count / total_validations if total_validations > 0 else 0,
                "total_issues": len(all_issues),
                "total_warnings": len(all_warnings),
                "common_issues": list(set(all_issues)),
                "common_warnings": list(set(all_warnings))
            }
            
            logger.info(f"验证摘要: {valid_count}/{total_validations} 通过验证")
            return summary
            
        except Exception as e:
            logger.error(f"生成验证摘要失败: {e}")
            return {}