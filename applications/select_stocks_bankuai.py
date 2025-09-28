# #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多因子选股系统
支持多个因子的权重调整，适用于不同市场环境下的选股需求

主要功能：
1. 支持多个因子：低价、小市值等，可根据市场调整
2. 因子权重可人为调整
3. 支持房地产板块测试
4. 使用quantile(0.3)作为阈值，低于阈值的股票不进行分值增减
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import sys
from typing import Dict, List, Optional, Union
from pathlib import Path
import logging

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# v2环境导入
from data_management.database_manager import DatabaseManager
from core.utils.logger import get_logger

# 设置日志
logger = get_logger("applications.select_stocks_bankuai")

class MultiFactorStockSelector:
    """多因子选股器"""
    
    def __init__(self, db_path: str = None):
        """
        初始化多因子选股器
        
        Args:
            db_path: 数据库路径，如果为None则使用默认路径
        """
        # 使用v2环境的数据库管理器
        self.db_manager = DatabaseManager(db_path)
        self.db_path = self.db_manager.db_path
        self.factor_weights = {}
        self.factor_thresholds = {}
        
        # 因子类型配置：continuous表示连续型，boolean表示布尔型
        self.factor_types = {
            'price': 'continuous',
            'market_cap': 'continuous',
            'circulating_market_cap': 'continuous',
            'pe_ratio': 'continuous',
            'pb_ratio': 'continuous',
            # 布尔型因子
            'is_central_soe': 'boolean',      # 是否为央企
            'is_below_ma120': 'boolean',      # 是否低于120日线
            'is_st': 'boolean',               # 是否为ST股票
            'is_new_stock': 'boolean',        # 是否为次新股
        }
        
        # 因子方向配置：True表示"越大越好"，False表示"越小越好"
        self.factor_directions = {
            'price': False,              # 价格越低越好
            'market_cap': False,         # 市值越小越好
            'circulating_market_cap': False,  # 流通市值越小越好
            'pe_ratio': False,           # PE越低越好
            'pb_ratio': False,           # PB越低越好
            # 布尔型因子的方向，True代表我们期望值为1
            'is_central_soe': True,      # 央企更好
            'is_below_ma120': True,      # 低于120日线更好（低位）
            'is_st': False,              # ST股票不好
            'is_new_stock': True,        # 次新股更好
        }
        
    def set_factor_weights(self, weights: Dict[str, float]):
        """
        设置因子权重
        
        Args:
            weights: 因子权重字典，如 {'price': 0.6, 'market_cap': 0.4}
        """
        # 验证权重总和为1
        total_weight = sum(weights.values())
        if abs(total_weight - 1.0) > 0.01:
            logger.warning(f"权重总和为 {total_weight:.3f}，建议调整为1.0")
        
        self.factor_weights = weights
        logger.info(f"因子权重设置: {weights}")
        
    def set_factor_thresholds(self, thresholds: Dict[str, float]):
        """
        设置因子阈值（quantile值）
        
        Args:
            thresholds: 因子阈值字典，如 {'price': 0.3, 'market_cap': 0.3}
        """
        self.factor_thresholds = thresholds
        logger.info(f"因子阈值设置: {thresholds}")
        
    def get_stock_data_from_db(self, stock_codes: List[str]) -> pd.DataFrame:
        """
        从数据库获取股票数据
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            pd.DataFrame: 包含股票基本信息的DataFrame
        """
        if not stock_codes:
            return pd.DataFrame()
            
        try:
            # 构建查询语句
            placeholders = ','.join(['?' for _ in stock_codes])
            query = f"""
            SELECT 
                stock_code,
                stock_name,
                收盘价 as price,
                流通值 as circulating_market_cap,
                流通A股 as circulating_shares,
                国企 as is_central_soe,
                老股 as is_new_stock
            FROM stock_basic_pro 
            WHERE stock_code IN ({placeholders})
            """
            
            logger.info(f"查询的股票代码: {stock_codes}")
            df = self.db_manager.execute_query(query, stock_codes)
            
            if df.empty:
                logger.warning("未找到股票数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取 {len(df)} 只股票数据")
            logger.info(f"获取到的股票: {df['stock_code'].tolist()}")
            missing_stocks = set(stock_codes) - set(df['stock_code'].tolist())
            if missing_stocks:
                logger.info(f"未获取到的股票: {missing_stocks}")
                
            # 数据类型转换
            numeric_columns = ['price', 'circulating_market_cap', 'circulating_shares']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 布尔型因子转换
            boolean_columns = ['is_central_soe', 'is_new_stock']
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].astype(int)  # 转换为0/1整数
            
            # 计算总市值（如果流通市值可用，可以用作市值代理）
            if 'circulating_market_cap' in df.columns:
                df['market_cap'] = df['circulating_market_cap']  # 使用流通市值作为市值代理
            
            logger.info(f"成功获取 {len(df)} 只股票数据")
            return df
            
        except Exception as e:
            logger.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_data_from_csv(self, csv_path: str) -> pd.DataFrame:
        """
        从CSV文件获取股票数据
        
        Args:
            csv_path: CSV文件路径
            
        Returns:
            pd.DataFrame: 股票代码列表
        """
        try:
            df = pd.read_csv(csv_path, dtype={'stock_code': str})
            if 'stock_code' in df.columns:
                stock_codes = df['stock_code'].tolist()
                logger.info(f"从CSV文件读取到 {len(stock_codes)} 只股票")
                logger.info(f"股票代码: {stock_codes}")
                return self.get_stock_data_from_db(stock_codes)
            else:
                logger.error("CSV文件中未找到stock_code列")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"读取CSV文件失败: {e}")
            return pd.DataFrame()
    
    def get_bankuai_stocks(self, bankuai_name: str) -> List[str]:
        """
        获取板块成分股（调用现有函数）
        
        Args:
            bankuai_name: 板块名称
            
        Returns:
            List[str]: 股票代码列表
        """
        try:
            # 使用v2环境中的get_bankuai_stocks函数
            from core.utils.stock_filter import get_bankuai_stocks
            
            stock_codes = get_bankuai_stocks(bankuai_name)
            logger.info(f"获取到 {len(stock_codes)} 只 {bankuai_name} 板块股票")
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取板块股票失败: {e}")
            return []
    
    def calculate_factor_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算各因子得分
        
        新的评分逻辑：
        1. 对于每个因子，将股票分为两组：
           - 优质组：因子值 <= quantile(threshold)，全部给予满分100分
           - 普通组：因子值 > quantile(threshold)，进行内部排名评分
        
        Args:
            df: 股票数据DataFrame
            
        Returns:
            pd.DataFrame: 增加了因子得分的DataFrame
        """
        if df.empty:
            return df
            
        result_df = df.copy()
        
        # 使用循环处理所有因子，避免重复代码
        for factor in self.factor_weights.keys():
            if factor not in df.columns:
                logger.warning(f"因子 {factor} 在数据中不存在，跳过")
                continue
                
            # 获取因子配置
            factor_type = self.factor_types.get(factor, 'continuous')  # 默认为连续型
            direction = self.factor_directions.get(factor, False)
            
            # 初始化得分
            score_col = f"{factor}_score"
            result_df[score_col] = 0.0
            
            # 根据因子类型选择评分逻辑
            if factor_type == 'continuous':
                # 连续型因子的评分逻辑（原有逻辑）
                threshold = self.factor_thresholds.get(factor, 0.3)
                
                # 根据因子方向计算阈值和划分群体
                if direction:  # "越大越好"的因子
                    # 计算高位分位数，选择顶部的threshold%作为优质组
                    factor_threshold = df[factor].quantile(1 - threshold)
                    full_score_mask = df[factor] >= factor_threshold  # 优质组：值最大的threshold%
                    ranking_mask = df[factor] < factor_threshold      # 普通组：值较小的(1-threshold)%
                else:  # "越小越好"的因子
                    # 计算低位分位数，选择底部的threshold%作为优质组
                    factor_threshold = df[factor].quantile(threshold)
                    full_score_mask = df[factor] <= factor_threshold  # 优质组：值最小的threshold%
                    ranking_mask = df[factor] > factor_threshold     # 普通组：值较大的(1-threshold)%
                
                # 为优质组赋满分
                if full_score_mask.any():
                    result_df.loc[full_score_mask, score_col] = 100.0
                    
                # 为普通组进行排名评分
                if ranking_mask.any():
                    # 根据因子方向确定排名方向
                    ascending = direction  # direction=True时ascending=True，direction=False时ascending=False
                    
                    factor_ranks = df.loc[ranking_mask, factor].rank(ascending=ascending, pct=True) * 100
                    result_df.loc[ranking_mask, score_col] = factor_ranks
                    
                logger.info(f"连续因子 {factor} 评分完成，阈值: {factor_threshold:.2f}，优质组: {full_score_mask.sum()}只，普通组: {ranking_mask.sum()}只")
                
            elif factor_type == 'boolean':
                # 布尔型因子的评分逻辑（新增逻辑）
                if direction:  # True，代表期望值为1
                    # 期望值(1)得100分，非期望值(0)得0分
                    result_df[score_col] = np.where(df[factor] == 1, 100.0, 0.0)
                else:  # False，代表期望值为0
                    # 期望值(0)得100分，非期望值(1)得0分
                    result_df[score_col] = np.where(df[factor] == 0, 100.0, 0.0)
                
                # 统计布尔型因子的分布
                value_1_count = (df[factor] == 1).sum()
                value_0_count = (df[factor] == 0).sum()
                logger.info(f"布尔因子 {factor} 评分完成，值为1: {value_1_count}只，值为0: {value_0_count}只")
        
        return result_df
    
    def calculate_final_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算最终加权得分
        
        Args:
            df: 包含各因子得分的DataFrame
            
        Returns:
            pd.DataFrame: 增加了最终得分的DataFrame
        """
        if df.empty or not self.factor_weights:
            return df

        result_df = df.copy()
        
        # 计算最终加权得分
        final_score = 0.0
        for factor, weight in self.factor_weights.items():
            score_col = f"{factor}_score"
            if score_col in result_df.columns:
                final_score += result_df[score_col] * weight
                logger.info(f"因子 {factor} 权重: {weight}")
            else:
                logger.warning(f"未找到因子 {factor} 的得分列")
        
        result_df['final_score'] = final_score
        
        # 按最终得分排序
        result_df = result_df.sort_values('final_score', ascending=False).reset_index(drop=True)
        result_df['rank'] = range(1, len(result_df) + 1)
        
        logger.info("最终得分计算完成")
        return result_df
    
    def select_stocks(self, 
                     stock_source: Union[str, List[str]], 
                     source_type: str = 'bankuai',
                     top_n: int = 10) -> pd.DataFrame:
        """
        多因子选股主函数

    Args:
            stock_source: 股票来源，可以是板块名称、CSV路径或股票代码列表
            source_type: 来源类型 ('bankuai', 'csv', 'list')
            top_n: 返回前N只股票

    Returns:
            pd.DataFrame: 选股结果
        """
        logger.info(f"开始多因子选股，来源: {stock_source}, 类型: {source_type}")
        
        # 获取股票数据
        if source_type == 'bankuai':
            if isinstance(stock_source, str):
                stock_codes = self.get_bankuai_stocks(stock_source)
                if not stock_codes:
                    logger.error(f"未获取到 {stock_source} 板块股票")
                    return pd.DataFrame()
                df = self.get_stock_data_from_db(stock_codes)
            else:
                logger.error("板块名称必须是字符串")
                return pd.DataFrame()
            
        elif source_type == 'csv':
            if isinstance(stock_source, str):
                df = self.get_stock_data_from_csv(stock_source)
            else:
                logger.error("CSV路径必须是字符串")
                return pd.DataFrame()
            
        elif source_type == 'list':
            if isinstance(stock_source, list):
                df = self.get_stock_data_from_db(stock_source)
            else:
                logger.error("股票代码列表必须是列表")
                return pd.DataFrame()
            
        else:
            logger.error(f"不支持的数据源类型: {source_type}")
            return pd.DataFrame()
        
        if df.empty:
            logger.error("未获取到股票数据")
            return pd.DataFrame()
        
        logger.info(f"获取到 {len(df)} 只股票数据，开始计算因子得分")
        logger.info(f"数据列: {list(df.columns)}")
        
        # 计算因子得分
        df_with_scores = self.calculate_factor_scores(df)
        
        if df_with_scores.empty:
            logger.error("因子得分计算失败")
            return pd.DataFrame()
        
        # 计算最终得分
        final_df = self.calculate_final_scores(df_with_scores)
        
        if final_df.empty:
            logger.error("最终得分计算失败")
            return pd.DataFrame()
    
        # 返回前N只股票
        result = final_df.head(top_n)
        
        logger.info(f"选股完成，返回前 {len(result)} 只股票")
        return result
    
    def analyze_results(self, df: pd.DataFrame) -> Dict:
        """
        分析选股结果
        
        Args:
            df: 选股结果DataFrame
            
        Returns:
            Dict: 分析结果
        """
        if df.empty:
            return {}
        
        analysis = {
            'total_stocks': len(df),
            'avg_final_score': df['final_score'].mean(),
            'score_std': df['final_score'].std(),
            'price_stats': {
                'min': df['price'].min(),
                'max': df['price'].max(),
                'mean': df['price'].mean(),
                'median': df['price'].median()
            },
            'market_cap_stats': {
                'min': df['market_cap'].min(),
                'max': df['market_cap'].max(),
                'mean': df['market_cap'].mean(),
                'median': df['market_cap'].median()
            }
        }
        
        # 如果有因子得分，添加得分统计
        for factor in self.factor_weights.keys():
            score_col = f"{factor}_score"
            if score_col in df.columns:
                analysis[f'{factor}_score_stats'] = {
                    'min': df[score_col].min(),
                    'max': df[score_col].max(),
                    'mean': df[score_col].mean(),
                    'std': df[score_col].std()
                }
        
        return analysis


def main():
    """主函数 - 测试多因子选股系统"""
    
    # 创建选股器实例
    selector = MultiFactorStockSelector()
    
    # 设置因子权重（可根据市场情况调整）
    factor_weights = {
        'price': 0.4,           # 低价因子权重40%
        'market_cap': 0.3,      # 小市值因子权重30%
        'is_central_soe': 0.15, # 央企因子权重15%
        'is_new_stock': 0.15    # 次新股因子权重15%
    }
    selector.set_factor_weights(factor_weights)
    
    # 设置因子阈值（quantile值，布尔型因子不需要设置阈值）
    factor_thresholds = {
        'price': 0.3,         # 价格低于30%分位数的股票不参与低价评分
        'market_cap': 0.3     # 市值低于30%分位数的股票不参与小市值评分
    }
    selector.set_factor_thresholds(factor_thresholds)
    
    print("=" * 60)
    print("多因子选股系统测试")
    print("=" * 60)
    
    # 测试1：使用房地产板块
    print("\n测试1：房地产板块选股")
    print("-" * 40)
    
    try:
        real_estate_result = selector.select_stocks(
            stock_source='房地产I',
            source_type='bankuai',
            top_n=10
        )
        
        if not real_estate_result.empty:
            print("房地产板块选股结果（前10只）:")
            print(real_estate_result[['rank', 'stock_code', 'stock_name', 'price', 'market_cap', 
                                    'price_score', 'market_cap_score', 'final_score']].round(2))
            
            # 分析结果
            analysis = selector.analyze_results(real_estate_result)
            print(f"\n分析结果:")
            print(f"总股票数: {analysis['total_stocks']}")
            print(f"平均最终得分: {analysis['avg_final_score']:.2f}")
            print(f"价格范围: {analysis['price_stats']['min']:.2f} - {analysis['price_stats']['max']:.2f}")
            print(f"市值范围: {analysis['market_cap_stats']['min']:.2f} - {analysis['market_cap_stats']['max']:.2f}")
        else:
            print("房地产板块选股失败")
            
    except Exception as e:
        print(f"房地产板块选股出错: {e}")
    
    # 测试2：使用CSV文件
    print("\n测试2：使用CSV文件选股")
    print("-" * 40)
    
    csv_path = "databases/操作板块/2025-09-27/合并_传媒I 和 老股_2025-09-27.csv"
    if os.path.exists(csv_path):
        try:
            csv_result = selector.select_stocks(
                stock_source=csv_path,
                source_type='csv',
                top_n=15
            )
            
            if not csv_result.empty:
                print("CSV文件选股结果（前15只）:")
                # 构建显示列，包含所有可用的因子得分
                display_columns = ['rank', 'stock_code', 'stock_name', 'price', 'market_cap', 
                                 'price_score', 'market_cap_score']
                
                # 添加布尔型因子得分（如果存在）
                if 'is_central_soe_score' in csv_result.columns:
                    display_columns.append('is_central_soe_score')
                if 'is_new_stock_score' in csv_result.columns:
                    display_columns.append('is_new_stock_score')
                
                display_columns.append('final_score')
                
                # 只显示存在的列
                available_columns = [col for col in display_columns if col in csv_result.columns]
                print(csv_result[available_columns].round(2))
            else:
                print("CSV文件选股失败")
                
        except Exception as e:
            print(f"CSV文件选股出错: {e}")
    else:
        print(f"CSV文件不存在: {csv_path}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
