#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块内部个股某种字段为True情况下的市值占比统计

功能：
1. 统计板块内部个股某种字段==True情况下个股的市值占比
2. 支持l1_name, l2_name, l3_name三个层级的板块分析
3. 计算大高、超强、超超强、国企等字段的市值占比

"""

import pandas as pd
import numpy as np
import sqlite3
import os
from typing import Dict, List, Tuple


class SectorMarketCapAnalyzer:
    """板块市值占比分析器"""
    
    def __init__(self, db_path: str = "databases/quant_system.db"):
        """
        初始化分析器
        
        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self.sw_cfg_df: pd.DataFrame = pd.DataFrame()
        self.stock_basic_pro_df: pd.DataFrame = pd.DataFrame()
        self.merged_df: pd.DataFrame = pd.DataFrame()
        
    def load_data(self) -> bool:
        """
        加载数据表
        
        Returns:
            bool: 加载是否成功
        """
        try:
            print("正在加载数据表...")
            
            # 读取sw_cfg_hierarchy表
            self.sw_cfg_df = pd.read_sql_table('sw_cfg_hierarchy', f'sqlite:///{self.db_path}')
            print(f"✓ 成功加载sw_cfg_hierarchy表，共{len(self.sw_cfg_df)}条记录")
            
            # 读取stock_basic_pro表
            self.stock_basic_pro_df = pd.read_sql_table('stock_basic_pro', f'sqlite:///{self.db_path}')
            print(f"✓ 成功加载stock_basic_pro表，共{len(self.stock_basic_pro_df)}条记录")
            
            # 合并数据
            self.merged_df = pd.merge(
                self.sw_cfg_df, 
                self.stock_basic_pro_df, 
                on='stock_code', 
                how='inner'
            )
            print(f"✓ 数据合并完成，共{len(self.merged_df)}条记录")
            
            return True
            
        except Exception as e:
            print(f"✗ 数据加载失败: {e}")
            return False
    
    def calculate_sector_ratios(self, sector_level: str, sector_name: str, index_code: str = '') -> Dict:
        """
        计算指定板块的市值占比
        
        Args:
            sector_level: 板块层级 ('l1_name', 'l2_name', 'l3_name')
            sector_name: 板块名称
            index_code: 板块代码（可选）
            
        Returns:
            Dict: 包含各种字段占比的字典
        """
        # 筛选指定板块的股票
        if index_code and index_code != '':
            # 根据板块层级使用对应的代码字段筛选
            if sector_level == 'l1_name':
                sector_stocks = self.merged_df[self.merged_df['l1_code'] == index_code].copy()
            elif sector_level == 'l2_name':
                sector_stocks = self.merged_df[self.merged_df['l2_code'] == index_code].copy()
            elif sector_level == 'l3_name':
                sector_stocks = self.merged_df[self.merged_df['l3_code'] == index_code].copy()
            else:
                sector_stocks = self.merged_df[self.merged_df['index_code'] == index_code].copy()
        else:
            # 否则使用板块层级和名称筛选
            sector_stocks = self.merged_df[self.merged_df[sector_level] == sector_name].copy()
        
        if sector_stocks.empty:
            print(f"  警告: {sector_level} {sector_name} 没有找到股票数据")
            return {}
        
        # 计算总流通值
        total_market_cap = sector_stocks['流通值'].sum()
        
        if total_market_cap == 0:
            print(f"  警告: {sector_level} {sector_name} 总流通值为0")
            return {}
        
        # 计算各种字段为True的流通值占比
        ratios = {}
        
        # 大高占比
        dagao_stocks = sector_stocks[sector_stocks['大高'] == True]
        dagao_market_cap = dagao_stocks['流通值'].sum()
        ratios['大高'] = dagao_market_cap / total_market_cap if total_market_cap > 0 else 0
        
        # 超强占比
        chaoqiang_stocks = sector_stocks[sector_stocks['超强'] == True]
        chaoqiang_market_cap = chaoqiang_stocks['流通值'].sum()
        ratios['超强'] = chaoqiang_market_cap / total_market_cap if total_market_cap > 0 else 0
        
        # 超超强占比
        chaochaoqiang_stocks = sector_stocks[sector_stocks['超超强'] == True]
        chaochaoqiang_market_cap = chaochaoqiang_stocks['流通值'].sum()
        ratios['超超强'] = chaochaoqiang_market_cap / total_market_cap if total_market_cap > 0 else 0
        
        # 国企占比
        guoqi_stocks = sector_stocks[sector_stocks['国企'] == True]
        guoqi_market_cap = guoqi_stocks['流通值'].sum()
        ratios['国企'] = guoqi_market_cap / total_market_cap if total_market_cap > 0 else 0
        
        # 添加基础信息
        ratios['板块名称'] = sector_name
        ratios['板块层级'] = sector_level
        # 对于L2和L3级板块，使用对应的代码字段
        if sector_level == 'l2_name':
            if index_code:
                ratios['index_code'] = index_code
            elif not sector_stocks.empty and 'l2_code' in sector_stocks.columns:
                ratios['index_code'] = str(sector_stocks['l2_code'].iloc[0])
            else:
                ratios['index_code'] = ''
        elif sector_level == 'l3_name':
            if index_code:
                ratios['index_code'] = index_code
            elif not sector_stocks.empty and 'l3_code' in sector_stocks.columns:
                ratios['index_code'] = str(sector_stocks['l3_code'].iloc[0])
            else:
                ratios['index_code'] = ''
        else:
            if index_code:
                ratios['index_code'] = index_code
            elif not sector_stocks.empty and 'index_code' in sector_stocks.columns:
                ratios['index_code'] = str(sector_stocks['index_code'].iloc[0])
            else:
                ratios['index_code'] = ''
        
        ratios['总流通值'] = total_market_cap
        ratios['股票数量'] = len(sector_stocks)
        
        return ratios
    
    def analyze_all_sectors(self) -> pd.DataFrame:
        """
        分析所有板块的市值占比
        
        Returns:
            pd.DataFrame: 包含所有板块分析结果的DataFrame
        """
        print("开始分析所有板块的市值占比...")
        
        all_results = []
        
        # 分析L1级板块
        print("分析L1级板块...")
        l1_sectors = self.merged_df[['l1_name', 'l1_code']].drop_duplicates()
        for _, row in l1_sectors.iterrows():
            if pd.notna(row['l1_name']) and row['l1_name'] != '':
                ratios = self.calculate_sector_ratios('l1_name', str(row['l1_name']), str(row['l1_code']))
                if ratios:
                    all_results.append(ratios)
        
        # 分析L2级板块
        print("分析L2级板块...")
        l2_sectors = self.merged_df[['l2_name', 'l2_code']].drop_duplicates()
        l2_sectors = l2_sectors.dropna(subset=['l2_name'])
        l2_sectors = l2_sectors[l2_sectors['l2_name'] != '']
        print(f"L2级板块数量: {len(l2_sectors)}")
        
        for _, row in l2_sectors.iterrows():
            ratios = self.calculate_sector_ratios('l2_name', str(row['l2_name']), str(row['l2_code']))
            if ratios:
                all_results.append(ratios)
        
        # 分析L3级板块
        print("分析L3级板块...")
        l3_sectors = self.merged_df[['l3_name', 'l3_code']].drop_duplicates()
        l3_sectors = l3_sectors.dropna(subset=['l3_name'])
        l3_sectors = l3_sectors[l3_sectors['l3_name'] != '']
        print(f"L3级板块数量: {len(l3_sectors)}")
        
        for _, row in l3_sectors.iterrows():
            ratios = self.calculate_sector_ratios('l3_name', str(row['l3_name']), str(row['l3_code']))
            if ratios:
                all_results.append(ratios)
        
        # 转换为DataFrame
        result_df = pd.DataFrame(all_results)
        
        print(f"✓ 分析完成，共{len(result_df)}个板块")
        return result_df
    
    def create_multi_level_dataframe(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        创建多层级的DataFrame
        
        Args:
            result_df: 分析结果DataFrame
            
        Returns:
            pd.DataFrame: 多层级的DataFrame
        """
        # 创建多层级索引
        multi_index_data = []
        
        for _, row in result_df.iterrows():
            sector_name = row['板块名称']
            sector_level = row['板块层级']
            index_code = row.get('index_code', '')
            
            # 为每个字段创建一行数据
            for field in ['大高', '超强', '超超强', '国企']:
                multi_index_data.append({
                    '板块名称': sector_name,
                    '板块层级': sector_level,
                    'index_code': index_code,
                    '字段类型': field,
                    '市值占比': row[field],
                    '总流通值': row['总流通值'],
                    '股票数量': row['股票数量']
                })
        
        multi_df = pd.DataFrame(multi_index_data)
        
        # 创建多层级索引
        multi_df.set_index(['板块名称', '字段类型'], inplace=True)
        
        return multi_df
    
    def save_results(self, result_df: pd.DataFrame, multi_df: pd.DataFrame, 
                    output_file: str = None) -> bool:
        if output_file is None:
            # 使用相对路径指向 databases 目录
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_file = os.path.join(current_dir, '..', 'databases', 'sector_market_cap_analysis.xlsx')
            output_file = os.path.abspath(output_file)
        """
        保存分析结果
        
        Args:
            result_df: 原始分析结果
            multi_df: 多层级DataFrame
            output_file: 输出文件名
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
                # 保存原始结果
                result_df.to_excel(writer, sheet_name='原始结果', index=False)
                
                # 保存多层级结果
                multi_df.to_excel(writer, sheet_name='多层级结果')
                
                # 按板块层级分别保存
                for level in ['l1_name', 'l2_name', 'l3_name']:
                    level_data = result_df[result_df['板块层级'] == level]
                    if not level_data.empty:
                        level_data.to_excel(writer, sheet_name=f'{level}板块', index=False)
            
            print(f"✓ 结果已保存到: {output_file}")
            return True
            
        except Exception as e:
            print(f"✗ 保存结果失败: {e}")
            return False
    
    def print_summary(self, result_df: pd.DataFrame) -> None:
        """
        打印分析摘要
        
        Args:
            result_df: 分析结果DataFrame
        """
        print("\n" + "="*80)
        print("📊 板块市值占比分析摘要")
        print("="*80)
        
        # 按板块层级统计
        level_stats = result_df.groupby('板块层级').agg({
            '板块名称': 'count',
            '总流通值': 'sum',
            '股票数量': 'sum'
        })
        level_stats = level_stats.rename(columns={'板块名称': '板块数量'})
        
        print("\n�� 各层级板块统计:")
        print(level_stats)
        
        # 显示各字段的平均占比
        field_ratios = result_df[['大高', '超强', '超超强', '国企']].mean()
        print("\n📊 各字段平均市值占比:")
        for field in ['大高', '超强', '超超强', '国企']:
            if field in field_ratios.index:
                print(f"  {field}: {field_ratios[field]:.2%}")
        
        # 显示占比最高的板块
        print("\n🏆 各字段占比最高的板块:")
        for field in ['大高', '超强', '超超强', '国企']:
            top_sector = result_df.loc[result_df[field].idxmax()]
            print(f"  {field}: {top_sector['板块名称']} ({top_sector[field]:.2%})")
        
        # 显示交通运输板块的分析结果
        transport_l1 = result_df[result_df['板块名称'] == '交通运输']
        if not transport_l1.empty:
            print("\n🚚 交通运输板块分析:")
            transport_row = transport_l1.iloc[0]
            print(f"  板块代码: {transport_row.get('index_code', 'N/A')}")
            for field in ['大高', '超强', '超超强', '国企']:
                print(f"  {field}: {transport_row[field]:.2%}")
        
        # 显示前5个板块的详细信息
        print("\n📋 前5个板块详细信息:")
        top_5 = result_df.head(5)
        for _, row in top_5.iterrows():
            print(f"\n  {row['板块名称']} ({row.get('index_code', 'N/A')})")
            print(f"    层级: {row['板块层级']}")
            print(f"    总流通值: {row['总流通值']:,.0f}")
            print(f"    股票数量: {row['股票数量']}")
            for field in ['大高', '超强', '超超强', '国企']:
                print(f"    {field}: {row[field]:.2%}")


def main():
    """主函数"""
    print("="*80)
    print("🚀 板块内部个股某种字段为True情况下的市值占比统计")
    print("="*80)
    
    # 初始化分析器
    analyzer = SectorMarketCapAnalyzer()
    
    # 加载数据
    if not analyzer.load_data():
        print("❌ 数据加载失败，程序退出")
        return False
    
    # 分析所有板块
    result_df = analyzer.analyze_all_sectors()
    
    if result_df.empty:
        print("❌ 未获取到分析结果")
        return False
    
    # 创建多层级DataFrame
    multi_df = analyzer.create_multi_level_dataframe(result_df)
    
    # 打印摘要
    analyzer.print_summary(result_df)
    
    # 保存结果
    analyzer.save_results(result_df, multi_df)
    
    print("\n" + "="*80)
    print("🎉 分析完成！")
    print("="*80)
    
    return True


if __name__ == "__main__":
    main()