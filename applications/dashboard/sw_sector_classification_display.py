#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
申万板块分类展示系统 (quant_v2 版本)

功能：
1. 只看1级板块
2. 只看2级板块  
3. 只看1级板块的子板块（2级和3级）
4. 只看2级板块的子板块（3级）
5. 展示板块指数代码格式变化
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple
import streamlit as st
import os
import sys

# 添加项目根目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_management.database_manager import DatabaseManager


# 创建全局数据库管理器
db_manager = DatabaseManager()


def get_sw_hierarchy_data():
    """获取申万板块层次结构数据"""
    query = """
    SELECT DISTINCT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
    FROM sw_cfg_hierarchy 
    WHERE (l1_code IS NOT NULL AND l1_name IS NOT NULL)
       OR (l2_code IS NOT NULL AND l2_name IS NOT NULL)
       OR (l3_code IS NOT NULL AND l3_name IS NOT NULL)
    ORDER BY l1_code, l2_code, l3_code
    """
    df = db_manager.execute_query(query)
    return df

def get_available_index_codes():
    """获取可用的板块指数代码"""
    # 获取所有板块指数（已合并到index_k_daily表）
    query = "SELECT DISTINCT index_code FROM index_k_daily WHERE index_code LIKE '801%' ORDER BY index_code"
    df = db_manager.execute_query(query)
    
    return {
        'all': df['index_code'].tolist()
    }

def display_l1_sectors_only():
    """展示所有1级板块"""
    st.subheader("📊 申万1级板块列表")
    
    df = get_sw_hierarchy_data()
    l1_sectors = df[df['l1_code'].notna() & df['l1_name'].notna()][['l1_code', 'l1_name']].drop_duplicates()
    
    st.write(f"**总计：{len(l1_sectors)} 个1级板块**")
    
    # 创建展示表格
    display_df = l1_sectors.copy()
    display_df.columns = ['板块代码', '板块名称']
    display_df['指数代码(.ZS)'] = display_df['板块代码'].str.replace('.SI', '.ZS')
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    return l1_sectors

def display_l2_sectors_only():
    """展示所有2级板块"""
    st.subheader("📊 申万2级板块列表")
    
    df = get_sw_hierarchy_data()
    l2_sectors = df[df['l2_code'].notna() & df['l2_name'].notna()][['l2_code', 'l2_name']].drop_duplicates()
    
    st.write(f"**总计：{len(l2_sectors)} 个2级板块**")
    
    # 创建展示表格
    display_df = l2_sectors.copy()
    display_df.columns = ['板块代码', '板块名称']
    display_df['指数代码(.ZS)'] = display_df['板块代码'].str.replace('.SI', '.ZS')
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    return l2_sectors

def display_l1_with_sub_sectors():
    """展示1级板块及其所有子板块（2级和3级）"""
    st.subheader("📊 申万1级板块及其子板块")
    
    df = get_sw_hierarchy_data()
    
    # 获取所有1级板块
    l1_sectors = df[df['l1_code'].notna() & df['l1_name'].notna()][['l1_code', 'l1_name']].drop_duplicates()
    
    st.write(f"**总计：{len(l1_sectors)} 个1级板块**")
    
    for _, l1 in l1_sectors.iterrows():
        l1_code = l1['l1_code']
        l1_name = l1['l1_name']
        
        st.write(f"### {l1_name} ({l1_code})")
        
        # 获取该1级板块下的所有2级板块
        l2_sectors = df[
            (df['l1_code'] == l1_code) & 
            (df['l2_code'].notna()) & 
            (df['l2_name'].notna())
        ][['l2_code', 'l2_name']].drop_duplicates()
        
        if not l2_sectors.empty:
            st.write("**2级板块：**")
            for _, l2 in l2_sectors.iterrows():
                l2_code = l2['l2_code']
                l2_name = l2['l2_name']
                
                # 获取该2级板块下的所有3级板块
                l3_sectors = df[
                    (df['l2_code'] == l2_code) & 
                    (df['l3_code'].notna()) & 
                    (df['l3_name'].notna())
                ][['l3_code', 'l3_name']].drop_duplicates()
                
                if not l3_sectors.empty:
                    st.write(f"  - **{l2_name}** ({l2_code})")
                    for _, l3 in l3_sectors.iterrows():
                        l3_code = l3['l3_code']
                        l3_name = l3['l3_name']
                        st.write(f"    - {l3_name} ({l3_code})")
                else:
                    st.write(f"  - {l2_name} ({l2_code})")
        else:
            st.write("无子板块")

def display_l2_with_sub_sectors():
    """展示2级板块及其所有子板块（3级）"""
    st.subheader("📊 申万2级板块及其子板块")
    
    df = get_sw_hierarchy_data()
    
    # 获取所有2级板块
    l2_sectors = df[df['l2_code'].notna() & df['l2_name'].notna()][['l2_code', 'l2_name']].drop_duplicates()
    
    st.write(f"**总计：{len(l2_sectors)} 个2级板块**")
    
    for _, l2 in l2_sectors.iterrows():
        l2_code = l2['l2_code']
        l2_name = l2['l2_name']
        
        # 获取该2级板块下的所有3级板块
        l3_sectors = df[
            (df['l2_code'] == l2_code) & 
            (df['l3_code'].notna()) & 
            (df['l3_name'].notna())
        ][['l3_code', 'l3_name']].drop_duplicates()
        
        if not l3_sectors.empty:
            st.write(f"### {l2_name} ({l2_code})")
            for _, l3 in l3_sectors.iterrows():
                l3_code = l3['l3_code']
                l3_name = l3['l3_name']
                st.write(f"- {l3_name} ({l3_code})")
        else:
            st.write(f"### {l2_name} ({l2_code}) - 无子板块")

def display_sector_refinement_relationships():
    """展示板块细化关系"""
    st.subheader("📊 板块细化关系")
    
    # 获取所有板块指数代码
    index_codes = get_available_index_codes()
    
    if index_codes['all']:
        # 分析细化标识
        refinement_analysis = {}
        
        for code in index_codes['all']:
            # 提取细化标识
            if '.' in code:
                base_code, suffix = code.split('.', 1)
                if suffix not in refinement_analysis:
                    refinement_analysis[suffix] = []
                refinement_analysis[suffix].append(code)
        
        # 显示细化关系
        st.write("**板块细化标识统计：**")
        
        for suffix, codes in refinement_analysis.items():
            st.write(f"- **{suffix}**: {len(codes)} 个板块")
            if len(codes) <= 10:
                for code in codes:
                    st.write(f"  - {code}")
            else:
                for code in codes[:5]:
                    st.write(f"  - {code}")
                st.write(f"  ... 还有 {len(codes) - 5} 个")
        
        # 显示细化标识说明
        st.write("**细化标识说明：**")
        refinement_labels = {
            'ZS': '标准板块指数',
            'DSZ': '大市值板块',
            'XSZ': '小市值板块', 
            'GBJ': '高价股板块',
            'DBJ': '低价股板块',
            'DG': '大高股板块',
            'GQ': '国企股板块',
            'CQ': '超强股板块'
        }
        
        for suffix, label in refinement_labels.items():
            if suffix in refinement_analysis:
                st.write(f"- **{suffix}**: {label} ({len(refinement_analysis[suffix])} 个)")
    else:
        st.write("无板块指数数据")

def display_index_code_analysis():
    """展示指数代码分析"""
    st.subheader("📊 指数代码分析")
    
    # 获取所有板块指数代码
    index_codes = get_available_index_codes()
    
    if index_codes['all']:
        # 分析代码格式
        standard_codes = [code for code in index_codes['all'] if code.endswith('.ZS')]
        refined_codes = [code for code in index_codes['all'] if not code.endswith('.ZS')]
        
        st.write(f"**总计：{len(index_codes['all'])} 个板块指数**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**标准板块指数**")
            st.write(f"数量：{len(standard_codes)} 个")
            if standard_codes:
                sample_standard = standard_codes[:10]
                for code in sample_standard:
                    st.write(f"• {code}")
                if len(standard_codes) > 10:
                    st.write(f"... 还有 {len(standard_codes) - 10} 个")
        
        with col2:
            st.write("**细化板块指数**")
            st.write(f"数量：{len(refined_codes)} 个")
            if refined_codes:
                sample_refined = refined_codes[:10]
                for code in sample_refined:
                    st.write(f"• {code}")
                if len(refined_codes) > 10:
                    st.write(f"... 还有 {len(refined_codes) - 10} 个")
        
        # 显示代码格式说明
        st.write("**代码格式说明：**")
        st.write("• 配置表代码格式：`801010.SI`（.SI后缀）")
        st.write("• 标准指数：`801010.ZS`（.ZS后缀）")
        st.write("• 细化指数：`801010.DSZ`（大市值）、`801010.XSZ`（小市值）等")
        st.write("• 所有指数已统一存储在index_k_daily表中")
    else:
        st.write("无板块指数数据")
    st.write("• 细化标识：DSZ(大市值)、XSZ(小市值)、GBJ(高价股)、DBJ(低价股)、DG(大高股)、GQ(国企股)、CQ(超强股)")

def main():
    """主函数"""
    st.set_page_config(
        page_title="申万板块分类展示系统",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 申万板块分类展示系统")
    st.caption("展示申万三级板块的层次结构和指数代码格式")
    
    # 创建标签页
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "只看1级板块", 
        "只看2级板块", 
        "1级板块+子板块", 
        "2级板块+子板块",
        "板块细化关系",
        "指数代码分析"
    ])
    
    with tab1:
        display_l1_sectors_only()
    
    with tab2:
        display_l2_sectors_only()
    
    with tab3:
        display_l1_with_sub_sectors()
    
    with tab4:
        display_l2_with_sub_sectors()
    
    with tab5:
        display_sector_refinement_relationships()
    
    with tab6:
        display_index_code_analysis()

if __name__ == "__main__":
    main()
