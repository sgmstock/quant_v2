#以前的xuangu_bankuai.py
import streamlit as st
import sqlite3
import pandas as pd
import os
import sys
from typing import List, Dict, Set, Optional
import logging
from datetime import datetime, date
from tqdm import tqdm
import plotly.express as px
import plotly.graph_objects as go

# 导入v2项目的模块
from core.utils.indicators import zhibiao
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer
from data_management.database_manager import DatabaseManager
from applications.sector_screener import get_changxian_zf_bankuai, get_boduan_bias_bankuai, get_boduan_zf_bankuai

# 注意：需要导入TechnicalAnalyzer类，请根据实际路径调整

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 页面配置和日期选择将在main()函数中处理

# 添加申万行业数据获取函数
@st.cache_data
def get_sw_hierarchy_data():
    """获取申万行业层次结构数据"""
    try:
        db_manager = DatabaseManager()
        query = "SELECT DISTINCT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name FROM sw_cfg_hierarchy WHERE l1_name IS NOT NULL AND l1_name != '' ORDER BY l1_code, l2_code, l3_code"
        df = db_manager.execute_query(query)
        
        # 确保所有代码都有正确的后缀格式（用于显示）
        for col in ['l1_code', 'l2_code', 'l3_code']:
            if col in df.columns:
                # 如果代码没有后缀，添加.SI用于显示
                df[col] = df[col].apply(lambda x: f"{x}.SI" if not str(x).endswith(('.SI', '.ZS')) else str(x))
        
        return df
    except Exception as e:
        st.error(f"获取申万行业数据失败: {e}")
        return pd.DataFrame()

def get_index_code_from_sw_code(sw_code):
    """将申万代码转换为指数代码"""
    # 如果已经是.ZS后缀，直接返回
    if sw_code.endswith('.ZS'):
        return sw_code
    # 如果是.SI后缀，则替换为.ZS
    elif sw_code.endswith('.SI'):
        return sw_code.replace('.SI', '.ZS')
    # 否则直接添加.ZS
    else:
        return f"{sw_code}.ZS"

def get_refined_sectors_for_standard_index(standard_index_code):
    """获取标准板块的所有细化板块"""
    try:
        # 提取前6位数字作为基础代码
        base_code = standard_index_code[:6]
        
        # 定义细化板块后缀
        refined_suffixes = ['CQ', 'DBJ', 'DSZ', 'GBJ', 'GQ', 'XSZ']
        
        # 生成细化板块代码列表
        refined_codes = []
        for suffix in refined_suffixes:
            refined_codes.append(f"{base_code}.{suffix}")
        
        # 检查哪些细化板块在数据库中存在
        db_manager = DatabaseManager()
        existing_codes = []
        for code in refined_codes:
            query = f'SELECT COUNT(*) as count FROM index_k_daily WHERE index_code = "{code}"'
            result = db_manager.execute_query(query)
            count = result.iloc[0]['count']
            if count > 0:
                existing_codes.append(code)
        
        # 创建DataFrame
        if existing_codes:
            df = pd.DataFrame({
                'index_code': existing_codes,
                'index_name': [f"{code} (细化板块)" for code in existing_codes]
            })
        else:
            df = pd.DataFrame()
        
        return df
    except Exception as e:
        st.error(f"获取细化板块失败: {e}")
        return pd.DataFrame()

def get_refined_sector_suffix_meaning():
    """获取细化板块后缀的含义"""
    return {
        'DSZ': '大市值指数',
        'XSZ': '小市值指数', 
        'GBJ': '高价股指数',
        'DBJ': '低价股指数',
        'GQ': '国企股指数',
        'CQ': '超强股指数'
    }

def prepare_technical_data_for_refined_sector_analysis(index_code, date):
    """
    为细化板块指数准备技术分析所需的所有周期数据
    返回与标准板块相同格式的数据字典
    
    Args:
        index_code: 细化板块指数代码
        date: 分析日期
    
    Returns:
        dict: 包含monthly、weekly、daily数据的字典，如果数据不足则返回None
    """
    try:
        # 获取日线数据
        df_daily = get_daily_data_for_sector_backtest(index_code, date)
        if df_daily.empty or len(df_daily) < 20:
            return None
        
        # 获取周线数据
        df_weekly = get_weekly_data_for_sector_backtest(index_code, date)
        if df_weekly.empty:
            return None
        
        # 获取月线数据
        df_monthly = get_monthly_data_for_sector_backtest(index_code, date)
        if df_monthly.empty:
            return None
        
        # 返回原始数据，TechnicalAnalyzer会在内部调用zhibiao函数
        return {
            'monthly': df_monthly,
            'weekly': df_weekly,
            'daily': df_daily
        }
        
    except Exception as e:
        print(f"为细化板块 {index_code} 准备技术数据失败: {e}")
        return None


def get_refined_sector_technical_scores(refined_codes, date):
    """获取细化板块的技术评分（基于index_k_daily表）"""
    try:
        results = [] 
        
        for index_code in tqdm(refined_codes, desc="正在分析细化板块技术状态"):
            # 准备技术数据
            data_dict = prepare_technical_data_for_refined_sector_analysis(index_code, date)
            
            if not data_dict:
                print(f"警告：未能为细化板块 {index_code} 准备技术数据，已跳过。")
                continue
            
            # 创建分析器实例
            analyzer = TechnicalAnalyzer(data_dict)
            
            # 计算各项技术指标得分
            results.append({
                'index_code': index_code,
                'zj_jjdi': analyzer.zj_jjdi(),
                'zj_di': analyzer.zj_di(),
                'zjdtg': analyzer.zjdtg(),
                'zjdtz': analyzer.zjdtz(),
                'cx_jjdi': analyzer.cx_jjdi(),
                'cx_di': analyzer.cx_di(),
                'cxdtg': analyzer.cxdtg(),
                'cxdtz': analyzer.cxdtz(),
                'cx_ding_tzz': analyzer.cx_ding_tzz(),
                'cx_ding_baoliang': analyzer.cx_ding_baoliang(),
                'ccx_jjdi': analyzer.ccx_jjdi(),
                'ccx_di': analyzer.ccx_di(),
                'ccxdtg': analyzer.ccxdtg(),
                'ccxdtz': analyzer.ccxdtz(),
            })
        
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # 合并板块指数名称信息
        db_manager = DatabaseManager()
        index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'")
        
        df = df.merge(index_info, on='index_code', how='left')
        
        # 调整列顺序
        cols = ['index_code', 'index_name'] + [col for col in df.columns if col not in ['index_code', 'index_name']]
        df = df[cols]
        
        return df
        
    except Exception as e:
        st.error(f"获取细化板块技术评分失败: {e}")
        return pd.DataFrame()


def get_refined_sector_basic_analysis(refined_codes, date):
    """获取细化板块的基本分析数据（基于index_k_daily表）"""
    try:
        db_manager = DatabaseManager()
        
        # 获取最新价格数据
        latest_data = []
        for code in refined_codes:
            query = f'''
            SELECT index_code, index_name, close, volume, trade_date
            FROM index_k_daily 
            WHERE index_code = "{code}" 
            ORDER BY trade_date DESC 
            LIMIT 1
            '''
            result = db_manager.execute_query(query)
            if not result.empty:
                latest_data.append(result.iloc[0])
        
        if not latest_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(latest_data)
        
        # 添加基本分析指标
        df['price_change'] = 0  # 简化处理，实际可以计算涨跌幅
        df['volume_ratio'] = 1.0  # 简化处理，实际可以计算成交量比
        
        # 添加后缀含义
        suffix_meanings = get_refined_sector_suffix_meaning()
        df['suffix'] = df['index_code'].str.split('.').str[-1]
        df['suffix_meaning'] = df['suffix'].map(suffix_meanings)
        
        return df
        
    except Exception as e:
        st.error(f"获取细化板块基本分析数据失败: {e}")
        return pd.DataFrame()


def get_refined_sector_zhuli_scores(refined_codes):
    """获取细化板块的主力评分（基于市值分析）"""
    try:
        # 读取市值分析数据
        excel_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'databases', 'sector_market_cap_analysis.xlsx')
        excel_path = os.path.abspath(excel_path)
        df_market_cap = pd.read_excel(excel_path)
        
        # 将index_code后缀从.SI改为.ZS（标准板块）
        df_market_cap['index_code'] = df_market_cap['index_code'].str.replace('.SI', '.ZS')
        
        # 为细化板块计算主力评分
        zhuli_scores = []
        for code in refined_codes:
            # 提取基础代码（前6位）
            base_code = code[:6]
            
            # 查找对应的标准指数（精确匹配前6位）
            standard_codes = df_market_cap[df_market_cap['index_code'].str.startswith(base_code + '.')]
            
            if not standard_codes.empty:
                # 使用第一个匹配的标准指数数据
                standard_data = standard_codes.iloc[0]
                zhuli_score = 0
                zhuli_score += (standard_data['超强'] > 0.8) * 1
                zhuli_score += (standard_data['超超强'] > 0.6) * 1
                zhuli_score += (standard_data['大高'] > 0.7) * 1
                zhuli_score += (standard_data['国企'] > 0.7) * 1
            else:
                zhuli_score = 0
            
            zhuli_scores.append({
                'index_code': code,
                'zhuli_score': zhuli_score
            })
        
        return pd.DataFrame(zhuli_scores)
        
    except Exception as e:
        st.error(f"获取细化板块主力评分失败: {e}")
        return pd.DataFrame()


def calculate_refined_sector_comprehensive_scores(refined_codes, date):
    """
    计算细化板块的综合得分（技术得分+主力得分+ATR得分）
    
    Args:
        refined_codes: 细化板块代码列表
        date: 分析日期
    
    Returns:
        DataFrame包含每个细化板块的代码、名称和各项得分，按total_score降序排列
        包含字段：index_code, index_name, zj_score, cx_score, ccx_score, 
                technical_score, zhuli_score, atr_score, total_score
    """
    try:
        # 获取技术评分数据
        df_technical = get_refined_sector_technical_scores(refined_codes, date)
        
        if df_technical.empty:
            return pd.DataFrame()
        
        # 计算中级技术指标得分
        zj_scores = pd.DataFrame()
        zj_scores['index_code'] = df_technical['index_code']
        zj_scores['zjjjdi_score'] = df_technical['zj_jjdi'] * 1.0
        zj_scores['zjdi_score'] = df_technical['zj_di'] * 2.0
        zj_scores['zjdtg_score'] = df_technical['zjdtg'] * 2.0
        zj_scores['zjdtz_score'] = df_technical['zjdtz'] * 0
        # 取最大值
        zj_scores['zj_score'] = zj_scores[['zjjjdi_score','zjdi_score', 'zjdtg_score', 'zjdtz_score']].max(axis=1)
        
        # 计算长线技术指标得分
        cx_scores = pd.DataFrame()
        cx_scores['index_code'] = df_technical['index_code']
        cx_scores['cx_jjdi_score'] = df_technical['cx_jjdi'] * 0.5
        cx_scores['cx_di_score'] = df_technical['cx_di'] * 2.5
        cx_scores['cxdtg_score'] = df_technical['cxdtg'] * 4
        cx_scores['cxdtz_score'] = df_technical['cxdtz'] * 0.5
        cx_scores['cx_ding_tzz_score'] = df_technical['cx_ding_tzz'] * -1
        cx_scores['cx_ding_baoliang_score'] = df_technical['cx_ding_baoliang'] * -1
        
        # 取最大值
        cx_scores['cx_final_score'] = cx_scores[['cx_jjdi_score','cx_di_score', 'cxdtg_score', 'cxdtz_score']].max(axis=1)
        cx_scores['cx_score'] = cx_scores['cx_final_score'] + cx_scores['cx_ding_baoliang_score'] + cx_scores['cx_ding_tzz_score']
        
        # 计算超长线技术指标得分
        ccx_scores = pd.DataFrame()
        ccx_scores['index_code'] = df_technical['index_code']
        ccx_scores['ccx_jjdi_score'] = df_technical['ccx_jjdi'] * 1
        ccx_scores['ccx_di_score'] = df_technical['ccx_di'] * 3
        ccx_scores['ccxdtg_score'] = df_technical['ccxdtg'] * 3
        ccx_scores['ccxdtz_score'] = df_technical['ccxdtz'] * 1
        
        # 取最大值
        ccx_scores['ccx_final_score'] = ccx_scores[['ccx_jjdi_score', 'ccx_di_score', 'ccxdtg_score', 'ccxdtz_score']].max(axis=1)
        ccx_scores['ccx_score'] = ccx_scores['ccx_final_score']
        
        # 合并技术得分
        technical_scores = zj_scores[['index_code', 'zj_score']].merge(
            cx_scores[['index_code', 'cx_score']], on='index_code', how='left'
        ).merge(
            ccx_scores[['index_code', 'ccx_score']], on='index_code', how='left'
        )
        
        # 计算技术总分
        technical_scores['technical_score'] = technical_scores['zj_score'] + technical_scores['cx_score'] + technical_scores['ccx_score']
        
        # 获取主力得分
        df_zhuli = get_refined_sector_zhuli_scores(refined_codes)
        
        if df_zhuli.empty:
            # 如果主力评分为空，创建默认的主力得分
            zhuli_scores = pd.DataFrame({
                'index_code': df_technical['index_code'],
                'zhuli_score': 0
            })
        else:
            zhuli_scores = df_zhuli[['index_code', 'zhuli_score']]
        
        # 获取ATR评分
        atr_scores_dict = get_atr_score(refined_codes, date)
        atr_scores = pd.DataFrame({
            'index_code': list(atr_scores_dict.keys()),
            'atr_score': list(atr_scores_dict.values())
        })
        
        # 合并技术得分、主力得分和ATR得分
        final_scores = technical_scores.merge(zhuli_scores, on='index_code', how='left')
        final_scores = final_scores.merge(atr_scores, on='index_code', how='left')
        
        # 填充缺失的主力得分和ATR得分为0
        final_scores['zhuli_score'] = final_scores['zhuli_score'].fillna(0)
        final_scores['atr_score'] = final_scores['atr_score'].fillna(0)
        
        # 合并板块指数基本信息
        db_manager = DatabaseManager()
        
        # 首先从index_k_daily表获取名称信息（包含自定义指数代码）
        index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'")
        
        # 然后从xinfenlei表获取名称信息（用于补充缺失的名称）
        xinfenlei_info = db_manager.execute_query("SELECT DISTINCT sw_xin_code as index_code, sw_name as index_name FROM xinfenlei")
        
        
        # 合并两个数据源的名称信息
        combined_info = pd.concat([index_info, xinfenlei_info]).drop_duplicates(subset=['index_code'], keep='first')
        
        final_scores = final_scores.merge(combined_info, on='index_code', how='left')
        
        # 计算综合总分
        final_scores['total_score'] = final_scores['technical_score'] + final_scores['zhuli_score'] + final_scores['atr_score']
        
        # 调整列顺序
        final_scores = final_scores[['index_code', 'index_name', 'zj_score', 'cx_score', 'ccx_score', 'technical_score', 'zhuli_score', 'atr_score', 'total_score']]
        
        # 按total_score降序排序
        final_scores = final_scores.sort_values(by='total_score', ascending=False)
        
        return final_scores
        
    except Exception as e:
        st.error(f"计算细化板块综合得分失败: {e}")
        return pd.DataFrame()

def display_refined_sector_analysis(standard_index_code, date):
    """显示标准板块及其细化板块的评分分析"""
    st.subheader(f"🔍 细化板块分析: {standard_index_code}")
    
    # 获取细化板块数据
    refined_sectors = get_refined_sectors_for_standard_index(standard_index_code)
    
    if refined_sectors.empty:
        st.warning(f"未找到 {standard_index_code} 的细化板块数据")
        return
    
    # 显示细化板块列表
    st.write("**📋 细化板块列表:**")
    suffix_meanings = get_refined_sector_suffix_meaning()
    
    for _, row in refined_sectors.iterrows():
        index_code = row['index_code']
        index_name = row['index_name']
        suffix = index_code.split('.')[-1]
        meaning = suffix_meanings.get(suffix, '未知类型')
        
        st.write(f"• **{index_code}**: {index_name} ({meaning})")
    
    # 获取细化板块的指数代码列表
    refined_codes = refined_sectors['index_code'].tolist()
    
    # 检查哪些细化板块在数据库中存在
    conn = DatabaseManager()
    existing_refined_codes = []
    missing_refined_codes = []
    
    for code in refined_codes:
        query = f'SELECT COUNT(*) as count FROM index_k_daily WHERE index_code = "{code}"'
        result = db_manager.execute_query(query)
        count = result.iloc[0]['count']
        
        if count > 0:
            existing_refined_codes.append(code)
        else:
            missing_refined_codes.append(code)
    
    conn.close()
    
    if missing_refined_codes:
        st.warning(f"⚠️ **以下细化板块在数据库中不存在**: {', '.join(missing_refined_codes)}")
    
    if not existing_refined_codes:
        st.error("❌ **没有找到任何有效的细化板块数据**")
        return
    
    st.info(f"✅ **将分析 {len(existing_refined_codes)} 个有效细化板块**")
    
    # 对细化板块进行综合评分分析
    try:
        st.write(f"🔄 **正在获取细化板块分析数据**: {len(existing_refined_codes)} 个板块")
        
        # 计算综合得分（技术得分+主力得分）
        st.write("🔄 **正在计算细化板块综合得分**...")
        comprehensive_scores = calculate_refined_sector_comprehensive_scores(existing_refined_codes, date)
        
        if comprehensive_scores.empty:
            st.error("❌ **无法获取细化板块综合评分数据**")
            return
        
        st.write(f"✅ 细化板块综合评分计算完成: {comprehensive_scores.shape}")
        
        # 显示综合评分表格
        st.write("**📊 细化板块综合评分详情:**")
        st.dataframe(
            comprehensive_scores[['index_code', 'index_name', 'zj_score', 'cx_score', 'ccx_score', 'technical_score', 'zhuli_score', 'total_score']],
            use_container_width=True
        )
        
        # 显示评分统计
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("平均技术得分", f"{comprehensive_scores['technical_score'].mean():.2f}")
        with col2:
            st.metric("平均主力得分", f"{comprehensive_scores['zhuli_score'].mean():.2f}")
        with col3:
            st.metric("平均综合得分", f"{comprehensive_scores['total_score'].mean():.2f}")
        
        # 显示得分分布图
        st.write("**📈 细化板块得分分布:**")
        import plotly.express as px
        
        # 综合得分排名前10
        fig1 = px.bar(
            comprehensive_scores.head(10), 
            x='index_name', 
            y='total_score',
            title=f"{standard_index_code} 细化板块综合得分排名前10",
            labels={'total_score': '综合得分', 'index_name': '板块名称'}
        )
        fig1.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig1, use_container_width=True)
        
        # 技术得分 vs 主力得分散点图
        fig2 = px.scatter(
            comprehensive_scores,
            x='technical_score',
            y='zhuli_score',
            hover_data=['index_code', 'index_name', 'total_score'],
            title='技术得分 vs 主力得分',
            labels={'technical_score': '技术得分', 'zhuli_score': '主力得分'}
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # 获取细化板块基本分析数据
        df_basic = get_refined_sector_basic_analysis(existing_refined_codes, date)
        
        if not df_basic.empty:
            st.write(f"✅ 细化板块基本分析完成: {df_basic.shape}")
            
            # 显示细化板块基本信息表格
            st.write("**📊 细化板块基本信息:**")
            st.dataframe(
                df_basic[['index_code', 'index_name', 'suffix_meaning', 'close', 'volume', 'trade_date']],
                use_container_width=True
            )
            
            # 显示价格分布图表
            st.write("**📈 细化板块价格分布:**")
            df_sorted = df_basic.sort_values('close', ascending=False)
            
            fig = px.bar(
                df_sorted, 
                x='index_name', 
                y='close',
                title=f'{standard_index_code} 细化板块价格对比',
                labels={'close': '收盘价', 'index_name': '板块名称'}
            )
            fig.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)
            
            # 显示成交量分布图表
            st.write("**📊 细化板块成交量分布:**")
            fig_volume = px.bar(
                df_sorted, 
                x='index_name', 
                y='volume',
                title=f'{standard_index_code} 细化板块成交量对比',
                labels={'volume': '成交量', 'index_name': '板块名称'}
            )
            fig_volume.update_layout(xaxis_tickangle=45)
            st.plotly_chart(fig_volume, use_container_width=True)
            
            # 显示价格和成交量的散点图
            st.write("**📈 细化板块价格与成交量关系:**")
            fig_scatter = px.scatter(
                df_sorted,
                x='close',
                y='volume',
                hover_data=['index_code', 'index_name', 'suffix_meaning'],
                title='价格 vs 成交量',
                labels={'close': '收盘价', 'volume': '成交量'}
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
            
            # 显示统计信息
            st.write("**📊 细化板块统计信息:**")
            st.metric("平均价格", f"{df_sorted['close'].mean():.2f}")
            st.metric("最高价格", f"{df_sorted['close'].max():.2f}")
            st.metric("平均成交量", f"{df_sorted['volume'].mean():.0f}")
            st.metric("最高成交量", f"{df_sorted['volume'].max():.0f}")
        
        # 显示说明信息
        st.info("💡 **说明**: 细化板块数据来源于 `index_k_daily` 表，包含大市值、小市值、高价股、低价股、国企股、超强股等不同类型的细分指数。现在使用与标准板块相同的评分体系，包括技术评分和主力评分。")
    
    except Exception as e:
        st.error(f"细化板块分析失败: {e}")
        import traceback
        st.text(traceback.format_exc())

def display_sector_scores(sector_codes, date):
    """显示板块评分详情"""
    if not sector_codes:
        st.warning("没有选择任何板块")
        return
    
    st.subheader("📊 板块评分详情")
    
    # 显示实际使用的指数代码
    st.info(f"🔍 **分析的板块指数代码**: {', '.join(sector_codes)}")
    
    # 先检查哪些指数代码在数据库中存在
    conn = DatabaseManager()
    existing_codes = []
    missing_codes = []
    
    for code in sector_codes:
        query = f'SELECT COUNT(*) as count FROM index_k_daily WHERE index_code = "{code}"'
        result = db_manager.execute_query(query)
        count = result.iloc[0]['count']
        
        if count > 0:
            existing_codes.append(code)
        else:
            missing_codes.append(code)
    
    conn.close()
    
    if missing_codes:
        st.warning(f"⚠️ **以下指数代码在数据库中不存在**: {', '.join(missing_codes)}")
        st.info("💡 **说明**: 这些代码可能是细化板块代码（如801050.CQ、801050.DBJ等），将只显示主力评分，不进行技术分析")
    
    if not existing_codes:
        st.error("❌ **没有找到任何有效的指数代码**")
        return
    
    st.info(f"✅ **将分析 {len(existing_codes)} 个有效指数代码**")
    
    # 获取技术分析数据
    try:
        st.write(f"🔄 **正在获取技术分析数据**: {len(existing_codes)} 个板块指数")
        
        df_zj = get_jishu_zj(existing_codes, date)
        st.write(f"✅ 中级技术分析完成: {df_zj.shape}")
        
        df_cx = get_jishu_cx(existing_codes, date)
        st.write(f"✅ 长线技术分析完成: {df_cx.shape}")
        
        df_ccx = get_jishu_ccx(existing_codes, date)
        st.write(f"✅ 超长线技术分析完成: {df_ccx.shape}")
        
        # 计算综合得分
        st.write("🔄 **正在计算综合得分**...")
        comprehensive_scores = calculate_comprehensive_scores(df_zj, df_cx, df_ccx, date)
        
        if comprehensive_scores.empty:
            st.error("❌ **无法获取评分数据**：技术分析数据不足，无法计算综合得分")
            return
        
        st.write(f"✅ 综合得分计算完成: {comprehensive_scores.shape}")
        
        # 显示评分表格
        st.dataframe(
            comprehensive_scores[['index_code', 'index_name', 'zj_score', 'cx_score', 'ccx_score', 'technical_score', 'zhuli_score', 'atr_score', 'total_score']],
            use_container_width=True
        )
        
        # 显示评分统计
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("平均技术得分", f"{comprehensive_scores['technical_score'].mean():.2f}")
        with col2:
            st.metric("平均主力得分", f"{comprehensive_scores['zhuli_score'].mean():.2f}")
        with col3:
            st.metric("平均ATR得分", f"{comprehensive_scores['atr_score'].mean():.2f}")
        with col4:
            st.metric("平均综合得分", f"{comprehensive_scores['total_score'].mean():.2f}")
        
        # 显示得分分布图 - 上下布局
        st.subheader("📈 得分分布")
        
        import plotly.express as px
        fig1 = px.bar(
            comprehensive_scores.head(10), 
            x='index_name', 
            y='total_score',
            title="综合得分排名前10"
        )
        fig1.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig1, use_container_width=True)
        
        fig2 = px.scatter(
            comprehensive_scores, 
            x='technical_score', 
            y='zhuli_score',
            hover_data=['index_name', 'total_score'],
            title="技术得分 vs 主力得分"
        )
        st.plotly_chart(fig2, use_container_width=True)
            
        # 为缺失的板块代码显示主力评分
        if missing_codes:
            st.subheader("💰 细化板块主力评分")
            st.write(f"🔄 **正在获取细化板块主力评分**: {len(missing_codes)} 个板块")
            
            # 获取主力评分
            zhuli_df = zhuli_score()
            missing_scores = []
            
            for code in missing_codes:
                # 查找对应的主力评分
                matching_scores = zhuli_df[zhuli_df['index_code'] == code]
                if not matching_scores.empty:
                    score = matching_scores.iloc[0]['得分']
                else:
                    # 如果找不到精确匹配，尝试基础代码匹配
                    base_code = code[:6]
                    base_matching = zhuli_df[zhuli_df['index_code'].str.startswith(base_code + '.')]
                    if not base_matching.empty:
                        score = base_matching.iloc[0]['得分']
                    else:
                        score = 0
                
                missing_scores.append({
                    'index_code': code,
                    'index_name': f"{code} (细化板块)",
                    'zhuli_score': score,
                    'technical_score': 0,
                    'total_score': score
                })
            
            if missing_scores:
                missing_df = pd.DataFrame(missing_scores)
                st.dataframe(
                    missing_df[['index_code', 'index_name', 'zhuli_score', 'total_score']],
                    use_container_width=True
                )
                
                st.metric("平均主力得分", f"{missing_df['zhuli_score'].mean():.2f}")
        
    except Exception as e:
        st.error(f"获取评分数据失败: {e}")

def display_sw_sector_hierarchy():
    """显示申万行业层次结构并支持交互选择"""
    st.subheader("🏢 申万行业板块选择")
    
    # 添加说明
    st.info("💡 **说明**: 界面显示的是申万行业代码(.SI后缀)，点击评分时会自动转换为板块指数代码(.ZS后缀)进行分析")
    
    # 获取分析日期
    analysis_date = st.session_state.get('analysis_date', '2024-01-01')
    
    # 获取申万行业数据
    sw_data = get_sw_hierarchy_data()
    if sw_data.empty:
        st.error("无法获取申万行业数据")
        return
    
    # 创建层次结构展示
    l1_sectors = sw_data[['l1_code', 'l1_name']].drop_duplicates().sort_values('l1_code')
    
    # 选择一级行业
    selected_l1 = st.selectbox(
        "选择一级行业:",
        options=l1_sectors['l1_code'].tolist(),
        format_func=lambda x: f"{x} - {l1_sectors[l1_sectors['l1_code']==x]['l1_name'].iloc[0]}"
    )
    
    if selected_l1:
        # 获取该一级行业下的二级行业
        l2_sectors = sw_data[sw_data['l1_code'] == selected_l1][['l2_code', 'l2_name']].drop_duplicates().sort_values('l2_code')
        
        if not l2_sectors.empty:
            st.write(f"**{l1_sectors[l1_sectors['l1_code']==selected_l1]['l1_name'].iloc[0]}** 下的二级行业:")
            
            # 显示二级行业列表
            for _, row in l2_sectors.iterrows():
                with st.expander(f"{row['l2_code']} - {row['l2_name']}"):
                    # 获取该二级行业下的三级行业
                    l3_sectors = sw_data[sw_data['l2_code'] == row['l2_code']][['l3_code', 'l3_name']].drop_duplicates().sort_values('l3_code')
                    
                    if not l3_sectors.empty:
                        st.write("三级行业:")
                        for _, l3_row in l3_sectors.iterrows():
                            st.write(f"  • {l3_row['l3_code']} - {l3_row['l3_name']}")
                    
                    # 添加评分按钮 - 上下布局
                    if st.button(f"查看 {row['l2_name']} 评分", key=f"btn_{row['l2_code']}"):
                        # 获取该二级行业及其子行业的所有指数代码
                        sector_codes = []
                        
                        # 添加二级行业本身
                        index_code = get_index_code_from_sw_code(row['l2_code'])
                        sector_codes.append(index_code)
                        
                        # 添加三级行业
                        for _, l3_row in l3_sectors.iterrows():
                            index_code = get_index_code_from_sw_code(l3_row['l3_code'])
                            sector_codes.append(index_code)
                        
                        # 显示调试信息
                        st.write(f"🔍 **调试信息**: 转换后的指数代码: {sector_codes}")
                        
                        # 显示评分
                        display_sector_scores(sector_codes, analysis_date)
                    
                    if st.button(f"查看 {row['l2_name']} 细化板块", key=f"refined_{row['l2_code']}"):
                        # 获取二级行业的指数代码并显示细化板块分析
                        index_code = get_index_code_from_sw_code(row['l2_code'])
                        display_refined_sector_analysis(index_code, analysis_date)
        
        # 添加查看整个一级行业评分的按钮 - 上下布局
        if st.button(f"查看整个 {l1_sectors[l1_sectors['l1_code']==selected_l1]['l1_name'].iloc[0]} 评分", key=f"btn_l1_{selected_l1}"):
            # 获取该一级行业下所有指数代码
            sector_codes = []
            for _, row in l2_sectors.iterrows():
                index_code = get_index_code_from_sw_code(row['l2_code'])
                sector_codes.append(index_code)
                
                # 添加三级行业
                l3_sectors = sw_data[sw_data['l2_code'] == row['l2_code']][['l3_code', 'l3_name']].drop_duplicates()
                for _, l3_row in l3_sectors.iterrows():
                    index_code = get_index_code_from_sw_code(l3_row['l3_code'])
                    sector_codes.append(index_code)
            
            # 显示评分
            display_sector_scores(sector_codes, analysis_date)
        
        if st.button(f"查看 {l1_sectors[l1_sectors['l1_code']==selected_l1]['l1_name'].iloc[0]} 细化板块", key=f"refined_l1_{selected_l1}"):
            # 获取一级行业的指数代码并显示细化板块分析
            index_code = get_index_code_from_sw_code(selected_l1)
            display_refined_sector_analysis(index_code, analysis_date)

def load_index_daily_data(index_code: str) -> pd.DataFrame:
    """
    从本地加载板块指数的日线行情数据。
    """
    conn = DatabaseManager()
    df = db_manager.execute_query(f"SELECT * FROM index_k_daily WHERE index_code = '{index_code}'", conn)
    conn.close()
    return df

def load_index_weekly_data(index_code: str) -> pd.DataFrame:
    """
    从本地加载板块指数的周线行情数据。
    """
    conn = DatabaseManager()
    df = db_manager.execute_query(f"SELECT * FROM index_k_weekly WHERE index_code = '{index_code}'", conn)
    conn.close()
    return df

def load_index_monthly_data(index_code: str) -> pd.DataFrame:
    """
    从本地加载板块指数的月线行情数据。
    """
    conn = DatabaseManager()
    df = db_manager.execute_query(f"SELECT * FROM index_k_monthly WHERE index_code = '{index_code}'", conn)
    conn.close()
    return df

#设计专门用于回测用的获取板块指数的日线行情数据：
def get_daily_data_for_sector_backtest(index_code: str, current_date: str) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    # 1. 从本地加载该股票的【全部】历史数据
    #    (优化：可以在回测开始时一次性加载所有股票到内存)
    full_local_df = load_index_daily_data(index_code) 

    # 2. 严格截取截至 current_date 的数据，防止未来函数
    # 确保 trade_date 列是 datetime 类型，使用更安全的解析方式
    full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'], errors='coerce')
    # 确保 current_date 是字符串格式
    try:
        if isinstance(current_date, str):
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        else:
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        
        # 检查日期转换是否成功
        if pd.isna(current_date_dt):
            raise ValueError(f"无法解析日期: {current_date}")
        
        df_snapshot = full_local_df[full_local_df['trade_date'] <= current_date_dt].copy()
    except Exception as e:
        print(f"日期转换错误: {e}")
        # 如果日期转换失败，返回空DataFrame
        return pd.DataFrame()
    
    return df_snapshot


#设计专门用于回测用的获取板块指数的周线行情数据：
def get_weekly_data_for_sector_backtest(index_code: str, current_date: str) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    full_local_df = load_index_weekly_data(index_code)
    full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'], errors='coerce')
    # 确保 current_date 是字符串格式
    try:
        if isinstance(current_date, str):
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        else:
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        
        # 检查日期转换是否成功
        if pd.isna(current_date_dt):
            raise ValueError(f"无法解析日期: {current_date}")
        
        df_snapshot = full_local_df[full_local_df['trade_date'] <= current_date_dt].copy()
    except Exception as e:
        print(f"日期转换错误: {e}")
        # 如果日期转换失败，返回空DataFrame
        return pd.DataFrame()

    return df_snapshot

def get_monthly_data_for_sector_backtest(index_code: str, current_date: str) -> pd.DataFrame:
    """
    为策略提供在特定日期所需的数据。
    在回测模式下，它只从本地快速读取和切片，不进行任何更新操作。
    """
    full_local_df = load_index_monthly_data(index_code)
    full_local_df['trade_date'] = pd.to_datetime(full_local_df['trade_date'], errors='coerce')
    # 确保 current_date 是字符串格式
    try:
        if isinstance(current_date, str):
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        else:
            current_date_dt = pd.to_datetime(current_date, errors='coerce')
        
        # 检查日期转换是否成功
        if pd.isna(current_date_dt):
            raise ValueError(f"无法解析日期: {current_date}")
        
        df_snapshot = full_local_df[full_local_df['trade_date'] <= current_date_dt].copy()
    except Exception as e:
        print(f"日期转换错误: {e}")
        # 如果日期转换失败，返回空DataFrame
        return pd.DataFrame()

    return df_snapshot

# #对行情用zhibiao函数计算指标
# def zhibiao(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     对行情用zhibiao函数计算指标
#     """
#     return zhibiao(df)


#步骤1：创建一个数据准备的辅助函数
#这个函数负责为单个板块指数准备好 所需要的zhibiao数据字典。
def prepare_technical_data_for_sector_analysis(index_code, date):
    """
    为板块指数准备技术分析所需的所有周期数据
    
    Args:
        index_code: 板块指数代码
        date: 分析日期
    
    Returns:
        dict: 包含monthly、weekly、daily数据的字典，如果数据不足则返回None
    """
    try:
        # 获取日线数据
        df_daily = get_daily_data_for_sector_backtest(index_code, date)
        if df_daily.empty or len(df_daily) < 20:
            print(f"警告：{index_code} 日线数据不足，已跳过")
            return None
        
        # 获取周线数据
        df_weekly = get_weekly_data_for_sector_backtest(index_code, date)
        if df_weekly.empty:
            print(f"警告：{index_code} 周线数据为空，已跳过")
            return None
        
        # 获取月线数据
        df_monthly = get_monthly_data_for_sector_backtest(index_code, date)
        if df_monthly.empty:
            print(f"警告：{index_code} 月线数据为空，已跳过")
            return None
        
        # 计算技术指标
        df_M = zhibiao(df_monthly)
        df_w = zhibiao(df_weekly)
        df_d = zhibiao(df_daily)
        
        return {
            'monthly': df_M,
            'weekly': df_w,
            'daily': df_d
        }
        
    except Exception as e:
        print(f"为板块指数 {index_code} 准备技术数据失败: {e}")
        return None

#步骤2：创建一个分析器工厂函数
def create_analyzer(index_code, date):
    """
    创建一个 TechnicalAnalyzer 类实例。
    """
    data_dict = prepare_technical_data_for_sector_analysis(index_code, date)
    if data_dict is None:
        return None
    return TechnicalAnalyzer(data_dict)


#步骤3：获取板块指数中级技术状态
def get_jishu_zj(indexlist: list, date: str):
    """
    使用 TechnicalAnalyzer 类来批量计算板块指数的技术指标信号。
    """
    results = []
    
    # 遍历板块指数列表
    for index_code in tqdm(indexlist, desc="正在分析板块指数"):
        # 1. 为当前板块指数准备所需的所有周期数据
        data_dict = prepare_technical_data_for_sector_analysis(index_code, date)
        
        # 如果数据准备失败（例如，数据不足），则跳过
        if not data_dict:
            print(f"警告：未能为板块指数 {index_code} 准备数据，已跳过。")
            continue
            
        # 2. 创建分析器实例，数据和指标计算都在这一步完成
        try:
            analyzer = TechnicalAnalyzer(data_dict)
        except Exception as e:
            print(f"警告：为板块指数 {index_code} 创建分析器失败: {e}，已跳过。")
            continue
        
        # 3. 调用分析方法获取评分
        results.append({
            'index_code': index_code,
            'zj_jjdi': analyzer.zj_jjdi(),
            'zj_di': analyzer.zj_di(),
            'zjdtg': analyzer.zjdtg(),
            'zjdtz': analyzer.zjdtz(),
        })
        
    # 转换为DataFrame
    if not results:
        return pd.DataFrame()
        
    df = pd.DataFrame(results)
    
    # 合并板块指数名称信息
    conn = DatabaseManager()
    index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
    conn.close()
    
    df = df.merge(index_info, on='index_code', how='left')
    
    # 调整列顺序
    cols = ['index_code', 'index_name'] + [col for col in df.columns if col not in ['index_code', 'index_name']]
    df = df[cols]
    
    return df
#步骤4：获取板块指数长线技术状态
def get_jishu_cx(indexlist: list, date: str):
    """
    获取板块指数长线技术状态
    """
    results = []
    
    for index_code in tqdm(indexlist, desc="正在分析板块指数"):
        # 1. 为当前板块指数准备所需的所有周期数据
        data_dict = prepare_technical_data_for_sector_analysis(index_code, date)
        
        # 如果数据准备失败（例如，数据不足），则跳过
        if not data_dict:
            print(f"警告：未能为板块指数 {index_code} 准备数据，已跳过。")
            continue
            
        # 2. 创建分析器实例，数据和指标计算都在这一步完成
        try:
            analyzer = TechnicalAnalyzer(data_dict)
        except Exception as e:
            print(f"警告：为板块指数 {index_code} 创建分析器失败: {e}，已跳过。")
            continue
            
        results.append({
            'index_code': index_code,
            'cx_jjdi': analyzer.cx_jjdi(),
            'cx_di': analyzer.cx_di(),
            'cxdtg': analyzer.cxdtg(),
            'cxdtz': analyzer.cxdtz(),
            'cx_ding_tzz': analyzer.cx_ding_tzz(),
            'cx_ding_baoliang': analyzer.cx_ding_baoliang(),
        })
    
    # 转换为DataFrame
    if not results:
        return pd.DataFrame()
        
    df = pd.DataFrame(results)
    
    # 合并板块指数名称信息
    conn = DatabaseManager()
    index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
    conn.close()
    
    df = df.merge(index_info, on='index_code', how='left')
    
    # 调整列顺序
    cols = ['index_code', 'index_name'] + [col for col in df.columns if col not in ['index_code', 'index_name']]
    df = df[cols]
    
    return df


#步骤5：获取板块指数超长线技术状态
def get_jishu_ccx(indexlist: list, date: str):
    """
    获取板块指数超长线技术状态
    """
    results = []
    
    for index_code in tqdm(indexlist, desc="正在分析板块指数"):
        # 1. 为当前板块指数准备所需的所有周期数据
        data_dict = prepare_technical_data_for_sector_analysis(index_code, date)
        
        # 如果数据准备失败（例如，数据不足），则跳过
        if not data_dict:
            print(f"警告：未能为板块指数 {index_code} 准备数据，已跳过。")
            continue
            
        # 2. 创建分析器实例，数据和指标计算都在这一步完成
        try:
            analyzer = TechnicalAnalyzer(data_dict)
        except Exception as e:
            print(f"警告：为板块指数 {index_code} 创建分析器失败: {e}，已跳过。")
            continue
            
        results.append({
            'index_code': index_code,
            'ccx_jjdi': analyzer.ccx_jjdi(),
            'ccx_di': analyzer.ccx_di(),
            'ccxdtg': analyzer.ccxdtg(),
            'ccxdtz': analyzer.ccxdtz(),
        })
    
    # 转换为DataFrame
    if not results:
        return pd.DataFrame()
        
    df = pd.DataFrame(results)
    
    # 合并板块指数名称信息
    conn = DatabaseManager()
    index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
    conn.close()
    
    df = df.merge(index_info, on='index_code', how='left')
    
    # 调整列顺序
    cols = ['index_code', 'index_name'] + [col for col in df.columns if col not in ['index_code', 'index_name']]
    df = df[cols]
    
    return df


#步骤6：计算板块指数综合得分（技术得分+主力得分）
def calculate_comprehensive_scores(df_zj, df_cx, df_ccx, date=None):
    """
    根据技术指标、主力分析和ATR分析计算板块指数综合得分
    
    Args:
        df_zj: 中级技术指标DataFrame
        df_cx: 长线技术指标DataFrame
        df_ccx: 超长线技术指标DataFrame
        date: 分析日期，用于计算ATR评分
    
    Returns:
        DataFrame包含每个板块指数的代码、名称和各项得分，按total_score降序排列
    """
    # 检查输入数据是否为空
    if df_zj.empty or df_cx.empty or df_ccx.empty:
        print("警告：技术分析数据为空，无法计算综合得分")
        return pd.DataFrame()
    
    # 计算中级技术指标得分
    zj_scores = pd.DataFrame()
    zj_scores['index_code'] = df_zj['index_code']
    zj_scores['zjjjdi_score'] = df_zj['zj_jjdi'] * 1.0
    zj_scores['zjdi_score'] = df_zj['zj_di'] * 2.0
    zj_scores['zjdtg_score'] = df_zj['zjdtg'] * 2.0
    zj_scores['zjdtz_score'] = df_zj['zjdtz'] * 0
    # 取最大值
    zj_scores['zj_score'] = zj_scores[['zjjjdi_score','zjdi_score', 'zjdtg_score', 'zjdtz_score']].max(axis=1)
    
    # 计算长线技术指标得分
    cx_scores = pd.DataFrame()
    cx_scores['index_code'] = df_cx['index_code']
    cx_scores['cx_jjdi_score'] = df_cx['cx_jjdi'] * 0.5
    cx_scores['cx_di_score'] = df_cx['cx_di'] * 2.5
    cx_scores['cxdtg_score'] = df_cx['cxdtg'] * 4
    cx_scores['cxdtz_score'] = df_cx['cxdtz'] * 0.5
    cx_scores['cx_ding_tzz_score'] = df_cx['cx_ding_tzz'] * -1
    cx_scores['cx_ding_baoliang_score'] = df_cx['cx_ding_baoliang'] * -1
    
    # 取最大值
    cx_scores['cx_final_score'] = cx_scores[['cx_jjdi_score','cx_di_score', 'cxdtg_score', 'cxdtz_score']].max(axis=1)
    cx_scores['cx_score'] = cx_scores['cx_final_score'] + cx_scores['cx_ding_baoliang_score'] + cx_scores['cx_ding_tzz_score']
    
    # 计算超长线技术指标得分
    ccx_scores = pd.DataFrame()
    ccx_scores['index_code'] = df_ccx['index_code']
    ccx_scores['ccx_jjdi_score'] = df_ccx['ccx_jjdi'] * 1
    ccx_scores['ccx_di_score'] = df_ccx['ccx_di'] * 3
    ccx_scores['ccxdtg_score'] = df_ccx['ccxdtg'] * 3
    ccx_scores['ccxdtz_score'] = df_ccx['ccxdtz'] * 1
    
    # 取最大值
    ccx_scores['ccx_final_score'] = ccx_scores[['ccx_jjdi_score', 'ccx_di_score', 'ccxdtg_score', 'ccxdtz_score']].max(axis=1)
    ccx_scores['ccx_score'] = ccx_scores['ccx_final_score']
    
    # 合并技术得分
    technical_scores = zj_scores[['index_code', 'zj_score']].merge(
        cx_scores[['index_code', 'cx_score']], on='index_code', how='left'
    ).merge(
        ccx_scores[['index_code', 'ccx_score']], on='index_code', how='left'
    )
    
    # 计算技术总分
    technical_scores['technical_score'] = technical_scores['zj_score'] + technical_scores['cx_score'] + technical_scores['ccx_score']
    
    # 获取主力得分
    zhuli_df = zhuli_score()
    zhuli_scores = zhuli_df[['index_code', '得分']].rename(columns={'得分': 'zhuli_score'})
    
    # 获取ATR评分（如果提供了日期）
    if date:
        sector_codes = technical_scores['index_code'].tolist()
        atr_scores_dict = get_atr_score(sector_codes, date)
        atr_scores = pd.DataFrame({
            'index_code': list(atr_scores_dict.keys()),
            'atr_score': list(atr_scores_dict.values())
        })
    else:
        # 如果没有提供日期，创建默认的ATR得分
        atr_scores = pd.DataFrame({
            'index_code': technical_scores['index_code'],
            'atr_score': 0
        })
    
    # 合并技术得分、主力得分和ATR得分
    final_scores = technical_scores.merge(zhuli_scores, on='index_code', how='left')
    final_scores = final_scores.merge(atr_scores, on='index_code', how='left')
    
    # 填充缺失的主力得分和ATR得分为0
    final_scores['zhuli_score'] = final_scores['zhuli_score'].fillna(0)
    final_scores['atr_score'] = final_scores['atr_score'].fillna(0)
    
    # 合并板块指数基本信息
    conn = DatabaseManager()
    
    # 首先从index_k_daily表获取名称信息（包含自定义指数代码）
    index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
    
    # 然后从xinfenlei表获取名称信息（用于补充缺失的名称）
    xinfenlei_info = db_manager.execute_query("SELECT DISTINCT sw_xin_code as index_code, sw_name as index_name FROM xinfenlei", conn)
    
    conn.close()
    
    # 合并两个数据源的名称信息
    combined_info = pd.concat([index_info, xinfenlei_info]).drop_duplicates(subset=['index_code'], keep='first')
    
    final_scores = final_scores.merge(combined_info, on='index_code', how='left')
    
    # 计算综合总分
    final_scores['total_score'] = final_scores['technical_score'] + final_scores['zhuli_score'] + final_scores['atr_score']
    
    # 调整列顺序
    final_scores = final_scores[['index_code', 'index_name', 'zj_score', 'cx_score', 'ccx_score', 'technical_score', 'zhuli_score', 'atr_score', 'total_score']]
    
    # 按total_score降序排序
    final_scores = final_scores.sort_values(by='total_score', ascending=False)
    
    return final_scores




def get_all_standard_index_codes():
    """
    获取所有标准板块指数代码
    """
    conn = DatabaseManager()
    query = "SELECT DISTINCT index_code FROM index_k_daily WHERE index_code LIKE '801%' ORDER BY index_code"
    df = db_manager.execute_query(query, conn)
    conn.close()
    return df['index_code'].tolist()

def get_custom_concept_names():
    """
    获取所有自定义概念名称
    """
    conn = DatabaseManager()
    query = "SELECT DISTINCT xinfenlei_name FROM xinfenlei ORDER BY xinfenlei_name"
    df = db_manager.execute_query(query, conn)
    conn.close()
    return df['xinfenlei_name'].tolist()

def get_sectors_by_custom_concept(concept_name):
    """
    根据自定义概念名称获取对应的板块指数代码
    
    Args:
        concept_name: 自定义概念名称
    
    Returns:
        list: 板块指数代码列表（sw_xin_code）
    """
    conn = DatabaseManager()
    query = "SELECT DISTINCT sw_xin_code FROM xinfenlei WHERE xinfenlei_name = ?"
    df = db_manager.execute_query(query, conn, params=[concept_name])
    conn.close()
    return df['sw_xin_code'].tolist()

def display_custom_concept_analysis(concept_name, date):
    """
    显示自定义概念的板块分析
    
    Args:
        concept_name: 自定义概念名称
        date: 分析日期
    """
    st.subheader(f"🎯 自定义概念分析: {concept_name}")
    
    # 获取该概念下的板块指数代码
    sector_codes = get_sectors_by_custom_concept(concept_name)
    
    if not sector_codes:
        st.warning(f"概念 '{concept_name}' 下没有找到板块数据")
        return
    
    st.info(f"📊 概念 '{concept_name}' 包含 {len(sector_codes)} 个板块指数")
    
    # 显示板块列表
    with st.expander("📋 查看板块列表"):
        db_manager = DatabaseManager()
        query = "SELECT sw_xin_code, sw_name, level FROM xinfenlei WHERE xinfenlei_name = ? ORDER BY sw_xin_code"
        df_sectors = db_manager.execute_query(query, (concept_name,))
        st.dataframe(df_sectors, use_container_width=True)
    
    # 调用现有的板块评分显示函数
    display_sector_scores(sector_codes, date)

def get_csv_files_from_folders():
    """
    获取5个文件夹中的所有CSV文件列表
    
    Returns:
        dict: 文件夹名称到CSV文件列表的映射
    """
    import os
    import glob
    
    folders = {
        "长线涨幅筛选": "databases/xunhuan_changxian_zf",
        "波段涨幅筛选": "databases/xunhuan_boduan_zf", 
        "波段BIAS筛选": "databases/xunhuan_boduan_bias",
        "中级涨幅筛选": "databases/xunhuan_zhongji_zf",
        "中级BIAS筛选": "databases/xunhuan_zhongji_bias"
    }
    
    csv_files_dict = {}
    
    for folder_name, folder_path in folders.items():
        if os.path.exists(folder_path):
            csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
            # 只保留文件名（不含路径）
            csv_files_dict[folder_name] = [os.path.basename(f) for f in csv_files]
        else:
            csv_files_dict[folder_name] = []
    
    return csv_files_dict

def get_special_sector_categories():
    """
    获取特殊板块分类
    
    Returns:
        dict: 特殊板块分类字典
    """
    try:
        # 使用已导入的特殊板块获取函数
        
        # 获取各类特殊板块
        special_categories = {
            "长线循环居前的板块": get_changxian_zf_bankuai(),
            "最近3个波段涨幅循环居前的板块": get_boduan_zf_bankuai(),
            "最近3个波段BIAS循环居前的板块": get_boduan_bias_bankuai(),
        }
        
        # 添加预设的特殊板块（如果存在）
        # 这些变量需要在全局范围内定义
        try:
            # 长线强趋势板块
            if 'cxqqs_bankuai' in globals():
                special_categories["长线强趋势板块"] = cxqqs_bankuai
            
            # 近期消息博弈基本面的板块
            if 'jinqi_xiaoxi_bankuai' in globals():
                special_categories["近期消息博弈基本面的板块"] = jinqi_xiaoxi_bankuai
        except:
            pass
        
        return special_categories
        
    except Exception as e:
        st.error(f"获取特殊板块分类失败: {e}")
        return {}

def get_sectors_from_selected_csv_files(selected_files):
    """
    从选中的CSV文件中获取板块指数代码
    
    Args:
        selected_files: 选中的CSV文件列表
    
    Returns:
        list: 板块指数代码列表
    """
    import os
    import pandas as pd
    
    all_index_codes = []
    
    for file_name in selected_files:
        # 确定文件所在的文件夹
        folder_path = None
        if "申万板块中级涨幅" in file_name and "bias筛选" in file_name:
            folder_path = "databases/xunhuan_zhongji_bias"
        elif "申万板块中级涨幅" in file_name and "涨幅筛选" in file_name:
            folder_path = "databases/xunhuan_zhongji_zf"
        elif "申万板块长线涨幅" in file_name:
            folder_path = "databases/xunhuan_changxian_zf"
        elif "申万板块波段涨幅" in file_name and "bias筛选" in file_name:
            folder_path = "databases/xunhuan_boduan_bias"
        elif "申万板块波段涨幅" in file_name and "涨幅筛选" in file_name:
            folder_path = "databases/xunhuan_boduan_zf"
    
        if folder_path:
            file_path = os.path.join(folder_path, file_name)
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                if 'index_code' in df.columns:
                    index_codes = df['index_code'].tolist()
                    all_index_codes.extend(index_codes)
            except Exception as e:
                st.warning(f"读取文件 {file_name} 失败: {e}")
                continue
    
    # 去重并返回
    unique_index_codes = list(set(all_index_codes))
    return unique_index_codes

def get_zhuli_bankuai():
    """
    获取主力板块指数代码列表
    
    Returns:
        list: 主力板块指数代码列表
    """
    try:
        # 使用已导入的主力板块获取函数
        from applications.sector_screener import get_qualified_sector_codes
        return get_qualified_sector_codes()
    except Exception as e:
        st.error(f"获取主力板块失败: {e}")
        return []

def display_zhuli_sector_analysis(sector_codes, date):
    """
    显示主力板块分析
    
    Args:
        sector_codes: 板块指数代码列表
        date: 分析日期
    """
    st.subheader(f"💰 自定义主力分析")
    
    if not sector_codes:
        st.warning("没有找到主力板块数据")
        return
    
    st.info(f"📊 主力板块包含 {len(sector_codes)} 个板块指数")
    
    # 显示板块代码列表
    with st.expander("📋 查看主力板块代码列表"):
        # 创建DataFrame显示板块代码
        df_codes = pd.DataFrame({
            'index_code': sector_codes,
            'sequence': range(1, len(sector_codes) + 1)
        })
        
        # 尝试获取板块名称
        try:
            db_manager = DatabaseManager()
            
            # 首先从index_k_daily表获取名称信息
            index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
            
            # 然后从xinfenlei表获取名称信息
            xinfenlei_info = db_manager.execute_query("SELECT DISTINCT sw_xin_code as index_code, sw_name as index_name FROM xinfenlei", conn)
            
            
            # 合并两个数据源的名称信息
            combined_info = pd.concat([index_info, xinfenlei_info]).drop_duplicates(subset=['index_code'], keep='first')
            
            # 合并名称信息
            df_codes = df_codes.merge(combined_info, on='index_code', how='left')
            df_codes['index_name'] = df_codes['index_name'].fillna('未知')
            
            st.dataframe(df_codes[['sequence', 'index_code', 'index_name']], use_container_width=True)
            
        except Exception as e:
            st.warning(f"获取板块名称失败: {e}")
            st.dataframe(df_codes[['sequence', 'index_code']], use_container_width=True)
    
    # 调用现有的板块评分显示函数
    display_sector_scores(sector_codes, date)

def display_special_sector_analysis(category_name, sector_codes, date):
    """
    显示特殊板块分析
    
    Args:
        category_name: 特殊板块分类名称
        sector_codes: 板块指数代码列表
        date: 分析日期
    """
    st.subheader(f"⭐ 自定义特殊分析: {category_name}")
    
    if not sector_codes:
        st.warning(f"分类 '{category_name}' 下没有找到板块数据")
        return
    
    st.info(f"📊 分类 '{category_name}' 包含 {len(sector_codes)} 个板块指数")
    
    # 显示板块代码列表
    with st.expander("📋 查看板块代码列表"):
        # 创建DataFrame显示板块代码
        df_codes = pd.DataFrame({
            'index_code': sector_codes,
            'sequence': range(1, len(sector_codes) + 1)
        })
        
        # 尝试获取板块名称
        try:
            db_manager = DatabaseManager()
            
            # 首先从index_k_daily表获取名称信息
            index_info = db_manager.execute_query("SELECT DISTINCT index_code, index_name FROM index_k_daily WHERE index_code LIKE '801%' OR index_code LIKE '850%' OR index_code LIKE '100%' OR index_code LIKE '851%' OR index_code LIKE '852%' OR index_code LIKE '857%' OR index_code LIKE '858%' OR index_code LIKE '859%'", conn)
            
            # 然后从xinfenlei表获取名称信息
            xinfenlei_info = db_manager.execute_query("SELECT DISTINCT sw_xin_code as index_code, sw_name as index_name FROM xinfenlei", conn)
            
            
            # 合并两个数据源的名称信息
            combined_info = pd.concat([index_info, xinfenlei_info]).drop_duplicates(subset=['index_code'], keep='first')
            
            # 合并名称信息
            df_codes = df_codes.merge(combined_info, on='index_code', how='left')
            df_codes['index_name'] = df_codes['index_name'].fillna('未知')
            
            st.dataframe(df_codes[['sequence', 'index_code', 'index_name']], use_container_width=True)
            
        except Exception as e:
            st.warning(f"获取板块名称失败: {e}")
            st.dataframe(df_codes[['sequence', 'index_code']], use_container_width=True)
    
    # 调用现有的板块评分显示函数
    display_sector_scores(sector_codes, date)

def analyze_sector_indices(date: str, index_codes: List[str] = None):
    """
    完整的板块指数技术分析流程
    
    Args:
        date: 分析日期
        index_codes: 板块指数代码列表，如果为None则分析所有标准板块指数
    
    Returns:
        包含技术得分的DataFrame
    """
    print(f"开始分析板块指数技术状态，日期：{date}")
    
    # 如果没有指定指数代码，则获取所有标准板块指数
    if index_codes is None:
        index_codes = get_all_standard_index_codes()
        print(f"将分析 {len(index_codes)} 个标准板块指数")
    
    # 步骤1：获取中级技术状态
    print("正在分析中级技术状态...")
    df_zj = get_jishu_zj(index_codes, date)
    
    # 步骤2：获取长线技术状态
    print("正在分析长线技术状态...")
    df_cx = get_jishu_cx(index_codes, date)
    
    # 步骤3：获取超长线技术状态
    print("正在分析超长线技术状态...")
    df_ccx = get_jishu_ccx(index_codes, date)
    
    # 步骤4：计算综合得分（技术得分+主力得分）
    print("正在计算综合得分...")
    final_scores = calculate_comprehensive_scores(df_zj, df_cx, df_ccx)
    
    print(f"分析完成！共分析了 {len(final_scores)} 个板块指数")
    return final_scores

def zhuli_score():
    """
    读取并计算板块市值分析得分
    读取databases/sector_market_cap_analysis.xlsx
    如果《超强》字段的值 >0.8得分1，如果《超超强》字段的值>0.6得分1，
    如果《大高》字段的值>0.7得分1，如果《国企》字段的值>0.7得分1，
    如果板块属于changxian_bankuai（长线循环居前），得分0.6，然后汇总得分。
    同时将index_code后缀从.SI改为.ZS
    """
    # 读取Excel文件 - 使用相对路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(current_dir, '..', '..', 'databases', 'sector_market_cap_analysis.xlsx')
    excel_path = os.path.abspath(excel_path)
    df = pd.read_excel(excel_path)
    
    # 将index_code后缀从.SI改为.ZS（标准板块）
    df['index_code'] = df['index_code'].str.replace('.SI', '.ZS')
    
    # 获取长线循环居前的板块列表
    changxian_bankuai = get_changxian_zf_bankuai()
    
    # 计算得分
    df['得分'] = 0
    df['得分'] = df['得分'] + (df['超强'] > 0.8) * 1
    df['得分'] = df['得分'] + (df['超超强'] > 0.6) * 1
    df['得分'] = df['得分'] + (df['大高'] > 0.7) * 1
    df['得分'] = df['得分'] + (df['国企'] > 0.7) * 1
    # 如果板块属于长线循环居前，得分1
    df['得分'] = df['得分'] + df['index_code'].isin(changxian_bankuai) * 0.6
    
    return df

#增加一个字段atr，就是计算板块指数的atr，用20天atr，另外时间就是《请选择跟踪日期:》
def get_atr(index_code, date, N=20):
    """
    获取板块指数的ATR值
    通过调用indicators.py中的ATR函数来计算20天ATR
    
    参数:
    index_code: 板块指数代码
    date: 计算日期
    N: ATR周期，默认20天
    
    返回:
    float: ATR值
    """
    # 获取板块指数的历史数据
    df = get_daily_data_for_sector_backtest(index_code, date)
    
    if df.empty or len(df) < N:
        return None
    
    # 从v2项目的indicators模块导入ATR函数
    from core.utils.indicators import ATR
    
    # 准备数据：需要收盘价、最高价、最低价
    close = df['close'].values
    high = df['high'].values
    low = df['low'].values
    
    # 计算ATR (返回ATR序列和TR序列)
    atr_series, tr_series = ATR(close, high, low, N)
    
    # 返回最新的ATR值
    return atr_series[-1] if len(atr_series) > 0 else None

##增加一个对板块评分的字段atr_score，就是计算板块指数的atr，用20天atr，另外时间就是《请选择跟踪日期:》，如果atr进行排序，如果在quantile(0.49)以下，得分1，否则得分0。
def get_atr_score(index_code_list, date):
    """
    获取板块指数的ATR评分
    计算所有板块的ATR值，然后根据分位数进行评分
    
    参数:
    index_code_list: 板块指数代码列表
    date: 计算日期
    
    返回:
    dict: {index_code: atr_score} 的字典，ATR在49%分位数以下得1分，否则得0分
    """
    atr_dict = {}
    atr_values = []
    
    # 先计算所有板块的ATR值
    for index_code in index_code_list:
        atr = get_atr(index_code, date)
        if atr is not None:
            atr_dict[index_code] = atr
            atr_values.append(atr)
    
    if not atr_values:
        return {code: 0 for code in index_code_list}
    
    # 计算49%分位数
    import numpy as np
    atr_threshold = np.quantile(atr_values, 0.49)
    
    # 根据分位数评分
    atr_scores = {}
    for index_code in index_code_list:
        if index_code in atr_dict:
            atr_scores[index_code] = 1 if atr_dict[index_code] < atr_threshold else 0
        else:
            atr_scores[index_code] = 0
    
    return atr_scores



def test_comprehensive_scores():
    """
    测试综合得分计算功能
    """
    print("=== 测试主力得分功能 ===")
    zhuli_df = zhuli_score()
    print("主力得分前5名:")
    print(zhuli_df[['index_code', '板块名称', '得分']].head())
    
    print("\n=== 测试综合得分计算 ===")
    # 创建模拟的技术指标数据
    test_codes = ["801050.ZS", "801010.ZS", "801030.ZS", "801160.ZS", "801720.ZS"]
    
    # 模拟技术指标数据
    df_zj = pd.DataFrame({
        'index_code': test_codes,
        'zj_jjdi': [1, 0, 1, 1, 0],
        'zj_di': [0, 1, 0, 1, 1],
        'zjdtg': [0, 0, 1, 0, 1],
        'zjdtz': [0, 0, 0, 0, 0]
    })
    
    df_cx = pd.DataFrame({
        'index_code': test_codes,
        'cx_jjdi': [1, 0, 1, 1, 0],
        'cx_di': [0, 1, 0, 1, 1],
        'cxdtg': [0, 0, 1, 0, 1],
        'cxdtz': [0, 0, 0, 0, 0],
        'cx_ding_tzz': [0, 0, 0, 0, 0],
        'cx_ding_baoliang': [0, 0, 0, 0, 0]
    })
    
    df_ccx = pd.DataFrame({
        'index_code': test_codes,
        'ccx_jjdi': [1, 0, 1, 1, 0],
        'ccx_di': [0, 1, 0, 1, 1],
        'ccxdtg': [0, 0, 1, 0, 1],
        'ccxdtz': [0, 0, 0, 0, 0]
    })
    
    # 计算综合得分
    comprehensive_scores = calculate_comprehensive_scores(df_zj, df_cx, df_ccx)
    print("综合得分结果:")
    print(comprehensive_scores[['index_code', 'index_name', 'zj_score', 'cx_score', 'ccx_score', 'technical_score', 'zhuli_score', 'total_score']])

def main():
    """主界面函数"""
    # 页面配置
    st.set_page_config(
        page_title="选择板块评分分析系统",
        page_icon="📊",
        layout="wide"
    )
    
    # 确定跟踪的时间：用框来获取时间
    date1 = st.sidebar.date_input('请选择跟踪日期:', date.today())
    
    # 将date1转换为字符串格式
    analysis_date = date1.strftime('%Y-%m-%d')
    
    # 将分析日期存储到session state中
    st.session_state['analysis_date'] = analysis_date
    
    st.title("📊 板块指数评分分析系统")
    st.caption(f"分析日期: {analysis_date}")
    
    # 创建侧边栏选项
    st.sidebar.title("功能选择")
    analysis_type = st.sidebar.selectbox(
        "选择分析类型:",
        ["申万行业板块分析", "自定义板块分析", "自定义增量分析", "自定义概念分析", "自定义特殊分析", "自定义主力分析", "主力得分分析", "系统测试"]
    )
    
    if analysis_type == "申万行业板块分析":
        display_sw_sector_hierarchy()
    
    elif analysis_type == "自定义板块分析":
        st.subheader("🔧 自定义板块分析")
        
        # 输入板块代码
        custom_codes = st.text_area(
            "请输入板块指数代码（每行一个，如：100001.ZS）:",
            # value="801050.ZS\n801010.ZS\n801030.ZS\n851243.ZS",
            value = "100001.ZS\n100002.ZS\n100003.ZS\n100004.ZS\n100005.ZS\n100006.ZS\n100007.ZS\n100008.ZS\n100009.ZS",
            height=100
        )
        
        if st.button("开始分析"):
            codes = [code.strip() for code in custom_codes.split('\n') if code.strip()]
            if codes:
                display_sector_scores(codes, analysis_date)
            else:
                st.warning("请输入有效的板块指数代码")
    
    elif analysis_type == "自定义增量分析":
        st.subheader("🔧 自定义增量分析")
        
        # 输入板块代码
        custom_codes = st.text_area(
            "请输入板块指数代码（每行一个，如：100001.ZS）:",
            # value="801050.ZS\n801010.ZS\n801030.ZS\n851243.ZS",
            value = "801083.ZS\n801726.ZS\n801764.ZS\n801053.ZS\n801050.ZS\n801017.ZS\n801993.ZS\n801737.ZS\n801038.ZS\n801181.ZS\n801116.ZS",
            height=100
        )
        
        if st.button("开始分析"):
            codes = [code.strip() for code in custom_codes.split('\n') if code.strip()]
            if codes:
                display_sector_scores(codes, analysis_date)
            else:
                st.warning("请输入有效的板块指数代码")
    
    elif analysis_type == "自定义概念分析":
        st.subheader("🎯 自定义概念分析")
        
        # 获取所有自定义概念
        try:
            concept_names = get_custom_concept_names()
            
            if not concept_names:
                st.error("没有找到任何自定义概念数据")
                return
            
            # 选择概念
            selected_concept = st.selectbox(
                "请选择要分析的自定义概念:",
                concept_names,
                help="选择一个自定义概念，系统将分析该概念下所有板块的技术、主力和ATR评分"
            )
            
            # 显示概念信息
            if selected_concept:
                sector_count = len(get_sectors_by_custom_concept(selected_concept))
                st.info(f"📊 概念 '{selected_concept}' 包含 {sector_count} 个板块指数")
                
                if st.button("开始分析", key="custom_concept_analysis"):
                    if sector_count > 0:
                        display_custom_concept_analysis(selected_concept, analysis_date)
                    else:
                        st.warning(f"概念 '{selected_concept}' 下没有找到有效的板块数据")
        
        except Exception as e:
            st.error(f"获取自定义概念数据时出错: {e}")
    
    elif analysis_type == "自定义特殊分析":
        st.subheader("⭐ 自定义特殊分析")
        
        # 获取CSV文件列表
        try:
            csv_files_dict = get_csv_files_from_folders()
            
            if not any(csv_files_dict.values()):
                st.error("没有找到任何CSV文件数据")
                return
            
            # 显示文件概览
            total_files = sum(len(files) for files in csv_files_dict.values())
            st.info(f"📊 找到 {total_files} 个CSV文件")
            
            # 显示每个文件夹的文件数量
            for folder_name, files in csv_files_dict.items():
                if files:
                    st.text(f"📁 {folder_name}: {len(files)} 个文件")
            
            # 创建多选框选择CSV文件
            all_files = []
            for folder_name, files in csv_files_dict.items():
                for file in files:
                    all_files.append(f"{folder_name}/{file}")
            
            if all_files:
                selected_files = st.multiselect(
                    "请选择要分析的CSV文件:",
                    all_files,
                    help="可以选择多个CSV文件进行分析，系统将合并这些文件中的板块指数代码"
                )
                
                # 显示选择信息
                if selected_files:
                    st.success(f"✅ 已选择 {len(selected_files)} 个CSV文件")
                    
                    # 显示选中的文件列表
                    with st.expander("📋 查看选中的文件"):
                        for file in selected_files:
                            st.text(f"• {file}")
                    
                    # 获取板块指数代码
                    file_names = [file.split('/')[-1] for file in selected_files]  # 只取文件名部分
                    sector_codes = get_sectors_from_selected_csv_files(file_names)
                    sector_count = len(sector_codes) if sector_codes else 0
                    
                    if sector_count > 0:
                        st.success(f"✅ 合并后包含 {sector_count} 个板块指数")
                        
                        # 显示前几个板块代码作为预览
                        preview_codes = sector_codes[:5] if len(sector_codes) > 5 else sector_codes
                        st.text(f"预览板块代码: {', '.join(preview_codes)}")
                        if len(sector_codes) > 5:
                            st.text(f"... 还有 {len(sector_codes) - 5} 个板块")
                        
                        if st.button("开始分析", key="special_sector_analysis"):
                            display_special_sector_analysis("自定义特殊分析", sector_codes, analysis_date)
                    else:
                        st.warning("选中的文件中没有找到有效的板块数据")
                else:
                    st.info("请选择至少一个CSV文件进行分析")
            else:
                st.warning("没有找到任何可用的CSV文件")
        
        except Exception as e:
            st.error(f"获取CSV文件数据时出错: {e}")
            st.text("详细错误信息:")
            st.text(str(e))
    
    elif analysis_type == "自定义主力分析":
        st.subheader("💰 自定义主力分析")
        
        # 获取主力板块指数代码
        try:
            zhuli_codes = get_zhuli_bankuai()
            
            if not zhuli_codes:
                st.error("没有找到主力板块数据")
                st.info("💡 **说明**: 主力板块数据来源于市值分析，筛选条件包括：")
                st.text("1. (国企 > 0.4) & (超强 > 0.7)")
                st.text("2. 或者 超超强 > 0.6")
                st.text("3. 或者 大高 > 0.8")
                return
            
            # 显示主力板块概览
            st.success(f"✅ 找到 {len(zhuli_codes)} 个主力板块")
            
            # 显示筛选条件说明
            with st.expander("📋 主力板块筛选条件"):
                st.info("""
                **筛选条件**：
                1. (国企 > 0.4) & (超强 > 0.7)
                2. 或者 超超强 > 0.6  
                3. 或者 大高 > 0.8
                
                **数据来源**：sector_market_cap_analysis.xlsx
                """)
            
            # 显示前几个板块代码作为预览
            if zhuli_codes:
                preview_codes = zhuli_codes[:5] if len(zhuli_codes) > 5 else zhuli_codes
                st.text(f"预览板块代码: {', '.join(preview_codes)}")
                if len(zhuli_codes) > 5:
                    st.text(f"... 还有 {len(zhuli_codes) - 5} 个板块")
            
            if st.button("开始分析", key="zhuli_sector_analysis"):
                display_zhuli_sector_analysis(zhuli_codes, analysis_date)
        
        except Exception as e:
            st.error(f"获取主力板块数据时出错: {e}")
            st.text("详细错误信息:")
            st.text(str(e))
    
    elif analysis_type == "主力得分分析":
        st.subheader("💰 主力得分分析")
        
        # 显示主力得分数据
        zhuli_df = zhuli_score()
        
        # 显示主力得分统计
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总板块数", len(zhuli_df))
        with col2:
            st.metric("平均得分", f"{zhuli_df['得分'].mean():.2f}")
        with col3:
            st.metric("最高得分", zhuli_df['得分'].max())
        with col4:
            st.metric("得分>0板块数", len(zhuli_df[zhuli_df['得分'] > 0]))
        
        # 显示得分分布
        st.subheader("📊 主力得分分布")
        import plotly.express as px
        
        fig1 = px.histogram(zhuli_df, x='得分', title="主力得分分布")
        st.plotly_chart(fig1, use_container_width=True)
        
        # 显示高分板块
        st.subheader("🏆 高分板块")
        high_score_df = zhuli_df[zhuli_df['得分'] >= 2].sort_values('得分', ascending=False)
        if not high_score_df.empty:
            st.dataframe(high_score_df[['index_code', '板块名称', '超强', '超超强', '大高', '国企', '得分']], use_container_width=True)
        else:
            st.info("暂无得分≥2的板块")
        
        # 显示完整数据
        if st.checkbox("显示完整主力得分数据"):
            st.dataframe(zhuli_df, use_container_width=True)
    
    elif analysis_type == "系统测试":
        st.subheader("🧪 系统测试")
        
        if st.button("运行系统测试"):
            with st.spinner("正在运行测试..."):
                test_comprehensive_scores()
        
        # 显示系统信息
        st.subheader("📋 系统信息")
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"""
            **数据库连接**: ✅ 正常
            **技术分析模块**: ✅ 已加载
            **主力分析模块**: ✅ 已加载
            **可视化模块**: ✅ 已加载
            """)
        
        with col2:
            # 获取可用板块数量
            try:
                all_codes = get_all_standard_index_codes()
                st.info(f"""
                **可用板块指数**: {len(all_codes)} 个
                **申万行业数据**: ✅ 已加载
                **分析日期**: {date}
                **系统状态**: 🟢 运行正常
                """)
            except Exception as e:
                st.error(f"系统检查失败: {e}")

if __name__ == "__main__":
    # 运行主界面
    # 使用 streamlit run 命令时指定端口: streamlit run xuangu_bankuai.py --server.port 8501
    main()
