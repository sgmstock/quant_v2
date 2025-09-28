# 板块趋势力度分析系统 - Streamlit版本（最终修复版）
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging

# 导入v2项目的模块
from core.utils.indicators import DMI, MACD

# 设置页面配置
st.set_page_config(
    page_title="板块趋势力度分析系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SectorTrendStrengthAnalyzer:
    """
    板块趋势力度分析器 - Streamlit版本
    
    功能：
    1. 计算板块内个股的ADX值（仅在MACD>0时计算）
    2. 根据个股市值进行加权
    3. 使用Winsorizing处理异常值
    4. 计算板块整体趋势力度
    """
    
    def __init__(self, db_path: str = 'databases/quant_system.db'):
        self.db_path = db_path
        
    def get_sw_sectors(self) -> pd.DataFrame:
        """获取申万三级板块列表"""
        try:
            from data_management.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            query = """
            SELECT DISTINCT l1_name, l2_name, l3_name, l3_code
            FROM sw_cfg_hierarchy 
            WHERE l3_code IS NOT NULL AND l3_name IS NOT NULL
            ORDER BY l1_name, l2_name, l3_name
            """
            df = db_manager.execute_query(query)
            return df
        except Exception as e:
            st.error(f"获取申万板块失败: {e}")
            return pd.DataFrame()
    
    def get_sector_stocks_by_sw(self, l1_name: str, l2_name: Optional[str] = None, l3_name: Optional[str] = None) -> List[str]:
        """根据申万板块获取成分股"""
        try:
            from data_management.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            # 动态构建查询条件
            conditions, params = [], []
            if l1_name:
                conditions.append("l1_name = ?")
                params.append(l1_name)
            if l2_name:
                conditions.append("l2_name = ?")
                params.append(l2_name)
            if l3_name:
                conditions.append("l3_name = ?")
                params.append(l3_name)
            
            if not conditions:
                return []
            
            where_clause = " AND ".join(conditions)
            query = f"SELECT DISTINCT stock_code FROM sw_cfg_hierarchy WHERE {where_clause} AND stock_code IS NOT NULL"
            
            df = db_manager.execute_query(query, tuple(params))
            return df['stock_code'].tolist()
        except Exception as e:
            st.error(f"获取申万板块成分股失败: {e}")
            return []
    
    def get_stocks_from_csv(self, uploaded_file) -> List[str]:
        """从上传的CSV文件获取股票代码"""
        try:
            df = pd.read_csv(uploaded_file)
            if 'stock_code' in df.columns:
                return df['stock_code'].astype(str).str.zfill(6).tolist()
            else:
                st.error("CSV文件中未找到'stock_code'列")
                return []
        except Exception as e:
            st.error(f"读取CSV文件失败: {e}")
            return []
    
    def get_stock_data(self, stock_code: str, days: int = 50, analysis_date: Optional[str] = None) -> pd.DataFrame:
        """获取个股历史数据"""
        try:
            from data_management.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            if analysis_date:
                # 如果指定了分析日期，获取该日期之前的数据
                query = """
                SELECT trade_date, open, high, low, close, volume
                FROM k_daily 
                WHERE stock_code = ? AND trade_date <= ?
                ORDER BY trade_date DESC 
                LIMIT ?
                """
                df = db_manager.execute_query(query, (stock_code, analysis_date, days))
            else:
                # 默认获取最新数据
                query = """
                SELECT trade_date, open, high, low, close, volume
                FROM k_daily 
                WHERE stock_code = ? 
                ORDER BY trade_date DESC 
                LIMIT ?
                """
                df = db_manager.execute_query(query, (stock_code, days))
            
            if df.empty:
                return pd.DataFrame()
                
            # 按日期升序排列
            df = df.sort_values('trade_date').reset_index(drop=True)
            return df
        except Exception as e:
            return pd.DataFrame()
    
    def get_market_cap(self, stock_code: str) -> float:
        """获取个股市值"""
        try:
            from data_management.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            query = """
            SELECT market_cap 
            FROM stock_basic_pro 
            WHERE stock_code = ?
            """
            df = db_manager.execute_query(query, (stock_code,))
            
            if df.empty:
                return 0.0
                
            market_cap = df['market_cap'].iloc[0]
            return float(market_cap) if not pd.isna(market_cap) else 0.0
        except Exception as e:
            return 0.0
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> Dict:
        """计算个股技术指标"""
        if df.empty or len(df) < 30:
            return {}
            
        try:
            close = df['close'].values
            high = df['high'].values
            low = df['low'].values
            
            # 计算MACD
            dif, dea, macd = MACD(close, SHORT=12, LONG=26, M=9)
            
            # 计算DMI/ADX
            pdi, mdi, adx, adxr = DMI(close, high, low, M1=14, M2=6)
            
            # 获取最新值
            latest_macd = macd[-1] if not np.isnan(macd[-1]) else 0
            latest_adx = adx[-1] if not np.isnan(adx[-1]) else 0
            
            return {
                'macd': latest_macd,
                'adx': latest_adx,
                'valid': not np.isnan(latest_adx) and not np.isnan(latest_macd)
            }
        except Exception as e:
            return {}
    
    def calculate_sector_trend_strength(self, stock_codes: List[str], 
                                      sector_name: str,
                                      days: int = 20,
                                      min_market_cap: float = 1e8,
                                      winsorize_percentile: float = 0.95,
                                      analysis_date: Optional[str] = None) -> Dict:
        """计算板块趋势力度"""
        
        # 2. 计算每只股票的技术指标
        stock_results = []
        valid_stocks = 0
        total_market_cap = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, stock_code in enumerate(stock_codes):
            # 更新进度
            progress = (i + 1) / len(stock_codes)
            progress_bar.progress(progress)
            status_text.text(f"正在分析股票 {stock_code} ({i+1}/{len(stock_codes)})")
            
            # 获取股票数据
            df = self.get_stock_data(stock_code, days + 30, analysis_date)
            if df.empty:
                continue
                
            # 计算技术指标
            indicators = self.calculate_technical_indicators(df)
            if not indicators.get('valid', False):
                continue
                
            # 获取市值
            market_cap = self.get_market_cap(stock_code)
            if market_cap < min_market_cap:
                continue
                
            # 只在MACD>0时计算ADX
            if indicators['macd'] > 0:
                stock_results.append({
                    'stock_code': stock_code,
                    'adx': indicators['adx'],
                    'macd': indicators['macd'],
                    'market_cap': market_cap
                })
                valid_stocks += 1
                total_market_cap += market_cap
        
        # 清除进度条
        progress_bar.empty()
        status_text.empty()
        
        if not stock_results:
            return {
                'sector_name': sector_name,
                'trend_strength': 0,
                'weighted_adx': 0,
                'avg_adx': 0,
                'valid_stocks': 0,
                'total_stocks': len(stock_codes),
                'total_market_cap': 0,
                'winsorize_upper_bound': 0,
                'stock_details': [],
                'message': '没有符合条件的股票',
                'analysis_date': analysis_date or pd.Timestamp.now().strftime('%Y-%m-%d')
            }
        
        # 3. Winsorizing处理异常值
        adx_values = [result['adx'] for result in stock_results]
        adx_series = pd.Series(adx_values)
        
        # 计算上限值
        upper_bound = adx_series.quantile(winsorize_percentile)
        
        # 应用Winsorizing
        clipped_adx_values = adx_series.clip(upper=upper_bound)
        
        # 更新stock_results中的ADX值
        for i, result in enumerate(stock_results):
            if i < len(clipped_adx_values):
                result['adx_clipped'] = clipped_adx_values.iloc[i]
            else:
                result['adx_clipped'] = result['adx']
        
        # 4. 计算市值加权ADX
        weighted_adx_sum = 0
        for result in stock_results:
            weight = result['market_cap'] / total_market_cap
            weighted_adx_sum += result['adx_clipped'] * weight
        
        # 5. 计算平均ADX（非加权）
        if len(clipped_adx_values) > 0:
            avg_adx = float(np.mean(clipped_adx_values))
        else:
            avg_adx = 0.0
        
        # 6. 计算趋势强度评分
        trend_strength = self._calculate_trend_score(weighted_adx_sum)
        
        return {
            'sector_name': sector_name,
            'trend_strength': trend_strength,
            'weighted_adx': weighted_adx_sum,
            'avg_adx': avg_adx,
            'valid_stocks': valid_stocks,
            'total_stocks': len(stock_codes),
            'total_market_cap': total_market_cap,
            'winsorize_upper_bound': upper_bound,
            'stock_details': stock_results,
            'analysis_date': analysis_date or pd.Timestamp.now().strftime('%Y-%m-%d')
        }
    
    def _calculate_trend_score(self, adx_value: float) -> float:
        """将ADX值转换为趋势强度评分（0-100）"""
        if adx_value < 20:
            return adx_value * 2.5  # 0-50分
        elif adx_value < 30:
            return 50 + (adx_value - 20) * 3  # 50-80分
        else:
            return min(80 + (adx_value - 30) * 0.5, 100)  # 80-100分

def main():
    """主函数"""
    st.title("📊 板块趋势力度分析系统")
    st.markdown("---")
    
    # 初始化分析器
    analyzer = SectorTrendStrengthAnalyzer()
    
    # 侧边栏 - 板块选择
    st.sidebar.header("🔧 分析配置")
    
    # 选择数据源
    data_source = st.sidebar.radio(
        "选择数据源",
        ["申万三级板块", "CSV文件导入"]
    )
    
    stock_codes = []
    sector_name = ""
    
    if data_source == "申万三级板块":
        # 获取申万板块列表
        sw_sectors = analyzer.get_sw_sectors()
        
        if not sw_sectors.empty:
            # 一级板块选择
            l1_options = [''] + sorted(sw_sectors['l1_name'].dropna().unique().tolist())
            l1_name = st.sidebar.selectbox("选择一级板块", l1_options)
            
            if l1_name:
                # 二级板块选择
                l2_df = sw_sectors[sw_sectors['l1_name'] == l1_name]
                l2_options = [''] + sorted(l2_df['l2_name'].dropna().unique().tolist())
                l2_name = st.sidebar.selectbox("选择二级板块", l2_options)
                
                if l2_name:
                    # 三级板块选择
                    l3_df = sw_sectors[
                        (sw_sectors['l1_name'] == l1_name) & 
                        (sw_sectors['l2_name'] == l2_name)
                    ]
                    l3_options = [''] + sorted(l3_df['l3_name'].dropna().unique().tolist())
                    l3_name = st.sidebar.selectbox("选择三级板块", l3_options)
                    
                    if l3_name:
                        # 获取成分股
                        stock_codes = analyzer.get_sector_stocks_by_sw(l1_name, l2_name, l3_name)
                        sector_name = f"{l1_name} → {l2_name} → {l3_name}"
                    else:
                        # 二级板块成分股
                        stock_codes = analyzer.get_sector_stocks_by_sw(l1_name, l2_name)
                        sector_name = f"{l1_name} → {l2_name}"
                else:
                    # 一级板块成分股
                    stock_codes = analyzer.get_sector_stocks_by_sw(l1_name)
                    sector_name = l1_name
        else:
            st.sidebar.error("无法获取申万板块数据")
    
    else:  # CSV文件导入
        uploaded_file = st.sidebar.file_uploader(
            "上传CSV文件",
            type=['csv'],
            help="CSV文件必须包含'stock_code'列"
        )
        
        if uploaded_file is not None:
            stock_codes = analyzer.get_stocks_from_csv(uploaded_file)
            sector_name = f"自定义板块 ({uploaded_file.name})"
    
    # 分析参数
    st.sidebar.header("⚙️ 分析参数")
    
    # 分析日期选择
    analysis_date = st.sidebar.date_input(
        "分析日期",
        value=datetime.now().date(),
        help="选择分析的具体日期，用于回测分析"
    )
    
    days = st.sidebar.slider("分析天数", 10, 50, 20, help="用于计算技术指标的历史天数")
    min_market_cap = st.sidebar.number_input(
        "最小市值（亿元）", 
        min_value=0.1, 
        max_value=1000.0, 
        value=1.0,
        step=0.1,
        help="过滤小盘股，单位：亿元"
    ) * 1e8  # 转换为元
    winsorize_percentile = st.sidebar.slider(
        "Winsorizing百分位数", 
        0.8, 0.99, 0.95, 0.01,
        help="用于处理ADX异常值的百分位数"
    )
    
    # 显示当前选择的板块信息
    if stock_codes:
        st.sidebar.success(f"✅ 已选择板块: {sector_name}")
        st.sidebar.info(f"📊 成分股数量: {len(stock_codes)} 只")
        
        # 显示前几只股票代码作为预览
        if len(stock_codes) > 0:
            preview_stocks = stock_codes[:5]  # 显示前5只
            st.sidebar.text(f"股票代码预览: {', '.join(preview_stocks)}")
            if len(stock_codes) > 5:
                st.sidebar.text(f"... 还有 {len(stock_codes) - 5} 只股票")
    
    # 开始分析按钮
    if st.sidebar.button("🚀 开始分析", type="primary"):
        if not stock_codes:
            st.error("请先选择板块或上传CSV文件")
        else:
            with st.spinner("正在分析板块趋势力度..."):
                # 将日期转换为字符串格式
                analysis_date_str = analysis_date.strftime('%Y-%m-%d') if analysis_date else None
                
                result = analyzer.calculate_sector_trend_strength(
                    stock_codes, sector_name, days, min_market_cap, winsorize_percentile, analysis_date_str
                )
                
                # 显示结果
                display_results(result)

def display_results(result: Dict):
    """显示分析结果"""
    st.header("📊 分析结果")
    
    # 显示分析日期信息
    if 'analysis_date' in result:
        st.info(f"📅 分析日期: {result['analysis_date']}")
    
    # 检查是否有错误信息
    if 'message' in result and result['message']:
        st.warning(f"⚠️ {result['message']}")
        if result['valid_stocks'] == 0:
            st.info("💡 建议：尝试调整分析参数，如降低最小市值要求或增加分析天数")
            return
    
    # 基本信息
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "趋势力度评分",
            f"{result.get('trend_strength', 0):.2f}",
            help="0-100分，分数越高表示趋势越强"
        )
    
    with col2:
        st.metric(
            "加权ADX",
            f"{result.get('weighted_adx', 0):.2f}",
            help="市值加权后的ADX值"
        )
    
    with col3:
        st.metric(
            "有效股票数",
            f"{result.get('valid_stocks', 0)}/{result.get('total_stocks', 0)}",
            help="符合MACD>0条件的股票数量"
        )
    
    with col4:
        st.metric(
            "Winsorizing上限",
            f"{result.get('winsorize_upper_bound', 0):.2f}",
            help="ADX异常值处理的上限"
        )
    
    # 趋势强度可视化
    st.subheader("📈 趋势强度分析")
    
    # 创建趋势强度仪表盘
    fig_gauge = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = result.get('trend_strength', 0),
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "趋势强度评分"},
        delta = {'reference': 50},
        gauge = {
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 30], 'color': "lightgray"},
                {'range': [30, 60], 'color': "yellow"},
                {'range': [60, 100], 'color': "green"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 80
            }
        }
    ))
    
    fig_gauge.update_layout(height=300)
    st.plotly_chart(fig_gauge, use_container_width=True)
    
    # 个股详细分析
    if 'stock_details' in result and result['stock_details'] and len(result['stock_details']) > 0:
        st.subheader("🔍 个股详细分析")
        
        # 创建个股数据表格
        stock_df = pd.DataFrame(result['stock_details'])
        stock_df['market_cap_yi'] = stock_df['market_cap'] / 1e8  # 转换为亿元
        stock_df['weight'] = stock_df['market_cap'] / result.get('total_market_cap', 1)
        
        # 显示表格
        display_columns = ['stock_code', 'adx', 'adx_clipped', 'macd', 'market_cap_yi', 'weight']
        column_names = ['股票代码', '原始ADX', '处理后ADX', 'MACD', '市值(亿)', '权重']
        
        stock_display_df = stock_df[display_columns].copy()
        stock_display_df.columns = column_names
        stock_display_df = stock_display_df.round(4)
        
        st.dataframe(stock_display_df, use_container_width=True)
        
        # ADX分布图
        col1, col2 = st.columns(2)
        
        with col1:
            # 原始ADX分布
            fig_hist_orig = px.histogram(
                stock_df, 
                x='adx', 
                title='原始ADX分布',
                nbins=20
            )
            fig_hist_orig.update_layout(height=400)
            st.plotly_chart(fig_hist_orig, use_container_width=True)
        
        with col2:
            # 处理后ADX分布
            fig_hist_clipped = px.histogram(
                stock_df, 
                x='adx_clipped', 
                title='处理后ADX分布',
                nbins=20
            )
            fig_hist_clipped.update_layout(height=400)
            st.plotly_chart(fig_hist_clipped, use_container_width=True)
        
        # 市值权重分析
        st.subheader("💰 市值权重分析")
        
        # 市值分布
        fig_market_cap = px.scatter(
            stock_df, 
            x='market_cap_yi', 
            y='adx_clipped',
            size='weight',
            title='市值与ADX关系',
            labels={'market_cap_yi': '市值(亿元)', 'adx_clipped': 'ADX值'}
        )
        st.plotly_chart(fig_market_cap, use_container_width=True)
        
        # 权重分布饼图
        top_10_stocks = stock_df.nlargest(10, 'weight')
        fig_pie = px.pie(
            top_10_stocks, 
            values='weight', 
            names='stock_code',
            title='前10大权重股'
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    # 分析总结
    st.subheader("📝 分析总结")
    
    if result.get('trend_strength', 0) >= 80:
        st.success("🔥 板块趋势非常强劲！建议重点关注。")
    elif result.get('trend_strength', 0) >= 60:
        st.info("📈 板块趋势较强，值得关注。")
    elif result.get('trend_strength', 0) >= 40:
        st.warning("⚠️ 板块趋势一般，需要谨慎观察。")
    else:
        st.error("📉 板块趋势较弱，建议避免。")
    
    # 技术指标解释
    with st.expander("📚 技术指标说明"):
        st.markdown("""
        **ADX (Average Directional Index)**
        - 衡量趋势强度的指标
        - ADX > 25: 强趋势
        - ADX < 20: 弱趋势/盘整
        
        **MACD (Moving Average Convergence Divergence)**
        - 趋势跟踪指标
        - MACD > 0: 上升趋势
        - MACD < 0: 下降趋势
        
        **Winsorizing处理**
        - 处理ADX异常值的方法
        - 将超过95%分位数的值替换为95%分位数
        - 避免极端值影响整体分析
        """)

if __name__ == "__main__":
    main()
