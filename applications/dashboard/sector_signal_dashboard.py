"""
板块信号分析仪表板
基于 sector_signal_analyzer.py 和 sel_1bzl.py 的 Streamlit 应用
显示最近10天的板块比例分析和1波增量个股挑选
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sys
import os

# 导入v2项目的模块
from data_management.sector_signal_analyzer import SectorSignalAnalyzer
# 暂时注释掉，需要确认正确的导入路径
# from applications.sector_analysis import SectorMomentumAnalyzer
from core.utils.stock_filter import get_bankuai_stocks, StockXihua
from data_management.data_processor import get_last_trade_date

# 页面配置
st.set_page_config(
    page_title="板块信号分析仪表板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义CSS样式
st.markdown("""
<style>
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .signal-positive {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0.25rem;
    }
    .signal-negative {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0.25rem;
    }
    .signal-neutral {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0.25rem;
    }
    .stSelectbox > div > div > select {
        background-color: white;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # 缓存5分钟
def get_sw_hierarchy_data():
    """获取申万行业层次结构数据"""
    try:
        from data_management.database_manager import DatabaseManager
        db_manager = DatabaseManager()
        query = "SELECT DISTINCT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name FROM sw_cfg_hierarchy WHERE l1_name IS NOT NULL AND l1_name != '' ORDER BY l1_code, l2_code, l3_code"
        df = db_manager.execute_query(query)
        return df
    except Exception as e:
        st.error(f"获取申万行业数据失败: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_stocks_by_sw_sector(l1_name=None, l2_name=None, l3_name=None):
    """根据申万板块获取股票列表"""
    try:
        from data_management.database_manager import DatabaseManager
        db_manager = DatabaseManager()
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
        
        stock_codes = df['stock_code'].tolist()
        
        # 使用StockXihua进行基本条件筛选
        if stock_codes:
            stock_filter = StockXihua()
            filtered_stocks = stock_filter.filter_basic_conditions(stock_codes)
            return filtered_stocks
        return []
        
    except Exception as e:
        st.error(f"获取申万板块股票失败: {e}")
        return []

@st.cache_data(ttl=300)
def get_stocks_by_custom_index(index_code):
    """根据自定义板块指数代码获取股票列表"""
    try:
        import sqlite3
        # 添加.SI后缀
        full_index_code = f"{index_code}.SI"
        
        conn = sqlite3.connect('databases/quant_system.db')
        
        # 查询板块成分股 - 使用sw_cfg_hierarchy表
        query = """
        SELECT DISTINCT stock_code 
        FROM sw_cfg_hierarchy 
        WHERE (l1_code = ? OR l2_code = ? OR l3_code = ?)
        AND stock_code IS NOT NULL
        """
        
        df = pd.read_sql_query(query, conn, params=[full_index_code, full_index_code, full_index_code])
        conn.close()
        
        stock_codes = df['stock_code'].tolist()
        
        if stock_codes:
            # 使用StockXihua进行基本条件筛选
            stock_filter = StockXihua()
            filtered_stocks = stock_filter.filter_basic_conditions(stock_codes)
            return filtered_stocks
        else:
            st.warning(f"未找到板块指数 {full_index_code} 的成分股数据")
            return []
        
    except Exception as e:
        st.error(f"获取自定义板块股票失败: {e}")
        return []

@st.cache_data(ttl=300)
def get_custom_index_name(index_code):
    """根据板块指数代码获取板块名称"""
    try:
        import sqlite3
        # 添加.SI后缀
        full_index_code = f"{index_code}.SI"
        
        conn = sqlite3.connect('databases/quant_system.db')
        
        # 查询板块名称 - 使用sw_cfg_hierarchy表
        query = """
        SELECT DISTINCT l1_name, l2_name, l3_name
        FROM sw_cfg_hierarchy 
        WHERE (l1_code = ? OR l2_code = ? OR l3_code = ?)
        AND (l1_name IS NOT NULL OR l2_name IS NOT NULL OR l3_name IS NOT NULL)
        LIMIT 1
        """
        
        df = pd.read_sql_query(query, conn, params=[full_index_code, full_index_code, full_index_code])
        conn.close()
        
        if not df.empty:
            # 优先使用l1_name，然后是l2_name，最后是l3_name
            if not pd.isna(df.iloc[0]['l1_name']) and df.iloc[0]['l1_name']:
                return df.iloc[0]['l1_name']
            elif not pd.isna(df.iloc[0]['l2_name']) and df.iloc[0]['l2_name']:
                return df.iloc[0]['l2_name']
            elif not pd.isna(df.iloc[0]['l3_name']) and df.iloc[0]['l3_name']:
                return df.iloc[0]['l3_name']
        
        # 如果没找到名称，返回默认名称
        return f"板块{index_code}"
        
    except Exception as e:
        st.error(f"获取板块名称失败: {e}")
        return f"板块{index_code}"

@st.cache_data(ttl=300)
def get_stocks_from_csv(csv_file_path):
    """从CSV文件读取成分股列表"""
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        
        # 检查必要的列
        if 'stock_code' not in df.columns:
            st.error("CSV文件必须包含'stock_code'列")
            return []
        
        # 获取股票代码列表
        stock_codes = df['stock_code'].astype(str).tolist()
        
        # 确保股票代码格式正确（6位数字）
        valid_stocks = []
        for code in stock_codes:
            # 移除可能的空格和特殊字符
            clean_code = str(code).strip().zfill(6)
            if clean_code.isdigit() and len(clean_code) == 6:
                valid_stocks.append(clean_code)
        
        if valid_stocks:
            # 使用StockXihua进行基本条件筛选
            stock_filter = StockXihua()
            filtered_stocks = stock_filter.filter_basic_conditions(valid_stocks)
            return filtered_stocks
        else:
            st.warning("CSV文件中没有找到有效的股票代码")
            return []
        
    except Exception as e:
        st.error(f"读取CSV文件失败: {e}")
        return []

@st.cache_data(ttl=300)
def get_csv_files_list():
    """获取操作板块目录下的CSV文件列表"""
    try:
        import os
        import glob
        
        csv_files = []
        # 使用相对路径，确保跨平台兼容性
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(current_dir, '..', '..', '..', 'quant_v2', '操作板块')
        base_path = os.path.abspath(base_path)
        
        # 搜索所有CSV文件
        pattern = os.path.join(base_path, "**", "*.csv")
        all_csv_files = glob.glob(pattern, recursive=True)
        
        # 按修改时间排序
        all_csv_files.sort(key=os.path.getmtime, reverse=True)
        
        for file_path in all_csv_files:
            # 获取相对路径作为显示名称
            rel_path = os.path.relpath(file_path, base_path)
            csv_files.append({
                'path': file_path,
                'name': rel_path,
                'size': os.path.getsize(file_path)
            })
        
        return csv_files
        
    except Exception as e:
        st.error(f"获取CSV文件列表失败: {e}")
        return []




@st.cache_data(ttl=300)  # 缓存5分钟
def load_sector_stocks(sector_name):
    """加载板块股票列表（保持向后兼容性）"""
    try:
        raw_stocks = get_bankuai_stocks(sector_name)
        stock_filter = StockXihua()
        filtered_stocks = stock_filter.filter_basic_conditions(raw_stocks)
        return filtered_stocks
    except Exception as e:
        st.error(f"加载板块股票失败: {e}")
        return []

@st.cache_data(ttl=300)
def get_1bzl_stocks(stock_list, end_date=None):
    """获取1波增量个股列表"""
    if not stock_list:
        return []
    
    try:
        analyzer = SectorMomentumAnalyzer(stock_list, end_date)
        analyzer.run_analysis()
        results = analyzer.get_results()
        
        if (results and results.get('status') == 'Success' and 
            'final_selection' in results and 
            results['final_selection'] is not None):
            
            final_selection = results['final_selection']
            if (hasattr(final_selection, 'empty') and not final_selection.empty and
                hasattr(final_selection, 'columns') and '股票代码' in final_selection.columns and
                hasattr(final_selection, '__getitem__')):
                stock_codes = final_selection['股票代码']
                if hasattr(stock_codes, 'tolist'):
                    bzl_stocks = stock_codes.tolist()
                else:
                    bzl_stocks = list(stock_codes)
                st.info(f"✅ 成功筛选出 {len(bzl_stocks)} 只1波增量个股用于板块比例计算")
                return bzl_stocks
        
        st.warning("⚠️ 未筛选出1波增量个股，将使用全部板块股票计算比例")
        return stock_list
        
    except Exception as e:
        st.error(f"1波增量股筛选失败: {e}，将使用全部板块股票")
        return stock_list

@st.cache_data(ttl=300)
def get_sector_analysis(stock_list, end_date=None):
    """获取板块信号分析（基于1波增量个股）"""
    if not stock_list:
        return None
    
    try:
        # 首先获取1波增量个股
        bzl_stocks = get_1bzl_stocks(stock_list, end_date)
        
        if not bzl_stocks:
            st.error("无法获取1波增量个股，无法进行板块分析")
            return None
        
        # 使用1波增量个股进行板块信号分析
        data_source = 'realtime' if end_date is None else 'backtest'
        analyzer = SectorSignalAnalyzer(bzl_stocks, data_source, end_date)
        
        # 获取所有比例计算结果
        all_proportions = analyzer.get_all_proportions()
        
        # 获取信号状态
        signals = {
            'xuangu_signal': analyzer.get_xuangu_date(threshold=0.32),
            'cx_signal': analyzer.get_cx_date(threshold=0.1),
            'jjdb_signal': analyzer.get_bankuai_jjdb(),
            'db_signal': analyzer.get_bankuai_db(),
            'jjding_signal': analyzer.get_bankuai_jjding(),
            'ding_signal': analyzer.get_bankuai_ding()
        }
        
        return {
            'proportions': all_proportions,
            'signals': signals,
            'analyzer': analyzer,
            'bzl_stocks': bzl_stocks,
            'total_sector_stocks': len(stock_list)
        }
    except Exception as e:
        st.error(f"板块分析失败: {e}")
        return None

@st.cache_data(ttl=300)
def get_1bzl_selection(stock_list, end_date=None):
    """获取1波增量个股筛选结果"""
    if not stock_list:
        return None
    
    try:
        analyzer = SectorMomentumAnalyzer(stock_list, end_date)
        analyzer.run_analysis()
        results = analyzer.get_results()
        return results
    except Exception as e:
        st.error(f"1波增量股筛选失败: {e}")
        return None

def create_proportion_chart(proportions_data, title="板块比例趋势"):
    """创建比例趋势图表"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('MACD<0比例', 'MA7<MA26比例', '持续跟踪比例', 'MA7>MA26且MACD>0比例'),
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    # 获取最近10天数据
    for i, (name, data) in enumerate(proportions_data.items()):
        if data is not None and not data.empty:
            recent_data = data.tail(10)
            subplot_row = i // 2 + 1
            subplot_col = i % 2 + 1
            
            fig.add_trace(
                go.Scatter(
                    x=recent_data.index,
                    y=recent_data.values,
                    mode='lines+markers',
                    name=name,
                    line=dict(color=colors[i], width=2),
                    marker=dict(size=6),
                    hovertemplate='%{x}<br>%{y:.2%}<extra></extra>'
                ),
                row=subplot_row, col=subplot_col
            )
            
            # 添加阈值线 (忽略类型检查)
            if name == 'genzj':  # MACD<0比例
                try:
                    fig.add_hline(y=0.32, line_dash="dash", line_color="red", 
                                 annotation_text="选股阈值(32%)", row=subplot_row, col=subplot_col)  # type: ignore
                    fig.add_hline(y=0.7, line_dash="dash", line_color="orange", 
                                 annotation_text="底部信号(70%)", row=subplot_row, col=subplot_col)  # type: ignore
                except Exception:
                    pass  # 忽略阈值线添加错误
            elif name == 'genzjding':  # 持续跟踪比例
                try:
                    fig.add_hline(y=0.7, line_dash="dash", line_color="purple", 
                                 annotation_text="顶部信号(70%)", row=subplot_row, col=subplot_col)  # type: ignore
                except Exception:
                    pass  # 忽略阈值线添加错误
    
    fig.update_layout(
        title=title,
        height=600,
        showlegend=False,
        template='plotly_white'
    )
    
    fig.update_yaxes(tickformat='.1%')
    
    return fig

def display_signal_status(signals):
    """显示信号状态"""
    st.subheader("📡 信号状态面板")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 买入信号")
        
        # 选股信号
        if signals['xuangu_signal']:
            st.markdown(f"""
            <div class="signal-positive">
                <strong>✅ 选股信号</strong><br>
                触发日期: {signals['xuangu_signal']}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-negative">
                <strong>❌ 选股信号</strong><br>
                未触发
            </div>
            """, unsafe_allow_html=True)
        
        # 长线信号
        if signals['cx_signal']:
            st.markdown(f"""
            <div class="signal-positive">
                <strong>✅ 长线信号</strong><br>
                触发日期: {signals['cx_signal']}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-negative">
                <strong>❌ 长线信号</strong><br>
                未触发
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 底部信号")
        
        # 接近底部
        if signals['jjdb_signal']:
            st.markdown("""
            <div class="signal-positive">
                <strong>✅ 接近底部</strong><br>
                板块处于接近底部区域
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-neutral">
                <strong>⚠️ 接近底部</strong><br>
                板块未接近底部
            </div>
            """, unsafe_allow_html=True)
        
        # 明确底部
        if signals['db_signal']:
            st.markdown("""
            <div class="signal-positive">
                <strong>🚨 明确底部</strong><br>
                板块明确底部，积极操作
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-neutral">
                <strong>⚠️ 明确底部</strong><br>
                板块未明确底部
            </div>
            """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("### 顶部信号")
        
        # 接近顶部
        if signals['jjding_signal']:
            st.markdown("""
            <div class="signal-negative">
                <strong>⚠️ 接近顶部</strong><br>
                准备卖出操作
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-positive">
                <strong>✅ 接近顶部</strong><br>
                板块未接近顶部
            </div>
            """, unsafe_allow_html=True)
        
        # 明确顶部
        if signals['ding_signal']:
            st.markdown("""
            <div class="signal-negative">
                <strong>🚨 明确顶部</strong><br>
                需要卖出操作
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="signal-positive">
                <strong>✅ 明确顶部</strong><br>
                板块未明确顶部
            </div>
            """, unsafe_allow_html=True)

def display_1bzl_results(results):
    """显示1波增量股结果"""
    st.subheader("🎯 1波增量个股筛选")
    
    if results and results.get('status') == 'Success':
        final_selection = results.get('final_selection')
        
        if final_selection is not None and not final_selection.empty:
            st.success("✅ 成功筛选出1波增量个股")
            
            # 显示关键信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("事件触发日(T0)", results.get('t0_date', pd.NaT).strftime('%Y-%m-%d'))
            with col2:
                st.metric("T0日MACD>0比例", f"{results.get('t0_ratio', 0):.2%}")
            with col3:
                st.metric("筛选股票数", len(final_selection))
            
            # 显示股票列表
            st.markdown("### 筛选结果")
            
            # 格式化显示
            display_df = final_selection.copy()
            if '10日涨幅' in display_df.columns:
                display_df['10日涨幅'] = pd.to_numeric(display_df['10日涨幅'], errors='coerce')
                display_df['10日涨幅'] = display_df['10日涨幅'].map('{:.2%}'.format)
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
            
            # 显示时间窗口信息
            if 'window_start_date' in results and 'window_end_date' in results:
                st.info(f"观测窗口: {results['window_start_date'].strftime('%Y-%m-%d')} 至 {results['window_end_date'].strftime('%Y-%m-%d')}")
            
            if 'most_active_day' in results:
                st.info(f"爆发最集中日期: {results['most_active_day'].strftime('%Y-%m-%d')}")
        else:
            st.warning("未筛选出1波增量个股")
    else:
        st.error("1波增量股筛选失败或无有效结果")

def main():
    st.title("📊 板块信号分析仪表板")
    st.markdown("基于1波增量个股的板块信号分析")
    
    # 侧边栏配置
    with st.sidebar:
        st.header("⚙️ 配置参数")
        
        # 板块选择模式
        sector_mode = st.radio(
            "板块选择模式",
            ["申万行业分类", "自定义板块", "自定义成分股"],
            index=0
        )
        
        if sector_mode == "申万行业分类":
            # 板块选择 - 使用申万行业分类
            st.subheader("📊 申万行业选择")
            
            # 获取申万层次数据
            sw_data = get_sw_hierarchy_data()
            
            if not sw_data.empty:
                # 一级行业选择
                l1_series = sw_data['l1_name'].dropna().unique()
                l1_names = sorted(l1_series.tolist()) if hasattr(l1_series, 'tolist') else sorted(list(l1_series))
                selected_l1 = st.selectbox(
                    "一级行业",
                    ['请选择'] + l1_names,
                    index=0
                )
                
                # 二级行业选择
                if selected_l1 != '请选择':
                    l2_data = sw_data[sw_data['l1_name'] == selected_l1]
                    l2_series = l2_data['l2_name'].dropna().unique()
                    l2_names = sorted(l2_series.tolist()) if hasattr(l2_series, 'tolist') else sorted(list(l2_series))
                    
                    if l2_names:
                        selected_l2 = st.selectbox(
                            "二级行业",
                            ['全部二级行业'] + l2_names,
                            index=0
                        )
                    else:
                        selected_l2 = '全部二级行业'
                        st.info("该一级行业下无二级分类")
                else:
                    selected_l2 = None
                
                # 显示选择结果
                if selected_l1 != '请选择':
                    if selected_l2 and selected_l2 != '全部二级行业':
                        st.success(f"已选择: {selected_l1} → {selected_l2}")
                        sector_selection = {'l1_name': selected_l1, 'l2_name': selected_l2}
                    else:
                        st.success(f"已选择: {selected_l1} (全部二级行业)")
                        sector_selection = {'l1_name': selected_l1}
                else:
                    sector_selection = None
                    st.warning("请选择板块")
            else:
                st.error("无法加载申万行业数据")
                sector_selection = None
                
        elif sector_mode == "自定义板块":
            # 自定义板块选择
            st.subheader("🎯 自定义板块")
            
            # 从v2项目的sector_screener导入jinqi_1bzl_bankuai
            from applications.sector_screener import jinqi_1bzl_bankuai
            
            # 处理板块代码，去掉.ZS后缀
            bankuai_options = []
            for code in jinqi_1bzl_bankuai:
                if code.endswith('.ZS'):
                    clean_code = code[:-3]  # 去掉.ZS后缀
                    bankuai_options.append(clean_code)
                else:
                    bankuai_options.append(code)
            
            # 创建下拉框选择
            custom_index_code = st.selectbox(
                "选择板块指数代码",
                options=bankuai_options,
                help="从近期1波明显增量板块中选择"
            )
            
            if custom_index_code:
                # 获取板块名称
                index_name = get_custom_index_name(custom_index_code)
                st.success(f"已选择板块代码: {custom_index_code}")
                st.info(f"板块名称: {index_name}")
                sector_selection = {'custom_index': custom_index_code}
            else:
                sector_selection = None
                st.warning("请选择板块指数代码")
        
        elif sector_mode == "自定义成分股":
            # 自定义成分股选择
            st.subheader("📁 自定义成分股")
            
            # 获取CSV文件列表
            csv_files = get_csv_files_list()
            
            if csv_files:
                # 显示文件选择器
                file_options = [f"{file['name']} ({file['size']} bytes)" for file in csv_files]
                selected_file_idx = st.selectbox(
                    "选择CSV文件",
                    range(len(file_options)),
                    format_func=lambda x: file_options[x],
                    help="选择包含成分股的CSV文件，文件必须包含'stock_code'列"
                )
                
                if selected_file_idx is not None:
                    selected_file = csv_files[selected_file_idx]
                    st.success(f"已选择文件: {selected_file['name']}")
                    st.info(f"文件路径: {selected_file['path']}")
                    sector_selection = {'csv_file': selected_file['path']}
                else:
                    sector_selection = None
                    st.warning("请选择CSV文件")
            else:
                st.error("未找到CSV文件，请检查 C:\\quant_v2\\操作板块 目录")
                sector_selection = None
        
        # 分析模式
        analysis_mode = st.radio(
            "分析模式",
            ["实时数据", "历史回测"],
            index=0
        )
        
        end_date = None
        if analysis_mode == "历史回测":
            end_date = st.date_input(
                "选择截止日期",
                value=datetime.now().date() - timedelta(days=1),
                max_value=datetime.now().date()
            ).strftime('%Y-%m-%d')
        
        # 刷新按钮
        if st.button("🔄 刷新数据", type="primary"):
            st.cache_data.clear()
            st.rerun()
    
    # 主要内容区域
    try:
        if not sector_selection:
            st.warning("请在左侧选择板块后开始分析")
            return
            
        # 加载股票列表
        if 'custom_index' in sector_selection:
            # 自定义板块
            with st.spinner("正在加载自定义板块股票..."):
                stock_list = get_stocks_by_custom_index(sector_selection['custom_index'])
                # 获取板块名称
                index_name = get_custom_index_name(sector_selection['custom_index'])
                sector_display_name = f"{index_name} ({sector_selection['custom_index']}.SI)"
        elif 'csv_file' in sector_selection:
            # 自定义成分股
            with st.spinner("正在加载CSV文件成分股..."):
                stock_list = get_stocks_from_csv(sector_selection['csv_file'])
                # 从文件路径获取显示名称
                import os
                file_name = os.path.basename(sector_selection['csv_file'])
                sector_display_name = f"自定义成分股 ({file_name})"
        else:
            # 申万板块
            with st.spinner("正在加载申万板块股票..."):
                if 'l2_name' in sector_selection:
                    stock_list = get_stocks_by_sw_sector(
                        l1_name=sector_selection['l1_name'],
                        l2_name=sector_selection['l2_name']
                    )
                    sector_display_name = f"{sector_selection['l1_name']} → {sector_selection['l2_name']}"
                else:
                    stock_list = get_stocks_by_sw_sector(
                        l1_name=sector_selection['l1_name']
                    )
                    sector_display_name = sector_selection['l1_name']
        
        if not stock_list:
            st.error("未能加载板块股票，请检查板块选择或数据库连接")
            return
        
        st.success(f"成功加载 {len(stock_list)} 只 {sector_display_name} 板块股票")
        
        # 创建标签页
        tab1, tab2, tab3 = st.tabs(["📈 板块信号分析", "🎯 1波增量筛选", "📊 详细数据"])
        
        with tab1:
            st.header("板块信号分析（基于1波增量个股）")
            
            with st.spinner("正在筛选1波增量个股并分析板块信号..."):
                analysis_result = get_sector_analysis(stock_list, end_date)
            
            if analysis_result:
                # 显示基础信息
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("板块总股票数", analysis_result['total_sector_stocks'])
                with col2:
                    st.metric("1波增量个股数", len(analysis_result['bzl_stocks']))
                with col3:
                    ratio = len(analysis_result['bzl_stocks']) / analysis_result['total_sector_stocks']
                    st.metric("增量股占比", f"{ratio:.1%}")
                
                st.info(f"📌 所有板块比例计算均基于 {len(analysis_result['bzl_stocks'])} 只1波增量个股")
                
                # 显示信号状态
                display_signal_status(analysis_result['signals'])
                
                st.divider()
                
                # 显示比例趋势图
                st.subheader("📈 最近10天板块比例趋势（基于1波增量个股）")
                
                proportions = analysis_result['proportions']
                if any(data is not None and not data.empty for data in proportions.values()):
                    chart = create_proportion_chart(proportions, title="1波增量个股板块比例趋势")
                    st.plotly_chart(chart, use_container_width=True)
                    
                    # 显示最新数值
                    st.subheader("📊 最新比例数值（基于1波增量个股）")
                    cols = st.columns(4)
                    
                    labels = ['MACD<0比例', 'MA7<MA26比例', '持续跟踪比例', 'MA7>MA26且MACD>0比例']
                    keys = ['genzj', 'genzj7x26', 'genzjding', 'genzj7d26']
                    
                    for i, (key, label) in enumerate(zip(keys, labels)):
                        with cols[i]:
                            if proportions[key] is not None and not proportions[key].empty:
                                latest_value = proportions[key].iloc[-1]
                                latest_date = proportions[key].index[-1].strftime('%Y-%m-%d')
                                st.metric(
                                    label,
                                    f"{latest_value:.2%}",
                                    help=f"最新日期: {latest_date}\n基于{len(analysis_result['bzl_stocks'])}只1波增量个股"
                                )
                            else:
                                st.metric(label, "无数据")
                else:
                    st.warning("无法获取比例数据")
        
        with tab2:
            st.header("1波增量个股筛选详情")
            
            with st.spinner("正在筛选1波增量个股..."):
                bzl_results = get_1bzl_selection(stock_list, end_date)
            
            if bzl_results:
                display_1bzl_results(bzl_results)
            else:
                st.error("1波增量股筛选失败")
        
        with tab3:
            st.header("详细数据")
            
            if analysis_result and analysis_result['proportions']:
                st.subheader("📈 原始比例数据（基于1波增量个股）")
                
                # 指标名称映射
                indicator_names = {
                    'genzj': 'MACD<0比例',
                    'genzj7x26': 'MA7<MA26比例', 
                    'genzjding': '持续跟踪比例',
                    'genzj7d26': 'MA7>MA26且MACD>0比例'
                }
                
                for name, data in analysis_result['proportions'].items():
                    if data is not None and not data.empty:
                        display_name = indicator_names.get(name, name)
                        with st.expander(f"{name} 详细数据"):
                            # 显示最近15天比例数据
                            recent_data = data.tail(15)
                            df_display = pd.DataFrame({
                                '日期': recent_data.index.strftime('%Y-%m-%d'),
                                '比例': recent_data.values,
                                '百分比': [f"{x:.2%}" for x in recent_data.values]
                            })
                            st.dataframe(df_display, use_container_width=True, hide_index=True)
                            
                            st.divider()
                            
                            # 显示对应的个股状态表格
                            st.markdown(f"**📊 1波增量个股{display_name}状态（最近10天）**")
                            
                            with st.spinner(f"正在生成{display_name}个股状态表格..."):
                                try:
                                    analyzer = analysis_result['analyzer']
                                    
                                    # 根据不同指标获取对应的状态数据
                                    condition_panel = None
                                    
                                    if name == 'genzj' and (hasattr(analyzer, 'strategy') and analyzer.strategy and 
                                        hasattr(analyzer.strategy, 'indicator_panels') and
                                        'MACD' in analyzer.strategy.indicator_panels):
                                        # MACD<0状态
                                        macd_panel = analyzer.strategy.indicator_panels['MACD']
                                        condition_panel = macd_panel < 0
                                        
                                    elif name == 'genzj7x26' and (hasattr(analyzer, 'strategy') and analyzer.strategy and 
                                        hasattr(analyzer.strategy, 'indicator_panels') and
                                        'MA_7' in analyzer.strategy.indicator_panels and
                                        'MA_26' in analyzer.strategy.indicator_panels):
                                        # MA7<MA26状态
                                        ma7_panel = analyzer.strategy.indicator_panels['MA_7']
                                        ma26_panel = analyzer.strategy.indicator_panels['MA_26']
                                        condition_panel = ma7_panel < ma26_panel
                                        
                                    elif name == 'genzjding' and (hasattr(analyzer, 'strategy') and analyzer.strategy and 
                                        hasattr(analyzer.strategy, 'indicator_panels')):
                                        # 持续跟踪状态 - 这里需要根据具体的持续跟踪逻辑来实现
                                        # 暂时使用MACD>0作为示例
                                        if 'MACD' in analyzer.strategy.indicator_panels:
                                            macd_panel = analyzer.strategy.indicator_panels['MACD']
                                            condition_panel = macd_panel > 0
                                            
                                    elif name == 'genzj7d26' and (hasattr(analyzer, 'strategy') and analyzer.strategy and 
                                        hasattr(analyzer.strategy, 'indicator_panels') and
                                        'MA_7' in analyzer.strategy.indicator_panels and
                                        'MA_26' in analyzer.strategy.indicator_panels and
                                        'MACD' in analyzer.strategy.indicator_panels):
                                        # MA7>MA26且MACD>0状态
                                        ma7_panel = analyzer.strategy.indicator_panels['MA_7']
                                        ma26_panel = analyzer.strategy.indicator_panels['MA_26']
                                        macd_panel = analyzer.strategy.indicator_panels['MACD']
                                        condition_panel = (ma7_panel > ma26_panel) & (macd_panel > 0)
                                    
                                    if condition_panel is not None:
                                        # 获取最近10天的数据
                                        recent_data = condition_panel.tail(10)
                                        
                                        if not recent_data.empty:
                                            # 创建表格数据
                                            table_data = []
                                            
                                            # 添加个股数据行
                                            for stock in analysis_result['bzl_stocks']:
                                                if stock in recent_data.columns:
                                                    row_data = {'股票代码': stock}
                                                    for date in recent_data.index:
                                                        value = recent_data.loc[date, stock]
                                                        row_data[date.strftime('%Y-%m-%d')] = "True" if value else "False"
                                                    table_data.append(row_data)
                                            
                                            # 添加True占比行
                                            ratio_row = {'股票代码': 'True占比'}
                                            for date in recent_data.index:
                                                true_count = recent_data.loc[date].sum()
                                                total_count = len(analysis_result['bzl_stocks'])
                                                ratio = true_count / total_count if total_count > 0 else 0
                                                ratio_row[date.strftime('%Y-%m-%d')] = f"{ratio:.1f}"
                                            table_data.append(ratio_row)
                                            
                                            # 转换为DataFrame并显示
                                            df_display = pd.DataFrame(table_data)
                                            
                                            # 设置样式
                                            def highlight_ratio_row(row):
                                                if row['股票代码'] == 'True占比':
                                                    return ['background-color: #f0f2f6; font-weight: bold'] * len(row)
                                                return [''] * len(row)
                                            
                                            styled_df = df_display.style.apply(highlight_ratio_row, axis=1)
                                            st.dataframe(styled_df, use_container_width=True, hide_index=True)
                                        else:
                                            st.warning(f"无法获取{display_name}状态数据")
                                    else:
                                        st.warning(f"{display_name}指标数据未准备好")
                                        
                                except Exception as e:
                                    st.error(f"生成{display_name}状态表格失败: {e}")
    
    except Exception as e:
        st.error(f"应用运行出错: {e}")
        st.exception(e)

if __name__ == "__main__":
    main()
