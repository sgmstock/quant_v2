#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streamlit板块市值分析应用

功能：
1. 读取sector_market_cap_analysis.xlsx文件
2. 提供选择按钮对不同字段进行排序
3. 动态展示排序结果

"""

import streamlit as st
import pandas as pd
import os

# 设置页面配置
st.set_page_config(
    page_title="板块市值分析",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """
    加载Excel数据
    """
    # 使用相对路径，确保跨平台兼容性
    current_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(current_dir, '..', '..', 'databases', 'sector_market_cap_analysis.xlsx')
    excel_path = os.path.abspath(excel_path)
    
    if not os.path.exists(excel_path):
        st.error(f"❌ 文件不存在: {excel_path}")
        return None
    
    try:
        # 尝试使用openpyxl引擎读取Excel文件
        df = pd.read_excel(excel_path, engine='openpyxl')
        return df
    except Exception as e:
        st.error(f"❌ 读取Excel文件失败: {e}")
        st.warning("🔄 尝试读取CSV格式文件...")
        
        # 如果Excel读取失败，尝试读取CSV文件
        csv_path = excel_path.replace('.xlsx', '.csv')
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path, encoding='utf-8-sig')
                st.success("✅ 成功读取CSV文件")
                return df
            except Exception as csv_e:
                st.error(f"❌ 读取CSV文件也失败: {csv_e}")
                return None
        else:
            st.error("❌ 找不到对应的CSV文件")
            return None

def format_number(value):
    """
    格式化数字显示
    """
    if pd.isna(value):
        return "-"
    
    if isinstance(value, (int, float)):
        if value >= 1e12:
            return f"{value/1e12:.2f}万亿"
        elif value >= 1e8:
            return f"{value/1e8:.2f}亿"
        elif value >= 1e4:
            return f"{value/1e4:.2f}万"
        else:
            return f"{value:.2f}"
    
    return str(value)

def main():
    """
    主函数
    """
    # 页面标题
    st.title("📊 板块市值分析系统")
    st.markdown("---")
    
    # 加载数据
    df = load_data()
    if df is None:
        return
    
    # 侧边栏配置
    st.sidebar.header("📋 排序配置")
    
    # 排序字段选择
    sort_fields = ['超强', '超超强', '大高', '国企', '总流通值', '股票数量']
    selected_field = st.sidebar.selectbox(
        "选择排序字段：",
        sort_fields,
        index=0,
        help="选择要进行排序的字段"
    )
    
    # 排序方向选择
    sort_order = st.sidebar.radio(
        "排序方向：",
        ["降序（从高到低）", "升序（从低到高）"],
        index=0,
        help="选择排序的方向"
    )
    
    # 显示数量选择
    display_count = st.sidebar.slider(
        "显示数量：",
        min_value=10,
        max_value=len(df),
        value=40,
        step=10,
        help="选择要显示的记录数量"
    )
    
    # 数据统计信息
    st.sidebar.markdown("---")
    st.sidebar.header("📈 数据概览")
    st.sidebar.metric("总板块数", len(df))
    
    if selected_field in df.columns:
        field_stats = df[selected_field].describe()
        st.sidebar.metric(f"{selected_field} 平均值", f"{field_stats['mean']:.4f}")
        st.sidebar.metric(f"{selected_field} 最大值", f"{field_stats['max']:.4f}")
        st.sidebar.metric(f"{selected_field} 最小值", f"{field_stats['min']:.4f}")
    
    # 主内容区域
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header(f"🔽 按《{selected_field}》字段排序结果")
    
    with col2:
        # 刷新按钮
        if st.button("🔄 刷新数据", help="重新加载数据"):
            st.cache_data.clear()
            st.rerun()
    
    # 执行排序
    ascending = sort_order == "升序（从低到高）"
    df_sorted = df.sort_values(selected_field, ascending=ascending)
    
    # 显示排序结果
    display_df = df_sorted.head(display_count).copy()
    
    # 格式化显示
    if '总流通值' in display_df.columns:
        display_df['总流通值_格式化'] = display_df['总流通值'].apply(format_number)
    
    # 创建显示用的DataFrame
    show_columns = ['板块名称', selected_field]
    if '总流通值_格式化' in display_df.columns:
        show_columns.append('总流通值_格式化')
    if '股票数量' in display_df.columns:
        show_columns.append('股票数量')
    if '板块层级' in display_df.columns:
        show_columns.append('板块层级')
    
    # 重命名列以便更好显示
    display_columns = {}
    for col in show_columns:
        if col in display_df.columns:
            if col == '总流通值_格式化':
                display_columns[col] = '总流通值'
            else:
                display_columns[col] = col
    
    # 显示数据表格
    st.dataframe(
        display_df[list(display_columns.keys())].rename(columns=display_columns),
        use_container_width=True,
        height=600
    )
    
    # 显示详细统计
    st.markdown("---")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📊 字段统计")
        if selected_field in df.columns:
            stats_df = df[selected_field].describe().to_frame(selected_field)
            st.dataframe(stats_df, use_container_width=True)
    
    with col2:
        st.subheader("🏆 前5名板块")
        top_5 = df_sorted.head(5)[['板块名称', selected_field]]
        for i, (_, row) in enumerate(top_5.iterrows(), 1):
            st.write(f"{i}. **{row['板块名称']}**: {row[selected_field]:.4f}")
    
    with col3:
        st.subheader("📉 后5名板块")
        bottom_5 = df_sorted.tail(5)[['板块名称', selected_field]]
        for i, (_, row) in enumerate(bottom_5.iterrows(), 1):
            st.write(f"{i}. **{row['板块名称']}**: {row[selected_field]:.4f}")
    
    # 下载功能
    st.markdown("---")
    st.subheader("💾 数据下载")
    
    # 创建下载选项
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # CSV格式（GBK编码）
        csv_data = df_sorted.to_csv(index=False, encoding='gbk')
        st.download_button(
            label="📥 下载 CSV (GBK编码)",
            data=csv_data,
            file_name=f"板块_{selected_field}_排序结果.csv",
            mime="text/csv",
            help="下载CSV文件，适合在Excel中打开"
        )
    
    with col2:
        # Excel格式
        import io
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df_sorted.to_excel(writer, index=False, sheet_name='排序结果')
        excel_data = excel_buffer.getvalue()
        
        st.download_button(
            label="📊 下载 Excel",
            data=excel_data,
            file_name=f"板块_{selected_field}_排序结果.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="下载Excel文件，完美支持中文"
        )
    
    with col3:
        # JSON格式
        json_data = df_sorted.to_json(orient='records', force_ascii=False, indent=2)
        
        st.download_button(
            label="📄 下载 JSON",
            data=json_data,
            file_name=f"板块_{selected_field}_排序结果.json",
            mime="application/json",
            help="下载JSON文件，程序友好格式"
        )
    
    # 页脚信息
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; font-size: 12px;'>
        📊 板块市值分析系统 | 数据来源: sector_market_cap_analysis.xlsx
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()