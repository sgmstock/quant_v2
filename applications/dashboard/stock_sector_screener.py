"""
Streamlit板块分析系统 (修改版 V4)

功能：
1. 申万行业三级联动选择
2. 地域板块选择
3. 基本面板块选择
4. 概念板块选择
5. 板块交集计算
6. 个股信息展示
7. 板块列表管理与多种方式保存（优化了文件名，并增加股票名称列）
"""

import streamlit as st
import sqlite3
import pandas as pd
import os
from typing import List, Dict, Set, Optional
import logging
import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 页面配置
st.set_page_config(
    page_title="板块分析系统",
    page_icon="📊",
    layout="wide"
)

# ----------------------------------------------------------------------
# 数据处理函数 (这部分未作修改)
# ----------------------------------------------------------------------
@st.cache_resource
def get_db_connection():
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None

@st.cache_data
def get_sw_hierarchy_data():
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = "SELECT DISTINCT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name FROM sw_cfg_hierarchy WHERE l1_name IS NOT NULL AND l1_name != '' ORDER BY l1_code, l2_code, l3_code"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"获取申万行业数据失败: {e}")
        return pd.DataFrame()

@st.cache_data
def get_province_data():
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = "SELECT DISTINCT province FROM stock_basic WHERE province IS NOT NULL AND province != '' ORDER BY province"
        df = pd.read_sql_query(query, conn)
        conn.close()
        provinces = df['province'].dropna().tolist()
        return sorted(list(set(provinces)))
    except Exception as e:
        st.error(f"获取省份数据失败: {e}")
        return []

@st.cache_data
def get_fundamental_sectors():
    return ['国企', 'B股', 'H股', '老股', '大高', '高价', '低价', '次新', '非公开多', '非公开', '超20', '超40', '超60', '超强', '超超强']

@st.cache_data
def get_concept_sectors():
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = "SELECT DISTINCT industry_name FROM tdx_cfg WHERE industry_name IS NOT NULL AND industry_name != '' ORDER BY industry_name"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df['industry_name'].tolist()
    except Exception as e:
        st.error(f"获取概念板块数据失败: {e}")
        return []

def get_stocks_by_sw_sector(l1_name: Optional[str] = None, l2_name: Optional[str] = None, l3_name: Optional[str] = None) -> Set[str]:
    try:
        conn = sqlite3.connect('databases/quant_system.db')
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
            conn.close()
            return set()
        where_clause = " AND ".join(conditions)
        query = f"SELECT DISTINCT stock_code FROM sw_cfg_hierarchy WHERE {where_clause} AND stock_code IS NOT NULL"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return set(df['stock_code'].tolist())
    except Exception as e:
        st.error(f"获取申万行业股票失败: {e}")
        return set()

def get_stocks_by_province(province: str) -> Set[str]:
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = "SELECT DISTINCT stock_code FROM stock_basic WHERE province = ? AND stock_code IS NOT NULL"
        df = pd.read_sql_query(query, conn, params=[province])
        conn.close()
        return set(df['stock_code'].tolist())
    except Exception as e:
        st.error(f"获取省份股票失败: {e}")
        return set()

def get_stocks_by_fundamental_sector(sector: str) -> Set[str]:
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = f"SELECT DISTINCT stock_code FROM stock_basic_pro WHERE `{sector}` = 1 AND stock_code IS NOT NULL"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return set(df['stock_code'].tolist())
    except Exception as e:
        st.error(f"获取基本面板块股票失败: {e}")
        return set()

def get_stocks_by_concept_sector(concept: str) -> Set[str]:
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        query = "SELECT DISTINCT stock_code FROM tdx_cfg WHERE industry_name = ? AND stock_code IS NOT NULL"
        df = pd.read_sql_query(query, conn, params=[concept])
        conn.close()
        return set(df['stock_code'].tolist())
    except Exception as e:
        st.error(f"获取概念板块股票失败: {e}")
        return set()

def get_stock_details(stock_codes: Set[str]) -> pd.DataFrame:
    if not stock_codes:
        return pd.DataFrame()
    try:
        conn = sqlite3.connect('databases/quant_system.db')
        placeholders = ','.join(['?' for _ in stock_codes])
        basic_query = f"SELECT stock_code, stock_name, listing_date FROM stock_basic WHERE stock_code IN ({placeholders})"
        basic_df = pd.read_sql_query(basic_query, conn, params=list(stock_codes))
        pro_query = f"SELECT stock_code, 国企, B股, H股, 老股, 大高, 高价, 低价, 次新, 非公开多, 非公开, 超20, 超40, 超60, 超强, 超超强 FROM stock_basic_pro WHERE stock_code IN ({placeholders})"
        pro_df = pd.read_sql_query(pro_query, conn, params=list(stock_codes))
        conn.close()
        if not basic_df.empty and not pro_df.empty:
            result_df = pd.merge(basic_df, pro_df, on='stock_code', how='outer')
        elif not basic_df.empty:
            result_df = basic_df
        else:
            result_df = pro_df
        if not result_df.empty:
            basic_cols = ['stock_code', 'stock_name', 'listing_date']
            pro_cols = ['国企', 'B股', 'H股', '老股', '大高', '高价', '低价', '次新', '非公开多', '非公开', '超20', '超40', '超60', '超强', '超超强']
            available_basic_cols = [col for col in basic_cols if col in result_df.columns]
            available_pro_cols = [col for col in pro_cols if col in result_df.columns]
            result_df = result_df[available_basic_cols + available_pro_cols]
        return result_df
    except Exception as e:
        st.error(f"获取股票重要信息失败: {e}")
        return pd.DataFrame()

# 辅助函数 (这部分未作修改)
def make_safe_filename(name: str, max_length: int = 50) -> str:
    safe_name = name.replace('&', '和').replace(':', ' ').replace('/', ' ').replace('\\', ' ')
    safe_name = "".join([c for c in safe_name if c.isalnum() or c in ('_', '-', ' ') or '\u4e00' <= c <= '\u9fff'])
    safe_name = safe_name.strip()
    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length] + "..."
    return safe_name

# ----------------------------------------------------------------------
# <--- 修改点 1：修改独立保存函数，使其可以处理并保存 stock_name
# ----------------------------------------------------------------------
def save_sector_to_csv_simple(sector_data: Dict, date: str) -> Optional[str]:
    """将单个板块数据保存到CSV文件（包含股票代码和名称）"""
    base_dir = f"databases/操作板块/{date}"
    os.makedirs(base_dir, exist_ok=True)
    
    safe_name = make_safe_filename(sector_data['name'])
    filename = f"{base_dir}/{sector_data['type']}_{safe_name}.csv"
    
    # 直接使用传入的 stock_info 列表创建 DataFrame
    df = pd.DataFrame(sector_data['stock_info'])
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logger.info(f"板块 '{sector_data['name']}' 已保存到 {filename}")
    return filename

# ----------------------------------------------------------------------
# <--- 修改点 2：修改合并保存函数，使其可以处理并保存 stock_name
# ----------------------------------------------------------------------
def save_all_sectors_in_one_csv(sectors_data: List[Dict], date: str) -> Optional[str]:
    """将所有板块的股票合并保存到一个CSV文件，并生成描述性文件名。"""
    if not sectors_data:
        return None

    base_dir = f"databases/操作板块/{date}"
    os.makedirs(base_dir, exist_ok=True)
    
    first_sector_name = sectors_data[0]['name']
    if len(sectors_data) > 1:
        file_title = f"{first_sector_name}_等{len(sectors_data)}个板块"
    else:
        file_title = first_sector_name
        
    safe_file_title = make_safe_filename(file_title)
    filename = f"{base_dir}/合并_{safe_file_title}_{date}.csv"
    
    all_stocks = []
    for sector in sectors_data:
        # 遍历 stock_info 列表，它现在是 [{'stock_code': ..., 'stock_name': ...}, ...]
        for stock_info in sector['stock_info']:
            all_stocks.append({
                'stock_code': stock_info['stock_code'],
                'stock_name': stock_info['stock_name'], # <--- 增加股票名称
                'sector_name': sector['name'],
                'sector_type': sector['type']
            })
            
    if not all_stocks:
        return None

    df = pd.DataFrame(all_stocks)
    # 调整列顺序，让 stock_name 紧随 stock_code
    df = df[['stock_code', 'stock_name', 'sector_name', 'sector_type']]
    df.drop_duplicates(subset=['stock_code', 'sector_name'], inplace=True)
    df.sort_values(by='stock_code', inplace=True)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logger.info(f"所有板块已合并保存到 {filename}")
    return filename

# ----------------------------------------------------------------------
# 主页面 UI 渲染与交互逻辑
# ----------------------------------------------------------------------
if 'selected_sectors' not in st.session_state:
    st.session_state.selected_sectors = []

st.title("📊 板块分析系统")
st.caption("选择不同维度的板块进行交集分析，筛选出符合所有条件的股票。")

with st.container(border=True):
    st.markdown("**跟踪日期**")
    selected_date_obj = st.date_input("选择操作日期", datetime.date.today(), key="track_date", label_visibility="collapsed")
    date_str = selected_date_obj.strftime('%Y-%m-%d')
    st.info(f"当前操作日期: **{date_str}**。所有保存的文件都将存放在该日期的文件夹下。")

st.header("板块选择区", divider='rainbow')

# --- 板块选择UI (这部分未作修改) ---
sw_data = get_sw_hierarchy_data()
provinces = get_province_data()
fundamental_sectors = get_fundamental_sectors()
concept_sectors = get_concept_sectors()
col1, col2 = st.columns(2)
with col1.container(border=True):
    st.subheader("申万行业板块")
    if not sw_data.empty:
        l1_options = [''] + sorted(sw_data['l1_name'].dropna().unique())
        selected_l1 = st.selectbox("一级行业", options=l1_options, key='sw_l1')
        l2_options = ['']
        if selected_l1:
            l2_options += sorted(sw_data[sw_data['l1_name'] == selected_l1]['l2_name'].dropna().unique())
        selected_l2 = st.selectbox("二级行业", options=l2_options, key='sw_l2', disabled=not selected_l1)
        l3_options = ['']
        if selected_l1 and selected_l2:
            l3_options += sorted(sw_data[(sw_data['l1_name'] == selected_l1) & (sw_data['l2_name'] == selected_l2)]['l3_name'].dropna().unique())
        selected_l3 = st.selectbox("三级行业", options=l3_options, key='sw_l3', disabled=not selected_l2)
    else:
        st.error("无法加载申万行业数据。")
with col1.container(border=True):
    st.subheader("地域板块")
    selected_provinces = st.multiselect("选择省份", options=provinces, key='provinces')
with col2.container(border=True):
    st.subheader("基本面板块")
    selected_fundamentals = st.multiselect("选择基本面特征", options=fundamental_sectors, key='fundamentals')
with col2.container(border=True):
    st.subheader("概念板块")
    selected_concepts = st.multiselect("选择概念板块", options=concept_sectors, key='concepts')
st.divider()

if st.button("🚀 开始分析", type="primary", use_container_width=True):
    selected_stocks_sets, selection_summary, sector_name_parts = [], [], []
    if selected_l3:
        sw_stocks = get_stocks_by_sw_sector(l3_name=selected_l3)
        selection_summary.append(f"申万三级: {selected_l3} ({len(sw_stocks)}只)")
        if sw_stocks: selected_stocks_sets.append(sw_stocks); sector_name_parts.append(selected_l3)
    elif selected_l2:
        sw_stocks = get_stocks_by_sw_sector(l2_name=selected_l2)
        selection_summary.append(f"申万二级: {selected_l2} ({len(sw_stocks)}只)")
        if sw_stocks: selected_stocks_sets.append(sw_stocks); sector_name_parts.append(selected_l2)
    elif selected_l1:
        sw_stocks = get_stocks_by_sw_sector(l1_name=selected_l1)
        selection_summary.append(f"申万一级: {selected_l1} ({len(sw_stocks)}只)")
        if sw_stocks: selected_stocks_sets.append(sw_stocks); sector_name_parts.append(selected_l1)
    for province in selected_provinces:
        province_stocks = get_stocks_by_province(province)
        selection_summary.append(f"地域: {province} ({len(province_stocks)}只)")
        if province_stocks: selected_stocks_sets.append(province_stocks); sector_name_parts.append(province)
    for fundamental in selected_fundamentals:
        fundamental_stocks = get_stocks_by_fundamental_sector(fundamental)
        selection_summary.append(f"基本面: {fundamental} ({len(fundamental_stocks)}只)")
        if fundamental_stocks: selected_stocks_sets.append(fundamental_stocks); sector_name_parts.append(fundamental)
    for concept in selected_concepts:
        concept_stocks = get_stocks_by_concept_sector(concept)
        selection_summary.append(f"概念: {concept} ({len(concept_stocks)}只)")
        if concept_stocks: selected_stocks_sets.append(concept_stocks); sector_name_parts.append(concept)
    st.session_state.last_analysis = {"summary": selection_summary, "sets": selected_stocks_sets, "name_parts": sector_name_parts}

if 'last_analysis' in st.session_state:
    st.header("分析结果", divider='rainbow')
    analysis = st.session_state.last_analysis
    if not analysis["sets"]:
        st.warning("您没有选择任何有效的板块，或者所选板块下没有股票。")
    else:
        st.write("您选择的板块组合如下："); st.info(" & ".join(analysis["summary"]))
        intersection_stocks = set.intersection(*analysis["sets"])
        st.success(f"所有选定板块的交集共有 **{len(intersection_stocks)}** 只股票。")
        if intersection_stocks:
            stock_details_df = get_stock_details(intersection_stocks)
            if not stock_details_df.empty:
                st.dataframe(stock_details_df, use_container_width=True, hide_index=True)
                # ----------------------------------------------------------------------
                # <--- 修改点 3：修改“添加”逻辑，保存 code 和 name
                # ----------------------------------------------------------------------
                if st.button("📋 添加到待保存列表", type="secondary"):
                    sector_name = " & ".join(analysis["name_parts"])
                    # 从已查询到的 stock_details_df 中提取 code 和 name，并转为字典列表
                    stock_info_list = stock_details_df[['stock_code', 'stock_name']].sort_values(by='stock_code').to_dict('records')
                    sector_data = {
                        'name': sector_name, 'type': '交集板块',
                        'stock_info': stock_info_list, # <--- 使用新的 key 存储更完整的信息
                        'description': f"交集板块: {' & '.join(analysis['summary'])}"
                    }
                    st.session_state.selected_sectors.append(sector_data)
                    st.success(f"✅ 已添加板块 '{sector_name}' 到待保存列表。")
                    st.rerun()
            else:
                st.error("未能查询到交集股票的详细信息。")

if st.session_state.selected_sectors:
    st.header("待保存的板块列表", divider='rainbow')
    # ----------------------------------------------------------------------
    # <--- 修改点 4：更新UI显示和预览逻辑，以使用新的数据结构 'stock_info'
    # ----------------------------------------------------------------------
    for i, sector in reversed(list(enumerate(st.session_state.selected_sectors))):
        with st.container(border=True):
            col1, col2, col3 = st.columns([4, 1, 1])
            # 使用 len(sector['stock_info']) 来获取股票数量
            col1.markdown(f"**{i+1}. {sector['name']}** (`{sector['type']}`) - **{len(sector['stock_info'])}** 只股票")
            if col2.button("👁️ 预览", key=f"preview_{i}"):
                st.session_state[f"show_preview_{i}"] = not st.session_state.get(f"show_preview_{i}", False)
            if col3.button("🗑️ 删除", key=f"delete_{i}"):
                st.session_state.selected_sectors.pop(i)
                st.rerun()
            if st.session_state.get(f"show_preview_{i}", False):
                # 从 stock_info 中提取 code 集合用于查询完整的股票详情
                stock_codes_for_preview = {info['stock_code'] for info in sector['stock_info']}
                stock_details = get_stock_details(stock_codes_for_preview)
                if not stock_details.empty:
                    st.dataframe(stock_details, use_container_width=True, hide_index=True, height=200)
                else:
                    st.warning("无法获取股票详情。")

    # --- 保存与导出区域 (这部分未作修改) ---
    st.header("保存与导出", divider='rainbow')
    save_dir = f"databases/操作板块/{date_str}/"
    st.markdown(f"当前板块列表中的所有板块将被保存到目录: `{save_dir}`")
    btn_col1, btn_col2, btn_col3 = st.columns(3)
    with btn_col1:
        if st.button("📁 保存每个板块为独立文件", use_container_width=True):
            saved_files = [save_sector_to_csv_simple(s, date_str) for s in st.session_state.selected_sectors]
            saved_files = [f for f in saved_files if f]
            st.success(f"✅ 操作成功！共保存了 {len(saved_files)} 个独立文件。")
            st.session_state.saved_files_info = {"files": saved_files, "dir": save_dir}
    with btn_col2:
        if st.button("📦 合并所有板块保存为单个文件", type="primary", use_container_width=True):
            file_path = save_all_sectors_in_one_csv(st.session_state.selected_sectors, date_str)
            if file_path:
                st.success(f"✅ 操作成功！已将所有板块合并保存到: `{file_path}`")
                st.session_state.saved_files_info = {"files": [file_path], "dir": save_dir}
    with btn_col3:
        if st.button("❌ 清空待保存列表", use_container_width=True):
            st.session_state.selected_sectors = []
            if 'saved_files_info' in st.session_state: del st.session_state['saved_files_info']
            st.rerun()
    if 'saved_files_info' in st.session_state:
        with st.expander("📂 查看已保存文件并下载", expanded=True):
            info = st.session_state.saved_files_info
            st.write(f"以下文件已保存到目录 `{info['dir']}`:")
            for i, f_path in enumerate(info['files']):
                f_name = os.path.basename(f_path)
                st.markdown(f"**{i+1}. {f_name}**")
                with open(f_path, "rb") as f:
                    st.download_button(label=f"📥 下载 {f_name}", data=f.read(), file_name=f_name, mime="text/csv", key=f"download_{i}")
            st.info("提示：文件已保存在运行本程序的电脑的本地磁盘上，您也可以直接在对应目录找到它们。")