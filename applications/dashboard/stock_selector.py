import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
# 修改导入路径以适配v2项目结构
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from core.utils.stock_filter import StockXihua
from pathlib import Path
from core.technical_analyzer.technical_analyzer import TechnicalAnalyzer, create_analyzer
from data_management.data_processor import update_and_load_data_daily, update_and_load_data_weekly, update_and_load_data_monthly
from datetime import datetime, timedelta
import warnings

# 由于config/settings.py中没有这些函数，我们需要创建替代函数
def get_operation_folder():
    """获取操作板块文件夹路径"""
    return "databases/操作板块"

def get_path_str(key):
    """获取路径字符串"""
    if key == 'operation_folder':
        return "databases/操作板块"
    return ""
warnings.filterwarnings('ignore')

def validate_stock_code(stock_code):
    """
    验证和格式化股票代码
    
    Args:
        stock_code: 股票代码（可能是字符串或数字）
        
    Returns:
        str: 格式化后的6位股票代码字符串
    """
    if stock_code is None:
        return None
    
    # 转换为字符串并去除空格
    code_str = str(stock_code).strip()
    
    # 去除非数字字符，但保留小数点前的数字
    if '.' in code_str:
        code_str = code_str.split('.')[0]
    code_str = ''.join(filter(str.isdigit, code_str))
    
    # 如果提取后为空字符串，返回None
    if not code_str:
        return None
    
    # 确保是6位数字
    if len(code_str) < 6:
        code_str = code_str.zfill(6)
    elif len(code_str) > 6:
        code_str = code_str[:6]
    
    return code_str

# 设置页面配置
st.set_page_config(
    page_title="选股系统",
    page_icon="📈",
    layout="wide"
)

# 设置页面标题
st.title("📈 选股系统")
st.markdown("---")

# 侧边栏配置
st.sidebar.title("⚙️ 配置选项")

# 确定跟踪的时间：用框来获取时间
date1 = st.sidebar.date_input('请选择跟踪日期:', datetime.today())

# 将date1转换为字符串格式
date = date1.strftime('%Y-%m-%d')

# 确定跟踪的股票：用框来获取股票代码
stock_code = st.sidebar.text_input('请输入股票代码:', '000001')

# 操作板块文件夹路径 - 使用相对路径管理
operation_folder = get_path_str('operation_folder')

def get_latest_date_folder():
    """获取最新日期文件夹"""
    try:
        if not os.path.exists(operation_folder):
            return None
        
        # 获取所有日期文件夹
        date_folders = []
        for item in os.listdir(operation_folder):
            item_path = os.path.join(operation_folder, item)
            if os.path.isdir(item_path):
                try:
                    # 尝试解析日期
                    folder_date = datetime.strptime(item, '%Y-%m-%d')
                    date_folders.append((item, folder_date))
                except ValueError:
                    continue
        
        if not date_folders:
            return None
        
        # 按日期排序，返回最新的
        date_folders.sort(key=lambda x: x[1], reverse=True)
        return date_folders[0][0]
    except Exception as e:
        st.error(f"获取最新日期文件夹时出错: {e}")
        return None

def get_csv_files_in_folder(folder_path):
    """获取指定文件夹中的所有CSV文件"""
    try:
        if not os.path.exists(folder_path):
            return []
        
        csv_files = []
        for file in os.listdir(folder_path):
            if file.endswith('.csv'):
                csv_files.append(file)
        
        return sorted(csv_files)
    except Exception as e:
        st.error(f"获取CSV文件时出错: {e}")
        return []

def get_all_csv_files():
    """获取所有CSV文件（按日期分组）"""
    try:
        all_files = {}
        if not os.path.exists(operation_folder):
            return all_files
        
        for item in os.listdir(operation_folder):
            item_path = os.path.join(operation_folder, item)
            if os.path.isdir(item_path):
                try:
                    # 验证是否为日期格式
                    datetime.strptime(item, '%Y-%m-%d')
                    csv_files = get_csv_files_in_folder(item_path)
                    if csv_files:
                        all_files[item] = csv_files
                except ValueError:
                    continue
        
        return all_files
    except Exception as e:
        st.error(f"获取所有CSV文件时出错: {e}")
        return {}

def load_stock_list_from_csv(file_path):
    """从CSV文件加载股票列表"""
    try:
        df = pd.read_csv(file_path)
        if 'stock_code' in df.columns:
            # 使用验证函数确保股票代码是6位字符串格式
            stock_codes = df['stock_code'].apply(validate_stock_code).unique().tolist()
            # 过滤掉None值
            stock_codes = [code for code in stock_codes if code is not None]
            return stock_codes
        else:
            st.error(f"CSV文件 {file_path} 中没有找到 'stock_code' 列")
            return []
    except Exception as e:
        st.error(f"加载CSV文件 {file_path} 时出错: {e}")
        return []

# 股票池选择
st.sidebar.markdown("### 📊 候选股票池选择")

# 选择模式
selection_mode = st.sidebar.radio(
    "选择候选股票池方式:",
    ["最新日期", "指定CSV文件"],
    help="最新日期：自动选择最新日期文件夹中的所有CSV文件\n指定CSV文件：手动选择特定的CSV文件"
)

stock_list = []

if selection_mode == "最新日期":
    st.sidebar.markdown("#### 📅 最新日期模式")
    
    # 获取最新日期
    latest_date = get_latest_date_folder()
    
    if latest_date:
        st.sidebar.success(f"最新日期: {latest_date}")
        
        # 获取该日期下的所有CSV文件
        latest_folder_path = os.path.join(operation_folder, latest_date)
        csv_files = get_csv_files_in_folder(latest_folder_path)
        
        if csv_files:
            st.sidebar.info(f"找到 {len(csv_files)} 个CSV文件")
            
            # 显示文件列表
            for csv_file in csv_files:
                st.sidebar.text(f"📄 {csv_file}")
            
            # 加载所有股票
            all_stocks = set()
            for csv_file in csv_files:
                file_path = os.path.join(latest_folder_path, csv_file)
                stocks = load_stock_list_from_csv(file_path)
                all_stocks.update(stocks)
            
            stock_list = list(all_stocks)
            st.sidebar.success(f"总共加载 {len(stock_list)} 只股票")
        else:
            st.sidebar.warning(f"日期 {latest_date} 下没有找到CSV文件")
    else:
        st.sidebar.error("没有找到任何日期文件夹")

elif selection_mode == "指定CSV文件":
    st.sidebar.markdown("#### 📁 指定CSV文件模式")
    
    # 获取所有CSV文件
    all_csv_files = get_all_csv_files()
    
    if all_csv_files:
        # 选择日期
        selected_date = st.sidebar.selectbox(
            "选择日期:",
            list(all_csv_files.keys()),
            help="选择包含CSV文件的日期文件夹"
        )
        
        if selected_date:
            csv_files = all_csv_files[selected_date]
            
            # 多选CSV文件
            selected_csv_files = st.sidebar.multiselect(
                "选择CSV文件:",
                csv_files,
                help="可以选择多个CSV文件"
            )
            
            if selected_csv_files:
                # 加载选中的股票
                all_stocks = set()
                for csv_file in selected_csv_files:
                    file_path = os.path.join(operation_folder, selected_date, csv_file)
                    stocks = load_stock_list_from_csv(file_path)
                    all_stocks.update(stocks)
                
                stock_list = list(all_stocks)
                st.sidebar.success(f"从 {len(selected_csv_files)} 个文件中加载了 {len(stock_list)} 只股票")
                
                # 显示选中的文件
                st.sidebar.markdown("**选中的文件:**")
                for csv_file in selected_csv_files:
                    st.sidebar.text(f"📄 {csv_file}")
            else:
                st.sidebar.warning("请选择至少一个CSV文件")
    else:
        st.sidebar.error("没有找到任何CSV文件")

# 显示股票池信息
if stock_list:
    st.markdown("### 📋 股票池信息")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("股票数量", len(stock_list))
    with col2:
        st.metric("选择模式", selection_mode)
    with col3:
        if selection_mode == "最新日期":
            st.metric("数据日期", latest_date if latest_date else "未知")
        else:
            st.metric("数据日期", selected_date if 'selected_date' in locals() else "未知")
    
    # 显示股票列表（前20只）
    st.markdown("#### 📊 候选股票池列表预览（前20只）")
    preview_df = pd.DataFrame({'股票代码': stock_list[:20]})
    st.dataframe(preview_df, use_container_width=True)
    
    if len(stock_list) > 20:
        st.info(f"显示前20只股票，总共 {len(stock_list)} 只候选股票")
else:
    st.warning("⚠️ 没有加载到任何股票，请检查配置")

# 评分板块设置
if stock_list:
    st.markdown("---")
    st.markdown("### ⚙️ 评分设置")
    
    # 板块名称输入
    st.markdown("#### 📋 板块名称设置")
    sheding_bankuai_input = st.text_input(
        "请输入用于评分的板块名称（用逗号分隔）:",
        value="国企,有色金属",
        help="例如：国企,有色金属,新能源,科技"
    )
    
    # 解析输入的板块名称
    if sheding_bankuai_input:
        sheding_bankuai = [name.strip() for name in sheding_bankuai_input.split(',') if name.strip()]
        test_bankuais = sheding_bankuai
        st.info(f"当前设置的板块: {', '.join(test_bankuais)}")
    else:
        test_bankuais = ['国企', '有色金属']  # 默认值
        st.warning("未输入板块名称，使用默认值: 国企, 有色金属")

# 主力评分功能
if stock_list:
    st.markdown("---")
    st.markdown("### 🎯 主力评分")
    
    # 添加调试信息
    st.info(f"🔍 调试信息: stock_list长度 = {len(stock_list)}, 前5个股票 = {stock_list[:5]}")
    
    if st.button("🚀 开始主力评分分析", type="primary"):
        with st.spinner("正在计算主力评分..."):
            try:
                # 导入主力评分函数
                from core.utils.stock_filter import zhuli_scores
                
                # 计算主力评分
                st.info(f"正在计算 {len(stock_list)} 只股票的主力评分...")
                zhuli_df = zhuli_scores(stock_list)
                st.info(f"zhuli_scores返回结果: {len(zhuli_df)} 条记录")
                
                if not zhuli_df.empty:
                    st.success(f"✅ 成功计算 {len(zhuli_df)} 只股票的主力评分")
                    
                    # 显示主力评分结果
                    st.markdown("#### 📊 主力评分结果")
                    
                    # 格式化显示数据
                    display_df = zhuli_df.copy()
                    
                    # 格式化数值列
                    if '汇总得分' in display_df.columns:
                        display_df['汇总得分'] = display_df['汇总得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if '得分' in display_df.columns:
                        display_df['得分'] = display_df['得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    
                    # 重命名列
                    column_mapping = {
                        'stock_code': '股票代码',
                        'name': '股票名称',
                        '超强': '超强',
                        '超超强': '超超强',
                        '大高': '大高',
                        '央企': '央企',
                        '国企': '国企',
                        '汇总得分': '汇总得分',
                        '得分': '最终得分'
                    }
                    
                    display_df = display_df.rename(columns=column_mapping)
                    
                    st.dataframe(display_df, use_container_width=True)
                    
                    # 提供下载功能
                    csv_data = zhuli_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 下载主力评分结果",
                        data=csv_data,
                        file_name=f"主力评分_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("❌ 没有找到有效的主力评分数据")
                    
            except Exception as e:
                st.error(f"❌ 主力评分计算过程中出现错误: {str(e)}")
                st.exception(e)

# 所属板块评分功能
if stock_list:
    st.markdown("---")
    st.markdown("### 🏢 所属板块评分")
    
    if st.button("🚀 开始板块评分分析", type="primary"):
        with st.spinner("正在计算板块评分..."):
            try:
                # 导入板块评分函数
                from core.utils.stock_filter import bankuai_scores
                
                # 计算板块评分
                bankuai_df = bankuai_scores(stock_list, *test_bankuais)
                
                if not bankuai_df.empty:
                    st.success(f"✅ 成功计算 {len(bankuai_df)} 只股票的板块评分")
                    st.info(f"测试板块: {', '.join(test_bankuais)}")
                    
                    # 显示板块评分结果
                    st.markdown("#### 📊 板块评分结果")
                    
                    # 格式化显示数据
                    display_df = bankuai_df.copy()
                    
                    # 格式化数值列
                    if '原始得分' in display_df.columns:
                        display_df['原始得分'] = display_df['原始得分'].astype(str).apply(lambda x: f"{float(x):.0f}" if x != 'nan' and x != '' else "N/A")
                    if '得分' in display_df.columns:
                        display_df['得分'] = display_df['得分'].astype(str).apply(lambda x: f"{float(x):.0f}" if x != 'nan' and x != '' else "N/A")
                    
                    # 重命名列
                    column_mapping = {
                        'stock_code': '股票代码',
                        'name': '股票名称',
                        '原始得分': '原始得分',
                        '得分': '最终得分'
                    }
                    
                    # 添加板块列的重命名
                    for bankuai in test_bankuais:
                        if bankuai in display_df.columns:
                            column_mapping[bankuai] = bankuai
                    
                    display_df = display_df.rename(columns=column_mapping)
                    
                    st.dataframe(display_df, use_container_width=True)
                        
                        # 提供下载功能
                    csv_data = bankuai_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 下载板块评分结果",
                            data=csv_data,
                        file_name=f"板块评分_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                else:
                    st.error("❌ 没有找到有效的板块评分数据")
                        
            except Exception as e:
                st.error(f"❌ 板块评分计算过程中出现错误: {str(e)}")
                st.exception(e)

# 技术指标分析功能
if stock_list:
    st.markdown("---")
    st.markdown("### 📈 技术指标分析")
    
    # 执行技术分析
    if st.button("🔍 开始技术分析", type="primary"):
        with st.spinner("正在进行技术指标分析..."):
            try:
                # 导入StockTechnicalAnalyzer类
                from core.technical_analyzer.stock_technical_analyzer import StockTechnicalAnalyzer
                
                # 创建分析器实例
                analyzer = StockTechnicalAnalyzer()
                
                # 测试数据库连接
                try:
                    from data_management.database_manager import DatabaseManager
                    db_manager = DatabaseManager()
                    # 测试查询
                    test_query = "SELECT COUNT(*) as count FROM k_daily LIMIT 1"
                    result = db_manager.execute_query(test_query)
                    if not result.empty:
                        st.info(f"✅ 数据库连接正常，日线数据表记录数: {result.iloc[0]['count']}")
                    else:
                        st.warning("⚠️ 数据库连接正常，但日线数据表为空")
                except Exception as e:
                    st.error(f"❌ 数据库连接失败: {e}")
                    st.stop()
                
                # 使用跟踪日期进行分析
                analysis_date_str = date  # 使用跟踪日期
                
                # 添加调试信息
                st.info(f"🔍 开始分析 {len(stock_list)} 只股票，分析日期: {analysis_date_str}")
                
                # 分别计算四种技术指标
                st.info("📊 计算中级技术指标...")
                zj_df = analyzer.get_jishu_zj(stock_list, analysis_date_str)
                st.info(f"中级技术指标结果: {len(zj_df)} 条记录")
                
                st.info("📊 计算长线技术指标...")
                cx_df = analyzer.get_jishu_cx(stock_list, analysis_date_str)
                st.info(f"长线技术指标结果: {len(cx_df)} 条记录")
                
                st.info("📊 计算超长线技术指标...")
                ccx_df = analyzer.get_jishu_ccx(stock_list, analysis_date_str)
                st.info(f"超长线技术指标结果: {len(ccx_df)} 条记录")
                
                st.info("📊 计算ATR技术指标...")
                atr_df = analyzer.get_jishu_atr(stock_list, analysis_date_str)
                st.info(f"ATR技术指标结果: {len(atr_df)} 条记录")
                
                # 计算综合技术指标评分
                st.info("📊 计算综合技术指标评分...")
                scores_df = analyzer.get_jishu_scores(stock_list, analysis_date_str)
                st.info(f"综合技术指标评分结果: {len(scores_df)} 条记录")
                
                if not scores_df.empty:
                    st.success(f"✅ 成功分析 {len(scores_df)} 只股票")
                    
                    # 显示技术指标分析结果
                    st.markdown("#### 📊 技术指标分析结果")
                    
                    # 格式化显示数据
                    display_df = scores_df.copy()
                    
                    # 格式化数值列
                    if 'zj_score' in display_df.columns:
                        display_df['zj_score'] = display_df['zj_score'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if 'cx_score' in display_df.columns:
                        display_df['cx_score'] = display_df['cx_score'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if 'ccx_score' in display_df.columns:
                        display_df['ccx_score'] = display_df['ccx_score'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if 'atr_score' in display_df.columns:
                        display_df['atr_score'] = display_df['atr_score'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if 'total_score' in display_df.columns:
                        display_df['total_score'] = display_df['total_score'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    
                    st.dataframe(display_df, use_container_width=True)
                    
                    # 准备详细的下载数据
                    # 合并所有详细的技术指标数据
                    detailed_df = pd.DataFrame({'stock_code': stock_list})
                    
                    # 合并中级技术指标详细数据
                    if not zj_df.empty:
                        zj_detail = zj_df[['stock_code', 'zj_jjdi', 'zj_di', 'zjdtg', 'zjdtz', 'zj_score']].copy()
                        detailed_df = detailed_df.merge(zj_detail, on='stock_code', how='left')
                    
                    # 合并长线技术指标详细数据
                    if not cx_df.empty:
                        cx_detail = cx_df[['stock_code', 'cx_jjdi', 'cx_di', 'cxdtg', 'cxdtz', 'cx_ding_tzz', 'cx_ding_baoliang', 'cx_score']].copy()
                        detailed_df = detailed_df.merge(cx_detail, on='stock_code', how='left')
                    
                    # 合并超长线技术指标详细数据
                    if not ccx_df.empty:
                        ccx_detail = ccx_df[['stock_code', 'ccx_jjdi', 'ccx_di', 'ccxdtg', 'ccxdtz', 'ccx_score']].copy()
                        detailed_df = detailed_df.merge(ccx_detail, on='stock_code', how='left')
                    
                    # 合并ATR技术指标详细数据
                    if not atr_df.empty:
                        atr_detail = atr_df[['stock_code', 'atr_value', 'atr_score']].copy()
                        detailed_df = detailed_df.merge(atr_detail, on='stock_code', how='left')
                    
                    # 添加总评分
                    if not scores_df.empty:
                        total_detail = scores_df[['stock_code', 'total_score']].copy()
                        detailed_df = detailed_df.merge(total_detail, on='stock_code', how='left')
                    
                    # 添加股票名称
                    if not zj_df.empty and 'stock_name' in zj_df.columns:
                        name_detail = zj_df[['stock_code', 'stock_name']].copy()
                        detailed_df = detailed_df.merge(name_detail, on='stock_code', how='left')
                    
                    # 调整列顺序，将基本信息放在前面
                    basic_cols = ['stock_code', 'stock_name'] if 'stock_name' in detailed_df.columns else ['stock_code']
                    other_cols = [col for col in detailed_df.columns if col not in basic_cols]
                    detailed_df = detailed_df[basic_cols + other_cols]
                    
                    # 提供下载功能
                    csv_data = detailed_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 下载技术分析结果",
                        data=csv_data,
                        file_name=f"技术分析结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("❌ 没有找到有效的技术指标数据")
                    
            except Exception as e:
                st.error(f"❌ 技术分析过程中出现错误: {str(e)}")
                st.exception(e)

# 汇总上面3项：主力，板块，技术的得分，合并成一个df，并计算总得分
if stock_list:
    st.markdown("---")
    st.markdown("### 📈 汇总得分")
    if st.button("🚀 开始汇总得分分析", type="primary"):
        with st.spinner("正在进行汇总得分分析..."):
            try:
                # 导入汇总得分函数
                from core.utils.stock_filter import total_scores
                
                # 使用跟踪日期进行分析
                analysis_date_str = date  # 使用跟踪日期
                
                # 计算汇总得分
                total_df = total_scores(stock_list, *test_bankuais, date=analysis_date_str)
                
                if not total_df.empty:
                    st.success(f"✅ 成功计算 {len(total_df)} 只股票的汇总得分")
                    st.info(f"测试板块: {', '.join(test_bankuais)}")
                    
                    # 显示汇总得分结果
                    st.markdown("#### 📊 汇总得分结果")
                    
                    # 格式化显示数据
                    display_df = total_df.copy()
                    
                    # 格式化数值列
                    if '总得分' in display_df.columns:
                        display_df['总得分'] = display_df['总得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if '技术得分' in display_df.columns:
                        display_df['技术得分'] = display_df['技术得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if '主力得分' in display_df.columns:
                        display_df['主力得分'] = display_df['主力得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    if '板块得分' in display_df.columns:
                        display_df['板块得分'] = display_df['板块得分'].astype(str).apply(lambda x: f"{float(x):.2f}" if x != 'nan' and x != '' else "N/A")
                    
                    # 重命名列
                    column_mapping = {
                        'stock_code': '股票代码',
                        'name': '股票名称',
                        '总得分': '总得分',
                        '技术得分': '技术得分',
                        '主力得分': '主力得分',
                        '板块得分': '板块得分'
                    }
                    
                    display_df = display_df.rename(columns=column_mapping)
                    
                    st.dataframe(display_df, use_container_width=True)
                    
                    # 提供下载功能
                    csv_data = total_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 下载汇总得分结果",
                        data=csv_data,
                        file_name=f"汇总得分_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.error("❌ 没有找到有效的汇总得分数据")
                    
            except Exception as e:
                st.error(f"❌ 汇总得分计算过程中出现错误: {str(e)}")
                st.exception(e)


# 页面底部信息
st.markdown("---")
st.markdown("### ℹ️ 使用说明")
st.markdown("""
1. **候选股票池选择**: 可以选择最新日期的所有CSV文件，或手动选择特定的CSV文件
2. **基础筛选**: 自动去除ST股票和新股，支持按基本面特征筛选
3. **技术指标分析**: 支持多种技术指标分析，可单只股票详细分析或批量快速分析
4. **结果导出**: 所有分析结果都可以导出为CSV文件
""")

st.markdown("---")
st.markdown(f"📅 系统时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
