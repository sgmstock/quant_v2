#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票细化筛选类func_xuangu

实现功能：对板块的候选股按照基本面等情况进行筛选，继续进行细分。设计成类，可以调用这些筛选对象
读取stock_basic表。去掉输入参数stocklist上市时间在2023-07-05之后的新股股票.去掉stocklist的stock_code不匹配的股票（实际就是停牌股票。暂停上市）
读取去掉st,新股后。和stock_code不匹配的股票。
继续进行筛选，筛选方案如下：
先对stocklist生成dataframe,字段有code、name、流通值、收盘价、超强、超超强、国企、次新、老股、非公开多、非公开、H、B
元素值获得方式是读取stock_basic表，stock_basic_pro表。
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
# 更新导入路径以符合v2项目架构
from data_management.database_manager import DatabaseManager
# from core.technical_analysis.stock_technical_analyzer import StockTechnicalAnalyzer  # 技术分析器

class StockXihua:
    """股票细化筛选类
    
    用于对板块候选股进行基本面筛选和细分，支持多种筛选条件：
    - 去除ST股票和新股
    - 基本面特征筛选（国企、次新、老股等）
    - 基于分位数的股票分类（大市值、小市值、高价股、低价股等）
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """初始化股票细化筛选器
        
        Args:
            db_path: 数据库路径，默认使用项目数据库
        """
        if db_path is None:
            db_path = "databases/quant_system.db"
        self.db_connection = DatabaseManager(db_path)
        
        # 新股判断基准日期
        self.new_stock_cutoff_date = '2023-07-05'
        
        # 缓存数据
        self._stock_basic_cache = None
        self._stock_basic_pro_cache = None
        self._valid_stock_codes = None
        
        # 分位数分类结果
        self.dsz = []  # 大市值股票
        self.xsz = []  # 小市值股票
        self.gbj = []  # 高价股
        self.dbj = []  # 低价股
        self.dg = []   # 大高股（大市值且高价）
        
        # 基本面特征分类结果
        self.gq = []   # 国企股
        self.cq = []   # 超强股
    
    def get_connection(self):
        """获取数据库连接（向后兼容）"""
        return self.db_connection
    
    def _load_stock_basic(self) -> pd.DataFrame:
        """加载stock_basic表数据"""
        if self._stock_basic_cache is None:
            query = """
            SELECT stock_code, stock_name, listing_date, company_name,
                   industry_a, industry_b, industry_c, province, city,
                   ownership_type, ipo_price, ipo_shares, status_code
            FROM stock_basic
            """
            self._stock_basic_cache = self.db_connection.execute_query(query)
        return self._stock_basic_cache
    
    def _load_stock_basic_pro(self) -> pd.DataFrame:
         """加载stock_basic_pro表数据"""
         if self._stock_basic_pro_cache is None:
             query = """
             SELECT stock_code, 国企, B股, H股, 老股, 大高, 高价, 低价, 次新,
                    非公开多, 非公开, 超20, 超40, 超60, 超强, 超超强,
                    流通值, 收盘价
             FROM stock_basic_pro
             """
             self._stock_basic_pro_cache = self.db_connection.execute_query(query)
         return self._stock_basic_pro_cache
    
    def filter_basic_conditions(self, stock_list: List[str]) -> List[str]:
        """基础条件筛选：去掉ST股票和新股
        
        Args:
            stock_list: 输入的股票代码列表
            
        Returns:
            过滤后的股票代码列表
        """
        if not stock_list:
            return []
        
        # 加载基础数据
        stock_basic = self._load_stock_basic()
        
        
        filtered_stocks = []
        
        for stock_code in stock_list:
            # 检查股票是否在基础数据中
            stock_info = stock_basic[stock_basic['stock_code'] == stock_code]
            if stock_info.empty:
                continue
            
            stock_row = stock_info.iloc[0]
            stock_name = stock_row['stock_name']
            listing_date = stock_row['listing_date']
            
            # 1. 去掉ST股票
            if 'ST' in stock_name or '*ST' in stock_name or 'st' in stock_name.lower():
                continue
            
            # 2. 去掉新股（2023-07-05之后上市）
            if listing_date and listing_date > self.new_stock_cutoff_date:
                continue
            
            
            # 通过所有筛选条件
            filtered_stocks.append(stock_code)
        
        return filtered_stocks
    
    def create_stock_dataframe(self, stock_list: List[str]) -> pd.DataFrame:
        """创建股票数据框架
        
        生成包含以下字段的DataFrame：
        code、name、流通值、收盘价、超强、超超强、国企、次新、老股、非公开多、非公开、H、B
        
        Args:
            stock_list: 股票代码列表
            
        Returns:
            包含股票详细信息的DataFrame
        """
        if not stock_list:
            return pd.DataFrame()
        
        # 先进行基础条件筛选
        filtered_stocks = self.filter_basic_conditions(stock_list)
        
        if not filtered_stocks:
            return pd.DataFrame()
        
        # 加载数据
        stock_basic = self._load_stock_basic()
        stock_basic_pro = self._load_stock_basic_pro()
        
        # 创建结果DataFrame
        result_data = []
        
        for stock_code in filtered_stocks:
            # 获取基础信息
            basic_info = stock_basic[stock_basic['stock_code'] == stock_code]
            if basic_info.empty:
                continue
            
            basic_row = basic_info.iloc[0]
            
            # 获取扩展信息
            pro_info = stock_basic_pro[stock_basic_pro['stock_code'] == stock_code]
            
            # 构建数据行
            row_data = {
                'code': stock_code,
                'name': basic_row['stock_name'],
                '流通值': 0.0,  # 将从stock_basic_pro表获取
                '收盘价': 0.0,  # 将从stock_basic_pro表获取
            }
            
            # 添加基本面特征（从stock_basic_pro表）
            if not pro_info.empty:
                pro_row = pro_info.iloc[0]
                row_data.update({
                    '流通值': float(pro_row.get('流通值', 0.0)) if pro_row.get('流通值') is not None else 0.0,
                    '收盘价': float(pro_row.get('收盘价', 0.0)) if pro_row.get('收盘价') is not None else 0.0,
                    '超强': bool(pro_row.get('超强', False)),
                    '超超强': bool(pro_row.get('超超强', False)),
                    '国企': bool(pro_row.get('国企', False)),
                    '次新': bool(pro_row.get('次新', False)),
                    '老股': bool(pro_row.get('老股', False)),
                    '非公开多': bool(pro_row.get('非公开多', False)),
                    '非公开': bool(pro_row.get('非公开', False)),
                    'H': bool(pro_row.get('H股', False)),
                    'B': bool(pro_row.get('B股', False))
                })
            else:
                # 如果没有扩展信息，设置默认值
                row_data.update({
                    '超强': False,
                    '超超强': False,
                    '国企': False,
                    '次新': False,
                    '老股': False,
                    '非公开多': False,
                    '非公开': False,
                    'H': False,
                    'B': False
                })
            
            result_data.append(row_data)
        
        return pd.DataFrame(result_data)
    
    def calculate_quantile_categories(self, df: pd.DataFrame):
        """基于分位数计算股票分类
        
        Args:
            df: 包含流通值、收盘价和基本面特征的股票DataFrame
        """
        if df.empty or '流通值' not in df.columns or '收盘价' not in df.columns:
            return
        
        # 计算分位数
        market_value_80 = df['流通值'].quantile(0.80)
        market_value_20 = df['流通值'].quantile(0.20)
        price_80 = df['收盘价'].quantile(0.80)
        price_20 = df['收盘价'].quantile(0.20)
        
        # 大市值股票（流通值 > 80分位数）
        self.dsz = df[df['流通值'] > market_value_80]['code'].tolist()
        
        # 小市值股票（流通值 < 20分位数）
        self.xsz = df[df['流通值'] < market_value_20]['code'].tolist()
        
        # 高价股（收盘价 > 80分位数）
        self.gbj = df[df['收盘价'] > price_80]['code'].tolist()
        
        # 低价股（收盘价 < 20分位数）
        self.dbj = df[df['收盘价'] < price_20]['code'].tolist()
        
        # 大高股（大市值且高价）
        self.dg = df[(df['流通值'] > market_value_80) & (df['收盘价'] > price_80)]['code'].tolist()
        
        # 基本面特征分类
        # 国企股（国企字段为True）
        if '国企' in df.columns:
            self.gq = df[df['国企'] == True]['code'].tolist()
        else:
            self.gq = []
        
        # 超强股（超强字段为True）
        if '超强' in df.columns:
            self.cq = df[df['超强'] == True]['code'].tolist()
        else:
            self.cq = []



def zhuli_scores(stocklist):
    """
    根据股票的各项指标计算综合得分，并将最终得分调整到0-2的范围内
    
    评分规则（原始分值）：
    - 超强: 1分 (如果同时有超超强，则不计算此分)
    - 超超强: 1.5分
    - 大高: 0.5分
    - 央企: 1.5分
    - 国企: 1分
    
    返回：
    DataFrame: 包含原始数据和归一化后的得分(0-2范围)的DataFrame
    """
    # 获取数据库连接
    db_conn = DatabaseManager()
    
    # 处理stocks中的代码格式，去掉交易所后缀
    stocklist = [stock.split('.')[0].zfill(6) for stock in stocklist]
    
    # 构建SQL查询，从stock_basic_pro表获取数据
    # 注意：stock_basic_pro表中没有'央企'字段，需要从stock_basic表的ownership_type字段判断
    query = """
    SELECT 
        sbp.stock_code as code,
        sbp.stock_name as name,
        sbp.超强,
        sbp.超超强,
        sbp.大高,
        CASE 
            WHEN sb.ownership_type LIKE '%央企%' OR sb.ownership_type LIKE '%中央企业%' 
            THEN 1 ELSE 0 
        END as 央企,
        sbp.国企
    FROM stock_basic_pro sbp
    LEFT JOIN stock_basic sb ON sbp.stock_code = sb.stock_code
    WHERE sbp.stock_code IN ({})
    """.format(','.join(['?' for _ in stocklist]))
    
    # 执行查询
    df = db_conn.execute_query(query, tuple(stocklist))
    
    if df.empty:
        return pd.DataFrame()
    
    # 将布尔值转换为分值
    # 只有在不是超超强的情况下才计算超强的分值
    df['超强分值'] = ((df['超强'] == 1) & (df['超超强'] == 0)) * 1.0
    df['超超强分值'] = (df['超超强'] == 1) * 1.5
    df['大高分值'] = (df['大高'] == 1) * 0.5
    df['央企分值'] = (df['央企'] == 1) * 1.5
    df['国企分值'] = (df['国企'] == 1) * 1.0
    
    # 计算汇总得分
    df['汇总得分'] = df[['超强分值', '超超强分值', '大高分值', '央企分值', '国企分值']].sum(axis=1)
    
    # 计算最终得分：超过2分的都记为2分
    df['得分'] = df['汇总得分'].clip(upper=2.0)*0.8
    
    # 按得分降序排序
    df_sorted = df.sort_values('得分', ascending=False)
    
    # 只保留需要的列
    final_columns = ['code', 'name', '超强', '超超强', '大高', '央企', '国企', '汇总得分', '得分']
    df_sorted = df_sorted[final_columns]
    
    # 重命名code列为stock_code
    df_sorted.columns = ['stock_code' if col == 'code' else col for col in df_sorted.columns]
    
    return df_sorted


def get_bankuai_stocks(bankuai_name):
    """
    根据板块名称或指数代码获取该板块的所有成分股
    
    Args:
        bankuai_name (str): 板块名称或6位数字指数代码（如 '399001'）
    
    Returns:
        list: 该板块的股票代码列表
    """
    db_conn = DatabaseManager()
    stock_codes = []
    
    try:
        # 检查输入是否为6位数字指数代码
        is_index_code = bankuai_name.isdigit() and len(bankuai_name) == 6
        
        if is_index_code:
            # 如果是指数代码，直接查询相关表
            # 1. 查询通达信板块（通过index_code，支持纯数字匹配）
            tdx_index_query = """
            SELECT DISTINCT stock_code 
            FROM tdx_cfg 
            WHERE index_code = :bankuai_name OR index_code LIKE :bankuai_name_pattern
            """
            tdx_df = db_conn.execute_query(tdx_index_query, {"bankuai_name": bankuai_name, "bankuai_name_pattern": f"{bankuai_name}%"})
            if not tdx_df.empty:
                stock_codes.extend(tdx_df['stock_code'].tolist())
            
            # 2. 查询申万板块（通过l1_code, l2_code, l3_code的前6位数字匹配）
            sw_index_query = """
            SELECT DISTINCT stock_code 
            FROM sw_cfg 
            WHERE l1_code LIKE :pattern OR l2_code LIKE :pattern OR l3_code LIKE :pattern
            """
            pattern = f"{bankuai_name}.%"
            sw_df = db_conn.execute_query(sw_index_query, {"pattern": pattern})
            if not sw_df.empty:
                stock_codes.extend(sw_df['stock_code'].tolist())
            
            # 3. 查询股票分类映射表（通过index_code）
            mapping_query = """
            SELECT DISTINCT stock_code 
            FROM stock_category_mapping 
            WHERE index_code = :bankuai_name OR index_code = :bankuai_name_zs
            """
            # 尝试两种格式：纯数字和带后缀的格式
            mapping_df = db_conn.execute_query(mapping_query, {"bankuai_name": bankuai_name, "bankuai_name_zs": f"{bankuai_name}.ZS"})
            if not mapping_df.empty:
                stock_codes.extend(mapping_df['stock_code'].tolist())
        
        else:
            # 如果是板块名称，使用原有逻辑
            # 1. 查询通达信板块
            tdx_query = """
            SELECT DISTINCT stock_code 
            FROM tdx_cfg 
            WHERE industry_name = :bankuai_name
            """
            tdx_df = db_conn.execute_query(tdx_query, {"bankuai_name": bankuai_name})
            if not tdx_df.empty:
                stock_codes.extend(tdx_df['stock_code'].tolist())
        
        # 2. 查询大概念板块
        concept_mapping = {
            '国企': '国企',
            'B股': 'B股', 
            'H股': 'H股',
            '老股': '老股',
            '次新': '次新'
        }
        
        if bankuai_name in concept_mapping:
            concept_field = concept_mapping[bankuai_name]
            concept_query = f"""
            SELECT stock_code 
            FROM stock_basic_pro 
            WHERE {concept_field} = 1
            """
            concept_df = db_conn.execute_query(concept_query)
            if not concept_df.empty:
                stock_codes.extend(concept_df['stock_code'].tolist())
        
        # 3. 查询申万板块（支持模糊匹配）
        # 构建模糊匹配的查询条件，匹配去掉罗马数字后缀的板块名称
        sw_query = """
        SELECT DISTINCT stock_code 
        FROM sw_cfg_hierarchy 
        WHERE 
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l1_name, 'I', ''), 'II', ''), 'III', ''), 'Ⅳ', ''), 'Ⅴ', ''), 'Ⅵ', '') = :bankuai_name
            OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l2_name, 'I', ''), 'II', ''), 'III', ''), 'Ⅳ', ''), 'Ⅴ', ''), 'Ⅵ', '') = :bankuai_name
            OR REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l3_name, 'I', ''), 'II', ''), 'III', ''), 'Ⅳ', ''), 'Ⅴ', ''), 'Ⅵ', '') = :bankuai_name
        """
        sw_df = db_conn.execute_query(sw_query, {"bankuai_name": bankuai_name})
        if not sw_df.empty:
            stock_codes.extend(sw_df['stock_code'].tolist())
        
        # 4. 查询地域板块
        province_query = """
        SELECT stock_code 
        FROM stock_basic 
        WHERE province = :bankuai_name
        """
        province_df = db_conn.execute_query(province_query, {"bankuai_name": bankuai_name})
        if not province_df.empty:
            stock_codes.extend(province_df['stock_code'].tolist())
        
        # 去重并返回
        return list(set(stock_codes))
        
    except Exception as e:
        print(f"查询板块 {bankuai_name} 成分股失败: {e}")
        return []
    finally:
        pass


def get_sector_name(stock_code):
    """
    确定股票所属板块的功能
    
    Args:
        stock_code (str): 股票代码，如 '000001'
    
    Returns:
        dict: 包含四类板块信息的字典
            {
                '通达信': [list],  # 通达信板块名称列表
                '大概念': [list],  # 大概念板块名称列表  
                '申万板块': [list], # 申万板块名称列表
                '地域': [list]     # 地域信息列表
            }
    """
    # 获取数据库连接
    db_conn = DatabaseManager()
    
    # 标准化股票代码格式
    stock_code = stock_code.split('.')[0].zfill(6)
    
    result = {
        '通达信': [],
        '大概念': [],
        '申万板块': [],
        '地域': []
    }
    
    try:
        # 1. 查询通达信板块 (tdx_cfg表)
        tdx_query = """
        SELECT DISTINCT industry_name 
        FROM tdx_cfg 
        WHERE stock_code = ?
        """
        tdx_df = db_conn.execute_query(tdx_query, (stock_code,))
        if not tdx_df.empty:
            result['通达信'] = tdx_df['industry_name'].tolist()
        
        # 2. 查询大概念板块 (stock_basic_pro表)
        # 检查国企、B股、H股、老股、次新字段，值为1就是属于该板块
        concept_query = """
        SELECT 国企, B股, H股, 老股, 次新
        FROM stock_basic_pro 
        WHERE stock_code = ?
        """
        concept_df = db_conn.execute_query(concept_query, (stock_code,))
        if not concept_df.empty:
            concept_row = concept_df.iloc[0]
            if concept_row.get('国企') == 1:
                result['大概念'].append('国企')
            if concept_row.get('B股') == 1:
                result['大概念'].append('B股')
            if concept_row.get('H股') == 1:
                result['大概念'].append('H股')
            if concept_row.get('老股') == 1:
                result['大概念'].append('老股')
            if concept_row.get('次新') == 1:
                result['大概念'].append('次新')
        
        # 3. 查询申万板块 (sw_cfg_hierarchy表)
        # 获取l1_name, l2_name, l3_name三个字段
        sw_query = """
        SELECT DISTINCT l1_name, l2_name, l3_name
        FROM sw_cfg_hierarchy 
        WHERE stock_code = ?
        """
        sw_df = db_conn.execute_query(sw_query, (stock_code,))
        if not sw_df.empty:
            sw_sectors = []
            for _, row in sw_df.iterrows():
                l1_name = row.get('l1_name')
                l2_name = row.get('l2_name')
                l3_name = row.get('l3_name')
                
                if pd.notna(l1_name) and str(l1_name).strip() != '':
                    sw_sectors.append(str(l1_name))
                if pd.notna(l2_name) and str(l2_name).strip() != '':
                    sw_sectors.append(str(l2_name))
                if pd.notna(l3_name) and str(l3_name).strip() != '':
                    sw_sectors.append(str(l3_name))
            # 去重
            result['申万板块'] = list(set(sw_sectors))
        
        # 4. 查询地域信息 (stock_basic表的province字段)
        province_query = """
        SELECT province
        FROM stock_basic 
        WHERE stock_code = ?
        """
        province_df = db_conn.execute_query(province_query, (stock_code,))
        if not province_df.empty:
            province_value = province_df.iloc[0]['province']
            if pd.notna(province_value) and str(province_value).strip():
                result['地域'].append(str(province_value))
        
        return result
        
    except Exception as e:
        print(f"查询股票 {stock_code} 板块信息失败: {e}")
        return result
    finally:
        # 数据库连接会在DatabaseConnection类中自动管理
        pass


#寻找多个板块的交集股
def jiaoji(*lists):
    # 过滤掉空列表
    non_empty_lists = [lst for lst in lists if lst]
    
    # 如果没有非空列表，返回空列表
    if not non_empty_lists:
        return []
    
    # 如果只有一个非空列表，返回该列表的去重结果
    if len(non_empty_lists) == 1:
        return list(set(non_empty_lists[0]))
    
    # 对多个非空列表求交集
    result = set(non_empty_lists[0])
    for lst in non_empty_lists[1:]:
        result &= set(lst)
    
    return list(result)



def bankuai_scores(stocklist, *bankuai_names):
    """
    计算股票在多个板块中的得分
    
    参数：
    stocklist: list, 股票代码列表
    bankuai_names: tuple, 多个板块名称
    
    返回：
    DataFrame: 包含股票代码、名称和板块得分的DataFrame
    """
    # 处理stocks中的代码格式，去掉交易所后缀
    stocklist = [stock.split('.')[0].zfill(6) for stock in stocklist]
    
    # 创建结果DataFrame，包含股票代码和名称
    # 从数据库获取股票名称
    db_conn = DatabaseManager()
    try:
        # 构建查询语句获取股票名称
        placeholders = ','.join(['?' for _ in stocklist])
        name_query = f"""
        SELECT stock_code, stock_name 
        FROM stock_basic 
        WHERE stock_code IN ({placeholders})
        """
        name_df = db_conn.execute_query(name_query, tuple(stocklist))
        
        # 创建基础DataFrame
        df = pd.DataFrame({'stock_code': stocklist, 'name': [''] * len(stocklist)})
        
        # 合并股票名称
        if not name_df.empty:
            name_mapping = dict(zip(name_df['stock_code'], name_df['stock_name']))
            df['name'] = df['stock_code'].map(name_mapping).fillna('')
    except Exception as e:
        print(f"获取股票名称失败: {e}")
        # 如果获取名称失败，创建基础的DataFrame
        df = pd.DataFrame({'stock_code': stocklist, 'name': [''] * len(stocklist)})
    finally:
        pass
    
    # 为每个板块创建一个列
    for bankuai in bankuai_names:
        # 获取板块成分股
        bankuai_stocks = get_bankuai_stocks(bankuai)
        # 处理板块股票代码格式
        bankuai_stocks = [stock.split('.')[0].zfill(6) for stock in bankuai_stocks]
        # 创建布尔列，判断每只股票是否属于该板块
        df[bankuai] = df['stock_code'].astype(str).isin(bankuai_stocks)
    
    # 计算总得分（每个True得1分，最高不超过3分）
    df['原始得分'] = df[list(bankuai_names)].sum(axis=1)
    df['得分'] = df['原始得分'].clip(upper=3)  # 最高不超过3分
    
    # 按得分降序排序
    df_sorted = df.sort_values('得分', ascending=False)
    
    return df_sorted


# 假设 zhibiao 和 get_stock_name 是您已经定义好的函数
# from your_library import zhibiao, get_stock_name

def get_stocks_with_low_bias_high_4d_growth(stocklist, end_date):
    """
    获取指定股票列表中，120日BIAS偏中低，且最近4日涨幅相对偏高的股票。

    参数:
    stocklist (list): 股票代码列表。
    end_date (str): 数据获取的截止日期，格式如 'YYYY-MM-DD'。

    返回:
    pandas.DataFrame: 包含'code', 'name', '120BIAS', 'growth_rate_4d'的DataFrame。
    """
    from data_management.data_processor import get_daily_data_for_backtest
    from core.utils.indicators import MA
    
    # 创建用于存储结果的列表
    results = []
    
    for stock in stocklist:
        try:
            # 1. 获取股票数据
            df_daily = get_daily_data_for_backtest(stock, end_date)
            if df_daily is None:
                print(f"股票 {stock} 数据获取失败，跳过")
                continue
            if df_daily.empty:
                print(f"股票 {stock} 数据为空，跳过")
                continue
            
            # 2. 数据有效性检查：确保有足够数据计算4日涨幅和120日BIAS
            if len(df_daily) < 120:  # 需要至少120天数据计算120日BIAS
                print(f"股票 {stock} 数据不足120天，无法计算120日BIAS，已跳过。")
                continue
            
            # 3. 计算120日BIAS
            close_prices = df_daily['close']
            ma_120 = MA(close_prices, 120)
            bias_120_value = (close_prices.iloc[-1] - ma_120[-1]) / ma_120[-1] * 100
            
            # 检查BIAS值是否有效
            if pd.isna(bias_120_value):
                print(f"股票 {stock} 的BIAS_120值为空，跳过")
                continue
            
            # 4. 计算最近4个交易日的涨幅
            if len(df_daily) < 5:
                print(f"股票 {stock} 数据不足5天，无法计算4日涨幅，已跳过。")
                continue
                
            end_price = df_daily['close'].iloc[-1]
            start_price_4d_ago = df_daily['close'].iloc[-5]
            
            # 避免除以零的错误
            if start_price_4d_ago == 0:
                growth_rate = 0.0
            else:
                growth_rate = (end_price - start_price_4d_ago) / start_price_4d_ago * 100
            
            # 5. 获取股票名称（简化处理，直接使用股票代码）
            stock_name = stock
            
            # 6. 添加到结果列表
            results.append({
                'code': stock,
                'name': stock_name,
                '120BIAS': bias_120_value,
                'growth_rate_4d': growth_rate
            })
            
        except Exception as e:
            print(f"处理股票 {stock} 时发生错误: {e}")
            continue
    
    # 转换为DataFrame
    result_df = pd.DataFrame(results)
    
    if result_df.empty:
        print("没有获取到有效的数据")
        return result_df
    
    # 筛选条件：BIAS偏中低且4日涨幅相对偏高
    # BIAS偏中低：小于50%分位数
    # 4日涨幅相对偏高：大于50%分位数
    bias_threshold = result_df['120BIAS'].quantile(0.4)
    growth_threshold = result_df['growth_rate_4d'].quantile(0.6)
    
    filtered_df = result_df[
        (result_df['120BIAS'] < bias_threshold) & 
        (result_df['growth_rate_4d'] > growth_threshold)
    ]
    
    print(f"筛选条件：BIAS < {bias_threshold:.2f}, 4日涨幅 > {growth_threshold:.2f}%")
    print(f"符合条件的股票数量: {len(filtered_df)}")
    
    return filtered_df

# --- 使用示例 ---
# 假设您有以下股票列表和截止日期
# stock_list_example = ['000001', '600519', '300750'] # 示例股票池
# today_date_example = '2023-10-27' # 示例日期

# 调用函数
# final_results = get_stocks_with_low_bias_high_4d_growth(stock_list_example, today_date_example)

# 排序并显示结果：按4日涨幅降序排列
# if not final_results.empty:
#     final_results_sorted = final_results.sort_values(by='growth_rate_4d', ascending=False)
#     print("\n--- 最终筛选结果 (按4日涨幅排序) ---")
#     print(final_results_sorted)

#针对对get_stocks_with_low_bias_high_4d_growth函数筛选出来的股票进行得分计算,给予1分
def get_stocks_with_low_bias_high_4d_growth_scores(stocklist, end_date):
    """
    对get_stocks_with_low_bias_high_4d_growth函数筛选出来的股票进行得分计算
    
    参数:
    stocklist (list): 股票代码列表
    end_date (str): 数据获取的截止日期，格式如 'YYYY-MM-DD'
    
    返回:
    pandas.DataFrame: 包含股票代码、名称和得分的DataFrame
    """
    # 首先获取符合条件的股票
    filtered_stocks = get_stocks_with_low_bias_high_4d_growth(stocklist, end_date)
    
    if filtered_stocks.empty:
        print("没有符合条件的股票")
        return pd.DataFrame(columns=['code', 'name', 'low_bias_high_growth_score'])
    
    # 为符合条件的股票给予1分
    filtered_stocks['low_bias_high_growth_score'] = 1.0
    
    # 只返回需要的列
    result_df = filtered_stocks[['code', 'name', 'low_bias_high_growth_score']].copy()
    
    print(f"低BIAS高涨幅股票得分计算完成，共 {len(result_df)} 只股票获得1分")
    print("股票列表:")
    for _, row in result_df.iterrows():  # type: ignore
        print(f"  {row['code']} - {row['name']} - 得分: {row['low_bias_high_growth_score']}")
    
    return result_df

def total_scores(stocklist, *bankuai_names, date=None):
    """
    汇总技术面、基本面和板块的综合得分
    
    参数：
    stocklist: list, 股票代码列表
    bankuai_names: tuple, 板块名称列表
    date: str, 日期字符串，默认为None
    
    返回：
    DataFrame: 包含所有得分的汇总结果
    """
    try:
        # 导入技术分析器
        from core.technical_analyzer.stock_technical_analyzer import StockTechnicalAnalyzer
        
        # 创建技术分析器实例
        analyzer = StockTechnicalAnalyzer()
        
        # 获取三个维度的得分
        if date:
            # 使用StockTechnicalAnalyzer获取技术面得分
            jishu_df = analyzer.get_jishu_scores(stocklist, date)
        else:
            # 如果没有提供日期，创建空的技术面得分
            jishu_df = pd.DataFrame({'stock_code': stocklist, 'stock_name': [''] * len(stocklist), 'total_score': [0] * len(stocklist)})
        
        zhuli_df = zhuli_scores(stocklist)         # 主力得分
        bankuai_df = bankuai_scores(stocklist, *bankuai_names)  # 板块得分
        low_bias_df = get_stocks_with_low_bias_high_4d_growth_scores(stocklist, date) if date else pd.DataFrame()  # 低BIAS高涨幅得分
        
        # 创建基础DataFrame
        merged_df = pd.DataFrame({'stock_code': stocklist})
        
        # 合并技术面得分
        if not jishu_df.empty and 'total_score' in jishu_df.columns:
            jishu_cols = ['stock_code', 'total_score']
            if 'stock_name' in jishu_df.columns:
                jishu_cols.append('stock_name')
            jishu_detail = jishu_df[jishu_cols].copy()
            merged_df = merged_df.merge(jishu_detail, on='stock_code', how='left')
        else:
            merged_df['total_score'] = 0.0
            merged_df['stock_name'] = ''
        
        # 合并主力得分
        if not zhuli_df.empty and '得分' in zhuli_df.columns:
            zhuli_cols = ['stock_code', '得分']
            if 'name' in zhuli_df.columns:
                zhuli_cols.append('name')
            zhuli_detail = zhuli_df[zhuli_cols].copy()
            # 重命名列以避免冲突
            if 'name' in zhuli_detail.columns:
                zhuli_detail = zhuli_detail.rename(columns={'得分': 'zhuli_score', 'name': 'zhuli_name'})
            else:
                zhuli_detail = zhuli_detail.rename(columns={'得分': 'zhuli_score'})
            merged_df = merged_df.merge(zhuli_detail, on='stock_code', how='left')
        else:
            merged_df['zhuli_score'] = 0.0
        
        # 合并板块得分
        if not bankuai_df.empty and '得分' in bankuai_df.columns:
            bankuai_cols = ['stock_code', '得分']
            if 'name' in bankuai_df.columns:
                bankuai_cols.append('name')
            bankuai_detail = bankuai_df[bankuai_cols].copy()
            # 重命名列以避免冲突
            if 'name' in bankuai_detail.columns:
                bankuai_detail = bankuai_detail.rename(columns={'得分': 'bankuai_score', 'name': 'bankuai_name'})
            else:
                bankuai_detail = bankuai_detail.rename(columns={'得分': 'bankuai_score'})
            merged_df = merged_df.merge(bankuai_detail, on='stock_code', how='left')
        else:
            merged_df['bankuai_score'] = 0.0
        
        # 合并低BIAS高涨幅得分
        if not low_bias_df.empty and 'low_bias_high_growth_score' in low_bias_df.columns:
            low_bias_cols = ['code', 'low_bias_high_growth_score']
            if 'name' in low_bias_df.columns:
                low_bias_cols.append('name')
            low_bias_detail = low_bias_df[low_bias_cols].copy()
            # 重命名列以匹配stock_code
            low_bias_detail = low_bias_detail.rename(columns={'code': 'stock_code', 'low_bias_high_growth_score': 'low_bias_score'})
            if 'name' in low_bias_detail.columns:
                low_bias_detail = low_bias_detail.rename(columns={'name': 'low_bias_name'})
            merged_df = merged_df.merge(low_bias_detail, on='stock_code', how='left')
        else:
            merged_df['low_bias_score'] = 0.0
        
        # 填充缺失值
        merged_df['total_score'] = merged_df['total_score'].fillna(0.0)
        merged_df['zhuli_score'] = merged_df['zhuli_score'].fillna(0.0)
        merged_df['bankuai_score'] = merged_df['bankuai_score'].fillna(0.0)
        merged_df['low_bias_score'] = merged_df['low_bias_score'].fillna(0.0)
        
        # 使用技术面的股票名称作为主要名称
        if 'stock_name' in merged_df.columns:
            merged_df['name'] = merged_df['stock_name'].fillna('')
        elif 'zhuli_name' in merged_df.columns:
            merged_df['name'] = merged_df['zhuli_name'].fillna('')
        elif 'bankuai_name' in merged_df.columns:
            merged_df['name'] = merged_df['bankuai_name'].fillna('')
        else:
            merged_df['name'] = ''
        
        # 重命名列以更清晰地显示各个得分
        merged_df.rename(columns={
            'total_score': '技术得分',
            'zhuli_score': '主力得分',
            'bankuai_score': '板块得分',
            'low_bias_score': '低BIAS得分'
        }, inplace=True)
        
        # 计算总得分（四个维度的总和）
        merged_df['总得分'] = (merged_df['技术得分'] + merged_df['主力得分'] + merged_df['板块得分'] + merged_df['低BIAS得分'])
        
        # 将总得分四舍五入到两位小数
        merged_df['总得分'] = merged_df['总得分'].round(2)
        
        # 按总得分降序排序
        final_df_sorted = merged_df.sort_values('总得分', ascending=False)
        
        # 重新排列列的顺序，使其更有逻辑性
        columns_order = ['stock_code', 'name', '总得分', '技术得分', '主力得分', '板块得分', '低BIAS得分']
        final_df_sorted = final_df_sorted[columns_order]
        
        return final_df_sorted
        
    except Exception as e:
        print(f"汇总得分计算失败: {e}")
        # 返回基础DataFrame
        return pd.DataFrame({
            'stock_code': stocklist,
            'name': [''] * len(stocklist),
            '总得分': [0] * len(stocklist),
            '技术得分': [0] * len(stocklist),
            '主力得分': [0] * len(stocklist),
            '板块得分': [0] * len(stocklist),
            '低BIAS得分': [0] * len(stocklist)
        })


#获取高bias且主力强的股票，暂不使用。
def get_high_bias_stocks(stock_list, date):
    """
    获取高BIAS股票列表
    """
    from data_management.data_processor import get_daily_data_for_backtest
    from core.utils.indicators import BIAS, MA
    
    bias_results = []
    
    for stock in stock_list:
        try:
            # 获取股票数据
            df_daily = get_daily_data_for_backtest(stock, date)
            if df_daily is None:
                print(f"股票 {stock} 数据获取失败，跳过")
                continue
            if df_daily.empty:
                print(f"股票 {stock} 数据为空，跳过")
                continue
            
            # 检查数据是否包含必要的列
            if 'close' not in df_daily.columns:
                print(f"股票 {stock} 数据中缺少close列，跳过")
                continue
            
            # 直接计算BIAS_120
            close_prices = df_daily['close']
            
            # 计算120日移动平均线
            ma_120 = MA(close_prices, 120)
            
            # 计算BIAS_120 = (收盘价 - 120日均线) / 120日均线 * 100
            # MA函数返回numpy数组，使用[-1]索引获取最后一个值
            bias_120_value = (close_prices.iloc[-1] - ma_120[-1]) / ma_120[-1] * 100
            
            # 检查BIAS值是否有效
            if pd.isna(bias_120_value):
                print(f"股票 {stock} 的BIAS_120值为空，跳过")
                continue
            
            bias_results.append({
                'stock_code': stock,
                '120BIAS': bias_120_value
            })
            
        except Exception as e:
            print(f"计算股票 {stock} 的120BIAS失败: {e}")
            continue
    
    # 转换为DataFrame
    result_df = pd.DataFrame(bias_results)
    
    if result_df.empty:
        print("没有获取到有效的BIAS数据")
        return []
    
    # 筛选高BIAS股票
    bias_condition1 = result_df['120BIAS'] > result_df['120BIAS'].quantile(0.75)
    bias_condition3 = (result_df['120BIAS'] < 80) & (result_df['120BIAS'] > 3)
    tiaojian_bias = bias_condition1 & bias_condition3
    
    gao_bias_stocks = result_df[tiaojian_bias]['stock_code'].tolist()
    print("高BIAS股票:", gao_bias_stocks)
    print("高BIAS股票详情:")
    print(result_df[tiaojian_bias])

    # 获取高BIAS股票的主力强的。用stockxihua类的 calculate_quantile_categories(self, df: pd.DataFrame)，来获取cq,dg,xsz,gbj,dbj,dg的股票。
    print("\n开始分析高BIAS股票的主力强度...")
    
    # 获取高BIAS股票的基本面数据
    stock_xihua = StockXihua()
    
    # 为高BIAS股票获取基本面数据
    if gao_bias_stocks:
        bias_stocks_data = stock_xihua.create_stock_dataframe(gao_bias_stocks)
        
        if not bias_stocks_data.empty:
            # 计算分位数分类
            stock_xihua.calculate_quantile_categories(bias_stocks_data)
            
            # 获取各类股票
            cq_stocks = stock_xihua.cq  # 超强股
            dg_stocks = stock_xihua.dg  # 大高股
            # xsz_stocks = stock_xihua.xsz  # 小市值股
            gbj_stocks = stock_xihua.gbj  # 高价股
            # dbj_stocks = stock_xihua.dbj  # 低价股
            # gq_stocks = stock_xihua.gq  # 国企股
            
            print(f"超强股(cq): {len(cq_stocks)}只 - {cq_stocks}")
            print(f"大高股(dg): {len(dg_stocks)}只 - {dg_stocks}")
            print(f"高价股(gbj): {len(gbj_stocks)}只 - {gbj_stocks}")
            
            # 找出高BIAS且主力强的股票（超强股或大高股）
            strong_bias_stocks = list(set(cq_stocks + dg_stocks + gbj_stocks))
            print(f"\n高BIAS且主力强的股票: {len(strong_bias_stocks)}只 - {strong_bias_stocks}")
            
            return strong_bias_stocks
        else:
            print("未能获取到高BIAS股票的基本面数据")
            return gao_bias_stocks
    else:
        print("没有高BIAS股票")
        return []
    
    return strong_bias_stocks
    


def stock_codes_to_dataframe(stock_codes):
    """
    将股票代码列表转换为包含股票代码和名称的DataFrame
    
    Args:
        stock_codes: 股票代码列表
        
    Returns:
        pd.DataFrame: 包含stock_code和stock_name列的DataFrame
    """
    if not stock_codes:
        return pd.DataFrame(columns=['stock_code', 'stock_name'])
    
    try:
        # 获取数据库连接
        db_conn = DatabaseManager()
        
        # 标准化股票代码格式（去掉交易所后缀，补齐6位）
        normalized_codes = [code.split('.')[0].zfill(6) for code in stock_codes]
        
        # 构建查询语句
        placeholders = ','.join(['?' for _ in normalized_codes])
        query = f"""
        SELECT stock_code, stock_name 
        FROM stock_basic 
        WHERE stock_code IN ({placeholders})
        """
        
        # 执行查询
        result_df = db_conn.execute_query(query, tuple(normalized_codes))
        
        if result_df.empty:
            print(f"警告: 未找到任何股票信息")
            return pd.DataFrame(columns=['stock_code', 'stock_name'])
        
        # 确保列名正确
        if 'stock_code' not in result_df.columns or 'stock_name' not in result_df.columns:
            print(f"警告: 查询结果列名不正确: {list(result_df.columns)}")
            return pd.DataFrame(columns=['stock_code', 'stock_name'])
        
        print(f"成功获取 {len(result_df)} 只股票的名称信息")
        return result_df[['stock_code', 'stock_name']]
        
    except Exception as e:
        print(f"获取股票名称失败: {e}")
        # 如果失败，返回只有股票代码的DataFrame
        return pd.DataFrame({
            'stock_code': normalized_codes,
            'stock_name': [''] * len(normalized_codes)
        })


# 3. 编写一个函数来自动保存字典里的所有DataFrame
def save_pools_to_csv(pools_dict):
    """
    遍历一个字典，将每个DataFrame保存为CSV文件。
    CSV的文件名来自于字典的键。
    
    Args:
        pools_dict: 包含DataFrame的字典
    """
    print("\n--- 开始自动生成CSV文件 ---")
    
    # 创建目标文件夹（如果不存在）
    output_dir = '操作板块'
    os.makedirs(output_dir, exist_ok=True)
    print(f"输出目录: {output_dir}")
    
    # 检查文件夹中是否已有CSV文件
    existing_csv_files = []
    if os.path.exists(output_dir):
        for file in os.listdir(output_dir):
            if file.endswith('.csv'):
                existing_csv_files.append(file)
    
    if existing_csv_files:
        print(f"⚠️ 发现文件夹中已存在 {len(existing_csv_files)} 个CSV文件:")
        for csv_file in existing_csv_files:
            print(f"    - {csv_file}")
        print("❌ 跳过保存操作，避免覆盖现有文件")
        return
    else:
        print("✅ 文件夹为空，开始保存新文件...")
    
    # 遍历字典的键和值
    for pool_name, df_data in pools_dict.items():
        # pool_name 是 'houxuan_mmm', 'houxuan_youse_xiaojinshu' 等字符串
        # df_data 是对应的DataFrame
        
        if df_data.empty:
            print(f"⚠️ 跳过空的股票池: {pool_name}")
            continue
        
        # 动态生成文件名
        file_path = f'{output_dir}/{pool_name}.csv'
        
        try:
            # 保存文件
            df_data.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"✅ 成功保存文件: {file_path} (包含 {len(df_data)} 只股票)")
            
            # 显示前几只股票作为示例
            if len(df_data) > 0:
                print(f"   示例股票: {df_data.head(3).to_dict('records')}")
                
        except Exception as e:
            print(f"❌ 保存文件失败 {file_path}: {e}")

if __name__ == "__main__":
    # 测试通过6位数字指数代码查询板块成分股
    test_cases = [
        ("801010", "申万农林牧渔"),
        ("801170", "申万交通运输"),
        # ("100001", "国企指数"),
        # ("399001", "深证成指"),
    ]
    
    print("=== 测试通过6位数字查询板块成分股功能 ===\n")
    
    for code, name in test_cases:
        print(f"🔍 测试 {code} ({name}):")
        stocks = get_bankuai_stocks(code)
        if stocks:
            print(f"✅ 找到 {len(stocks)} 只成分股")
            print(f"   前10只: {stocks[:10]}")
        else:
            print("⚠️  未找到成分股")
        print()
    
    # 测试板块名称查询（确保原功能正常）
    print("🔍 测试板块名称查询:")
    name_stocks = get_bankuai_stocks("银行")
    if name_stocks:
        print(f"✅ 银行板块找到 {len(name_stocks)} 只成分股")
        print(f"   前5只: {name_stocks[:5]}")
    else:
        print("⚠️  银行板块未找到成分股")
    
    print("\n✅ 测试完成！")