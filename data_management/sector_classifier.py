#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
板块分类器 (quant_v2 版本)

功能：
1. 板块分类和筛选
2. 获取申万板块层次结构数据
3. 板块分类管理
"""

import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import numpy as np
import sqlite3
import re
from typing import Dict, List, Any, Optional
from data_management.database_manager import DatabaseManager
# 从sector_screener模块导入相关函数
from applications.sector_screener import get_boduan_zf_bankuai, get_changxian_zf_bankuai, get_boduan_bias_bankuai, get_qualified_sector_codes


# from sector_high_breakout_zhixin import main11



#特殊板块：(循环板块不需要,因为有直接针对csv文件，长线强趋势板块，消息博弈，基本面板块都放在字典里面...主力强板块单独调用)
#长线循环居前的板块
changxian_bankuai = get_changxian_zf_bankuai()
#最近3个波段循环居前的板块
boduan_zf_bankuai = get_boduan_zf_bankuai()
#最近3个波段循环居前的板块
boduan_bias_bankuai = get_boduan_bias_bankuai()
#长线强趋势板块。
cxqqs_bk = []
#近期消息博弈基本面的板块
jinqi_xiaoxi_bk = []
#主力强的板块
zhuli_bankuai = get_qualified_sector_codes()  
#近期1波明显增量板块。通过函数获取后，可以固定下来
#jinqi_1bzl_bankuai = main11()
#print(jinqi_1bzl_bankuai)
jinqi_1bzl_bankuai = ['801083.ZS', '801726.ZS', '801764.ZS', '801053.ZS', '801050.ZS', '801017.ZS', '801993.ZS', '801737.ZS', '801038.ZS', '801181.ZS', '801116.ZS']
#1,调用


#小盘小市值（小市值，新股次新），权重大市值（H股，中字头，央企，地方垄断国企，名字中华龙），低价（ST，重组，老股/上海北京四川B股，B股，超跌），高比价（），地域（各种地域，及地域概念），


#=================================================
# 将标准的行业分类（如申万）根据自己的投资逻辑或分析维度进行重组，关键步骤：创建映射文件
# 组织结构： 创建一个名为 xinfenlei.csv 的文件，至少包含以下几列：
# sw_code  sw_name      sw_xin_code      xinfenlei_code    xinfenlei_name
# 801780.SI    银行I         801780.ZS              01          quanzhong
# 801790.SI    非银金融I      801790.ZS              01          quanzhong
# 801180.SI   房地产开发II    801180.ZS              02          dijia
# 申万一级二级三级行业的代码和名称。
# sw_xin_code: 为了后期调用申万板块指数，和申万板块指数进行映射。
# xinfenlei_code    xinfenlei_name 您自定义的聚类板块的代码和名称。

# 量化分析中的使用： 在进行分析时，首先获取所有股票对应的申万行业分类（sw_cfg_hierarchy），然后将这个表格与您的自定义映射表进行合并（Merge/Join），这样每只股票就有了一个新的xinfenlei_name标签，之后就可以基于这个新标签进行 groupby 聚合分析了。

# quant_system，查找sw_cfg_hierarchy
#按照我下面的分类，创建一个映射文件，然后和sw_cfg_hierarchy进行合并，这样每只股票就有了一个新的xinfenlei_name标签，之后就可以基于这个新标签进行 groupby 聚合分析了。
#然后按照这个映射文件，创建一个聚类板块的代码和名称。

# 自定义分类映射数据
CUSTOM_CLASSIFICATION_MAPPING = {
    'quanzhong': {
        'code': '01',
        'name': '权重板块',
        'industries': [
            '银行I', '股份制银行II', '城商行II', '农商行II', '国有大型银行II',
            '股份制银行III', '城商行III', '农商行III', '国有大型银行III',
            '油气开采II', '电信运营商III', '非银金融I', '证券II', '多元金融II', '保险II',
            '金融控股III', '保险III', '其他多元金融III', '期货III', '金融信息服务III',
            '白酒II', '核力发电III'
        ]
    },
    'feiquanzhong': {
        'code': '02', 
        'name': '非权重板块',
        'industries': [
            '煤炭I', '煤炭开采II', '焦炭II', '有色金属I', '工业金属II', '能源金属II',
            '黄金II', '金属非金属新材料II', '稀有金属II', '交通运输I', '航运港口II',
            '航空机场II', '铁路公路II', '物流II', '港口III', '机场III', '航空运输III',
            '高速公路III', '航运III', '电力II', '燃气II', '石油石化I', '炼化及贸易II',
            '油气开采II', '油服工程II', '钢铁I', '冶钢原料II', '特钢II', '普钢II'
        ]
    },
    'dijia': {
        'code': '03',
        'name': '低价板块', 
        'industries': [
            '房地产I', '房地产开发II', '房地产服务II', '房地产开发III', '商业地产III',
            '物业管理III', '房产租赁经纪III', '产业地产III', '房地产综合服务III',
            '商业贸易I', '一般零售II', '贸易II', '专业零售II', '百货III', '超市III',
            '专业连锁III', '农林牧渔I', '农产品加工II', '林业II', '饲料II', '种植业II',
            '畜禽养殖II', '渔业II', '农业综合II', '水电III', '汽车零部件II',
            '底盘与发动机系统III', '化工I', '化学纤维II', '农化制品II', '化学原料II',
            '化学制品II', '塑料II', '橡胶II', '非金属材料II', '轻工制造I', '造纸II',
            '包装印刷II', '家用轻工II'
        ]
    },
    'zhongxiao': {
        'code': '04',
        'name': '中小盘板块',
        'industries': [
            '计算机I', '软件开发II', 'IT服务II', '计算机设备II', '横向通用软件III',
            'IT服务III', '其他计算机设备III', '垂直应用软件III', '安防设备III',
            '互联网电商II', '综合电商III', '电商服务III', '跨境电商III', '电子I',
            '光学光电子II', '电子制造II', '其他电子II', '元件II', '半导体II',
            '电子化学品II', '显示器件III', '军工电子II', '被动元件III',
            '数字芯片设计III', '光学元件III', '集成电路封测III', '半导体材料III',
            'LEDIII', '半导体设备III', '电子化学品III', '分立器件III',
            '集成电路制造III', '模拟芯片设计III', '通信I', '通信设备II', '磁性材料III'
        ]
    },
    'lengmen': {
        'code': '05',
        'name': '冷门板块',
        'industries': [
            '环保I', '环境治理II', '环保设备II'
        ]
    },
    'diyuhangye': {
        'code': '06',
        'name': '地域行业板块',
        'industries': [
            '建筑装饰I', '基础建设II', '专业工程II', '房屋建设II', '工程咨询服务II',
            '装修装饰II', '基建市政工程III', '运输设备II', '工程机械II', '建筑材料I',
            '玻璃制造II', '其他建材II', '水泥制造II', '航运港口II', '房地产开发II',
            '特钢II', '普钢II'
        ]
    },
    'zhineng': {
        'code': '07',
        'name': '智能板块',
        'industries': [
            '自动化设备II', '工控设备III', '机器人III', '激光设备III', '其他自动化设备III', '电机II'
        ]
    },
    'mingpai': {
        'code': '08',
        'name': '名牌消费板块',
        'industries': [
            '家用电器I', '白色家电II', '家电零部件II', '照明设备II', '视听器材II',
            '小家电II', '厨卫电器II', '其他家电II', '纺织服装I', '饰品II', '纺织制造II',
            '服装家纺II', '商用车II', '乘用车II', '休闲服务I', '酒店餐饮II', '旅游及景区II',
            '教育II', '专业服务II', '体育II', '食品饮料I', '食品加工II', '白酒II',
            '休闲食品II', '非白酒II', '饮料乳品II', '调味发酵品II', '品牌消费电子III',
            '美容护理I', '医疗美容II', '个护用品II', '化妆品II'
        ]
    },
    'qiangqushi': {
        'code': '09',
        'name': '强势板块',
        'industries': [
            '自动化设备II', '机器人III', '电机III', '软件开发II', '产业地产III',
            '印制电路板III', '锂III', '黄金III', '钨III', '磁性材料III', '稀土III',
            '通信设备II', '半导体II', '底盘与发动机系统III','国有大型银行II', '证券II',
            '多元金融II','地面兵装II','水电III','稀有金属II'
        ]
    },
    'fanneijuan': {
        'code': '10',
        'name': '反内卷板块',
        'industries': [
            '电池II', '光伏设备II', '电池化学品III', '锂电池III', '光伏辅材III'
        ]
    },
    'duli': {
        'code': '11',
        'name': '独立板块',
        'industries': [
            '医药生物I', '化学制药II', '中药II', '传媒I', '电视广播II', '数字媒体II',
            '出版II', '影视院线II', '游戏II'
        ]
    },
    'zhuangbei': {
        'code': '12',
        'name': '装备板块',
        'industries': [
            '国防军工I', '地面兵装II', '军工电子II', '航空装备II', '航天装备II',
            '船舶制造II', '电气设备I', '电网设备II', '风电设备II', '电源设备II',
            '自动化设备II', '工程机械II', '铁路设备III'
        ]
    },
    'xinnengyuanqiche': {
        'code': '13',
        'name': '新能源车板块',
        'industries': [
            '汽车零部件II', '能源金属II', '锂电池III', '汽车I', '商用车II', '乘用车II',
            '汽车电子电气系统III', '摩托车III', '电动乘用车III', '底盘与发动机系统III'
        ]
    },
    'xiaoxiboyimian': {
        'code': '14',
        'name': '消息博弈板块',
        'industries': [
            '半导体II', '电子化学品ⅡII','数字芯片设计III','集成电路封测III','半导体材料III',
            '半导体设备III','分立器件III','集成电路制造III','模拟芯片设计III'
        ]
    },
    'jibenmian': {
        'code': '15',
        'name': '基本面板块',
        'industries': [
            '电池II', '光伏设备II', '锂电池III'
        ]
    }

}

# 创建全局数据库管理器
db_manager = DatabaseManager()

def get_db_connection():
    """获取数据库连接（兼容性函数）"""
    # 使用DatabaseManager的engine获取连接
    return db_manager.engine.connect()

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

def create_mapping_file():
    """创建自定义分类映射文件"""
    print("开始创建自定义分类映射文件...")
    
    # 获取申万行业数据并去重
    sw_data = get_sw_hierarchy_data()
    print(f"获取到申万行业数据: {len(sw_data)} 条记录")
    
    # 获取唯一的行业代码和名称
    unique_industries = []
    
    # 收集所有唯一的L1级行业
    l1_industries = sw_data[['l1_code', 'l1_name']].dropna().drop_duplicates()
    for _, row in l1_industries.iterrows():
        unique_industries.append({
            'sw_code': row['l1_code'],
            'sw_name': row['l1_name'],
            'level': 'L1'
        })
    
    # 收集所有唯一的L2级行业
    l2_industries = sw_data[['l2_code', 'l2_name']].dropna().drop_duplicates()
    for _, row in l2_industries.iterrows():
        unique_industries.append({
            'sw_code': row['l2_code'],
            'sw_name': row['l2_name'],
            'level': 'L2'
        })
    
    # 收集所有唯一的L3级行业
    l3_industries = sw_data[['l3_code', 'l3_name']].dropna().drop_duplicates()
    for _, row in l3_industries.iterrows():
        unique_industries.append({
            'sw_code': row['l3_code'],
            'sw_name': row['l3_name'],
            'level': 'L3'
        })
    
    print(f"获取到唯一行业: {len(unique_industries)} 个")
    
    mapping_records = []
    
    # 遍历自定义分类
    for category_key, category_info in CUSTOM_CLASSIFICATION_MAPPING.items():
        xinfenlei_code = category_info['code']
        xinfenlei_name = category_info['name']
        target_industries = category_info['industries']
        
        print(f"处理分类: {xinfenlei_name} ({xinfenlei_code})")
        
        # 匹配唯一行业数据
        for industry in unique_industries:
            sw_code = industry['sw_code']
            sw_name = industry['sw_name']
            level = industry['level']
            
            if str(sw_name) in target_industries:
                sw_xin_code = str(sw_code).replace('.SI', '.ZS') if pd.notna(sw_code) else ''
                mapping_records.append({
                    'sw_code': sw_code,
                    'sw_name': sw_name,
                    'sw_xin_code': sw_xin_code,
                    'xinfenlei_code': xinfenlei_code,
                    'xinfenlei_name': xinfenlei_name,
                    'level': level
                })
    
    # 创建映射DataFrame
    mapping_df = pd.DataFrame(mapping_records)
    
    if len(mapping_df) == 0:
        print("警告: 没有找到匹配的行业数据")
        return mapping_df
    
    # 保存到CSV文件
    output_file = 'xinfenlei.csv'
    mapping_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"映射文件创建完成: {output_file}")
    print(f"总共创建了 {len(mapping_df)} 条映射记录")
    
    # 显示统计信息
    print("\n各分类统计:")
    stats = mapping_df.groupby(['xinfenlei_code', 'xinfenlei_name']).size().reset_index(name='count')
    for _, row in stats.iterrows():
        print(f"  {row['xinfenlei_code']} - {row['xinfenlei_name']}: {row['count']} 条")
    
    return mapping_df

def merge_with_stock_data():
    """将映射文件与股票数据合并 - 支持多重分类"""
    print("开始合并股票数据（支持多重分类）...")
    
    # 读取映射文件
    mapping_df = pd.read_csv('xinfenlei.csv', encoding='utf-8-sig')
    
    # 获取股票申万分类数据
    conn = get_db_connection()
    query = """
    SELECT stock_code, stock_name, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
    FROM sw_cfg_hierarchy 
    WHERE stock_code IS NOT NULL
    """
    stock_data = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"获取到股票数据: {len(stock_data)} 条记录")
    print(f"映射数据: {len(mapping_df)} 条记录")
    
    # 收集所有股票的多重分类
    stock_classifications = []
    
    for _, stock in stock_data.iterrows():
        stock_code = stock['stock_code']
        stock_name = stock['stock_name']
        
        # 收集该股票的所有分类
        classifications = []
        
        # 检查L1级分类
        if pd.notna(stock['l1_code']) and pd.notna(stock['l1_name']):
            l1_matches = mapping_df[
                (mapping_df['level'] == 'L1') & 
                (mapping_df['sw_code'] == stock['l1_code']) & 
                (mapping_df['sw_name'] == stock['l1_name'])
            ]
            for _, match in l1_matches.iterrows():
                classifications.append({
                    'xinfenlei_code': match['xinfenlei_code'],
                    'xinfenlei_name': match['xinfenlei_name'],
                    'level': 'L1',
                    'sw_code': match['sw_code'],
                    'sw_name': match['sw_name']
                })
        
        # 检查L2级分类
        if pd.notna(stock['l2_code']) and pd.notna(stock['l2_name']):
            l2_matches = mapping_df[
                (mapping_df['level'] == 'L2') & 
                (mapping_df['sw_code'] == stock['l2_code']) & 
                (mapping_df['sw_name'] == stock['l2_name'])
            ]
            for _, match in l2_matches.iterrows():
                classifications.append({
                    'xinfenlei_code': match['xinfenlei_code'],
                    'xinfenlei_name': match['xinfenlei_name'],
                    'level': 'L2',
                    'sw_code': match['sw_code'],
                    'sw_name': match['sw_name']
                })
        
        # 检查L3级分类
        if pd.notna(stock['l3_code']) and pd.notna(stock['l3_name']):
            l3_matches = mapping_df[
                (mapping_df['level'] == 'L3') & 
                (mapping_df['sw_code'] == stock['l3_code']) & 
                (mapping_df['sw_name'] == stock['l3_name'])
            ]
            for _, match in l3_matches.iterrows():
                classifications.append({
                    'xinfenlei_code': match['xinfenlei_code'],
                    'xinfenlei_name': match['xinfenlei_name'],
                    'level': 'L3',
                    'sw_code': match['sw_code'],
                    'sw_name': match['sw_name']
                })
        
        # 为每个分类创建一条记录
        if classifications:
            for classification in classifications:
                stock_classifications.append({
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'l1_code': stock['l1_code'],
                    'l1_name': stock['l1_name'],
                    'l2_code': stock['l2_code'],
                    'l2_name': stock['l2_name'],
                    'l3_code': stock['l3_code'],
                    'l3_name': stock['l3_name'],
                    'xinfenlei_code': classification['xinfenlei_code'],
                    'xinfenlei_name': classification['xinfenlei_name'],
                    'classification_level': classification['level'],
                    'matched_sw_code': classification['sw_code'],
                    'matched_sw_name': classification['sw_name']
                })
        else:
            # 没有分类的股票也保留
            stock_classifications.append({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'l1_code': stock['l1_code'],
                'l1_name': stock['l1_name'],
                'l2_code': stock['l2_code'],
                'l2_name': stock['l2_name'],
                'l3_code': stock['l3_code'],
                'l3_name': stock['l3_name'],
                'xinfenlei_code': None,
                'xinfenlei_name': None,
                'classification_level': None,
                'matched_sw_code': None,
                'matched_sw_name': None
            })
    
    # 创建最终结果DataFrame
    final_result = pd.DataFrame(stock_classifications)
    
    # 保存结果
    output_file = 'stock_with_custom_classification.csv'
    final_result.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"合并完成，结果保存到: {output_file}")
    print(f"总共生成了 {len(final_result)} 条记录")
    
    # 显示分类统计
    print("\n股票分类统计:")
    classified_records = final_result[final_result['xinfenlei_name'].notna()]
    stats = classified_records.groupby(['xinfenlei_code', 'xinfenlei_name']).size().reset_index(name='record_count')
    stats = stats.sort_values('record_count', ascending=False)
    
    for _, row in stats.iterrows():
        print(f"  {row['xinfenlei_code']} - {row['xinfenlei_name']}: {row['record_count']} 条记录")
    
    # 显示多重分类统计
    print(f"\n多重分类统计:")
    unique_stocks = final_result['stock_code'].nunique()
    classified_stocks = final_result[final_result['xinfenlei_name'].notna()]['stock_code'].nunique()
    print(f"  总股票数: {unique_stocks}")
    print(f"  有分类的股票数: {classified_stocks}")
    print(f"  无分类的股票数: {unique_stocks - classified_stocks}")
    print(f"  总记录数: {len(final_result)} (包含多重分类)")
    
    # 显示多重分类的例子
    multi_classified = final_result[final_result['xinfenlei_name'].notna()].groupby('stock_code').size()
    multi_classified = multi_classified[multi_classified > 1]
    if len(multi_classified) > 0:
        print(f"  有多重分类的股票数: {len(multi_classified)}")
        print("  多重分类示例:")
        for stock_code in multi_classified.head(5).index:
            stock_classifications = final_result[final_result['stock_code'] == stock_code]
            stock_name = stock_classifications.iloc[0]['stock_name']
            categories = stock_classifications[stock_classifications['xinfenlei_name'].notna()]['xinfenlei_name'].tolist()
            print(f"    {stock_code} {stock_name}: {', '.join(categories)}")
    
    return final_result

def analyze_by_custom_classification():
    """基于自定义分类进行聚合分析 - 支持多重分类"""
    print("开始基于自定义分类进行聚合分析（多重分类模式）...")
    
    # 读取合并后的数据
    df = pd.read_csv('stock_with_custom_classification.csv', encoding='utf-8-sig')
    
    # 基本统计
    unique_stocks = df['stock_code'].nunique()
    classified_records = df[df['xinfenlei_name'].notna()]
    classified_stocks = classified_records['stock_code'].nunique()
    
    print(f"总记录数: {len(df)} (包含多重分类)")
    print(f"唯一股票数: {unique_stocks}")
    print(f"有分类的记录数: {len(classified_records)}")
    print(f"有分类的股票数: {classified_stocks}")
    print(f"无分类的股票数: {unique_stocks - classified_stocks}")
    
    # 按自定义分类统计记录数
    print("\n=== 自定义分类记录统计 ===")
    classification_stats = classified_records.groupby(['xinfenlei_code', 'xinfenlei_name']).agg({
        'stock_code': ['count', 'nunique'],
        'stock_name': lambda x: ', '.join(x.drop_duplicates().head(5).tolist()) + ('...' if x.nunique() > 5 else '')
    })
    classification_stats.columns = ['记录数', '股票数', '代表股票']
    classification_stats = classification_stats.sort_values('记录数', ascending=False)
    
    print(classification_stats)
    
    # 多重分类分析
    print("\n=== 多重分类分析 ===")
    multi_classified = classified_records.groupby('stock_code').size()
    multi_classified = multi_classified[multi_classified > 1]
    
    if len(multi_classified) > 0:
        print(f"有多重分类的股票数: {len(multi_classified)}")
        print(f"平均每只多重分类股票的分类数: {multi_classified.mean():.2f}")
        print(f"最多分类数: {multi_classified.max()}")
        
        # 显示多重分类分布
        classification_dist = multi_classified.value_counts().sort_index()
        print("\n多重分类分布:")
        for num_classifications, count in classification_dist.items():
            print(f"  {num_classifications} 个分类: {count} 只股票")
        
        # 显示多重分类示例
        print("\n多重分类股票示例:")
        for stock_code in multi_classified.head(10).index:
            stock_records = df[df['stock_code'] == stock_code]
            stock_name = stock_records.iloc[0]['stock_name']
            categories = stock_records[stock_records['xinfenlei_name'].notna()]['xinfenlei_name'].tolist()
            print(f"  {stock_code} {stock_name}: {', '.join(categories)}")
    else:
        print("没有多重分类的股票")
    
    # 按申万L1级行业统计（去重）
    print("\n=== 申万L1级行业统计（唯一股票数） ===")
    l1_stats = df[['stock_code', 'l1_name']].drop_duplicates().groupby('l1_name').size().sort_values(ascending=False)
    print(l1_stats.head(10))
    
    return df

def sync_to_database():
    """将结果同步到数据库"""
    print("开始同步数据到数据库...")
    
    try:
        conn = get_db_connection()
        
        # 1. 同步映射文件到数据库
        print("同步映射文件到数据库...")
        mapping_df = pd.read_csv('xinfenlei.csv', encoding='utf-8-sig')
        
        # 创建映射表
        mapping_table_name = 'xinfenlei'
        mapping_df.to_sql(mapping_table_name, conn, if_exists='replace', index=False)
        print(f"映射表 {mapping_table_name} 创建完成，包含 {len(mapping_df)} 条记录")
        
        # 2. 同步股票分类数据到数据库
        print("同步股票分类数据到数据库...")
        stock_df = pd.read_csv('stock_with_custom_classification.csv', encoding='utf-8-sig')
        
        # 创建股票分类表
        stock_table_name = 'stock_with_custom_classification'
        stock_df.to_sql(stock_table_name, conn, if_exists='replace', index=False)
        print(f"股票分类表 {stock_table_name} 创建完成，包含 {len(stock_df)} 条记录")
        
        # 3. 创建索引以提高查询性能
        print("创建数据库索引...")
        cursor = conn.cursor()
        
        # 为映射表创建索引
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{mapping_table_name}_sw_code ON {mapping_table_name}(sw_code)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{mapping_table_name}_xinfenlei_code ON {mapping_table_name}(xinfenlei_code)")
        
        # 为股票分类表创建索引
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{stock_table_name}_stock_code ON {stock_table_name}(stock_code)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{stock_table_name}_xinfenlei_code ON {stock_table_name}(xinfenlei_code)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{stock_table_name}_l1_code ON {stock_table_name}(l1_code)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{stock_table_name}_l2_code ON {stock_table_name}(l2_code)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{stock_table_name}_l3_code ON {stock_table_name}(l3_code)")
        
        conn.commit()
        print("数据库索引创建完成")
        
        # 4. 验证数据同步
        print("\n=== 数据库同步验证 ===")
        
        # 验证映射表
        mapping_count = cursor.execute(f"SELECT COUNT(*) FROM {mapping_table_name}").fetchone()[0]
        print(f"映射表 {mapping_table_name}: {mapping_count} 条记录")
        
        # 验证股票分类表
        stock_count = cursor.execute(f"SELECT COUNT(*) FROM {stock_table_name}").fetchone()[0]
        print(f"股票分类表 {stock_table_name}: {stock_count} 条记录")
        
        # 显示各分类的股票数量
        print("\n各分类股票数量统计:")
        query = f"""
        SELECT xinfenlei_code, xinfenlei_name, COUNT(*) as stock_count
        FROM {stock_table_name}
        WHERE xinfenlei_name IS NOT NULL
        GROUP BY xinfenlei_code, xinfenlei_name
        ORDER BY stock_count DESC
        """
        results = cursor.execute(query).fetchall()
        for row in results:
            print(f"  {row[0]} - {row[1]}: {row[2]} 只股票")
        
        conn.close()
        print("\n数据库同步完成!")
        
        return True
        
    except Exception as e:
        print(f"数据库同步过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return False

def query_stocks_by_classification(xinfenlei_code: Optional[str] = None, xinfenlei_name: Optional[str] = None, limit: int = 10):
    """根据自定义分类查询股票"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 构建查询条件
        where_conditions = []
        params = []
        
        if xinfenlei_code:
            where_conditions.append("xinfenlei_code = ?")
            params.append(xinfenlei_code)
        
        if xinfenlei_name:
            where_conditions.append("xinfenlei_name = ?")
            params.append(xinfenlei_name)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = f"""
        SELECT stock_code, stock_name, l1_name, l2_name, l3_name, xinfenlei_code, xinfenlei_name
        FROM stock_with_custom_classification
        WHERE {where_clause}
        LIMIT ?
        """
        params.append(limit)
        
        results = cursor.execute(query, params).fetchall()
        conn.close()
        
        if results:
            print(f"\n查询结果 (限制 {limit} 条):")
            print("股票代码 | 股票名称 | L1行业 | L2行业 | L3行业 | 分类代码 | 分类名称")
            print("-" * 80)
            for row in results:
                print(f"{row[0]} | {row[1]} | {row[2] or ''} | {row[3] or ''} | {row[4] or ''} | {row[5] or ''} | {row[6] or ''}")
        else:
            print("没有找到匹配的股票")
        
        return results
        
    except Exception as e:
        print(f"查询过程中出现错误: {e}")
        return []




def main():
    """主函数"""
    try:
        # 1. 创建映射文件
        mapping_df = create_mapping_file()
        
        # 2. 合并股票数据
        merged_df = merge_with_stock_data()
        
        # 3. 进行分析
        analysis_df = analyze_by_custom_classification()
        
        # 4. 同步到数据库
        sync_success = sync_to_database()
        
        print("\n=== 处理完成 ===")
        print("生成的文件:")
        print("  - xinfenlei.csv: 自定义分类映射文件")
        print("  - stock_with_custom_classification.csv: 带自定义分类的股票数据")
        
        if sync_success:
            print("\n数据库表:")
            print("  - xinfenlei: 自定义分类映射表")
            print("  - stock_with_custom_classification: 带自定义分类的股票数据表")
            
            # 示例查询
            print("\n=== 示例查询 ===")
            print("查询权重板块股票:")
            query_stocks_by_classification(xinfenlei_code='01', limit=5)
            
            print("\n查询中小盘板块股票:")
            query_stocks_by_classification(xinfenlei_code='04', limit=5)
        
    except Exception as e:
        print(f"处理过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
