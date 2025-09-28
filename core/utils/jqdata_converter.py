"""
聚宽数据格式转换器

负责处理聚宽数据格式转换和验证
"""

import pandas as pd
import os
from typing import Tuple, Dict
from core.utils.logger import get_logger

logger = get_logger("core.utils.jqdata_converter")


class JQDataConverter:
    """聚宽数据格式转换器"""
    
    def __init__(self):
        self.suffixes_to_remove = ['.XSHE', '.XSHG', '.XSHE', '.XSHG']
    
    def clean_stock_code(self, stock_code: str) -> str:
        """
        清理股票代码，去掉聚宽后缀
        
        Args:
            stock_code: 原始股票代码，如 '000001.XSHE'
            
        Returns:
            清理后的股票代码，如 '000001'
        """
        if pd.isna(stock_code):
            return stock_code
            
        stock_code = str(stock_code).strip()
        
        # 去掉各种后缀
        for suffix in self.suffixes_to_remove:
            if stock_code.endswith(suffix):
                stock_code = stock_code[:-len(suffix)]
                break
        
        # 确保是6位数字格式
        if stock_code.isdigit():
            return stock_code.zfill(6)
        
        return stock_code
    
    def convert_csv_file(self, input_path: str, output_path: str, 
                        stock_code_col: str = 'stock_code') -> Tuple[bool, int]:
        """
        转换聚宽CSV文件格式
        
        Args:
            input_path: 输入文件路径
            output_path: 输出文件路径
            stock_code_col: 股票代码列名
            
        Returns:
            (是否成功, 转换的记录数)
        """
        try:
            logger.info(f"开始转换聚宽数据文件: {input_path}")
            
            # 读取CSV文件
            df = pd.read_csv(input_path)
            logger.info(f"原始数据行数: {len(df)}")
            
            # 检查必要的列
            if stock_code_col not in df.columns:
                logger.error(f"CSV文件中没有找到股票代码列: {stock_code_col}")
                return False, 0
            
            # 转换股票代码格式
            df[stock_code_col] = df[stock_code_col].apply(self.clean_stock_code)
            
            # 过滤掉无效的股票代码
            original_count = len(df)
            df = df[df[stock_code_col].str.match(r'^\d{6}$', na=False)]
            filtered_count = len(df)
            
            if filtered_count < original_count:
                logger.info(f"过滤掉 {original_count - filtered_count} 条无效股票代码记录")
            
            # 保存转换后的文件
            df.to_csv(output_path, index=False)
            logger.info(f"转换完成，保存到: {output_path}")
            logger.info(f"有效数据行数: {filtered_count}")
            
            return True, filtered_count
            
        except Exception as e:
            logger.error(f"转换文件时出错: {e}")
            return False, 0
    
    def convert_and_validate(self, input_path: str, output_path: str,
                           required_cols: list = None) -> Tuple[bool, dict]:
        """
        转换并验证数据格式
        
        Args:
            input_path: 输入文件路径
            output_path: 输出文件路径
            required_cols: 必需的列名列表
            
        Returns:
            (是否成功, 验证结果字典)
        """
        if required_cols is None:
            required_cols = ['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume']
        
        try:
            # 转换文件
            success, record_count = self.convert_csv_file(input_path, output_path)
            if not success:
                return False, {'error': '文件转换失败'}
            
            # 验证转换结果
            df = pd.read_csv(output_path)
            
            # 检查必需列
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return False, {'error': f'缺少必需列: {missing_cols}'}
            
            # 检查股票代码格式
            if 'stock_code' in df.columns:
                # 确保stock_code列是字符串类型
                df['stock_code'] = df['stock_code'].astype(str)
                invalid_codes = df[~df['stock_code'].str.match(r'^\d{6}$', na=False)]
                if len(invalid_codes) > 0:
                    logger.warning(f"发现 {len(invalid_codes)} 个格式不正确的股票代码")
            
            # 检查数据完整性
            null_counts = df.isnull().sum()
            data_quality = {
                'total_records': len(df),
                'null_counts': null_counts.to_dict(),
                'date_range': {
                    'start': df['date'].min() if 'date' in df.columns else None,
                    'end': df['date'].max() if 'date' in df.columns else None
                },
                'stock_count': df['stock_code'].nunique() if 'stock_code' in df.columns else 0
            }
            
            logger.info(f"数据验证完成: {data_quality}")
            return True, data_quality
            
        except Exception as e:
            logger.error(f"验证数据时出错: {e}")
            return False, {'error': str(e)}
    
    def batch_convert(self, input_dir: str, output_dir: str, 
                     pattern: str = "*.csv") -> dict:
        """
        批量转换目录中的文件
        
        Args:
            input_dir: 输入目录
            output_dir: 输出目录
            pattern: 文件匹配模式
            
        Returns:
            转换结果统计
        """
        import glob
        
        os.makedirs(output_dir, exist_ok=True)
        
        input_files = glob.glob(os.path.join(input_dir, pattern))
        results = {
            'total_files': len(input_files),
            'success_count': 0,
            'failed_files': [],
            'total_records': 0
        }
        
        for input_file in input_files:
            filename = os.path.basename(input_file)
            output_file = os.path.join(output_dir, f"converted_{filename}")
            
            success, record_count = self.convert_csv_file(input_file, output_file)
            
            if success:
                results['success_count'] += 1
                results['total_records'] += record_count
                logger.info(f"✅ 成功转换: {filename} ({record_count} 条记录)")
            else:
                results['failed_files'].append(filename)
                logger.error(f"❌ 转换失败: {filename}")
        
        logger.info(f"批量转换完成: {results['success_count']}/{results['total_files']} 个文件成功")
        return results
