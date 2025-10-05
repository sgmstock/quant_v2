import pandas as pd
import numpy as np
import os
import sys
from typing import List, Dict, Optional

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# 从正确的路径导入 StockXihua 类
from core.utils.stock_filter import StockXihua, get_bankuai_stocks
from core.utils.logger import get_logger

# 初始化日志记录器
logger = get_logger(__name__) 

class FactorScorer:
    """
    个股分类因子评分器
    
    该类根据预设的因子及其权重，为输入的股票列表计算综合得分。
    它使用 StockXihua 类来获取股票的分类特征。
    
    新增功能：
    1. 支持从CSV文件读取股票列表
    2. 支持从板块获取成分股
    3. 支持多种数据源的评分计算
    
    使用方法：
    - calculate_scores(): 直接传入股票代码列表
    - calculate_scores_from_csv(): 从CSV文件读取股票并评分
    - calculate_scores_from_bankuai(): 从板块获取股票并评分
    """
    
    def __init__(self, factor_weights: Dict[str, float], db_path: Optional[str] = None):
        """
        初始化评分器
        
        Args:
            factor_weights (Dict[str, float]): 
                一个包含因子名称和对应权重的字典。
                例如: {'国企': 0.4, '小市值': 0.6, 'B股': 0.9}
            db_path (Optional[str], optional): 数据库路径. 默认为 None.
        """
        if not factor_weights:
            raise ValueError("因子权重字典 (factor_weights) 不能为空。")
            
        self.factor_weights = factor_weights
        self.xihua = StockXihua(db_path=db_path)
        print("✅ 因子评分器初始化成功！")
        print(f"当前配置的因子权重: {self.factor_weights}")

    def calculate_scores(self, stock_list: List[str], normalization_method: str = 'min_max') -> pd.DataFrame:
        """
        为股票列表计算因子得分
        
        Args:
            stock_list (List[str]): 需要评分的股票代码列表.
            normalization_method (str, optional): 
                分数处理方法. 'min_max' (归一化到0-100) 或 'z_score' (标准化).
                默认为 'min_max'.
                
        Returns:
            pd.DataFrame: 包含股票代码、名称、原始分、和归一化/标准化得分的DataFrame.
        """
        if not stock_list:
            print("⚠️ 输入的股票列表为空，返回空的DataFrame。")
            return pd.DataFrame()

        # --- 步骤 1: 使用 StockXihua 获取所有股票的分类数据 ---
        print(f"正在为 {len(stock_list)} 只股票准备基础数据...")
        df = self.xihua.create_stock_dataframe(stock_list)
        
        # 计算分位数相关的分类 (例如大/小市值, 高/低价股)
        self.xihua.calculate_quantile_categories(df)
        
        if df.empty:
            print("⚠️ 经过基础筛选后，没有有效的股票可供评分。")
            return pd.DataFrame()

        # --- 步骤 2: 计算每个股票的原始累计得分 ---
        print("正在计算原始因子得分...")
        
        # 定义因子名称到实际数据的映射关系
        # 'type': 'column' -> 直接从df的列取bool值
        # 'type': 'list' -> 判断 'code' 是否在 xihua 的某个列表属性中
        factor_map = {
            '国企': {'type': 'column', 'value': '国企'},
            'B股': {'type': 'column', 'value': 'B'},
            'H股': {'type': 'column', 'value': 'H'},
            '次新': {'type': 'column', 'value': '次新'},
            '老股': {'type': 'column', 'value': '老股'},
            '超强': {'type': 'column', 'value': '超强'},
            '大市值': {'type': 'list', 'value': self.xihua.dsz},
            '小市值': {'type': 'list', 'value': self.xihua.xsz},
            '高价股': {'type': 'list', 'value': self.xihua.gbj},
            '低价股': {'type': 'list', 'value': self.xihua.dbj},
            '大高股': {'type': 'list', 'value': self.xihua.dg}
        }
        
        def get_raw_score(row):
            score = 0.0
            for factor_name, weight in self.factor_weights.items():
                if factor_name not in factor_map:
                    continue # 如果权重中的因子未定义，则跳过
                
                mapping = factor_map[factor_name]
                is_true = False
                if mapping['type'] == 'column':
                    is_true = row[mapping['value']]
                elif mapping['type'] == 'list':
                    is_true = row['code'] in mapping['value']
                
                if is_true:
                    score += weight
            return score

        df['raw_score'] = df.apply(get_raw_score, axis=1)

        # --- 步骤 3: 统一使用 Min-Max 归一化到 [0, 100] 区间 ---
        print("正在对分数进行 Min-Max 归一化处理...")
        
        raw_scores = df['raw_score']
        
        # 统一使用 Min-Max 归一化到 [0, 100] 区间
        min_score = raw_scores.min()
        max_score = raw_scores.max()
        if max_score == min_score:
            # 如果所有分数都一样，则全部设为50
            df['final_score'] = 50.0 if len(raw_scores) > 0 else 0.0
        else:
            df['final_score'] = 100 * (raw_scores - min_score) / (max_score - min_score)
            
        # 整理输出结果
        result_df = df[['code', 'name', 'raw_score', 'final_score']].copy()
        # 重命名列以保持一致性
        result_df.rename(columns={'code': 'stock_code', 'name': 'stock_name'}, inplace=True)
        result_df.sort_values(by='final_score', ascending=False, inplace=True)
        
        print("✅ 评分计算完成！")
        return result_df

    def get_bankuai_stocks(self, bankuai_name: str) -> List[str]:
        """
        获取板块成分股
        
        Args:
            bankuai_name: 板块名称
            
        Returns:
            List[str]: 股票代码列表
        """
        try:
            stock_codes = get_bankuai_stocks(bankuai_name)
            logger.info(f"获取到 {len(stock_codes)} 只 {bankuai_name} 板块股票")
            return stock_codes
            
        except Exception as e:
            logger.error(f"获取板块股票失败: {e}")
            return []
    
    def calculate_scores_from_csv(self, csv_path: str) -> pd.DataFrame:
        """
        (优化后) 从CSV文件读取股票列表并计算因子得分
        """
        try:
            # 1. 从CSV文件中只获取股票代码列表
            df_csv = pd.read_csv(csv_path, dtype={'stock_code': str})
            if 'stock_code' not in df_csv.columns:
                logger.error(f"CSV文件 {csv_path} 中未找到 'stock_code' 列")
                return pd.DataFrame()
                
            stock_codes = df_csv['stock_code'].dropna().unique().tolist()
            logger.info(f"从CSV文件读取到 {len(stock_codes)} 只唯一股票代码")

            # 2. 将代码列表交给核心评分函数处理
            return self.calculate_scores(stock_codes)

        except FileNotFoundError:
            logger.error(f"CSV文件未找到: {csv_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"处理CSV文件 {csv_path} 时失败: {e}")
            return pd.DataFrame()
    
    def calculate_scores_from_bankuai(self, bankuai_name: str) -> pd.DataFrame:
        """
        (优化后) 从板块获取股票列表并计算因子得分
        """
        # 1. 获取板块股票代码列表
        stock_codes = self.get_bankuai_stocks(bankuai_name)
        
        if not stock_codes:
            print(f"⚠️ 未获取到 {bankuai_name} 板块的股票")
            return pd.DataFrame()
            
        # 2. 将代码列表交给核心评分函数处理
        return self.calculate_scores(stock_codes)


if __name__ == "__main__":
    print("=== FactorScorer 使用示例 ===")
    
    # =================================================================
    # 1. 构建分类因子：在这里定义你想要的因子和对应的权重
    #    这是整个策略的核心配置区域！
    # =================================================================
    style_factor_weights = {
        '国企': 0.4,
        '小市值': 0.6,
        'B股': -0.9,  # 权重也可以是负数，代表不希望这类股票出现
        '次新': 0.5,
        '高价股': 0.2
    }

    # =================================================================
    # 2. 评估个股的分类因子的分值
    # =================================================================
    
    # 初始化评分器
    # 假设你的数据库在 "databases/quant_system.db"
    scorer = FactorScorer(factor_weights=style_factor_weights)
    
    # 准备一个待测试的股票列表
    # 在实际使用中，这个列表可能来自于你的板块选股或其他粗选结果
    # 这里我们从业经筛选的有效股中随机选50只作为示例
    try:
        # all_valid_stocks = scorer.xihua.filter_basic_conditions(
        #     scorer.xihua._load_stock_basic()['stock_code'].tolist()
        # )
        # sample_stocks = all_valid_stocks[:50] # 取前50只作为演示
        print("正在获取房地产开发板块股票...")
        sample_stocks = scorer.get_bankuai_stocks("房地产开发")
        print(f"获取到 {len(sample_stocks)} 只房地产开发板块股票")
        
        if not sample_stocks:
            print("房地产开发板块没有股票，尝试使用银行板块...")
            sample_stocks = scorer.get_bankuai_stocks("银行")
            print(f"获取到 {len(sample_stocks)} 只银行板块股票")
        
        if not sample_stocks:
            print("没有获取到任何板块股票，使用示例股票列表...")
            sample_stocks = ['000001', '000002', '600519', '300750']
            print(f"使用示例股票: {sample_stocks}")

        # === 执行评分 ===
        
        # 使用统一的 Min-Max 归一化 (结果在 0-100 之间，直观)
        print("\n--- 正在使用 Min-Max 归一化进行评分 ---")
        scores_min_max = scorer.calculate_scores(sample_stocks)
        print("评分结果 (Top 10):")
        print(scores_min_max.head(10).to_string(index=False))
        
        # =================================================================
        # 3. 演示新的数据源方法
        # =================================================================
        print("\n" + "="*50 + "\n")
        print("--- 演示从板块获取股票并评分 ---")
        
        # 示例：从银行板块获取股票并评分
        try:
            bankuai_results = scorer.calculate_scores_from_bankuai("银行")
            if not bankuai_results.empty:
                print("银行板块评分结果 (Top 10):")
                print(bankuai_results.head(10).to_string(index=False))
            else:
                print("银行板块未获取到有效股票")
        except Exception as e:
            print(f"板块评分失败: {e}")
        
        # 示例：从CSV文件获取股票并评分（如果文件存在）
        csv_file_path = "操作板块/示例股票池.csv"  # 请根据实际情况修改路径
        if os.path.exists(csv_file_path):
            print("\n--- 演示从CSV文件获取股票并评分 ---")
            try:
                csv_results = scorer.calculate_scores_from_csv(csv_file_path)
                if not csv_results.empty:
                    print("CSV文件评分结果 (Top 10):")
                    print(csv_results.head(10).to_string(index=False))
                else:
                    print("CSV文件未获取到有效股票")
            except Exception as e:
                print(f"CSV评分失败: {e}")
        else:
            print(f"\nCSV文件 {csv_file_path} 不存在，跳过CSV演示")
        
    except Exception as e:
        print(f"运行示例时出现错误: {e}")
        print("请检查数据库连接和股票数据是否可用")