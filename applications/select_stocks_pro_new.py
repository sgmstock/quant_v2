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

# 导入真实的评分模块
from core.technical_analyzer.technical_ccx_score import LongLongTermTechnicalScorer
from core.technical_analyzer.technical_cx_score import LongTermTechnicalScorer
from core.technical_analyzer.technical_zj_score import TechnicalScoringSystem
from applications.select_stocks_bankuai_new import FactorScorer
from core.utils.logger import get_logger
from data_management.database_manager import DatabaseManager

# 初始化日志记录器
logger = get_logger(__name__)

# --- 这是核心的总指挥类 ---

class ComprehensiveScorer:
    """
    一个完整的、多层次的股票评分系统总指挥。
    - 负责执行初筛流程。
    - 负责调用所有子评分模块。
    - 负责根据权重合成最终总分。
    """
    def __init__(self, weights: Dict[str, float], factor_weights: Dict[str, float], 
                 pre_screen_threshold: int = 50, db_path: Optional[str] = None, 
                 analysis_date: Optional[str] = None):
        """
        初始化总评分系统
        
        Args:
            weights (Dict[str, float]): 各个子评分系统的最终权重.
            factor_weights (Dict[str, float]): 板块因子评分器的内部权重.
            pre_screen_threshold (int): 板块评分初筛的门槛分数 (0-100). 低于此分数的股票将被过滤.
            db_path (Optional[str]): 数据库路径，用于技术指标计算.
            analysis_date (Optional[str]): 分析日期，格式为 'YYYY-MM-DD'，默认为今日.
        """
        self.weights = weights
        if not np.isclose(sum(self.weights.values()), 1.0):
            raise ValueError("总权重的合计数必须为 1.0")
            
        self.factor_weights = factor_weights
        self.pre_screen_threshold = pre_screen_threshold
        self.db_path = db_path
        
        # 设置分析日期
        if analysis_date is None:
            self.analysis_date = pd.Timestamp.now().strftime('%Y-%m-%d')
        else:
            self.analysis_date = analysis_date
        
        # 初始化数据库管理器
        self.db_manager = DatabaseManager(db_path=db_path)
        
        # 初始化所有需要的子评分模块
        self.factor_scorer = FactorScorer(factor_weights=self.factor_weights, db_path=db_path)
        self.ultra_long_scorer = LongLongTermTechnicalScorer()
        self.long_scorer = LongTermTechnicalScorer()
        self.medium_scorer = TechnicalScoringSystem()
        
        logger.info("✅ 总评分系统初始化成功！")
        logger.info(f"分析日期: {self.analysis_date}")
        logger.info(f"最终权重配置: {self.weights}")
        logger.info(f"初筛门槛分数: {self.pre_screen_threshold}")

    def run_pipeline(self, initial_stock_list: List[str]) -> pd.DataFrame:
        """
        执行完整的评分流水线
        
        Args:
            initial_stock_list (List[str]): 初始的股票池.

        Returns:
            pd.DataFrame: 包含所有评分和最终总分的详细结果.
        """
        logger.info("🚀 开始执行评分流水线...")
        logger.info(f"初始股票池数量: {len(initial_stock_list)}")
        
        # --- 阶段 1: 板块评分初筛 ---
        try:
            factor_scores_df = self.factor_scorer.calculate_scores(initial_stock_list)
            
            screened_df = factor_scores_df[factor_scores_df['final_score'] >= self.pre_screen_threshold]
            screened_stocks = screened_df['stock_code'].tolist()
        
            logger.info(f"📊 初筛完成！{len(screened_stocks)} / {len(initial_stock_list)} 只股票通过初筛（分数 >= {self.pre_screen_threshold}）。")
        
            if not screened_stocks:
                logger.warning("⚠️ 没有股票通过初筛，流程终止。")
                return pd.DataFrame()

        except Exception as e:
            logger.warning(f"板块评分失败，跳过初筛: {e}")
            # 如果板块评分失败，直接进行技术评分
            screened_stocks = initial_stock_list
            screened_df = pd.DataFrame({
                'stock_code': initial_stock_list,
                'stock_name': [f'股票{code}' for code in initial_stock_list],
                'factor_score': [50.0] * len(initial_stock_list)  # 给予默认板块分数
            })
            logger.info(f"📊 跳过板块评分，直接进行技术评分，股票数量: {len(screened_stocks)}")

        # --- 阶段 2: 对通过初筛的股票进行技术评分 ---
        logger.info("开始计算技术指标评分...")
        
        # 计算技术评分
        ultra_long_scores = []
        long_scores = []
        medium_scores = []
        
        for stock_code in screened_stocks:
            try:
                # 获取股票数据并计算技术指标
                stock_data = self._get_stock_technical_data(stock_code)
                
                if stock_data is not None:
                    # 超长线评分 (月线数据)
                    ultra_long_score = self.ultra_long_scorer.get_long_term_final_score(stock_data['monthly'])[0]
                    ultra_long_scores.append(ultra_long_score)
                    
                    # 长线评分 (周线数据)
                    long_score = self.long_scorer.get_long_term_final_score(stock_data['weekly'])[0]
                    long_scores.append(long_score)
                    
                    # 中线评分 (日线数据)
                    medium_score = self.medium_scorer.get_final_score(stock_data['daily'])[0]
                    medium_scores.append(medium_score)
                else:
                    # 数据获取失败，给予默认分数
                    ultra_long_scores.append(30.0)
                    long_scores.append(30.0)
                    medium_scores.append(30.0)
                    
            except Exception as e:
                logger.warning(f"计算 {stock_code} 技术评分失败: {e}")
                ultra_long_scores.append(30.0)
                long_scores.append(30.0)
                medium_scores.append(30.0)

        # --- 阶段 3: 合并所有评分到一个DataFrame ---
        final_df = screened_df.copy()
        
        # 确保评分结果的长度与筛选后的股票数量匹配
        if len(ultra_long_scores) == len(screened_stocks):
            final_df['ultra_long_term_score'] = ultra_long_scores
        else:
            logger.warning(f"超长线评分数量不匹配: {len(ultra_long_scores)} vs {len(screened_stocks)}")
            final_df['ultra_long_term_score'] = [30.0] * len(screened_stocks)
            
        if len(long_scores) == len(screened_stocks):
            final_df['long_term_score'] = long_scores
        else:
            logger.warning(f"长线评分数量不匹配: {len(long_scores)} vs {len(screened_stocks)}")
            final_df['long_term_score'] = [30.0] * len(screened_stocks)
            
        if len(medium_scores) == len(screened_stocks):
            final_df['medium_term_score'] = medium_scores
        else:
            logger.warning(f"中线评分数量不匹配: {len(medium_scores)} vs {len(screened_stocks)}")
            final_df['medium_term_score'] = [30.0] * len(screened_stocks)
            
        final_df.rename(columns={'final_score': 'factor_score'}, inplace=True)

        # --- 阶段 4: 根据权重计算最终总分 ---
        logger.info("正在计算最终加权总分...")
        final_df['total_score'] = (
            final_df['long_term_score'] * self.weights['long_term'] +
            final_df['medium_term_score'] * self.weights['medium_term'] +
            final_df['factor_score'] * self.weights['factor'] +
            final_df['ultra_long_term_score'] * self.weights['ultra_long_term']
        )
        
        # 结果排序
        final_df.sort_values(by='total_score', ascending=False, inplace=True)
        
        logger.info("🎉 评分流水线执行完毕！")
        return final_df.reset_index(drop=True)
    
    def _get_stock_technical_data(self, stock_code: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        获取股票的技术指标数据
        
        Args:
            stock_code (str): 股票代码
            
        Returns:
            Optional[Dict[str, pd.DataFrame]]: 包含日线、周线、月线数据的字典
        """
        try:
            from data_management.data_processor import get_daily_data_for_backtest, get_weekly_data_for_backtest, get_monthly_data_for_backtest
            from core.utils.indicators import zhibiao
            
            # 使用共享的数据库管理器实例，避免重复创建连接
            daily_data = get_daily_data_for_backtest(stock_code, self.analysis_date, db_manager=self.db_manager)
            weekly_data = get_weekly_data_for_backtest(stock_code, self.analysis_date, db_manager=self.db_manager)
            monthly_data = get_monthly_data_for_backtest(stock_code, self.analysis_date, db_manager=self.db_manager)
            
            if daily_data.empty or weekly_data.empty or monthly_data.empty:
                logger.warning(f"股票 {stock_code} 在 {self.analysis_date} 的数据不完整")
                return None
            
            # 计算技术指标
            daily_with_indicators = zhibiao(daily_data)
            weekly_with_indicators = zhibiao(weekly_data)
            monthly_with_indicators = zhibiao(monthly_data)
            
            return {
                'daily': daily_with_indicators,
                'weekly': weekly_with_indicators,
                'monthly': monthly_with_indicators
            }
            
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 在 {self.analysis_date} 的技术数据失败: {e}")
            return None
    
    def run_pipeline_from_bankuai(self, bankuai_name: str) -> pd.DataFrame:
        """
        从指定板块运行评分流水线
        
        Args:
            bankuai_name (str): 板块名称
            
        Returns:
            pd.DataFrame: 评分结果
        """
        try:
            # 获取板块股票
            stock_list = self.factor_scorer.get_bankuai_stocks(bankuai_name)
            if not stock_list:
                logger.warning(f"板块 {bankuai_name} 没有股票")
                return pd.DataFrame()
            
            logger.info(f"从板块 {bankuai_name} 获取到 {len(stock_list)} 只股票")
            return self.run_pipeline(stock_list)
            
        except Exception as e:
            logger.error(f"从板块 {bankuai_name} 运行评分流水线失败: {e}")
            return pd.DataFrame()
    
    def analyze_results(self, results_df: pd.DataFrame) -> Dict[str, any]:
        """
        分析评分结果
        
        Args:
            results_df (pd.DataFrame): 评分结果DataFrame
            
        Returns:
            Dict[str, any]: 分析结果
        """
        if results_df.empty:
            return {"error": "结果为空"}
        
        analysis = {
            "total_stocks": len(results_df),
            "avg_total_score": results_df['total_score'].mean(),
            "avg_factor_score": results_df['factor_score'].mean(),
            "avg_long_term_score": results_df['long_term_score'].mean(),
            "avg_medium_term_score": results_df['medium_term_score'].mean(),
            "avg_ultra_long_term_score": results_df['ultra_long_term_score'].mean(),
            "top_10_stocks": results_df.head(10)[['stock_code', 'stock_name', 'total_score']].to_dict('records'),
            "score_distribution": {
                "excellent": len(results_df[results_df['total_score'] >= 80]),
                "good": len(results_df[(results_df['total_score'] >= 60) & (results_df['total_score'] < 80)]),
                "average": len(results_df[(results_df['total_score'] >= 40) & (results_df['total_score'] < 60)]),
                "poor": len(results_df[results_df['total_score'] < 40])
            }
        }
        
        return analysis
    
    def set_analysis_date(self, analysis_date: str):
        """
        设置分析日期
        
        Args:
            analysis_date (str): 分析日期，格式为 'YYYY-MM-DD'
        """
        self.analysis_date = analysis_date
        logger.info(f"分析日期已更新为: {self.analysis_date}")
    
    def get_analysis_date(self) -> str:
        """
        获取当前分析日期
        
        Returns:
            str: 当前分析日期
        """
        return self.analysis_date


def example_usage():
    """
    使用示例函数
    展示如何使用综合评分系统
    """
    print("=== 综合评分系统使用示例 ===")
    
    # 1. 定义评分权重
    style_factor_weights = {
        '国企': 0.4,
        '小市值': 0.6,
        'B股': -0.9,
        '次新': 0.5,
        '高价股': 0.2
    }
    
    final_score_weights = {
        'long_term': 0.50,
        'medium_term': 0.25,
        'factor': 0.15,
        'ultra_long_term': 0.10,
    }

    # 2. 创建评分系统
    scorer = ComprehensiveScorer(
        weights=final_score_weights,
        factor_weights=style_factor_weights,
        pre_screen_threshold=60,
        db_path="quant_system.db",
        analysis_date="2025-06-03"  # 可以修改为其他日期
    )
    
    # 3. 示例1: 从板块评分
    print("\n--- 示例1: 从房地产开发板块评分 ---")
    try:
        results = scorer.run_pipeline_from_bankuai("旅游与景区")
        if not results.empty:
            print(f"旅游与景区板块评分完成，共 {len(results)} 只股票")
            print("Top 5 股票:")
            print(results.head(5)[['stock_code', 'stock_name', 'total_score']].to_string(index=False))
        else:
            print("旅游与景区板块没有股票通过评分")
    except Exception as e:
        print(f"旅游与景区板块评分失败: {e}")
    
    # 4. 示例2: 自定义股票列表评分
    print("\n--- 示例2: 自定义股票列表评分 ---")
    custom_stocks = ['000001', '000002', '600519', '300750', '000858']
    try:
        results = scorer.run_pipeline(custom_stocks)
        if not results.empty:
            print(f"自定义股票列表评分完成，共 {len(results)} 只股票")
            print("Top 3 股票:")
            print(results.head(3)[['stock_code', 'stock_name', 'total_score']].to_string(index=False))
        else:
            print("自定义股票列表没有股票通过评分")
    except Exception as e:
        print(f"自定义股票列表评分失败: {e}")
    
    # 5. 示例3: 动态设置分析日期
    print("\n--- 示例3: 动态设置分析日期 ---")
    try:
        # 设置不同的分析日期
        scorer.set_analysis_date("2025-05-01")
        print(f"当前分析日期: {scorer.get_analysis_date()}")
        
        # 可以基于不同日期进行评分
        results = scorer.run_pipeline(['000001', '000002'])
        if not results.empty:
            print(f"基于 {scorer.get_analysis_date()} 的评分完成")
        else:
            print("该日期没有股票通过评分")
    except Exception as e:
        print(f"动态设置分析日期失败: {e}")


# --- 旅游与景区板块评分 ---
if __name__ == '__main__':
    print("=== 旅游与景区板块综合评分系统 ===")
    print("开始初始化...")
    try:
        # 1. 定义板块因子的内部权重
        style_factor_weights = {
            '国企': 0.4,
            '小市值': 0.6,
            'B股': -0.9,
            '次新': 0.5,
            '高价股': 0.2
        }
    
        # 2. 定义四个评分系统的最终权重 (合计为1.0)
        final_score_weights = {
            'long_term': 0.50,
            'medium_term': 0.25,
            'factor': 0.15,
            'ultra_long_term': 0.10,
        }

        # 3. 创建总指挥实例
        #    这里设定板块评分低于60分的股票在第一轮就会被淘汰
        # 使用绝对路径确保数据库连接正确
        print("正在初始化评分器...")
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'databases', 'quant_system.db')
        comprehensive_scorer = ComprehensiveScorer(
        weights=final_score_weights,
        factor_weights=style_factor_weights,
            pre_screen_threshold=60,
            db_path=db_path,  # 使用绝对路径
            analysis_date="2025-09-03"  # 指定分析日期为2025-09-03
        )
        print(f"数据库路径: {db_path}")
        print(f"分析日期: 2025-09-03")
        print(f"目标板块: 旅游与景区")
        
        # 4. 运行旅游与景区板块评分流水线
        try:
            logger.info("开始运行旅游与景区板块评分流水线...")
            final_results = comprehensive_scorer.run_pipeline_from_bankuai("旅游及景区")
            logger.info("旅游及景区板块评分流水线运行完成")
        except Exception as e:
            logger.error(f"运行旅游与景区板块评分流水线失败: {e}")
            import traceback
            traceback.print_exc()
            final_results = pd.DataFrame()
    
        # 5. 查看最终结果
        if not final_results.empty:
            logger.info("🏆 旅游及景区板块最终评分排名:")
            print(final_results.to_string())
            
            # 分析结果
            analysis = comprehensive_scorer.analyze_results(final_results)
            logger.info("📊 旅游及景区板块评分结果分析:")
            print(f"总股票数: {analysis['total_stocks']}")
            print(f"平均总分: {analysis['avg_total_score']:.2f}")
            print(f"平均板块分: {analysis['avg_factor_score']:.2f}")
            print(f"平均长线分: {analysis['avg_long_term_score']:.2f}")
            print(f"平均中线分: {analysis['avg_medium_term_score']:.2f}")
            print(f"平均超长线分: {analysis['avg_ultra_long_term_score']:.2f}")
            print(f"优秀股票(≥80分): {analysis['score_distribution']['excellent']} 只")
            print(f"良好股票(60-80分): {analysis['score_distribution']['good']} 只")
            print(f"一般股票(40-60分): {analysis['score_distribution']['average']} 只")
            print(f"较差股票(<40分): {analysis['score_distribution']['poor']} 只")
            
            # 保存结果到CSV文件
            output_file = f"旅游及景区板块评分结果_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            final_results.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"结果已保存到: {output_file}")
        else:
            logger.warning("旅游及景区板块没有股票通过评分流程")
    except Exception as e:
        print(f"程序运行出错: {e}")
        import traceback
        traceback.print_exc()