"""
股票分类指数映射器

根据stock_basic_pro表中的字段值，将股票分类到不同的指数中：
- 国企指数 (100001.ZS)
- B股指数 (100002.ZS)  
- H股指数 (100003.ZS)
- 老股指数 (100004.ZS)
- 大高指数 (100005.ZS)
- 高价指数 (100006.ZS)
- 低价指数 (100007.ZS)
- 次新指数 (100008.ZS)
- 超强指数 (100009.ZS)
"""

import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import numpy as np


class StockCategoryIndexMapper:
    """股票分类指数映射器"""
    
    def __init__(self, db_path=None):
        """
        初始化映射器
        
        Args:
            db_path: 数据库路径，默认为项目中的quant_system.db
        """
        if db_path is None:
            # 使用相对于当前文件的路径，确保路径正确
            current_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(current_dir, '..', 'databases', 'quant_system.db')
            db_path = os.path.abspath(db_path)
        
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}')
        
        # 定义指数映射表
        self.index_mapping = {
            '国企': {'index_code': '100001.ZS', 'index_name': '国企指数'},
            'B股': {'index_code': '100002.ZS', 'index_name': 'B股指数'},
            'H股': {'index_code': '100003.ZS', 'index_name': 'H股指数'},
            '老股': {'index_code': '100004.ZS', 'index_name': '老股指数'},
            '大高': {'index_code': '100005.ZS', 'index_name': '大高指数'},
            '高价': {'index_code': '100006.ZS', 'index_name': '高价指数'},
            '低价': {'index_code': '100007.ZS', 'index_name': '低价指数'},
            '次新': {'index_code': '100008.ZS', 'index_name': '次新指数'},
            '超强': {'index_code': '100009.ZS', 'index_name': '超强指数'}
        }
    
    def get_stock_categories(self):
        """
        从stock_basic_pro表获取股票分类信息
        
        Returns:
            pd.DataFrame: 包含股票代码、名称和分类字段的DataFrame
        """
        try:
            # 查询stock_basic_pro表
            query = """
                SELECT stock_code, stock_name, 国企, B股, H股, 老股, 大高, 高价, 低价, 次新, 超强
                FROM stock_basic_pro
                WHERE stock_code IS NOT NULL
            """
            
            df = pd.read_sql_query(query, self.engine)
            print(f"✓ 成功读取stock_basic_pro表，共{len(df)}条记录")
            
            return df
            
        except Exception as e:
            print(f"✗ 读取stock_basic_pro表失败: {e}")
            return pd.DataFrame()
    
    def create_category_mapping_table(self):
        """
        创建股票分类映射表
        
        Returns:
            pd.DataFrame: 股票分类映射表
        """
        # 获取股票分类信息
        df_stocks = self.get_stock_categories()
        
        if df_stocks.empty:
            print("✗ 无法获取股票数据")
            return pd.DataFrame()
        
        # 创建映射表
        mapping_records = []
        
        for _, stock in df_stocks.iterrows():
            stock_code = stock['stock_code']
            stock_name = stock['stock_name']
            
            # 检查每个分类字段
            for category, index_info in self.index_mapping.items():
                if stock[category] == 1 or stock[category] is True:
                    mapping_records.append({
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'category': category,
                        'index_code': index_info['index_code'],
                        'index_name': index_info['index_name']
                    })
        
        df_mapping = pd.DataFrame(mapping_records)
        
        if not df_mapping.empty:
            print(f"✓ 创建分类映射表完成，共{len(df_mapping)}条映射记录")
        else:
            print("⚠ 未找到任何符合条件的股票")
        
        return df_mapping
    
    def get_stocks_by_category(self, category):
        """
        根据分类获取股票列表
        
        Args:
            category: 分类名称（如'国企'、'B股'等）
            
        Returns:
            list: 股票代码列表
        """
        if category not in self.index_mapping:
            print(f"✗ 无效的分类名称: {category}")
            return []
        
        df_stocks = self.get_stock_categories()
        
        if df_stocks.empty:
            return []
        
        # 筛选符合条件的股票
        filtered_stocks = df_stocks[
            (df_stocks[category] == 1) | (df_stocks[category] == True)
        ]
        
        stock_codes = filtered_stocks['stock_code'].tolist()
        print(f"✓ {category}分类共有{len(stock_codes)}只股票")
        
        return stock_codes
    
    def get_all_category_summary(self):
        """
        获取所有分类的统计摘要
        
        Returns:
            pd.DataFrame: 分类统计摘要
        """
        df_stocks = self.get_stock_categories()
        
        if df_stocks.empty:
            return pd.DataFrame()
        
        summary_records = []
        
        for category, index_info in self.index_mapping.items():
            count = df_stocks[
                (df_stocks[category] == 1) | (df_stocks[category] == True)
            ].shape[0]
            
            summary_records.append({
                'category': category,
                'index_code': index_info['index_code'],
                'index_name': index_info['index_name'],
                'stock_count': count
            })
        
        df_summary = pd.DataFrame(summary_records)
        df_summary = df_summary.sort_values('stock_count', ascending=False)
        
        print("✓ 分类统计摘要:")
        for _, row in df_summary.iterrows():
            print(f"  {row['category']} ({row['index_code']}): {row['stock_count']}只股票")
        
        return df_summary
    
    def save_mapping_to_database(self, table_name='stock_category_mapping'):
        """
        将映射表保存到数据库
        
        Args:
            table_name: 表名
        """
        df_mapping = self.create_category_mapping_table()
        
        if df_mapping.empty:
            print("✗ 没有数据可保存")
            return False
        
        try:
            # 删除已存在的表
            with self.engine.connect() as conn:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # 保存新表
            df_mapping.to_sql(table_name, self.engine, index=False, if_exists='replace')
            
            print(f"✓ 成功保存映射表到数据库: {table_name}")
            print(f"  共{len(df_mapping)}条记录")
            
            return True
            
        except Exception as e:
            print(f"✗ 保存到数据库失败: {e}")
            return False
    
    def export_mapping_to_csv(self, filename=None):
        """
        导出映射表到CSV文件
        
        Args:
            filename: 文件名，默认为带时间戳的文件名
        """
        df_mapping = self.create_category_mapping_table()
        
        if df_mapping.empty:
            print("✗ 没有数据可导出")
            return False
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'stock_category_mapping_{timestamp}.csv'
        
        try:
            df_mapping.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✓ 成功导出映射表到CSV文件: {filename}")
            return True
            
        except Exception as e:
            print(f"✗ 导出CSV文件失败: {e}")
            return False
    
    def create_index_k_daily_table(self):
        """
        创建index_k_daily表（如果不存在）
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS index_k_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                index_code TEXT NOT NULL,
                index_name TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(index_code, trade_date)
            )
            """
            
            conn.execute(create_table_sql)
            
            # 创建索引
            conn.execute("CREATE INDEX IF NOT EXISTS idx_index_k_daily_code ON index_k_daily (index_code)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_index_k_daily_date ON index_k_daily (trade_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_index_k_daily_code_date ON index_k_daily (index_code, trade_date)")
            
            conn.commit()
            conn.close()
            print("✅ index_k_daily表创建成功或已存在")
            
        except Exception as e:
            print(f"❌ 创建index_k_daily表失败: {e}")
    
    def get_stock_kline_data(self, stock_codes, start_date=None, end_date=None):
        """
        获取股票的K线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
            
        Returns:
            pd.DataFrame: K线数据
        """
        try:
            if not stock_codes:
                return pd.DataFrame()
            
            # 构建查询条件
            placeholders = ','.join(['?' for _ in stock_codes])
            query = f"""
                SELECT stock_code, trade_date, open, high, low, close, volume
                FROM k_daily 
                WHERE stock_code IN ({placeholders})
            """
            params = list(stock_codes)
            
            if start_date:
                query += " AND trade_date >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND trade_date <= ?"
                params.append(end_date)
            
            query += " ORDER BY trade_date, stock_code"
            
            # 使用sqlite3直接连接而不是pandas的read_sql_query
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            print(f"✓ 获取K线数据成功，共{len(df)}条记录")
            
            return df
            
        except Exception as e:
            print(f"✗ 获取K线数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_index_kline(self, stock_codes, index_code, index_name, start_date=None, end_date=None):
        """
        计算板块指数的K线数据
        
        Args:
            stock_codes: 股票代码列表
            index_code: 指数代码
            index_name: 指数名称
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 指数K线数据
        """
        if not stock_codes:
            print(f"⚠ {index_name}没有成分股，跳过计算")
            return pd.DataFrame()
        
        print(f"📊 计算{index_name}({index_code})指数...")
        print(f"   成分股数量: {len(stock_codes)}")
        
        # 获取股票K线数据
        df_kline = self.get_stock_kline_data(stock_codes, start_date, end_date)
        
        if df_kline.empty:
            print(f"⚠ {index_name}没有K线数据，跳过计算")
            return pd.DataFrame()
        
        # 按日期分组计算指数
        index_data = []
        
        for trade_date, group in df_kline.groupby('trade_date'):
            # 过滤掉停牌股票（成交量为0或缺失）
            active_stocks = group[group['volume'] > 0].copy()
            
            if len(active_stocks) == 0:
                continue
            
            # 计算市值加权平均价格
            # 使用收盘价作为权重（简化处理，实际应该用流通市值）
            weights = active_stocks['close'] / active_stocks['close'].sum()
            
            # 计算加权平均的开高低收
            index_open = (active_stocks['open'] * weights).sum()
            index_high = (active_stocks['high'] * weights).sum()
            index_low = (active_stocks['low'] * weights).sum()
            index_close = (active_stocks['close'] * weights).sum()
            
            # 计算总成交量
            index_volume = active_stocks['volume'].sum()
            
            index_data.append({
                'index_code': index_code,
                'index_name': index_name,
                'trade_date': trade_date,
                'open': round(index_open, 2),
                'high': round(index_high, 2),
                'low': round(index_low, 2),
                'close': round(index_close, 2),
                'volume': int(index_volume)
            })
        
        df_index = pd.DataFrame(index_data)
        
        if not df_index.empty:
            print(f"✓ {index_name}指数计算完成，共{len(df_index)}个交易日")
        else:
            print(f"⚠ {index_name}指数计算失败，无有效数据")
        
        return df_index
    
    def save_index_data_to_db(self, index_data, table_name='index_k_daily', replace_existing=True):
        """
        保存指数数据到数据库
        
        Args:
            index_data: 指数数据DataFrame
            table_name: 表名
            replace_existing: 是否替换已存在的数据
        """
        if index_data.empty:
            print("⚠ 没有指数数据可保存")
            return False
        
        try:
            # 确保表存在
            self.create_index_k_daily_table()
            
            if replace_existing:
                # 先删除已存在的数据，再插入新数据
                for _, row in index_data.iterrows():
                    delete_query = """
                        DELETE FROM index_k_daily 
                        WHERE index_code = ? AND trade_date = ?
                    """
                    conn = sqlite3.connect(self.db_path)
                    conn.execute(delete_query, (row['index_code'], row['trade_date']))
                    conn.commit()
                    conn.close()
                
                # 插入新数据
                index_data.to_sql(table_name, self.engine, if_exists='append', index=False)
                print(f"✓ 成功保存{len(index_data)}条指数数据到{table_name}表（已替换重复数据）")
            else:
                # 直接追加，遇到重复会报错
                index_data.to_sql(table_name, self.engine, if_exists='append', index=False)
                print(f"✓ 成功保存{len(index_data)}条指数数据到{table_name}表")
            
            return True
            
        except Exception as e:
            print(f"✗ 保存指数数据失败: {e}")
            return False
    
    def calculate_all_category_indices(self, start_date=None, end_date=None, save_to_db=True, replace_existing=True):
        """
        计算所有分类指数的K线数据
        
        Args:
            start_date: 开始日期，格式'YYYY-MM-DD'
            end_date: 结束日期，格式'YYYY-MM-DD'
            save_to_db: 是否保存到数据库
            replace_existing: 是否替换已存在的数据
            
        Returns:
            dict: 各分类指数的数据
        """
        print("🚀 开始计算所有分类指数...")
        
        # 获取所有分类的统计信息
        summary = self.get_all_category_summary()
        
        if summary.empty:
            print("✗ 没有分类数据可计算")
            return {}
        
        all_index_data = {}
        total_records = 0
        
        # 计算每个分类的指数
        for _, row in summary.iterrows():
            category = row['category']
            index_code = row['index_code']
            index_name = row['index_name']
            
            # 获取该分类的股票列表
            stock_codes = self.get_stocks_by_category(category)
            
            # 计算指数K线数据
            index_data = self.calculate_index_kline(
                stock_codes, index_code, index_name, start_date, end_date
            )
            
            if not index_data.empty:
                all_index_data[category] = index_data
                total_records += len(index_data)
                
                # 保存到数据库
                if save_to_db:
                    self.save_index_data_to_db(index_data, replace_existing=replace_existing)
        
        print(f"✅ 所有分类指数计算完成！")
        print(f"   共计算{len(all_index_data)}个指数")
        print(f"   总记录数: {total_records}")
        
        return all_index_data
    
    def get_index_performance_summary(self, index_code, days=30):
        """
        获取指数表现摘要
        
        Args:
            index_code: 指数代码
            days: 统计天数
            
        Returns:
            dict: 表现摘要
        """
        try:
            # 获取最近N天的数据
            query = """
                SELECT trade_date, open, high, low, close, volume
                FROM index_k_daily 
                WHERE index_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn, params=[index_code, days])
            conn.close()
            
            if df.empty:
                return {}
            
            # 计算统计指标
            latest_close = df.iloc[0]['close']
            oldest_close = df.iloc[-1]['close']
            total_return = (latest_close - oldest_close) / oldest_close * 100
            
            max_price = df['high'].max()
            min_price = df['low'].min()
            avg_volume = df['volume'].mean()
            
            return {
                'index_code': index_code,
                'latest_close': latest_close,
                'total_return_pct': round(total_return, 2),
                'max_price': max_price,
                'min_price': min_price,
                'avg_volume': int(avg_volume),
                'trading_days': len(df)
            }
            
        except Exception as e:
            print(f"✗ 获取指数表现摘要失败: {e}")
            return {}


def main():
    """主函数 - 演示使用方法"""
    print("=== 股票分类指数映射器演示 ===\n")
    
    # 创建映射器实例
    mapper = StockCategoryIndexMapper()
    
    # 1. 获取所有分类的统计摘要
    print("1. 获取分类统计摘要:")
    summary = mapper.get_all_category_summary()
    print()
    
    # 2. 获取特定分类的股票列表（以国企为例）
    print("2. 获取国企股票列表:")
    guoqi_stocks = mapper.get_stocks_by_category('国企')
    if guoqi_stocks:
        print(f"   前10只国企股票: {guoqi_stocks[:10]}")
    print()
    
    # 3. 创建完整的映射表
    print("3. 创建完整映射表:")
    mapping_df = mapper.create_category_mapping_table()
    if not mapping_df.empty:
        print(f"   映射表预览（前5条）:")
        print(mapping_df.head().to_string(index=False))
    print()
    
    # 4. 保存到数据库
    print("4. 保存映射表到数据库:")
    mapper.save_mapping_to_database()
    print()
    
    # 5. 导出到CSV
    print("5. 导出映射表到CSV:")
    mapper.export_mapping_to_csv()
    print()
    
    # 6. 计算板块指数（新增功能）
    print("6. 计算板块指数并保存到index_k_daily表:")
    print("   计算最近30天的指数数据...")
    
    # # 设置日期范围（最近30天）
    # end_date = datetime.now().strftime('%Y-%m-%d')
    # start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        # 定义参数
    start_date = '2020-02-03'
    end_date = '2025-09-11'
    
    # 计算所有分类指数
    all_index_data = mapper.calculate_all_category_indices(
        start_date=start_date, 
        end_date=end_date, 
        save_to_db=True
    )
    print()
    
    # 7. 显示指数表现摘要
    print("7. 指数表现摘要（最近30天）:")
    for category, index_info in mapper.index_mapping.items():
        if category in all_index_data:
            performance = mapper.get_index_performance_summary(index_info['index_code'], days=30)
            if performance:
                print(f"   {index_info['index_name']}: 收盘价={performance['latest_close']}, "
                      f"涨跌幅={performance['total_return_pct']}%, "
                      f"交易天数={performance['trading_days']}")
    print()
    
    print("✅ 演示完成！")


if __name__ == "__main__":
    main()
