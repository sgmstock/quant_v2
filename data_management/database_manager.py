"""
数据库管理器

负责数据库连接、表管理、数据存储和检索
基于 quant_v2/data/data_handle.py 的核心功能
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime, date, time, timedelta
from sqlalchemy import create_engine, text
from typing import Optional, List, Dict, Any
from pathlib import Path

from core.utils.logger import get_logger

logger = get_logger("data_management.database_manager")


class DatabaseManager:
    """数据库管理器"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, db_path: str = None):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, db_path: str = None):
        if self._initialized:
            return
            
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径，如果为None则使用默认路径
        """
        if db_path is None:
            # 使用绝对路径确保从任何目录运行都能找到数据库
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            db_path = os.path.join(current_dir, 'databases', 'quant_system.db')
            db_path = os.path.abspath(db_path)
        
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}')
        self._ensure_database_directory()
        self._create_tables()
        self._initialized = True
    
    def _ensure_database_directory(self):
        """确保数据库目录存在"""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
    
    def _create_tables(self):
        """创建必要的数据库表"""
        try:
            with self.engine.connect() as conn:
                # 创建交易日历表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS trade_calendar (
                        trade_date TEXT PRIMARY KEY,
                        trade_status INTEGER DEFAULT 1
                    )
                """))
                
                # 创建日线数据表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS k_daily (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        open REAL NOT NULL,
                        close REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        volume INTEGER NOT NULL,
                        UNIQUE(stock_code, trade_date)
                    )
                """))
                
                # 创建周线数据表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS k_weekly (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        open REAL NOT NULL,
                        close REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        volume INTEGER NOT NULL,
                        UNIQUE(stock_code, trade_date)
                    )
                """))
                
                # 创建月线数据表
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS k_monthly (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        open REAL NOT NULL,
                        close REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        volume INTEGER NOT NULL,
                        UNIQUE(stock_code, trade_date)
                    )
                """))
                
                logger.info("数据库表创建成功")
                
        except Exception as e:
            logger.error(f"创建数据库表失败: {e}")
            raise
    
    def get_last_trade_date(self, today_date: Optional[str] = None) -> Optional[str]:
        """
        获取最新交易日
        
        Args:
            today_date: 指定日期，格式为'YYYY-MM-DD'。如果不指定，则使用当前日期
            
        Returns:
            str: 最新交易日，格式为'YYYY-MM-DD'，如果未找到则返回None
        """
        try:
            if today_date is None:
                today_date = datetime.now().strftime('%Y-%m-%d')
            
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT trade_date FROM trade_calendar
                    WHERE trade_date <= :today_date AND trade_status = 1
                    ORDER BY trade_date DESC
                    LIMIT 1
                """), {"today_date": today_date})
                
                row = result.fetchone()
                if row:
                    return row[0]
                return None
                
        except Exception as e:
            logger.error(f"获取最新交易日失败: {e}")
            return None
    
    def save_stock_data(self, data: pd.DataFrame, table_name: str, 
                       conflict_resolution: str = "replace") -> bool:
        """
        保存股票数据到数据库
        
        Args:
            data: 股票数据DataFrame，必须包含列：stock_code, trade_date, open, close, high, low, volume
            table_name: 表名 (k_daily, k_weekly, k_monthly)
            conflict_resolution: 冲突解决策略 ("replace", "ignore", "update")
            
        Returns:
            bool: 保存是否成功
        """
        try:
            # 验证数据格式
            required_columns = ['stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume']
            if not all(col in data.columns for col in required_columns):
                logger.error(f"数据缺少必需列: {required_columns}")
                return False
            
            if data.empty:
                logger.warning("数据为空，跳过保存")
                return True
            
            # 根据冲突解决策略处理数据
            if conflict_resolution == "replace":
                success = self._save_with_replace(data, table_name)
            elif conflict_resolution == "ignore":
                success = self._save_with_ignore(data, table_name)
            elif conflict_resolution == "update":
                success = self._save_with_update(data, table_name)
            else:
                logger.error(f"不支持的冲突解决策略: {conflict_resolution}")
                return False
            
            if success:
                logger.info(f"成功保存 {len(data)} 条数据到 {table_name} (策略: {conflict_resolution})")
                return True
            else:
                logger.error(f"保存数据到 {table_name} 失败")
                return False
            
        except Exception as e:
            logger.error(f"保存数据到 {table_name} 失败: {e}")
            return False
    
    def _save_with_replace(self, data: pd.DataFrame, table_name: str) -> bool:
        """
        使用 INSERT OR REPLACE 策略保存数据
        
        Args:
            data: 股票数据DataFrame
            table_name: 表名
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with self.engine.connect() as conn:
                # 使用 INSERT OR REPLACE 语句
                for _, row in data.iterrows():
                    conn.execute(text(f"""
                        INSERT OR REPLACE INTO {table_name} 
                        (stock_code, trade_date, open, close, high, low, volume)
                        VALUES (:stock_code, :trade_date, :open, :close, :high, :low, :volume)
                    """), {
                        'stock_code': row['stock_code'],
                        'trade_date': row['trade_date'],
                        'open': row['open'],
                        'close': row['close'],
                        'high': row['high'],
                        'low': row['low'],
                        'volume': row['volume']
                    })
                return True
                
        except Exception as e:
            logger.error(f"使用 REPLACE 策略保存数据失败: {e}")
            return False
    
    def _save_with_ignore(self, data: pd.DataFrame, table_name: str) -> bool:
        """
        使用 INSERT OR IGNORE 策略保存数据
        
        Args:
            data: 股票数据DataFrame
            table_name: 表名
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with self.engine.connect() as conn:
                # 使用 INSERT OR IGNORE 语句
                for _, row in data.iterrows():
                    conn.execute(text(f"""
                        INSERT OR IGNORE INTO {table_name} 
                        (stock_code, trade_date, open, close, high, low, volume)
                        VALUES (:stock_code, :trade_date, :open, :close, :high, :low, :volume)
                    """), {
                        'stock_code': row['stock_code'],
                        'trade_date': row['trade_date'],
                        'open': row['open'],
                        'close': row['close'],
                        'high': row['high'],
                        'low': row['low'],
                        'volume': row['volume']
                    })
                return True
                
        except Exception as e:
            logger.error(f"使用 IGNORE 策略保存数据失败: {e}")
            return False
    
    def _save_with_update(self, data: pd.DataFrame, table_name: str) -> bool:
        """
        使用 UPDATE 策略保存数据（先删除再插入）
        
        Args:
            data: 股票数据DataFrame
            table_name: 表名
            
        Returns:
            bool: 保存是否成功
        """
        try:
            with self.engine.connect() as conn:
                # 先删除要更新的数据，再插入新数据
                for _, row in data.iterrows():
                    # 删除已存在的数据
                    conn.execute(text(f"""
                        DELETE FROM {table_name} 
                        WHERE stock_code = :stock_code AND trade_date = :trade_date
                    """), {
                        'stock_code': row['stock_code'],
                        'trade_date': row['trade_date']
                    })
                    
                    # 插入新数据
                    conn.execute(text(f"""
                        INSERT INTO {table_name} 
                        (stock_code, trade_date, open, close, high, low, volume)
                        VALUES (:stock_code, :trade_date, :open, :close, :high, :low, :volume)
                    """), {
                        'stock_code': row['stock_code'],
                        'trade_date': row['trade_date'],
                        'open': row['open'],
                        'close': row['close'],
                        'high': row['high'],
                        'low': row['low'],
                        'volume': row['volume']
                    })
                return True
                
        except Exception as e:
            logger.error(f"使用 UPDATE 策略保存数据失败: {e}")
            return False
    
    def save_stock_data_batch(self, data: pd.DataFrame, table_name: str, 
                            conflict_resolution: str = "replace", batch_size: int = 1000) -> bool:
        """
        批量保存股票数据（性能优化版本）
        
        Args:
            data: 股票数据DataFrame
            table_name: 表名
            conflict_resolution: 冲突解决策略
            batch_size: 批处理大小
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if data.empty:
                logger.warning("数据为空，跳过保存")
                return True
            
            # 分批处理数据
            total_batches = (len(data) + batch_size - 1) // batch_size
            logger.info(f"开始批量保存 {len(data)} 条数据，分 {total_batches} 批处理")
            
            for i in range(0, len(data), batch_size):
                batch_data = data.iloc[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                logger.info(f"处理第 {batch_num}/{total_batches} 批数据 ({len(batch_data)} 条)")
                
                success = self.save_stock_data(batch_data, table_name, conflict_resolution)
                if not success:
                    logger.error(f"第 {batch_num} 批数据保存失败")
                    return False
            
            logger.info(f"批量保存完成，共处理 {len(data)} 条数据")
            return True
            
        except Exception as e:
            logger.error(f"批量保存数据失败: {e}")
            return False
    
    def get_stock_data(self, stock_code: str, start_date: str, end_date: str, 
                      table_name: str = "k_daily") -> pd.DataFrame:
        """
        获取股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            table_name: 表名 (k_daily, k_weekly, k_monthly)
            
        Returns:
            pd.DataFrame: 股票数据
        """
        try:
            query = f"""
                SELECT stock_code, trade_date, open, close, high, low, volume
                FROM {table_name}
                WHERE stock_code = :stock_code 
                AND trade_date BETWEEN :start_date AND :end_date
                ORDER BY trade_date
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {
                    "stock_code": stock_code,
                    "start_date": start_date,
                    "end_date": end_date
                })
                
                data = pd.DataFrame(result.fetchall(), columns=[
                    'stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume'
                ])
                
                logger.info(f"成功获取 {len(data)} 条 {stock_code} 数据")
                return data
                
        except Exception as e:
            logger.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
    
    def get_latest_data_date(self, stock_code: str, table_name: str = "k_daily") -> Optional[str]:
        """
        获取股票最新数据日期
        
        Args:
            stock_code: 股票代码
            table_name: 表名
            
        Returns:
            str: 最新数据日期，如果没有数据则返回None
        """
        try:
            query = f"""
                SELECT MAX(trade_date) as latest_date
                FROM {table_name}
                WHERE stock_code = :stock_code
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"stock_code": stock_code})
                row = result.fetchone()
                
                if row and row[0]:
                    return row[0]
                return None
                
        except Exception as e:
            logger.error(f"获取最新数据日期失败: {e}")
            return None
    
    def get_latest_stock_data(self, stock_code: str, table_name: str = "k_daily", limit: int = 5) -> pd.DataFrame:
        """
        获取股票最新的N条数据记录（按日期降序）
        
        Args:
            stock_code: 股票代码
            table_name: 表名
            limit: 返回的记录数限制
            
        Returns:
            pd.DataFrame: 股票数据
        """
        try:
            query = f"""
                SELECT stock_code, trade_date, open, close, high, low, volume
                FROM {table_name}
                WHERE stock_code = :stock_code
                ORDER BY trade_date DESC
                LIMIT :limit
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {
                    "stock_code": stock_code,
                    "limit": limit
                })
                
                data = pd.DataFrame(result.fetchall(), columns=[
                    'stock_code', 'trade_date', 'open', 'close', 'high', 'low', 'volume'
                ])
                
                # 按日期升序排列，保持时间顺序
                if not data.empty:
                    data = data.sort_values('trade_date').reset_index(drop=True)
                
                logger.info(f"成功获取 {len(data)} 条 {stock_code} 最新数据")
                return data
                
        except Exception as e:
            logger.error(f"获取最新股票数据失败: {e}")
            return pd.DataFrame()
    
    def get_stock_list(self, table_name: str = "k_daily") -> List[str]:
        """
        获取数据库中的股票代码列表
        
        Args:
            table_name: 表名
            
        Returns:
            List[str]: 股票代码列表
        """
        try:
            query = f"""
                SELECT DISTINCT stock_code
                FROM {table_name}
                ORDER BY stock_code
            """
            
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                stock_codes = [row[0] for row in result.fetchall()]
                
                logger.info(f"获取到 {len(stock_codes)} 个股票代码")
                return stock_codes
                
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {e}")
            return []
    
    def get_data_summary(self, table_name: str = "k_daily") -> Dict[str, Any]:
        """
        获取数据摘要信息
        
        Args:
            table_name: 表名
            
        Returns:
            Dict[str, Any]: 数据摘要
        """
        try:
            with self.engine.connect() as conn:
                # 获取总记录数
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                total_count = count_result.fetchone()[0]
                
                # 获取股票数量
                stock_result = conn.execute(text(f"SELECT COUNT(DISTINCT stock_code) FROM {table_name}"))
                stock_count = stock_result.fetchone()[0]
                
                # 获取日期范围
                date_result = conn.execute(text(f"""
                    SELECT MIN(trade_date), MAX(trade_date) FROM {table_name}
                """))
                date_range = date_result.fetchone()
                
                summary = {
                    "table_name": table_name,
                    "total_records": total_count,
                    "stock_count": stock_count,
                    "date_range": {
                        "start": date_range[0],
                        "end": date_range[1]
                    }
                }
                
                logger.info(f"数据摘要: {summary}")
                return summary
                
        except Exception as e:
            logger.error(f"获取数据摘要失败: {e}")
            return {}
    
    def execute_query(self, query: str, params=None) -> pd.DataFrame:
        """
        执行SQL查询并返回DataFrame
        
        Args:
            query: SQL查询语句
            params: 查询参数，可以是字典或元组
            
        Returns:
            pd.DataFrame: 查询结果
        """
        try:
            # 使用原始sqlite3连接，而不是SQLAlchemy连接
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            
            if params:
                # 使用pandas的read_sql_query方法，能正确处理参数化查询
                if isinstance(params, dict):
                    df = pd.read_sql_query(query, conn, params=params)
                else:
                    df = pd.read_sql_query(query, conn, params=params)
            else:
                df = pd.read_sql_query(query, conn)
            
            conn.close()
            
            logger.info(f"成功执行查询，返回 {len(df)} 行数据")
            return df
                
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            return pd.DataFrame()
    
    def execute_ddl(self, query: str) -> bool:
        """
        执行DDL操作（CREATE TABLE, ALTER TABLE等）
        
        Args:
            query: DDL语句
            
        Returns:
            bool: 执行是否成功
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query))
                logger.info("DDL操作执行成功")
                return True
        except Exception as e:
            logger.error(f"DDL操作执行失败: {e}")
            return False
    
    def execute_dml(self, query: str, params=None) -> bool:
        """
        执行DML操作（INSERT, UPDATE, DELETE等）
        
        Args:
            query: DML语句
            params: 查询参数，可以是字典、元组或列表
            
        Returns:
            bool: 执行是否成功
        """
        try:
            with self.engine.begin() as conn:
                if params:
                    # 处理不同类型的参数
                    if isinstance(params, dict):
                        conn.execute(text(query), params)
                    elif isinstance(params, (tuple, list)):
                        # 将元组/列表转换为字典格式，使用位置参数
                        # SQLAlchemy的text()需要命名参数，所以我们需要转换
                        # 这里使用原始sqlite3连接来处理元组参数
                        import sqlite3
                        sqlite_conn = sqlite3.connect(self.db_path)
                        cursor = sqlite_conn.cursor()
                        cursor.execute(query, params)
                        sqlite_conn.commit()
                        sqlite_conn.close()
                    else:
                        conn.execute(text(query), params)
                else:
                    conn.execute(text(query))
                logger.info("DML操作执行成功")
                return True
        except Exception as e:
            logger.error(f"DML操作执行失败: {e}")
            return False
    
    def batch_insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                              conflict_resolution: str = "replace") -> bool:
        """
        批量插入DataFrame数据到数据库表
        
        Args:
            df: 要插入的DataFrame
            table_name: 目标表名
            conflict_resolution: 冲突解决策略 ('replace', 'ignore', 'update')
            
        Returns:
            bool: 插入是否成功
        """
        if df.empty:
            logger.warning("DataFrame为空，跳过插入")
            return True
            
        try:
            with self.engine.begin() as conn:
                # 使用pandas的to_sql方法进行批量插入
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists='append',
                    index=False,
                    method='multi'  # 使用批量插入
                )
                logger.info(f"成功批量插入 {len(df)} 行数据到 {table_name} 表")
                return True
        except Exception as e:
            logger.error(f"批量插入失败: {e}")
            return False