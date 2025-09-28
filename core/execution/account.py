"""
账户管理

负责：
1. 资金管理
2. 持仓管理
3. 交易记录
4. 风险控制
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import os
import sqlite3
import datetime as dt

logger = logging.getLogger(__name__)




class Account:
    def __init__(self, starting_cash, db_path=None):
        """
        初始化账户，使用SQLite数据库。

        参数:
        starting_cash (float): 初始投入资金
        db_path (str): 数据库文件路径
        """
        self.starting_cash = starting_cash
        self.available_cash = starting_cash
        self.positions = {}
        
        # 设置数据库路径 - 适配v2项目结构
        if db_path is None:
            # 使用v2项目的数据库路径
            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            db_path = os.path.join(current_dir, 'databases', 'quant_system.db')
        
        self.db_path = db_path
        self.table_name = f'positions_{int(starting_cash)}'
        
        # 确保目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        # 确保表存在 (虽然setup脚本已创建，但这是个好习惯)
        self._initialize_db()
        # 初始化时自动重构持仓
        self.reconstruct_positions()

    def _initialize_db(self):
        """
        初始化数据库表。如果表不存在，创建表结构。
        """
        cursor = self.conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_code TEXT NOT NULL,
                trade_amount INTEGER NOT NULL,
                trade_price REAL NOT NULL,
                commission REAL NOT NULL,
                trade_time TEXT NOT NULL
            )
        ''')
        self.conn.commit()
        print(f"✓ '{self.table_name}' 表已确认存在。")

    class Position:
        def __init__(self, trade_code, total_amount=0, current_price=0):
            self.trade_code = trade_code
            self.current_price = current_price  # 实时价格
            self.trade_price = 0  # 最后一次交易价格
            self.total_cost = 0  # 持仓的总成本
            self.avg_cost = 0  # 持仓的平均成本，仅在买入时更新
            self.init_time = None  # 个股的第一次建仓时间
            self.transact_time = None  # 最后交易时间
            self.total_amount = total_amount
            self.value = total_amount * current_price
            self.closeable_amount = 0  # 可卖出的仓位
            self.today_bought = 0  # 今日买入的数量

        def update(self, trade_amount, trade_price, trade_time):
            commission = 3  # 始终固定为3元手续费

            if self.total_amount == 0 and trade_amount > 0:
                self.init_time = trade_time

            self.total_amount += trade_amount
            self.trade_price = trade_price  # 记录最后一次交易价格
            self.transact_time = trade_time

            trade_value = trade_price * trade_amount

            if trade_amount > 0:  # 买入操作
                self.total_cost += trade_value + commission
                self.avg_cost = self.total_cost / self.total_amount
                if trade_time.date() == dt.datetime.now().date():
                    self.today_bought += trade_amount
            else:  # 卖出操作
                sell_amount = abs(trade_amount)
                self.total_cost -= sell_amount * self.avg_cost
                self.closeable_amount -= sell_amount

            if self.total_amount > 0:
                self.value = self.total_amount * self.current_price
                self.closeable_amount = self.total_amount - self.today_bought
            else:
                self.value = 0
                self.avg_cost = 0
                self.total_cost = 0
                self.closeable_amount = 0
                self.today_bought = 0

    def load_trades(self):
        """
        从SQLite数据库加载所有交易记录

        返回:
        list of dict: 所有交易记录
        """
        trades = []
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(f'''
                SELECT trade_code, trade_amount, trade_price, commission, trade_time 
                FROM {self.table_name} 
                ORDER BY trade_time
            ''')
            
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    # 转换数据类型
                    processed_row = {
                        'trade_code': str(row[0]).strip(),
                        'trade_amount': int(row[1]),
                        'trade_price': float(row[2]),
                        'commission': float(row[3]),
                        'trade_time': dt.datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S')
                    }
                    trades.append(processed_row)
                    
                except ValueError as e:
                    print(f"❌ 数据格式错误: {e}")
                    print(f"问题行数据: {row}")
                    # 记录错误到日志
                    with open('trading_errors.log', 'a', encoding='utf-8') as log_file:
                        log_file.write(f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 数据库读取错误: {e}\n")
                        log_file.write(f"问题行数据: {row}\n\n")
                    continue
                except Exception as e:
                    print(f"❌ 处理时发生未知错误: {e}")
                    continue
                        
        except Exception as e:
            print(f"❌ 读取数据库时发生错误: {e}")
            with open('trading_errors.log', 'a', encoding='utf-8') as log_file:
                log_file.write(f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 数据库读取错误: {e}\n\n")
        
        print(f"✅ 成功加载 {len(trades)} 条交易记录")
        return trades

    def save_trade(self, trade):
        """
        保存一条新的交易记录到SQLite数据库

        参数:
        trade (dict): 交易记录
        """
        try:
            # 1. 数据验证和清理
            trade_data = self._validate_and_clean_trade_data(trade)
            if not trade_data:
                return False
            
            # 2. 插入到数据库
            cursor = self.conn.cursor()
            cursor.execute(f'''
                INSERT INTO {self.table_name} 
                (trade_code, trade_amount, trade_price, commission, trade_time)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                trade_data['trade_code'],
                trade_data['trade_amount'],
                trade_data['trade_price'],
                trade_data['commission'],
                trade_data['trade_time']
            ))
            
            # 3. 提交事务
            self.conn.commit()
            
            print(f"✅ 交易记录已保存: {trade_data['trade_code']} {trade_data['trade_amount']}股 @{trade_data['trade_price']}")
            return True
            
        except Exception as e:
            print(f"❌ 保存交易记录时发生错误: {e}")
            print(f"交易数据: {trade}")
            # 回滚事务
            self.conn.rollback()
            # 记录错误到日志文件
            with open('trading_errors.log', 'a', encoding='utf-8') as log_file:
                log_file.write(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - 数据库写入错误: {e}\n")
                log_file.write(f"交易数据: {trade}\n\n")
            return False
    
    def _validate_and_clean_trade_data(self, trade):
        """
        验证和清理交易数据 - 增强版本
        
        功能:
        1. 严格验证所有必要字段的存在性和格式
        2. 验证股票代码格式（6位数字）
        3. 验证交易数量和价格的合理性
        4. 验证手续费范围
        5. 验证时间格式
        6. 防止SQL注入和数据污染
        
        返回:
        dict: 清理后的交易数据，如果验证失败返回None
        """
        try:
            # 1. 验证必要字段存在性
            required_fields = ['trade_code', 'trade_amount', 'trade_price', 'commission', 'trade_time']
            for field in required_fields:
                if field not in trade:
                    print(f"❌ 缺少必要字段: {field}")
                    return None
                if trade[field] is None:
                    print(f"❌ 字段 {field} 不能为空")
                    return None
            
            # 2. 验证和清理股票代码
            trade_code = str(trade['trade_code']).strip()
            if not trade_code:
                print(f"❌ 股票代码不能为空")
                return None
            if len(trade_code) != 6:
                print(f"❌ 股票代码长度必须为6位: {trade_code} (长度: {len(trade_code)})")
                return None
            if not trade_code.isdigit():
                print(f"❌ 股票代码必须为纯数字: {trade_code}")
                return None
            
            # 3. 验证交易数量
            try:
                trade_amount = int(float(trade['trade_amount']))  # 先转float再转int，处理"100.0"这种情况
            except (ValueError, TypeError):
                print(f"❌ 交易数量格式错误: {trade['trade_amount']}")
                return None
            
            if trade_amount == 0:
                print(f"❌ 交易数量不能为0: {trade_amount}")
                return None
            if abs(trade_amount) > 1000000:  # 防止异常大的交易量
                print(f"❌ 交易数量过大，可能存在数据错误: {trade_amount}")
                return None
            
            # 4. 验证交易价格
            try:
                trade_price = float(trade['trade_price'])
            except (ValueError, TypeError):
                print(f"❌ 交易价格格式错误: {trade['trade_price']}")
                return None
            
            if trade_price <= 0:
                print(f"❌ 交易价格必须大于0: {trade_price}")
                return None
            if trade_price > 10000:  # 防止异常高的价格
                print(f"❌ 交易价格异常高，可能存在数据错误: {trade_price}")
                return None
            
            # 5. 验证手续费
            try:
                commission = float(trade['commission'])
            except (ValueError, TypeError):
                print(f"❌ 手续费格式错误: {trade['commission']}")
                return None
            
            if commission < 0:
                print(f"❌ 手续费不能为负数: {commission}")
                return None
            if commission > 1000:  # 防止异常高的手续费
                print(f"❌ 手续费异常高，可能存在数据错误: {commission}")
                return None
            
            # 6. 验证和处理时间字段
            if hasattr(trade['trade_time'], 'strftime'):
                # 如果是datetime对象
                trade_time = trade['trade_time'].strftime('%Y-%m-%d %H:%M:%S')
            else:
                # 如果是字符串
                trade_time = str(trade['trade_time']).strip()
                if not trade_time:
                    print(f"❌ 交易时间不能为空")
                    return None
                
                # 验证时间格式
                try:
                    # 尝试解析时间，确保格式正确
                    parsed_time = dt.datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%S')
                    # 检查时间是否合理（不能是未来时间太久，也不能是太久以前）
                    now = dt.datetime.now()
                    if parsed_time > now:
                        print(f"⚠️ 警告: 交易时间为未来时间: {trade_time}")
                    elif (now - parsed_time).days > 365 * 10:  # 超过10年
                        print(f"⚠️ 警告: 交易时间过于久远: {trade_time}")
                except ValueError:
                    print(f"❌ 时间格式错误，应为 'YYYY-MM-DD HH:MM:SS': {trade_time}")
                    return None
            
            # 7. 返回清理后的数据
            cleaned_data = {
                'trade_code': trade_code,
                'trade_amount': trade_amount,
                'trade_price': round(trade_price, 2),  # 价格保留2位小数
                'commission': round(commission, 2),    # 手续费保留2位小数
                'trade_time': trade_time
            }
            
            return cleaned_data
            
        except Exception as e:
            print(f"❌ 数据验证过程中发生未知错误: {e}")
            print(f"原始交易数据: {trade}")
            return None
    

    def get_current_price(self, trade_code):
        """
        获取当前价格的函数，调用data_processor的get_latest_price

        返回:
        float: 实时价格
        """
        # 导入data_processor的get_latest_price函数
        import sys
        import os
        # 添加项目根目录到Python路径
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if current_dir not in sys.path:
            sys.path.append(current_dir)
        
        from data_management.data_processor import get_latest_price
        return get_latest_price(trade_code)

    def reconstruct_positions(self): 
        """
        根据交易记录重构当前持仓并计算可用资金
        """
        self.positions = {}
        self.available_cash = self.starting_cash  # 重置可用资金

        trades = self.load_trades()
        #print(f"加载到的交易记录: {trades}")  # 调试输出

        for trade in sorted(trades, key=lambda x: x['trade_time']):
            trade_code = trade['trade_code']
            amount = trade['trade_amount']
            trade_price = trade['trade_price']
            trade_time = trade['trade_time']

            if trade_code not in self.positions:
                self.positions[trade_code] = self.Position(trade_code)

            # 更新可用资金
            self.available_cash -= (amount * trade_price + trade['commission'])

            self.positions[trade_code].update(amount, trade_price, trade_time)

        # 使用 list(self.positions.keys()) 创建键的副本，以避免在迭代时修改字典
        for trade_code in list(self.positions.keys()):
            position = self.positions[trade_code]
            if position.total_amount > 0:
                position.current_price = self.get_current_price(trade_code)
                # 处理current_price为None的情况
                if position.current_price is None:
                    print(f"警告: 无法获取 {trade_code} 的当前价格，使用最后交易价格")
                    position.current_price = position.trade_price if position.trade_price > 0 else 0.0
                position.value = position.total_amount * position.current_price
            else:
                del self.positions[trade_code]

        print(f"当前持仓: {self.positions.keys()}")  # 调试输出

    def update_position(self, trade_code, amount, trade_price, trade_time=None):
        """
        更新持仓信息并保存交易

        参数:
        trade_code: 标的代码
        amount: 交易数量（正数为买入，负数为卖出）
        trade_price: 交易价格
        trade_time: 交易时间（可选，默认为当前时间）

        返回:
        Order: 创建的订单对象
        """
        if trade_time is None:
            trade_time = dt.datetime.now()

        trade = {
            'trade_code': trade_code,
            'trade_amount': amount,
            'trade_price': trade_price,
            'commission': 3,  # 始终固定为3元手续费
            'trade_time': trade_time
        }

        self.save_trade(trade)

        # 更新持仓信息
        if trade_code not in self.positions:
            self.positions[trade_code] = self.Position(trade_code)
        self.positions[trade_code].update(amount, trade_price, trade_time)

        # 更新可用资金
        self.available_cash -= (amount * trade_price + 3)  # 3是固定手续费

        # 创建并返回订单对象
        direction = 'buy' if amount > 0 else 'sell'
        return Order(trade_code, amount, trade_price, direction)

    def order_buy(self, trade_code, amount):
        """
        创建买单

        参数:
        trade_code: 标的代码
        amount: 买入数量

        返回:
        Order: 创建的订单对象
        """
        trade_price = self.get_current_price(trade_code)
        # update_position 内部已经调用 save_trade 将交易存入数据库
        return self.update_position(trade_code, amount, trade_price)

    def order_sell(self, trade_code, amount):
        """
        创建卖单

        参数:
        trade_code: 标的代码
        amount: 卖出数量

        返回:
        Order: 创建的订单对象，如果无法执行则返回 None
        """
        # 首先更新持仓情况
        self.reconstruct_positions()

        # 获取可卖出数量
        closeable_amount = self.get_closeable_amount(trade_code)

        # 检查是否有足够的持仓可以卖出
        if closeable_amount < amount:
            print(f"警告：尝试卖出 {amount} 股 {trade_code}，但当前只有 {closeable_amount} 股可卖出。操作已取消。")
            return None

        # 获取当前价格
        trade_price = self.get_current_price(trade_code)

        if trade_price is None:
            print(f"无法获取 {trade_code} 的实时价格，卖出操作已取消。")
            return None

        # 执行卖出操作 (update_position 内部已经调用 save_trade 将交易存入数据库)
        order = self.update_position(trade_code, -amount, trade_price)

        print(f"已卖出 {amount} 股 {trade_code}，价格：{trade_price}")

        return order

    def order_target(self, trade_code, target_amount):
        """
        创建目标持仓订单，将持仓调整到指定的目标数量

        参数:
        trade_code: 标的代码
        target_amount: 目标持仓数量

        返回:
        Order or None: 创建的订单对象，如果无需调整，则返回 None
        """
        # 重新构建持仓以确保数据最新
        self.reconstruct_positions()

        current_amount = self.positions[trade_code].total_amount if trade_code in self.positions else 0
        delta = target_amount - current_amount

        if delta > 0:
            # 需要买入
            return self.order_buy(trade_code, delta)
        elif delta < 0:
            # 需要卖出
            return self.order_sell(trade_code, abs(delta))
        else:
            # 不需要调整
            print(f"持仓已经达到目标数量：{target_amount} 股。无需操作。")
            return None

    # def display_positions(self):
    #     """
    #     显示当前持仓情况
    #     """
    #     print("\n当前持仓情况：")
    #     for trade_code, pos in self.positions.items():
    #         print(f"证券代码: {trade_code}")
    #         print(f"  总数量: {pos.total_amount}")
    #         print(f"  可卖出数量: {pos.closeable_amount}")
    #         print(f"  实时价格: {pos.current_price}")
    #         print(f"  累计平均成本: {pos.avg_cost:.2f} 元")
    #         print(f"  总价值: {pos.value:.2f} 元\n")
    def display_positions_with_pnl(self):
        """
        显示当前持仓的详细情况，包括精确的盈亏分析。
        这是 display_positions 的增强版本。
        """
        # 首先，必须重构持仓以获取最新的成本和市价信息
        self.reconstruct_positions()
        
        print("\n" + "="*60)
        print("当前持仓详情 (含盈亏分析)")
        print("="*60)

        if not self.positions:
            print("当前无任何持仓。")
            # 即使没有持仓，也显示账户总览
            total_equity = self.get_total_equity() # get_total_equity 会再次调用 reconstruct_positions, 安全但略有冗余
            print(f"可用资金: {self.available_cash:,.2f} 元")
            print(f"账户总权益: {total_equity:,.2f} 元")
            print("="*60)
            return

        total_market_value = 0
        total_pnl = 0
        total_cost = 0

        # 遍历所有持仓
        for trade_code, pos in self.positions.items():
            # 累加总市值和总成本
            total_market_value += pos.value
            total_cost += pos.total_cost
            
            # 初始化盈亏指标
            pnl_per_share = 0.0
            total_position_pnl = 0.0
            pnl_ratio_percentage = 0.0

            # --- 核心计算：确保持仓成本大于0，避免除零错误 ---
            if pos.avg_cost > 0:
                # 1. 每股盈亏 = 当前市价 - 平均成本
                pnl_per_share = pos.current_price - pos.avg_cost
                
                # 2. 单只股票的总盈亏 = 每股盈亏 * 总数量
                total_position_pnl = pnl_per_share * pos.total_amount
                
                # 3. 收益率(%) = (现价 / 持仓成本 - 1) * 100
                pnl_ratio_percentage = (pos.current_price / pos.avg_cost - 1) * 100
            
            # 累加总盈亏
            total_pnl += total_position_pnl

            # --- 格式化输出 ---
            pnl_prefix = "📈" if total_position_pnl >= 0 else "📉"
            
            print(f"--- 证券代码: {trade_code} ---")
            print(f"  持仓数量: {pos.total_amount} 股")
            print(f"  持仓成本: {pos.avg_cost:,.2f} 元")
            print(f"  当前市价: {pos.current_price:,.2f} 元")
            print(f"  持仓市值: {pos.value:,.2f} 元")
            print(f"  {pnl_prefix} 每股盈亏: {pnl_per_share:,.2f} 元")
            print(f"  {pnl_prefix} 持仓总盈亏: {total_position_pnl:,.2f} 元")
            print(f"  {pnl_prefix} 收益率: {pnl_ratio_percentage:.2f}%")
            print("-" * 25)

        # --- 打印账户总览 ---
        print("\n" + "="*25 + " 账户总览 " + "="*25)
        print(f"可用资金: {self.available_cash:,.2f} 元")
        print(f"持仓总市值: {total_market_value:,.2f} 元")
        print(f"持仓总成本: {total_cost:,.2f} 元")
        # 根据总盈亏判断账户整体盈亏状态
        total_pnl_prefix = "📈" if total_pnl >= 0 else "📉"
        print(f"{total_pnl_prefix} 持仓总盈亏: {total_pnl:,.2f} 元")
        
        # 总权益 = 可用资金 + 持仓市值
        total_equity = self.available_cash + total_market_value
        print(f"账户总权益: {total_equity:,.2f} 元")
        print("="*60)
    
    def get_position_pnl(self, stock_code):
        """
        获取单只持仓股票的成本与盈亏详情。
        这是策略（特别是卖出策略）调用的核心接口。

        参数:
        stock_code (str): 股票代码

        返回:
        dict: 包含盈亏信息的字典，如果未持仓则返回 None。
              字典格式: {
                  'stock_code': str,
                  'total_amount': int,
                  'avg_cost': float,
                  'current_price': float,
                  'market_value': float,
                  'pnl_per_share': float,
                  'total_pnl': float,
                  'pnl_ratio': float (例如 0.1 代表 10% 收益)
              }
        """
        # 确保持仓数据是基于最新价格的
        # 注意：为了性能，如果在一个循环中对多只股票调用此函数，
        # 最好在循环外先调用一次 reconstruct_positions()。
        # 但为了接口的独立性和安全性，这里保留了调用。
        self.reconstruct_positions()

        position = self.positions.get(stock_code)

        # 如果未持有该股票，直接返回 None
        if not position:
            return None

        # --- 核心数据提取与计算 ---
        avg_cost = position.avg_cost
        current_price = position.current_price

        pnl_per_share = 0.0
        total_pnl = 0.0
        pnl_ratio = 0.0

        # 确保持仓成本大于0，避免除零错误
        if avg_cost > 0:
            pnl_per_share = current_price - avg_cost
            total_pnl = pnl_per_share * position.total_amount
            pnl_ratio = round((current_price / avg_cost) - 1, 3)  # 直接计算比率，保留3位小数

        # 组装成字典返回
        pnl_info = {
            'stock_code': stock_code,
            'total_amount': position.total_amount,
            'avg_cost': avg_cost,
            'current_price': current_price,
            'market_value': position.value,
            'pnl_per_share': pnl_per_share,
            'total_pnl': total_pnl,
            'pnl_ratio': pnl_ratio  # 收益率，例如 0.1 表示10%
        }
        
        return pnl_info


    def display_available_cash(self):
        """
        显示可用资金
        """
        print(f"可用资金: {self.available_cash:.2f} 元")

    def get_closeable_amount(self, trade_code):
        """
        安全地获取某个证券的可卖出数量。如果证券不存在，返回0。

        参数:
        trade_code (str): 证券代码

        返回:
        int: 可卖出数量
        """
        position = self.positions.get(trade_code)
        if position:
            return position.closeable_amount
        else:
            return 0

    def get_today_bought(self, trade_code):
        """
        安全地获取某个证券今日买入的数量。如果证券不存在，返回0。

        参数:
        trade_code (str): 证券代码

        返回:
        int: 今日买入数量
        """
        position = self.positions.get(trade_code)
        if position:
            return position.today_bought
        else:
            return 0
        
    def get_position_trading_days(self, trade_code):
        """
        获取某个证券的持仓交易天数
        
        参数:
        trade_code (str): 证券代码
        
        返回:
        int: 持仓的交易天数，如果未持仓则返回0
        """
        position = self.positions.get(trade_code)
        if not position or not position.init_time:
            return 0
            
        # 获取从建仓日期到今天的交易天数
        start_date = position.init_time.date()
        end_date = dt.datetime.now().date()
        
        try:
            # 通过数据库查询获取交易日
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查询指定日期范围内的交易日数量
            query = """
            SELECT COUNT(*) FROM trade_calendar 
            WHERE trade_date >= ? AND trade_date <= ? AND is_trading_day = 1
            """
            cursor.execute(query, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            trading_days_count = cursor.fetchone()[0]
            
            conn.close()
            return trading_days_count
            
        except Exception as e:
            print(f"查询交易日数据时出错: {e}")
            # 如果数据库查询失败，使用简单的日期差作为备用方案
            # 假设每周5个交易日，每月约22个交易日
            days_diff = (end_date - start_date).days
            estimated_trading_days = int(days_diff * 5 / 7)  # 粗略估算
            return max(0, estimated_trading_days)

    def get_total_equity(self):
        """
        获取账户总权益（可用资金 + 持仓市值），优化后版本
        """
        # 1. 确保所有状态是最新
        self.reconstruct_positions()

        # 2. 直接使用重构后的持仓市值进行加总
        total_position_value = sum(pos.value for pos in self.positions.values())

        # 3. 总权益 = 可用资金 + 持仓市值
        total_equity = self.available_cash + total_position_value

        return total_equity  
    
    def has_trade_in_last_n_days(self, stock_code, last_n_trading_days):
        """
        【优化版】检查某只股票在最近N个交易日内是否有交易记录。
        使用SQL查询直接在数据库层面进行筛选，提升性能。
        
        Args:
            stock_code (str): 要检查的股票代码。
            last_n_trading_days (list): 最近N个交易日的日期字符串列表。
        
        Returns:
            bool: 如果有交易记录，返回 True；否则返回 False。
        """
        if not last_n_trading_days:
            return False
        
        try:
            # 构建SQL查询，使用IN子句检查日期范围
            placeholders = ','.join(['?' for _ in last_n_trading_days])
            query = f"""
                SELECT trade_time, trade_amount 
                FROM {self.table_name} 
                WHERE trade_code = ? 
                AND DATE(trade_time) IN ({placeholders})
                LIMIT 1
            """
            
            # 执行查询
            cursor = self.conn.cursor()
            params = [stock_code] + last_n_trading_days
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                trade_date = result[0].split(' ')[0]  # 提取日期部分
                trade_amount = result[1]
                action = 'buy' if trade_amount > 0 else 'sell'
                print(f"  - [风控检查] 发现 {stock_code} 在 {trade_date} 有一笔 '{action}' 记录。")
                return True
            
            return False
            
        except Exception as e:
            print(f"⚠️ 查询交易记录时出错: {e}")
            return False
    
    def close(self):
        """
        优雅地关闭数据库连接
        
        功能:
        1. 提交所有待处理的事务
        2. 关闭数据库连接
        3. 清理连接对象
        """
        if hasattr(self, 'conn') and self.conn:
            try:
                # 提交所有待处理的事务
                self.conn.commit()
                # 关闭连接
                self.conn.close()
                print("✅ 数据库连接已优雅关闭")
            except Exception as e:
                print(f"⚠️ 关闭数据库连接时发生错误: {e}")
            finally:
                # 清理连接对象引用
                self.conn = None
    
    def __del__(self):
        """
        析构函数：确保在对象被销毁时关闭数据库连接
        
        注意：这不是最可靠的方式，建议显式调用close()方法
        """
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except:
                pass  # 忽略析构函数中的错误
    
    


def initialize_account_and_run_example(starting_cash):
    """
    初始化账户并运行示例交易

    参数:
    starting_cash (float): 账户初始资金
    """
    account = Account(starting_cash)

    # 模拟一次买入交易
    # trade_time = datetime.datetime.strptime('2025-09-17 10:05:02', '%Y-%m-%d %H:%M:%S')
    # account.update_position(
    #     trade_code='000001',
    #     amount=100,  # 买入100股
    #     trade_price=10.09,
    #     trade_time=trade_time  # 设置买进时间
    # )

    # 添加更多交易
    trade_time = dt.datetime.strptime('2025-07-16 10:12:44', '%Y-%m-%d %H:%M:%S')
    account.update_position('600839', 600,5.22, trade_time) 
    # trade_time = datetime.datetime.strptime('2025-09-15 10:06:04', '%Y-%m-%d %H:%M:%S')
    # account.update_position('000962', 200, 22.4, trade_time)    # 买入200股，手续费固定3元
    # trade_time = datetime.datetime.strptime('2025-09-16 10:12:44', '%Y-%m-%d %H:%M:%S')
    # account.update_position('002182', 600,14.22, trade_time)     # 买入600股，手续费固定3元

    # # 创建target订单
    # order = account.order_target('000002', 600)  # 目标持仓600股
    # if order:
    #     print(order.trade_code, order.amount, order.price, order.direction)

    # 显示持仓和可用资金
    account.display_positions_with_pnl()
    account.display_available_cash()
    
    # 显示账户总权益
    total_equity = account.get_total_equity()
    print(f"账户总权益: {total_equity:.2f} 元")





if __name__ == "__main__":
    # 1. 初始化账户并确保持仓已从CSV加载
    account = Account(starting_cash=1000000.0)
    initialize_account_and_run_example(starting_cash=1000000.0)

    # 测试价格获取功能
    try:
        import sys
        import os
        # 添加项目根目录到Python路径
        current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if current_dir not in sys.path:
            sys.path.append(current_dir)
        
        from data_management.data_processor import get_latest_price
        print(get_latest_price('000029'))
    except ImportError as e:
        print(f"无法导入get_latest_price: {e}")
    #reconstruct_positions 会被 get_position_pnl 自动调用，但先调用一次可以清晰地看到初始持仓
    # account.reconstruct_positions() 
    # print(f"初始持仓列表: {list(account.positions.keys())}")
    # pnl_data = account.get_position_pnl('600839')
    # if pnl_data:
    #     print(pnl_data['pnl_ratio'])
    # else:
    #     print("未找到该股票的持仓信息")
    
    # # 2. 定义卖出策略参数
    # PROFIT_TARGET = 0.20  # 止盈目标：盈利20%
    # STOP_LOSS_TARGET = -0.10 # 止损目标：亏损10%

    # print("\n开始执行卖出信号检查...")
    # print("-" * 40)
    
    # sell_signals = []

    # # 3. 遍历当前所有持仓的股票
    # #    使用 list(account.positions.keys()) 来创建一个副本，防止在循环中修改字典
    # for stock_code in list(account.positions.keys()):
        
    #     # 4. 调用新方法获取该股票的盈亏详情
    #     pnl_data = account.get_position_pnl(stock_code)
        
    #     if pnl_data:
    #         print(f"检查股票: {stock_code}, "
    #               f"当前收益率: {pnl_data['pnl_ratio']:.2%}, "
    #               f"持仓成本: {pnl_data['avg_cost']:.2f}")

    #         # 5. 设计卖出逻辑
    #         # 检查是否达到止盈条件
    #         if pnl_data['pnl_ratio'] >= PROFIT_TARGET:
    #             print(f"  => 触发止盈信号！收益率达到 {pnl_data['pnl_ratio']:.2%}")
    #             sell_signals.append({'code': stock_code, 'reason': '止盈'})

    #         # 检查是否达到止损条件
    #         elif pnl_data['pnl_ratio'] <= STOP_LOSS_TARGET:
    #             print(f"  => 触发止损信号！收益率达到 {pnl_data['pnl_ratio']:.2%}")
    #             sell_signals.append({'code': stock_code, 'reason': '止损'})
    #         else:
    #             print("  => 未触发任何卖出信号。")

    # # 6. 汇总并执行卖出（此处仅为打印演示）
    # print("-" * 40)
    # if sell_signals:
    #     print(f"\n检测到 {len(sell_signals)} 个卖出信号:")
    #     for signal in sell_signals:
    #         stock_to_sell = signal['code']
    #         # 获取可卖出数量
    #         sell_amount = account.get_closeable_amount(stock_to_sell)
    #         if sell_amount > 0:
    #             print(f"  准备执行卖出: 卖出 {sell_amount} 股 {stock_to_sell}，原因: {signal['reason']}")
    #             # account.order_sell(stock_to_sell, sell_amount) # 在实盘中取消这行注释来执行交易
    #         else:
    #             print(f"  警告: {stock_to_sell} 有卖出信号，但可卖出数量为0（可能是T+1限制）。")
    # else:
    #     print("\n未检测到任何需要执行的卖出信号。")

    



# class Account:
#     """账户管理类"""
    
#     def __init__(self, account_id: str, initial_cash: float = 1000000.0):
#         self.account_id = account_id
#         self.cash = initial_cash
#         self.initial_cash = initial_cash
#         self.positions: Dict[str, float] = {}  # 持仓 {symbol: quantity}
#         self.orders: List[Dict[str, Any]] = []  # 订单记录
#         self.trades: List[Dict[str, Any]] = []  # 成交记录
        
#     def get_balance(self) -> float:
#         """获取账户余额"""
#         return self.cash
        
#     def get_positions(self) -> Dict[str, float]:
#         """获取持仓"""
#         return self.positions.copy()
        
#     def get_total_value(self, prices: Dict[str, float]) -> float:
#         """获取总资产价值"""
#         total = self.cash
#         for symbol, quantity in self.positions.items():
#             if symbol in prices:
#                 total += quantity * prices[symbol]
#         return total
        
#     def can_buy(self, symbol: str, quantity: int, price: float) -> bool:
#         """检查是否可以买入"""
#         required_cash = quantity * price
#         return self.cash >= required_cash
        
#     def can_sell(self, symbol: str, quantity: int) -> bool:
#         """检查是否可以卖出"""
#         return self.positions.get(symbol, 0) >= quantity
        
#     def buy(self, symbol: str, quantity: int, price: float, timestamp: datetime = None):
#         """买入股票"""
#         if not self.can_buy(symbol, quantity, price):
#             raise ValueError(f"资金不足，需要 {quantity * price}，可用 {self.cash}")
            
#         cost = quantity * price
#         self.cash -= cost
#         self.positions[symbol] = self.positions.get(symbol, 0) + quantity
        
#         # 记录交易
#         trade = {
#             "timestamp": timestamp or datetime.now(),
#             "symbol": symbol,
#             "action": "buy",
#             "quantity": quantity,
#             "price": price,
#             "amount": cost
#         }
#         self.trades.append(trade)
        
#         logger.info(f"买入 {symbol}: {quantity}股 @ {price}")
        
#     def sell(self, symbol: str, quantity: int, price: float, timestamp: datetime = None):
#         """卖出股票"""
#         if not self.can_sell(symbol, quantity):
#             raise ValueError(f"持仓不足，需要 {quantity}，可用 {self.positions.get(symbol, 0)}")
            
#         proceeds = quantity * price
#         self.cash += proceeds
#         self.positions[symbol] = self.positions.get(symbol, 0) - quantity
        
#         # 记录交易
#         trade = {
#             "timestamp": timestamp or datetime.now(),
#             "symbol": symbol,
#             "action": "sell",
#             "quantity": quantity,
#             "price": price,
#             "amount": proceeds
#         }
#         self.trades.append(trade)
        
#         logger.info(f"卖出 {symbol}: {quantity}股 @ {price}")
        
#     def get_position_value(self, symbol: str, price: float) -> float:
#         """获取持仓价值"""
#         return self.positions.get(symbol, 0) * price
        
#     def get_position_ratio(self, symbol: str, price: float) -> float:
#         """获取持仓比例"""
#         position_value = self.get_position_value(symbol, price)
#         total_value = self.get_total_value({symbol: price})
#         return position_value / total_value if total_value > 0 else 0
        
#     def get_trade_history(self, symbol: str = None) -> List[Dict[str, Any]]:
#         """获取交易历史"""
#         if symbol is None:
#             return self.trades.copy()
#         else:
#             return [trade for trade in self.trades if trade["symbol"] == symbol]
            
#     def get_performance_metrics(self, current_prices: Dict[str, float]) -> Dict[str, float]:
#         """获取绩效指标"""
#         total_value = self.get_total_value(current_prices)
#         total_return = (total_value - self.initial_cash) / self.initial_cash
        
#         return {
#             "total_value": total_value,
#             "cash": self.cash,
#             "total_return": total_return,
#             "position_count": len([p for p in self.positions.values() if p > 0])
#         }
