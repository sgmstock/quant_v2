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

logger = logging.getLogger(__name__)


class Account:
    """账户管理类"""
    
    def __init__(self, account_id: str, initial_cash: float = 1000000.0):
        self.account_id = account_id
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions: Dict[str, float] = {}  # 持仓 {symbol: quantity}
        self.orders: List[Dict[str, Any]] = []  # 订单记录
        self.trades: List[Dict[str, Any]] = []  # 成交记录
        
    def get_balance(self) -> float:
        """获取账户余额"""
        return self.cash
        
    def get_positions(self) -> Dict[str, float]:
        """获取持仓"""
        return self.positions.copy()
        
    def get_total_value(self, prices: Dict[str, float]) -> float:
        """获取总资产价值"""
        total = self.cash
        for symbol, quantity in self.positions.items():
            if symbol in prices:
                total += quantity * prices[symbol]
        return total
        
    def can_buy(self, symbol: str, quantity: int, price: float) -> bool:
        """检查是否可以买入"""
        required_cash = quantity * price
        return self.cash >= required_cash
        
    def can_sell(self, symbol: str, quantity: int) -> bool:
        """检查是否可以卖出"""
        return self.positions.get(symbol, 0) >= quantity
        
    def buy(self, symbol: str, quantity: int, price: float, timestamp: datetime = None):
        """买入股票"""
        if not self.can_buy(symbol, quantity, price):
            raise ValueError(f"资金不足，需要 {quantity * price}，可用 {self.cash}")
            
        cost = quantity * price
        self.cash -= cost
        self.positions[symbol] = self.positions.get(symbol, 0) + quantity
        
        # 记录交易
        trade = {
            "timestamp": timestamp or datetime.now(),
            "symbol": symbol,
            "action": "buy",
            "quantity": quantity,
            "price": price,
            "amount": cost
        }
        self.trades.append(trade)
        
        logger.info(f"买入 {symbol}: {quantity}股 @ {price}")
        
    def sell(self, symbol: str, quantity: int, price: float, timestamp: datetime = None):
        """卖出股票"""
        if not self.can_sell(symbol, quantity):
            raise ValueError(f"持仓不足，需要 {quantity}，可用 {self.positions.get(symbol, 0)}")
            
        proceeds = quantity * price
        self.cash += proceeds
        self.positions[symbol] = self.positions.get(symbol, 0) - quantity
        
        # 记录交易
        trade = {
            "timestamp": timestamp or datetime.now(),
            "symbol": symbol,
            "action": "sell",
            "quantity": quantity,
            "price": price,
            "amount": proceeds
        }
        self.trades.append(trade)
        
        logger.info(f"卖出 {symbol}: {quantity}股 @ {price}")
        
    def get_position_value(self, symbol: str, price: float) -> float:
        """获取持仓价值"""
        return self.positions.get(symbol, 0) * price
        
    def get_position_ratio(self, symbol: str, price: float) -> float:
        """获取持仓比例"""
        position_value = self.get_position_value(symbol, price)
        total_value = self.get_total_value({symbol: price})
        return position_value / total_value if total_value > 0 else 0
        
    def get_trade_history(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取交易历史"""
        if symbol is None:
            return self.trades.copy()
        else:
            return [trade for trade in self.trades if trade["symbol"] == symbol]
            
    def get_performance_metrics(self, current_prices: Dict[str, float]) -> Dict[str, float]:
        """获取绩效指标"""
        total_value = self.get_total_value(current_prices)
        total_return = (total_value - self.initial_cash) / self.initial_cash
        
        return {
            "total_value": total_value,
            "cash": self.cash,
            "total_return": total_return,
            "position_count": len([p for p in self.positions.values() if p > 0])
        }
