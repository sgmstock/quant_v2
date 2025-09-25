"""
持仓管理

负责：
1. 持仓跟踪
2. 持仓分析
3. 持仓调整
4. 持仓风险控制
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PositionManager:
    """持仓管理器"""
    
    def __init__(self, account):
        self.account = account
        self.position_history: List[Dict[str, Any]] = []
        
    def get_position(self, symbol: str) -> float:
        """获取持仓数量"""
        return self.account.positions.get(symbol, 0)
        
    def get_position_value(self, symbol: str, price: float) -> float:
        """获取持仓价值"""
        return self.get_position(symbol) * price
        
    def get_position_ratio(self, symbol: str, price: float) -> float:
        """获取持仓比例"""
        return self.account.get_position_ratio(symbol, price)
        
    def get_all_positions(self, prices: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
        """获取所有持仓信息"""
        positions = {}
        for symbol, quantity in self.account.positions.items():
            if quantity > 0 and symbol in prices:
                positions[symbol] = {
                    "quantity": quantity,
                    "price": prices[symbol],
                    "value": quantity * prices[symbol],
                    "ratio": self.get_position_ratio(symbol, prices[symbol])
                }
        return positions
        
    def calculate_position_metrics(self, symbol: str, current_price: float, 
                                  entry_prices: List[float]) -> Dict[str, float]:
        """计算持仓指标"""
        position = self.get_position(symbol)
        if position == 0:
            return {}
            
        # 计算平均成本
        avg_cost = sum(entry_prices) / len(entry_prices) if entry_prices else 0
        
        # 计算盈亏
        pnl = (current_price - avg_cost) * position
        pnl_ratio = (current_price - avg_cost) / avg_cost if avg_cost > 0 else 0
        
        return {
            "position": position,
            "avg_cost": avg_cost,
            "current_price": current_price,
            "pnl": pnl,
            "pnl_ratio": pnl_ratio
        }
        
    def should_rebalance(self, target_ratios: Dict[str, float], 
                        current_prices: Dict[str, float], 
                        tolerance: float = 0.05) -> List[Dict[str, Any]]:
        """检查是否需要调仓"""
        rebalance_actions = []
        current_ratios = {}
        
        # 计算当前持仓比例
        total_value = self.account.get_total_value(current_prices)
        for symbol in target_ratios.keys():
            if symbol in current_prices:
                current_ratios[symbol] = self.get_position_ratio(symbol, current_prices[symbol])
            else:
                current_ratios[symbol] = 0
                
        # 检查每个标的的调仓需求
        for symbol, target_ratio in target_ratios.items():
            if symbol not in current_prices:
                continue
                
            current_ratio = current_ratios.get(symbol, 0)
            ratio_diff = target_ratio - current_ratio
            
            if abs(ratio_diff) > tolerance:
                target_value = total_value * target_ratio
                current_value = self.get_position_value(symbol, current_prices[symbol])
                value_diff = target_value - current_value
                
                if value_diff > 0:  # 需要买入
                    quantity = int(value_diff / current_prices[symbol])
                    if quantity > 0:
                        rebalance_actions.append({
                            "symbol": symbol,
                            "action": "buy",
                            "quantity": quantity,
                            "price": current_prices[symbol],
                            "reason": f"调仓至目标比例 {target_ratio:.2%}"
                        })
                else:  # 需要卖出
                    quantity = int(abs(value_diff) / current_prices[symbol])
                    if quantity > 0:
                        rebalance_actions.append({
                            "symbol": symbol,
                            "action": "sell",
                            "quantity": quantity,
                            "price": current_prices[symbol],
                            "reason": f"调仓至目标比例 {target_ratio:.2%}"
                        })
                        
        return rebalance_actions
        
    def record_position_snapshot(self, prices: Dict[str, float]):
        """记录持仓快照"""
        snapshot = {
            "timestamp": datetime.now(),
            "positions": self.get_all_positions(prices),
            "total_value": self.account.get_total_value(prices),
            "cash": self.account.cash
        }
        self.position_history.append(snapshot)
        
    def get_position_history(self, symbol: str = None) -> List[Dict[str, Any]]:
        """获取持仓历史"""
        if symbol is None:
            return self.position_history.copy()
        else:
            filtered_history = []
            for snapshot in self.position_history:
                if symbol in snapshot["positions"]:
                    filtered_history.append({
                        "timestamp": snapshot["timestamp"],
                        "position": snapshot["positions"][symbol]
                    })
            return filtered_history
