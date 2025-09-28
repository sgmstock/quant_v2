"""
风险管理

负责：
1. 风险指标计算
2. 风险控制规则
3. 风险预警
4. 风险报告
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import numpy as np

logger = logging.getLogger(__name__)


class RiskManager:
    """风险管理器"""
    
    def __init__(self, account, max_position_ratio: float = 0.1, 
                 max_total_risk: float = 0.2, stop_loss_ratio: float = 0.05):
        self.account = account
        self.max_position_ratio = max_position_ratio  # 单个持仓最大比例
        self.max_total_risk = max_total_risk  # 总风险敞口
        self.stop_loss_ratio = stop_loss_ratio  # 止损比例
        self.risk_alerts: List[Dict[str, Any]] = []
        
    def check_position_risk(self, symbol: str, quantity: int, price: float) -> Dict[str, Any]:
        """检查持仓风险"""
        position_value = quantity * price
        total_value = self.account.get_total_value({symbol: price})
        position_ratio = position_value / total_value if total_value > 0 else 0
        
        risk_info = {
            "symbol": symbol,
            "position_value": position_value,
            "position_ratio": position_ratio,
            "is_risk_ok": position_ratio <= self.max_position_ratio,
            "risk_level": "high" if position_ratio > self.max_position_ratio else "normal"
        }
        
        if not risk_info["is_risk_ok"]:
            self._add_risk_alert(f"持仓风险过高: {symbol} 比例 {position_ratio:.2%}")
            
        return risk_info
        
    def check_total_risk(self, positions: Dict[str, float], prices: Dict[str, float]) -> Dict[str, Any]:
        """检查总风险"""
        total_position_value = sum(positions.get(symbol, 0) * prices.get(symbol, 0) 
                                 for symbol in positions.keys() if symbol in prices)
        total_value = self.account.get_total_value(prices)
        total_risk_ratio = total_position_value / total_value if total_value > 0 else 0
        
        risk_info = {
            "total_position_value": total_position_value,
            "total_risk_ratio": total_risk_ratio,
            "is_risk_ok": total_risk_ratio <= self.max_total_risk,
            "risk_level": "high" if total_risk_ratio > self.max_total_risk else "normal"
        }
        
        if not risk_info["is_risk_ok"]:
            self._add_risk_alert(f"总风险过高: {total_risk_ratio:.2%}")
            
        return risk_info
        
    def check_stop_loss(self, symbol: str, current_price: float, 
                       entry_price: float) -> Dict[str, Any]:
        """检查止损"""
        if entry_price <= 0:
            return {"should_stop": False, "reason": "无效的入场价格"}
            
        price_change = (current_price - entry_price) / entry_price
        should_stop = price_change <= -self.stop_loss_ratio
        
        stop_info = {
            "symbol": symbol,
            "current_price": current_price,
            "entry_price": entry_price,
            "price_change": price_change,
            "should_stop": should_stop,
            "stop_loss_ratio": self.stop_loss_ratio
        }
        
        if should_stop:
            self._add_risk_alert(f"触发止损: {symbol} 跌幅 {price_change:.2%}")
            
        return stop_info
        
    def calculate_var(self, returns: List[float], confidence_level: float = 0.05) -> float:
        """计算风险价值(VaR)"""
        if not returns:
            return 0.0
            
        return np.percentile(returns, confidence_level * 100)
        
    def calculate_max_drawdown(self, values: List[float]) -> Dict[str, float]:
        """计算最大回撤"""
        if not values:
            return {"max_drawdown": 0.0, "max_drawdown_ratio": 0.0}
            
        peak = values[0]
        max_dd = 0.0
        max_dd_ratio = 0.0
        
        for value in values:
            if value > peak:
                peak = value
            else:
                drawdown = peak - value
                drawdown_ratio = drawdown / peak if peak > 0 else 0
                if drawdown > max_dd:
                    max_dd = drawdown
                    max_dd_ratio = drawdown_ratio
                    
        return {
            "max_drawdown": max_dd,
            "max_drawdown_ratio": max_dd_ratio
        }
        
    def calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.03) -> float:
        """计算夏普比率"""
        if not returns or len(returns) < 2:
            return 0.0
            
        excess_returns = [r - risk_free_rate / 252 for r in returns]  # 日收益率
        if np.std(excess_returns) == 0:
            return 0.0
            
        return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
        
    def get_risk_metrics(self, prices: Dict[str, float], 
                        returns: Dict[str, List[float]] = None) -> Dict[str, Any]:
        """获取风险指标"""
        positions = self.account.get_positions()
        total_value = self.account.get_total_value(prices)
        
        # 计算持仓风险
        position_risks = {}
        for symbol, quantity in positions.items():
            if quantity > 0 and symbol in prices:
                position_risks[symbol] = {
                    "value": quantity * prices[symbol],
                    "ratio": (quantity * prices[symbol]) / total_value if total_value > 0 else 0
                }
                
        # 计算总风险
        total_risk = self.check_total_risk(positions, prices)
        
        # 计算风险指标
        risk_metrics = {
            "total_value": total_value,
            "position_risks": position_risks,
            "total_risk_ratio": total_risk["total_risk_ratio"],
            "risk_level": total_risk["risk_level"],
            "alerts_count": len(self.risk_alerts)
        }
        
        # 如果有收益率数据，计算高级风险指标
        if returns:
            all_returns = []
            for symbol_returns in returns.values():
                all_returns.extend(symbol_returns)
                
            if all_returns:
                risk_metrics.update({
                    "var_5pct": self.calculate_var(all_returns, 0.05),
                    "max_drawdown": self.calculate_max_drawdown(all_returns),
                    "sharpe_ratio": self.calculate_sharpe_ratio(all_returns)
                })
                
        return risk_metrics
        
    def _add_risk_alert(self, message: str):
        """添加风险预警"""
        alert = {
            "timestamp": datetime.now(),
            "message": message,
            "level": "warning"
        }
        self.risk_alerts.append(alert)
        logger.warning(f"风险预警: {message}")
        
    def get_risk_alerts(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取风险预警"""
        return self.risk_alerts[-limit:]
        
    def clear_risk_alerts(self):
        """清除风险预警"""
        self.risk_alerts.clear()
