"""
信号生成器

负责：
1. 信号生成逻辑
2. 信号强度计算
3. 信号过滤
4. 信号历史记录
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """信号类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class Signal:
    """信号类"""
    
    def __init__(self, symbol: str, signal_type: SignalType, strength: float, 
                 reason: str = "", timestamp: datetime = None):
        self.symbol = symbol
        self.signal_type = signal_type
        self.strength = strength
        self.reason = reason
        self.timestamp = timestamp or datetime.now()
        self.id = f"{symbol}_{signal_type.value}_{self.timestamp.strftime('%Y%m%d_%H%M%S')}"
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "signal_type": self.signal_type.value,
            "strength": self.strength,
            "reason": self.reason,
            "timestamp": self.timestamp
        }


class SignalGenerator:
    """信号生成器"""
    
    def __init__(self, min_strength: float = 0.5, max_signals: int = 100):
        self.min_strength = min_strength
        self.max_signals = max_signals
        self.signals: List[Signal] = []
        self.signal_history: List[Signal] = []
        
    def generate_signal(self, symbol: str, signal_type: SignalType, 
                       strength: float, reason: str = "") -> Optional[Signal]:
        """生成信号"""
        if strength < self.min_strength:
            logger.debug(f"信号强度不足: {symbol} {signal_type.value} {strength}")
            return None
            
        signal = Signal(symbol, signal_type, strength, reason)
        self.signals.append(signal)
        self.signal_history.append(signal)
        
        # 限制信号数量
        if len(self.signals) > self.max_signals:
            self.signals = self.signals[-self.max_signals:]
            
        logger.info(f"生成信号: {symbol} {signal_type.value} 强度 {strength:.2f}")
        return signal
        
    def get_signals(self, symbol: str = None, signal_type: SignalType = None) -> List[Signal]:
        """获取信号"""
        filtered_signals = self.signals
        
        if symbol:
            filtered_signals = [s for s in filtered_signals if s.symbol == symbol]
            
        if signal_type:
            filtered_signals = [s for s in filtered_signals if s.signal_type == signal_type]
            
        return filtered_signals
        
    def get_latest_signal(self, symbol: str) -> Optional[Signal]:
        """获取最新信号"""
        signals = self.get_signals(symbol)
        if signals:
            return max(signals, key=lambda x: x.timestamp)
        return None
        
    def clear_signals(self, symbol: str = None):
        """清除信号"""
        if symbol:
            self.signals = [s for s in self.signals if s.symbol != symbol]
        else:
            self.signals.clear()
            
    def get_signal_statistics(self) -> Dict[str, Any]:
        """获取信号统计"""
        total_signals = len(self.signal_history)
        buy_signals = len([s for s in self.signal_history if s.signal_type == SignalType.BUY])
        sell_signals = len([s for s in self.signal_history if s.signal_type == SignalType.SELL])
        hold_signals = len([s for s in self.signal_history if s.signal_type == SignalType.HOLD])
        
        avg_strength = sum(s.strength for s in self.signal_history) / total_signals if total_signals > 0 else 0
        
        return {
            "total_signals": total_signals,
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "hold_signals": hold_signals,
            "avg_strength": avg_strength
        }
