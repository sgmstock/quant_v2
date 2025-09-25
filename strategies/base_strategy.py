"""
策略基类

定义所有策略必须实现的接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
from core.event_engine import EventEngine, EventType
from core.event import Event


class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.initialized = False
        self.event_engine: Optional[EventEngine] = None
        
    def set_event_engine(self, event_engine: EventEngine):
        """设置事件引擎"""
        self.event_engine = event_engine
        self._register_event_listeners()
        
    def _register_event_listeners(self):
        """注册事件监听器"""
        if self.event_engine:
            self.event_engine.register_listener(EventType.BAR, self.on_bar)
            self.event_engine.register_listener(EventType.TICK, self.on_tick)
            self.event_engine.register_listener(EventType.SIGNAL, self.on_signal)
            self.event_engine.register_listener(EventType.ORDER, self.on_order)
            self.event_engine.register_listener(EventType.FILL, self.on_fill)
            
    @abstractmethod
    def on_init(self):
        """策略初始化"""
        pass
        
    @abstractmethod
    def on_bar(self, event: Event):
        """处理K线数据事件"""
        pass
        
    @abstractmethod
    def on_tick(self, event: Event):
        """处理实时行情事件"""
        pass
        
    def on_signal(self, event: Event):
        """处理交易信号事件"""
        pass
        
    def on_order(self, event: Event):
        """处理订单事件"""
        pass
        
    def on_fill(self, event: Event):
        """处理成交事件"""
        pass
        
    def get_signals(self) -> List[Dict[str, Any]]:
        """获取交易信号"""
        return []
        
    def get_positions(self) -> Dict[str, float]:
        """获取持仓信息"""
        return {}
        
    def get_performance_metrics(self) -> Dict[str, Any]:
        """获取绩效指标"""
        return {}
