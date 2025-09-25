"""
事件驱动引擎

负责：
1. 事件循环管理
2. 事件分发
3. 事件监听器注册
4. 异步事件处理

设计特点：
- 支持多种事件类型
- 异步事件处理
- 事件优先级管理
- 事件历史记录
"""

import threading
import queue
import logging
from typing import Dict, List, Callable, Any, Optional
from datetime import datetime
from .event import Event, EventType

logger = logging.getLogger(__name__)


class EventEngine:
    """事件驱动引擎"""
    
    def __init__(self):
        self._event_queue = queue.Queue()
        self._listeners: Dict[EventType, List[Callable]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._event_history: List[Event] = []
        self._max_history_size = 10000
        
    def start(self):
        """启动事件引擎"""
        if self._running:
            logger.warning("事件引擎已经在运行")
            return
            
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("事件引擎已启动")
        
    def stop(self):
        """停止事件引擎"""
        if not self._running:
            logger.warning("事件引擎未运行")
            return
            
        self._running = False
        if self._thread:
            self._thread.join()
        logger.info("事件引擎已停止")
        
    def _run(self):
        """事件循环"""
        while self._running:
            try:
                event = self._event_queue.get(timeout=1.0)
                self._dispatch_event(event)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"事件处理错误: {e}")
                
    def _dispatch_event(self, event: Event):
        """分发事件"""
        # 记录事件历史
        self._add_to_history(event)
        
        # 获取事件监听器
        listeners = self._listeners.get(event.event_type, [])
        
        if not listeners:
            logger.debug(f"没有监听器处理事件: {event.event_type.value}")
            return
            
        # 分发事件给所有监听器
        for listener in listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"事件监听器错误: {e}")
                
    def _add_to_history(self, event: Event):
        """添加事件到历史记录"""
        self._event_history.append(event)
        
        # 限制历史记录大小
        if len(self._event_history) > self._max_history_size:
            self._event_history = self._event_history[-self._max_history_size:]
            
    def register_listener(self, event_type: EventType, listener: Callable):
        """注册事件监听器"""
        if event_type not in self._listeners:
            self._listeners[event_type] = []
        self._listeners[event_type].append(listener)
        logger.info(f"注册事件监听器: {event_type.value}")
        
    def unregister_listener(self, event_type: EventType, listener: Callable):
        """注销事件监听器"""
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(listener)
                logger.info(f"注销事件监听器: {event_type.value}")
            except ValueError:
                logger.warning(f"监听器未找到: {event_type.value}")
                
    def put_event(self, event: Event):
        """放入事件"""
        self._event_queue.put(event)
        logger.debug(f"放入事件: {event.event_type.value}")
        
    def put_tick_event(self, symbol: str, price: float, volume: int, 
                      timestamp: datetime = None):
        """放入TICK事件"""
        from .event import TickEvent
        event = TickEvent(symbol, price, volume, timestamp)
        self.put_event(event)
        
    def put_bar_event(self, symbol: str, bar_data: Dict[str, Any], 
                     timestamp: datetime = None):
        """放入BAR事件"""
        from .event import BarEvent
        event = BarEvent(symbol, bar_data, timestamp)
        self.put_event(event)
        
    def put_signal_event(self, symbol: str, signal_type: str, strength: float, 
                        direction: str, timestamp: datetime = None):
        """放入信号事件"""
        from .event import SignalEvent
        event = SignalEvent(symbol, signal_type, strength, direction, timestamp)
        self.put_event(event)
        
    def put_order_event(self, symbol: str, order_type: str, quantity: int, 
                       price: float = None, direction: str = "buy", 
                       timestamp: datetime = None):
        """放入订单事件"""
        from .event import OrderEvent
        event = OrderEvent(symbol, order_type, quantity, price, direction, timestamp)
        self.put_event(event)
        
    def put_fill_event(self, symbol: str, quantity: int, price: float, 
                      direction: str, commission: float = 0.0, 
                      timestamp: datetime = None):
        """放入成交事件"""
        from .event import FillEvent
        event = FillEvent(symbol, quantity, price, direction, commission, timestamp)
        self.put_event(event)
        
    def put_sector_selection_event(self, selected_sectors: list, 
                                  sector_scores: Dict[str, float], 
                                  timestamp: datetime = None):
        """放入板块选择事件"""
        from .event import SectorSelectionEvent
        event = SectorSelectionEvent(selected_sectors, sector_scores, timestamp)
        self.put_event(event)
        
    def put_stock_selection_event(self, selected_stocks: list, 
                                 stock_scores: Dict[str, float], 
                                 timestamp: datetime = None):
        """放入个股选择事件"""
        from .event import StockSelectionEvent
        event = StockSelectionEvent(selected_stocks, stock_scores, timestamp)
        self.put_event(event)
        
    def get_event_history(self, event_type: EventType = None, 
                         limit: int = 100) -> List[Event]:
        """获取事件历史"""
        if event_type is None:
            return self._event_history[-limit:]
        else:
            filtered_events = [e for e in self._event_history if e.event_type == event_type]
            return filtered_events[-limit:]
            
    def get_listener_count(self, event_type: EventType) -> int:
        """获取指定事件类型的监听器数量"""
        return len(self._listeners.get(event_type, []))
        
    def is_running(self) -> bool:
        """检查事件引擎是否运行"""
        return self._running
        
    def get_queue_size(self) -> int:
        """获取事件队列大小"""
        return self._event_queue.qsize()
