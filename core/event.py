"""
事件定义

定义系统中所有事件类型和事件数据结构
"""

from enum import Enum
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass


class EventType(Enum):
    """事件类型枚举"""
    TICK = "tick"           # 实时行情
    BAR = "bar"             # K线数据
    SIGNAL = "signal"       # 交易信号
    ORDER = "order"         # 订单事件
    FILL = "fill"           # 成交回报
    POSITION = "position"   # 持仓更新
    ACCOUNT = "account"     # 账户更新
    RISK = "risk"           # 风险事件
    SECTOR_SELECTION = "sector_selection"  # 板块选择
    STOCK_SELECTION = "stock_selection"     # 个股选择


@dataclass
class Event:
    """事件基类"""
    event_type: EventType
    data: Dict[str, Any]
    timestamp: datetime
    event_id: str = ""
    
    def __post_init__(self):
        if not self.event_id:
            self.event_id = f"{self.event_type.value}_{self.timestamp.strftime('%Y%m%d_%H%M%S_%f')}"


@dataclass
class TickEvent(Event):
    """实时行情事件"""
    symbol: str
    price: float
    volume: int
    
    def __init__(self, symbol: str, price: float, volume: int, timestamp: datetime = None):
        super().__init__(
            EventType.TICK,
            {
                "symbol": symbol,
                "price": price,
                "volume": volume
            },
            timestamp or datetime.now()
        )
        self.symbol = symbol
        self.price = price
        self.volume = volume


@dataclass
class BarEvent(Event):
    """K线数据事件"""
    symbol: str
    bar_data: Dict[str, Any]
    
    def __init__(self, symbol: str, bar_data: Dict[str, Any], timestamp: datetime = None):
        super().__init__(
            EventType.BAR,
            {
                "symbol": symbol,
                "bar_data": bar_data
            },
            timestamp or datetime.now()
        )
        self.symbol = symbol
        self.bar_data = bar_data


@dataclass
class SignalEvent(Event):
    """交易信号事件"""
    symbol: str
    signal_type: str
    strength: float
    direction: str  # 'buy', 'sell', 'hold'
    
    def __init__(self, symbol: str, signal_type: str, strength: float, 
                 direction: str, timestamp: datetime = None):
        super().__init__(
            EventType.SIGNAL,
            {
                "symbol": symbol,
                "signal_type": signal_type,
                "strength": strength,
                "direction": direction
            },
            timestamp or datetime.now()
        )
        self.symbol = symbol
        self.signal_type = signal_type
        self.strength = strength
        self.direction = direction


@dataclass
class OrderEvent(Event):
    """订单事件"""
    symbol: str
    order_type: str  # 'market', 'limit', 'stop'
    quantity: int
    price: Optional[float] = None
    direction: str = "buy"  # 'buy', 'sell'
    
    def __init__(self, symbol: str, order_type: str, quantity: int, 
                 price: Optional[float] = None, direction: str = "buy", 
                 timestamp: datetime = None):
        super().__init__(
            EventType.ORDER,
            {
                "symbol": symbol,
                "order_type": order_type,
                "quantity": quantity,
                "price": price,
                "direction": direction
            },
            timestamp or datetime.now()
        )
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.price = price
        self.direction = direction


@dataclass
class FillEvent(Event):
    """成交回报事件"""
    symbol: str
    quantity: int
    price: float
    direction: str
    commission: float = 0.0
    
    def __init__(self, symbol: str, quantity: int, price: float, 
                 direction: str, commission: float = 0.0, timestamp: datetime = None):
        super().__init__(
            EventType.FILL,
            {
                "symbol": symbol,
                "quantity": quantity,
                "price": price,
                "direction": direction,
                "commission": commission
            },
            timestamp or datetime.now()
        )
        self.symbol = symbol
        self.quantity = quantity
        self.price = price
        self.direction = direction
        self.commission = commission


@dataclass
class SectorSelectionEvent(Event):
    """板块选择事件"""
    selected_sectors: list
    sector_scores: Dict[str, float]
    
    def __init__(self, selected_sectors: list, sector_scores: Dict[str, float], 
                 timestamp: datetime = None):
        super().__init__(
            EventType.SECTOR_SELECTION,
            {
                "selected_sectors": selected_sectors,
                "sector_scores": sector_scores
            },
            timestamp or datetime.now()
        )
        self.selected_sectors = selected_sectors
        self.sector_scores = sector_scores


@dataclass
class StockSelectionEvent(Event):
    """个股选择事件"""
    selected_stocks: list
    stock_scores: Dict[str, float]
    
    def __init__(self, selected_stocks: list, stock_scores: Dict[str, float], 
                 timestamp: datetime = None):
        super().__init__(
            EventType.STOCK_SELECTION,
            {
                "selected_stocks": selected_stocks,
                "stock_scores": stock_scores
            },
            timestamp or datetime.now()
        )
        self.selected_stocks = selected_stocks
        self.stock_scores = stock_scores
