"""
事件引擎演示

展示事件驱动引擎的基本用法
"""

import time
from datetime import datetime
from core.event_engine import EventEngine
from core.event import EventType


def tick_listener(event):
    """TICK事件监听器"""
    print(f"收到TICK事件: {event.symbol} 价格 {event.price} 成交量 {event.volume}")


def bar_listener(event):
    """BAR事件监听器"""
    print(f"收到BAR事件: {event.symbol} 开 {event.bar_data['open']} 高 {event.bar_data['high']} 低 {event.bar_data['low']} 收 {event.bar_data['close']}")


def signal_listener(event):
    """信号事件监听器"""
    print(f"收到信号事件: {event.symbol} {event.direction} 强度 {event.strength}")


def main():
    """主函数"""
    print("=== 事件引擎演示 ===")
    
    # 创建事件引擎
    event_engine = EventEngine()
    
    # 注册事件监听器
    event_engine.register_listener(EventType.TICK, tick_listener)
    event_engine.register_listener(EventType.BAR, bar_listener)
    event_engine.register_listener(EventType.SIGNAL, signal_listener)
    
    # 启动事件引擎
    event_engine.start()
    print("事件引擎已启动")
    
    try:
        # 发送一些测试事件
        print("\n发送TICK事件...")
        event_engine.put_tick_event("000001", 10.5, 1000)
        event_engine.put_tick_event("000002", 20.3, 2000)
        
        time.sleep(0.1)
        
        print("\n发送BAR事件...")
        bar_data1 = {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 10000}
        bar_data2 = {"open": 20.0, "high": 20.5, "low": 19.8, "close": 20.2, "volume": 20000}
        event_engine.put_bar_event("000001", bar_data1)
        event_engine.put_bar_event("000002", bar_data2)
        
        time.sleep(0.1)
        
        print("\n发送信号事件...")
        event_engine.put_signal_event("000001", "momentum", 0.8, "buy")
        event_engine.put_signal_event("000002", "mean_reversion", 0.6, "sell")
        
        time.sleep(0.1)
        
        # 显示事件历史
        print(f"\n事件历史记录数量: {len(event_engine.get_event_history())}")
        print(f"TICK事件数量: {len(event_engine.get_event_history(EventType.TICK))}")
        print(f"BAR事件数量: {len(event_engine.get_event_history(EventType.BAR))}")
        print(f"信号事件数量: {len(event_engine.get_event_history(EventType.SIGNAL))}")
        
    finally:
        # 停止事件引擎
        event_engine.stop()
        print("\n事件引擎已停止")


if __name__ == "__main__":
    main()
