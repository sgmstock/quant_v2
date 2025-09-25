"""
事件引擎测试

测试事件驱动引擎的功能
"""

import pytest
from datetime import datetime
from core.event_engine import EventEngine
from core.event import EventType, TickEvent, BarEvent


class TestEventEngine:
    """事件引擎测试类"""
    
    def setup_method(self):
        """测试前准备"""
        self.event_engine = EventEngine()
        self.received_events = []
        
    def teardown_method(self):
        """测试后清理"""
        if self.event_engine.is_running():
            self.event_engine.stop()
            
    def test_event_engine_start_stop(self):
        """测试事件引擎启动和停止"""
        assert not self.event_engine.is_running()
        
        self.event_engine.start()
        assert self.event_engine.is_running()
        
        self.event_engine.stop()
        assert not self.event_engine.is_running()
        
    def test_event_listener_registration(self):
        """测试事件监听器注册"""
        def dummy_listener(event):
            self.received_events.append(event)
            
        self.event_engine.register_listener(EventType.TICK, dummy_listener)
        assert self.event_engine.get_listener_count(EventType.TICK) == 1
        
        self.event_engine.unregister_listener(EventType.TICK, dummy_listener)
        assert self.event_engine.get_listener_count(EventType.TICK) == 0
        
    def test_tick_event_processing(self):
        """测试TICK事件处理"""
        def tick_listener(event):
            self.received_events.append(event)
            
        self.event_engine.register_listener(EventType.TICK, tick_listener)
        self.event_engine.start()
        
        # 发送TICK事件
        self.event_engine.put_tick_event("000001", 10.5, 1000)
        
        # 等待事件处理
        import time
        time.sleep(0.1)
        
        assert len(self.received_events) == 1
        assert self.received_events[0].symbol == "000001"
        assert self.received_events[0].price == 10.5
        assert self.received_events[0].volume == 1000
        
    def test_bar_event_processing(self):
        """测试BAR事件处理"""
        def bar_listener(event):
            self.received_events.append(event)
            
        self.event_engine.register_listener(EventType.BAR, bar_listener)
        self.event_engine.start()
        
        # 发送BAR事件
        bar_data = {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 10000}
        self.event_engine.put_bar_event("000001", bar_data)
        
        # 等待事件处理
        import time
        time.sleep(0.1)
        
        assert len(self.received_events) == 1
        assert self.received_events[0].symbol == "000001"
        assert self.received_events[0].bar_data == bar_data
        
    def test_event_history(self):
        """测试事件历史记录"""
        self.event_engine.start()
        
        # 发送多个事件
        self.event_engine.put_tick_event("000001", 10.0, 1000)
        self.event_engine.put_tick_event("000002", 20.0, 2000)
        
        # 等待事件处理
        import time
        time.sleep(0.1)
        
        # 检查事件历史
        history = self.event_engine.get_event_history(EventType.TICK)
        assert len(history) == 2
        
        tick_history = self.event_engine.get_event_history(EventType.TICK, limit=1)
        assert len(tick_history) == 1
