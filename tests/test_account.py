"""
账户管理测试

测试账户管理功能
"""

import pytest
from datetime import datetime
from core.execution.account import Account


class TestAccount:
    """账户管理测试类"""
    
    def setup_method(self):
        """测试前准备"""
        self.account = Account("test_account", 1000000.0)
        
    def test_account_initialization(self):
        """测试账户初始化"""
        assert self.account.account_id == "test_account"
        assert self.account.cash == 1000000.0
        assert self.account.initial_cash == 1000000.0
        assert len(self.account.positions) == 0
        assert len(self.account.trades) == 0
        
    def test_buy_stock(self):
        """测试买入股票"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        self.account.buy(symbol, quantity, price)
        
        assert self.account.cash == 1000000.0 - (quantity * price)
        assert self.account.positions[symbol] == quantity
        assert len(self.account.trades) == 1
        assert self.account.trades[0]["action"] == "buy"
        assert self.account.trades[0]["quantity"] == quantity
        assert self.account.trades[0]["price"] == price
        
    def test_sell_stock(self):
        """测试卖出股票"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        # 先买入
        self.account.buy(symbol, quantity, price)
        initial_cash = self.account.cash
        
        # 再卖出
        self.account.sell(symbol, quantity, price * 1.1)  # 以更高价格卖出
        
        assert self.account.cash > initial_cash
        assert self.account.positions[symbol] == 0
        assert len(self.account.trades) == 2
        assert self.account.trades[1]["action"] == "sell"
        
    def test_insufficient_cash(self):
        """测试资金不足"""
        symbol = "000001"
        quantity = 1000000  # 超过可用资金
        price = 10.0
        
        with pytest.raises(ValueError, match="资金不足"):
            self.account.buy(symbol, quantity, price)
            
    def test_insufficient_position(self):
        """测试持仓不足"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        # 先买入
        self.account.buy(symbol, quantity, price)
        
        # 尝试卖出更多
        with pytest.raises(ValueError, match="持仓不足"):
            self.account.sell(symbol, quantity + 100, price)
            
    def test_get_total_value(self):
        """测试获取总资产价值"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        # 买入股票
        self.account.buy(symbol, quantity, price)
        
        # 计算总价值
        total_value = self.account.get_total_value({symbol: price * 1.1})
        expected_value = self.account.cash + quantity * price * 1.1
        
        assert abs(total_value - expected_value) < 0.01
        
    def test_get_position_ratio(self):
        """测试获取持仓比例"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        # 买入股票
        self.account.buy(symbol, quantity, price)
        
        # 计算持仓比例
        ratio = self.account.get_position_ratio(symbol, price)
        expected_ratio = (quantity * price) / self.account.get_total_value({symbol: price})
        
        assert abs(ratio - expected_ratio) < 0.01
        
    def test_performance_metrics(self):
        """测试绩效指标"""
        symbol = "000001"
        quantity = 1000
        price = 10.0
        
        # 买入股票
        self.account.buy(symbol, quantity, price)
        
        # 获取绩效指标
        metrics = self.account.get_performance_metrics({symbol: price * 1.1})
        
        assert "total_value" in metrics
        assert "cash" in metrics
        assert "total_return" in metrics
        assert "position_count" in metrics
        assert metrics["position_count"] == 1
