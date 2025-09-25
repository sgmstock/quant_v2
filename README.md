# 量化交易系统 v2.0

## 项目概述

这是一个基于事件驱动架构的现代化量化交易系统，采用清晰的分层设计，支持多数据源接入和丰富的策略开发。

## 🏗️ 系统架构

```
quant_v2/
├── 📁 data_management/              # 数据管理层
│   ├── sources/                     # 数据源
│   │   ├── market_data_source.py    # 行情数据源
│   │   ├── fundamental_data_source.py # 基本面数据源（限售股、财报等）
│   │   └── news_data_source.py      # 新闻数据源
│   ├── fundamental_data/            # 基本面数据处理
│   │   ├── stock_restrictions.py   # 限售股数据结构
│   │   ├── financial_reports.py     # 财报数据结构
│   │   └── shareholder_data.py     # 股东数据结构
│   ├── database_manager.py          # 数据库管理
│   ├── data_processor.py           # 数据处理器
│   └── data_validator.py           # 数据验证器
├── 📁 strategies/                   # 策略层
│   ├── base_strategy.py            # 策略基类
│   ├── sector_selection/           # 板块选择
│   │   ├── sector_selector.py      # 板块选择器
│   │   ├── sector_rotation.py      # 板块轮动策略
│   │   └── sector_momentum.py      # 板块动量策略
│   ├── stock_selection/            # 个股选择
│   │   ├── factor_model.py         # 多因子模型
│   │   ├── technical_screener.py   # 技术面筛选
│   │   └── fundamental_screener.py # 基本面筛选
│   ├── trading_strategies/         # 交易策略
│   │   ├── momentum_strategy.py    # 动量策略
│   │   ├── mean_reversion.py       # 均值回归策略
│   │   └── breakout_strategy.py    # 突破策略
│   └── signal_generator.py         # 信号生成器
├── 📁 applications/                 # 应用层
│   ├── dashboard/                  # 可视化界面
│   │   ├── sector_viewer.py        # 板块选择可视化
│   │   ├── stock_screener_viewer.py # 个股筛选可视化
│   │   ├── portfolio_viewer.py     # 组合可视化
│   │   └── backtest_viewer.py      # 回测结果可视化
│   ├── backtest/                   # 回测引擎
│   ├── realtime/                   # 实时交易
│   └── analysis/                   # 分析工具
├── 📁 core/                        # 核心模块
│   ├── event_engine.py            # 事件驱动引擎
│   ├── execution/                 # 交易执行层
│   │   ├── account.py             # 账户管理
│   │   ├── position_manager.py    # 持仓管理
│   │   ├── risk_manager.py        # 风险管理
│   │   ├── order_manager.py       # 订单管理
│   │   └── gateways/              # 交易接口
│   │       ├── base_gateway.py     # 网关基类
│   │       ├── paper_trading_gateway.py # 模拟交易网关
│   │       ├── real_broker_gateway.py   # 实盘交易网关
│   │       └── binance_gateway.py  # 币安网关（示例）
│   └── utils/                     # 核心工具
│       ├── indicators.py          # 技术指标
│       ├── logger.py              # 日志管理
│       └── helpers.py             # 辅助函数
├── 📁 config/                      # 配置管理
├── 📁 tests/                       # 测试模块
├── 📁 docs/                        # 文档
├── 📁 data/                        # 数据文件
├── 📁 logs/                        # 日志文件
├── 📁 results/                     # 结果输出
└── 📁 legacy/                      # 历史代码
```

## ✨ 核心特性

### 📊 数据管理
- **多数据源支持**: 支持多种数据源接入
- **基本面数据**: 限售股、财报、股东数据等
- **数据标准化**: 统一的数据格式和处理流程
- **数据验证**: 自动数据质量检查

### 🧠 策略开发
- **事件驱动**: 基于事件驱动的策略框架
- **板块选择**: 智能板块轮动和选择
- **个股筛选**: 多因子选股模型
- **信号生成**: 统一的信号生成机制

### ⚡ 交易执行
- **风险控制**: 多层次风险管理
- **订单管理**: 智能订单执行
- **账户管理**: 资金和持仓管理
- **接口适配**: 支持多种交易接口

### 📈 可视化分析
- **板块分析**: 板块选择和轮动可视化
- **个股筛选**: 多因子选股界面
- **回测分析**: 策略回测结果展示
- **实时监控**: 实时交易监控

## 🚀 快速开始

### 1. 环境准备

```bash
# 确保使用 Python 3.11+
python --version

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置设置

```bash
# 复制配置文件
cp config/config.yaml.example config/config.yaml

# 编辑配置文件，设置数据源API密钥等
```

### 3. 初始化数据库

```python
from data_management.database_manager import DatabaseManager

# 初始化数据库
db_manager = DatabaseManager()
db_manager.init_database()
```

## 📚 开发指南

### 事件驱动架构

系统采用事件驱动架构，核心组件包括：

1. **事件引擎**: 管理事件循环和分发
2. **事件类型**: 定义各种事件类型（TICK, BAR, SIGNAL, ORDER等）
3. **事件监听器**: 处理特定类型的事件
4. **事件队列**: 异步事件处理

### 策略开发

```python
from strategies.base_strategy import BaseStrategy
from core.event_engine import EventType

class MyStrategy(BaseStrategy):
    def on_init(self):
        # 策略初始化
        pass
        
    def on_bar(self, symbol, bar_data):
        # 处理K线数据
        pass
        
    def on_tick(self, symbol, tick_data):
        # 处理实时行情
        pass
```

### 数据管理

```python
from data_management.sources.market_data_source import MarketDataSource
from data_management.database_manager import DatabaseManager

# 获取数据
data_source = MarketDataSource()
data = data_source.get_stock_data("000001", "2024-01-01", "2024-12-31")

# 存储数据
db_manager = DatabaseManager()
db_manager.store_data("k_daily", data)
```

## 🧪 测试

```bash
# 运行所有测试
python -m pytest tests/ -v

# 运行特定测试
python tests/test_event_engine.py
python tests/test_strategies.py
```

## 📝 更新日志

### v2.0.0 (2025-01)
- ✅ 重构为事件驱动架构
- ✅ 模块化设计，职责清晰
- ✅ 支持多数据源和交易接口
- ✅ 完整的可视化界面
- ✅ 完善的测试覆盖

## 📄 许可证

本项目仅供个人学习和研究使用。

## 🤝 贡献

欢迎提交Issue和Pull Request来改进项目。

---

**注意**: 使用本系统前请确保已正确配置相关数据源的API密钥，并遵守各数据源的使用条款。
