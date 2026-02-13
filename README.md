# MNQ/MGC 期货历史数据采集与可视化系统 (ibhistorydb)

这是一个基于 Interactive Brokers (IB) API 的 Python 库，专用于采集、存储和可视化 MNQ (微型纳指) 和 MGC (微型黄金) 等连续合约的期货历史数据。

## 主要特性

- **自动换月算法**: 内置 2023-2026+ 年的期货合约日历，自动处理 MNQ (季月) 和 MGC (双月) 的主力合约切换。
- **智能分片采集**: 
  - 自动将长周期请求拆分为 IB 接受的小片段 (Time-Slicing)。
  - 支持从 `1m` (1分钟) 到 `1d` (日线) 的各种周期。
  - **8年回溯**: 能够获取长达 8 年前的过期合约数据。
- **数据连续性保障**:
  - **Overlapping**: 合约切换边界自动向前后各延伸 10 天，确保无数据断档。
  - **INSERT OR IGNORE**: 采用逐行去重写入，防止重叠数据导致的插入失败。
- **增量更新**: 
  - 自动检测数据库中已有的最新时间点，仅获取新数据。
  - 支持从空库自动 Fallback 到全量抓取模式。
- **可视化**:
  - 集成 `lightweight-charts`，提供类似 TradingView 的交互式图表。
  - 自动将 UTC 时间转换为北京时间 (Asia/Shanghai) 显示。

## 安装与依赖

1. **前提条件**:
   - 安装 [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) 或 TWS，并开启 API 端口 (默认 4002 或 7496)。
   - Python 3.12+

2. **安装依赖**:
   ```bash
   pip install ib_async pandas lightweight-charts
   ```
   *本项目建议使用 `uv` 或 `venv` 管理虚拟环境。*

## 快速开始

### 1. 数据采集 (CLI 方式)

使用 `fetch_mnq_data.py` (旧版脚本) 或直接编写脚本调用 `ibhistorydb`。建议使用库方式：

**全量抓取 2024 年 MNQ 15分钟数据:**
```bash
python fetch_mnq_15m_2024.py
```

**通用采集脚本示例:**
```python
import asyncio
from ibhistorydb import Collector

async def main():
    # 初始化采集器 (连接到本地 IB Gateway, ID=自动随机)
    col = Collector(db='futures_data.db')
    
    # 增量更新 MNQ 和 MGC 的 15分钟和 1小时数据
    await col.sync(['MNQ', 'MGC'], timeframes=['15m', '1h'])

if __name__ == '__main__':
    asyncio.run(main())
```

### 2. 查看图表

使用 `Viewer` 类直接查看数据库中的数据：

```python
from ibhistorydb import Viewer

# 初始化查看器
view = Viewer(db='futures_data.db')

# 显示 MNQ 15分钟 K线图
view.show('MNQ', '15m')
```

或者运行示例脚本：
```bash
python show_mnq_15m.py
```

## 库结构说明 (`ibhistorydb/`)

- **`collector.py`**: 核心采集逻辑。包含 `Collector` 类，负责 IB 连接、合约查找、数据分片下载和数据库写入。
- **`viewer.py`**: 可视化逻辑。包含 `Viewer` 类，负责读取 SQLite，转换时区并调用图表库。
- **`calendar.py`**: 合约日历定义。包含 MNQ 和 MGC 的详细换月规则和合约代码表。
- **`utils.py`**: 通用工具。处理时间周期映射 (`1m` -> `1 min`) 和日期计算。

## 常见问题

- **连接失败 (Connection Refused)**: 请检查 IB Gateway 是否运行，以及端口配置是否匹配 (代码默认 4002)。脚本会自动重试并随机化 ClientID 以避免冲突。
- **数据为空**: 
    - 检查 IB 订阅权限是否包含相应期货数据。
    - 确认请求的日期范围不在 IB 允许的历史限制之外。
- **时区问题**: 数据库统一存储为 UTC/字符串格式，`Viewer` 显示时会自动转换为北京时间。

## 许可证

MIT License
