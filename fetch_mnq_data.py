import asyncio
import sys
import os
import pandas as pd
import sqlite3

# 在导入 ib_async 之前创建一个事件循环，防止 eventkit 在初始化时报错
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Future, util

def get_timeframe_suffix(bar_size: str):
    """将 IB 的 barSizeSetting 转换为规范的表名后缀"""
    size_map = {
        '1 min': '1m', '2 mins': '2m', '3 mins': '3m', '5 mins': '5m',
        '10 mins': '10m', '15 mins': '15m', '30 mins': '30m',
        '1 hour': '1h', '2 hours': '2h', '4 hours': '4h',
        '1 day': '1d', '1 week': '1w', '1 month': '1M'
    }
    return size_map.get(bar_size, bar_size.replace(' ', '').lower())

async def main():
    ib = IB()
    try:
        # 连接到 IB Gateway，端口 4002
        await ib.connectAsync('127.0.0.1', 4002, clientId=1)
        print("Connected to IB Gateway")

        # 配置参数
        symbol = 'MNQ'
        bar_size = '3 mins'
        duration = '1 W'

        # 定义 MNQ 期货
        contract = Future(symbol=symbol, exchange='CME', currency='USD')
        
        # 获取合约详情以寻找主力合约
        details = await ib.reqContractDetailsAsync(contract)
        if not details:
            print(f"No contract details found for {symbol}")
            return

        # 按最后交易日排序获取主力合约
        details.sort(key=lambda x: x.contract.lastTradeDateOrContractMonth)
        print(f"Found {len(details)} contracts. Selecting the front month contract.")
        main_contract = details[0].contract
        print(f"Selected Contract: {main_contract.localSymbol} (Exp: {main_contract.lastTradeDateOrContractMonth})")

        # 请求历史数据 (使用 formatDate=2 获取 Unix 时间戳)
        bars = await ib.reqHistoricalDataAsync(
            main_contract,
            endDateTime='',
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=2  # 使用 Unix 时间戳 (秒)
        )

        if bars:
            df = util.df(bars)
            
            # 格式化数据：此时 'date' 列已经是 Timestamp 对象
            df = df.rename(columns={'date': 'time'})
            # 确保按时间排序
            df = df.sort_values('time')

            # 1. 保存为 CSV (备份)
            csv_filename = f'{symbol.lower()}_{get_timeframe_suffix(bar_size)}_data.csv'
            df.to_csv(csv_filename, index=True)

            # 2. 保存到 SQLite
            db_name = 'futures_data.db'
            table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(bar_size)}"
            
            conn = sqlite3.connect(db_name)
            # 存入 SQLite 时，Pandas 会自动将 Timestamp 转为标准的 ISO 字符串或整数，取决于驱动
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            
            print(f"Successfully saved {len(df)} bars (Unix format) to SQLite: {table_name}")
        else:
            print("No bars returned from IB")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
