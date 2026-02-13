import asyncio
import sys
import os
import pandas as pd
import sqlite3
from datetime import datetime

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Contract, util

async def fetch_and_save(ib, contract, end_time, symbol_label, table_suffix):
    print(f"\nFetching {symbol_label} data ending at {end_time}...")
    
    # 获取合约详情以确保 conId
    details = await ib.reqContractDetailsAsync(contract)
    if not details:
        print(f"Failed to find contract details for {symbol_label}")
        return
    
    target_contract = details[0].contract
    print(f"Qualified Contract: {target_contract.localSymbol} (conId: {target_contract.conId})")

    # 请求 1 周的 3 分钟数据
    bars = await ib.reqHistoricalDataAsync(
        target_contract,
        endDateTime=end_time,
        durationStr='1 W',
        barSizeSetting='3 mins',
        whatToShow='TRADES',
        useRTH=False,
        formatDate=2  # 使用 Unix 时间戳 (秒)
    )

    if bars:
        df = util.df(bars)
        # 统一格式化
        df = df.rename(columns={'date': 'time'})
        df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_localize(None)
        df = df.sort_values('time')

        # 保存到 SQLite
        db_name = 'futures_data.db'
        table_name = f"bars_{symbol_label.lower()}_{table_suffix}"
        
        conn = sqlite3.connect(db_name)
        # if_exists='append'，因为我们要累积数据
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        
        print(f"Successfully saved {len(df)} bars to {table_name}")
    else:
        print(f"No bars returned for {symbol_label}")

async def main():
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 4002, clientId=15)
        print("Connected to IB Gateway")

        # 选定结束时间为 2024年3月11日 (周一) 00:00:00
        # 这样会回溯获取之前一周的数据 (即 3月4日到3月10日)
        end_time = datetime(2024, 3, 11, 0, 0, 0)

        # 1. MNQH4 (March 2024)
        mnq_contract = Contract(
            secType='FUT', symbol='MNQ', lastTradeDateOrContractMonth='20240315',
            exchange='CME', currency='USD', includeExpired=True
        )
        await fetch_and_save(ib, mnq_contract, end_time, 'MNQ', '3m')

        # 2. MGCJ4 (April 2024 - Active in March)
        mgc_contract = Contract(
            secType='FUT', symbol='MGC', lastTradeDateOrContractMonth='20240426', # 2024年4月合约
            exchange='COMEX', currency='USD', includeExpired=True # 注意 MGC 在 COMEX
        )
        await fetch_and_save(ib, mgc_contract, end_time, 'MGC', '3m')

    except Exception as e:
        print(f"Error: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
