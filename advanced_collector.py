import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
import pandas as pd

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Contract, util
from futures_calendar import get_mnq_contracts, get_mgc_contracts, get_nth_weekday

def get_timeframe_suffix(bar_size: str):
    size_map = {
        '1 min': '1m', '2 mins': '2m', '3 mins': '3m', '5 mins': '5m',
        '15 mins': '15m', '30 mins': '30m', '1 hour': '1h', '1 day': '1d'
    }
    return size_map.get(bar_size, bar_size.replace(' ', '').lower())

async def fetch_slice(ib, contract, end_datetime, duration_str, bar_size):
    """抓取单个时间分片"""
    try:
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=end_datetime,
            durationStr=duration_str,
            barSizeSetting=bar_size,
            whatToShow='TRADES',
            useRTH=False,
            formatDate=2, # Unix timestamp
            timeout=60
        )
        return bars
    except Exception as e:
        print(f"  Error fetching {contract.localSymbol}: {e}")
        return None

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS failure_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            contract_code TEXT,
            bar_size TEXT,
            end_time TEXT,
            error_msg TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    return conn

async def run_task(ib, task, db_settings):
    symbol = task['symbol']
    bar_size = task['bar_size']
    start_year = task['start_year']
    slice_days = task['slice_days']
    db_name = db_settings['db_name']
    
    table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(bar_size)}"
    print(f"\n>>> Starting Task: {symbol} {bar_size} -> {table_name}")
    
    # 获取换月日历
    if symbol == 'MNQ':
        calendar_data = get_mnq_contracts(start_year, 2026)
    elif symbol == 'MGC':
        calendar_data = get_mgc_contracts(start_year, 2026)
    else:
        print(f"Unsupported symbol: {symbol}")
        return

    now = datetime.now()
    conn = init_db(db_name)

    for cal in calendar_data:
        contract_code = cal['code']
        active_start = cal['active_start']
        active_end = min(cal['active_end'], now)
        
        if active_start >= now:
            continue
            
        print(f"  Processing Contract: {contract_code} ({active_start.date()} to {active_end.date()})")
        
        # 定义合约
        ib_contract = Contract(
            secType='FUT', symbol=symbol, lastTradeDateOrContractMonth=cal['expiry_month'],
            exchange='CME' if symbol == 'MNQ' else 'COMEX', currency='USD', includeExpired=True
        )
        
        qualified = await ib.qualifyContractsAsync(ib_contract)
        if not qualified:
            print(f"    Failed to qualify {contract_code}")
            continue
        ib_contract = qualified[0]

        # 分片逻辑
        current_end = active_end
        while current_end > active_start:
            # IB 的 reqHistoricalData 是向后获取，结束时间为 current_end
            # 持续请求直到覆盖 active_start
            
            # 格式化结束时间为 IB 格式 (YYYYMMDD HH:mm:ss)
            ib_end_str = current_end.strftime('%Y%m%d %H:%M:%S')
            duration = f"{slice_days} D"
            
            bars = await fetch_slice(ib, ib_contract, current_end, duration, bar_size)
            
            if bars:
                df = util.df(bars)
                # 转换时间
                df = df.rename(columns={'date': 'time'})
                # 注意：这里我们存入原始数据，绘图时再处理精度
                
                # 存入数据库
                # 使用 unique constraint 或者先读取再合并来去重
                # 简便方案：每次存入后在 SQL 层做去重，或者 python 层合并
                
                # 检查是否已有数据 (粗略检查)
                try:
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                    # 立即去重 (SQLite 技巧)
                    # 虽然低效，但能保证数据干净。更好的办法是定义 UNIQUE(time)
                    pass 
                except Exception as e:
                    print(f"    DB Error: {e}")
                
                print(f"    Fetched {len(bars)} bars. Newest: {df['time'].max()}, Oldest: {df['time'].min()}")
                
                # 更新 current_end
                # 减去 duration 以继续向前抓取
                # 注意：IB 的 bars 是有序的，可以直接取最早的一条时间作为下一次的结束时间
                earliest_time = df['time'].min()
                if isinstance(earliest_time, pd.Timestamp):
                    # 确保是 offset-naive 以便和 active_start 比较
                    if earliest_time.tzinfo is not None:
                        current_end = earliest_time.tz_localize(None).to_pydatetime()
                    else:
                        current_end = earliest_time.to_pydatetime()
                else:
                    current_end -= timedelta(days=slice_days)
            else:
                # 记录失败
                cursor = conn.cursor()
                cursor.execute('INSERT INTO failure_logs (symbol, contract_code, bar_size, end_time, error_msg) VALUES (?, ?, ?, ?, ?)',
                               (symbol, contract_code, bar_size, ib_end_str, "No bars returned"))
                conn.commit()
                print(f"    Failed to fetch slice ending at {ib_end_str}. Logged.")
                current_end -= timedelta(days=slice_days)

            # 稍微停顿，避免触发 IB 频率限制
            await asyncio.sleep(1)

    # 任务结束后，对核心表进行全局去重
    print(f"  Finalizing {table_name}: Removing duplicates...")
    cursor = conn.cursor()
    # 建立唯一索引 (如果不存在)
    try:
        cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_time ON {table_name}(time)")
    except Exception as e:
        # 如果已有重复数据，CREATE INDEX 会报错，此时需要手动去重
        cursor.execute(f"""
            DELETE FROM {table_name} 
            WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table_name} GROUP BY time)
        """)
        cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_time ON {table_name}(time)")
    conn.commit()
    conn.close()

async def main():
    with open('config.json', 'r') as f:
        config = json.load(f)
        
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 4002, clientId=20)
        for task in config['tasks']:
            await run_task(ib, task, config['db_settings'])
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
