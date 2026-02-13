import asyncio
import json
import os
import sqlite3
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, sys_path)

from ib_async import IB, Contract, util
from futures_calendar import get_mnq_contracts, get_mgc_contracts

def get_timeframe_suffix(bar_size: str):
    size_map = {
        '1 min': '1m', '2 mins': '2m', '3 mins': '3m', '5 mins': '5m',
        '15 mins': '15m', '30 mins': '30m', '1 hour': '1h', '1 day': '1d'
    }
    return size_map.get(bar_size, bar_size.replace(' ', '').lower())

async def fetch_slice(ib, contract, end_datetime, duration_str, bar_size):
    """抓取单个时间分片，带重试机制"""
    print(f"    DEBUG: Requesting {duration_str} ending at {end_datetime} for {contract.localSymbol}...")
    for attempt in range(2):
        try:
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_datetime,
                durationStr=duration_str,
                barSizeSetting=bar_size,
                whatToShow='TRADES',
                useRTH=False,
                formatDate=2,
                timeout=90
            )
            return bars
        except asyncio.TimeoutError:
            print(f"  Timeout fetching {contract.localSymbol}, waiting 16s before retry ({attempt+1}/2)...")
            await asyncio.sleep(16)
        except Exception as e:
            if "query cancelled" in str(e).lower():
                print(f"  Query cancelled (Pacing violation), waiting 30s... ({attempt+1}/2)")
                await asyncio.sleep(30)
            else:
                print(f"  Error fetching {contract.localSymbol}: {e}")
                await asyncio.sleep(5)
    return None

def init_db(db_name):
    conn = sqlite3.connect(db_name, timeout=30) # 增加忙碌超时
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

def create_bars_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            time TIMESTAMP,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            average REAL,
            barCount INTEGER
        )
    ''')
    cursor.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_time ON {table_name}(time)")
    conn.commit()

def get_latest_timestamp(db_name, table_name):
    """查询表中最新的时间点"""
    try:
        conn = sqlite3.connect(db_name, timeout=30)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(time) FROM {table_name}")
        res = cursor.fetchone()[0]
        conn.close()
        if res:
            # 兼容 Unix 时间戳和 ISO 字符串
            dt = pd.to_datetime(res)
            if dt.tzinfo is not None:
                dt = dt.tz_localize(None)
            return dt.to_pydatetime()
    except:
        pass
    return None

async def retry_failures(ib, symbol, bar_size, task_start_time, db_name):
    print(f"\n--- Retrying Failures for {symbol} {bar_size} ---")
    conn = sqlite3.connect(db_name, timeout=30)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, contract_code, end_time 
        FROM failure_logs 
        WHERE symbol = ? AND bar_size = ? AND timestamp >= ? AND error_msg = 'No bars returned'
    """, (symbol, bar_size, task_start_time.strftime('%Y-%m-%d %H:%M:%S')))
    
    failures = cursor.fetchall()
    if not failures:
        print("No recent failures to retry.")
        return 0, 0

    retry_results = {"success": 0, "failed": 0}
    table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(bar_size)}"

    for log_id, contract_code, end_time_str in failures:
        try:
            end_dt = datetime.strptime(end_time_str, '%Y%m%d %H:%M:%S')
        except: continue

        ib_contract = Contract(secType='FUT', symbol=symbol, localSymbol=contract_code, exchange='CME' if symbol=='MNQ' else 'COMEX', currency='USD', includeExpired=True)
        qualified = await ib.qualifyContractsAsync(ib_contract)
        if not qualified: continue
        ib_contract = qualified[0]

        success_any = False
        for i in range(2):
            retry_end = end_dt - timedelta(days=i)
            bars = await fetch_slice(ib, ib_contract, retry_end, '1 D', bar_size)
            if bars:
                df = util.df(bars)
                df = df.rename(columns={'date': 'time'})
                df.to_sql(table_name, conn, if_exists='append', index=False)
                success_any = True
                break
            await asyncio.sleep(1)

        if success_any:
            cursor.execute("DELETE FROM failure_logs WHERE id = ?", (log_id,))
            retry_results["success"] += 1
        else:
            cursor.execute("UPDATE failure_logs SET error_msg = 'CONFIRMED_EMPTY' WHERE id = ?", (log_id,))
            retry_results["failed"] += 1
        conn.commit()

    conn.close()
    return retry_results["success"], retry_results["failed"]

async def run_task(ib, task, db_settings, is_full_mode):
    task_start_time = datetime.now()
    symbol = task['symbol']
    bar_size = task['bar_size']
    slice_days = task['slice_days']
    db_name = db_settings['db_name']
    table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(bar_size)}"
    
    conn = init_db(db_name)
    
    # 确定起点
    active_start_final = None
    if not is_full_mode:
        latest_ts = get_latest_timestamp(db_name, table_name)
        if latest_ts:
            # 增量模式：回溯 1 个分片长度以确保闭合
            active_start_final = latest_ts - timedelta(days=slice_days)
            print(f"\n>>> Incremental Task: {symbol} {bar_size} (Starting from {active_start_final})")
        else:
            print(f"\n>>> Table {table_name} not found. Falling back to FULL Mode for {symbol} {bar_size}.")
            is_full_mode = True

    if is_full_mode:
        print(f"\n>>> Full Task: {symbol} {bar_size} -> {table_name}")
        # 如果是全量且表已存在，由外部逻辑决定是否删除，这里只确保表存在
        active_start_final = datetime.strptime(task['start_date'], '%Y-%m-%d')
    
    create_bars_table(conn, table_name)
    
    if symbol == 'MNQ':
        calendar_data = get_mnq_contracts(task['start_year'], 2026)
    elif symbol == 'MGC':
        calendar_data = get_mgc_contracts(task['start_year'], 2026)
    else: return

    stats = {"total_bars": 0, "success_slices": 0, "failed_slices": 0}
    now = datetime.now()

    for cal in calendar_data:
        # 确保 cal['active_start'] 和 active_start_final 都是 naive 或 都是 aware
        # 这里统一转为 naive 比较
        s1 = cal['active_start'].replace(tzinfo=None)
        s2 = active_start_final.replace(tzinfo=None)
        
        c_start = max(s1, s2)
        c_end = min(cal['active_end'].replace(tzinfo=None), now.replace(tzinfo=None))
        
        if c_start >= c_end:
            continue
            
        print(f"  Contract: {cal['code']} ({c_start.date()} to {c_end.date()})")
        
        ib_contract = Contract(
            secType='FUT', symbol=symbol, lastTradeDateOrContractMonth=cal['expiry_month'],
            exchange='CME' if symbol == 'MNQ' else 'COMEX', currency='USD', includeExpired=True
        )
        qualified = await ib.qualifyContractsAsync(ib_contract)
        if not qualified: continue
        ib_contract = qualified[0]

        curr_end = c_end
        while curr_end > c_start:
            ib_end_str = curr_end.strftime('%Y%m%d %H:%M:%S')
            bars = await fetch_slice(ib, ib_contract, curr_end, f"{slice_days} D", bar_size)
            
            if bars:
                df = util.df(bars)
                df = df.rename(columns={'date': 'time'})
                try:
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                except sqlite3.IntegrityError:
                    pass
                except Exception as e:
                    print(f"    SQL Error while inserting into {table_name}: {e}")
                
                stats["total_bars"] += len(df)
                stats["success_slices"] += 1
                
                earliest = df['time'].min()
                if isinstance(earliest, pd.Timestamp):
                    curr_end = earliest.tz_localize(None).to_pydatetime() if earliest.tzinfo else earliest.to_pydatetime()
                else: curr_end -= timedelta(days=slice_days)
            else:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO failure_logs (symbol, contract_code, bar_size, end_time, error_msg) VALUES (?, ?, ?, ?, ?)',
                               (symbol, cal['code'], bar_size, ib_end_str, "No bars returned"))
                conn.commit()
                stats["failed_slices"] += 1
                curr_end -= timedelta(days=slice_days)
            await asyncio.sleep(11)

    # 去重
    print(f"  Finalizing {table_name}...")
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table_name} GROUP BY time)")
    conn.commit()
    conn.close()

    # 重试
    r_success, r_failed = await retry_failures(ib, symbol, bar_size, task_start_time, db_name)
    
    print(f"=== Summary {symbol} {bar_size}: {stats['total_bars']} bars, {r_success} retried ===")

def sync_config(args):
    config_path = 'config.json'
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        config = {"tasks": [], "db_settings": {"db_name": "futures_data.db"}}

    # 如果有命令行参数，覆盖/添加任务
    if args.symbols and args.timeframes:
        new_tasks = []
        for sym in args.symbols:
            for tf in args.timeframes:
                # 转换周期格式
                bar_size = tf.replace('m', ' mins')
                if 'h' in tf: bar_size = tf.replace('h', ' hour')
                
                # 默认值
                s_days = 4 if '3' in tf else 20
                
                # 查找旧配置保留 start_date 等
                existing = next((t for t in config['tasks'] if t['symbol']==sym and t['bar_size']==bar_size), None)
                
                task = {
                    "symbol": sym,
                    "bar_size": bar_size,
                    "slice_days": s_days,
                    "start_date": existing['start_date'] if existing else "2024-01-01",
                    "start_year": int(existing['start_year']) if existing else 2024
                }
                new_tasks.append(task)
        config['tasks'] = new_tasks
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
            print("Config updated based on parameters.")
    
    return config

async def main():
    parser = argparse.ArgumentParser(description="Futures Data Collector")
    parser.add_argument('--symbols', nargs='+', help='Symbols list e.g. MNQ MGC')
    parser.add_argument('--timeframes', nargs='+', help='Timeframes list e.g. 3m 15m')
    parser.add_argument('--full', action='store_true', help='Full fetch mode')
    args = parser.parse_args()

    config = sync_config(args)
    db_name = config['db_settings']['db_name']

    # 如果是全量模式，且指定了具体标的，则删除对应的表
    if args.full:
        conn = sqlite3.connect(db_name, timeout=30)
        c = conn.cursor()
        for task in config['tasks']:
            table_name = f"bars_{task['symbol'].lower()}_{get_timeframe_suffix(task['bar_size'])}"
            c.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Full mode: Dropped table {table_name}")
        conn.execute("VACUUM")
        conn.commit()
        conn.close()

    ib = IB()
    try:
        print("Connecting to IB Gateway...")
        await ib.connectAsync('127.0.0.1', 4002, clientId=55)
        for task in config['tasks']:
            await run_task(ib, task, config['db_settings'], is_full_mode=args.full)
    finally:
        if ib.isConnected():
            ib.disconnect()
        print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
