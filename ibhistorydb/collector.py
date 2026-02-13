import asyncio
import sqlite3
import random
from datetime import datetime, timedelta
import pandas as pd
from ib_async import IB, Contract, util
from .calendar import get_mnq_contracts, get_mgc_contracts
from .utils import (
    get_timeframe_suffix, 
    timeframe_to_bar_size, 
    get_slice_params, 
    calculate_date_range
)

class Collector:
    def __init__(self, client_id=None, db='ib_history.db'):
        self.client_id = client_id if client_id is not None else random.randint(100, 999)
        self.db = db
        self.ib = IB()

    async def _connect(self):
        if not self.ib.isConnected():
            # 随机化 ClientId 并增加等待，防止频繁连接导致 IB 拒绝
            import random, time
            if self.client_id < 1000:
                self.client_id = random.randint(1000, 9999)
            print(f"Connecting to IB Gateway (ClientId: {self.client_id})...")
            await asyncio.sleep(2)
            await self.ib.connectAsync('127.0.0.1', 4002, clientId=self.client_id)

    def _init_db(self):
        conn = sqlite3.connect(self.db, timeout=30)
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

    def _ensure_table(self, conn, table_name):
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

    def _get_latest_ts(self, table_name):
        try:
            conn = sqlite3.connect(self.db, timeout=30)
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(time) FROM {table_name}")
            res = cursor.fetchone()[0]
            conn.close()
            if res:
                dt = pd.to_datetime(res)
                return dt.tz_localize(None).to_pydatetime() if dt.tzinfo else dt.to_pydatetime()
        except: pass
        return None

    async def _fetch_slice(self, contract, end_dt, duration, bar_size):
        for attempt in range(2):
            try:
                bars = await self.ib.reqHistoricalDataAsync(
                    contract, endDateTime=end_dt, durationStr=duration,
                    barSizeSetting=bar_size, whatToShow='TRADES', useRTH=False, formatDate=2, timeout=90
                )
                return bars
            except asyncio.TimeoutError:
                await asyncio.sleep(16)
            except Exception as e:
                if "query cancelled" in str(e).lower():
                    await asyncio.sleep(30)
                else:
                    await asyncio.sleep(5)
        return None

    async def sync(self, symbols, start=None, end=None, timeframes=None, mode='update'):
        if timeframes is None: timeframes = ['3m']
        await self._connect()
        conn = self._init_db()
        
        try:
            for symbol in symbols:
                for tf in timeframes:
                    table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(tf)}"
                    bar_size = timeframe_to_bar_size(tf)
                    slice_days, duration_str = get_slice_params(tf)
                    
                    self._ensure_table(conn, table_name)
                    
                    # 确定起止点
                    active_start, active_end = calculate_date_range(start, end)
                    
                    if mode == 'update':
                        latest = self._get_latest_ts(table_name)
                        if latest:
                            # 增量回溯：如果有分片则回溯分片，否则回溯1天
                            backstep = slice_days if slice_days else 1
                            active_start = latest - timedelta(days=backstep)
                    elif mode == 'full':
                        cursor = conn.cursor()
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        conn.commit()
                        self._ensure_table(conn, table_name)

                    print(f"\n>>> Syncing {symbol} {tf} ({mode}) | Range: {active_start.date()} to {active_end.date()}")
                    
                    # 动态计算日历覆盖范围 (确保包含请求范围前后各一个季度)
                    cal_start_year = active_start.year - 1
                    cal_end_year = active_end.year + 1
                    calendar = get_mnq_contracts(cal_start_year, cal_end_year) if symbol == 'MNQ' else get_mgc_contracts(cal_start_year, cal_end_year)
                    
                    # 重叠窗口：在每个合约的活跃期两端各加 10 天 padding
                    # 这样相邻合约在换月边界处会有 ~20 天的重叠区域
                    # 数据库的 UNIQUE INDEX 会自动去重，保留先写入的那条
                    OVERLAP_DAYS = 10
                    
                    for cal in calendar:
                        s_dt = max(cal['active_start'].replace(tzinfo=None) - timedelta(days=OVERLAP_DAYS), active_start)
                        e_dt = min(cal['active_end'].replace(tzinfo=None) + timedelta(days=OVERLAP_DAYS), active_end)
                        
                        # Debug info
                        print(f"Checking {cal['code']}: Cal range {cal['active_start'].date()}~{cal['active_end'].date()} | Intersect: {s_dt.date()}~{e_dt.date()}")
                        
                        if s_dt >= e_dt: continue
                        
                        ib_contract = Contract(
                            secType='FUT', symbol=symbol, lastTradeDateOrContractMonth=cal['expiry_month'],
                            exchange='CME' if symbol=='MNQ' else 'COMEX', currency='USD', includeExpired=True
                        )
                        qualified = await self.ib.qualifyContractsAsync(ib_contract)
                        if not qualified:
                            print(f"  Warning: Could not qualify contract {cal['code']} ({cal['expiry_month']}). Skipping.")
                            continue
                        
                        ib_contract = qualified[0]
                        print(f"  Contract: {cal['code']} ({s_dt.date()} to {e_dt.date()})")
                        
                        curr_end = e_dt
                        while curr_end > s_dt:
                            # 确定分片时长
                            fetch_duration = duration_str if duration_str else f"{(curr_end - s_dt).days + 1} D"
                            
                            
                            # IB API 要求 endDateTime 为字符串格式，使用 23:59:59 确保包含当天数据
                            end_str = curr_end.strftime('%Y%m%d') + ' 23:59:59'
                            
                            bars = await self._fetch_slice(ib_contract, end_str, fetch_duration, bar_size)
                            if bars:
                                df = util.df(bars).rename(columns={'date': 'time'})
                                if df.empty:
                                    # 如果返回了空列表，说明这部分没数据，往前推
                                    curr_end -= timedelta(days=slice_days if slice_days else 1)
                                    continue
                                
                                # 将时间列转换为字符串以适配 sqlite3
                                df['time'] = df['time'].astype(str)
                                
                                # 使用 INSERT OR IGNORE 逐行写入，避免 UNIQUE 冲突导致整批失败
                                cursor = conn.cursor()
                                for _, row in df.iterrows():
                                    try:
                                        cursor.execute(
                                            f"INSERT OR IGNORE INTO {table_name} (time, open, high, low, close, volume, average, barCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                            (row['time'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['average'], row['barCount'])
                                        )
                                    except Exception as e:
                                        print(f"SQL Insert Error: {e}")
                                conn.commit()
                                
                                # 获取这批数据里最早的时间点
                                earliest = df['time'].min()
                                
                                # 日线数据返回的是 datetime.date，分钟线返回的是 pd.Timestamp
                                import datetime as dt_module
                                if isinstance(earliest, pd.Timestamp):
                                    new_end = earliest.tz_localize(None).to_pydatetime() if earliest.tzinfo else earliest.to_pydatetime()
                                elif isinstance(earliest, dt_module.date) and not isinstance(earliest, datetime):
                                    # datetime.date → datetime (午夜)
                                    new_end = datetime.combine(earliest, datetime.min.time())
                                elif isinstance(earliest, datetime):
                                    new_end = earliest.replace(tzinfo=None) if earliest.tzinfo else earliest
                                else:
                                    new_end = curr_end - timedelta(days=slice_days if slice_days else 1)
                                
                                print(f"    Earliest data point: {earliest} ({type(earliest)}) -> New end: {new_end}")

                                # 如果 new_end 没有往前推进，强制推 1 天避免死循环
                                if new_end >= curr_end:
                                    # 对于分钟线，1 天可能不够，尝试根据 slice_days 回退
                                    backstep = slice_days if slice_days else 1
                                    print(f"    Forcing backup {backstep} days (stuck at {curr_end})")
                                    new_end = curr_end - timedelta(days=backstep)
                                
                                curr_end = new_end
                                
                                # 如果已经抓到了 s_dt 之前，可以结束这个合约
                                if curr_end <= s_dt:
                                    print(f"    Reached start date {s_dt} for {cal['code']}.")
                                    break
                            else:
                                print(f"    IB returned None for {cal['code']} ending {curr_end}.")
                                break
                            
                            await asyncio.sleep(11)

                    # 结尾去重与收缩
                    conn.execute(f"DELETE FROM {table_name} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {table_name} GROUP BY time)")
                    conn.commit()
        finally:
            conn.close()
            self.ib.disconnect()
            print("Sync complete.")
