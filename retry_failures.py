import asyncio
import sqlite3
import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Contract, util

def get_timeframe_suffix(bar_size: str):
    size_map = {
        '1 min': '1m', '2 mins': '2m', '3 mins': '3m', '5 mins': '5m',
        '15 mins': '15m', '30 mins': '30m', '1 hour': '1h', '1 day': '1d'
    }
    return size_map.get(bar_size, bar_size.replace(' ', '').lower())

async def retry_task(ib, db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # 获取待处理的失败记录
    cursor.execute("SELECT id, symbol, contract_code, bar_size, end_time FROM failure_logs WHERE error_msg = 'No bars returned'")
    failures = cursor.fetchall()
    
    if not failures:
        print("No failures to retry.")
        return

    print(f"Found {len(failures)} failures to retry with 1-day slices...")

    for log_id, symbol, contract_code, bar_size, end_time_str in failures:
        # 解析结束时间
        try:
            # 原程序存储格式：%Y%m%d %H:%M:%S
            end_dt = datetime.strptime(end_time_str, '%Y%m%d %H:%M:%S')
        except:
            print(f"  Invalid time format: {end_time_str}")
            continue

        # 过滤校验：如果请求时间点比合约“预期活跃期”早太多，直接忽略
        # 例如：合约代码最后一位是年份。如果是 MGCJ4 (2024)，但请求的是 2022 年，那肯定没数据
        contract_year_char = contract_code[-1]
        try:
            # 简单启发式：假设年份是 202x
            contract_year = 2020 + int(contract_year_char)
            # 如果请求时间早于合约到期年前 15 个月，极大概率无成交
            if end_dt.year < contract_year - 1 and end_dt.month < 10:
                print(f"  [Skipping] {contract_code} at {end_time_str} is likely too early for historical data.")
                cursor.execute("UPDATE failure_logs SET error_msg = 'UNAVAILABLE' WHERE id = ?", (log_id,))
                conn.commit()
                continue
        except:
            pass

        print(f"  Retrying {contract_code} ({bar_size}) ending at {end_time_str} (1-day slices)...")
        
        # 定义合约 (尝试重新 qualify)
        # 我们假设 contract_code 里的月份字母对应的完整月份
        # 由于我们之前是用 expiry_month 存的，这里稍微麻烦点，但我们可以靠 symbol 和 code 搜
        ib_contract = Contract(secType='FUT', symbol=symbol, localSymbol=contract_code, exchange='CME' if symbol=='MNQ' else 'COMEX', currency='USD', includeExpired=True)
        qualified = await ib.qualifyContractsAsync(ib_contract)
        if not qualified:
            print(f"    Could not qualify contract {contract_code}")
            continue
        ib_contract = qualified[0]

        # 将原来的 7天/30天 片段拆解为 1天 的多次抓取
        # 假设原片段是 [end_dt - 7 days, end_dt] (具体取决于 collector 逻辑，但我们至少重试 end_dt 这一天)
        # 这里为了稳妥，我们尝试往前抓 3 天，每天一个 short request
        success_any = False
        table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(bar_size)}"
        
        for i in range(3): # 尝试最近的 3 天
            retry_end = end_dt - timedelta(days=i)
            print(f"    Sub-slice retry: {retry_end.strftime('%Y%m%d %H:%M:%S')} (1 D)")
            
            try:
                bars = await ib.reqHistoricalDataAsync(
                    ib_contract,
                    endDateTime=retry_end,
                    durationStr='1 D',
                    barSizeSetting=bar_size,
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=2,
                    timeout=30
                )
                
                if bars:
                    df = util.df(bars)
                    df = df.rename(columns={'date': 'time'})
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                    print(f"    Success! Saved {len(bars)} bars.")
                    success_any = True
            except Exception as e:
                print(f"    Failed sub-slice: {e}")
            
            await asyncio.sleep(1) # 避开频率限制

        if success_any:
            # 如果这 3 天里有任何一天成功，我们就认为修复了该 log 片段（或者该片段已部分恢复）
            cursor.execute("DELETE FROM failure_logs WHERE id = ?", (log_id,))
            print(f"    Cleared log entry {log_id}")
        else:
            # 依然失败，标记为再次确认后的空数据
            cursor.execute("UPDATE failure_logs SET error_msg = 'CONFIRMED_EMPTY' WHERE id = ?", (log_id,))
            print(f"    Marked as confirmed empty.")
        
        conn.commit()

    conn.close()

async def main():
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 4002, clientId=25)
        await retry_task(ib, 'futures_data.db')
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
