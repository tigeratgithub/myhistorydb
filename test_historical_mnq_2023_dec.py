import asyncio
import sys
import os
import pandas as pd
from datetime import datetime

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Contract, util

async def test_2023_dec_fetch():
    ib = IB()
    try:
        await ib.connectAsync('127.0.0.1', 4002, clientId=11)
        print("Connected to IB Gateway")

        # 2023年12月合约 (MNQZ23)
        # 12月15日是2023年12月的第三个周五
        contract_z23 = Contract(
            secType='FUT', 
            symbol='MNQ', 
            lastTradeDateOrContractMonth='20231215',
            exchange='CME', 
            currency='USD',
            includeExpired=True
        )
        
        print(f"\nTesting with MNQZ23 (Dec 2023)...")
        details = await ib.reqContractDetailsAsync(contract_z23)
        
        if details:
            target_contract = details[0].contract
            print(f"Contract found: {target_contract.localSymbol}, conId: {target_contract.conId}")
            
            # 尝试获取 2023年12月5日的数据 (在到期之前)
            end_time = datetime(2023, 12, 6, 0, 0, 0)
            bars = await ib.reqHistoricalDataAsync(
                target_contract,
                endDateTime=end_time,
                durationStr='1 D',
                barSizeSetting='3 mins',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if bars:
                print(f"Successfully fetched {len(bars)} bars for MNQZ23!")
                print(util.df(bars).head())
            else:
                print("No bars returned for MNQZ23.")
        else:
            print("Failed to find MNQZ23 contract details.")

    except Exception as e:
        print(f"Error during test: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(test_2023_dec_fetch())
