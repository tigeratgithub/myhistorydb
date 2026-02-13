import asyncio
import sys
import os
import pandas as pd
from datetime import datetime

# 路径处理
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'ib_async'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from ib_async import IB, Future, util

async def test_3year_fetch():
    ib = IB()
    try:
        # 连接到 IB Gateway
        await ib.connectAsync('127.0.0.1', 4002, clientId=9)
        print("Connected to IB Gateway")

        # 1. 先试试搜一下 MNQ 的匹配项
        print("Searching for MNQ matching symbols...")
        descriptions = await ib.reqMatchingSymbolsAsync('MNQ')
        for desc in descriptions:
            print(f"Match: {desc.contract.symbol} on {desc.contract.exchange}, conId: {desc.contract.conId}")

        # 2. 试试近一点的过期合约，比如 2024年12月 (MNQZ24)
        print("\nTesting with a more recent expired contract (MNQZ24)...")
        from ib_async import Contract
        contract_z24 = Contract(
            secType='FUT', 
            symbol='MNQ', 
            lastTradeDateOrContractMonth='20241220', # 2024年12月20日
            exchange='CME', 
            currency='USD',
            includeExpired=True
        )
        
        details = await ib.reqContractDetailsAsync(contract_z24)
        if details:
            target_contract = details[0].contract
            print(f"Contract found: {target_contract.localSymbol}, conId: {target_contract.conId}")
            
            # 尝试获取 2024年12月10日的数据
            end_time = datetime(2024, 12, 11, 0, 0, 0)
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
                print(f"Successfully fetched {len(bars)} bars for MNQZ24!")
            else:
                print("No bars for MNQZ24.")
        else:
            print("Failed to find MNQZ24 details.")

        # 请求 2023-02-14 00:00:00 结束的 1天数据
        end_time = datetime(2023, 2, 14, 0, 0, 0)
        
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
            df = util.df(bars)
            print(f"Successfully fetched {len(df)} bars from 3 years ago!")
            print(df.head())
        else:
            print("No bars returned. This might mean IBKR does not provide 3-minute historical data this far back for expired futures.")

    except Exception as e:
        print(f"Error during test: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    asyncio.run(test_3year_fetch())
