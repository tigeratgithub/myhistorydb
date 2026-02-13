import asyncio
from ibhistorydb import Collector, Viewer

async def sync_data(col, symbols, timeframes, start, end):
    await col.sync(
        symbols=symbols,
        timeframes=timeframes,
        start=start,
        end=end,
        mode='full'
    )

def run_2024_test():
    db_file = 'test_2024_daily.db'
    col = Collector(client_id=123, db=db_file)
    
    print(f"\n>>> 开始抓取 2024 全年日线数据 (MNQ, MGC)...")
    
    # 异步执行同步逻辑
    asyncio.run(sync_data(col, ['MNQ', 'MGC'], ['1d'], '2024-01-01', '2024-12-31'))
    
    print("\n>>> 抓取完成，现在可以安全启动图表显示 (非异步环境)...")
    
    # 验证显示数据
    view = Viewer(db=db_file)
    # 不传入 block 参数，或者由 Viewer 内部处理，确保在主线程运行即可
    view.show(symbol='MNQ', timeframe='1d', title="MNQ 2024 Daily Data")

if __name__ == "__main__":
    run_2024_test()
