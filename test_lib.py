import asyncio
from ibhistorydb import Collector, Viewer

async def test_library():
    # 1. 初始化采集器
    # 使用新库名 ib_history.db，ClientId 设为 88
    col = Collector(client_id=88, db='ib_history.db')
    
    # 2. 模拟增量同步 (update)
    # 因为数据库目前是空的，它会自动 fallback 到全量模式
    # 我们测试抓取 MNQ 的 15m 和 1d 周期
    print("\n--- Testing Sync: MNQ 15m and 1d ---\n")
    await col.sync(
        symbols=['MNQ'], 
        timeframes=['15m', '1d'], 
        mode='update' 
    )
    
    # 3. 验证显示数据
    print("\n--- Testing Viewer: Showing MNQ 1d ---\n")
    view = Viewer(db='ib_history.db')
    view.show(symbol='MNQ', timeframe='1d', title="Library Test - MNQ Daily")

if __name__ == "__main__":
    asyncio.run(test_library())
