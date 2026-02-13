import asyncio
from ibhistorydb import Collector, Viewer

async def fetch_15m_data():
    db_file = 'mnq_2024_15m.db'
    
    # 1. 初始化收集器
    col = Collector(client_id=130, db=db_file)
    
    print(f"\n>>> 开始抓取 2024 全年 15m 数据 (MNQ)...")
    print(f">>> 目标数据库: {db_file}")
    
    # 2. 执行全量同步 (15m 周期)
    # 库内部会自动处理: 
    # - 20天分片 (utils.py)
    # - 合约换月边界 10天重叠 (collector.py)
    # - 唯一索引去重 (INSERT OR IGNORE)
    await col.sync(
        symbols=['MNQ'],
        timeframes=['15m'],
        start='2024-01-01',
        end='2024-12-31',
        mode='full'
    )
    
    print("\n>>> 抓取完成!")

if __name__ == "__main__":
    asyncio.run(fetch_15m_data())
