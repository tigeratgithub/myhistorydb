from ibhistorydb import Viewer

if __name__ == '__main__':
    # 从最新的 15m 数据库中加载 MNQ 数据
    view = Viewer(db='ib_2024_2025.db')
    
    print("Showing MNQ 15m chart...")
    # 显示 MNQ 15m 图表，自动转换为北京时间
    view.show(symbol='MNQ', timeframe='15m', title='MNQ 2024 Full Year (15m)', block=True)
