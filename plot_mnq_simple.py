import pandas as pd
import sqlite3
import sys
import os

# 路径处理
lc_project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'lightweight-charts-python'))
if lc_project_path not in sys.path:
    sys.path.insert(0, lc_project_path)

os.environ['PYWEBVIEW_GUI'] = 'edgechromium'
from lightweight_charts import Chart

def plot_data():
    db_name = 'futures_data.db'
    table_name = 'bars_mnq_3m'
    
    if not os.path.exists(db_name):
        print(f"Database {db_name} not found!")
        return

    # 1. 从 SQLite 载入数据
    conn = sqlite3.connect(db_name)
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        print(f"Loaded {len(df)} rows from SQLite table: {table_name}")
    except Exception as e:
        print(f"Error reading from database: {e}")
        return
    finally:
        conn.close()
    
    # 2. 核心修复：转换为北京时间 (UTC+8)
    # 先确保为 UTC，再转为上海时区，最后抹除时区信息以适配图表库的计算 Bug
    df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('Asia/Shanghai').dt.tz_localize(None).astype('datetime64[ns]')
    
    # 按照图表库期望的列名保留数据
    plot_df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
    plot_df = plot_df.sort_values('time')

    # 3. 创建图表
    chart = Chart(title='MNQ SQLite Data (3-Minute)')
    
    # 设置样式
    chart.layout(background_color='#131722', text_color='#d1d4dc')
    chart.candle_style(up_color='#26a69a', down_color='#ef5350')
    chart.volume_config(up_color='#26a69a', down_color='#ef5350')

    # 加载数据
    chart.set(plot_df)
    
    chart.fit()
    print("Opening chart window...")
    chart.show(block=True)

if __name__ == '__main__':
    plot_data()
