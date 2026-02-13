import pandas as pd
import sqlite3
import os
import sys

# 处理内部依赖路径
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lc_path = os.path.join(base_dir, 'lightweight-charts-python')
if lc_path not in sys.path:
    sys.path.insert(0, lc_path)

from lightweight_charts import Chart
from .utils import get_timeframe_suffix

class Viewer:
    def __init__(self, db='ib_history.db'):
        self.db = db

    def show(self, symbol, timeframe, title=None, block=True):
        if not os.path.exists(self.db):
            print(f"Database {self.db} not found!")
            return

        table_name = f"bars_{symbol.lower()}_{get_timeframe_suffix(timeframe)}"
        conn = sqlite3.connect(self.db)
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        except Exception as e:
            print(f"Error reading {table_name}: {e}")
            return
        finally:
            conn.close()

        if df.empty:
            print(f"No data found in {table_name}")
            return

        # 时间处理 (转换为北京时间并抹除时区以适配 LWC 计算)
        df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('Asia/Shanghai').dt.tz_localize(None).astype('datetime64[ns]')
        plot_df = df[['time', 'open', 'high', 'low', 'close', 'volume']].sort_values('time')

        # 图表配置
        chart_title = title if title else f"{symbol} {timeframe} Historical Data"
        chart = Chart(title=chart_title)
        chart.layout(background_color='#131722', text_color='#d1d4dc')
        chart.candle_style(up_color='#26a69a', down_color='#ef5350')
        chart.set(plot_df)
        chart.fit()
        
        print(f"Showing chart for {symbol} {timeframe}...")
        chart.show(block=block)
