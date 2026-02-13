import pandas as pd
import sqlite3
import os

def check_gaps(db_name, table_name, expected_delta_mins):
    if not os.path.exists(db_name):
        print(f"Database {db_name} not found.")
        return

    conn = sqlite3.connect(db_name)
    df = pd.read_sql(f"SELECT time FROM {table_name} ORDER BY time", conn)
    conn.close()

    if df.empty:
        print(f"Table {table_name} is empty.")
        return

    df['time'] = pd.to_datetime(df['time'])
    df['diff'] = df['time'].diff()
    
    # 过滤掉周末 (通常是 48 小时左右的缺口)
    # MNQ/MGC 通常在周五 17:00 (EST) 到周日 18:00 (EST) 休息
    # 这里简单判断：超过预期 3 倍以上且不是周末的才算异常缺口
    threshold = pd.Timedelta(minutes=expected_delta_mins * 3)
    
    gaps = df[df['diff'] > threshold].copy()
    
    # 排除周末 (周五晚上到周日晚上)
    # 我们检查缺口开始和结束的时间戳
    def is_weekend_gap(row):
        # 简单逻辑：如果差值大于 24 小时，基本可以确认为正常周末或节假日
        if row['diff'] > pd.Timedelta(hours=24):
            return True
        return False

    real_gaps = gaps[~gaps.apply(is_weekend_gap, axis=1)]

    print(f"\n--- Verification Report: {table_name} ---")
    print(f"Total rows: {len(df)}")
    print(f"Time range: {df['time'].min()} to {df['time'].max()}")
    print(f"Potential Gaps Found: {len(real_gaps)}")
    if len(real_gaps) > 0:
        print(real_gaps[['time', 'diff']].head(20))
    else:
        print("No significant gaps detected (excluding weekends/holidays).")

if __name__ == "__main__":
    # 示例校验
    db = 'futures_data.db'
    check_gaps(db, 'bars_mnq_3m', 3)
    # check_gaps(db, 'bars_mgc_3m', 3)
