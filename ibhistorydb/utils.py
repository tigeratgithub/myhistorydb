from datetime import datetime, timedelta

def get_timeframe_suffix(tf: str):
    """
    将用户输入的周期映射为数据库后缀
    """
    size_map = {
        '1m': '1m', '3m': '3m', '15m': '15m', 
        '1d': '1d', '1w': '1w', '1mon': '1mon', '1y': '1y',
        '1M': '1mon' # 兼容大写 M
    }
    return size_map.get(tf, tf.lower())

def timeframe_to_bar_size(tf: str):
    """
    将用户周期映射为 IB 要求的 barSizeSetting
    """
    mapping = {
        '1m': '1 min',
        '3m': '3 mins',
        '15m': '15 mins',
        '1d': '1 day',
        '1w': '1 week',
        '1mon': '1 month',
        '1M': '1 month',
        '1y': '1 year'
    }
    return mapping.get(tf, '1 day')

def get_slice_params(tf: str):
    """
    映射不同轴的分片天数和抓取时长字符串 (durationStr)
    为了符合 IB 2500 条限制：
    - 1min: 1天
    - 3mins: 4天
    - 15mins: 20天
    - 1day: 60天 (约 45 根 K 线，安全且防截断)
    - 1week 及以上: 1年 (约 52 根 K 线)
    """
    if tf == '1m': return 1, '1 D'
    if tf == '3m': return 4, '4 D'
    if tf == '15m': return 20, '20 D'
    if tf in ['1d', '1w']: return 60, '60 D'
    return 365, '1 Y'

def calculate_date_range(start=None, end=None):
    """
    核心逻辑：
    - start: max(start, 8年前)
    - end: max(end, 8年前), 默认现在
    - 返回两个 naive datetime
    """
    # 将限制放宽到 8 年，这对过期期货已经足够
    limit_date = datetime.now() - timedelta(days=365 * 8)
    
    if end is None:
        final_end = datetime.now()
    else:
        if isinstance(end, str):
            final_end = datetime.strptime(end, '%Y-%m-%d')
        else:
            final_end = end
        final_end = max(final_end, limit_date)
        
    if start is None:
        final_start = limit_date
    else:
        if isinstance(start, str):
            final_start = datetime.strptime(start, '%Y-%m-%d')
        else:
            final_start = start
        final_start = max(final_start, limit_date)
        
    # 确保起点不晚于终点
    if final_start > final_end:
        final_start = final_end - timedelta(days=1)
        
    return final_start.replace(tzinfo=None), final_end.replace(tzinfo=None)
