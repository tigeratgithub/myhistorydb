import calendar
from datetime import datetime, timedelta

def get_mnq_contracts(start_year, end_year):
    """
    股指期货 (MNQ/MES) 滚动逻辑：
    - 活跃月份: 3, 6, 9, 12 (H, M, U, Z)
    - 滚动日期: 到期前一周的周四/周五。
    - 这里简化逻辑：合约在 3, 6, 9, 12 月的第 3 个周五到期，
    - 我们定义切换点为该月第 2 个周五。
    """
    contracts = []
    month_map = {3: 'H', 6: 'M', 9: 'U', 12: 'Z'}
    
    for year in range(start_year, end_year + 1):
        for month in [3, 6, 9, 12]:
            year_suffix = str(year)[-1]
            contract_code = f"MNQ{month_map[month]}{year_suffix}"
            expiry_month = f"{year}{month:02}"
            
            # 计算该月第 2 个周五作为切换点 (前一个合约结束，本合约开始)
            cal = calendar.monthcalendar(year, month)
            fridays = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
            roll_date = datetime(year, month, fridays[1])
            
            # 合约活跃期估算：
            # 本合约开始于上一个活跃月（3个月前）的第2个周五
            if month == 3:
                prev_year, prev_month = year - 1, 12
            else:
                prev_year, prev_month = year, month - 3
                
            prev_cal = calendar.monthcalendar(prev_year, prev_month)
            prev_fridays = [week[calendar.FRIDAY] for week in prev_cal if week[calendar.FRIDAY] != 0]
            start_date = datetime(prev_year, prev_month, prev_fridays[1])
            
            contracts.append({
                'code': contract_code,
                'expiry_month': expiry_month,
                'active_start': start_date,
                'active_end': roll_date
            })
    return contracts

def get_mgc_contracts(start_year, end_year):
    """
    黄金期货 (MGC) 滚动逻辑：
    - 活跃月份: 2, 4, 6, 8, 10, 12 (G, J, M, Q, V, Z)
    - 滚动规则: 通常在到期前一个月末（FND）滚动。
    - 简化：双月的第 1 个周五滚动。
    """
    contracts = []
    month_codes = {2: 'G', 4: 'J', 6: 'M', 8: 'Q', 10: 'V', 12: 'Z'}
    
    for year in range(start_year, end_year + 1):
        for month in [2, 4, 6, 8, 10, 12]:
            year_suffix = str(year)[-1]
            contract_code = f"MGC{month_codes[month]}{year_suffix}"
            expiry_month = f"{year}{month:02}"
            
            # 计算该月第 1 个周五作为切换点
            cal = calendar.monthcalendar(year, month)
            fridays = [week[calendar.FRIDAY] for week in cal if week[calendar.FRIDAY] != 0]
            roll_date = datetime(year, month, fridays[0])
            
            if month == 2:
                prev_year, prev_month = year - 1, 12
            else:
                prev_year, prev_month = year, month - 2
                
            prev_cal = calendar.monthcalendar(prev_year, prev_month)
            prev_fridays = [week[calendar.FRIDAY] for week in prev_cal if week[calendar.FRIDAY] != 0]
            start_date = datetime(prev_year, prev_month, prev_fridays[0])
            
            contracts.append({
                'code': contract_code,
                'expiry_month': expiry_month,
                'active_start': start_date,
                'active_end': roll_date
            })
    return contracts
