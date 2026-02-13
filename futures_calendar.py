from datetime import datetime, timedelta
import calendar

def get_nth_weekday(year, month, n, weekday):
    """获取某年某月的第 n 个周几 (0=周一, 4=周五)"""
    cal = calendar.monthcalendar(year, month)
    count = 0
    for week in cal:
        if week[weekday] != 0:
            count += 1
            if count == n:
                return datetime(year, month, week[weekday])
    return None

def get_mnq_contracts(start_year, end_year):
    """
    生成 MNQ 换月日历 (2024-2026)
    规则：季月 (3, 6, 9, 12)。滚动发生在到期月（第3个周五）那一周的周一。
    """
    contracts = []
    months = [3, 6, 9, 12]
    codes = {3: 'H', 6: 'M', 9: 'U', 12: 'Z'}
    
    # 按照季度滚动
    current_contract_start = datetime(start_year, 1, 1)
    
    for year in range(start_year, end_year + 1):
        for month in months:
            # 找到该月第3个周五之前那个周一作为换月点
            third_friday = get_nth_weekday(year, month, 3, 4)
            roll_date = third_friday - timedelta(days=4) # 周一
            
            contract_code = f"MNQ{codes[month]}{str(year)[-1 if year < 2030 else -2:]}"
            # 特殊处理下 2025/2026 的显示，通常是个位数年份代码
            # 但为了安全，我们这里统一使用最后一位
            
            contracts.append({
                'symbol': 'MNQ',
                'code': contract_code,
                'expiry_month': f"{year}{month:02}",
                'active_start': current_contract_start,
                'active_end': roll_date
            })
            current_contract_start = roll_date
            
    return contracts

def get_mgc_contracts(start_year, end_year):
    """
    生成 MGC 换月日历 (2024-2026)
    规则：双月 (2, 4, 6, 8, 10, 12)。滚动发生在到期前一个月（单月）的 25 日左右。
    """
    contracts = []
    months = [2, 4, 6, 8, 10, 12]
    codes = {2: 'G', 4: 'J', 6: 'M', 8: 'Q', 10: 'V', 12: 'Z'}
    
    current_contract_start = datetime(start_year, 1, 1)
    
    for year in range(start_year, end_year + 1):
        for month in months:
            # 换月发生在合约月份的前一个月 25 日
            if month == 2:
                roll_year, roll_month = year - 1, 1
            else:
                roll_year, roll_month = year, month - 1
            
            roll_date = datetime(roll_year, roll_month, 25)
            
            contract_code = f"MGC{codes[month]}{str(year)[-1]}"
            
            contracts.append({
                'symbol': 'MGC',
                'code': contract_code,
                'expiry_month': f"{year}{month:02}",
                'active_start': current_contract_start,
                'active_end': roll_date
            })
            current_contract_start = roll_date
            
    return contracts

if __name__ == "__main__":
    # 测试打印 2024-2025 日历
    print("--- MNQ Roll Calendar ---")
    for c in get_mnq_contracts(2024, 2025):
        print(f"{c['code']}: {c['active_start'].date()} to {c['active_end'].date()}")
        
    print("\n--- MGC Roll Calendar ---")
    for c in get_mgc_contracts(2024, 2025):
        print(f"{c['code']}: {c['active_start'].date()} to {c['active_end'].date()}")
