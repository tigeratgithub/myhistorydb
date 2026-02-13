from ibhistorydb.calendar import get_mnq_contracts

print("=== MNQ 期货日历 (覆盖 2024 年) ===\n")
print(f"{'合约代码':<12} {'到期月':<10} {'主力开始':<15} {'主力结束':<15}")
print("-" * 55)

contracts = get_mnq_contracts(2023, 2026)
for c in contracts:
    # 显示所有与 2024 年有交集的合约
    if c['active_end'].year >= 2024 and c['active_start'].year <= 2024:
        print(f"{c['code']:<12} {c['expiry_month']:<10} {str(c['active_start'].date()):<15} {str(c['active_end'].date()):<15}")
