# DECISIONS.md

1. **Indexes.** On attendance_logs: a unique (emp_code, date), so a person gets one record per day and per-employee lookups are fast; (date desc, emp_code) for the list sort and month scans; (status, date, emp_code) for the status filter; (emp_code, punch_in desc) so punch-out finds the latest record. On employees: unique emp_code, plus (department, emp_code) and (department, joined_on). I skipped an index on late_minutes: the date range already narrows the data, and every extra index slows writes.

2. **Punch-in race.** Both requests call insert_one straight away. The unique index lets the first one in and the second gets a duplicate key error, which I turn into a 409. I don't check first and then insert, because both requests could pass that check.

3. **Ties.** The leaderboard uses $rank, so tied people share a rank and the next rank is skipped (1, 2, 2, 4). The limit applies to the rank, so both tied people come back, even if that is more rows than the limit.

4. **Headcount.** The summary starts from the employees collection and looks up their logs, so someone with no logs still gets counted if they joined by the end of the month.

5. **100x data.** I'd keep a small monthly summary per employee, updated on every punch or correction, so analytics don't scan raw logs.
