# DECISIONS.md

1. **Indexes.** Unique `(emp_code, date)` on logs: it is the one-record-per-day guarantee, and it serves per-employee queries. `(date -1, emp_code 1)` serves the attendance list sort and the month scans. `(status, date -1, emp_code 1)` serves status filters. `(emp_code, punch_in -1)` finds the record to punch out. On employees: unique `emp_code`, plus `(department, emp_code)` and `(department, joined_on)`. I rejected a separate index on `late_minutes`: the month filter already narrows the scan, and it costs writes.
2. **Punch-in race.** Both requests try `insert_one`. The unique index lets one succeed; the other gets `DuplicateKeyError` and returns 409. No read-then-write.
3. **Ties.** Ranking is done by `$rank` (1, 2, 2, 4) and `limit` filters on rank, so both tied people are returned, even if that exceeds `limit`.
4. **Headcount.** The department summary starts from `employees` and `$lookup`s logs, so an employee with no logs still produces a row and is counted.
5. **100x data.** Pre-aggregate per employee per month (a summary collection updated on punch/patch), so analytics read small documents instead of scanning raw logs.
