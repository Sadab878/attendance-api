# Employee Attendance & Analytics API

FastAPI + MongoDB. All code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or a local .env
uvicorn app.main:app --port 8000
```
Real environment variables win over `.env`. Indexes are created at startup and are idempotent.

## Notes
- Needs MongoDB 6.0+ (`$densify`, `$setWindowFields`, `$rank`, `$lookup` with `localField` + `pipeline`).
- `moving_avg_7d` is the mean of the *reported* (already 4-decimal-rounded) daily rates.
- Rounding is half-up everywhere; sums of `work_hours` use Decimal128 so the average is exact.
- Instants are stored as UTC datetimes and converted to epoch milliseconds at the API boundary.
