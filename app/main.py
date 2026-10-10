"""Employee Attendance & Analytics API (FastAPI + MongoDB).

Everything lives in this one file. Sections:
  1. configuration and database handles
  2. time helpers and business rules (R1 - R5)
  3. serialisation helpers
  4. request models
  5. endpoints: system, employees, attendance
  6. analytics pipelines and endpoints
  7. admin explain endpoint
"""
import calendar
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from datetime import time as Time
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, StrictInt, field_validator, model_validator
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. configuration
# --------------------------------------------------------------------------- #
load_dotenv()  # never overrides variables that are already in the environment
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True: every datetime PyMongo hands back is UTC-aware, never naive.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]

PRESENCE = ["PRESENT", "WFH", "ON_DUTY"]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
HM_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"

_indexes_ready = False


def ensure_indexes() -> None:
    """Idempotent: fixed names, fixed keys."""
    global _indexes_ready
    employees.create_index([("emp_code", 1)], unique=True, name="uniq_emp_code")
    employees.create_index([("department", 1), ("emp_code", 1)], name="dept_emp_code")
    employees.create_index([("department", 1), ("joined_on", 1)], name="dept_joined_on")
    employees.create_index([("joined_on", 1)], name="joined_on")
    logs.create_index([("emp_code", 1), ("date", 1)], unique=True, name="uniq_emp_date")
    logs.create_index([("date", -1), ("emp_code", 1)], name="date_emp")
    logs.create_index([("status", 1), ("date", -1), ("emp_code", 1)], name="status_date_emp")
    logs.create_index([("emp_code", 1), ("punch_in", -1)], name="emp_punch_in")
    _indexes_ready = True


@asynccontextmanager
async def lifespan(_: FastAPI):
    deadline = time.monotonic() + 12
    while True:
        try:
            ensure_indexes()
            break
        except PyMongoError as exc:  # Mongo not reachable yet: /health will report 503
            log.warning("index creation failed: %s", exc)
            if time.monotonic() > deadline:
                break
            time.sleep(0.5)
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)

# --------------------------------------------------------------------------- #
# 2. time helpers and business rules
# --------------------------------------------------------------------------- #
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

Instant = Annotated[StrictInt, Field(ge=100_000_000_000, le=4_102_444_800_000)]


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> UTC datetime truncated to whole seconds (R1)."""
    return EPOCH + timedelta(seconds=ms // 1000)


def aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def dt_to_ms(dt: Optional[datetime]) -> Optional[int]:
    if dt is None:
        return None
    return (aware(dt) - EPOCH) // timedelta(milliseconds=1)


def now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def hm(value: str) -> int:
    h, m = value.split(":")
    return int(h) * 60 + int(m)


def is_overnight(emp: dict) -> bool:
    return hm(emp["shift_end"]) <= hm(emp["shift_start"])


def attendance_date(emp: dict, punch_in: datetime) -> Date:
    """R1: IST calendar date of the punch-in, shifted back a day for the early
    part of an overnight shift."""
    ist = punch_in.astimezone(IST)
    day = ist.date()
    seconds_into_day = ist.hour * 3600 + ist.minute * 60 + ist.second
    if is_overnight(emp) and seconds_into_day < hm(emp["shift_end"]) * 60:
        day -= timedelta(days=1)
    return day


def shift_bounds(emp: dict, day: Date) -> tuple[datetime, datetime]:
    sh, sm = divmod(hm(emp["shift_start"]), 60)
    eh, em = divmod(hm(emp["shift_end"]), 60)
    start = datetime.combine(day, Time(sh, sm), tzinfo=IST)
    end_day = day + timedelta(days=1) if is_overnight(emp) else day
    end = datetime.combine(end_day, Time(eh, em), tzinfo=IST)
    return start, end


def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def derive(emp: dict, day: Date, status: str, punch_in: Optional[datetime],
           punch_out: Optional[datetime]) -> dict:
    """R2 - R5. Returns the four derived fields."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    start, end = shift_bounds(emp, day)
    late_seconds = int((punch_in - start).total_seconds())
    if late_seconds > 600:                      # strictly more than 10:00
        out["late_minutes"] = late_seconds // 60  # measured from shift_start
    if punch_out is not None:
        over_seconds = int((punch_out - end).total_seconds())
        if over_seconds >= 30 * 60:
            out["overtime_minutes"] = over_seconds // 60
        worked = int((punch_out - punch_in).total_seconds())
        hours = round_half_up(Decimal(worked) / Decimal(3600), 2)
        out["work_hours"] = float(hours)
        out["half_day"] = hours < Decimal("4.50")
    return out


def count_weekdays(first: Date, last: Date) -> int:
    n, d = 0, first
    while d <= last:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


# --------------------------------------------------------------------------- #
# 3. validation / serialisation helpers
# --------------------------------------------------------------------------- #
def bad(loc: str, msg: str):
    """Raise a 422 with FastAPI's default body shape."""
    raise RequestValidationError([{"type": "value_error", "loc": ["query", loc], "msg": msg, "input": None}])


def parse_date(value: str, loc: str) -> Date:
    if not re.fullmatch(DATE_RE, value):
        bad(loc, "date must be YYYY-MM-DD")
    try:
        return Date.fromisoformat(value)
    except ValueError:
        bad(loc, "not a real calendar date")


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    if year < 1:
        bad("month", "year out of range")
    last = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{last:02d}"


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"], "name": doc["name"], "email": doc["email"],
        "department": doc["department"], "shift_start": doc["shift_start"],
        "shift_end": doc["shift_end"], "joined_on": doc["joined_on"],
        "created_at": dt_to_ms(doc["created_at"]),
    }


def record_out(doc: dict) -> dict:
    history = []
    for entry in doc.get("history") or []:
        changes = {}
        for key, val in (entry.get("changes") or {}).items():
            if key in ("punch_in", "punch_out"):
                changes[key] = {"from": dt_to_ms(val.get("from")), "to": dt_to_ms(val.get("to"))}
            else:
                changes[key] = val
        history.append({"at": dt_to_ms(entry.get("at")), "by": entry.get("by"),
                        "reason": entry.get("reason"), "changes": changes})
    return {
        "emp_code": doc["emp_code"], "date": doc["date"], "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")), "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": history,
    }


def dec(value: Any) -> Decimal:
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return Decimal(str(value or 0))


# --------------------------------------------------------------------------- #
# 4. request models
# --------------------------------------------------------------------------- #
class EmployeeCreate(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=HM_RE)
    shift_end: str = Field(default="18:30", pattern=HM_RE)
    joined_on: str = Field(pattern=DATE_RE)

    @field_validator("joined_on")
    @classmethod
    def _real_date(cls, v: str) -> str:
        Date.fromisoformat(v)  # ValueError becomes a 422
        if int(v[:4]) < 1:
            raise ValueError("year out of range")
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[Instant] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[Instant] = None


class RegularizeRequest(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[Instant] = None
    punch_out: Optional[Instant] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)


# --------------------------------------------------------------------------- #
# 5. endpoints: system, employees, attendance
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
        if not _indexes_ready:
            ensure_indexes()
    except PyMongoError:
        raise HTTPException(status_code=503, detail="MongoDB is not reachable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    created = datetime.now(UTC)
    doc = {**body.model_dump(), "created_at": created.replace(microsecond=created.microsecond // 1000 * 1000)}
    try:
        employees.insert_one(doc)  # the unique index on emp_code decides, not a prior lookup
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None,
                   page: int = Query(1, ge=1),
                   page_size: int = Query(20, ge=1, le=100)):
    flt = {} if department is None else {"department": department}
    skip = min((page - 1) * page_size, 2**62)
    rows = employees.find(flt).sort("emp_code", 1).skip(skip).limit(page_size)
    return {"items": [employee_out(r) for r in rows], "total": employees.count_documents(flt),
            "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="unknown employee")
    pin = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    day = attendance_date(emp, pin)
    derived = derive(emp, day, body.status, pin, None)
    doc = {"emp_code": body.emp_code, "date": day.isoformat(), "status": body.status,
           "punch_in": pin, "punch_out": None, **derived, "history": []}
    try:
        logs.insert_one(doc)  # unique (emp_code, date) index: one winner, the rest get 409
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="unknown employee")
    pout = ms_to_dt(body.punched_at) if body.punched_at is not None else now_dt()
    rec = logs.find_one({"emp_code": body.emp_code, "punch_in": {"$lte": pout}}, sort=[("punch_in", -1)])
    if rec is None:
        raise HTTPException(status_code=404, detail="no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="already punched out")
    pin = aware(rec["punch_in"])
    if pout <= pin or pout > pin + timedelta(hours=24):
        raise RequestValidationError([{"type": "value_error", "loc": ["body", "punched_at"],
                                       "msg": "punched_at must be after punch_in and within 24 hours",
                                       "input": None}])
    derived = derive(emp, Date.fromisoformat(rec["date"]), rec["status"], pin, pout)
    # the filter on punch_out: None makes two simultaneous punch-outs race on one document
    res = logs.update_one({"_id": rec["_id"], "punch_out": None}, {"$set": {"punch_out": pout, **derived}})
    if res.matched_count == 0:
        raise HTTPException(status_code=409, detail="already punched out")
    rec.update({"punch_out": pout, **derived})
    return record_out(rec)


def attendance_query(emp_code, date_from, date_to, status, page, page_size):
    flt: dict[str, Any] = {}
    if emp_code is not None:
        flt["emp_code"] = emp_code
    rng = {}
    if date_from is not None:
        rng["$gte"] = parse_date(date_from, "date_from").isoformat()
    if date_to is not None:
        rng["$lte"] = parse_date(date_to, "date_to").isoformat()
    if date_from is not None and date_to is not None and rng["$gte"] > rng["$lte"]:
        bad("date_from", "date_from must not be after date_to")
    if rng:
        flt["date"] = rng
    if status is not None:
        flt["status"] = status
    return flt, {"date": -1, "emp_code": 1}, min((page - 1) * page_size, 2**62), page_size


@app.get("/attendance")
def list_attendance(emp_code: Optional[str] = None, date_from: Optional[str] = None,
                    date_to: Optional[str] = None, status: Optional[Status] = None,
                    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
    flt, sort, skip, limit = attendance_query(emp_code, date_from, date_to, status, page, page_size)
    rows = logs.find(flt).sort(list(sort.items())).skip(skip).limit(limit)
    return {"items": [record_out(r) for r in rows], "total": logs.count_documents(flt),
            "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, date: str, body: RegularizeRequest):
    day = parse_date(date, "date")
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="unknown employee")
    rec = logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if rec is None:
        raise HTTPException(status_code=404, detail="no record for that date")

    def invalid(msg: str):
        raise RequestValidationError([{"type": "value_error", "loc": ["body"], "msg": msg, "input": None}])

    status = body.status or rec["status"]
    old_in, old_out = aware(rec.get("punch_in")), aware(rec.get("punch_out"))
    if status in ("ABSENT", "LEAVE"):
        if body.punch_in is not None or body.punch_out is not None:
            invalid("punch times cannot be supplied with status ABSENT or LEAVE")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else old_in
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else old_out
        if new_in is None:
            invalid("a presence status requires a punch_in")
        if attendance_date(emp, new_in) != day:
            invalid("punch_in must stay on the record's attendance date")
        if new_out is not None and (new_out <= new_in or new_out > new_in + timedelta(hours=24)):
            invalid("punch_out must be after punch_in and within 24 hours")

    derived = derive(emp, day, status, new_in, new_out)
    new_vals = {"status": status, "punch_in": new_in, "punch_out": new_out, **derived}
    old_vals = {"status": rec["status"], "punch_in": old_in, "punch_out": old_out,
                "work_hours": rec.get("work_hours"), "late_minutes": rec.get("late_minutes") or 0,
                "overtime_minutes": rec.get("overtime_minutes") or 0, "half_day": bool(rec.get("half_day"))}
    changes = {k: {"from": old_vals[k], "to": v} for k, v in new_vals.items() if old_vals[k] != v}
    if not changes:
        invalid("the request changes nothing")

    entry = {"at": datetime.now(UTC), "by": body.regularized_by, "reason": body.reason, "changes": changes}
    n = len(rec.get("history") or [])
    # Optimistic concurrency: only apply if the record is exactly as we read it
    # (same history length, same punches, same status). Otherwise someone else got there first.
    guard = {"_id": rec["_id"], f"history.{n}": {"$exists": False}, "status": old_vals["status"],
             "punch_in": rec.get("punch_in"), "punch_out": rec.get("punch_out")}
    res = logs.update_one(guard, {"$set": new_vals, "$push": {"history": entry}})
    if res.matched_count == 0:
        raise HTTPException(status_code=409, detail="record was modified concurrently, retry")
    rec.update(new_vals)
    rec["history"] = list(rec.get("history") or []) + [entry]
    return record_out(rec)


# --------------------------------------------------------------------------- #
# 6. analytics
# --------------------------------------------------------------------------- #
def present_weight(status: str, half_day: str, date_ref: Optional[str]) -> dict:
    """Aggregation expression: 2 = full present day, 1 = half day, 0 = not present
    (doubled so that sums stay exact integers). date_ref set => Mon-Fri only (R7)."""
    cond: list = [{"$in": [status, PRESENCE]}]
    if date_ref is not None:
        cond.append({"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": date_ref, "format": "%Y-%m-%d"}}}, 5]})
    return {"$cond": [{"$and": cond}, {"$cond": [{"$eq": [{"$ifNull": [half_day, False]}, True]}, 1, 2]}, 0]}


def monthly_pipeline(emp_code: str, start: str, end: str) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        {"$group": {
            "_id": None,
            "present2": {"$sum": present_weight("$status", "$half_day", "$date")},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}},
            "late_total": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
            "ot_total": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(pattern=MONTH_RE)):
    start, end = month_bounds(month)
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(status_code=404, detail="unknown employee")
    first = max(Date.fromisoformat(start), Date.fromisoformat(emp["joined_on"]))
    last = Date.fromisoformat(end)
    working_days = count_weekdays(first, last) if first <= last else 0
    agg = next(iter(logs.aggregate(monthly_pipeline(emp_code, start, end))), None) or {}
    present2 = int(agg.get("present2", 0))
    pct = None
    if working_days > 0:
        pct = float(round_half_up(Decimal(present2) * 50 / Decimal(working_days), 2))
    return {"emp_code": emp_code, "month": month, "working_days": working_days,
            "present_days": present2 / 2, "leave_days": int(agg.get("leave_days", 0)),
            "late_count": int(agg.get("late_count", 0)), "total_late_minutes": int(agg.get("late_total", 0)),
            "total_overtime_minutes": int(agg.get("ot_total", 0)), "attendance_pct": pct}


def department_pipeline(start: str, end: str, department: Optional[str]) -> list:
    """Starts from `employees`, so people with zero logs still count (R9)."""
    emp_match: dict[str, Any] = {"joined_on": {"$lte": end}}
    if department is not None:
        emp_match["department"] = department
    presence_with_hours = {"$filter": {"input": "$logs", "as": "l", "cond": {"$and": [
        {"$in": ["$$l.status", PRESENCE]}, {"$isNumber": "$$l.work_hours"}]}}}

    def count_where(cond: dict) -> dict:
        return {"$size": {"$filter": {"input": "$logs", "as": "l", "cond": cond}}}

    return [
        {"$match": emp_match},
        {"$lookup": {"from": "attendance_logs", "localField": "emp_code", "foreignField": "emp_code",
                     "pipeline": [{"$match": {"date": {"$gte": start, "$lte": end}}},
                                  {"$project": {"_id": 0, "date": 1, "status": 1, "half_day": 1,
                                                "late_minutes": 1, "work_hours": 1}}],
                     "as": "logs"}},
        {"$project": {
            "department": 1,
            "present2": {"$sum": {"$map": {"input": "$logs", "as": "l",
                                           "in": present_weight("$$l.status", "$$l.half_day", "$$l.date")}}},
            "late_count": count_where({"$gt": [{"$ifNull": ["$$l.late_minutes", 0]}, 0]}),
            "late_total": {"$sum": "$logs.late_minutes"},
            "leave_count": count_where({"$eq": ["$$l.status", "LEAVE"]}),
            "on_duty_count": count_where({"$eq": ["$$l.status", "ON_DUTY"]}),
            "wh_cnt": {"$size": presence_with_hours},
            # Decimal128 keeps the sum exact, so the half-up rounding below sees the true mean
            "wh_sum": {"$sum": {"$map": {"input": presence_with_hours, "as": "l",
                                         "in": {"$toDecimal": "$$l.work_hours"}}}},
        }},
        {"$group": {"_id": "$department", "headcount": {"$sum": 1}, "present2": {"$sum": "$present2"},
                    "late_count": {"$sum": "$late_count"}, "late_total": {"$sum": "$late_total"},
                    "leave_count": {"$sum": "$leave_count"}, "on_duty_count": {"$sum": "$on_duty_count"},
                    "wh_cnt": {"$sum": "$wh_cnt"}, "wh_sum": {"$sum": "$wh_sum"}}},
        {"$sort": {"_id": 1}},
    ]


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(pattern=MONTH_RE), department: Optional[str] = None):
    start, end = month_bounds(month)
    items = []
    for r in employees.aggregate(department_pipeline(start, end, department)):
        cnt = int(r["wh_cnt"])
        avg = float(round_half_up(dec(r["wh_sum"]) / cnt, 2)) if cnt else None
        items.append({"department": r["_id"], "headcount": int(r["headcount"]),
                      "present_days": int(r["present2"]) / 2, "avg_work_hours": avg,
                      "late_count": int(r["late_count"]), "total_late_minutes": int(dec(r["late_total"])),
                      "leave_count": int(r["leave_count"]), "on_duty_count": int(r["on_duty_count"])})
    return {"month": month, "items": items}


def leaderboard_pipeline(start: str, end: str, limit: int, department: Optional[str]) -> list:
    pipe: list = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees disappear here
    ]
    if department is not None:
        pipe.append({"$match": {"emp.department": department}})
    pipe += [
        # $rank is standard competition ranking: 1, 2, 2, 4 ...
        {"$setWindowFields": {"sortBy": {"total": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies to the rank, not the row count
        {"$sort": {"total": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": "$total", "late_count": 1}},
    ]
    return pipe


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: str = Query(pattern=MONTH_RE), limit: int = Query(10, ge=1, le=50),
                     department: Optional[str] = None):
    start, end = month_bounds(month)
    items = [{"rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"],
              "department": r["department"], "total_late_minutes": int(r["total_late_minutes"]),
              "late_count": int(r["late_count"])}
             for r in logs.aggregate(leaderboard_pipeline(start, end, limit, department))]
    return {"month": month, "items": items}


def trend_pipeline(department: str, frm: Date, to: Date) -> list:
    people = list(employees.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not people:
        raise HTTPException(status_code=404, detail="unknown department")
    codes = [p["emp_code"] for p in people]
    joined: dict[str, int] = {}
    for p in people:
        joined[p["joined_on"]] = joined.get(p["joined_on"], 0) + 1
    joined_list = [{"d": d, "n": n} for d, n in joined.items()]
    window = {"documents": [-6, 0]}
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": frm.isoformat(), "$lte": to.isoformat()}}},
        {"$group": {"_id": "$date",
                    "p2": {"$sum": present_weight("$status", "$half_day", None)},
                    "late": {"$sum": {"$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0]}}}},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # one row per calendar day; bounds are [lower, upper) so the upper bound is `to` + 1 day
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [
            datetime.combine(frm, Time(0), tzinfo=UTC),
            datetime.combine(to + timedelta(days=1), Time(0), tzinfo=UTC)]}}},
        {"$addFields": {"date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
                        "p2": {"$ifNull": ["$p2", 0]}, "late": {"$ifNull": ["$late", 0]}}},
        {"$sort": {"d": 1}},
        {"$addFields": {
            "wd": {"$lte": [{"$isoDayOfWeek": "$d"}, 5]},
            "hc": {"$reduce": {"input": {"$literal": joined_list}, "initialValue": 0,
                               "in": {"$cond": [{"$lte": ["$$this.d", "$date"]},
                                                {"$add": ["$$value", "$$this.n"]}, "$$value"]}}},
        }},
        # rate x 10000 rounded half-up using integer arithmetic: floor((10000*p2 + hc) / (2*hc))
        {"$addFields": {"rs": {"$cond": [
            {"$and": ["$wd", {"$gt": ["$hc", 0]}]},
            {"$trunc": [{"$divide": [{"$add": [{"$multiply": [10000, "$p2"]}, "$hc"]},
                                     {"$multiply": [2, "$hc"]}]}, 0]},
            None]}}},
        {"$setWindowFields": {"sortBy": {"d": 1}, "output": {
            "S": {"$sum": "$rs", "window": window},
            "n": {"$sum": {"$cond": [{"$eq": [{"$type": "$rs"}, "null"]}, 0, 1]}, "window": window}}}},
        {"$project": {"_id": 0, "date": 1, "wd": 1, "hc": 1, "p2": 1, "late": 1, "rs": 1, "S": 1, "n": 1}},
    ]


def trend_rows_when_no_logs(department_pipeline_input: list, frm: Date, to: Date) -> list:
    """Safety net only: the pipeline already yields every day via $densify. If a server
    ever returns nothing for an empty input, build the all-zero rows here."""
    stage = next(st for st in department_pipeline_input if "$addFields" in st and "hc" in st["$addFields"])
    joined = stage["$addFields"]["hc"]["$reduce"]["input"]["$literal"]
    rows, rates, d = [], [], frm
    while d <= to:
        hc = sum(j["n"] for j in joined if j["d"] <= d.isoformat())
        wd = d.weekday() < 5
        rs = 0 if (wd and hc > 0) else None
        rates.append(rs)
        recent = [x for x in rates[-7:] if x is not None]
        rows.append({"date": d.isoformat(), "wd": wd, "hc": hc, "p2": 0, "late": 0, "rs": rs,
                     "S": sum(recent), "n": len(recent)})
        d += timedelta(days=1)
    return rows


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: str = Query(alias="from"), to: str = Query()):
    frm, end = parse_date(from_, "from"), parse_date(to, "to")
    if end < frm:
        bad("to", "to must not be before from")
    if (end - frm).days + 1 > 92:
        bad("to", "range must not exceed 92 days")
    pipeline = trend_pipeline(department, frm, end)
    rows = list(logs.aggregate(pipeline)) or trend_rows_when_no_logs(pipeline, frm, end)
    items = []
    for r in rows:
        rs, n, s = r["rs"], int(r["n"]), int(r["S"])
        items.append({
            "date": r["date"], "is_working_day": bool(r["wd"]), "headcount": int(r["hc"]),
            "present_count": int(r["p2"]) / 2, "late_count": int(r["late"]),
            "attendance_rate": None if rs is None else int(rs) / 10000,
            # mean of the reported (4-decimal) rates, half-up: floor((2S + n) / 2n) / 10000
            "moving_avg_7d": None if n == 0 else ((2 * s + n) // (2 * n)) / 10000,
        })
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 7. admin: explain
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary",
                        "late_leaderboard", "department_trend"]


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(endpoint: ExplainTarget, emp_code: Optional[str] = None,
                     month: Optional[str] = Query(None, pattern=MONTH_RE), department: Optional[str] = None,
                     limit: int = Query(10, ge=1, le=50), date_from: Optional[str] = None,
                     date_to: Optional[str] = None, status: Optional[Status] = None,
                     from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                     page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)):
    def need(name: str, value):
        if value is None:
            raise RequestValidationError([{"type": "missing", "loc": ["query", name],
                                           "msg": "Field required", "input": None}])
        return value

    # Each branch builds the query with the very same helper the real endpoint uses.
    if endpoint == "attendance_list":
        flt, sort, skip, lim = attendance_query(emp_code, date_from, date_to, status, page, page_size)
        coll, command = "attendance_logs", {"find": "attendance_logs", "filter": flt, "sort": sort,
                                            "skip": skip, "limit": lim}
    elif endpoint == "employee_monthly":
        start, end = month_bounds(need("month", month))
        coll, command = "attendance_logs", {"aggregate": "attendance_logs", "cursor": {},
                                            "pipeline": monthly_pipeline(need("emp_code", emp_code), start, end)}
    elif endpoint == "department_summary":
        start, end = month_bounds(need("month", month))
        coll, command = "employees", {"aggregate": "employees", "cursor": {},
                                      "pipeline": department_pipeline(start, end, department)}
    elif endpoint == "late_leaderboard":
        start, end = month_bounds(need("month", month))
        coll, command = "attendance_logs", {"aggregate": "attendance_logs", "cursor": {},
                                            "pipeline": leaderboard_pipeline(start, end, limit, department)}
    else:
        dept = need("department", department)
        frm, end = parse_date(need("from", from_), "from"), parse_date(need("to", to), "to")
        if end < frm:
            bad("to", "to must not be before from")
        if (end - frm).days + 1 > 92:
            bad("to", "range must not exceed 92 days")
        coll, command = "attendance_logs", {"aggregate": "attendance_logs", "cursor": {},
                                            "pipeline": trend_pipeline(dept, frm, end)}
    try:
        result = db.command({"explain": command, "verbosity": "executionStats"})
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail=f"explain failed: {exc}")
    return JSONResponse({"endpoint": endpoint, "collection": coll,
                         "explain": json.loads(json_util.dumps(result))})
