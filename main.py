# main.py
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY", "CHANGE_ME")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")  # Render Disk path
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

app = FastAPI(title="Registro Gastos API")

# Serve static dashboard if present
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ======================================================
# DB HELPERS + MIGRATIONS
# ======================================================
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, col: str, col_type: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}  # row[1] = column name
    if col not in existing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        conn.commit()


def ensure_index(conn: sqlite3.Connection, sql: str) -> None:
    try:
        conn.execute(sql)
        conn.commit()
    except Exception:
        # Don't fail boot if index creation fails
        pass


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Base table (keep it permissive; migrations will extend)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            merchant_clean TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # --- Migrations / new columns (safe to run every boot) ---
    ensure_column(conn, "expenses", "merchant_raw", "TEXT")
    ensure_column(conn, "expenses", "date_raw", "TEXT")
    ensure_column(conn, "expenses", "date_iso", "TEXT")
    ensure_column(conn, "expenses", "sms_hash", "TEXT")

    # Unique index to dedupe by sms_hash (optional but recommended)
    ensure_index(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")

    # Useful indexes for dashboard queries
    ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_expenses_created_at ON expenses(created_at)")
    ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
    ensure_index(conn, "CREATE INDEX IF NOT EXISTS idx_expenses_user_id ON expenses(user_id)")

    conn.close()


@app.on_event("startup")
def _startup():
    init_db()


# ======================================================
# MODELS
# ======================================================
class IngestRequest(BaseModel):
    # Compatible with your Shortcuts payload
    user_id: str = Field(..., alias="userid")
    sms: str

    class Config:
        populate_by_name = True


# ======================================================
# AUTH
# ======================================================
def require_api_key(api_key: str) -> None:
    if API_KEY != "CHANGE_ME" and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid api_key")


# ======================================================
# SMS PARSING (simple + robust)
# You can refine regex later; this is stable enough for now.
# ======================================================
_AMOUNT_RE = re.compile(r"\b(KWD|KD|USD|EUR)\s*([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
_DATE_RE = re.compile(
    r"\b(20\d{2})[-/](\d{2})[-/](\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?\b"
)

def clean_merchant(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    # remove trailing commas/spaces
    s = re.sub(r"[,\s]+$", "", s)
    return s.lower()


def categorize(merchant_clean: str, sms: str) -> str:
    txt = (merchant_clean + " " + sms).lower()
    if any(k in txt for k in ["starbucks", "cafe", "coffee", "espresso", "latte"]):
        return "coffee"
    if any(k in txt for k in ["talabat", "deliveroo", "pickup", "pick daily", "restaurant", "burger", "pizza"]):
        return "food"
    if any(k in txt for k in ["uber", "careem", "taxi"]):
        return "transport"
    if any(k in txt for k in ["amazon", "noon", "shein"]):
        return "shopping"
    if any(k in txt for k in ["gym", "fitness"]):
        return "fitness"
    return "other"


def parse_sms(sms: str) -> Dict[str, Any]:
    # Amount & currency
    currency = "KWD"
    amount = 0.0
    m = _AMOUNT_RE.search(sms)
    if m:
        currency = m.group(1).upper().replace("KD", "KWD")
        try:
            amount = float(m.group(2))
        except Exception:
            amount = 0.0

    # Date parsing
    date_raw = None
    date_iso = None
    d = _DATE_RE.search(sms)
    if d:
        yyyy, mm, dd = d.group(1), d.group(2), d.group(3)
        hh = d.group(4) or "00"
        mi = d.group(5) or "00"
        ss = d.group(6) or "00"
        date_raw = d.group(0)
        try:
            dt = datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), int(ss), tzinfo=timezone.utc)
            date_iso = dt.isoformat()
        except Exception:
            date_iso = None

    # Merchant heuristic:
    # Try to extract "for X on" or "from X" patterns common in bank SMS
    merchant_raw = ""
    patterns = [
        r"\bfor\s+(.+?)\s+on\s+20\d{2}[-/]\d{2}[-/]\d{2}",
        r"\bfrom\s+(.+?)(?:,|\s{2,}|$)",
        r"\bat\s+(.+?)(?:,|\s{2,}|$)",
    ]
    for p in patterns:
        mm = re.search(p, sms, flags=re.IGNORECASE)
        if mm:
            merchant_raw = mm.group(1).strip()
            break

    if not merchant_raw:
        # fallback: use a short snippet
        merchant_raw = sms[:60].strip()

    merchant_clean = clean_merchant(merchant_raw)
    category = categorize(merchant_clean, sms)

    # Hash for dedupe
    sms_hash = hashlib.sha256(sms.strip().encode("utf-8")).hexdigest()

    return {
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": category,
        "date_raw": date_raw,
        "date_iso": date_iso,
        "sms_hash": sms_hash,
    }


# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def root():
    # Keep "/" simple; your logs show HEAD / 404 which is fine, but let's be friendly.
    return {"ok": True, "service": "sms-parser-api", "docs": "/docs", "app": "/app"}


@app.get("/app")
def app_page(api_key: str = Query(...)):
    require_api_key(api_key)

    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)

    # If you don't have static/index.html yet:
    return JSONResponse(
        {
            "message": "Dashboard not found. Create static/index.html and it will be served here.",
            "hint": "Folder structure: /static/index.html",
        }
    )


@app.post("/ingest")
async def ingest(payload: IngestRequest, api_key: str = Query(...)):
    require_api_key(api_key)

    user_id = payload.user_id.strip()
    sms = payload.sms.strip()
    if not user_id or not sms:
        raise HTTPException(status_code=400, detail="Missing userid or sms")

    parsed = parse_sms(sms)
    created_at = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    cur = conn.cursor()

    # Ensure schema is up to date (extra safety in case startup didn't run)
    ensure_column(conn, "expenses", "merchant_raw", "TEXT")
    ensure_column(conn, "expenses", "date_raw", "TEXT")
    ensure_column(conn, "expenses", "date_iso", "TEXT")
    ensure_column(conn, "expenses", "sms_hash", "TEXT")
    ensure_index(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")

    try:
        cur.execute(
            """
            INSERT INTO expenses (user_id, amount, currency, merchant_raw, merchant_clean, category, sms_hash, date_raw, date_iso, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                parsed["amount"],
                parsed["currency"],
                parsed["merchant_raw"],
                parsed["merchant_clean"],
                parsed["category"],
                parsed["sms_hash"],
                parsed["date_raw"],
                parsed["date_iso"],
                created_at,
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # likely duplicate sms_hash
        conn.close()
        return JSONResponse({"ok": True, "deduped": True, "sms_hash": parsed["sms_hash"]})
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    expense_id = cur.lastrowid
    conn.close()

    return {
        "ok": True,
        "id": expense_id,
        "user_id": user_id,
        **parsed,
        "created_at": created_at,
    }


@app.get("/expenses")
def list_expenses(
    api_key: str = Query(...),
    limit: int = Query(200, ge=1, le=2000),
    sort: str = Query("date_desc"),  # date_desc, date_asc, amount_desc, amount_asc
    category: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
):
    require_api_key(api_key)

    sort_map = {
        "date_desc": "COALESCE(date_iso, created_at) DESC",
        "date_asc": "COALESCE(date_iso, created_at) ASC",
        "amount_desc": "amount DESC",
        "amount_asc": "amount ASC",
    }
    order_by = sort_map.get(sort, sort_map["date_desc"])

    where = []
    params: List[Any] = []

    if category and category.lower() != "all":
        where.append("category = ?")
        params.append(category)
    if user_id:
        where.append("user_id = ?")
        params.append(user_id)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, user_id, amount, currency, merchant_raw, merchant_clean, category, sms_hash, date_raw, date_iso, created_at
        FROM expenses
        {where_sql}
        ORDER BY {order_by}
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    return {"count": len(rows), "expenses": rows}


@app.get("/api/insights")
def insights(api_key: str = Query(...), user_id: Optional[str] = Query(None)):
    require_api_key(api_key)

    conn = get_conn()
    cur = conn.cursor()

    where_sql = ""
    params: List[Any] = []
    if user_id:
        where_sql = "WHERE user_id = ?"
        params.append(user_id)

    # totals
    cur.execute(
        f"""
        SELECT
            COUNT(*) as n,
            COALESCE(SUM(amount), 0) as total
        FROM expenses
        {where_sql}
        """,
        params,
    )
    base = dict(cur.fetchone() or {})

    # by category
    cur.execute(
        f"""
        SELECT category, COUNT(*) as n, COALESCE(SUM(amount), 0) as total
        FROM expenses
        {where_sql}
        GROUP BY category
        ORDER BY total DESC
        """,
        params,
    )
    by_category = [dict(r) for r in cur.fetchall()]

    # distinct categories list (for dropdown)
    cur.execute(
        f"""
        SELECT DISTINCT category
        FROM expenses
        {where_sql}
        ORDER BY category ASC
        """,
        params,
    )
    categories = [r["category"] for r in cur.fetchall()]

    conn.close()

    return {
        "totals": base,
        "by_category": by_category,
        "categories": categories,
    }
