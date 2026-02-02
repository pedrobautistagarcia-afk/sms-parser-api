import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from threading import Lock
from typing import Optional, List, Any, Dict

from fastapi import FastAPI, Request, Query, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY", "CHANGE_ME")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")
APP_TITLE = os.getenv("APP_TITLE", "Registro Gastos API")

db_lock = Lock()
app = FastAPI(title=APP_TITLE)

# Static dashboard
app.mount("/static", StaticFiles(directory="static"), name="static")


# ======================================================
# DB
# ======================================================
def ensure_db_dir():
    folder = os.path.dirname(DB_PATH)
    if folder and folder != ".":
        os.makedirs(folder, exist_ok=True)

def get_conn() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def table_columns(cur: sqlite3.Cursor, table: str) -> List[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]  # name is index 1

def init_db():
    with db_lock:
        conn = get_conn()
        cur = conn.cursor()

        # 1) Create table if not exists (fresh installs)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                merchant_raw TEXT,
                merchant_clean TEXT,
                category TEXT,
                created_at TEXT NOT NULL,
                hash TEXT NOT NULL UNIQUE
            )
            """
        )

        # 2) MIGRATIONS for existing DBs (add missing columns safely)
        cols = set(table_columns(cur, "expenses"))

        # Columns that newer versions expect
        expected_additions = {
            "sms_raw": "TEXT",
            # these might already exist; we keep it safe
            "merchant_raw": "TEXT",
            "merchant_clean": "TEXT",
            "category": "TEXT",
            "created_at": "TEXT",
            "hash": "TEXT",
        }

        for col, coltype in expected_additions.items():
            if col not in cols:
                cur.execute(f"ALTER TABLE expenses ADD COLUMN {col} {coltype}")

        # If hash existed as column but not unique constraint in old DB,
        # enforce uniqueness via unique index (safe even if already unique)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")

        # Helpful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")

        conn.commit()
        conn.close()

init_db()


# ======================================================
# MODELS
# ======================================================
class IngestRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    sms: str

    class Config:
        populate_by_name = True


# ======================================================
# AUTH
# ======================================================
def require_api_key(x_api_key: Optional[str], api_key_q: Optional[str]):
    supplied = x_api_key or api_key_q
    if not supplied or supplied != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ======================================================
# PARSING
# ======================================================
CURRENCY_RE = r"(KWD|EUR|USD|GBP|AED|SAR|QAR|BHD|OMR)"
AMOUNT_RE = r"(\d+(?:[.,]\d{1,3})?)"

def _to_float(s: str) -> float:
    s = s.strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def clean_merchant(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"\s+", " ", text.strip())
    return t.strip(" ,.-")

def guess_category(merchant: str, sms: str) -> str:
    blob = f"{merchant or ''} {sms or ''}".lower()
    rules = [
        ("coffee", ["cafe", "coffee", "starbucks", "caribou", "tim hortons", "espresso"]),
        ("groceries", ["market", "supermarket", "grocery", "lulu", "carrefour", "sultan", "co-op", "coop"]),
        ("food", ["restaurant", "burger", "pizza", "kebab", "shawarma", "talabat", "deliveroo"]),
        ("transport", ["uber", "careem", "taxi", "fuel", "petrol", "gas", "knpc", "parking"]),
        ("shopping", ["store", "mall", "zara", "hm", "h&m", "nike", "adidas", "amazon", "noon"]),
        ("subscriptions", ["netflix", "spotify", "apple", "google", "subscription"]),
        ("bank_fee", ["fee", "charge", "commission"]),
    ]
    for cat, kws in rules:
        if any(k in blob for k in kws):
            return cat
    return "other"

def parse_sms(sms: str) -> Dict[str, Any]:
    sms_norm = re.sub(r"\s+", " ", sms.strip())

    m1 = re.search(rf"\b{CURRENCY_RE}\s*{AMOUNT_RE}\b", sms_norm, re.IGNORECASE)
    m2 = re.search(rf"\b{AMOUNT_RE}\s*{CURRENCY_RE}\b", sms_norm, re.IGNORECASE)

    currency = "KWD"
    amount = 0.0
    if m1:
        currency = m1.group(1).upper()
        amount = _to_float(m1.group(2))
    elif m2:
        amount = _to_float(m2.group(1))
        currency = m2.group(2).upper()

    merchant = ""
    mm = re.search(r"\bfor\s+(.+?)\s+\bon\b", sms_norm, re.IGNORECASE)
    if mm:
        merchant = mm.group(1)
    else:
        mm = re.search(r"\bfrom\s+(.+?)(?:\s*,\s*|$)", sms_norm, re.IGNORECASE)
        if mm:
            merchant = mm.group(1)
        else:
            mm = re.search(r"\bat\s+(.+?)(?:\s*,\s*|$)", sms_norm, re.IGNORECASE)
            if mm:
                merchant = mm.group(1)

    merchant_raw = merchant.strip() if merchant else ""

    date_iso = None
    dm = re.search(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\b", sms_norm)
    if dm:
        try:
            dt = datetime.fromisoformat(f"{dm.group(1)}T{dm.group(2)}").replace(tzinfo=timezone.utc)
            date_iso = dt.isoformat()
        except Exception:
            date_iso = None

    created_at = date_iso or datetime.now(timezone.utc).isoformat()
    merchant_clean = clean_merchant(merchant_raw)
    category = guess_category(merchant_clean, sms_norm)

    return {
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": category,
        "created_at": created_at,
        "sms_raw": sms_norm,
    }

def compute_hash(user_id: str, payload: Dict[str, Any]) -> str:
    base = (
        f"{user_id}|{payload.get('amount')}|{payload.get('currency')}|"
        f"{payload.get('merchant_clean')}|{payload.get('created_at')}|{payload.get('sms_raw')}"
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def root():
    return FileResponse("static/index.html")

@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH}

@app.post("/ingest")
async def ingest(
    req: IngestRequest,
    request: Request,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_api_key(x_api_key, api_key)

    payload = parse_sms(req.sms)
    h = compute_hash(req.user_id, payload)

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()

        try:
            cur.execute(
                """
                INSERT INTO expenses
                (user_id, amount, currency, merchant_raw, merchant_clean, category, sms_raw, created_at, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req.user_id,
                    payload["amount"],
                    payload["currency"],
                    payload["merchant_raw"],
                    payload["merchant_clean"],
                    payload["category"],
                    payload["sms_raw"],
                    payload["created_at"],
                    h,
                ),
            )
            conn.commit()
            inserted = True
            expense_id = cur.lastrowid
        except sqlite3.IntegrityError:
            inserted = False
            expense_id = None
        finally:
            conn.close()

    return {
        "inserted": inserted,
        "id": expense_id,
        "expense": {
            "user_id": req.user_id,
            "amount": payload["amount"],
            "currency": payload["currency"],
            "merchant_clean": payload["merchant_clean"],
            "category": payload["category"],
            "created_at": payload["created_at"],
        },
    }

@app.get("/expenses")
def list_expenses(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    currency: Optional[str] = Query(default=None),
):
    q = "SELECT id, user_id, amount, currency, merchant_clean, category, created_at FROM expenses"
    where = []
    params: List[Any] = []

    if user_id:
        where.append("user_id = ?")
        params.append(user_id)

    if currency:
        where.append("currency = ?")
        params.append(currency.upper())

    if where:
        q += " WHERE " + " AND ".join(where)

    q += " ORDER BY datetime(created_at) DESC LIMIT ?"
    params.append(limit)

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        conn.close()

    expenses = []
    for r in rows:
        expenses.append(
            {
                "id": r["id"],
                "user_id": r["user_id"],
                "amount": r["amount"],
                "currency": r["currency"],
                "merchant_clean": r["merchant_clean"] or "",
                "category": r["category"] or "other",
                "created_at": r["created_at"],
            }
        )

    return {"count": len(expenses), "expenses": expenses}

@app.delete("/expenses/{expense_id}")
def delete_expense(
    expense_id: int,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_api_key(x_api_key, api_key)

    with db_lock:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        conn.commit()
        deleted = cur.rowcount
        conn.close()

    return {"deleted": deleted}
