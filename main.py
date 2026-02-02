import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from threading import Lock
from typing import Optional, List, Any, Dict

from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

API_KEY = os.getenv("API_KEY", "CHANGE_ME")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")
app.mount("/static", StaticFiles(directory="static"), name="static")

db_lock = Lock()

def ensure_db_dir():
    folder = os.path.dirname(DB_PATH)
    if folder and folder != ".":
        os.makedirs(folder, exist_ok=True)

def conn():
    ensure_db_dir()
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def require_key(x_api_key: Optional[str], api_key: Optional[str]):
    k = x_api_key or api_key
    if not k or k != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def init_db():
    # Compatible con tu DB actual (sms NOT NULL, sms_hash UNIQUE)
    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            sms TEXT NOT NULL,
            amount REAL,
            currency TEXT,
            merchant_raw TEXT,
            merchant_clean TEXT,
            category TEXT,
            hash TEXT,
            created_at TEXT,
            date_raw TEXT,
            date_iso TEXT,
            sms_hash TEXT,
            sms_raw TEXT
        )
        """)
        # Índices (si ya existen, no pasa nada)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")
        c.commit()
        c.close()

init_db()

class IngestRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    sms: str
    class Config:
        populate_by_name = True

CURRENCY_RE = r"(KWD|EUR|USD|GBP|AED|SAR|QAR|BHD|OMR)"
AMOUNT_RE = r"(\d+(?:[.,]\d{1,3})?)"

def to_float(s: str) -> float:
    return float(s.replace(",", "."))

def parse_sms(sms: str) -> Dict[str, Any]:
    sms_norm = re.sub(r"\s+", " ", sms.strip())

    # currency + amount
    currency = "KWD"
    amount = 0.0
    m1 = re.search(rf"\b{CURRENCY_RE}\s*{AMOUNT_RE}\b", sms_norm, re.IGNORECASE)
    m2 = re.search(rf"\b{AMOUNT_RE}\s*{CURRENCY_RE}\b", sms_norm, re.IGNORECASE)
    if m1:
        currency = m1.group(1).upper()
        amount = to_float(m1.group(2))
    elif m2:
        amount = to_float(m2.group(1))
        currency = m2.group(2).upper()

    # merchant
    merchant = ""
    mm = re.search(r"\bfor\s+(.+?)\s+\bon\b", sms_norm, re.IGNORECASE)
    if mm:
        merchant = mm.group(1)
    else:
        mm = re.search(r"\bfrom\s+(.+?)(?:\s*,\s*|$)", sms_norm, re.IGNORECASE)
        if mm:
            merchant = mm.group(1)

    merchant_raw = merchant.strip()
    merchant_clean = merchant_raw.strip(" ,.-")

    # date
    created_at = datetime.now(timezone.utc).isoformat()
    dm = re.search(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\b", sms_norm)
    date_raw = None
    date_iso = None
    if dm:
        date_raw = f"{dm.group(1)} {dm.group(2)}"
        try:
            dt = datetime.fromisoformat(f"{dm.group(1)}T{dm.group(2)}").replace(tzinfo=timezone.utc)
            date_iso = dt.isoformat()
            created_at = date_iso
        except Exception:
            pass

    return {
        "sms_norm": sms_norm,
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": "other",
        "created_at": created_at,
        "date_raw": date_raw,
        "date_iso": date_iso,
    }

def compute_hash(user_id: str, p: Dict[str, Any]) -> str:
    base = f"{user_id}|{p['amount']}|{p['currency']}|{p['merchant_clean']}|{p['created_at']}"
    return sha256_hex(base)

@app.get("/")
def home():
    return FileResponse("static/index.html")

@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH}

@app.post("/ingest")
def ingest(
    req: IngestRequest,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)

    p = parse_sms(req.sms)
    h = compute_hash(req.user_id, p)
    sms_hash = sha256_hex(p["sms_norm"])

    with db_lock:
        c = conn()
        cur = c.cursor()
        try:
            cur.execute(
                """
                INSERT INTO expenses
                (user_id, sms, amount, currency, merchant_raw, merchant_clean, category, hash, created_at, date_raw, date_iso, sms_hash, sms_raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    req.user_id,
                    p["sms_norm"],         # sms NOT NULL
                    p["amount"],
                    p["currency"],
                    p["merchant_raw"],
                    p["merchant_clean"],
                    p["category"],
                    h,
                    p["created_at"],
                    p["date_raw"],
                    p["date_iso"],
                    sms_hash,
                    p["sms_norm"],         # sms_raw (same)
                ),
            )
            c.commit()
            new_id = cur.lastrowid
            c.close()
            return {"inserted": True, "id": new_id}
        except sqlite3.IntegrityError:
            c.close()
            return {"inserted": False, "id": None}

@app.get("/expenses")
def expenses(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    q = "SELECT id, user_id, amount, currency, merchant_clean, category, created_at FROM expenses"
    params: List[Any] = []
    if user_id:
        q += " WHERE user_id = ?"
        params.append(user_id)
    q += " ORDER BY datetime(created_at) DESC LIMIT ?"
    params.append(limit)

    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute(q, params)
        rows = cur.fetchall()
        c.close()

    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "user_id": r["user_id"],
            "amount": r["amount"],
            "currency": r["currency"],
            "merchant_clean": r["merchant_clean"] or "",
            "category": r["category"] or "other",
            "created_at": r["created_at"],
        })
    return {"count": len(out), "expenses": out}
