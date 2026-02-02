import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from threading import Lock
from typing import Optional, Any, Dict, List

from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIG
# ======================================================
API_KEY = os.getenv("API_KEY", "CHANGE_ME")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")
app.mount("/static", StaticFiles(directory="static"), name="static")

db_lock = Lock()

# ======================================================
# DB HELPERS
# ======================================================
def ensure_db_dir():
    folder = os.path.dirname(DB_PATH)
    if folder and folder != ".":
        os.makedirs(folder, exist_ok=True)

def conn() -> sqlite3.Connection:
    ensure_db_dir()
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def require_key(x_api_key: Optional[str], api_key: Optional[str]):
    k = x_api_key or api_key
    if not k or k != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def normalize_merchant(s: str) -> str:
    s = normalize_spaces(s)
    return s.strip(" ,.-")

# ======================================================
# DB INIT + RULES SEED
# ======================================================
def init_db():
    with db_lock:
        c = conn()
        cur = c.cursor()

        # Expenses
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")

        # Rules (auto-categorization / auto-merchant normalization)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            field TEXT NOT NULL DEFAULT 'merchant',     -- 'merchant' | 'sms'
            match_type TEXT NOT NULL DEFAULT 'contains',-- only 'contains' for v1
            set_category TEXT,                          -- optional
            set_merchant_clean TEXT,                    -- optional
            priority INTEGER NOT NULL DEFAULT 100,
            enabled INTEGER NOT NULL DEFAULT 1
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_enabled_priority ON rules(enabled, priority)")

        # Seed rules once (if empty)
        cur.execute("SELECT COUNT(1) AS n FROM rules")
        n = cur.fetchone()["n"]
        if n == 0:
            seeds = [
                # Food delivery
                ("TALABAT", "merchant", "contains", "food", "TALABAT", 300, 1),
                # Coffee
                ("STARBUCKS", "merchant", "contains", "coffee", "STARBUCKS", 250, 1),
                ("CAFE", "merchant", "contains", "coffee", None, 200, 1),
                # Transport
                ("UBER", "merchant", "contains", "transport", "UBER", 220, 1),
                # Groceries example
                ("PICK DAILY PICKS", "merchant", "contains", "groceries", "PICK DAILY PICKS", 210, 1),
            ]
            cur.executemany("""
                INSERT INTO rules(pattern, field, match_type, set_category, set_merchant_clean, priority, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, seeds)

        c.commit()
        c.close()

init_db()

# ======================================================
# MODELS
# ======================================================
class IngestRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    sms: str
    class Config:
        populate_by_name = True

class PatchCategoryRequest(BaseModel):
    category: str

class PatchMerchantRequest(BaseModel):
    merchant_clean: str

class RestoreRequest(BaseModel):
    user_id: str
    sms: str
    amount: Optional[float] = None
    currency: Optional[str] = None
    merchant_raw: Optional[str] = None
    merchant_clean: Optional[str] = None
    category: Optional[str] = None
    created_at: Optional[str] = None
    date_raw: Optional[str] = None
    date_iso: Optional[str] = None

# ======================================================
# PARSING
# ======================================================
CURRENCY_RE = r"(KWD|EUR|USD|GBP|AED|SAR|QAR|BHD|OMR)"
AMOUNT_RE = r"(\d+(?:[.,]\d{1,3})?)"

def to_float(s: str) -> float:
    return float(s.replace(",", "."))

def parse_sms(sms: str) -> Dict[str, Any]:
    sms_norm = normalize_spaces(sms)

    # Currency + Amount
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

    # Merchant
    merchant = ""
    mm = re.search(r"\bfor\s+(.+?)\s+\bon\b", sms_norm, re.IGNORECASE)
    if mm:
        merchant = mm.group(1)
    else:
        mm = re.search(r"\bfrom\s+(.+?)(?:\s*,\s*|\s+\bon\b|$)", sms_norm, re.IGNORECASE)
        if mm:
            merchant = mm.group(1)

    merchant_raw = normalize_spaces(merchant)
    merchant_clean = normalize_merchant(merchant_raw)

    # Date (optional)
    created_at = datetime.now(timezone.utc).isoformat()
    date_raw = None
    date_iso = None
    dm = re.search(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\b", sms_norm)
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

def compute_hash(user_id: str, amount: Any, currency: Any, merchant_clean: Any, created_at: Any) -> str:
    base = f"{user_id}|{amount}|{currency}|{merchant_clean}|{created_at}"
    return sha256_hex(base)

# ======================================================
# RULE ENGINE (v1: contains)
# ======================================================
def apply_rules(merchant_clean: str, sms_norm: str) -> Dict[str, Optional[str]]:
    """
    Returns possibly updated: category, merchant_clean
    """
    m_up = (merchant_clean or "").upper()
    sms_up = (sms_norm or "").upper()

    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute("""
            SELECT pattern, field, match_type, set_category, set_merchant_clean
            FROM rules
            WHERE enabled = 1
            ORDER BY priority DESC, id ASC
        """)
        rules = cur.fetchall()
        c.close()

    out_category = None
    out_merchant = None

    for r in rules:
        pattern = (r["pattern"] or "").upper()
        field = (r["field"] or "merchant").lower()
        match_type = (r["match_type"] or "contains").lower()

        haystack = m_up if field == "merchant" else sms_up
        ok = False
        if match_type == "contains" and pattern and pattern in haystack:
            ok = True

        if ok:
            if r["set_category"]:
                out_category = normalize_spaces(r["set_category"]).lower()
            if r["set_merchant_clean"]:
                out_merchant = normalize_merchant(r["set_merchant_clean"])
            # Primera regla que matchea con mayor prioridad manda
            break

    return {"category": out_category, "merchant_clean": out_merchant}

# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def home():
    return FileResponse("static/index.html")

@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH}

# --------- READ is FREE (no key) ---------

@app.get("/expenses")
def expenses(
    user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    q = """
    SELECT id, user_id, amount, currency, merchant_clean, category, created_at
    FROM expenses
    """
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

# --------- INGEST requires key ---------

@app.post("/ingest")
def ingest(
    req: IngestRequest,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)

    p = parse_sms(req.sms)

    # Apply rules BEFORE insert
    ruled = apply_rules(p["merchant_clean"], p["sms_norm"])
    if ruled["category"]:
        p["category"] = ruled["category"]
    if ruled["merchant_clean"]:
        p["merchant_clean"] = ruled["merchant_clean"]

    sms_hash = sha256_hex(p["sms_norm"])
    h = compute_hash(req.user_id, p["amount"], p["currency"], p["merchant_clean"], p["created_at"])

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
                    p["sms_norm"],
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
                    p["sms_norm"],
                ),
            )
            c.commit()
            new_id = cur.lastrowid
            c.close()
            return {"inserted": True, "id": new_id}
        except sqlite3.IntegrityError:
            cur.execute("SELECT id FROM expenses WHERE sms_hash = ?", (sms_hash,))
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT id FROM expenses WHERE hash = ?", (h,))
                row = cur.fetchone()
            c.close()
            return {"inserted": False, "id": (row["id"] if row else None)}

# --------- Write ops require key ---------

@app.patch("/expenses/{expense_id}/category")
def patch_category(
    expense_id: int,
    body: PatchCategoryRequest,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)
    cat = normalize_spaces(body.category).lower() or "other"

    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute("UPDATE expenses SET category = ? WHERE id = ?", (cat, expense_id))
        c.commit()
        updated = cur.rowcount
        c.close()

    return {"updated": updated, "id": expense_id, "category": cat}

@app.patch("/expenses/{expense_id}/merchant")
def patch_merchant(
    expense_id: int,
    body: PatchMerchantRequest,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)
    m = normalize_merchant(body.merchant_clean)

    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute("UPDATE expenses SET merchant_clean = ? WHERE id = ?", (m, expense_id))
        c.commit()
        updated = cur.rowcount
        c.close()

    return {"updated": updated, "id": expense_id, "merchant_clean": m}

@app.delete("/expenses/{expense_id}")
def delete_expense(
    expense_id: int,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)

    with db_lock:
        c = conn()
        cur = c.cursor()

        cur.execute("SELECT * FROM expenses WHERE id = ?", (expense_id,))
        row = cur.fetchone()
        if not row:
            c.close()
            return {"deleted": 0, "deleted_row": None}

        cur.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
        c.commit()
        deleted = cur.rowcount

        deleted_row = dict(row) if row else None
        c.close()

    return {"deleted": deleted, "deleted_row": deleted_row}

@app.post("/expenses/restore")
def restore_expense(
    body: RestoreRequest,
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)

    user_id = normalize_spaces(body.user_id)
    sms_norm = normalize_spaces(body.sms)

    amount = body.amount
    currency = (body.currency or "KWD").upper()
    merchant_raw = normalize_spaces(body.merchant_raw or (body.merchant_clean or ""))
    merchant_clean = normalize_merchant(body.merchant_clean or merchant_raw)
    category = normalize_spaces(body.category or "other").lower() or "other"

    created_at = body.created_at or datetime.now(timezone.utc).isoformat()
    date_raw = body.date_raw
    date_iso = body.date_iso

    sms_hash = sha256_hex(sms_norm)
    h = compute_hash(user_id, amount, currency, merchant_clean, created_at)

    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute(
            """
            INSERT INTO expenses
            (user_id, sms, amount, currency, merchant_raw, merchant_clean, category, hash, created_at, date_raw, date_iso, sms_hash, sms_raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                sms_norm,
                amount,
                currency,
                merchant_raw,
                merchant_clean,
                category,
                h,
                created_at,
                date_raw,
                date_iso,
                sms_hash,
                sms_norm,
            ),
        )
        c.commit()
        new_id = cur.lastrowid
        c.close()

    return {"restored": True, "id": new_id}

# Optional: debug rules (requires key)
@app.get("/rules")
def list_rules(
    x_api_key: Optional[str] = Header(default=None),
    api_key: Optional[str] = Query(default=None),
):
    require_key(x_api_key, api_key)
    with db_lock:
        c = conn()
        cur = c.cursor()
        cur.execute("SELECT * FROM rules ORDER BY priority DESC, id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        c.close()
    return {"count": len(rows), "rules": rows}
