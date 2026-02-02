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
API_KEY = os.getenv("API_KEY", "ElPortichuelo99")

# Render Disk (persistente)
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")

# Static (dashboard)
# Asegúrate de tener carpeta ./static con index.html
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ======================================================
# HELPERS
# ======================================================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_sms_hash(s: str) -> str:
    # Hash del SMS exacto (tras strip). Si cambia un carácter -> cambia el hash.
    return hashlib.sha256(s.strip().encode("utf-8")).hexdigest()


def require_api_key(api_key: str):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid api_key")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, col: str, coltype: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
        conn.commit()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = get_conn()
    cur = conn.cursor()

    # Tabla base (creación si no existe)
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
            sms_hash TEXT,
            date_raw TEXT,
            date_iso TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # Migraciones “por si vienes de versiones viejas”
    ensure_column(conn, "expenses", "merchant_raw", "TEXT")
    ensure_column(conn, "expenses", "merchant_clean", "TEXT")
    ensure_column(conn, "expenses", "category", "TEXT")
    ensure_column(conn, "expenses", "sms_hash", "TEXT")
    ensure_column(conn, "expenses", "date_raw", "TEXT")
    ensure_column(conn, "expenses", "date_iso", "TEXT")
    ensure_column(conn, "expenses", "created_at", "TEXT")

    # Índice único para evitar duplicados aunque haya bugs
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")
    conn.commit()
    conn.close()


init_db()

# ======================================================
# MODELS
# ======================================================
class IngestRequest(BaseModel):
    # compat: aceptamos userid y user_id
    user_id: str = Field(..., alias="userid")
    sms: str

    class Config:
        populate_by_name = True


# ======================================================
# SMS PARSING (muy simple, ajustable)
# ======================================================
_AMOUNT_RE = re.compile(r"\b(?:KWD|KD)\s*([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
_CURRENCY_RE = re.compile(r"\b(KWD|KD)\b", re.IGNORECASE)

# intenta capturar merchant: "from XXXX" o "for XXXX"
_MERCHANT_RE = re.compile(r"\b(?:from|for)\s+(.+?)(?:\s+on\b|\.|,?\s+Your\b|$)", re.IGNORECASE)

# intenta capturar fecha: "on YYYY-MM-DD HH:MM:SS"
_DATE_RE = re.compile(r"\bon\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\b", re.IGNORECASE)


def parse_sms(sms: str) -> Dict[str, Any]:
    s = sms.strip()

    # currency
    mcur = _CURRENCY_RE.search(s)
    currency = (mcur.group(1).upper() if mcur else "KWD").replace("KD", "KWD")

    # amount
    mam = _AMOUNT_RE.search(s)
    amount = float(mam.group(1)) if mam else 0.0

    # merchant
    mmer = _MERCHANT_RE.search(s)
    merchant_raw = mmer.group(1).strip() if mmer else ""
    merchant_clean = re.sub(r"\s+", " ", merchant_raw).strip()

    # date
    mdt = _DATE_RE.search(s)
    date_raw = mdt.group(1) if mdt else None
    date_iso = None
    if date_raw:
        try:
            dt = datetime.strptime(date_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            date_iso = dt.isoformat()
        except Exception:
            date_iso = None

    # category (heurística simple)
    low = (merchant_clean or "").lower()
    category = "other"
    if any(k in low for k in ["starbucks", "cafe", "cafeteria", "coffee"]):
        category = "coffee"
    elif any(k in low for k in ["talabat", "pickup", "pick daily", "restaurant", "burger", "pizza"]):
        category = "food"
    elif any(k in low for k in ["upayments", "stc", "zain", "ooredoo"]):
        category = "services"

    return {
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw or None,
        "merchant_clean": merchant_clean or None,
        "category": category,
        "date_raw": date_raw,
        "date_iso": date_iso,
    }


# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def root():
    # Simple health
    return {"ok": True, "service": "sms-parser-api"}


@app.get("/app")
def app_page(api_key: str = Query(...)):
    require_api_key(api_key)
    index_path = os.path.join("static", "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(status_code=404, detail="static/index.html not found")
    return FileResponse(index_path)


@app.post("/ingest")
async def ingest(req: IngestRequest, api_key: str = Query(...)):
    require_api_key(api_key)

    user_id = req.user_id.strip()
    sms_text = req.sms

    sms_hash = compute_sms_hash(sms_text)

    parsed = parse_sms(sms_text)
    amount = parsed["amount"]
    currency = parsed["currency"]
    merchant_raw = parsed["merchant_raw"]
    merchant_clean = parsed["merchant_clean"]
    category = parsed["category"]
    date_raw = parsed["date_raw"]
    date_iso = parsed["date_iso"]
    created_at = now_utc_iso()

    conn = get_conn()
    cur = conn.cursor()

    # DEDUPE REAL: solo si ya existe el mismo sms_hash
    cur.execute("SELECT 1 FROM expenses WHERE sms_hash = ? LIMIT 1", (sms_hash,))
    if cur.fetchone() is not None:
        conn.close()
        return {"ok": True, "deduped": True, "sms_hash": sms_hash}

    # Insert
    try:
        cur.execute(
            """
            INSERT INTO expenses (user_id, amount, currency, merchant_raw, merchant_clean, category, sms_hash, date_raw, date_iso, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, amount, currency, merchant_raw, merchant_clean, category, sms_hash, date_raw, date_iso, created_at),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # si el índice único salta por race condition
        conn.close()
        return {"ok": True, "deduped": True, "sms_hash": sms_hash}

    conn.close()
    return {"ok": True, "deduped": False, "sms_hash": sms_hash}


@app.get("/expenses")
def list_expenses(
    api_key: str = Query(...),
    limit: int = Query(200, ge=1, le=2000),
    sort: str = Query("date_desc"),
    category: Optional[str] = None,
    q: Optional[str] = None,
):
    require_api_key(api_key)

    conn = get_conn()
    cur = conn.cursor()

    where = []
    params: List[Any] = []

    if category and category != "all":
        where.append("category = ?")
        params.append(category)

    if q:
        where.append("(merchant_clean LIKE ? OR merchant_raw LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Orden: si no hay date_iso, usa created_at
    if sort == "date_asc":
        order_sql = "ORDER BY COALESCE(date_iso, created_at) ASC"
    else:
        order_sql = "ORDER BY COALESCE(date_iso, created_at) DESC"

    cur.execute(
        f"""
        SELECT id, user_id, amount, currency, merchant_raw, merchant_clean, category, sms_hash, date_raw, date_iso, created_at
        FROM expenses
        {where_sql}
        {order_sql}
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    return {"count": len(rows), "expenses": rows}


@app.get("/api/insights")
def insights(api_key: str = Query(...)):
    require_api_key(api_key)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) as n, COALESCE(SUM(amount),0) as total FROM expenses")
    row = cur.fetchone()
    total_count = int(row["n"])
    total_amount = float(row["total"])

    cur.execute(
        """
        SELECT COALESCE(category,'other') as category, COUNT(*) as n, COALESCE(SUM(amount),0) as total
        FROM expenses
        GROUP BY COALESCE(category,'other')
        ORDER BY total DESC
        """
    )
    by_cat = [dict(r) for r in cur.fetchall()]

    conn.close()
    return {
        "count": total_count,
        "total_amount": total_amount,
        "by_category": by_cat,
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Para que Render no te devuelva 500 “mudo”
    return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})
