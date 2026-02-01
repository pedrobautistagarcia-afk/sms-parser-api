# main.py
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIGURACIÓN
# ======================================================
API_KEY = os.getenv("API_KEY", "CHANGE_ME")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")

# Static files (dashboard)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ======================================================
# MODELOS
# ======================================================
class IngestRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    sms: str

    class Config:
        populate_by_name = True
        extra = "allow"

# ======================================================
# BASE DE DATOS
# ======================================================
def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            sms TEXT NOT NULL,
            amount REAL,
            currency TEXT,
            merchant_raw TEXT,
            merchant_clean TEXT,
            category TEXT,
            hash TEXT UNIQUE,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()

init_db()

# ======================================================
# SEGURIDAD – API KEY
#   - Web y endpoints de lectura públicos para el dashboard
#   - Ingest y parse protegidos
# ======================================================
PUBLIC_PATHS = {
    "/", "/app", "/health",
    "/docs", "/openapi.json", "/redoc", "/favicon.ico",
    "/expenses", "/summary", "/api/insights",
}
PUBLIC_PREFIXES = ("/static",)

@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    path = request.url.path

    if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
        return await call_next(request)

    header_key = request.headers.get("x-api-key")
    query_key = request.query_params.get("api_key")

    if header_key != API_KEY and query_key != API_KEY:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    return await call_next(request)

# ======================================================
# ENDPOINTS BÁSICOS
# ======================================================
@app.get("/")
def root():
    return RedirectResponse(url="/app")

@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()}

@app.get("/app")
def app_page():
    return FileResponse("static/index.html", media_type="text/html")

# ======================================================
# PARSEO DE SMS
# ======================================================
AMOUNT_REGEX = re.compile(r"\b(KWD|EUR|USD)\s*([\d,]+(?:\.\d+)?)\b", re.IGNORECASE)
MERCHANT_FOR_ON_REGEX = re.compile(r"\bfor\s+(.+?)\s+on\b", re.IGNORECASE)
MERCHANT_FROM_AT_REGEX = re.compile(r"\b(?:from|at)\s+(.+?)(?:\s+on\b|\.|$)", re.IGNORECASE)

def parse_sms(sms: str):
    amount = None
    currency = None
    merchant_raw = None
    merchant_clean = None

    m_amount = AMOUNT_REGEX.search(sms)
    if m_amount:
        currency = m_amount.group(1).upper()
        try:
            amount = float(m_amount.group(2).replace(",", ""))
        except ValueError:
            pass

    m_for = MERCHANT_FOR_ON_REGEX.search(sms)
    if m_for:
        merchant_raw = m_for.group(1).strip()
    else:
        m_from_at = MERCHANT_FROM_AT_REGEX.search(sms)
        if m_from_at:
            merchant_raw = m_from_at.group(1).strip()

    if merchant_raw:
        merchant_clean = " ".join(merchant_raw.split()).strip()
        merchant_clean = re.sub(
            r"\bYour\s+remaining\s+balance\b.*$",
            "",
            merchant_clean,
            flags=re.IGNORECASE,
        ).strip()

    return amount, currency, merchant_raw, merchant_clean

def make_hash(user_id, sms, amount, currency, merchant_clean):
    base = f"{user_id}|{sms}|{amount}|{currency}|{merchant_clean}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

# ======================================================
# CATEGORÍAS
# ======================================================
CATEGORY_KEYWORDS = {
    "coffee": ["coffee", "cafe", "café", "starbucks", "costa", "caribou"],
    "food": ["restaurant", "pizza", "burger", "shawarma", "kebab", "sushi", "talabat"],
    "groceries": ["supermarket", "lulu", "carrefour", "coop"],
    "transport": ["uber", "careem", "taxi", "fuel", "petrol", "parking", "wamd"],
    "shopping": ["amazon", "noon", "ikea", "zara", "nike"],
    "health": ["pharmacy", "clinic", "hospital", "gym"],
    "subscriptions": ["netflix", "spotify", "apple", "google"],
}

def categorize(merchant_raw: str | None, sms: str | None = None) -> str:
    haystack = " ".join([merchant_raw or "", sms or ""]).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in haystack:
                return category
    return "other"

# ======================================================
# PARSE (PROTEGIDO)
# ======================================================
@app.post("/parse")
async def parse_only(data: IngestRequest):
    amount, currency, merchant_raw, merchant_clean = parse_sms(data.sms)
    category = categorize(merchant_raw, data.sms)
    return {
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": category,
    }

# ======================================================
# INGEST (PROTEGIDO)
# ======================================================
@app.post("/ingest")
async def ingest(data: IngestRequest):
    user_id = data.user_id
    sms = data.sms

    amount, currency, merchant_raw, merchant_clean = parse_sms(sms)
    category = categorize(merchant_raw, sms)
    expense_hash = make_hash(user_id, sms, amount, currency, merchant_clean)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT id FROM expenses WHERE hash = ?", (expense_hash,))
    if cur.fetchone():
        conn.close()
        return {"status": "duplicate"}

    cur.execute(
        """
        INSERT INTO expenses (
            user_id, sms, amount, currency,
            merchant_raw, merchant_clean,
            category, hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            sms,
            amount,
            currency,
            merchant_raw,
            merchant_clean,
            category,
            expense_hash,
            datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        ),
    )

    conn.commit()
    conn.close()
    return {"status": "saved"}

# ======================================================
# FILTROS (misma lógica para tabla + insights)
# ======================================================
def _iso_start(date_str: str) -> str:
    return f"{date_str}T00:00:00+00:00"

def _iso_end(date_str: str) -> str:
    return f"{date_str}T23:59:59+00:00"

def _utc_range_from_preset(preset: str):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    if preset == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        return start.isoformat(), end.isoformat()

    if preset == "7d":
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start = (end - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start.isoformat(), end.isoformat()

    if preset == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            next_month = start.replace(year=start.year + 1, month=1)
        else:
            next_month = start.replace(month=start.month + 1)
        end = next_month - timedelta(seconds=1)
        return start.isoformat(), end.isoformat()

    if preset == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        next_year = start.replace(year=start.year + 1)
        end = next_year - timedelta(seconds=1)
        return start.isoformat(), end.isoformat()

    return None, None

def _build_where_and_params(
    user_id: str | None,
    category: str | None,
    q: str | None,
    date_from: str | None,  # YYYY-MM-DD
    date_to: str | None,    # YYYY-MM-DD
    range_preset: str | None = None,  # today | 7d | month | year
):
    where = []
    params = []

    if user_id:
        where.append("user_id = ?")
        params.append(user_id)

    if category:
        where.append("category = ?")
        params.append(category)

    if q:
        where.append("(merchant_clean LIKE ? OR sms LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])

    if range_preset:
        preset_from, preset_to = _utc_range_from_preset(range_preset)
        if preset_from and preset_to:
            where.append("created_at >= ?")
            params.append(preset_from)
            where.append("created_at <= ?")
            params.append(preset_to)
    else:
        if date_from:
            where.append("created_at >= ?")
            params.append(_iso_start(date_from))
        if date_to:
            where.append("created_at <= ?")
            params.append(_iso_end(date_to))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return where_sql, params

# ======================================================
# LISTADO DE GASTOS (PÚBLICO PARA DASHBOARD)
# ======================================================
@app.get("/expenses")
def list_expenses(
    user_id: str | None = None,
    limit: int = 200,
    category: str | None = None,
    q: str | None = None,
    date_from: str | None = None,  # YYYY-MM-DD
    date_to: str | None = None,    # YYYY-MM-DD
):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where_sql, params = _build_where_and_params(
        user_id=user_id,
        category=category,
        q=q,
        date_from=date_from,
        date_to=date_to,
        range_preset=None,
    )

    sql = f"""
        SELECT id, user_id, amount, currency, merchant_clean, category, created_at
        FROM expenses
        {where_sql}
        ORDER BY created_at DESC
        LIMIT ?
    """
    params.append(limit)

    cur.execute(sql, tuple(params))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "expenses": rows}

# ======================================================
# SUMMARY (por si lo sigues usando)
# ======================================================
@app.get("/summary")
def summary(user_id: str | None = None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    base_where = "WHERE 1=1"
    params = []
    if user_id:
        base_where += " AND user_id = ?"
        params.append(user_id)

    cur.execute(
        f"""SELECT COALESCE(SUM(amount),0)
            FROM expenses {base_where}
            AND datetime(created_at) >= datetime('now','start of day')""",
        tuple(params),
    )
    today_total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT COALESCE(SUM(amount),0)
            FROM expenses {base_where}
            AND datetime(created_at) >= datetime('now','-7 days')""",
        tuple(params),
    )
    last7_total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT COALESCE(SUM(amount),0)
            FROM expenses {base_where}
            AND strftime('%Y-%m', datetime(created_at)) = strftime('%Y-%m', 'now')""",
        tuple(params),
    )
    month_total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT COALESCE(SUM(amount),0)
            FROM expenses {base_where}
            AND strftime('%Y', datetime(created_at)) = strftime('%Y', 'now')""",
        tuple(params),
    )
    year_total = cur.fetchone()[0]

    conn.close()
    return {
        "today_total": today_total,
        "last7_total": last7_total,
        "month_total": month_total,
        "year_total": year_total,
    }

# ======================================================
# INSIGHTS DEL RANGO SELECCIONADO (ENDPOINT NUEVO)
#   GET /api/insights?user_id=pedro&range=today|7d|month|year&from=YYYY-MM-DD&to=YYYY-MM-DD&category=...&q=...
#
# Regla de oro: mismos filtros que /expenses.
# ======================================================
@app.get("/api/insights")
def insights(
    user_id: str | None = None,
    range: str | None = None,  # today | 7d | month | year
    from_: str | None = Query(default=None, alias="from"),  # YYYY-MM-DD
    to: str | None = None,  # YYYY-MM-DD
    category: str | None = None,
    q: str | None = None,
):
    range_preset = range if range in ("today", "7d", "month", "year") else None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    where_sql, params = _build_where_and_params(
        user_id=user_id,
        category=category,
        q=q,
        date_from=from_,
        date_to=to,
        range_preset=range_preset,
    )

    # total_amount + tx_count
    cur.execute(
        f"""
        SELECT
          COALESCE(SUM(amount), 0) AS total_amount,
          COUNT(*) AS tx_count
        FROM expenses
        {where_sql}
        """,
        tuple(params),
    )
    row = cur.fetchone()
    total_amount = float(row[0] or 0)
    tx_count = int(row[1] or 0)

    # top_merchants (Top 5 por gasto total)
    cur.execute(
        f"""
        SELECT
          COALESCE(NULLIF(TRIM(merchant_clean), ''), '(unknown)') AS merchant,
          COALESCE(SUM(amount), 0) AS total
        FROM expenses
        {where_sql}
        GROUP BY COALESCE(NULLIF(TRIM(merchant_clean), ''), '(unknown)')
        ORDER BY total DESC
        LIMIT 5
        """,
        tuple(params),
    )
    top_merchants = [{"merchant": r[0], "total": float(r[1] or 0)} for r in cur.fetchall()]

    # top_categories + category_share
    cur.execute(
        f"""
        SELECT
          COALESCE(NULLIF(TRIM(category), ''), 'other') AS category,
          COALESCE(SUM(amount), 0) AS total
        FROM expenses
        {where_sql}
        GROUP BY COALESCE(NULLIF(TRIM(category), ''), 'other')
        ORDER BY total DESC
        """,
        tuple(params),
    )
    cat_rows = [(r[0], float(r[1] or 0)) for r in cur.fetchall()]
    top_categories = [{"category": c, "total": t} for (c, t) in cat_rows]

    if total_amount > 0:
        category_share = [
            {"category": c, "share": (t / total_amount) * 100.0, "total": t}
            for (c, t) in cat_rows
        ]
    else:
        category_share = [{"category": c, "share": 0.0, "total": t} for (c, t) in cat_rows]

    conn.close()

    return {
        "total_amount": total_amount,
        "tx_count": tx_count,
        "top_merchants": top_merchants,
        "top_categories": top_categories,
        "category_share": category_share,
    }
