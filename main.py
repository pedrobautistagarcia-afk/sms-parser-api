import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# Router de suscripciones (archivo subscriptions.py en la misma carpeta)
from subscriptions import router as subscriptions_router

# ======================================================
# CONFIGURACIÓN
# ======================================================
API_KEY = os.getenv("API_KEY", "ElPortichuelo99")
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")

# Enchufamos el router de suscripciones
app.include_router(subscriptions_router)

# ======================================================
# STATIC FILES (dashboard)
# ======================================================
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/app", response_class=HTMLResponse)
def serve_app():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()


# ======================================================
# BASE DE DATOS
# ======================================================
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            amount REAL NOT NULL,
            currency TEXT NOT NULL,
            merchant_clean TEXT,
            category TEXT,
            sms_hash TEXT UNIQUE,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


init_db()

# ======================================================
# HELPERS
# ======================================================
def require_api_key(request: Request):
    key = request.query_params.get("api_key") or request.headers.get("x-api-key")
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def hash_sms(user_id: str, sms: str) -> str:
    return hashlib.sha256(f"{user_id}:{sms}".encode()).hexdigest()


def parse_amount(sms: str) -> float | None:
    m = re.search(r"KWD\s*([\d\.]+)", sms)
    if m:
        return float(m.group(1))
    return None


def parse_merchant(sms: str) -> str:
    # Ejemplos:
    # "debited with KWD 3.750 from PICK DAILY PICKS  , ALSALMIYA"
    # "debited with KWD 3.000 for WAMD Service on 2026-01-15 ..."
    m = re.search(r"\bfrom\s+(.+?)(?:\s+on|\s*,|\.)", sms, re.IGNORECASE)
    if m:
        return m.group(1).strip().lower()

    m2 = re.search(r"\bfor\s+(.+?)(?:\s+on|\s*,|\.)", sms, re.IGNORECASE)
    if m2:
        return m2.group(1).strip().lower()

    return "unknown"


def detect_category(sms: str, merchant_clean: str) -> str | None:
    """
    Regla simple y explícita:
    - si aparece 'upayments' en el sms o en el merchant => 'apartment rental'
    (lo demás lo dejamos como None por ahora).
    """
    s = (sms or "").lower()
    m = (merchant_clean or "").lower()

    if "upayments" in s or "upayments" in m:
        return "apartment rental"

    return None


# ======================================================
# INGEST (PROTEGIDO)
# ======================================================
@app.post("/ingest")
async def ingest(request: Request):
    require_api_key(request)

    body = await request.json()
    user_id = body.get("userid") or body.get("user_id")
    sms = body.get("sms")

    if not user_id or not sms:
        return JSONResponse(status_code=400, content={"error": "Missing userid or sms"})

    amount = parse_amount(sms)
    if amount is None:
        return JSONResponse(status_code=400, content={"error": "Amount not found"})

    merchant = parse_merchant(sms)
    category = detect_category(sms, merchant)

    currency = "KWD"
    created_at = datetime.now(timezone.utc).isoformat()

    sms_hash = hash_sms(user_id, sms)

    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO expenses
            (user_id, amount, currency, merchant_clean, category, sms_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, amount, currency, merchant, category, sms_hash, created_at),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return {"status": "duplicate", "count": 0, "expenses": []}

    last_id = cur.lastrowid
    row = cur.execute("SELECT * FROM expenses WHERE id = ?", (last_id,)).fetchone()
    conn.close()

    return {"status": "ok", "count": 1, "expenses": [dict(row)]}


# ======================================================
# EXPENSES (PÚBLICO – dashboard)
# ======================================================
@app.get("/expenses")
def get_expenses(
    user_id: str = Query("pedro"),
    category: str | None = None,
    q: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 500,
):
    conn = get_db()
    cur = conn.cursor()

    sql = "SELECT * FROM expenses WHERE user_id = ?"
    params = [user_id]

    if category:
        sql += " AND category = ?"
        params.append(category)

    if q:
        sql += " AND merchant_clean LIKE ?"
        params.append(f"%{q.lower()}%")

    if date_from:
        sql += " AND created_at >= ?"
        params.append(date_from)

    if date_to:
        sql += " AND created_at <= ?"
        params.append(date_to)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(sql, params).fetchall()
    conn.close()

    expenses = [dict(r) for r in rows]
    total = round(sum(float(r["amount"]) for r in expenses), 3)

    return {"count": len(expenses), "total": total, "expenses": expenses}


# ======================================================
# INSIGHTS (PÚBLICO – dashboard) — SIN TOP MERCHANTS
# ======================================================
@app.get("/api/insights")
def insights(
    user_id: str = Query("pedro"),
    range: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    category: str | None = None,
    q: str | None = None,
):
    now = datetime.now(timezone.utc)

    if range == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif range == "7d":
        start = now - timedelta(days=7)
    elif range == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif range == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif from_date:
        try:
            start = datetime.fromisoformat(from_date).replace(tzinfo=timezone.utc)
        except Exception:
            start = None
    else:
        start = None

    sql = "SELECT * FROM expenses WHERE user_id = ?"
    params = [user_id]

    if start:
        sql += " AND created_at >= ?"
        params.append(start.isoformat())

    if to_date:
        sql += " AND created_at <= ?"
        params.append(to_date)

    if category:
        sql += " AND category = ?"
        params.append(category)

    if q:
        sql += " AND merchant_clean LIKE ?"
        params.append(f"%{q.lower()}%")

    conn = get_db()
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    total_amount = round(sum(float(r["amount"]) for r in rows), 3)

    by_category = {}
    for r in rows:
        c = r["category"] or "uncategorized"
        by_category[c] = by_category.get(c, 0.0) + float(r["amount"])

    top_categories = sorted(by_category.items(), key=lambda x: x[1], reverse=True)

    category_share = []
    if total_amount > 0:
        for k, v in top_categories:
            category_share.append(
                {"category": k, "amount": round(v, 3), "pct": round(v / total_amount * 100, 1)}
            )

    return {
        "tx_count": len(rows),
        "total_amount": total_amount,
        "top_categories": top_categories,
        "category_share": category_share,
    }
