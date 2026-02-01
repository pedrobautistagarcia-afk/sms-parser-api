# main.py
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIGURACIÓN
# ======================================================
API_KEY = os.getenv("API_KEY", "CHANGE_ME")

# IMPORTANTE:
# - En Render con Disk montado en /var/data, esta ruta hace que SQLite sea persistente.
# - Si quieres cambiarla, usa la env var DB_PATH.
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
    # Crear carpeta si hace falta (p.ej. /var/data)
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
# SEGURIDAD – API KEY (HEADER O QUERY)
#   - Hacemos pública la web /app y los estáticos /static/*
#   - Protegemos los endpoints sensibles (/ingest, /parse, /expenses, etc.)
# ======================================================
PUBLIC_PATHS = {"/", "/app", "/health", "/docs", "/openapi.json", "/redoc", "/favicon.ico"}
PUBLIC_PREFIXES = ("/static",)

@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    path = request.url.path

    # Permitir web + assets sin API key
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
    # Para que abrir la URL base te lleve directamente al dashboard
    return RedirectResponse(url="/app")

@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()}

@app.get("/app")
def app_page():
    # Página del dashboard
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
# PARSE (SIN GUARDAR) - PROTEGIDO
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
# INGEST (GUARDAR) - PROTEGIDO
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
    row = cur.fetchone()
    if row:
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
# LISTADO - PROTEGIDO
# ======================================================
@app.get("/expenses")
def list_expenses(user_id: str | None = None, limit: int = 200):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if user_id:
        cur.execute(
            """
            SELECT id, user_id, amount, currency, merchant_clean, category, created_at
            FROM expenses
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
    else:
        cur.execute(
            """
            SELECT id, user_id, amount, currency, merchant_clean, category, created_at
            FROM expenses
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "expenses": rows}
