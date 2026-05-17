import os
import re
import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIG
# ======================================================
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")
API_KEY = os.getenv("API_KEY", "ElPortichuelo99")

def check_key(api_key):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid api_key")

app = FastAPI(title="Expense Tracker API")

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": repr(exc), "path": str(request.url.path)}
    )

if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# ── PWA static file routes ──────────────────────────────────────
@app.get("/manifest.json")
async def serve_manifest():
    return FileResponse("static/manifest.json", media_type="application/json")

@app.get("/sw.js")
async def serve_sw():
    return FileResponse("static/sw.js", media_type="application/javascript")

@app.get("/icon-192.png")
async def serve_icon192():
    return FileResponse("static/icon-192.png", media_type="image/png")

@app.get("/icon-512.png")
async def serve_icon512():
    return FileResponse("static/icon-512.png", media_type="image/png")


# ======================================================
# CATEGORIES — rules applied in order, first match wins
# ======================================================
CATEGORY_RULES = [
    # --- COFFEE ---
    ("coffee", [
        "starbucks", "costa coffee", "tim hortons", "caf cafe", "cafe", "coffee",
        "dunkin", "caribou", "peet", "lavazza", "nespresso", "barista",
    ]),
    # --- FOOD / DELIVERY ---
    ("food", [
        "talabat", "deliveroo", "carriage", "hunger station", "jahez",
        "mcdonald", "burger king", "kfc", "hardee", "popeye", "subway",
        "pizza hut", "domino", "papa john", "little caesar",
        "restaurant", "resto", "kitchen", "grill", "bbq", "shawarma",
        "falafel", "sushi", "ramen", "noodle", "chinese", "indian",
        "pick", "eat", "food", "dining", "bistro", "brasserie",
        "maki", "wasabi", "wagamama", "nando", "five guys",
        "shake shack", "smashburger", "johnny rocket",
    ]),
    # --- GROCERIES ---
    ("groceries", [
        "lulu", "sultan center", "carrefour", "coop", "cooperative",
        "geant", "hypermarket", "supermarket", "market", "grocery",
        "spinneys", "waitrose", "danube", "farm fresh", "organic",
        "al meera", "family food", "sultan", "cold storage",
    ]),
    # --- PHARMACY ---
    ("pharmacy", [
        "pharmacy", "pharmacie", "zafraan", "al dawaa", "boots",
        "life pharmacy", "ibn sina", "nahdi", "dawaa", "drugstore",
        "watson", "guardian", "medical store",
    ]),
    # --- TRANSPORT ---
    ("transport", [
        "uber", "careem", "bolt", "taxi", "cab", "lyft",
        "metro", "bus", "train", "transit", "transport",
        "parking", "salik", "petrol", "fuel", "gas station",
        "shell", "bp", "total", "adnoc", "q8", "kuwait national",
    ]),
    # --- UTILITIES ---
    ("utilities", [
        "wamd", "zain", "ooredoo", "viva", "stc", "du ", "etisalat",
        "electricity", "water", "mew ", "kahramaa", "utility",
        "internet", "broadband", "telecom", "mobile", "phone bill",
        "municipality", "baladiya",
    ]),
    # --- SHOPPING ---
    ("shopping", [
        "amazon", "noon", "namshi", "ounass", "sivvi",
        "h&m", "zara", "mango", "gap", "uniqlo", "primark",
        "marks spencer", "next ", "forever 21", "pull bear",
        "bershka", "stradivarius", "massimo dutti",
        "nike", "adidas", "puma", "reebok", "under armour",
        "sun and sand", "decathlon", "sports", "sport",
        "ikea", "home centre", "pottery barn", "west elm",
        "virgin megastore", "jarir", "lulu electronics",
        "apple store", "samsung", "huawei",
    ]),
    # --- ENTERTAINMENT ---
    ("entertainment", [
        "netflix", "spotify", "apple music", "youtube", "disney",
        "hulu", "paramount", "hbo", "osn", "shahid", "anghami",
        "cinema", "cinescape", "vox cinema", "imax",
        "playstation", "xbox", "steam", "nintendo", "google play",
        "app store", "itunes",
    ]),
    # --- HEALTH ---
    ("health", [
        "hospital", "clinic", "doctor", "dr ", "medical",
        "dental", "dentist", "optician", "optical", "laboratory",
        "lab ", "radiology", "physiotherapy", "gym", "fitness",
        "anytime fitness", "gold's gym", "curves", "kuwait hospital",
    ]),
    # --- TRAVEL ---
    ("travel", [
        "hotel", "marriott", "hilton", "hyatt", "sheraton", "radisson",
        "holiday inn", "ibis", "novotel", "rotana", "jumeirah",
        "airbnb", "booking.com", "expedia", "agoda",
        "airline", "kuwait airways", "emirates", "flydubai", "air arabia",
        "flynas", "jazeera", "airport", "duty free",
    ]),
    # --- EDUCATION ---
    ("education", [
        "school", "university", "college", "tuition", "course",
        "udemy", "coursera", "skillshare", "pluralsight",
        "book", "bookstore", "jarir bookstore", "virgin books",
    ]),
    # --- SALARY / INCOME ---
    ("salary", [
        "salary", "payroll", "wage",
    ]),
]

_KW_MAP: Dict[str, str] = {}
for _cat, _keywords in CATEGORY_RULES:
    for _kw in _keywords:
        _KW_MAP[_kw.lower()] = _cat


def categorize(merchant: str, sms: str) -> str:
    targets = [
        (merchant or "").lower(),
        (sms or "").lower(),
    ]
    for target in targets:
        if not target:
            continue
        for kw in sorted(_KW_MAP.keys(), key=len, reverse=True):
            if kw in target:
                return _KW_MAP[kw]
    return "other"


# ======================================================
# MERCHANT CLEANING
# ======================================================
def clean_merchant(raw: str) -> str:
    if not raw:
        return ""
    m = raw.strip()
    m = re.sub(r'\.?\s*(?:your\s+)?avl\.?\s*balance.*$', '', m, flags=re.IGNORECASE).strip()
    m = re.sub(r'\s*,\s*[A-Z][A-Z\s\-]{1,20}$', '', m).strip()
    m = m.rstrip('.,- ').strip()
    return m or raw.strip()


# ======================================================
# AMOUNT / CURRENCY EXTRACTION
# ======================================================
CURRENCIES = ("KWD", "USD", "EUR", "AED")

def extract_amount_currency(text: str):
    if not text:
        return None, None
    t = text.replace("\u00a0", " ")
    m = re.search(r'\b([A-Z]{3})\s*([0-9][0-9,]*\.?[0-9]*)\b', t)
    if m and m.group(1) in CURRENCIES:
        try:
            return float(m.group(2).replace(",", "")), m.group(1).upper()
        except ValueError:
            pass
    m = re.search(r'\b([0-9][0-9,]*\.?[0-9]*)\s*([A-Z]{3})\b', t)
    if m and m.group(2) in CURRENCIES:
        try:
            return float(m.group(1).replace(",", "")), m.group(2).upper()
        except ValueError:
            pass
    return None, None


# ======================================================
# DIRECTION DETECTION
# ======================================================
def detect_direction(sms: str) -> str:
    low = (sms or "").lower()
    income_keys = ["credited", "credit", "deposit", "salary", "received", "refund"]
    expense_keys = ["debited", "debit", "paid", "purchase", "withdrawn"]
    if any(k in low for k in expense_keys):
        return "expense"
    if any(k in low for k in income_keys):
        return "income"
    return "expense"


# ======================================================
# SMS PARSER
# ======================================================
DATE_RE = r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_sms(sms: str) -> Dict[str, Any]:
    txt = sms.strip()
    amount, currency = extract_amount_currency(txt)
    if not amount:
        m = re.search(r'\bwith\s+([0-9][0-9,]*\.?[0-9]*)\s*(KWD|USD|EUR|AED)\b', txt, re.IGNORECASE)
        if m:
            try:
                amount = float(m.group(1).replace(",", ""))
                currency = m.group(2).upper()
            except ValueError:
                pass
    amount = amount or 0.0
    currency = currency or "KWD"
    direction = detect_direction(txt)
    merchant_raw = ""
    m = re.search(r'\bfor\s+(.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2}|\.your\s+avl|,\s*your\s+avl|$)', txt, re.IGNORECASE)
    if not m:
        m = re.search(r'\bfrom\s+(.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2}|\.your\s+avl|,\s*your\s+avl|$)', txt, re.IGNORECASE)
    if not m:
        m = re.search(r'\bat\s+(.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2}|\.your\s+avl|,\s*your\s+avl|$)', txt, re.IGNORECASE)
    if m:
        merchant_raw = m.group(1).strip().strip(",").strip()
    merchant_clean = clean_merchant(merchant_raw) if merchant_raw else ""
    category = categorize(merchant_clean or merchant_raw, txt)
    if direction == "income" and category == "other":
        category = "income"
    date_raw = ""
    date_iso = ""
    m_date = re.search(DATE_RE, txt)
    if m_date:
        date_raw = m_date.group(1)
        try:
            dt = datetime.strptime(date_raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            date_iso = dt.isoformat()
        except Exception:
            date_iso = ""
    if not date_iso:
        dt = datetime.now(timezone.utc)
        date_iso = dt.isoformat()
        date_raw = dt.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "direction": direction,
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": category,
        "date_raw": date_raw,
        "date_iso": date_iso,
        "created_at": date_iso,
        "sms_raw": txt,
    }


# ======================================================
# RULES ENGINE
# ======================================================
def apply_rules(user_id: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM rules WHERE enabled=1 AND user_id=? ORDER BY priority ASC, id ASC",
        (user_id,)
    )
    rules = cur.fetchall()
    conn.close()
    merch_l = (parsed.get("merchant_clean") or parsed.get("merchant_raw") or "").lower()
    sms_l   = (parsed.get("sms_raw") or "").lower()
    for r in rules:
        match_field = (r["match_field"] or "merchant_clean").strip().lower()
        match_type  = (r["match_type"] or "contains").strip().lower()
        pattern     = (r["pattern"] or "").strip()
        if not pattern:
            continue
        target = sms_l if match_field in ("sms", "sms_raw") else merch_l
        ok = False
        if match_type == "contains":
            ok = pattern.lower() in target
        elif match_type == "equals":
            ok = pattern.lower() == target
        elif match_type == "regex":
            try:
                ok = bool(re.search(pattern, target, re.IGNORECASE))
            except re.error:
                ok = False
        if ok:
            if r["set_category"]:
                parsed["category"] = r["set_category"]
            if r["set_merchant_clean"]:
                parsed["merchant_clean"] = r["set_merchant_clean"]
    return parsed


# ======================================================
# DB
# ======================================================
def ensure_db_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def db() -> sqlite3.Connection:
    ensure_db_dir(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = db()
    cur = conn.cursor()
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
            direction TEXT,
            hash TEXT,
            created_at TEXT,
            date_raw TEXT,
            date_iso TEXT,
            sms_hash TEXT,
            sms_raw TEXT
        )
    """)
    cur.execute("PRAGMA table_info(expenses)")
    cols = {row[1] for row in cur.fetchall()}
    for col, coltype in [
        ("sms","TEXT"), ("sms_raw","TEXT"), ("sms_hash","TEXT"),
        ("amount","REAL"), ("currency","TEXT"), ("merchant_raw","TEXT"),
        ("merchant_clean","TEXT"), ("category","TEXT"), ("direction","TEXT"),
        ("hash","TEXT"), ("created_at","TEXT"), ("date_raw","TEXT"), ("date_iso","TEXT"),
    ]:
        if col not in cols:
            cur.execute(f"ALTER TABLE expenses ADD COLUMN {col} {coltype}")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            match_field TEXT NOT NULL DEFAULT 'merchant_clean',
            match_type TEXT NOT NULL DEFAULT 'contains',
            pattern TEXT NOT NULL,
            set_category TEXT,
            set_merchant_clean TEXT,
            created_at TEXT NOT NULL
        )
    """)
    cur.execute("PRAGMA table_info(rules)")
    rcols = {row[1] for row in cur.fetchall()}
    for col, coltype in [
        ("user_id","TEXT"), ("enabled","INTEGER"), ("priority","INTEGER"),
        ("match_field","TEXT"), ("match_type","TEXT"), ("pattern","TEXT"),
        ("set_category","TEXT"), ("set_merchant_clean","TEXT"), ("created_at","TEXT"),
    ]:
        if col not in rcols:
            cur.execute(f"ALTER TABLE rules ADD COLUMN {col} {coltype}")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deleted_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            original_id INTEGER,
            row_json TEXT NOT NULL,
            deleted_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_deleted_user_time ON deleted_expenses(user_id, deleted_at)")
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%credited%'")
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%salary%'")
    cur.execute("UPDATE expenses SET direction='expense' WHERE (direction IS NULL OR direction='')")
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


class RuleCreate(BaseModel):
    user_id: str = "pedro"
    enabled: bool = True
    priority: int = 100
    match_field: str = "merchant_clean"
    match_type: str = "contains"
    pattern: str
    set_category: Optional[str] = None
    set_merchant_clean: Optional[str] = None


# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def root():
    index_path = os.path.join("static", "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return {"ok": True, "message": "API running."}


@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH, "version": "2026-05-17-icons"}


@app.post("/ingest")
async def ingest(payload: IngestRequest):
    user_id = payload.user_id.strip()
    sms     = payload.sms.strip()
    if not sms:
        return {"inserted": False, "reason": "empty sms"}
    parsed = parse_sms(sms)
    parsed["user_id"] = user_id
    parsed = apply_rules(user_id, parsed)
    sms_hash = sha256(sms)
    row_hash = sha256(
        f"{user_id}|{parsed.get('amount')}|{parsed.get('currency')}|"
        f"{parsed.get('merchant_clean')}|{parsed.get('date_iso')}|{sms_hash}"
    )
    conn = db()
    cur  = conn.cursor()
    cur.execute("SELECT id FROM expenses WHERE sms_hash=?", (sms_hash,))
    existing = cur.fetchone()
    if existing:
        conn.close()
        return {"inserted": False, "reason": "duplicate", "existing_id": existing["id"]}
    try:
        cur.execute("""
            INSERT INTO expenses
              (user_id, sms, amount, currency, merchant_raw, merchant_clean,
               category, direction, hash, created_at, date_raw, date_iso, sms_hash, sms_raw)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            user_id, sms,
            parsed["amount"], parsed["currency"],
            parsed["merchant_raw"], parsed["merchant_clean"],
            parsed["category"], parsed["direction"],
            row_hash, parsed["created_at"],
            parsed["date_raw"], parsed["date_iso"],
            sms_hash, parsed["sms_raw"],
        ))
        conn.commit()
        new_id = cur.lastrowid
        conn.close()
        return {
            "inserted": True, "id": new_id,
            "merchant_clean": parsed["merchant_clean"],
            "category": parsed["category"],
            "direction": parsed["direction"],
            "amount": parsed["amount"],
            "currency": parsed["currency"],
        }
    except sqlite3.IntegrityError:
        conn.close()
        return {"inserted": False, "reason": "integrity_error"}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/expenses")
def list_expenses(
    user_id: str = Query("pedro"),
    limit: int = Query(500, ge=1, le=2000),
):
    conn = db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT id, user_id, sms, amount, currency, merchant_raw, merchant_clean,
               category, direction, created_at, date_iso
        FROM expenses
        WHERE user_id=?
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
    """, (user_id, limit))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "expenses": rows}


@app.post("/delete")
async def delete_expense(payload: dict, api_key: str = Query(None)):
    check_key(api_key or payload.get("api_key") or payload.get("key"))
    user_id = payload.get("userid") or payload.get("user_id")
    row_id  = payload.get("id")
    if not user_id or row_id is None:
        raise HTTPException(status_code=400, detail="Missing userid or id")
    try:
        row_id = int(row_id)
    except Exception:
        raise HTTPException(status_code=400, detail="id must be int")
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM expenses WHERE id=? AND user_id=?", (row_id, user_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Row not found")
        row_dict   = dict(row)
        deleted_at = datetime.now(timezone.utc).isoformat()
        cur.execute(
            "INSERT INTO deleted_expenses(user_id, original_id, row_json, deleted_at) VALUES(?,?,?,?)",
            (user_id, row_id, json.dumps(row_dict, ensure_ascii=False), deleted_at)
        )
        cur.execute("DELETE FROM expenses WHERE id=? AND user_id=?", (row_id, user_id))
        conn.commit()
        return {"ok": True, "deleted_id": row_id, "deleted_at": deleted_at}
    finally:
        conn.close()


@app.post("/undo_delete")
async def undo_delete(payload: dict, api_key: str = Query(None)):
    check_key(api_key or payload.get("api_key") or payload.get("key"))
    user_id = payload.get("userid") or payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing userid")
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, original_id, row_json, deleted_at FROM deleted_expenses WHERE user_id=? ORDER BY id DESC LIMIT 1",
            (user_id,)
        )
        d = cur.fetchone()
        if not d:
            raise HTTPException(status_code=404, detail="Nothing to undo")
        deleted_at = datetime.fromisoformat(d["deleted_at"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - deleted_at).total_seconds()
        if age > 30:
            raise HTTPException(status_code=400, detail="Undo window expired (>30s)")
        row = json.loads(d["row_json"])
        original_id = row.get("id")
        cols = [k for k in row.keys() if k != "id"]
        vals = [row[k] for k in cols]
        try:
            cur.execute(
                f'INSERT INTO expenses (id, {",".join(cols)}) VALUES (?,{",".join(["?"]*len(cols))})',
                [original_id] + vals
            )
            restored_id = original_id
        except Exception:
            cur.execute(
                f'INSERT INTO expenses ({",".join(cols)}) VALUES ({",".join(["?"]*len(cols))})',
                vals
            )
            restored_id = cur.lastrowid
        cur.execute("DELETE FROM deleted_expenses WHERE id=?", (d["id"],))
        conn.commit()
        return {"ok": True, "restored_id": restored_id}
    finally:
        conn.close()


@app.post("/update_field")
async def update_field(payload: dict, api_key: str = Query(None)):
    check_key(api_key or payload.get("api_key"))
    user_id = payload.get("userid") or payload.get("user_id")
    row_id  = payload.get("id")
    field   = (payload.get("field") or "").strip()
    value   = payload.get("value")
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing userid")
    if row_id is None:
        raise HTTPException(status_code=400, detail="Missing id")
    try:
        row_id = int(row_id)
    except Exception:
        raise HTTPException(status_code=400, detail="id must be int")
    allowed = {"merchant_clean", "category", "currency", "amount", "direction"}
    if field not in allowed:
        raise HTTPException(status_code=400, detail=f"Field not allowed: {field}")
    if field == "category":
        value = (str(value).strip().lower() if value else "other") or "other"
    elif field == "currency":
        value = str(value).strip().upper() if value else None
    elif field == "amount":
        try:
            value = float(str(value).replace(",", ""))
        except Exception:
            raise HTTPException(status_code=400, detail="amount must be a number")
    elif field == "merchant_clean":
        value = str(value).strip() if value is not None else ""
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(expenses)")
        db_cols = {r[1] for r in cur.fetchall()}
        if field not in db_cols:
            raise HTTPException(status_code=400, detail=f"Column not in DB: {field}")
        cur.execute(
            f"UPDATE expenses SET {field}=? WHERE id=? AND user_id=?",
            (value, row_id, user_id)
        )
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
        cur.execute(
            "SELECT id, user_id, amount, currency, merchant_clean, category, created_at, direction FROM expenses WHERE id=? AND user_id=?",
            (row_id, user_id)
        )
        row = cur.fetchone()
        return {"ok": True, "id": row_id, "field": field, "value": value, "row": dict(row) if row else None}
    finally:
        conn.close()


@app.get("/rules")
def list_rules(user_id: str = Query("pedro")):
    conn = db()
    cur  = conn.cursor()
    cur.execute(
        "SELECT * FROM rules WHERE user_id=? ORDER BY priority ASC, id ASC",
        (user_id,)
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "rules": rows}


@app.delete("/rules/{rule_id}")
async def delete_rule(rule_id: int, user_id: str = Query("pedro"), api_key: str = Query(None)):
    check_key(api_key)
    conn = db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM rules WHERE id=? AND user_id=?", (rule_id, user_id))
        conn.commit()
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Rule not found")
        return {"ok": True, "deleted_id": rule_id}
    finally:
        conn.close()


@app.post("/rules")
def create_rule(rule: RuleCreate):
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO rules (user_id, enabled, priority, match_field, match_type,
                           pattern, set_category, set_merchant_clean, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        rule.user_id, 1 if rule.enabled else 0, rule.priority,
        rule.match_field, rule.match_type, rule.pattern,
        rule.set_category, rule.set_merchant_clean, now,
    ))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"created": True, "id": new_id}


@app.post("/add_rule")
async def add_rule(req: Request):
    api_key = req.query_params.get("api_key")
    check_key(api_key)
    body     = await req.json()
    user_id  = body.get("userid") or body.get("user_id")
    merchant = (body.get("merchant") or body.get("pattern") or "").strip()
    category = (body.get("category") or "").strip().lower()
    match_type = (body.get("match_mode") or body.get("match_type") or "contains").strip().lower()
    if not user_id or not merchant or not category:
        return JSONResponse({"ok": False, "error": "userid, merchant, category required"}, status_code=400)
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO rules (user_id, enabled, priority, match_field, match_type,
                           pattern, set_category, created_at)
        VALUES (?,1,100,'merchant_clean',?,?,?,?)
    """, (user_id, match_type, merchant, category, now))
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return {"ok": True, "id": rid, "merchant": merchant, "category": category}


@app.get("/summary")
def summary(user_id: str = Query("pedro")):
    conn = db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT currency, direction, SUM(amount) as total, COUNT(*) as count
        FROM expenses WHERE user_id=?
        GROUP BY currency, direction
    """, (user_id,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"summary": rows}


# ======================================================
# DEBUG ENDPOINTS
# ======================================================
@app.get("/debug/db")
def debug_db():
    conn = db()
    cur  = conn.cursor()
    cur.execute("PRAGMA table_info(expenses)")
    columns = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
    cur.execute("SELECT user_id, COUNT(*) as n FROM expenses GROUP BY user_id ORDER BY n DESC")
    users = [{"user_id": r[0], "count": r[1]} for r in cur.fetchall()]
    conn.close()
    return {"db_path": DB_PATH, "columns": columns, "users": users}


@app.post("/debug/recategorize")
async def recategorize_all(api_key: str = Query(None)):
    check_key(api_key)
    conn = db()
    cur  = conn.cursor()
    cur.execute("SELECT id, merchant_clean, merchant_raw, sms FROM expenses")
    rows = cur.fetchall()
    updated = 0
    for row in rows:
        merchant = row["merchant_clean"] or row["merchant_raw"] or ""
        sms      = row["sms"] or ""
        new_cat  = categorize(merchant, sms)
        cur.execute("UPDATE expenses SET category=? WHERE id=?", (new_cat, row["id"]))
        updated += 1
    conn.commit()
    conn.close()
    return {"ok": True, "recategorized": updated}


@app.post("/debug/reclean_merchants")
async def reclean_merchants(api_key: str = Query(None)):
    check_key(api_key)
    conn = db()
    cur  = conn.cursor()
    cur.execute("SELECT id, merchant_raw FROM expenses WHERE merchant_raw IS NOT NULL AND merchant_raw != ''")
    rows = cur.fetchall()
    updated = 0
    for row in rows:
        cleaned = clean_merchant(row["merchant_raw"])
        cur.execute("UPDATE expenses SET merchant_clean=? WHERE id=?", (cleaned, row["id"]))
        updated += 1
    conn.commit()
    conn.close()
    return {"ok": True, "recleaned": updated}
