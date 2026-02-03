import os
import re
import sqlite3
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ======================================================
# CONFIG
# ======================================================
# Render disk mounted at /var/data
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")


def detect_direction(sms: str) -> str:
    low = (sms or "").lower()
    income_keys = ["credited", "credit", "deposit", "salary", "received", "refund", "transfer received"]
    expense_keys = ["debited", "debit", "paid", "purchase", "card purchase", "withdrawn"]

    # prioridad: si aparece debit -> expense
    if any(k in low for k in expense_keys):
        return "expense"
    if any(k in low for k in income_keys):
        return "income"
    return "expense"


app = FastAPI(title="Registro Gastos API (single-user, no api_key)")

# Serve dashboard
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ======================================================
# DB helpers
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

    # --- expenses table
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
            direction TEXT,
            hash TEXT,
            created_at TEXT,
            date_raw TEXT,
            date_iso TEXT,
            sms_hash TEXT,
            sms_raw TEXT
        )
        """
    )

    # Migrations for expenses: add columns if missing
    cur.execute("PRAGMA table_info(expenses)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        nonlocal cols
        if name not in cols:
            cur.execute(f"ALTER TABLE expenses ADD COLUMN {name} {coltype}")
            cols.add(name)

    add_col("sms", "TEXT")
    add_col("sms_raw", "TEXT")
    add_col("sms_hash", "TEXT")
    add_col("amount", "REAL")
    add_col("currency", "TEXT")
    add_col("merchant_raw", "TEXT")
    add_col("merchant_clean", "TEXT")
    add_col("category", "TEXT")
    add_col("direction", "TEXT")
    add_col("hash", "TEXT")
    add_col("created_at", "TEXT")
    add_col("date_raw", "TEXT")
    add_col("date_iso", "TEXT")

    # Indexes for expenses
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")

    # --- rules table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            match_field TEXT NOT NULL DEFAULT 'merchant',
            match_type TEXT NOT NULL DEFAULT 'contains',
            pattern TEXT NOT NULL,
            set_category TEXT,
            set_merchant_clean TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    # Migrations for rules
    cur.execute("PRAGMA table_info(rules)")
    rcols = {row[1] for row in cur.fetchall()}

    def add_rule_col(name: str, coltype: str):
        nonlocal rcols
        if name not in rcols:
            cur.execute(f"ALTER TABLE rules ADD COLUMN {name} {coltype}")
            rcols.add(name)

    add_rule_col("user_id", "TEXT")
    add_rule_col("enabled", "INTEGER")
    add_rule_col("priority", "INTEGER")
    add_rule_col("match_field", "TEXT")
    add_rule_col("match_type", "TEXT")
    add_rule_col("pattern", "TEXT")
    add_rule_col("set_category", "TEXT")
    add_rule_col("set_merchant_clean", "TEXT")
    add_rule_col("created_at", "TEXT")

    # fill user_id for legacy rows
    cur.execute("UPDATE rules SET user_id = 'pedro' WHERE user_id IS NULL OR TRIM(user_id) = ''")

    # Indexes for rules
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_user ON rules(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_enabled ON rules(enabled)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_priority ON rules(priority)")


    # BACKFILL_DIRECTION_V1
    # Si direction está NULL en filas antiguas, lo rellenamos desde sms
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%credited%'")
    cur.execute("UPDATE expenses SET direction='expense' WHERE (direction IS NULL OR direction='') AND (lower(sms) LIKE '%debited%' OR direction IS NULL OR direction='')")

    # BACKFILL_DIRECTION_ALWAYS_V1
    # Rellenar direction en registros antiguos
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%credit%'")
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%deposit%'")
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%salary%'")
    cur.execute("UPDATE expenses SET direction='income' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%received%'")
    cur.execute("UPDATE expenses SET direction='expense' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%debit%'")
    cur.execute("UPDATE expenses SET direction='expense' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%paid%'")
    cur.execute("UPDATE expenses SET direction='expense' WHERE (direction IS NULL OR direction='') AND lower(sms) LIKE '%purchase%'")

    conn.commit()
    conn.close()


init_db()


# ======================================================
# Models
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
    match_field: str = "merchant"   # merchant
    match_type: str = "contains"    # contains | equals | regex
    pattern: str
    set_category: Optional[str] = None
    set_merchant_clean: Optional[str] = None


# ======================================================
# Parsing
# ======================================================
CURRENCY_RE = r"(KWD|USD|EUR)"
AMOUNT_RE = r"([0-9]+(?:\.[0-9]+)?)"
DATE_RE = r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_sms(sms: str) -> Dict[str, Any]:
    txt = sms.strip()

    # Detect expense/income keyword (for future, not stored yet)
    lower = txt.lower()

    # Currency + amount
    m_amt = re.search(rf"\b{CURRENCY_RE}\s+{AMOUNT_RE}\b", txt)
    currency = m_amt.group(1) if m_amt else "KWD"
    amount = float(m_amt.group(2)) if m_amt else 0.0

    # Merchant: "for XXX" OR "from XXX" (stop at " on YYYY-" if present)
    merchant_raw = ""
    m_merch = re.search(r"\bfor\s+(.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2}\b|$)", txt, flags=re.IGNORECASE)
    if not m_merch:
        m_merch = re.search(r"\bfrom\s+(.+?)(?:\s+on\s+\d{4}-\d{2}-\d{2}\b|$)", txt, flags=re.IGNORECASE)

    if m_merch:
        merchant_raw = m_merch.group(1).strip().strip(",")
    merchant_clean = merchant_raw.strip()

    # Date
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
    else:
        dt = datetime.now(timezone.utc)
        date_iso = dt.isoformat()
        date_raw = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Direction (expense vs income)
    direction = "expense"
    if "credited" in lower:
        direction = "income"
    elif "debited" in lower:
        direction = "expense"

# Category (basic heuristic)
    cat = "other"
    merch_l = merchant_clean.lower()
    if any(k in merch_l for k in ["starbucks", "cafe", "coffee"]):
        cat = "coffee"
    elif any(k in merch_l for k in ["talabat", "pick", "restaurant", "burger", "pizza"]):
        cat = "food"

    return {
        "direction": direction,
        "amount": amount,
        "currency": currency,
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": cat,
        "date_raw": date_raw,
        "date_iso": date_iso,
        "created_at": date_iso,
        "sms_raw": txt,
    }


def apply_rules(user_id: str, merchant: str, current: Dict[str, Any]) -> Dict[str, Any]:
    """Apply enabled rules for the user to parsed fields."""
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM rules
        WHERE enabled = 1 AND user_id = ?
        ORDER BY priority ASC, id ASC
        """,
        (user_id,),
    )
    rules = cur.fetchall()
    conn.close()

    merch_l = (merchant or "").lower()

    for r in rules:
        match_field = (r["match_field"] or "merchant").lower()
        match_type = (r["match_type"] or "contains").lower()
        pattern = (r["pattern"] or "")

        target = merch_l if match_field == "merchant" else ""

        ok = False
        if match_type == "contains":
            ok = pattern.lower() in target
        elif match_type == "equals":
            ok = pattern.lower() == target
        elif match_type == "regex":
            try:
                ok = re.search(pattern, target, flags=re.IGNORECASE) is not None
            except re.error:
                ok = False

        if ok:
            if r["set_category"]:
                current["category"] = r["set_category"]
            if r["set_merchant_clean"]:
                current["merchant_clean"] = r["set_merchant_clean"]

    return current


# ======================================================
# Routes
# ======================================================
@app.get("/")
def root():
    # Serve dashboard if exists
    index_path = os.path.join("static", "index.html")
    if os.path.isfile(index_path):
        return FileResponse(index_path)
    return {"ok": True, "message": "API running. Add /static/index.html for dashboard."}


@app.get("/health")
def health():
    return {"ok": True, "db_path": DB_PATH}


@app.get("/debug/db")
def debug_db():
    conn = db()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(expenses)")
    columns = [{"cid": r[0], "name": r[1], "type": r[2], "notnull": r[3], "dflt": r[4], "pk": r[5]} for r in cur.fetchall()]
    cur.execute("PRAGMA index_list(expenses)")
    idxs = []
    for row in cur.fetchall():
        name = row[1]
        unique = bool(row[2])
        cur.execute(f"PRAGMA index_info({name})")
        cols = [x[2] for x in cur.fetchall()]
        idxs.append({"name": name, "unique": unique, "columns": cols})
    conn.close()
    return {"db_path": DB_PATH, "columns": columns, "indexes": idxs}


@app.post("/ingest")
async def ingest(payload: IngestRequest):
    user_id = payload.user_id.strip()
    sms = payload.sms.strip()

    parsed = parse_sms(sms)
    parsed["user_id"] = user_id
    if not parsed.get("direction"):
        parsed["direction"] = detect_direction(sms)

    # FALLBACK_DIRECTION_V1
    # Por si alguna fila antigua o parser raro deja direction vacío
    if not parsed.get("direction"):
        low = (sms or "").lower()
        if ("credit" in low) or ("credited" in low) or ("deposit" in low) or ("salary" in low) or ("received" in low):
            parsed["direction"] = "income"
        else:
            parsed["direction"] = "expense"

    # Apply rules
    parsed = apply_rules(user_id, parsed.get("merchant_clean", ""), parsed)

    # Hashes
    sms_hash = sha256((sms or "").strip())
    row_hash = sha256(f"{user_id}|{parsed.get('amount')}|{parsed.get('currency')}|{parsed.get('merchant_clean')}|{parsed.get('date_iso')}|{sms_hash}")

    conn = db()
    cur = conn.cursor()

    # If duplicate sms_hash exists, return inserted false + existing row id (if any)
    cur.execute("SELECT id FROM expenses WHERE sms_hash = ?", (sms_hash,))
    existing = cur.fetchone()
    if existing:
        conn.close()
        return {"inserted": False, "id": None, "existing": {"id": existing["id"]}}

    try:
        cur.execute(
            """
            INSERT INTO expenses (
                user_id, sms, amount, currency, merchant_raw, merchant_clean, category,
                hash, created_at, date_raw, date_iso, sms_hash, sms_raw
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                sms,
                parsed.get("amount"),
                parsed.get("currency"),
                parsed.get("merchant_raw"),
                parsed.get("merchant_clean"),
                parsed.get("category"),
                row_hash,
                parsed.get("created_at"),
                parsed.get("date_raw"),
                parsed.get("date_iso"),
                sms_hash,
                parsed.get("sms_raw"),
            ),
        )
        conn.commit()
        new_id = cur.lastrowid
        conn.close()
        return {"inserted": True, "id": new_id}
    except sqlite3.IntegrityError:
        conn.close()
        return {"inserted": False, "id": None}
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/expenses")
def list_expenses(
    user_id: str = Query("pedro"),
    limit: int = Query(50, ge=1, le=500),
):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, user_id, sms, amount, currency, merchant_clean, category, direction, created_at
        FROM expenses
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "expenses": rows}


@app.delete("/expense/{expense_id}")
def delete_expense(expense_id: int, user_id: str = Query("pedro")):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM expenses WHERE id = ? AND user_id = ?", (expense_id, user_id))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return {"deleted": bool(deleted), "id": expense_id}


@app.post("/rules")
def create_rule(rule: RuleCreate):
    now = datetime.now(timezone.utc).isoformat()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO rules (user_id, enabled, priority, match_field, match_type, pattern, set_category, set_merchant_clean, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rule.user_id,
            1 if rule.enabled else 0,
            rule.priority,
            rule.match_field,
            rule.match_type,
            rule.pattern,
            rule.set_category,
            rule.set_merchant_clean,
            now,
        ),
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"created": True, "id": new_id}


@app.get("/rules")
def list_rules(user_id: str = Query("pedro")):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, user_id, enabled, priority, match_field, match_type, pattern, set_category, set_merchant_clean, created_at
        FROM rules
        WHERE user_id = ?
        ORDER BY priority ASC, id ASC
        """,
        (user_id,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"count": len(rows), "rules": rows}


@app.get("/debug/users")
def debug_users():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT user_id, COUNT(*) as n FROM expenses GROUP BY user_id ORDER BY n DESC")
    rows = [{"user_id": r[0], "count": r[1]} for r in cur.fetchall()]
    conn.close()
    return {"db_path": DB_PATH, "users": rows}


@app.post("/debug/backfill_direction")
def debug_backfill_direction(api_key: str = ""):
    if api_key and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Bad api key")

    conn = db()
    cur = conn.cursor()

    # Detecta si existe sms_raw
    cur.execute("PRAGMA table_info(expenses)")
    cols = [r[1] for r in cur.fetchall()]
    sms_col = "sms_raw" if "sms_raw" in cols else "sms"

    # 1) SQL backfill por LIKE (rápido)
    cur.execute(f"""
        UPDATE expenses
        SET direction='income'
        WHERE (direction IS NULL OR direction='')
          AND lower(COALESCE({sms_col}, '')) LIKE '%credit%'
    """)
    income_like = cur.rowcount

    cur.execute(f"""
        UPDATE expenses
        SET direction='expense'
        WHERE (direction IS NULL OR direction='')
          AND lower(COALESCE({sms_col}, '')) LIKE '%debit%'
    """)
    expense_like = cur.rowcount

    # 2) Python fallback para lo que siga vacío
    cur.execute(f"SELECT id, COALESCE({sms_col}, '') FROM expenses WHERE direction IS NULL OR direction=''")
    rows = cur.fetchall()
    py_filled = 0
    for _id, _sms in rows:
        d = detect_direction(_sms)
        cur.execute("UPDATE expenses SET direction=? WHERE id=?", (d, _id))
        py_filled += 1

    conn.commit()
    conn.close()
    return {
        "ok": True,
        "db_path": DB_PATH,
        "sms_col_used": sms_col,
        "income_like_updated": income_like,
        "expense_like_updated": expense_like,
        "python_filled": py_filled
    }


# ======================================================
# UPDATE ENDPOINTS (inline edit from UI)
# ======================================================
from pydantic import BaseModel

class UpdateCategoryReq(BaseModel):
    id: int
    category: str

class UpdateMerchantReq(BaseModel):
    id: int
    merchant: str

def _normalize_category(cat: str) -> str:
    cat = (cat or "").strip()
    if not cat:
        return "other"
    # normalizamos a lowercase + espacios a _
    cat = cat.lower().replace(" ", "_")
    # opcional: recortar chars raros
    cat = "".join(ch for ch in cat if ch.isalnum() or ch in "_-")
    return cat or "other"

@app.post("/update/category")
async def update_category(req: UpdateCategoryReq):
    try:
        cat = _normalize_category(req.category)
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE expenses SET category=? WHERE id=?", (cat, int(req.id)))
        conn.commit()
        updated = cur.rowcount
        conn.close()
        if updated == 0:
            return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
        return {"ok": True, "id": int(req.id), "category": cat}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/update/merchant")
async def update_merchant(req: UpdateMerchantReq):
    try:
        merchant = (req.merchant or "").strip()
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE expenses SET merchant_clean=? WHERE id=?", (merchant, int(req.id)))
        conn.commit()
        updated = cur.rowcount
        conn.close()
        if updated == 0:
            return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
        return {"ok": True, "id": int(req.id), "merchant_clean": merchant}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# ======================================================
# DELETE + UNDO (Trash bin simple)
# ======================================================
def init_trash():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trash (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_at TEXT NOT NULL,
            row_json TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trash_deleted_at ON trash(deleted_at)")
    conn.commit()
    conn.close()

init_trash()

class DeleteRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    id: int
    class Config:
        populate_by_name = True

class UndoRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    class Config:
        populate_by_name = True

# ======================================================
# DELETE + UNDO (Trash bin simple)
# ======================================================
def init_trash():
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trash (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_at TEXT NOT NULL,
            row_json TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trash_deleted_at ON trash(deleted_at)")
    conn.commit()
    conn.close()

init_trash()

class DeleteRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    id: int

    class Config:
        populate_by_name = True

class UndoRequest(BaseModel):
    user_id: str = Field(..., alias="userid")

    class Config:
        populate_by_name = True

@app.post("/delete")
def delete_expense(req: DeleteRequest, request: Request):
    require_key(request)

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM expenses WHERE id=? AND user_id=?", (req.id, req.user_id))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Row not found")

    cols = [c[1] for c in cur.execute("PRAGMA table_info(expenses)").fetchall()]
    data = dict(zip(cols, row))

    cur.execute(
        "INSERT INTO trash(deleted_at, row_json) VALUES (?, ?)",
        (datetime.now(timezone.utc).isoformat(), json.dumps(data, ensure_ascii=False))
    )

    cur.execute("DELETE FROM expenses WHERE id=? AND user_id=?", (req.id, req.user_id))
    conn.commit()
    conn.close()
    return {"ok": True, "deleted_id": req.id}

@app.post("/undo_delete")
def undo_delete(req: UndoRequest, request: Request):
    require_key(request)

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT id, row_json FROM trash ORDER BY id DESC LIMIT 1")
    t = cur.fetchone()
    if not t:
        conn.close()
        return {"ok": False, "restored": False, "reason": "nothing_to_undo"}

    trash_id, row_json = t
    data = json.loads(row_json)

    if data.get("user_id") != req.user_id:
        conn.close()
        raise HTTPException(status_code=403, detail="Cannot undo other user's row")

    existing_cols = [r[1] for r in cur.execute("PRAGMA table_info(expenses)").fetchall()]
    insert_cols = [c for c in existing_cols if c in data and c != "id"]
    insert_vals = [data[c] for c in insert_cols]

    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO expenses({','.join(insert_cols)}) VALUES ({placeholders})"
    cur.execute(sql, insert_vals)

    cur.execute("DELETE FROM trash WHERE id=?", (trash_id,))
    conn.commit()
    conn.close()
    return {"ok": True, "restored": True}

@app.post("/undo_delete")
def undo_delete(req: UndoRequest, request: Request):
    require_key(request)

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT id, row_json FROM trash ORDER BY id DESC LIMIT 1")
    t = cur.fetchone()
    if not t:
        conn.close()
        return {"ok": False, "restored": False, "reason": "nothing_to_undo"}

    trash_id, row_json = t
    data = json.loads(row_json)

    if data.get("user_id") != req.user_id:
        conn.close()
        raise HTTPException(status_code=403, detail="Cannot undo other user's row")

    existing_cols = [r[1] for r in cur.execute("PRAGMA table_info(expenses)").fetchall()]
    insert_cols = [c for c in existing_cols if c in data and c != "id"]
    insert_vals = [data[c] for c in insert_cols]

    placeholders = ",".join(["?"] * len(insert_cols))
    sql = f"INSERT INTO expenses({','.join(insert_cols)}) VALUES ({placeholders})"
    cur.execute(sql, insert_vals)

    cur.execute("DELETE FROM trash WHERE id=?", (trash_id,))
    conn.commit()
    conn.close()
    return {"ok": True, "restored": True}

# ======================================================
# UPDATE FIELD (merchant_clean / category)
# ======================================================
class UpdateFieldRequest(BaseModel):
    user_id: str = Field(..., alias="userid")
    id: int
    field: str
    value: str

    class Config:
        populate_by_name = True

@app.post("/update_field")
def update_field(req: UpdateFieldRequest, api_key: str = Query(None)):
    # ---- API KEY ----
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid api_key")

    # ---- validate field ----
    allowed = {
        "merchant_clean": "TEXT",
        "merchant_raw": "TEXT",
        "category": "TEXT",
        "amount": "REAL",
        "currency": "TEXT",
        "direction": "TEXT",
        "created_at": "TEXT",
        "date_raw": "TEXT",
        "date_iso": "TEXT",
        "sms_raw": "TEXT",
        "sms": "TEXT",
    }
    field = (req.field or "").strip()

    if field not in allowed:
        raise HTTPException(status_code=400, detail=f"Field not allowed: {field}")

    value = req.value

    # normalize some fields
    if field == "category" and value is not None:
        value = str(value).strip().lower() or "other"
    if field == "currency" and value is not None:
        value = str(value).strip().upper()
    if field == "amount" and value is not None:
        try:
            value = float(value)
        except Exception:
            raise HTTPException(status_code=400, detail="amount must be a number")
    if field == "merchant_clean" and value is not None:
        value = str(value).strip()

    try:
        conn = db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Make sure column exists (avoid sqlite errors -> 500)
        cur.execute("PRAGMA table_info(expenses)")
        cols = {row[1] for row in cur.fetchall()}
        if field not in cols:
            raise HTTPException(status_code=400, detail=f"Column not in DB: {field}")

        cur.execute(
            f"UPDATE expenses SET {field} = ? WHERE id = ? AND user_id = ?",
            (value, req.id, req.user_id),
        )
        conn.commit()

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")

        cur.execute("SELECT * FROM expenses WHERE id = ? AND user_id = ?", (req.id, req.user_id))
        row = cur.fetchone()
        return {"ok": True, "id": req.id, "field": field, "value": value, "row": dict(row) if row else None}

    except HTTPException:
        raise
    except sqlite3.Error as e:
        # return a clear error instead of generic 500 with no info
        raise HTTPException(status_code=500, detail=f"sqlite error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

