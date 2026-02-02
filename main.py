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

# DB persistente en Render (Disk montado en /var/data)
DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")

app = FastAPI(title="Registro Gastos API")

# Static
app.mount("/static", StaticFiles(directory="static"), name="static")

# ======================================================
# HELPERS
# ======================================================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def require_api_key(api_key: str):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

# ======================================================
# DB INIT + MIGRATION
# ======================================================
def init_db():
    conn = db()
    cur = conn.cursor()

    # 1) Crear tabla base si no existe (mínimo viable)
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
            hash TEXT,
            created_at TEXT
        )
        """
    )

    # 2) Migraciones: añadir columnas si faltan
    cur.execute("PRAGMA table_info(expenses)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        nonlocal cols
        if name not in cols:
            cur.execute(f"ALTER TABLE expenses ADD COLUMN {name} {coltype}")
            cols.add(name)

    # Columnas extendidas que tu app usa
    add_col("date_raw", "TEXT")
    add_col("date_iso", "TEXT")
    add_col("sms_hash", "TEXT")   # UNIQUE index después
    add_col("sms_raw", "TEXT")

    # 3) Índices (después de asegurar columnas)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_expenses_hash ON expenses(hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_user ON expenses(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_created ON expenses(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_currency ON expenses(currency)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category)")

    # sms_hash puede existir pero sin índice aún
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_expenses_sms_hash ON expenses(sms_hash)")

    # rules table (crear + migrar)
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

    # Migraciones para rules (si la tabla ya existía con otro schema)
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

    # Rellenar user_id nulo para reglas antiguas (por defecto pedro)
    cur.execute("UPDATE rules SET user_id = 'pedro' WHERE user_id IS NULL OR TRIM(user_id) = ''")

    # Índices rules
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_user ON rules(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_enabled ON rules(enabled)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_priority ON rules(priority)")


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
    user_id: str
    pattern: str
    match_field: str = "merchant"   # merchant | sms
    match_type: str = "contains"    # contains | regex
    priority: int = 100
    enabled: bool = True
    set_category: Optional[str] = None
    set_merchant_clean: Optional[str] = None

class RulePatch(BaseModel):
    pattern: Optional[str] = None
    match_field: Optional[str] = None
    match_type: Optional[str] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None
    set_category: Optional[str] = None
    set_merchant_clean: Optional[str] = None

class ExpensePatch(BaseModel):
    amount: Optional[float] = None
    currency: Optional[str] = None
    merchant_clean: Optional[str] = None
    category: Optional[str] = None
    created_at: Optional[str] = None

# ======================================================
# PARSER (robusto para KWD/USD/EUR y "from/for" opcional)
# ======================================================
AMT_RE = re.compile(r"\b([A-Z]{3})\s*([0-9][0-9,]*\.?[0-9]*)\b")
DT_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\b")

def parse_sms(sms: str) -> Dict[str, Any]:
    raw = sms or ""
    sms_n = norm_spaces(raw)

    # currency + amount
    currency = None
    amount = None
    m = AMT_RE.search(sms_n)
    if m:
        currency = m.group(1)
        amt_txt = m.group(2).replace(",", "")
        try:
            amount = float(amt_txt)
        except:
            amount = None

    # date
    created_at = now_iso_utc()
    date_raw = None
    date_iso = None
    mdt = DT_RE.search(sms_n)
    if mdt:
        date_raw = f"{mdt.group(1)} {mdt.group(2)}"
        try:
            dt = datetime.fromisoformat(f"{mdt.group(1)}T{mdt.group(2)}+00:00")
            created_at = dt.isoformat()
            date_iso = dt.isoformat()
        except:
            pass

    # merchant extraction:
    # patterns like:
    # "debited with KWD 3.000 from PICK DAILY PICKS , ALSALMIYA on 2026-..."
    # "debited with KWD 3.000 for WAMD Service on 2026-..."
    # "debited with USD 1.00 on 2026-..." (no merchant)
    merchant_raw = ""
    mm = re.search(r"\b(?:from|for)\s+(.+?)\s+on\s+20\d{2}-\d{2}-\d{2}\b", sms_n, flags=re.IGNORECASE)
    if mm:
        merchant_raw = mm.group(1).strip(" .,-")

    merchant_clean = norm_spaces(merchant_raw)
    if merchant_clean:
        merchant_clean = merchant_clean.strip(" ,.-")

    category = "other"
    # super básico: si no parsea bien, lo marcas como other (se puede mejorar con reglas)
    return {
        "amount": amount if amount is not None else 0.0,
        "currency": currency or "KWD",
        "merchant_raw": merchant_raw,
        "merchant_clean": merchant_clean,
        "category": category,
        "created_at": created_at,
        "date_raw": date_raw,
        "date_iso": date_iso,
        "sms_norm": sms_n,
        "sms_raw": raw
    }

# ======================================================
# RULES ENGINE v1
# ======================================================
def load_rules(user_id: str) -> List[sqlite3.Row]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM rules
        WHERE user_id = ? AND enabled = 1
        ORDER BY priority ASC, id ASC
        """,
        (user_id,)
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def rule_matches(rule: sqlite3.Row, merchant_clean: str, sms_norm: str) -> bool:
    field = (rule["match_field"] or "merchant").lower()
    mtype = (rule["match_type"] or "contains").lower()
    pattern = (rule["pattern"] or "").strip()
    if not pattern:
        return False

    hay = sms_norm if field == "sms" else (merchant_clean or "")
    hay_l = hay.lower()
    pat_l = pattern.lower()

    if mtype == "contains":
        return pat_l in hay_l
    elif mtype == "regex":
        try:
            return re.search(pattern, hay, flags=re.IGNORECASE) is not None
        except:
            return False
    else:
        return False

def apply_rules(user_id: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    # first-match-wins por campo (estable)
    merchant_clean = parsed.get("merchant_clean", "") or ""
    sms_norm = parsed.get("sms_norm", "") or ""

    fixed_category = None
    fixed_merchant = None

    for r in load_rules(user_id):
        if not rule_matches(r, merchant_clean, sms_norm):
            continue

        if fixed_category is None and r["set_category"]:
            fixed_category = r["set_category"].strip().lower()

        if fixed_merchant is None and r["set_merchant_clean"]:
            fixed_merchant = norm_spaces(r["set_merchant_clean"])

        # si ya fijamos ambos, paramos
        if fixed_category is not None and fixed_merchant is not None:
            break

    if fixed_category is not None:
        parsed["category"] = fixed_category
    if fixed_merchant is not None:
        parsed["merchant_clean"] = fixed_merchant

    return parsed

# ======================================================
# ROUTES
# ======================================================
@app.get("/")
def home():
    # Evita caché rara de HTML
    return FileResponse("static/index.html", headers={"Cache-Control": "no-store"})

@app.get("/health")
def health():
    conn = db()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(expenses)")
    cols = [dict(r) for r in cur.fetchall()]
    cur.execute("PRAGMA index_list(expenses)")
    idxs = []
    for r in cur.fetchall():
        name = r["name"]
        cur.execute(f"PRAGMA index_info({name})")
        idx_cols = [x["name"] for x in cur.fetchall()]
        idxs.append({"name": name, "unique": bool(r["unique"]), "columns": idx_cols})
    conn.close()
    return {"db_path": DB_PATH, "columns": cols, "indexes": idxs}

@app.post("/ingest")
async def ingest(req: IngestRequest, api_key: str = Query(...)):
    require_api_key(api_key)

    user_id = norm_spaces(req.user_id).lower()
    sms_raw = req.sms or ""
    sms_norm = norm_spaces(sms_raw)

    parsed = parse_sms(sms_raw)
    parsed["sms_norm"] = sms_norm
    parsed["sms_raw"] = sms_raw

    # aplica reglas v1
    parsed = apply_rules(user_id, parsed)

    # hashes
    sms_hash = sha256(f"{user_id}|{sms_norm}")
    # hash transaccional (más "estable")
    tx_hash = sha256(
        f"{user_id}|{parsed.get('amount')}|{parsed.get('currency')}|{parsed.get('merchant_clean')}|{parsed.get('created_at')}"
    )

    conn = db()
    cur = conn.cursor()

    # si existe por sms_hash, devolvemos el existente
    cur.execute("SELECT id, user_id, amount, currency, merchant_clean, category, created_at FROM expenses WHERE sms_hash = ?", (sms_hash,))
    row = cur.fetchone()
    if row:
        conn.close()
        return {
            "inserted": False,
            "id": row["id"],
            "expense": {
                "user_id": row["user_id"],
                "amount": row["amount"],
                "currency": row["currency"],
                "merchant_clean": row["merchant_clean"],
                "category": row["category"],
                "created_at": row["created_at"],
            },
            "existing": "sms_hash"
        }

    try:
        cur.execute(
            """
            INSERT INTO expenses (
                user_id, sms, amount, currency,
                merchant_raw, merchant_clean, category,
                hash, created_at, date_raw, date_iso,
                sms_hash, sms_raw
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                sms_norm,
                float(parsed.get("amount", 0.0)),
                parsed.get("currency"),
                parsed.get("merchant_raw"),
                parsed.get("merchant_clean"),
                parsed.get("category"),
                tx_hash,
                parsed.get("created_at"),
                parsed.get("date_raw"),
                parsed.get("date_iso"),
                sms_hash,
                sms_raw
            )
        )
        conn.commit()
        new_id = cur.lastrowid
        conn.close()
        return {
            "inserted": True,
            "id": new_id,
            "hash": tx_hash,
            "sms_hash": sms_hash,
            "expense": {
                "user_id": user_id,
                "amount": float(parsed.get("amount", 0.0)),
                "currency": parsed.get("currency"),
                "merchant_clean": parsed.get("merchant_clean"),
                "category": parsed.get("category"),
                "created_at": parsed.get("created_at"),
            }
        }
    except sqlite3.IntegrityError as e:
        # por si choca por hash unique
        cur.execute("SELECT id FROM expenses WHERE sms_hash = ?", (sms_hash,))
        r2 = cur.fetchone()
        conn.close()
        return {"inserted": False, "id": (r2["id"] if r2 else None), "integrity_error": str(e)}

@app.get("/expenses")
def list_expenses(
    user_id: Optional[str] = None,
    limit: int = 200,
    offset: int = 0
):
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)

    conn = db()
    cur = conn.cursor()

    if user_id:
        uid = norm_spaces(user_id).lower()
        cur.execute("SELECT COUNT(*) AS c FROM expenses WHERE user_id = ?", (uid,))
        count = cur.fetchone()["c"]
        cur.execute(
            """
            SELECT id, user_id, amount, currency, merchant_clean, category, created_at
            FROM expenses
            WHERE user_id = ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (uid, limit, offset)
        )
    else:
        cur.execute("SELECT COUNT(*) AS c FROM expenses")
        count = cur.fetchone()["c"]
        cur.execute(
            """
            SELECT id, user_id, amount, currency, merchant_clean, category, created_at
            FROM expenses
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset)
        )

    rows = cur.fetchall()
    conn.close()

    expenses = []
    for r in rows:
        expenses.append({
            "id": r["id"],
            "user_id": r["user_id"],
            "amount": r["amount"],
            "currency": r["currency"],
            "merchant_clean": r["merchant_clean"],
            "category": r["category"],
            "created_at": r["created_at"],
        })

    return {"count": count, "expenses": expenses}

@app.patch("/expenses/{expense_id}")
async def patch_expense(expense_id: int, payload: ExpensePatch, api_key: str = Query(...)):
    require_api_key(api_key)

    fields = {}
    for k, v in payload.model_dump(exclude_none=True).items():
        fields[k] = v

    if not fields:
        return {"updated": False}

    allowed = {"amount", "currency", "merchant_clean", "category", "created_at"}
    for k in list(fields.keys()):
        if k not in allowed:
            fields.pop(k, None)

    if not fields:
        return {"updated": False}

    sets = ", ".join([f"{k} = ?" for k in fields.keys()])
    params = list(fields.values()) + [expense_id]

    conn = db()
    cur = conn.cursor()
    cur.execute(f"UPDATE expenses SET {sets} WHERE id = ?", params)
    conn.commit()
    updated = cur.rowcount
    conn.close()
    return {"updated": bool(updated)}

@app.delete("/expenses/{expense_id}")
async def delete_expense(expense_id: int, api_key: str = Query(...)):
    require_api_key(api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return {"deleted": bool(deleted)}

# ---------------- RULES API ----------------
@app.get("/rules")
def get_rules(user_id: str):
    uid = norm_spaces(user_id).lower()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, user_id, enabled, priority, match_field, match_type, pattern,
               set_category, set_merchant_clean, created_at
        FROM rules
        WHERE user_id = ?
        ORDER BY priority ASC, id ASC
        """,
        (uid,)
    )
    rows = cur.fetchall()
    conn.close()
    return {
        "count": len(rows),
        "rules": [dict(r) for r in rows]
    }

@app.post("/rules")
def create_rule(rule: RuleCreate, api_key: str = Query(...)):
    require_api_key(api_key)
    uid = norm_spaces(rule.user_id).lower()

    match_field = (rule.match_field or "merchant").lower()
    match_type = (rule.match_type or "contains").lower()
    if match_field not in ("merchant", "sms"):
        raise HTTPException(400, "match_field must be 'merchant' or 'sms'")
    if match_type not in ("contains", "regex"):
        raise HTTPException(400, "match_type must be 'contains' or 'regex'")

    pattern = norm_spaces(rule.pattern)
    if not pattern:
        raise HTTPException(400, "pattern is required")

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO rules (user_id, enabled, priority, match_field, match_type, pattern, set_category, set_merchant_clean, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            uid,
            1 if rule.enabled else 0,
            int(rule.priority),
            match_field,
            match_type,
            pattern,
            (rule.set_category.strip().lower() if rule.set_category else None),
            (norm_spaces(rule.set_merchant_clean) if rule.set_merchant_clean else None),
            now_iso_utc()
        )
    )
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return {"created": True, "id": rid}

@app.patch("/rules/{rule_id}")
def patch_rule(rule_id: int, payload: RulePatch, api_key: str = Query(...)):
    require_api_key(api_key)
    fields = payload.model_dump(exclude_none=True)
    if not fields:
        return {"updated": False}

    if "match_field" in fields:
        mf = (fields["match_field"] or "").lower()
        if mf not in ("merchant", "sms"):
            raise HTTPException(400, "match_field must be 'merchant' or 'sms'")
        fields["match_field"] = mf

    if "match_type" in fields:
        mt = (fields["match_type"] or "").lower()
        if mt not in ("contains", "regex"):
            raise HTTPException(400, "match_type must be 'contains' or 'regex'")
        fields["match_type"] = mt

    if "pattern" in fields:
        fields["pattern"] = norm_spaces(fields["pattern"])
        if not fields["pattern"]:
            raise HTTPException(400, "pattern cannot be empty")

    if "set_category" in fields and fields["set_category"] is not None:
        fields["set_category"] = fields["set_category"].strip().lower()

    if "set_merchant_clean" in fields and fields["set_merchant_clean"] is not None:
        fields["set_merchant_clean"] = norm_spaces(fields["set_merchant_clean"])

    if "enabled" in fields:
        fields["enabled"] = 1 if fields["enabled"] else 0

    sets = ", ".join([f"{k} = ?" for k in fields.keys()])
    params = list(fields.values()) + [rule_id]

    conn = db()
    cur = conn.cursor()
    cur.execute(f"UPDATE rules SET {sets} WHERE id = ?", params)
    conn.commit()
    updated = cur.rowcount
    conn.close()
    return {"updated": bool(updated)}

@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, api_key: str = Query(...)):
    require_api_key(api_key)
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return {"deleted": bool(deleted)}
