import os
import re
import sqlite3
import hashlib
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

    # Category (basic heuristic)
    cat = "other"
    merch_l = merchant_clean.lower()
    if any(k in merch_l for k in ["starbucks", "cafe", "coffee"]):
        cat = "coffee"
    elif any(k in merch_l for k in ["talabat", "pick", "restaurant", "burger", "pizza"]):
        cat = "food"

    return {
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
        SELECT id, user_id, amount, currency, merchant_clean, category, created_at
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
