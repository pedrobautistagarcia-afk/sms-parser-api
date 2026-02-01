# subscriptions.py
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from statistics import median
from fastapi import APIRouter, Query

router = APIRouter()

DB_PATH = os.getenv("DB_PATH", "/var/data/gastos.db")


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_iso(dt_str: str) -> datetime:
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.strptime(
            dt_str[:19], "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=timezone.utc)


def _amount_stable(amounts: list[float], tolerance: float = 0.10) -> bool:
    if not amounts:
        return False
    mean_amt = sum(amounts) / len(amounts)
    if mean_amt <= 0:
        return False
    mn, mx = min(amounts), max(amounts)
    return ((mx - mn) / mean_amt) <= tolerance


def _detect_frequency(diffs_days: list[int]) -> str | None:
    if len(diffs_days) < 2:
        return None
    m = median(diffs_days)

    if 27 <= m <= 32:
        return "monthly"
    if 6 <= m <= 8:
        return "weekly"
    return None


def _monthly_equivalent(freq: str, mean_amount: float) -> float:
    if freq == "monthly":
        return mean_amount
    if freq == "weekly":
        return mean_amount * 4.348
    return 0.0


def _confidence(n: int, diffs_days: list[int], amounts: list[float]) -> str:
    if n >= 6 and _amount_stable(amounts, 0.07):
        return "high"
    if n >= 4 and _amount_stable(amounts, 0.10):
        return "medium"
    return "low"


@router.get("/api/subscriptions")
def get_subscriptions(
    user_id: str = Query(...),
    lookback_days: int = Query(365, ge=30, le=3650),
    min_occurrences: int = Query(3, ge=3, le=12),
    amount_tolerance: float = Query(0.10, ge=0.01, le=0.30),
):
    since_dt = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    since_iso = since_dt.isoformat()

    conn = _db()
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT merchant_clean, amount, currency, created_at
        FROM expenses
        WHERE user_id = ?
          AND created_at >= ?
          AND amount > 0
          AND merchant_clean IS NOT NULL
          AND TRIM(merchant_clean) != ''
        ORDER BY merchant_clean ASC, created_at ASC
        """,
        (user_id, since_iso),
    ).fetchall()

    by_merchant = {}
    for r in rows:
        by_merchant.setdefault(r["merchant_clean"], []).append(r)

    subs = []
    monthly_commitment = 0.0

    for merchant, items in by_merchant.items():
        if len(items) < min_occurrences:
            continue

        dates = [_parse_iso(it["created_at"]) for it in items]
        amounts = [float(it["amount"]) for it in items]
        currency = items[-1]["currency"] or "KWD"

        diffs = []
        for i in range(1, len(dates)):
            d = (dates[i] - dates[i - 1]).days
            if d > 0:
                diffs.append(d)

        freq = _detect_frequency(diffs)
        if not freq:
            continue

        if not _amount_stable(amounts, amount_tolerance):
            continue

        mean_amt = sum(amounts) / len(amounts)
        next_date = dates[-1] + timedelta(days=int(round(median(diffs))))
        m_equiv = _monthly_equivalent(freq, mean_amt)

        conf = _confidence(len(items), diffs, amounts)

        subs.append(
            {
                "merchant": merchant,
                "frequency": freq,
                "currency": currency,
                "occurrences": len(items),
                "mean_amount": round(mean_amt, 3),
                "monthly_equivalent": round(m_equiv, 3),
                "last_seen": dates[-1].date().isoformat(),
                "next_expected": next_date.date().isoformat(),
                "confidence": conf,
            }
        )

        monthly_commitment += m_equiv

    subs.sort(key=lambda x: x["monthly_equivalent"], reverse=True)

    return {
        "user_id": user_id,
        "lookback_days": lookback_days,
        "monthly_commitment_estimated": round(monthly_commitment, 3),
        "subscriptions": subs,
    }
