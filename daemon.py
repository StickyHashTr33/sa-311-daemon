import os
import time
import sqlite3
import requests
import schedule
from datetime import datetime, timedelta

# --- CONFIGURATION ---
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")
if not WEBHOOK_URL:
    raise RuntimeError("DISCORD_WEBHOOK environment variable is not set. Refusing to start.")

SA_Data_API = "https://data.sanantonio.gov/api/3/action/datastore_search_sql"
RESOURCE_ID = "20eb6d22-7eac-425a-85c1-fdb365fd3cd7"

SPIKE_MULTIPLIER = 1.5
MIN_ABSOLUTE_THRESHOLD = 15

DB_PATH = os.environ.get("DB_PATH", "data/baselines.db")


def initialize_database():
    """Creates the local SQLite ledger that stores per-ZIP rolling baselines."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS zip_metrics (
                zipcode      TEXT NOT NULL,
                reason       TEXT NOT NULL,
                total_calls  INTEGER NOT NULL DEFAULT 0,
                days_tracked INTEGER NOT NULL DEFAULT 0,
                daily_average REAL NOT NULL DEFAULT 0.0,
                PRIMARY KEY (zipcode, reason)
            )
        """)
        conn.commit()


def _update_baseline(conn, zipcode: str, reason: str, yesterday_count: int):
    """Increments the rolling average for a (zipcode, reason) pair."""
    row = conn.execute(
        "SELECT total_calls, days_tracked FROM zip_metrics WHERE zipcode=? AND reason=?",
        (zipcode, reason),
    ).fetchone()

    if row:
        total = row[0] + yesterday_count
        days = row[1] + 1
        conn.execute(
            """UPDATE zip_metrics
               SET total_calls=?, days_tracked=?, daily_average=?
               WHERE zipcode=? AND reason=?""",
            (total, days, total / days, zipcode, reason),
        )
    else:
        conn.execute(
            """INSERT INTO zip_metrics (zipcode, reason, total_calls, days_tracked, daily_average)
               VALUES (?, ?, ?, 1, ?)""",
            (zipcode, reason, yesterday_count, float(yesterday_count)),
        )


def fetch_and_analyze():
    """Fetches yesterday's 311 data, runs the delta check, and fires Discord if anomalies exist."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[{datetime.now().isoformat(timespec='seconds')}] "
          f"Waking up — executing 4:20 AM delta check for {yesterday}...")

    sql = (
        f'SELECT "ZIPCODE", "REASON", COUNT(*) as call_count '
        f'FROM "{RESOURCE_ID}" '
        f"WHERE \"CREATEDDATE\" LIKE '{yesterday}%' "
        f'GROUP BY "ZIPCODE", "REASON"'
    )

    try:
        resp = requests.get(SA_Data_API, params={"sql": sql}, timeout=30)
        resp.raise_for_status()
        records = resp.json().get("result", {}).get("records", [])
    except Exception as exc:
        print(f"API fetch failed: {exc}")
        return

    if not records:
        print("No records returned for yesterday. Returning to sleep.")
        return

    anomalies = []
    with sqlite3.connect(DB_PATH) as conn:
        for row in records:
            zipcode = str(row.get("ZIPCODE", "UNKNOWN")).strip()
            reason = str(row.get("REASON", "UNKNOWN")).strip()
            count = int(row.get("call_count", 0))

            baseline_row = conn.execute(
                "SELECT daily_average, days_tracked FROM zip_metrics WHERE zipcode=? AND reason=?",
                (zipcode, reason),
            ).fetchone()

            if baseline_row and baseline_row[1] >= 7:
                avg = baseline_row[0]
                if count >= MIN_ABSOLUTE_THRESHOLD and count > avg * SPIKE_MULTIPLIER:
                    anomalies.append({
                        "zipcode": zipcode,
                        "reason": reason,
                        "count": count,
                        "avg": round(avg, 1),
                        "pct": round((count / avg - 1) * 100),
                    })
            elif count >= MIN_ABSOLUTE_THRESHOLD:
                anomalies.append({
                    "zipcode": zipcode,
                    "reason": reason,
                    "count": count,
                    "avg": None,
                    "pct": None,
                })

            _update_baseline(conn, zipcode, reason, count)
        conn.commit()

    if not anomalies:
        print("No localized anomalies detected. Returning to sleep.")
        return

    lines = ["### \U0001f6a8 311 Municipal Anomaly Report", f"**Date:** {yesterday}", ""]
    for a in sorted(anomalies, key=lambda x: x["count"], reverse=True):
        if a["pct"] is not None:
            lines.append(
                f"* **ZIP {a['zipcode']}** — {a['count']} calls re `{a['reason']}` "
                f"(+{a['pct']}% above {a['avg']}/day avg)"
            )
        else:
            lines.append(
                f"* **ZIP {a['zipcode']}** — {a['count']} calls re `{a['reason']}` *(baseline building)*"
            )
    lines.append("")
    lines.append("*System returning to sleep mode.*")

    payload = {"content": "\n".join(lines)}

    try:
        r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code == 204:
            print(f"Payload delivered — {len(anomalies)} anomaly(s) reported.")
        else:
            print(f"Webhook delivery failed. HTTP {r.status_code}: {r.text}")
    except Exception as exc:
        print(f"Webhook request error: {exc}")


if __name__ == "__main__":
    initialize_database()
    print("Daemon initialized. Standing by for the autonomous 4:20 AM loop.")

    schedule.every().day.at("04:20").do(fetch_and_analyze)

    while True:
        schedule.run_pending()
        time.sleep(60)