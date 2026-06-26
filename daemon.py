import os
import time
import sqlite3
import requests
import schedule
from datetime import datetime, timedelta

# --- CONFIGURATION ---
WEBHOOK_URL = os.environ.get(
    "DISCORD_WEBHOOK",
    "https://discord.com/api/webhooks/1520195532432085104/RxlKK7bcFsqbJzB91uFZMMAqTR25msj2zfZ2ImSeE-41r8nJu5m1FO__s1oRk5zeuvdV"
)

SA_DATA_API = "https://data.sanantonio.gov/api/3/action/datastore_search_sql"
# Paste the 32-character Resource ID from the SA Data Portal "Data API" button here
RESOURCE_ID = "20eb6d22-7eac-425a-85c1-fdb365fd3cd7"

# A ZIP's daily call count must exceed its historical average by this factor to flag
SPIKE_MULTIPLIER = 1.5
# Minimum absolute calls before a ZIP is even considered (filters noise in low-traffic ZIPs)
MIN_ABSOLUTE_THRESHOLD = 15


def initialize_database():
    """Creates the local SQLite ledger that stores per-ZIP rolling baselines."""
    with sqlite3.connect("baselines.db") as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS zip_metrics (
                zipcode     TEXT NOT NULL,
                reason      TEXT NOT NULL,
                total_calls INTEGER NOT NULL DEFAULT 0,
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

    # 1. Pull yesterday's call counts grouped by ZIP and reason
    sql = (
        f'SELECT "ZIPCODE", "REASON", COUNT(*) as call_count '
        f'FROM "{RESOURCE_ID}" '
        f"WHERE \"CREATEDDATE\" LIKE '{yesterday}%' "
        f'GROUP BY "ZIPCODE", "REASON"'
    )

    try:
        resp = requests.get(SA_DATA_API, params={"sql": sql}, timeout=30)
        resp.raise_for_status()
        records = resp.json().get("result", {}).get("records", [])
    except Exception as exc:
        print(f"API fetch failed: {exc}")
        return

    if not records:
        print("No records returned for yesterday. Returning to sleep.")
        return

    # 2. Delta check against stored baselines; update baselines afterward
    anomalies = []
    with sqlite3.connect("baselines.db") as conn:
        for row in records:
            zipcode = str(row.get("ZIPCODE", "UNKNOWN")).strip()
            reason = str(row.get("REASON", "UNKNOWN")).strip()
            count = int(row.get("call_count", 0))

            baseline_row = conn.execute(
                "SELECT daily_average, days_tracked FROM zip_metrics WHERE zipcode=? AND reason=?",
                (zipcode, reason),
            ).fetchone()

            if baseline_row and baseline_row[1] >= 7:
                # Only flag once we have a week of history to compare against
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
                # No history yet — flag anything above the hard floor during bootstrap
                anomalies.append({
                    "zipcode": zipcode,
                    "reason": reason,
                    "count": count,
                    "avg": None,
                    "pct": None,
                })

            _update_baseline(conn, zipcode, reason, count)
        conn.commit()

    # 3. Build and dispatch the Discord payload
    if not anomalies:
        print("No localized anomalies detected. Returning to sleep.")
        return

    lines = [f"### \U0001f6a8 311 Municipal Anomaly Report", f"**Date:** {yesterday}", ""]
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
