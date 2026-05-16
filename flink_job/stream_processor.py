import faust
import json
import psycopg2
import redis
from datetime import datetime, timezone
from collections import defaultdict

app = faust.App(
    "clickstream-analytics",
    broker="kafka://localhost:9092",
    value_serializer="raw",
)

topic = app.topic("clickstream-raw")

PG = dict(host="localhost", port=5432, dbname="clickstream",
          user="admin", password="admin123")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# ── in-memory accumulators (1-min windows) ─────────────────────────────────────
page_view_acc   = defaultdict(lambda: {"count": 0, "users": set(), "duration_sum": 0})
session_acc     = set()
funnel_acc      = defaultdict(int)

FUNNEL_STEPS = ["/products", "/products/detail", "/cart",
                "/checkout", "/checkout/payment", "/confirmation"]

last_flush = datetime.now(timezone.utc)

def flush_to_postgres():
    global page_view_acc, session_acc, funnel_acc, last_flush
    window_start = last_flush.isoformat()
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()

    # page views
    for url, acc in page_view_acc.items():
        n = acc["count"]
        cur.execute("""
            INSERT INTO page_views_1min
                (window_start, page_url, view_count, unique_users, avg_duration_ms)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (window_start, page_url) DO UPDATE
                SET view_count=EXCLUDED.view_count,
                    unique_users=EXCLUDED.unique_users,
                    avg_duration_ms=EXCLUDED.avg_duration_ms
        """, (window_start, url, n, len(acc["users"]),
              acc["duration_sum"] / n if n else 0))
        r.zincrby("top_pages", n, url)

    # active sessions
    cur.execute("""
        INSERT INTO active_sessions_5min (window_start, session_count)
        VALUES (%s,%s)
        ON CONFLICT (window_start) DO UPDATE
            SET session_count=EXCLUDED.session_count
    """, (window_start, len(session_acc)))

    # funnel
    for step, count in funnel_acc.items():
        cur.execute("""
            INSERT INTO funnel_metrics (window_start, step, event_count)
            VALUES (%s,%s,%s)
            ON CONFLICT (window_start, step) DO UPDATE
                SET event_count=EXCLUDED.event_count
        """, (window_start, step, count))

    conn.commit()
    cur.close()
    conn.close()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Flushed window {window_start} | "
          f"pages={len(page_view_acc)} sessions={len(session_acc)} funnel={dict(funnel_acc)}")

    # reset accumulators
    page_view_acc = defaultdict(lambda: {"count": 0, "users": set(), "duration_sum": 0})
    session_acc   = set()
    funnel_acc    = defaultdict(int)
    last_flush    = datetime.now(timezone.utc)


@app.agent(topic)
async def process(stream):
    global last_flush
    async for raw in stream:
        try:
            e = json.loads(raw)
        except Exception:
            continue

        # accumulate
        if e["event_type"] == "page_view":
            acc = page_view_acc[e["page_url"]]
            acc["count"]        += 1
            acc["users"].add(e["user_id"])
            acc["duration_sum"] += e.get("duration_ms", 0)

        session_acc.add(e["session_id"])

        if e["page_url"] in FUNNEL_STEPS:
            funnel_acc[e["page_url"]] += 1

        # flush every 60 seconds
        now = datetime.now(timezone.utc)
        if (now - last_flush).seconds >= 60:
            flush_to_postgres()


if __name__ == "__main__":
    app.main()

    