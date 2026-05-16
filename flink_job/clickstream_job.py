import json
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration
from pyflink.common import Types, Time
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction, AggregateFunction
import psycopg2
import redis

# ── env setup ──────────────────────────────────────────────────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
env.enable_checkpointing(30_000, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_storage_uri("file:///tmp/flink-checkpoints")

# ── kafka source ───────────────────────────────────────────────────────────────
kafka_props = {
    "bootstrap.servers": "kafka:29092",
    "group.id":          "flink-clickstream",
    "auto.offset.reset": "latest",
}

consumer = FlinkKafkaConsumer(
    topics="clickstream-raw",
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props,
)

# Watermark: allow up to 30s late data
consumer.assign_timestamps_and_watermarks(
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(
            lambda event, _: int(
                datetime.fromisoformat(
                    json.loads(event)["timestamp"].replace("Z", "+00:00")
                ).timestamp() * 1000
            )
        )
)

raw_stream = env.add_source(consumer)

# ── parse ──────────────────────────────────────────────────────────────────────
def parse(raw):
    try:
        return json.loads(raw)
    except Exception:
        return None

parsed = raw_stream.map(parse).filter(lambda e: e is not None)

# ── aggregation: page views per 1-min tumbling window ─────────────────────────
class PageViewAgg(AggregateFunction):
    def create_accumulator(self):
        return {"count": 0, "users": set(), "duration_sum": 0}

    def add(self, value, acc):
        acc["count"] += 1
        acc["users"].add(value["user_id"])
        acc["duration_sum"] += value.get("duration_ms", 0)
        return acc

    def get_result(self, acc):
        n = acc["count"]
        return {
            "view_count":      n,
            "unique_users":    len(acc["users"]),
            "avg_duration_ms": acc["duration_sum"] / n if n else 0,
        }

    def merge(self, a, b):
        a["count"]        += b["count"]
        a["users"]        |= b["users"]
        a["duration_sum"] += b["duration_sum"]
        return a


class PageViewProcess(ProcessWindowFunction):
    def process(self, key, ctx, elements):
        agg = list(elements)[0]
        yield {
            "window_start":    datetime.fromtimestamp(
                                   ctx.window().start / 1000, tz=timezone.utc
                               ).isoformat(),
            "page_url":        key,
            "view_count":      agg["view_count"],
            "unique_users":    agg["unique_users"],
            "avg_duration_ms": agg["avg_duration_ms"],
        }


page_views = (
    parsed
    .filter(lambda e: e["event_type"] == "page_view")
    .key_by(lambda e: e["page_url"])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(PageViewAgg(), window_function=PageViewProcess())
)

# ── aggregation: active sessions per 5-min sliding window ─────────────────────
class SessionCountAgg(AggregateFunction):
    def create_accumulator(self):  return set()
    def add(self, v, acc):         acc.add(v["session_id"]); return acc
    def get_result(self, acc):     return len(acc)
    def merge(self, a, b):         return a | b


class SessionProcess(ProcessWindowFunction):
    def process(self, key, ctx, elements):
        yield {
            "window_start":  datetime.fromtimestamp(
                                 ctx.window().start / 1000, tz=timezone.utc
                             ).isoformat(),
            "session_count": list(elements)[0],
        }


active_sessions = (
    parsed
    .key_by(lambda e: "global")
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .aggregate(SessionCountAgg(), window_function=SessionProcess())
)

# ── funnel metrics ─────────────────────────────────────────────────────────────
FUNNEL_STEPS = [
    "/products", "/products/detail", "/cart",
    "/checkout", "/checkout/payment", "/confirmation"
]

class FunnelAgg(AggregateFunction):
    def create_accumulator(self):  return 0
    def add(self, v, acc):         return acc + 1
    def get_result(self, acc):     return acc
    def merge(self, a, b):         return a + b


class FunnelProcess(ProcessWindowFunction):
    def process(self, key, ctx, elements):
        yield {
            "window_start": datetime.fromtimestamp(
                                ctx.window().start / 1000, tz=timezone.utc
                            ).isoformat(),
            "step":         key,
            "event_count":  list(elements)[0],
        }


funnel = (
    parsed
    .filter(lambda e: e["page_url"] in FUNNEL_STEPS)
    .key_by(lambda e: e["page_url"])
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(FunnelAgg(), window_function=FunnelProcess())
)

# ── sinks ──────────────────────────────────────────────────────────────────────
PG = dict(host="postgres", port=5432, dbname="clickstream",
          user="admin", password="admin123")

def sink_page_views(record):
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO page_views_1min
            (window_start, page_url, view_count, unique_users, avg_duration_ms)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (window_start, page_url) DO UPDATE
            SET view_count      = EXCLUDED.view_count,
                unique_users    = EXCLUDED.unique_users,
                avg_duration_ms = EXCLUDED.avg_duration_ms
    """, (record["window_start"], record["page_url"],
          record["view_count"],   record["unique_users"],
          record["avg_duration_ms"]))
    conn.commit(); cur.close(); conn.close()

    # also write hot counter to Redis
    r = redis.Redis(host="redis", port=6379, decode_responses=True)
    r.zincrby("top_pages", record["view_count"], record["page_url"])


def sink_sessions(record):
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO active_sessions_5min (window_start, session_count)
        VALUES (%s,%s)
        ON CONFLICT (window_start) DO UPDATE
            SET session_count = EXCLUDED.session_count
    """, (record["window_start"], record["session_count"]))
    conn.commit(); cur.close(); conn.close()


def sink_funnel(record):
    conn = psycopg2.connect(**PG)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO funnel_metrics (window_start, step, event_count)
        VALUES (%s,%s,%s)
        ON CONFLICT (window_start, step) DO UPDATE
            SET event_count = EXCLUDED.event_count
    """, (record["window_start"], record["step"], record["event_count"]))
    conn.commit(); cur.close(); conn.close()


page_views.map(lambda r: (sink_page_views(r), r)[1])
active_sessions.map(lambda r: (sink_sessions(r), r)[1])
funnel.map(lambda r: (sink_funnel(r), r)[1])

env.execute("clickstream-analytics")