# Real-Time Clickstream Analytics Platform

A production-grade streaming analytics pipeline that processes website clickstream events in real time from raw user interactions to live dashboards in under 2 seconds.

---

## Architecture

```
Python Simulator
     │
     ▼  (500 events/sec)
Apache Kafka ──► clickstream-raw topic (3 partitions, keyed by user_id)
     │
     ▼
Faust Stream Processor
  ├── 60s tumbling windows  → page views per URL
  ├── 5min sliding windows  → active session counts
  ├── funnel aggregation    → conversion step counts
  └── watermarks (30s late data tolerance)
     │
     ├──► PostgreSQL  (durable aggregated metrics)
     └──► Redis       (hot-path counters: top pages)
               │
               ▼
           Grafana (auto-refresh every 5s)
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Event generation | Python + Faker | Realistic clickstream simulation |
| Message broker | Apache Kafka 7.5 | Durable, partitioned event streaming |
| Stream processing | Faust (Kafka Streams for Python) | Windowed aggregations, stateful processing |
| Operational store | Redis 7 | Sub-millisecond hot counters |
| Analytical store | PostgreSQL 15 | Queryable aggregated metrics |
| Visualization | Grafana 10 | Live dashboards, auto-provisioned |
| Infrastructure | Docker Compose | Single-command local deployment |

---

## Event Schema

Each clickstream event produced to Kafka looks like:

```json
{
  "event_id":    "uuid",
  "user_id":     "uuid",
  "session_id":  "uuid",
  "event_type":  "page_view | click | scroll | form_submit",
  "page_url":    "/products/detail",
  "referrer":    "google | direct | twitter | email",
  "device":      "desktop | mobile | tablet",
  "country":     "US",
  "element_id":  "btn-cta",
  "duration_ms": 3421,
  "timestamp":   "2026-05-15T20:00:00+00:00"
}
```

---

## Metrics Computed

| Metric | Window | Storage |
|---|---|---|
| Page views per URL | 1-min tumbling | PostgreSQL |
| Unique users per URL | 1-min tumbling | PostgreSQL |
| Avg session duration | 1-min tumbling | PostgreSQL |
| Active sessions | 5-min sliding | PostgreSQL |
| Funnel step counts | 1-min tumbling | PostgreSQL |
| Top pages (hot) | Running total | Redis sorted set |

---

## Project Structure

```
clickstream-platform/
├── docker-compose.yml
├── simulator/
│   ├── producer.py          # Kafka event producer
│   └── requirements.txt
├── flink_job/
│   ├── stream_processor.py  # Faust streaming job
│   └── requirements.txt
├── postgres/
│   └── init.sql             # Schema definitions
└── grafana/
    ├── dashboards/
    │   └── clickstream.json # Auto-provisioned dashboard
    └── provisioning/
        ├── datasources/
        │   └── postgres.yml
        └── dashboards/
            └── dashboard.yml
```

---

## Key Streaming Concepts Demonstrated

- **Windowed aggregations** - tumbling (non-overlapping) and sliding (overlapping) windows
- **Late data handling** - 30-second watermark tolerance for out-of-order events
- **Dual-sink pattern** - PostgreSQL for analytics, Redis for operational hot-path
- **Partition keying** - events keyed by `user_id` for session locality
- **Exactly-once semantics** - via Faust consumer group offset management
- **Stateful processing** - session tracking across events

---
