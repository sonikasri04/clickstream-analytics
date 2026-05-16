CREATE TABLE page_views_1min (
    window_start     TIMESTAMPTZ NOT NULL,
    page_url         TEXT NOT NULL,
    view_count       BIGINT,
    unique_users     BIGINT,
    avg_duration_ms  DOUBLE PRECISION,
    PRIMARY KEY (window_start, page_url)
);

CREATE TABLE active_sessions_5min (
    window_start   TIMESTAMPTZ NOT NULL,
    session_count  BIGINT,
    PRIMARY KEY (window_start)
);

CREATE TABLE funnel_metrics (
    window_start  TIMESTAMPTZ NOT NULL,
    step          TEXT NOT NULL,
    event_count   BIGINT,
    PRIMARY KEY (window_start, step)
);