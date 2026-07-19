

import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import asyncpg

#  CONFIG 
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL     = os.getenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
GROUP_ID         = "edu-analytics"
FLUSH_INTERVAL_S = 30    

TOPICS = [
    "edu.enrollment",
    "edu.course_registration",
    "edu.feedback",
    "edu.onboarding",
    "edu.interaction_log",
    "edu.state_change",
]

#  LOGGING 
logging.basicConfig(
    format="%(asctime)s │ %(levelname)s │ %(name)s │ %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("analytics_consumer")




class AnalyticsState:
    """Holds rolling aggregations for all active metrics."""

    def __init__(self):
        self.prof_ratings: dict[int, dict] = defaultdict(lambda: {"sum": 0, "count": 0})

        self.daily_enrollments: dict[str, int] = defaultdict(int)

        self.daily_course_regs: dict[str, int] = defaultdict(int)

        self.student_last_active: dict[int, str] = {}

        self.command_counts: dict[str, int] = defaultdict(int)

        self.response_times: dict[str, list] = defaultdict(lambda: [0, 0])


        self.dept_interactions: dict[int, int] = defaultdict(int)

        self.daily_onboardings: dict[str, int] = defaultdict(int)

        self.total_events = 0
        self.errors       = 0


#  EVENT HANDLERS

def on_session_enrolled(state: AnalyticsState, payload: dict) -> None:
    today = date.today().isoformat()
    state.daily_enrollments[today] += 1
    sid = payload.get("student_id")
    if sid:
        state.student_last_active[sid] = datetime.utcnow().isoformat()


def on_course_registered(state: AnalyticsState, payload: dict) -> None:
    today = date.today().isoformat()
    state.daily_course_regs[today] += 1
    sid = payload.get("student_id")
    if sid:
        state.student_last_active[sid] = datetime.utcnow().isoformat()


def on_feedback_submitted(state: AnalyticsState, payload: dict) -> None:
    prof_id = payload.get("professor_id")
    rating  = payload.get("rating", 0)
    if prof_id and isinstance(rating, int) and 1 <= rating <= 5:
        state.prof_ratings[prof_id]["sum"]   += rating
        state.prof_ratings[prof_id]["count"] += 1
    sid = payload.get("student_id")
    if sid:
        state.student_last_active[sid] = datetime.utcnow().isoformat()


def on_telegram_linked(state: AnalyticsState, payload: dict) -> None:
    today = date.today().isoformat()
    state.daily_onboardings[today] += 1


def on_bot_interaction(state: AnalyticsState, payload: dict) -> None:
    cmd = payload.get("command", "unknown")
    ms  = payload.get("ms", 0)
    state.command_counts[cmd] += 1
    state.response_times[cmd][0] += ms
    state.response_times[cmd][1] += 1
    sid = payload.get("student_id")
    if sid:
        state.student_last_active[sid] = datetime.utcnow().isoformat()


def on_state_upserted(state: AnalyticsState, payload: dict) -> None:
    sid = payload.get("student_id")
    if sid:
        state.student_last_active[sid] = datetime.utcnow().isoformat()


# ─────────── Dispatch table ────────────────────────────────────────────────
ANALYTICS_HANDLERS = {
    "session_enrolled":   on_session_enrolled,
    "course_registered":  on_course_registered,
    "feedback_submitted": on_feedback_submitted,
    "telegram_linked":    on_telegram_linked,
    "bot_interaction":    on_bot_interaction,
    "state_upserted":     on_state_upserted,
}


# ═══════════════════════════════════════════════════════════════════════════
#  DB FLUSH  — write aggregated metrics to analytics tables
#  These are APPEND-ONLY snapshot tables, not the main transactional schema.
#  Power BI reads from these via Supabase.
# ═══════════════════════════════════════════════════════════════════════════

ANALYTICS_SCHEMA = """
-- Run once before starting the analytics consumer:

CREATE TABLE IF NOT EXISTS analytics_professor_ratings (
    snapshot_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    professor_id   INT         NOT NULL,
    avg_rating     NUMERIC(3,2),
    review_count   INT,
    PRIMARY KEY (snapshot_ts, professor_id)
);

CREATE TABLE IF NOT EXISTS analytics_daily_activity (
    activity_date      DATE        NOT NULL,
    new_enrollments    INT         DEFAULT 0,
    new_course_regs    INT         DEFAULT 0,
    new_onboardings    INT         DEFAULT 0,
    PRIMARY KEY (activity_date)
);

CREATE TABLE IF NOT EXISTS analytics_student_activity (
    student_id      INT         NOT NULL PRIMARY KEY,
    last_active_at  TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_command_stats (
    snapshot_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    command       VARCHAR(50) NOT NULL,
    call_count    INT,
    avg_response_ms NUMERIC(8,2),
    PRIMARY KEY (snapshot_ts, command)
);
"""

async def flush_to_db(pool: asyncpg.Pool, state: AnalyticsState) -> None:
    """Write all pending aggregations to analytics tables and reset accumulators."""
    now = datetime.utcnow()
    logger.info("Flushing analytics snapshot [events=%d]", state.total_events)

    async with pool.acquire() as conn:
        async with conn.transaction():

            # ── 1. Professor rating snapshots ─────────────────────────────
            if state.prof_ratings:
                rows = [
                    (now, prof_id, round(v["sum"] / v["count"], 2), v["count"])
                    for prof_id, v in state.prof_ratings.items()
                    if v["count"] > 0
                ]
                await conn.executemany(
                    """
                    INSERT INTO analytics_professor_ratings
                        (snapshot_ts, professor_id, avg_rating, review_count)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
                logger.info("  ✓ professor_ratings: %d rows", len(rows))

            # ── 2. Daily activity counts ──────────────────────────────────
            all_dates = set(state.daily_enrollments) | set(state.daily_course_regs) | \
                        set(state.daily_onboardings)
            if all_dates:
                day_rows = [
                    (
                        d,
                        state.daily_enrollments.get(d, 0),
                        state.daily_course_regs.get(d, 0),
                        state.daily_onboardings.get(d, 0),
                    )
                    for d in all_dates
                ]
                await conn.executemany(
                    """
                    INSERT INTO analytics_daily_activity
                        (activity_date, new_enrollments, new_course_regs, new_onboardings)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (activity_date) DO UPDATE SET
                        new_enrollments  = analytics_daily_activity.new_enrollments  + EXCLUDED.new_enrollments,
                        new_course_regs  = analytics_daily_activity.new_course_regs  + EXCLUDED.new_course_regs,
                        new_onboardings  = analytics_daily_activity.new_onboardings  + EXCLUDED.new_onboardings
                    """,
                    day_rows,
                )
                logger.info("  ✓ daily_activity: %d dates", len(day_rows))

            # ── 3. Student last-active ────────────────────────────────────
            if state.student_last_active:
                student_rows = [
                    (sid, ts) for sid, ts in state.student_last_active.items()
                ]
                await conn.executemany(
                    """
                    INSERT INTO analytics_student_activity (student_id, last_active_at)
                    VALUES ($1, $2::TIMESTAMPTZ)
                    ON CONFLICT (student_id) DO UPDATE SET
                        last_active_at = GREATEST(
                            analytics_student_activity.last_active_at,
                            EXCLUDED.last_active_at
                        ),
                        updated_at = NOW()
                    """,
                    student_rows,
                )
                logger.info("  ✓ student_activity: %d students", len(student_rows))

            # ── 4. Command stats ──────────────────────────────────────────
            if state.command_counts:
                cmd_rows = []
                for cmd, count in state.command_counts.items():
                    rt = state.response_times[cmd]
                    avg_ms = round(rt[0] / rt[1], 2) if rt[1] > 0 else 0
                    cmd_rows.append((now, cmd, count, avg_ms))
                await conn.executemany(
                    """
                    INSERT INTO analytics_command_stats
                        (snapshot_ts, command, call_count, avg_response_ms)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                    """,
                    cmd_rows,
                )
                logger.info("  ✓ command_stats: %d commands", len(cmd_rows))

    # ── Reset accumulators (keep student_last_active — just overwrite) ───
    state.prof_ratings.clear()
    state.daily_enrollments.clear()
    state.daily_course_regs.clear()
    state.daily_onboardings.clear()
    state.command_counts.clear()
    state.response_times.clear()
    # student_last_active: keep the latest values, cleared after flush
    state.student_last_active.clear()
    logger.info("  ✓ Flush complete")


# ═══════════════════════════════════════════════════════════════════════════
#  REPORTING HELPERS
#  These functions can be called on demand (e.g. via a REST endpoint)
#  to generate on-the-fly reports from the analytics tables.
# ═══════════════════════════════════════════════════════════════════════════

async def report_top_professors(pool: asyncpg.Pool, limit: int = 10) -> list[dict]:
    """Top-rated professors based on latest snapshot."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (r.professor_id)
                   p.professor_name,
                   p.academic_title,
                   d.department_name,
                   r.avg_rating,
                   r.review_count
            FROM   analytics_professor_ratings r
            JOIN   professors   p ON r.professor_id = p.professor_id
            JOIN   departments  d ON p.department_id = d.department_id
            ORDER  BY r.professor_id, r.snapshot_ts DESC, r.avg_rating DESC
            LIMIT  $1
            """,
            limit,
        )
    return [dict(r) for r in rows]


async def report_enrollment_trend(
    pool: asyncpg.Pool, days: int = 30
) -> list[dict]:
    """Daily enrollment trend for the last N days."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT activity_date,
                   new_enrollments,
                   new_course_regs,
                   new_onboardings,
                   SUM(new_enrollments) OVER (ORDER BY activity_date) AS cumulative_enrollments
            FROM   analytics_daily_activity
            WHERE  activity_date >= CURRENT_DATE - ($1::int || ' days')::INTERVAL
            ORDER  BY activity_date
            """,
            days,
        )
    return [dict(r) for r in rows]


async def report_student_engagement(
    pool: asyncpg.Pool, inactive_days: int = 7
) -> dict:
    """Students active in last N days vs inactive."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE last_active_at >= NOW() - ($1::int || ' days')::INTERVAL
                ) AS active_count,
                COUNT(*) FILTER (
                    WHERE last_active_at < NOW() - ($1::int || ' days')::INTERVAL
                    OR last_active_at IS NULL
                ) AS inactive_count,
                COUNT(*) AS total
            FROM students s
            LEFT JOIN analytics_student_activity a ON s.student_id = a.student_id
            WHERE s.is_active = TRUE
            """,
            inactive_days,
        )
    return dict(row)


async def report_command_performance(pool: asyncpg.Pool) -> list[dict]:
    """Latest average response time per bot command."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (command)
                   command, call_count, avg_response_ms
            FROM   analytics_command_stats
            ORDER  BY command, snapshot_ts DESC
            """,
        )
    return [dict(r) for r in rows]


#  MAIN CONSUMER LOOP

async def run_analytics_consumer() -> None:
    pool = await asyncpg.create_pool(
        DATABASE_URL, ssl="require",
        min_size=1, max_size=4,
        max_inactive_connection_lifetime=300,
    )
    logger.info("✅ DB pool ready")

    # ── Kafka consumer 
    from aiokafka import AIOKafkaConsumer
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,          
        auto_commit_interval_ms=5_000,
        max_poll_records=100,
    )

    await consumer.start()
    logger.info(
        "✅ Analytics consumer started | group=%s | topics=%s",
        GROUP_ID, TOPICS,
    )

    state       = AnalyticsState()
    running     = True
    last_flush  = time.monotonic()

    def _stop(*_):
        nonlocal running
        logger.info("Shutdown signal received.")
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    try:
        while running:
            try:
                msg_batch = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000, max_records=100),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                msg_batch = {}

            # Process messages
            for _, messages in msg_batch.items():
                for msg in messages:
                    try:
                        event   = json.loads(msg.value.decode("utf-8"))
                        etype   = event.get("event_type", "")
                        payload = event.get("payload", {})
                        handler = ANALYTICS_HANDLERS.get(etype)
                        if handler:
                            handler(state, payload)
                        state.total_events += 1
                    except Exception as exc:
                        state.errors += 1
                        logger.warning("Analytics parse error: %s", exc)

            # Periodic flush to DB
            if time.monotonic() - last_flush >= FLUSH_INTERVAL_S:
                try:
                    await flush_to_db(pool, state)
                    last_flush = time.monotonic()
                except Exception as exc:
                    logger.error("Flush failed (will retry next cycle): %s", exc)

    finally:
        try:
            if state.total_events > 0:
                await flush_to_db(pool, state)
        except Exception as exc:
            logger.error("Final flush failed: %s", exc)
        await consumer.stop()
        await pool.close()
        logger.info(
            "Analytics consumer shut down. total_events=%d errors=%d",
            state.total_events, state.errors,
        )


if __name__ == "__main__":
    asyncio.run(run_analytics_consumer())
