

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import CommitFailedError

#  CONFIG 
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATABASE_URL      = os.getenv("DATABASE_URL", "postgresql://user:pass@host:5432/db")
GROUP_ID          = "edu-db-writers"
DLQ_TOPIC         = "edu.dlq"             
MAX_RETRY         = 3                      
RETRY_BACKOFF_S   = [1, 3, 8]            
COMMIT_INTERVAL_S = 5                      

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
logger = logging.getLogger("db_consumer")

#  SIMPLE METRICS 
_metrics: dict[str, int] = {
    "processed": 0,
    "errors":    0,
    "dlq_sent":  0,
    "retries":   0,
}

def _inc(key: str, n: int = 1) -> None:
    _metrics[key] = _metrics.get(key, 0) + n



async def handle_session_enrolled(conn: asyncpg.Connection, p: dict) -> None:
    """
    Idempotent: ON CONFLICT DO NOTHING prevents double-enroll.
    After insert, refresh the materialized view for this student.
    """
    result = await conn.execute(
        """
        INSERT INTO enrollments (student_id, session_id, semester)
        SELECT $1, $2, semester FROM sessions WHERE session_id = $2
        ON CONFLICT (student_id, session_id) DO NOTHING
        """,
        p["student_id"], p["session_id"],
    )
    inserted = result.endswith("1")
    if inserted:
        try:
            await conn.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_student_schedule"
            )
        except Exception as exc:
            logger.warning("MV refresh failed (non-fatal): %s", exc)
    logger.info(
        "enrollment: student=%s session=%s inserted=%s",
        p["student_id"], p["session_id"], inserted,
    )


async def handle_course_registered(conn: asyncpg.Connection, p: dict) -> None:
    """Idempotent course registration — composite PK prevents duplicates."""
    result = await conn.execute(
        """
        INSERT INTO student_courses (student_id, course_id, semester, academic_year)
        SELECT $1, $2, c.semester, $3
        FROM   courses c
        WHERE  c.course_id = $2 AND c.semester IS NOT NULL
        ON CONFLICT (student_id, course_id, semester, academic_year) DO NOTHING
        """,
        p["student_id"], p["course_id"], p["academic_year"],
    )
    logger.info(
        "course_reg: student=%s course=%s result=%s",
        p["student_id"], p["course_id"], result,
    )


async def handle_feedback_submitted(conn: asyncpg.Connection, p: dict) -> None:
    await conn.execute(
        """
        INSERT INTO feedback (student_id, session_id, professor_id, rating, comments)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (student_id, session_id)
        DO UPDATE SET rating=$4, comments=$5
        """,
        p["student_id"], p["session_id"],
        p["professor_id"], p["rating"], p.get("comment", ""),
    )
    logger.info(
        "feedback: student=%s session=%s rating=%s",
        p["student_id"], p["session_id"], p["rating"],
    )


async def handle_telegram_linked(conn: asyncpg.Connection, p: dict) -> None:

    await conn.execute(
        """
        UPDATE students
        SET telegram_id=$1, telegram_username=$2
        WHERE student_id=$3
          AND (telegram_id IS DISTINCT FROM $1
               OR telegram_username IS DISTINCT FROM $2)
        """,
        p["telegram_id"], p.get("telegram_username", ""), p["student_id"],
    )
    logger.info("onboarding: student=%s tg_id=%s", p["student_id"], p["telegram_id"])


async def handle_bot_interaction(conn: asyncpg.Connection, p: dict) -> None:

    await conn.execute(
        """
        INSERT INTO bot_interaction_logs
            (telegram_chat_id, telegram_username, student_id,
             command, message_received, bot_response, response_time_ms)
        SELECT $1, $2, $3, $4, $5, $6, $7
        WHERE NOT EXISTS (
            SELECT 1 FROM bot_interaction_logs
            WHERE telegram_chat_id=$1
              AND interaction_timestamp > NOW() - INTERVAL '5 seconds'
              AND command=$4
              AND response_time_ms=$7
        )
        """,
        p["chat_id"], p.get("username", ""), p.get("student_id"),
        p.get("command", ""), p.get("msg_in", ""),
        p.get("msg_out", ""), p.get("ms", 0),
    )


async def handle_state_upserted(conn: asyncpg.Connection, p: dict) -> None:
    """FSM state UPSERT — uses existing stored procedure."""
    await conn.execute(
        "SELECT fn_upsert_bot_state($1, $2, $3, $4::jsonb)",
        p["chat_id"], p.get("student_id"), p["state"],
        json.dumps(p.get("data")) if p.get("data") else None,
    )


#  Dispatch table 
HANDLERS = {
    "session_enrolled":    handle_session_enrolled,
    "course_registered":   handle_course_registered,
    "feedback_submitted":  handle_feedback_submitted,
    "telegram_linked":     handle_telegram_linked,
    "bot_interaction":     handle_bot_interaction,
    "state_upserted":      handle_state_upserted,
}


#  DEAD-LETTER QUEUE

async def send_to_dlq(
    dlq_producer,
    original_topic: str,
    raw_value: bytes,
    error_msg: str,
) -> None:
    """Wrap the failed message with error metadata and publish to DLQ."""
    try:
        envelope = {
            "original_topic": original_topic,
            "error":          error_msg,
            "failed_at":      datetime.utcnow().isoformat(),
            "raw_value":      raw_value.decode("utf-8", errors="replace"),
        }
        await dlq_producer.send_and_wait(
            DLQ_TOPIC,
            value=json.dumps(envelope).encode("utf-8"),
        )
        _inc("dlq_sent")
        logger.warning("DLQ ▶ %s | %s", original_topic, error_msg)
    except Exception as exc:
        logger.error("DLQ publish failed: %s", exc)


#  MESSAGE PROCESSOR

async def process_message(
    pool: asyncpg.Pool,
    dlq_producer,
    topic: str,
    raw_value: bytes,
) -> bool:
    """
    Deserialize → dispatch → write to DB.
    Retries up to MAX_RETRY times before sending to DLQ.
    Returns True if the offset should be committed.
    """
    # 1. Parse
    try:
        event    = json.loads(raw_value.decode("utf-8"))
        etype    = event.get("event_type", "")
        payload  = event.get("payload", {})
        event_id = event.get("event_id", "?")
    except Exception as exc:
        logger.error("Malformed message on %s: %s", topic, exc)
        await send_to_dlq(dlq_producer, topic, raw_value, f"parse_error: {exc}")
        return True   
    # 2. Lookup handler
    handler = HANDLERS.get(etype)
    if handler is None:
        logger.warning("No handler for event_type=%s on topic=%s", etype, topic)
        return True   

    # 3. Execute with retries
    for attempt in range(1, MAX_RETRY + 1):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await handler(conn, payload)
            _inc("processed")
            return True
        except asyncpg.UniqueViolationError:
            logger.debug("Duplicate event skipped: %s id=%s", etype, event_id)
            _inc("processed")
            return True
        except Exception as exc:
            _inc("errors")
            _inc("retries")
            if attempt < MAX_RETRY:
                wait = RETRY_BACKOFF_S[attempt - 1]
                logger.warning(
                    "Retry %d/%d for %s id=%s in %ds: %s",
                    attempt, MAX_RETRY, etype, event_id, wait, exc,
                )
                await asyncio.sleep(wait)
            else:
                logger.error(
                    "All retries exhausted for %s id=%s: %s",
                    etype, event_id, exc,
                )
                await send_to_dlq(dlq_producer, topic, raw_value, str(exc))
                return True   # commit to avoid infinite stall

    return True


#  MAIN CONSUMER LOOP

async def run_consumer() -> None:
    #  DB pool 
    pool = await asyncpg.create_pool(
        DATABASE_URL, ssl="require",
        min_size=2, max_size=8,
        max_inactive_connection_lifetime=300,
    )
    logger.info("✅ DB pool ready")

    #  Kafka consumer 
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",      
        enable_auto_commit=False,          
        max_poll_records=50,               
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        fetch_max_bytes=1_048_576,        
    )

    #  DLQ producer 
    from aiokafka import AIOKafkaProducer
    dlq_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
    )

    await consumer.start()
    await dlq_producer.start()
    logger.info("✅ Consumer started | topics=%s", TOPICS)

    #  Graceful shutdown 
    running   = True
    last_commit = time.monotonic()

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
                    consumer.getmany(timeout_ms=1000, max_records=50),
                    timeout=2.0,
                )
            except asyncio.TimeoutError:
                msg_batch = {}

            for tp, messages in msg_batch.items():
                for msg in messages:
                    t0 = time.monotonic()
                    ok = await process_message(
                        pool, dlq_producer, msg.topic, msg.value
                    )
                    elapsed = int((time.monotonic() - t0) * 1000)
                    if ok:
                        logger.debug(
                            "✓ %s | offset=%s | %dms",
                            msg.topic, msg.offset, elapsed,
                        )

            if time.monotonic() - last_commit > COMMIT_INTERVAL_S:
                try:
                    await consumer.commit()
                    last_commit = time.monotonic()
                except CommitFailedError as exc:
                    logger.warning("Offset commit failed (will retry): %s", exc)

            if int(time.monotonic()) % 60 == 0:
                logger.info("Metrics: %s", _metrics)

    finally:
        try:
            await consumer.commit()
        except Exception:
            pass
        await consumer.stop()
        await dlq_producer.stop()
        await pool.close()
        logger.info("DB Consumer shut down cleanly. Final metrics: %s", _metrics)


if __name__ == "__main__":
    asyncio.run(run_consumer())
