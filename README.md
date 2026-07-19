<div align="center">

# EduMetrics — University Telegram Bot

### *An Automated Data Pipeline for Real-Time Student Tracking and Reporting*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Telegram](https://img.shields.io/badge/Telegram%20Bot-API-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots/api)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Supabase](https://img.shields.io/badge/Supabase-Cloud%20DB-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)](https://supabase.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-Educational-blue?style=for-the-badge)](#license)

> *"We don't just move data — we build truth."*

</div>

---

## 📋 Table of Contents

- [Overview](#overview)
- [Audience](#audience)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Setup](#database-setup)
- [Running the Project](#running-the-project)
- [Usage Examples](#usage-examples)
- [Kafka Event Schemas](#kafka-event-schemas)
- [Simulating the Attendance System](#simulating-the-attendance-system)
- [Analytics Tables](#analytics-tables)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Linting & Formatting](#linting--formatting)
- [CI/CD](#cicd)
- [Deployment](#deployment)
- [Environment Variables](#environment-variables)
- [Known Issues & Future Improvements](#known-issues--future-improvements)
- [Dashboard Showcase](#dashboard-showcase)
- [Authors & Acknowledgments](#authors--acknowledgments)

---

## Overview

**EduMetrics** is a production-grade data engineering system that fully digitizes university academic operations. Students interact with their academic records through a **conversational Telegram Bot** instead of slow web portals, while all data flows through a robust event-streaming pipeline — from raw bot interactions all the way to a Power BI analytics dashboard.

The system is aligned with **Egypt's Vision 2030** for digital transformation and was built under the **Digital Egypt** initiative.

### The Problem It Solves

| Pain Point | EduMetrics Solution |
|---|---|
| System crashes during registration peaks | Apache Kafka absorbs concurrency spikes as a real-time event buffer |
| Synchronous backend bottlenecks | Fully async Python (`asyncpg` + `python-telegram-bot`) decouples UX from DB operations |
| Complex, desktop-only university portals | Mobile-first Telegram Bot with Arabic-language inline keyboards |
| No real-time academic analytics | Dedicated analytics consumer producing Power BI-ready snapshots |

### Goal

To provide a **single, mobile-first channel** through which students can view schedules, attendance, grades, and GPA; register courses and sessions; and submit professor feedback — all while engineering teams get a reliable, observable data pipeline with zero data loss.

---

## Audience

| Audience | How They Use EduMetrics |
|---|---|
| **Students** | Interact daily via the Telegram Bot (Arabic UI) |
| **University Admins** | Monitor the Kafka UI dashboard and Power BI reports |
| **Data Engineers** | Extend consumers, add new Kafka topics, tune performance |
| **Backend Developers** | Contribute to the bot handlers, DB schema, or stored procedures |
| **DevOps / Maintainers** | Manage Docker deployments, monitor health endpoints, rotate credentials |

---

## Features

### Bot Commands

| Command | Button | Description |
|---|---|---|
| `/start` | — | Onboarding: links Telegram account to student via National ID |
| `/schedule` | 📅 جدولي | Weekly timetable grouped by day, with room and professor |
| `/attendance` | ✅ الحضور | Per-course attendance breakdown with percentage and color indicators |
| `/grades` | 🎓 درجاتي | Grades and GPA per course per semester |
| `/gpa` | 📊 تفاصيل GPA | Cumulative GPA, per-semester breakdown, and grade distribution |
| `/courses` | 📚 موادي | Registered courses with session booking status |
| `/enroll` | 📋 حجز سيكشن | Enroll in an available session (Lecture / Section / Lab) |
| `/regcourse` | ➕ تسجيل مادة | Register for a new course in the student's department |
| `/feedback` | ⭐ تقييم أستاذ | Multi-step professor rating (1–5 stars + optional comment) |
| `/profile` | 👤 بياناتي | Full student profile (name, ID, department, faculty, year) |
| `/menu` | — | Show main inline keyboard |
| `/help` | ℹ️ مساعدة | Display all available commands |
| `/cancel` | — | Abort any active conversation flow |

### Database Highlights

- **15-table normalized schema** covering the full university hierarchy
- **Bot state machine** persisted in `Bot_States` with JSONB payloads and automatic 2-hour session expiry
- **Interaction audit log** in `Bot_Interaction_Logs` with response-time tracking (ms)
- **Materialized view** `mv_student_schedule` for zero-latency schedule queries
- **UPSERT stored procedures** for atomic bot-state management
- **Partial indexes** on active Telegram IDs for hot-path performance

### Analytics Dashboard (Power BI)

- Attendance status breakdown (Present / Absent / Late / Excused)
- Department-level attendance trends
- Session type distribution (Lecture / Section / Lab)
- Enrollment year cohort analysis
- Gender distribution
- Day-of-week attendance filters
- Top professor ratings and response-time leaderboard per bot command

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Bot Framework | `python-telegram-bot` | v21+ (async) |
| Database Driver | `asyncpg` | Latest |
| Database | PostgreSQL via Supabase (SSL) | 16 |
| Event Streaming | Apache Kafka | 7.6.0 (Confluent) |
| Kafka Client | `aiokafka` | Latest |
| Analytics Processing | `pandas` | Latest |
| Async Runtime | Python + `nest_asyncio` | 3.11+ |
| Analytics / BI | Power BI (Galaxy Schema DWH) | — |
| Containerization | Docker + Docker Compose | Latest |

---

## Architecture

### High-Level Data Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                      Telegram Bot (Producer)                      │
│           All WRITES → Kafka    ·    All READS → PostgreSQL       │
└──────────────────────────┬───────────────────────────────────────┘
                           │  6 topics
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Apache Kafka Broker                           │
│  edu.enrollment · edu.feedback · edu.interaction_log             │
│  edu.onboarding · edu.course_registration · edu.state_change     │
└───────────────┬──────────────────────────────┬───────────────────┘
                │                              │
     ┌──────────▼──────────┐       ┌───────────▼──────────┐
     │    DB Consumer       │       │  Analytics Consumer   │
     │  group:edu-db-writers│       │  group:edu-analytics  │
     └──────────┬──────────┘       └───────────┬──────────┘
                │                              │
     ┌──────────▼──────────┐       ┌───────────▼──────────┐
     │ PostgreSQL·Supabase  │       │  Analytics Tables     │
     │ 15-table schema      │       │  + Power BI Dashboard │
     └─────────────────────┘       └──────────────────────┘
```

The bot **reads** directly from PostgreSQL (schedule, attendance, grades) for zero latency. All **writes** (enroll, register, feedback, onboarding) go through Kafka first; if Kafka is unavailable, an automatic fallback writes directly to the database.

### Component Map

| Component | File | Responsibility |
|---|---|---|
| Telegram Bot | `telegram_bot_kafka.py` | User-facing interface; Kafka producer for all write events |
| DB Consumer | `db_consumer.py` | Consumes all topics, writes to PostgreSQL with UPSERT + retry |
| Analytics Consumer | `analytics_consumer.py` | Aggregates events in memory, flushes snapshots to analytics tables |
| PostgreSQL Schema | `university_bot.sql` | 15 tables, indexes, stored procedures, materialized view |
| Stack Orchestration | `docker-compose.yml` | Zookeeper, Kafka, Kafka UI, bot, both consumers |

### Database Entity Hierarchy

```
Universities
    └── Faculties
            └── Departments
                    ├── Professors
                    ├── Students ───────────────────────────────┐
                    └── Courses                                  │
                            └── Sessions (Lecture/Section/Lab)  │
                                    └── Enrollments ◄───────────┘
                                            ├── Attendance
                                            └── Feedback
```

### Key Database Objects

| Object | Type | Purpose |
|---|---|---|
| `Students` | Table | Core student record with Telegram ID linkage |
| `Sessions` | Table | Recurring class meetings with room-conflict guard |
| `Enrollments` | Table | Student ↔ Session binding with grade and GPA |
| `Attendance` | Table | Per-session daily attendance with check-in time |
| `Bot_States` | Table | FSM state per chat (JSONB payload, auto-expires 2h) |
| `Bot_Interaction_Logs` | Table | Full audit trail with latency tracking |
| `mv_student_schedule` | Materialized View | Pre-joined weekly schedule for fast bot queries |
| `fn_upsert_bot_state()` | Function | Atomic insert-or-update of conversation state |
| `fn_purge_expired_bot_states()` | Function | Cron cleanup target for stale sessions |
| `fn_get_student_by_telegram()` | Function | Optimized lookup by Telegram ID |

---

## Prerequisites

Make sure the following are installed before you begin:

| Tool | Minimum Version | Install Link |
|---|---|---|
| Python | 3.11 | [python.org](https://www.python.org/downloads/) |
| Docker Desktop | Latest | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Git | Any | [git-scm.com](https://git-scm.com/) |
| A Telegram Bot Token | — | [@BotFather](https://t.me/BotFather) on Telegram |
| A Supabase project | — | [supabase.com](https://supabase.com/) (or local PostgreSQL 16) |

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/edumetrics.git
cd edumetrics
```

### 2. Create and Activate a Virtual Environment

```bash
# Create
python -m venv .venv

# Activate — macOS / Linux
source .venv/bin/activate

# Activate — Windows
.venv\Scripts\activate
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

**`requirements.txt`**
```
python-telegram-bot==21.*
asyncpg
aiokafka
nest_asyncio
pandas
pytest
pytest-asyncio
black
isort
flake8
```

---

## Configuration

### 1. Set Up Environment Variables

```bash
cp .env.example .env
```

Open `.env` and fill in your credentials:

```env
BOT_TOKEN=your_telegram_bot_token_here
DATABASE_URL=postgresql://user:password@db.supabase.co:5432/postgres
```

> Never commit `.env` to version control. It is already listed in `.gitignore`.

### 2. Optional Overrides (in `.env`)

```env
# Kafka — defaults to the Docker Compose service name
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Set to "false" to run the bot in DB-direct mode (no Kafka)
KAFKA_ENABLED=true
```

---

## Database Setup

### Option A — Supabase (Recommended for Production)

1. Create a new project at [supabase.com](https://supabase.com/)
2. Open the **SQL Editor** in your Supabase dashboard
3. Paste the full contents of `university_bot.sql` and run it
4. Copy your connection string into `DATABASE_URL` in `.env`

### Option B — Local PostgreSQL via Docker Compose

Uncomment the `postgres` service block at the bottom of `docker-compose.yml`, then run:

```bash
docker compose up -d postgres
```

The schema is automatically applied via the `docker-entrypoint-initdb.d` mount. Your local `DATABASE_URL` will be:

```env
DATABASE_URL=postgresql://edu:edu_local_pass@localhost:5432/edumetrics
```

### Option C — Existing PostgreSQL Instance

```bash
psql -h <host> -U <user> -d <database> -f university_bot.sql
```

### Analytics Tables (Required for Analytics Consumer)

After the main schema is applied, run the following to create the analytics snapshot tables:

```sql
CREATE TABLE IF NOT EXISTS analytics_professor_ratings (
    snapshot_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    professor_id  INT         NOT NULL,
    avg_rating    NUMERIC(3,2),
    review_count  INT,
    PRIMARY KEY (snapshot_ts, professor_id)
);

CREATE TABLE IF NOT EXISTS analytics_daily_activity (
    activity_date     DATE NOT NULL PRIMARY KEY,
    new_enrollments   INT  DEFAULT 0,
    new_course_regs   INT  DEFAULT 0,
    new_onboardings   INT  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS analytics_student_activity (
    student_id     INT         NOT NULL PRIMARY KEY,
    last_active_at TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_command_stats (
    snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    command         VARCHAR(50) NOT NULL,
    call_count      INT,
    avg_response_ms NUMERIC(8,2),
    PRIMARY KEY (snapshot_ts, command)
);
```

---

##  Running the Project

### Full Stack (Recommended)

Starts Zookeeper, Kafka, Kafka UI, and all three Python services in one command:

```bash
docker compose up -d
```

Verify all services are healthy:

```bash
docker compose ps
```

All services should show `Up` and `healthy` status.

### Individual Services

| Service | Command |
|---|---|
| Telegram Bot only | `python telegram_bot_kafka.py` |
| DB Consumer only | `python db_consumer.py` |
| Analytics Consumer only | `python analytics_consumer.py` |
| Kafka + Zookeeper only | `docker compose up -d zookeeper kafka kafka-init kafka-ui` |

### Kafka UI

Open your browser at `http://localhost:8080` to inspect topics, consumer groups, offsets, and live message flow.

### View Logs

```bash
# All services
docker compose logs -f

# Individual service
docker compose logs -f bot
docker compose logs -f db-consumer
docker compose logs -f analytics
```

### Stopping

```bash
# Stop services (keep volumes)
docker compose down

# Stop and delete all data (full reset)
docker compose down -v
```

---

## Usage Examples

### Basic Student Workflow

```
Student opens Telegram and sends /start
→ Bot asks for National ID
→ Student sends their 14-digit national ID
→ Bot links their Telegram account and shows the main menu

Student taps 📅 جدولي
→ Bot queries mv_student_schedule (materialized view)
→ Returns weekly timetable grouped by day with times and room

Student taps 📋 حجز سيكشن
→ Bot lists available sessions for their registered courses
→ Student picks a session
→ Bot publishes edu.enrollment event to Kafka (optimistic success message)
→ DB Consumer receives event, writes to Enrollments table
→ Materialized view is refreshed
```

### Rating a Professor

```
/feedback
→ Bot shows list of enrolled sessions
→ Student picks a session
→ Bot shows star rating keyboard (⭐ 1 to ⭐⭐⭐⭐⭐ 5)
→ Student picks 4 stars
→ Bot asks for optional comment; student types or sends /skip
→ Bot publishes edu.feedback event to Kafka
→ DB Consumer writes to Feedback table with UPSERT
→ Analytics Consumer updates professor rating aggregate
```

### Viewing GPA Details

```
/gpa
→ Bot queries Enrollments joined with Sessions/Courses
→ Returns:
    🎯 Cumulative GPA: 3.45
    📚 Graded courses: 12 / 14
    By semester:
      Fall 2024: 3.60 (4/4 courses)
      Spring 2024: 3.30 (4/4 courses)
    Grade distribution:
      A+: 2 courses  A: 5 courses  B+: 3 courses ...
```

---

## Kafka Event Schemas

All events share a standard envelope:

```json
{
  "event_type":     "session_enrolled",
  "event_id":       "550e8400-e29b-41d4-a716-446655440000",
  "event_ts":       "2025-03-15T10:22:01.345Z",
  "schema_version": "1.0",
  "payload":        { ... }
}
```

### Topic Reference

| Topic | Partitions | Retention | Triggered By |
|---|---|---|---|
| `edu.enrollment` | 3 | 7 days | Student books a session |
| `edu.course_registration` | 3 | 7 days | Student registers a course |
| `edu.feedback` | 3 | 7 days | Student submits professor rating |
| `edu.onboarding` | 3 | 30 days | Student links Telegram account |
| `edu.interaction_log` | 6 | 3 days | Every bot command execution |
| `edu.state_change` | 3 | 2 hours | Every FSM state transition |
| `edu.dlq` | 1 | 30 days | Failed messages (Dead-Letter Queue) |

### Payload Schemas

**`edu.enrollment`**
```json
{
  "student_id": 42,
  "session_id": 17
}
```

**`edu.course_registration`**
```json
{
  "student_id":    42,
  "course_id":     8,
  "academic_year": "2024/2025"
}
```

**`edu.feedback`**
```json
{
  "student_id":   42,
  "session_id":   17,
  "professor_id": 5,
  "rating":       4,
  "comment":      "Great explanation of algorithms."
}
```

**`edu.onboarding`**
```json
{
  "student_id":        42,
  "telegram_id":       987654321,
  "telegram_username": "ahmed_2025"
}
```

**`edu.interaction_log`**
```json
{
  "chat_id":    123456789,
  "username":   "ahmed_2025",
  "student_id": 42,
  "command":    "/schedule",
  "msg_in":     "/schedule",
  "msg_out":    "📅 جدولك الأسبوعي...",
  "ms":         143
}
```

**`edu.state_change`**
```json
{
  "chat_id":    123456789,
  "student_id": 42,
  "state":      "AWAITING_FEEDBACK_RATING",
  "data":       { "session_id": 17, "professor_id": 5 }
}
```

---

## Simulating the Attendance System

To test and demo the full pipeline end-to-end — Kafka → DB Consumer / Analytics Consumer → Power BI dashboard — without waiting on real students to interact with the Telegram Bot, the project includes two standalone scripts that simulate the university attendance workflow:

| File | Role |
|---|---|
| [`attendance_producer.py`](sim_producer.py) | **Kafka Producer** — generates simulated attendance events (check-ins, absences, excused/late records) for students across courses and sessions, and publishes them to the attendance topic |
| [`attendance_consumer.py`](sim_consumer.py) | **Kafka Consumer** — subscribes to the topic under the `attendance_writer` consumer group, consumes the simulated events, and writes them into PostgreSQL exactly like the production DB Consumer |

This lets the Analytics Consumer and Power BI dashboard be populated and verified with realistic attendance data on demand, independent of live bot traffic. Messages can be inspected live in the Kafka UI (`http://localhost:8080`) under **Topics → messages**, filtered by the `attendance_writer` consumer group:

![Kafka UI showing simulated attendance messages](kafka.png)

> **Note:** `attendance_producer.py` and `attendance_consumer.py` are placeholder filenames — replace them with the actual filenames of your two scripts so the links resolve correctly.

---

## Analytics Tables

The analytics consumer writes pre-aggregated snapshots every 30 seconds. Power BI connects to these tables via Supabase's direct connection.

| Table | Updated Every | Used For |
|---|---|---|
| `analytics_professor_ratings` | 30 s | Average rating per professor over time |
| `analytics_daily_activity` | 30 s | Daily enrollment and registration trend lines |
| `analytics_student_activity` | 30 s | Active vs inactive student counts |
| `analytics_command_stats` | 30 s | Bot command performance and usage frequency |

### Example Queries

**Top 5 Rated Professors**
```sql
SELECT DISTINCT ON (professor_id)
    p.professor_name, r.avg_rating, r.review_count
FROM analytics_professor_ratings r
JOIN professors p ON r.professor_id = p.professor_id
ORDER BY professor_id, snapshot_ts DESC, avg_rating DESC
LIMIT 5;
```

**30-Day Enrollment Trend**
```sql
SELECT activity_date, new_enrollments,
       SUM(new_enrollments) OVER (ORDER BY activity_date) AS cumulative
FROM analytics_daily_activity
WHERE activity_date >= CURRENT_DATE - 30
ORDER BY activity_date;
```

---

## Development Workflow

### Branch Strategy

```
main          — production-ready code only
dev           — integration branch
feature/<name> — individual feature branches
fix/<name>    — bug fix branches
```

### Standard Contribution Flow

```bash
# 1. Create a feature branch from dev
git checkout dev
git pull origin dev
git checkout -b feature/your-feature-name

# 2. Make your changes
# 3. Run tests and linting
pytest
black . && isort . && flake8 .

# 4. Commit with a descriptive message
git add .
git commit -m "feat: add professor availability endpoint"

# 5. Push and open a Pull Request to dev
git push origin feature/your-feature-name
```

### Commit Message Convention

```
feat:     New feature
fix:      Bug fix
refactor: Code restructure without behavior change
docs:     Documentation update
test:     Add or update tests
chore:    Dependency or config update
```

### Adding a New Kafka Topic

1. Add the topic constant in `telegram_bot_kafka.py`
2. Add it to the `kafka-init` command list in `docker-compose.yml`
3. Add a handler function in `db_consumer.py` and register it in `HANDLERS`
4. Add an aggregator in `analytics_consumer.py` and register it in `ANALYTICS_HANDLERS`
5. Document the payload schema in this README under [Kafka Event Schemas](#kafka-event-schemas)

---

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run a specific test file
pytest tests/test_db_consumer.py

# Run with coverage report
pytest --cov=. --cov-report=html
open htmlcov/index.html
```

### Test Structure

```
tests/
├── conftest.py                  # Shared fixtures (mock pool, mock producer)
├── test_bot_handlers.py         # Unit tests for command handlers
├── test_db_consumer.py          # Unit tests for all DB handler functions
├── test_analytics_consumer.py   # Unit tests for aggregation logic
├── test_kafka_publish.py        # Integration tests for Kafka publish/fallback
└── test_sql_schema.py           # Schema integrity checks (FK, constraints)
```

### Writing a Test

```python
import pytest
import pytest_asyncio

@pytest.mark.asyncio
async def test_handle_session_enrolled_idempotent(mock_pool):
    """Enrolling twice should not raise or insert a duplicate."""
    payload = {"student_id": 1, "session_id": 5}
    await handle_session_enrolled(mock_pool, payload)
    await handle_session_enrolled(mock_pool, payload)   # second call = no-op
    # Assert only one row exists
    count = await mock_pool.fetchval(
        "SELECT COUNT(*) FROM enrollments WHERE student_id=1 AND session_id=5"
    )
    assert count == 1
```

> **Note:** Integration tests that hit a real database require a running local PostgreSQL instance. Set `TEST_DATABASE_URL` in `.env` to point to a dedicated test database.

---

## Linting & Formatting

This project enforces consistent code style using three tools:

| Tool | Purpose | Command |
|---|---|---|
| `black` | Auto-formatter (line length 88) | `black .` |
| `isort` | Import sorter | `isort .` |
| `flake8` | Style and error linter | `flake8 .` |

Run all three together:

```bash
black . && isort . && flake8 .
```

**`setup.cfg`** (add to project root):
```ini
[flake8]
max-line-length = 88
extend-ignore = E203, E501
exclude = .venv, __pycache__

[isort]
profile = black
```

---

## CI/CD

### GitHub Actions Pipeline

Create `.github/workflows/ci.yml`:

```yaml
name: EduMetrics CI

on:
  push:
    branches: [main, dev]
  pull_request:
    branches: [main, dev]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_DB: edumetrics_test
          POSTGRES_USER: edu
          POSTGRES_PASSWORD: test_pass
        ports: ["5432:5432"]
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Apply schema
        run: psql postgresql://edu:test_pass@localhost:5432/edumetrics_test -f university_bot.sql

      - name: Lint
        run: black --check . && isort --check . && flake8 .

      - name: Test
        env:
          TEST_DATABASE_URL: postgresql://edu:test_pass@localhost:5432/edumetrics_test
        run: pytest --cov=. --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          file: ./coverage.xml
```

### Deployment Pipeline (on `main` merge)

```yaml
  deploy:
    needs: lint-and-test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - name: SSH deploy to server
        uses: appleboy/ssh-action@v1
        with:
          host: ${{ secrets.SERVER_HOST }}
          username: ${{ secrets.SERVER_USER }}
          key: ${{ secrets.SSH_PRIVATE_KEY }}
          script: |
            cd /opt/edumetrics
            git pull origin main
            docker compose pull
            docker compose up -d --build
```

---

## Deployment

### Development

```bash
docker compose up -d
```

### Production Checklist

Before going to production, complete the following:

- [ ] Load all secrets from environment variables or a secrets manager (never hardcode)
- [ ] Set `KAFKA_DEFAULT_REPLICATION_FACTOR: 3` in `docker-compose.yml` and run a 3-broker cluster
- [ ] Enable Kafka `ssl` and `sasl` for broker authentication
- [ ] Replace `ssl="require"` with full SSL certificate verification for PostgreSQL
- [ ] Set `KAFKA_ENABLED=true` and confirm fallback behaviour is tested
- [ ] Scale the DB consumer: `docker compose up --scale db-consumer=3`
- [ ] Configure log aggregation (e.g., Datadog, Loki)
- [ ] Set up alerting on the `edu.dlq` topic (dead-letter queue)
- [ ] Schedule `fn_purge_expired_bot_states()` as a cron job (every 30 minutes)
- [ ] Set up `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_student_schedule` on a cron (every 5 minutes)

### Horizontal Scaling

The DB consumer supports horizontal scaling natively. Kafka rebalances partitions automatically across instances:

```bash
# Scale to 3 DB consumer instances (requires ≥ 3 partitions per topic)
docker compose up -d --scale db-consumer=3
```

### Kubernetes (Placeholder)

A Helm chart and Kubernetes manifests for production deployment are planned. See [Future Improvements](#known-issues--future-improvements).

---

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `BOT_TOKEN` | ✅ | — | Telegram Bot API token from BotFather |
| `DATABASE_URL` | ✅ | — | PostgreSQL connection string (SSL enforced) |
| `KAFKA_BOOTSTRAP_SERVERS` | ✅ | `kafka:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_ENABLED` | ❌ | `true` | Set `false` to bypass Kafka and write directly to DB |

> All variables are read from `.env` via Docker Compose. In production, inject them from your secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.).

---

## Security Notes

- Student **National IDs are never logged** — the audit log records `[nid_hidden]` during onboarding.
- `Bot_States` sessions expire automatically after **2 hours** of inactivity.
- Database connections use **SSL** (`ssl="require"`) enforced at the connection pool level.
- `Telegram_Username` is stored in logs **without a foreign key** to tolerate username changes without breaking audit history.
- Kafka payloads for onboarding carry `telegram_id` but never the raw National ID.
- Use `.gitignore` to ensure `.env` is never committed.

---

## Known Issues & Future Improvements

### Known Issues

| Issue | Impact | Workaround |
|---|---|---|
| Materialized view refresh is manual / cron-based | Schedule shown may lag up to 5 min after a new enrollment is processed | Trigger `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_student_schedule` from `db_consumer.py` after each enrollment (already implemented) |
| Single Kafka broker in `docker-compose.yml` | Not fault-tolerant; broker restart = downtime | Kafka fallback in bot writes directly to DB; extend to 3-broker cluster for production |
| `analytics_consumer` holds state in memory | Service restart clears un-flushed aggregations | Flush interval is 30 s; worst-case data loss is 30 s of counts — acceptable for analytics |
| No rate limiting on bot commands | A user could spam the bot and flood the DB consumer | Add per-chat rate limiting in the bot middleware layer |

### Planned Improvements

- [ ] **Kubernetes Helm Chart** — deploy to EKS / GKE for autoscaling
- [ ] **Schema Registry** — enforce Avro/JSON Schema on all Kafka topics via Confluent Schema Registry
- [ ] **Exactly-once semantics** — enable Kafka transactions between consumer and DB writes
- [ ] **REST API layer** — FastAPI service exposing bot data for third-party integrations
- [ ] **Push notifications** — proactive alerts (exam reminders, low attendance warnings)
- [ ] **Grade prediction model** — ML consumer that predicts at-risk students from attendance and grade streams
- [ ] **Prometheus + Grafana** — replace the lightweight counter metrics with a full observability stack
- [ ] **Multi-university support** — tenant isolation per `University_Id` at the Kafka topic level

---

## Dashboard Showcase

Alongside the bot and streaming pipeline, the team built a **Power BI analytics dashboard** on top of the data the Analytics Consumer streams into the snapshot tables. Its role is to turn raw Kafka-streamed attendance and enrollment events into a live, filterable view for university admins — surfacing overall attendance rate, student/course/professor/session counts, attendance status breakdown (Present/Absent/Late/Excused), department-level attendance, enrollment-year and gender distribution, and session-type mix, filterable by semester (Fall/Spring).

![University Attendance Analytics Dashboard — Fall](dashboard.png)

![University Attendance Analytics Dashboard — Spring](dashboard_2.png)

---

## Authors & Acknowledgments

This project was built as part of a **Data Engineering** track under the **Digital Egypt** initiative.

| Role | Name |
|---|---|
| Team Lead | Ahmed Alqadi |
| Data Engineer | Bishoy Halim |
| Data Engineer | Abdelrahman Mohamed |
| Data Engineer | David Wagih |
| Data Engineer | Ahmed Ramadan |

---
