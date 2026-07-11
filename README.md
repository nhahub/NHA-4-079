# 🎓 University Boot

**An automated, event-driven data pipeline for real-time student attendance tracking and reporting.**

> Graduation Project — Digital Egypt Pioneers Initiative (DEPI), Microsoft Data Engineering Track

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Data%20Warehouse-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboards-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

## 📌 Overview

Attendance data at the college used to be collected manually — slow, error-prone, and hours (or days) behind reality. **University Boot** replaces that process with a fully automated pipeline:

```
College Data Source → Telegram Bot → Apache Kafka → ETL → Data Warehouse → Power BI
```

A Telegram bot stays online continuously and exposes attendance/schedule/grades data to students, while attendance events are streamed through Kafka, transformed by an ETL job, loaded into a Databricks-based star-schema Data Warehouse, and finally visualized in a live Power BI dashboard.

## ✨ Features

- 🤖 **Telegram Bot** — students check their schedule, grades, attendance, GPA, and enroll in courses/sessions directly from Telegram.
- 🔄 **Real-time streaming** — attendance events published to Kafka topics and consumed reliably, in order.
- 🗄️ **Normalized OLTP schema** — PostgreSQL schema with enums, triggers, materialized views, and bot-state tracking for multi-step conversations.
- 🏗️ **Star-schema Data Warehouse** — fact/dimension model on Databricks (Delta Lake) optimized for analytics.
- ⏱️ **Orchestration** — Apache Airflow (via Docker Compose) schedules and monitors the ETL pipeline.
- 📊 **Power BI Dashboard** — live attendance rate, per-department trends, session types, enrollment year breakdown, and more.

## 🏛️ Architecture

```
┌────────────────────┐     ┌───────────────┐     ┌────────────────┐     ┌──────────────────┐     ┌────────────┐
│  College Data /     │────▶│  Telegram Bot │────▶│  Apache Kafka  │────▶│  ETL (Consumer /  │────▶│  Power BI  │
│  Student Interaction│     │ (Bot_worker.py│     │ (producer.py / │     │  Databricks DWH)  │     │  Dashboard │
│                      │     │  + Postgres)  │     │  consumer.py)  │     │                    │     │            │
└────────────────────┘     └───────────────┘     └────────────────┘     └──────────────────┘     └────────────┘
                                                          ▲
                                                          │
                                                  Orchestrated by Apache Airflow
```

## 🗂️ Repository Structure

```
university-boot/
├── Bot_worker.py                       # Telegram bot (python-telegram-bot + asyncpg)
├── producer.py                         # Kafka producer — reads attendance sessions, publishes events
├── consumer.py                         # Kafka consumer — writes attendance events into the DWH
├── depi.yaml                           # Docker Compose: Kafka, Kafka-UI, Airflow, Postgres (bot DB)
├── university_bot_schema.sql           # PostgreSQL OLTP schema (tables, enums, triggers, functions)
├── university_bot_insert_data_full.sql # Seed / sample data
├── create_DWH.ipynb                    # Databricks notebook — creates the star-schema DWH
├── Load_data_DWH.ipynb                 # Databricks notebook — loads dims & fact table from OLTP
├── University_Boot_Documentation.docx  # Full graduation project report
└── assets/
    ├── schema_DWH.png                  # Data Warehouse ER diagram (star schema)
    └── dashboard.png                   # Power BI dashboard screenshot
```

## 🧱 Data Warehouse — Star Schema

`fact_attendance` sits at the center, linked to `dim_student`, `dim_course`, `dim_professor`, `dim_session`, `dim_department`, and `dim_date`.

![Data Warehouse Schema](assets/schema_DWH.png)

## 📊 Power BI Dashboard

![University Attendance Analytics Dashboard](assets/dashboard.png)

## 🛠️ Tech Stack

| Layer            | Technology                                      |
|-------------------|-------------------------------------------------|
| Bot               | Python, `python-telegram-bot`, `asyncpg`         |
| OLTP Database     | PostgreSQL 15                                   |
| Streaming         | Apache Kafka (`kafka-python`)                    |
| Orchestration     | Apache Airflow 2.9.3 (Docker Compose)            |
| Data Warehouse    | Databricks (Delta Lake / SQL Warehouse)          |
| Visualization     | Power BI                                         |
| Containerization  | Docker & Docker Compose                          |

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- A Databricks workspace + SQL warehouse (for the DWH)
- A Telegram Bot token ([BotFather](https://t.me/BotFather))

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/university-boot.git
cd university-boot
```

### 2. Start the infrastructure (Kafka, Airflow, Postgres)
```bash
docker compose -f depi.yaml up -d
```
- Kafka UI → http://localhost:8085
- Airflow UI → http://localhost:8080 (`admin` / `admin`)

### 3. Load the OLTP database schema
```bash
psql -h localhost -U ahmed -d university_bot -f university_bot_schema.sql
psql -h localhost -U ahmed -d university_bot -f university_bot_insert_data_full.sql
```

### 4. Configure secrets
Set the following as environment variables (do **not** hardcode them):
```bash
export BOT_TOKEN="your-telegram-bot-token"
export DATABASE_URL="postgresql://user:password@host:5432/university_bot"
export DATABRICKS_SERVER_HOSTNAME="..."
export DATABRICKS_HTTP_PATH="..."
export DATABRICKS_ACCESS_TOKEN="..."
```

### 5. Run the pipeline
```bash
python Bot_worker.py     # Telegram bot
python producer.py       # Kafka producer
python consumer.py       # Kafka consumer / ETL writer
```

### 6. Build the Data Warehouse
Run `create_DWH.ipynb` then `Load_data_DWH.ipynb` on your Databricks workspace to create and populate the star schema.

### 7. Connect Power BI
Point Power BI to the Databricks SQL warehouse (`university_bot.analytics.*` tables) and refresh the dashboard.

> ⚠️ **Security note:** the notebooks/scripts in this repo previously contained hardcoded credentials (DB passwords, Databricks tokens, bot token). Rotate any exposed credentials and load secrets from environment variables or a secrets manager before deploying.

## 🔮 Future Work

- AI-based prediction for at-risk (low-attendance) students
- SMS & email notifications
- Dedicated mobile application
- Real-time Power BI dashboard with sub-minute refresh

## 👥 Team

Graduation project — DEPI Microsoft Data Engineering Track, supervised by **Eng. Mohamed Hamed**.

| Name | Role | GitHub |
|------|------|--------|
| Abdelrahman Mohamed Saeed | Data Engineer | [AbdelrahmanM00hammed)](https://github.com/AbdelrahmanM00hammed) |
| Bishoy Halim | Data Engineer | [bishoyhanyhalim](https://github.com/bishoyhanyhalim/) |
| David Wagih | Data Engineer | [Dov-elhacker](https://github.com/Dov-elhacker) |
| Ahmed El-Kadi | Data Engineer | [ahmedmohamedalqadi](https://github.com/ahmedmohamedalqadi9999-hash) |
| Ahmed Ramadan | Data Engineer | [Ahmed-Ramadan-Ismail](https://github.com/Ahmed-Ramadan-Ismail) |

## 📄 License

This project was developed for educational purposes as part of the DEPI Microsoft Data Engineering Track (July 2026).
