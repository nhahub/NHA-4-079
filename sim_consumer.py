import json

import psycopg2
from kafka import KafkaConsumer
from databricks import sql

KAFKA_BOOTSTRAP = "localhost:9093"  
TOPIC = "student_attendance"

DB_CONFIG = sql.connect(
    server_hostname="dbc-18d774f3-4d28.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/b1da869bcd1227b7",
    access_token="dapid51a351b302909df9f2fd0c4372e9aa2"
)


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="attendance_writer",
)


DB_CONFIG.autocommit = True
cur = DB_CONFIG.cursor()

print("Listening for attendance events...")

for message in consumer:
    event = message.value
    cur.execute(
        """
        INSERT INTO university_bot.core.attendance (Student_Id , Session_Id, Attendance_Date, Status , Check_In_Time, Created_At)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
      
        (
            event["student_id"],
            event["session_id"],
            event["attendance_date"],
            event["status"],
            event["checkin_time"],
            event["created_at"]
        ),
    )
    print(f"Inserted attendance for {event['student_id']}")
