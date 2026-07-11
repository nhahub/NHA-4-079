import json
import random
import time
from datetime import datetime, timezone
from databricks import sql

import psycopg2
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9093" 
TOPIC = "student_attendance"

DB_CONFIG = sql.connect(
    server_hostname="dbc-18d774f3-4d28.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/b1da869bcd1227b7",
    access_token="dapid51a351b302909df9f2fd0c4372e9aa2"
)

### 

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def get_students():

    cur = DB_CONFIG.cursor()
    cur.execute("SELECT student_id , session_id FROM attendace_nd")
    rows = cur.fetchall()
    cur.close()
    DB_CONFIG.close()
    return rows


def send_checkin(student_id: int, session_id: str):
    status = random.choice(["Present","Absent"])
    event = {
        "student_id": student_id,
        "session_id": session_id,
        "attendance_date":datetime.now(timezone.utc).date().isoformat(),
        "status": status,
        "checkin_time": datetime.now(timezone.utc).isoformat(),
        "created_at" : datetime.now(timezone.utc).isoformat()
    }
    producer.send(TOPIC, value=event)
    producer.flush()
    print(f"Sent: {event}")


if __name__ == "__main__":
    students = get_students()
    for student_id, session_id in students:
        send_checkin(student_id, session_id )
        time.sleep(1)  
