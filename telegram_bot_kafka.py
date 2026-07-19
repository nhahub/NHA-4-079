

import json, logging, time, uuid
from datetime import datetime, timezone
from typing import Optional

import nest_asyncio
nest_asyncio.apply()

import asyncpg
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, Update,
)
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters,
)

#  CONFIG 
BOT_TOKEN    = "YOUR_TELEGRAM_BOT_TOKEN"
DATABASE_URL = "postgresql://user:password@host:5432/database"

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"   
KAFKA_ENABLED = True                     

#  TOPICS 
TOPIC_ENROLLMENT   = "edu.enrollment"          
TOPIC_COURSE_REG   = "edu.course_registration" 
TOPIC_FEEDBACK     = "edu.feedback"            
TOPIC_ONBOARDING   = "edu.onboarding"          
TOPIC_INTERACTION  = "edu.interaction_log"     
TOPIC_STATE_CHANGE = "edu.state_change"        

logging.basicConfig(
    format="%(asctime)s │ %(levelname)s │ %(name)s │ %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

AWAIT_NID, FB_SESSION, FB_RATING, FB_COMMENT, ENROLL_PICK, COURSE_PICK = range(6)

#  GLOBALS 
_pool:     asyncpg.Pool      | None = None
_producer: AIOKafkaProducer  | None = None


def pool() -> asyncpg.Pool:
    assert _pool is not None, "DB pool not initialised"
    return _pool


#  KAFKA LAYER

def _make_event(event_type: str, payload: dict) -> dict:
    """Wrap a payload with standard envelope fields."""
    return {
        "event_type":    event_type,
        "event_id":      str(uuid.uuid4()),          # idempotency key
        "event_ts":      datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "payload":        payload,
    }


async def kafka_publish(topic: str, event_type: str, payload: dict) -> bool:

    if not KAFKA_ENABLED or _producer is None:
        return False

    event = _make_event(event_type, payload)
    key_val = str(payload.get("student_id") or payload.get("chat_id", "")).encode()
    value   = json.dumps(event, default=str).encode("utf-8")

    try:
        await _producer.send_and_wait(topic, value=value, key=key_val)
        logger.info("Kafka ▶ %s | %s | id=%s", topic, event_type, event["event_id"])
        return True
    except Exception as exc:
        logger.error("Kafka publish failed [%s/%s]: %s", topic, event_type, exc)
        return False


#  DATABASE LAYER  (reads — unchanged from original)

async def db_student_by_tg(tg_id: int) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            "SELECT * FROM fn_get_student_by_telegram($1)", tg_id
        )

async def db_student_by_nid(nid: str) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            """SELECT student_id, student_name, study_year, is_active
               FROM   students WHERE national_id = $1""", nid,
        )

async def db_schedule(tg_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   ses.session_type::text  AS session_type,
                   ses.day_of_week::text   AS day_of_week,
                   ses.start_time, ses.end_time,
                   ses.location, p.professor_name,
                   e.semester::text AS semester
            FROM   students    s
            JOIN   enrollments e   ON s.student_id    = e.student_id
            JOIN   sessions    ses ON e.session_id    = ses.session_id
            JOIN   courses     c   ON ses.course_id   = c.course_id
            JOIN   professors  p   ON ses.professor_id = p.professor_id
            WHERE  s.telegram_id = $1 AND s.is_active = TRUE
            ORDER BY
                CASE ses.day_of_week::text
                    WHEN 'Sunday' THEN 0 WHEN 'Monday' THEN 1
                    WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 ELSE 6
                END, ses.start_time
            """, tg_id,
        )

async def db_attendance(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_name,
                   ses.session_type::text AS session_type,
                   COUNT(a.attendance_id) FILTER (WHERE a.status = 'Present')  AS present,
                   COUNT(a.attendance_id) FILTER (WHERE a.status = 'Absent')   AS absent,
                   COUNT(a.attendance_id) FILTER (WHERE a.status = 'Late')     AS late,
                   COUNT(a.attendance_id) FILTER (WHERE a.status = 'Excused')  AS excused,
                   COUNT(a.attendance_id)                                        AS total
            FROM   enrollments    e
            JOIN   sessions       ses ON e.session_id  = ses.session_id
            JOIN   courses        c   ON ses.course_id  = c.course_id
            LEFT JOIN attendance  a   ON a.student_id  = e.student_id
                                     AND a.session_id  = e.session_id
            WHERE  e.student_id = $1
            GROUP  BY c.course_name, ses.session_type
            ORDER  BY c.course_name
            """, student_id,
        )

async def db_grades(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   e.grade::text, e.gpa,
                   e.semester::text AS semester
            FROM enrollments e
            JOIN sessions    ses ON e.session_id = ses.session_id
            JOIN courses     c   ON ses.course_id = c.course_id
            WHERE e.student_id=$1 AND e.grade IS NOT NULL
            ORDER BY e.semester DESC, c.course_name
            """, student_id,
        )

async def db_enrolled_sessions(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT ses.session_id, c.course_name,
                   ses.session_type::text AS session_type,
                   p.professor_id, p.professor_name
            FROM enrollments e
            JOIN sessions   ses ON e.session_id    = ses.session_id
            JOIN courses    c   ON ses.course_id   = c.course_id
            JOIN professors p   ON ses.professor_id = p.professor_id
            WHERE e.student_id=$1
            ORDER BY c.course_name
            """, student_id,
        )

async def db_student_profile(student_id: int) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            """
            SELECT s.student_name, s.national_id, s.phone,
                   s.address, s.birthdate, s.gender::text AS gender,
                   s.study_year, s.enrollment_year, s.telegram_username,
                   d.department_name, f.faculty_name
            FROM   students    s
            JOIN   departments d ON s.department_id = d.department_id
            JOIN   faculties   f ON d.faculty_id    = f.faculty_id
            WHERE  s.student_id = $1
            """, student_id,
        )

async def db_gpa_detail(student_id: int) -> dict:
    async with pool().acquire() as c:
        by_sem = await c.fetch(
            """
            SELECT e.semester::text AS semester,
                   ROUND(AVG(e.gpa)::numeric, 2) AS avg_gpa,
                   COUNT(*) FILTER (WHERE e.gpa IS NOT NULL) AS graded,
                   COUNT(*) AS total
            FROM   enrollments e
            WHERE  e.student_id = $1
            GROUP  BY e.semester ORDER BY e.semester DESC
            """, student_id,
        )
        overall = await c.fetchrow(
            """
            SELECT ROUND(AVG(gpa)::numeric, 2)             AS overall_gpa,
                   COUNT(*) FILTER (WHERE gpa IS NOT NULL) AS graded_courses,
                   COUNT(*)                                 AS total_enrolled
            FROM   enrollments WHERE student_id = $1
            """, student_id,
        )
        dist = await c.fetch(
            """
            SELECT grade::text AS grade, COUNT(*) AS cnt
            FROM   enrollments
            WHERE  student_id = $1 AND grade IS NOT NULL
            GROUP  BY grade ORDER BY cnt DESC
            """, student_id,
        )
    return {"by_semester": by_sem, "overall": overall, "distribution": dist}

async def db_available_sessions(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT ses.session_id, c.course_code, c.course_name,
                   ses.session_type::text AS session_type,
                   ses.day_of_week::text  AS day_of_week,
                   ses.start_time, ses.end_time, ses.location,
                   ses.semester::text AS semester, p.professor_name
            FROM   sessions   ses
            JOIN   courses    c ON ses.course_id    = c.course_id
            JOIN   professors p ON ses.professor_id = p.professor_id
            WHERE  ses.course_id IN (
                SELECT course_id FROM student_courses WHERE student_id = $1
            )
            AND ses.session_id NOT IN (
                SELECT session_id FROM enrollments WHERE student_id = $1
            )
            ORDER BY c.course_name, ses.session_type::text
            LIMIT 30
            """, student_id,
        )

async def db_available_courses(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_id, c.course_code, c.course_name,
                   c.credit_hours, c.semester::text AS semester, c.study_year
            FROM   courses c
            WHERE  c.department_id = (
                       SELECT department_id FROM students WHERE student_id = $1
                   )
              AND  c.course_id NOT IN (
                       SELECT course_id FROM student_courses WHERE student_id = $1
                   )
            ORDER BY c.study_year, c.course_name LIMIT 30
            """, student_id,
        )

async def db_registered_courses(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   sc.semester::text AS semester, sc.academic_year,
                   c.credit_hours, c.study_year,
                   EXISTS (
                       SELECT 1 FROM enrollments e
                       JOIN sessions ses ON e.session_id = ses.session_id
                       WHERE e.student_id = $1 AND ses.course_id = c.course_id
                   ) AS has_session
            FROM   student_courses sc
            JOIN   courses         c ON sc.course_id = c.course_id
            WHERE  sc.student_id = $1
            ORDER  BY sc.academic_year DESC, c.course_name
            """, student_id,
        )

async def db_get_state(chat_id: int) -> Optional[asyncpg.Record]:
    """State reads are still direct — needed synchronously during handler dispatch."""
    async with pool().acquire() as c:
        return await c.fetchrow(
            """SELECT current_state, state_data, student_id FROM bot_states
               WHERE  telegram_chat_id=$1 AND expires_at > NOW()""",
            chat_id,
        )


#  WRITE OPERATIONS 

async def write_link_telegram(student_id: int, tg_id: int, username: str) -> None:
    """Onboarding: link Telegram account to student record."""
    ok = await kafka_publish(TOPIC_ONBOARDING, "telegram_linked", {
        "student_id": student_id,
        "telegram_id": tg_id,
        "telegram_username": username,
    })
    if not ok:                      
        async with pool().acquire() as c:
            await c.execute(
                "UPDATE students SET telegram_id=$1, telegram_username=$2 WHERE student_id=$3",
                tg_id, username, student_id,
            )


async def write_upsert_state(
    chat_id: int, student_id: Optional[int], state: str, data: dict | None = None,
) -> None:
    ok = await kafka_publish(TOPIC_STATE_CHANGE, "state_upserted", {
        "chat_id":    chat_id,
        "student_id": student_id,
        "state":      state,
        "data":       data,
    })
    if not ok:
        async with pool().acquire() as c:
            await c.execute(
                "SELECT fn_upsert_bot_state($1,$2,$3,$4::jsonb)",
                chat_id, student_id, state,
                json.dumps(data) if data else None,
            )


async def write_enroll_session(student_id: int, session_id: int) -> bool:

    async with pool().acquire() as c:
        exists = await c.fetchval(
            "SELECT 1 FROM enrollments WHERE student_id=$1 AND session_id=$2",
            student_id, session_id,
        )
    if exists:
        return False   

    ok = await kafka_publish(TOPIC_ENROLLMENT, "session_enrolled", {
        "student_id": student_id,
        "session_id": session_id,
    })
    if not ok:        
        async with pool().acquire() as c:
            result = await c.execute(
                """INSERT INTO enrollments (student_id, session_id, semester)
                   SELECT $1, $2, semester FROM sessions WHERE session_id = $2
                   ON CONFLICT (student_id, session_id) DO NOTHING""",
                student_id, session_id,
            )
        return result.endswith("1")
    return True  


async def write_register_course(student_id: int, course_id: int) -> bool:
    """Publish course registration to Kafka."""
    async with pool().acquire() as c:
        exists = await c.fetchval(
            """SELECT 1 FROM student_courses
               WHERE student_id=$1 AND course_id=$2""",
            student_id, course_id,
        )
    if exists:
        return False

    academic_year = f"{datetime.now().year}/{datetime.now().year + 1}"
    ok = await kafka_publish(TOPIC_COURSE_REG, "course_registered", {
        "student_id":   student_id,
        "course_id":    course_id,
        "academic_year": academic_year,
    })
    if not ok:
        async with pool().acquire() as c:
            result = await c.execute(
                """INSERT INTO student_courses (student_id, course_id, semester, academic_year)
                   SELECT $1, $2, c.semester, $3 FROM courses c WHERE c.course_id = $2
                     AND c.semester IS NOT NULL
                   ON CONFLICT (student_id, course_id, semester, academic_year) DO NOTHING""",
                student_id, course_id, academic_year,
            )
        return result.endswith("1")
    return True


async def write_feedback(
    student_id: int, session_id: int,
    professor_id: int, rating: int, comment: str,
) -> None:
    ok = await kafka_publish(TOPIC_FEEDBACK, "feedback_submitted", {
        "student_id":   student_id,
        "session_id":   session_id,
        "professor_id": professor_id,
        "rating":       rating,
        "comment":      comment,
    })
    if not ok:
        async with pool().acquire() as c:
            await c.execute(
                """INSERT INTO feedback (student_id, session_id, professor_id, rating, comments)
                   VALUES ($1,$2,$3,$4,$5)
                   ON CONFLICT (student_id, session_id)
                   DO UPDATE SET rating=$4, comments=$5""",
                student_id, session_id, professor_id, rating, comment,
            )


async def write_log(
    chat_id: int, username: str, student_id: Optional[int],
    command: str, msg_in: str, msg_out: str, ms: int,
) -> None:
    """Publish interaction log — fire-and-forget, no DB fallback needed."""
    await kafka_publish(TOPIC_INTERACTION, "bot_interaction", {
        "chat_id":    chat_id,
        "username":   username,
        "student_id": student_id,
        "command":    command,
        "msg_in":     msg_in[:500],    
        "msg_out":    msg_out[:500],
        "ms":         ms,
    })


#  UI HELPERS

def user_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📅 جدولي",       "✅ الحضور"],
            ["🎓 درجاتي",      "📊 تفاصيل GPA"],
            ["📚 موادي",        "👤 بياناتي"],
            ["📋 حجز سيكشن",  "➕ تسجيل مادة"],
            ["⭐ تقييم أستاذ", "ℹ️ مساعدة"],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="اختر من الأزرار أو اكتب أمراً...",
    )

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 جدولي",     callback_data="schedule"),
            InlineKeyboardButton("✅ الحضور",     callback_data="attendance"),
        ],
        [
            InlineKeyboardButton("🎓 درجاتي",    callback_data="grades"),
            InlineKeyboardButton("📚 موادي",      callback_data="courses"),
        ],
        [InlineKeyboardButton("⭐ تقييم أستاذ",  callback_data="feedback")],
    ])

BUTTON_TEXTS = [
    "📅 جدولي", "✅ الحضور", "🎓 درجاتي", "📊 تفاصيل GPA",
    "📚 موادي",  "👤 بياناتي", "📋 حجز سيكشن", "➕ تسجيل مادة",
    "⭐ تقييم أستاذ", "ℹ️ مساعدة",
]
button_filter = filters.Text(BUTTON_TEXTS)

_DAY_AR = {
    "Sunday": "الأحد", "Monday": "الاثنين", "Tuesday": "الثلاثاء",
    "Wednesday": "الأربعاء", "Thursday": "الخميس",
    "Friday": "الجمعة", "Saturday": "السبت",
}

def fmt_schedule(rows: list) -> str:
    if not rows:
        return "📭 لا يوجد جدول مسجل لك حالياً."
    blocks: dict[str, list[str]] = {}
    for r in rows:
        day   = _DAY_AR.get(r["day_of_week"], r["day_of_week"])
        start = r["start_time"].strftime("%H:%M") if r["start_time"] else "؟"
        end   = r["end_time"].strftime("%H:%M")   if r["end_time"]   else "؟"
        loc   = r["location"] or "أونلاين"
        blocks.setdefault(day, []).append(
            f"  📘 *{r['course_code']}* — {r['course_name']}\n"
            f"     🕐 {start}–{end}  📍 {loc}\n"
            f"     👨‍🏫 {r['professor_name']}  [{r['session_type']}]"
        )
    parts = ["📅 *جدولك الأسبوعي*\n"]
    for day, lines in blocks.items():
        parts.append(f"*{day}*")
        parts.extend(lines)
        parts.append("")
    return "\n".join(parts)

def fmt_attendance(rows: list) -> str:
    if not rows:
        return "📭 لا توجد بيانات حضور بعد."
    lines = ["✅ *سجل الحضور*\n"]
    for r in rows:
        total = r["total"] or 1
        pct   = round((r["present"] / total) * 100)
        icon  = "🟢" if pct >= 75 else "🟡" if pct >= 60 else "🔴"
        lines.append(
            f"{icon} *{r['course_name']}* [{r['session_type']}]\n"
            f"   ✅ {r['present']} | ❌ {r['absent']} | ⏰ {r['late']} | 📝 {r['excused']}"
            f"   —  *{pct}%*\n"
        )
    return "\n".join(lines)

def fmt_grades(rows: list) -> str:
    if not rows:
        return "📭 لا توجد درجات مسجلة بعد."
    lines = ["🎓 *درجاتك*\n"]
    for r in rows:
        gpa_str = f"{r['gpa']:.2f}" if r["gpa"] else "—"
        lines.append(
            f"• *{r['course_code']}* — {r['course_name']}\n"
            f"  [{r['semester']}]  الدرجة: *{r['grade']}*  GPA: {gpa_str}\n"
        )
    return "\n".join(lines)

def fmt_gpa_detail(data: dict) -> str:
    ov = data["overall"]
    lines = [
        "📊 *تفاصيل GPA*\n",
        f"🎯 الإجمالي التراكمي: *{ov['overall_gpa'] or '—'}*",
        f"📚 المواد المقيّمة: {ov['graded_courses']} / {ov['total_enrolled']}\n",
        "*بالفصل الدراسي:*",
    ]
    for s in data["by_semester"]:
        lines.append(f"  • {s['semester']}: {s['avg_gpa']} ({s['graded']}/{s['total']} مواد)")
    if data["distribution"]:
        lines.append("\n*توزيع الدرجات:*")
        for d in data["distribution"]:
            lines.append(f"  {d['grade']}: {d['cnt']} مواد")
    return "\n".join(lines)

def fmt_profile(r) -> str:
    lines = [
        "👤 *بياناتي الشخصية*\n",
        f"👨‍🎓 الاسم:       {r['student_name']}",
        f"🏛️  الكلية:     {r['faculty_name']}",
        f"📐  القسم:      {r['department_name']}",
        f"📅  السنة:      {r['study_year']}",
        f"🗓️  دفعة:      {r['enrollment_year'] or '—'}",
        f"📞  الهاتف:     {r['phone'] or '—'}",
        f"♂️  النوع:      {r['gender'] or '—'}",
        f"🎂  الميلاد:    {r['birthdate'] or '—'}",
        f"💬  تيليجرام:  @{r['telegram_username'] or '—'}",
    ]
    return "\n".join(lines)


#  GUARD: require authenticated student

async def require_student(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE,
) -> Optional[asyncpg.Record]:
    s = await db_student_by_tg(update.effective_user.id)
    if not s or not s["is_active"]:
        await update.effective_message.reply_text(
            "🔐 يرجى ربط حسابك أولاً بإرسال /start."
        )
        return None
    return s


#  COMMAND HANDLERS

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    s = await db_student_by_tg(update.effective_user.id)
    if s and s["is_active"]:
        await update.message.reply_text(
            f"👋 أهلاً {s['student_name']}!\nأنت مسجل بالفعل.",
            reply_markup=user_keyboard(),
        )
        return ConversationHandler.END

    await write_upsert_state(update.effective_chat.id, None, "AWAIT_NID")
    await update.message.reply_text(
        "🎓 *أهلاً بك في بوت جامعتك!*\n\n"
        "لربط حسابك، أرسل رقمك القومي:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )
    return AWAIT_NID

async def recv_national_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    nid = update.message.text.strip()
    if not nid.isdigit():
        await update.message.reply_text("❌ الرقم القومي يجب أن يحتوي أرقاماً فقط.")
        return AWAIT_NID

    s = await db_student_by_nid(nid)
    if not s:
        await update.message.reply_text("❌ الرقم القومي غير موجود. حاول مجدداً.")
        return AWAIT_NID
    if not s["is_active"]:
        await update.message.reply_text("❌ حسابك غير نشط. تواصل مع الإدارة.")
        return ConversationHandler.END

    await write_link_telegram(
        s["student_id"],
        update.effective_user.id,
        update.effective_user.username or "",
    )
    await write_upsert_state(update.effective_chat.id, s["student_id"], "MAIN_MENU")

    await update.message.reply_text(
        f"✅ تم ربط حسابك بنجاح!\n"
        f"أهلاً *{s['student_name']}* 🎉\n\n"
        "اختر من الأزرار:",
        parse_mode="Markdown",
        reply_markup=user_keyboard(),
    )
    return ConversationHandler.END

async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not await require_student(update, ctx):
        return
    await update.message.reply_text(
        "🏠 *القائمة الرئيسية*", parse_mode="Markdown",
        reply_markup=main_menu_kb(),
    )

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "ℹ️ *الأوامر المتاحة*\n\n"
        "/start       — ربط الحساب\n"
        "/schedule    — الجدول الأسبوعي\n"
        "/attendance  — سجل الحضور\n"
        "/grades      — الدرجات\n"
        "/gpa         — تفاصيل GPA\n"
        "/courses     — موادي المسجلة\n"
        "/enroll      — حجز سيكشن\n"
        "/regcourse   — تسجيل مادة جديدة\n"
        "/feedback    — تقييم أستاذ\n"
        "/profile     — بياناتي الشخصية\n"
        "/menu        — القائمة الرئيسية\n"
        "/cancel      — إلغاء العملية الحالية"
    )
    await update.effective_message.reply_text(text, parse_mode="Markdown")

async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    t0 = time.monotonic()
    s  = await require_student(update, ctx)
    if not s:
        return
    rows = await db_schedule(update.effective_user.id)
    text = fmt_schedule(rows)
    await update.effective_message.reply_text(text, parse_mode="Markdown")
    await write_log(
        update.effective_chat.id, update.effective_user.username or "",
        s["student_id"], "/schedule", "", text,
        int((time.monotonic() - t0) * 1000),
    )

async def cmd_attendance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    t0 = time.monotonic()
    s  = await require_student(update, ctx)
    if not s:
        return
    rows = await db_attendance(s["student_id"])
    text = fmt_attendance(rows)
    await update.effective_message.reply_text(text, parse_mode="Markdown")
    await write_log(
        update.effective_chat.id, update.effective_user.username or "",
        s["student_id"], "/attendance", "", text,
        int((time.monotonic() - t0) * 1000),
    )

async def cmd_grades(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    t0 = time.monotonic()
    s  = await require_student(update, ctx)
    if not s:
        return
    rows = await db_grades(s["student_id"])
    text = fmt_grades(rows)
    await update.effective_message.reply_text(text, parse_mode="Markdown")
    await write_log(
        update.effective_chat.id, update.effective_user.username or "",
        s["student_id"], "/grades", "", text,
        int((time.monotonic() - t0) * 1000),
    )

async def cmd_courses(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = await require_student(update, ctx)
    if not s:
        return
    courses = await db_registered_courses(s["student_id"])
    if not courses:
        await update.effective_message.reply_text(
            "📭 لا توجد مواد مسجلة.\nاضغط ➕ تسجيل مادة."
        )
        return
    lines = ["📚 *موادي المسجلة:*\n"]
    for r in courses:
        status = "✅ محجوز" if r["has_session"] else "⚠️ لم تحجز سيكشن بعد"
        lines.append(
            f"• *{r['course_code']}* — {r['course_name']}\n"
            f"  📅 {r['semester']} | سنة {r['study_year']} | {r['credit_hours']} ساعات\n"
            f"  {status}"
        )
    await update.effective_message.reply_text("\n".join(lines), parse_mode="Markdown")

async def cmd_profile(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s   = await require_student(update, ctx)
    if not s:
        return
    row = await db_student_profile(s["student_id"])
    await update.effective_message.reply_text(
        fmt_profile(row) if row else "❌ تعذّر جلب البيانات.",
        parse_mode="Markdown",
    )

async def cmd_gpa_detail(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = await require_student(update, ctx)
    if not s:
        return
    data = await db_gpa_detail(s["student_id"])
    await update.effective_message.reply_text(fmt_gpa_detail(data), parse_mode="Markdown")


#  ENROLL CONVERSATION

async def cmd_enroll_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    s = await require_student(update, ctx)
    if not s:
        return ConversationHandler.END

    sessions = await db_available_sessions(s["student_id"])
    if not sessions:
        await update.effective_message.reply_text(
            "📭 لا توجد جلسات متاحة للحجز."
        )
        return ConversationHandler.END

    ctx.user_data["enroll_sid"] = s["student_id"]
    buttons = []
    for r in sessions:
        start = r["start_time"].strftime("%H:%M") if r["start_time"] else "؟"
        end   = r["end_time"].strftime("%H:%M")   if r["end_time"]   else "؟"
        label = (
            f"{r['course_code']} | {r['session_type']} | "
            f"{_DAY_AR.get(r['day_of_week'], r['day_of_week'])} {start}-{end}"
        )
        buttons.append([InlineKeyboardButton(label, callback_data=f"enroll_{r['session_id']}")])
    buttons.append([InlineKeyboardButton("❌ إلغاء", callback_data="enroll_cancel")])

    await update.effective_message.reply_text(
        "📋 *حجز سيكشن جديد*\n\nاختر الجلسة:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return ENROLL_PICK

async def enroll_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()

    if q.data == "enroll_cancel":
        await q.edit_message_text("❌ تم الإلغاء.")
        return ConversationHandler.END

    session_id = int(q.data.split("_")[1])
    # Write via Kafka — optimistic success
    success = await write_enroll_session(ctx.user_data["enroll_sid"], session_id)

    if success:
        await q.edit_message_text(
            "✅ تم إرسال طلب الحجز بنجاح!\n"
            "سيظهر في جدولك خلال لحظات."
        )
    else:
        await q.edit_message_text("⚠️ أنت مسجّل في هذه الجلسة مسبقاً.")

    return ConversationHandler.END


#  COURSE REGISTRATION 

async def cmd_course_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    s = await require_student(update, ctx)
    if not s:
        return ConversationHandler.END

    courses = await db_available_courses(s["student_id"])
    if not courses:
        await update.effective_message.reply_text(
            "📭 لا توجد مواد إضافية متاحة للتسجيل."
        )
        return ConversationHandler.END

    ctx.user_data["course_sid"] = s["student_id"]
    buttons = [
        [InlineKeyboardButton(
            f"{r['course_code']} | {r['course_name']} | سنة {r['study_year']}",
            callback_data=f"creg_{r['course_id']}",
        )]
        for r in courses
    ]
    buttons.append([InlineKeyboardButton("❌ إلغاء", callback_data="creg_cancel")])

    await update.effective_message.reply_text(
        "➕ *تسجيل مادة جديدة*\n\nاختر المادة:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return COURSE_PICK

async def course_pick(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()

    if q.data == "creg_cancel":
        await q.edit_message_text("❌ تم الإلغاء.")
        return ConversationHandler.END

    course_id = int(q.data.split("_")[1])
    success   = await write_register_course(ctx.user_data["course_sid"], course_id)

    await q.edit_message_text(
        "✅ تم إرسال طلب تسجيل المادة!" if success
        else "⚠️ أنت مسجّل في هذه المادة مسبقاً."
    )
    return ConversationHandler.END


#  FEEDBACK CONVERSATION

async def feedback_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    s = await require_student(update, ctx)
    if not s:
        return ConversationHandler.END

    sessions = await db_enrolled_sessions(s["student_id"])
    if not sessions:
        await update.effective_message.reply_text("📭 لا توجد مواد للتقييم.")
        return ConversationHandler.END

    ctx.user_data["fb_sid"] = s["student_id"]
    buttons = [
        [InlineKeyboardButton(
            f"📘 {r['course_name']} [{r['session_type']}] — {r['professor_name']}",
            callback_data=f"fbsess_{r['session_id']}_{r['professor_id']}",
        )]
        for r in sessions
    ]
    buttons.append([InlineKeyboardButton("❌ إلغاء", callback_data="fb_cancel")])
    await write_upsert_state(update.effective_chat.id, s["student_id"], "AWAITING_FEEDBACK_SESSION")
    await update.effective_message.reply_text(
        "⭐ *تقييم أستاذ*\n\nاختر المادة:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return FB_SESSION

async def feedback_session(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    if q.data == "fb_cancel":
        await q.edit_message_text("❌ تم إلغاء التقييم.")
        return ConversationHandler.END

    _, session_id, professor_id = q.data.split("_")
    ctx.user_data["fb_session_id"]   = int(session_id)
    ctx.user_data["fb_professor_id"] = int(professor_id)
    await write_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "AWAITING_FEEDBACK_RATING")
    await q.edit_message_text(
        "📊 اختر تقييمك من 1 إلى 5:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⭐ 1",      callback_data="fbrate_1"),
                InlineKeyboardButton("⭐⭐ 2",    callback_data="fbrate_2"),
                InlineKeyboardButton("⭐⭐⭐ 3",  callback_data="fbrate_3"),
            ],
            [
                InlineKeyboardButton("⭐⭐⭐⭐ 4",   callback_data="fbrate_4"),
                InlineKeyboardButton("⭐⭐⭐⭐⭐ 5", callback_data="fbrate_5"),
            ],
        ]),
    )
    return FB_RATING

async def feedback_rating(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    await q.answer()
    ctx.user_data["fb_rating"] = int(q.data.split("_")[1])
    await write_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "AWAITING_FEEDBACK_COMMENT")
    await q.edit_message_text(
        f"اخترت: {'⭐' * ctx.user_data['fb_rating']}\n\n✍️ أضف تعليقاً أو أرسل /skip:"
    )
    return FB_COMMENT

async def feedback_comment(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    comment = "" if update.message.text.strip() == "/skip" else update.message.text.strip()
    await write_feedback(
        ctx.user_data["fb_sid"],
        ctx.user_data["fb_session_id"],
        ctx.user_data["fb_professor_id"],
        ctx.user_data["fb_rating"],
        comment,
    )
    await write_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "MAIN_MENU")
    ctx.user_data.clear()
    await update.message.reply_text(
        "🎉 شكراً! تم حفظ تقييمك بنجاح.", reply_markup=user_keyboard(),
    )
    return ConversationHandler.END


#  ROUTING

async def text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    dispatch = {
        "📅 جدولي":         cmd_schedule,
        "✅ الحضور":         cmd_attendance,
        "🎓 درجاتي":        cmd_grades,
        "📊 تفاصيل GPA":    cmd_gpa_detail,
        "📚 موادي":          cmd_courses,
        "👤 بياناتي":        cmd_profile,
        "📋 حجز سيكشن":     cmd_enroll_start,
        "➕ تسجيل مادة":     cmd_course_start,
        "⭐ تقييم أستاذ":   feedback_start,
        "ℹ️ مساعدة":        cmd_help,
    }
    fn = dispatch.get(update.message.text)
    if fn:
        await fn(update, ctx)

async def menu_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.callback_query.answer()
    dispatch = {
        "schedule":   cmd_schedule,
        "attendance": cmd_attendance,
        "grades":     cmd_grades,
        "courses":    cmd_courses,
    }
    fn = dispatch.get(update.callback_query.data)
    if fn:
        await fn(update, ctx)

async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    st = await db_get_state(update.effective_chat.id)
    await write_upsert_state(
        update.effective_chat.id,
        st["student_id"] if st else None, "IDLE",
    )
    ctx.user_data.clear()
    await update.message.reply_text("❌ تم الإلغاء.", reply_markup=user_keyboard())
    return ConversationHandler.END

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception:", exc_info=ctx.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("⚠️ حدث خطأ. حاول مجدداً أو أرسل /menu.")



async def post_init(app: Application) -> None:
    global _pool, _producer

    # 1. DB connection pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL, ssl="require",
        min_size=2, max_size=5,
        max_inactive_connection_lifetime=300,
    )
    logger.info("✅ PostgreSQL pool ready")

    # 2. Kafka producer
    if KAFKA_ENABLED:
        try:
            _producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=None,     
                acks="all",                
                enable_idempotence=True,   
                max_batch_size=65536,
                linger_ms=5,               
                compression_type="gzip",
                request_timeout_ms=30_000,
                retry_backoff_ms=500,
            )
            await _producer.start()
            logger.info("✅ Kafka producer ready [%s]", KAFKA_BOOTSTRAP_SERVERS)
        except KafkaConnectionError as exc:
            logger.warning(
                "⚠️  Kafka unavailable — running in DB-direct fallback mode: %s", exc
            )
            _producer = None

async def post_shutdown(app: Application) -> None:
    if _producer:
        await _producer.stop()
        logger.info("🔌 Kafka producer stopped.")
    if _pool:
        await _pool.close()
        logger.info("🔌 PostgreSQL pool closed.")


def build_app() -> Application:
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    onboarding = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            AWAIT_NID: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & ~button_filter,
                    recv_national_id,
                )
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    feedback = ConversationHandler(
        entry_points=[
            CommandHandler("feedback", feedback_start),
            CallbackQueryHandler(feedback_start, pattern="^feedback$"),
        ],
        states={
            FB_SESSION: [CallbackQueryHandler(feedback_session, pattern=r"^(fbsess_\d+_\d+|fb_cancel)$")],
            FB_RATING:  [CallbackQueryHandler(feedback_rating,  pattern=r"^fbrate_[1-5]$")],
            FB_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND & ~button_filter, feedback_comment),
                CommandHandler("skip", feedback_comment),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    enroll_conv = ConversationHandler(
        entry_points=[
            CommandHandler("enroll", cmd_enroll_start),
            MessageHandler(filters.Text(["📋 حجز سيكشن"]), cmd_enroll_start),
        ],
        states={
            ENROLL_PICK: [CallbackQueryHandler(enroll_pick, pattern=r"^(enroll_\d+|enroll_cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    course_conv = ConversationHandler(
        entry_points=[
            CommandHandler("regcourse", cmd_course_start),
            MessageHandler(filters.Text(["➕ تسجيل مادة"]), cmd_course_start),
        ],
        states={
            COURSE_PICK: [CallbackQueryHandler(course_pick, pattern=r"^(creg_\d+|creg_cancel)$")],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        allow_reentry=True,
    )

    app.add_handler(onboarding)
    app.add_handler(feedback)
    app.add_handler(enroll_conv)
    app.add_handler(course_conv)

    for cmd, fn in [
        ("menu",       cmd_menu),
        ("schedule",   cmd_schedule),
        ("attendance", cmd_attendance),
        ("grades",     cmd_grades),
        ("gpa",        cmd_gpa_detail),
        ("profile",    cmd_profile),
        ("enroll",     cmd_enroll_start),
        ("regcourse",  cmd_course_start),
        ("help",       cmd_help),
    ]:
        app.add_handler(CommandHandler(cmd, fn))

    app.add_handler(
        CallbackQueryHandler(menu_callback, pattern=r"^(schedule|attendance|grades|courses)$")
    )
    app.add_handler(MessageHandler(button_filter, text_router))
    app.add_error_handler(on_error)
    return app


if __name__ == "__main__":
    build_app().run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )
