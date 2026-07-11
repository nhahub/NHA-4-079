# pip install python-telegram-bot asyncpg nest_asyncio

import json, logging, time
from typing import Optional


import nest_asyncio
nest_asyncio.apply()

import asyncpg
from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove, Update,
)
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, ConversationHandler, MessageHandler, filters,
)


BOT_TOKEN = "put Bot token"

DATABASE_URL = ("put database url")

logging.basicConfig(
    format="%(asctime)s │ %(levelname)s │ %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

AWAIT_NID, FB_SESSION, FB_RATING, FB_COMMENT, ENROLL_PICK, COURSE_PICK = range(6)

_pool: asyncpg.Pool | None = None

def pool() -> asyncpg.Pool:
    assert _pool is not None, "DB pool not initialized."
    return _pool


#  DATABASE LAYER

async def db_student_by_tg(tg_id: int) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            "SELECT * FROM fn_get_student_by_telegram($1)", tg_id
        )

async def db_student_by_nid(nid: str) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            """SELECT student_id, student_name, study_year, is_active
               FROM   students WHERE national_id = $1""",
            nid,
        )

async def db_link_telegram(student_id: int, tg_id: int, username: str) -> None:
    async with pool().acquire() as c:
        await c.execute(
            "UPDATE students SET telegram_id=$1, telegram_username=$2 WHERE student_id=$3",
            tg_id, username, student_id,
        )

async def db_upsert_state(
    chat_id: int, student_id: Optional[int],
    state: str, data: dict | None = None,
) -> None:
    async with pool().acquire() as c:
        await c.execute(
            "SELECT fn_upsert_bot_state($1,$2,$3,$4::jsonb)",
            chat_id, student_id, state,
            json.dumps(data) if data else None,
        )

async def db_get_state(chat_id: int) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            """SELECT current_state, state_data, student_id FROM bot_states
               WHERE  telegram_chat_id=$1 AND expires_at > NOW()""",
            chat_id,
        )

async def db_schedule(tg_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   ses.session_type::text  AS session_type,
                   ses.day_of_week::text   AS day_of_week,
                   ses.start_time, ses.end_time,
                   ses.location,
                   p.professor_name,
                   e.semester::text        AS semester
            FROM   students    s
            JOIN   enrollments e   ON s.student_id    = e.student_id
            JOIN   sessions    ses ON e.session_id    = ses.session_id
            JOIN   courses     c   ON ses.course_id   = c.course_id
            JOIN   professors  p   ON ses.professor_id = p.professor_id
            WHERE  s.telegram_id = $1
              AND  s.is_active   = TRUE
            ORDER BY
                CASE ses.day_of_week::text
                    WHEN 'Sunday'    THEN 0 WHEN 'Monday'    THEN 1
                    WHEN 'Tuesday'   THEN 2 WHEN 'Wednesday' THEN 3
                    WHEN 'Thursday'  THEN 4 WHEN 'Friday'    THEN 5
                    ELSE 6
                END,
                ses.start_time
            """,
            tg_id,
        )

async def db_attendance(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_name,
                   ses.session_type::text                          AS session_type,
                   COUNT(a.attendance_id) FILTER
                       (WHERE a.status = 'Present')                AS present,
                   COUNT(a.attendance_id) FILTER
                       (WHERE a.status = 'Absent')                 AS absent,
                   COUNT(a.attendance_id) FILTER
                       (WHERE a.status = 'Late')                   AS late,
                   COUNT(a.attendance_id) FILTER
                       (WHERE a.status = 'Excused')                AS excused,
                   COUNT(a.attendance_id)                          AS total
            FROM   enrollments    e
            JOIN   sessions       ses ON e.session_id  = ses.session_id
            JOIN   courses        c   ON ses.course_id  = c.course_id
            LEFT JOIN attendance  a   ON a.student_id  = e.student_id
                                     AND a.session_id  = e.session_id
            WHERE  e.student_id = $1
            GROUP  BY c.course_name, ses.session_type
            ORDER  BY c.course_name
            """,
            student_id,
        )

async def db_grades(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   e.grade::text, e.gpa, e.semester::text AS semester
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

async def db_save_feedback(
    student_id: int, session_id: int,
    professor_id: int, rating: int, comment: str,
) -> None:
    async with pool().acquire() as c:
        await c.execute(
            """
            INSERT INTO feedback (student_id, session_id, professor_id, rating, comments)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (student_id, session_id)
            DO UPDATE SET rating=$4, comments=$5
            """,
            student_id, session_id, professor_id, rating, comment,
        )

async def db_log(
    chat_id: int, username: str, student_id: Optional[int],
    command: str, msg_in: str, msg_out: str, ms: int,
) -> None:
    try:
        async with pool().acquire() as c:
            await c.execute(
                """
                INSERT INTO bot_interaction_logs
                    (telegram_chat_id, telegram_username, student_id,
                     command, message_received, bot_response, response_time_ms)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                chat_id, username, student_id, command, msg_in, msg_out, ms,
            )
    except Exception as e:
        logger.warning("Log failed: %s", e)

async def db_student_profile(student_id: int) -> Optional[asyncpg.Record]:
    async with pool().acquire() as c:
        return await c.fetchrow(
            """
            SELECT s.student_name, s.national_id, s.phone,
                   s.address, s.birthdate,
                   s.gender::text   AS gender,
                   s.study_year, s.enrollment_year,
                   s.telegram_username,
                   d.department_name,
                   f.faculty_name
            FROM   students    s
            JOIN   departments d ON s.department_id = d.department_id
            JOIN   faculties   f ON d.faculty_id    = f.faculty_id
            WHERE  s.student_id = $1
            """,
            student_id,
        )


async def db_gpa_detail(student_id: int) -> dict:
    async with pool().acquire() as c:
        by_sem = await c.fetch(
            """
            SELECT e.semester::text                  AS semester,
                   ROUND(AVG(e.gpa)::numeric, 2)     AS avg_gpa,
                   COUNT(*) FILTER (WHERE e.gpa IS NOT NULL) AS graded,
                   COUNT(*)                           AS total
            FROM   enrollments e
            WHERE  e.student_id = $1
            GROUP  BY e.semester
            ORDER  BY e.semester DESC
            """,
            student_id,
        )
        overall = await c.fetchrow(
            """
            SELECT
                ROUND(AVG(gpa)::numeric, 2)              AS overall_gpa,
                COUNT(*) FILTER (WHERE gpa IS NOT NULL)  AS graded_courses,
                COUNT(*)                                  AS total_enrolled
            FROM enrollments
            WHERE student_id = $1
            """,
            student_id,
        )
        dist = await c.fetch(
            """
            SELECT grade::text AS grade, COUNT(*) AS cnt
            FROM   enrollments
            WHERE  student_id = $1 AND grade IS NOT NULL
            GROUP  BY grade
            ORDER  BY cnt DESC
            """,
            student_id,
        )
    return {"by_semester": by_sem, "overall": overall, "distribution": dist}


async def db_available_sessions(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT ses.session_id,
                   c.course_code, c.course_name,
                   ses.session_type::text AS session_type,
                   ses.day_of_week::text  AS day_of_week,
                   ses.start_time, ses.end_time,
                   ses.location,
                   ses.semester::text     AS semester,
                   p.professor_name
            FROM   sessions   ses
            JOIN   courses    c ON ses.course_id    = c.course_id
            JOIN   professors p ON ses.professor_id = p.professor_id
            WHERE  ses.course_id IN (
                       -- فقط مواد سجّلها الطالب
                       SELECT course_id
                       FROM   student_courses
                       WHERE  student_id = $1
                   )
              AND  ses.session_id NOT IN (
                       -- استثنِ السيكشنات المحجوزة مسبقاً
                       SELECT session_id
                       FROM   enrollments
                       WHERE  student_id = $1
                   )
            ORDER  BY c.course_name, ses.session_type::text
            LIMIT  30
            """,
            student_id,
        )


async def db_enroll_session(student_id: int, session_id: int) -> bool:
 
    async with pool().acquire() as c:
        result = await c.execute(
            """
            INSERT INTO enrollments (student_id, session_id, semester)
            SELECT $1, $2, semester FROM sessions WHERE session_id = $2
            ON CONFLICT (student_id, session_id) DO NOTHING
            """,
            student_id, session_id,
        )
    return result.endswith("1")  # "INSERT 0 1" → True



async def db_available_courses(student_id: int) -> list:
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_id, c.course_code, c.course_name,
                   c.credit_hours, c.semester::text AS semester,
                   c.study_year
            FROM   courses c
            WHERE  c.department_id = (
                       SELECT department_id FROM students WHERE student_id = $1
                   )
              AND  c.course_id NOT IN (
                       SELECT course_id FROM student_courses WHERE student_id = $1
                   )
            ORDER  BY c.study_year, c.course_name
            LIMIT  30
            """,
            student_id,
        )


async def db_register_course(student_id: int, course_id: int) -> bool:
    from datetime import datetime
    academic_year = f"{datetime.now().year}/{datetime.now().year + 1}"
    async with pool().acquire() as c:
        result = await c.execute(
            """
            INSERT INTO student_courses
                (student_id, course_id, semester, academic_year)
            SELECT $1, $2, c.semester, $3
            FROM   courses c
            WHERE  c.course_id = $2
              AND  c.semester IS NOT NULL
            ON CONFLICT (student_id, course_id, semester, academic_year)
            DO NOTHING
            """,
            student_id, course_id, academic_year,
        )
    return result.endswith("1")

async def db_registered_courses(student_id: int) -> list:
 
    async with pool().acquire() as c:
        return await c.fetch(
            """
            SELECT c.course_code, c.course_name,
                   sc.semester::text  AS semester,
                   sc.academic_year,
                   c.credit_hours,
                   c.study_year,
                   -- هل الطالب حجز سيكشن لهذه المادة؟
                   EXISTS (
                       SELECT 1 FROM enrollments e
                       JOIN sessions ses ON e.session_id = ses.session_id
                       WHERE e.student_id = $1
                         AND ses.course_id = c.course_id
                   ) AS has_session
            FROM   student_courses sc
            JOIN   courses         c ON sc.course_id = c.course_id
            WHERE  sc.student_id = $1
            ORDER  BY sc.academic_year DESC, c.course_name
            """,
            student_id,
        )

#  UI HELPERS

def user_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📅 جدولي",          "✅ الحضور"],
            ["🎓 درجاتي",         "📊 تفاصيل GPA"],
            ["📚 موادي",           "👤 بياناتي"],
            ["📋 حجز سيكشن",     "➕ تسجيل مادة"],
            ["⭐ تقييم أستاذ",    "ℹ️ مساعدة"],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="اختر من الأزرار أو اكتب أمراً...",
    )

def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 جدولي",      callback_data="schedule"),
            InlineKeyboardButton("✅ الحضور",      callback_data="attendance"),
        ],
        [
            InlineKeyboardButton("🎓 درجاتي",     callback_data="grades"),
            InlineKeyboardButton("📚 موادي",       callback_data="courses"),
        ],
        [InlineKeyboardButton("⭐ تقييم أستاذ",   callback_data="feedback")],
    ])

BUTTON_TEXTS = [
    "📅 جدولي", "✅ الحضور",
    "🎓 درجاتي", "📊 تفاصيل GPA",
    "📚 موادي",  "👤 بياناتي",
    "📋 حجز سيكشن", "➕ تسجيل مادة",
    "⭐ تقييم أستاذ", "ℹ️ مساعدة",
]
button_filter = filters.Text(BUTTON_TEXTS)

_DAY_AR = {
    "Sunday": "الأحد",    "Monday":    "الاثنين", "Tuesday":   "الثلاثاء",
    "Wednesday": "الأربعاء", "Thursday": "الخميس",
    "Friday": "الجمعة",   "Saturday":  "السبت",
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
        pct   = round(r["present"] / r["total"] * 100) if r["total"] else 0
        emoji = "🟢" if pct >= 75 else ("🟡" if pct >= 60 else "🔴")
        lines.append(
            f"{emoji} *{r['course_name']}* [{r['session_type']}]\n"
            f"   حضر: {r['present']} | غياب: {r['absent']} | "
            f"تأخر: {r['late']} | معذور: {r['excused']}\n"
            f"   نسبة الحضور: *{pct}%*\n"
        )
    return "\n".join(lines)

def fmt_grades(rows: list) -> str:
    if not rows:
        return "📭 لا توجد درجات مسجلة بعد."
    lines = ["🎓 *درجاتي*\n"]
    for r in rows:
        gpa_str = f"  (GPA: {r['gpa']})" if r["gpa"] else ""
        lines.append(
            f"📘 *{r['course_code']}* — {r['course_name']}\n"
            f"   التقدير: *{r['grade'] or 'لم يُسجَّل'}*{gpa_str}  |  {r['semester']}\n"
        )
    return "\n".join(lines)

def fmt_profile(r: asyncpg.Record) -> str:
    birth = r["birthdate"].strftime("%Y-%m-%d") if r["birthdate"] else "—"
    return (
        "👤 *بياناتك الشخصية*\n\n"
        f"📛 الاسم:         *{r['student_name']}*\n"
        f"🆔 الرقم القومي: `{r['national_id']}`\n"
        f"📱 الهاتف:        {r['phone'] or '—'}\n"
        f"🎂 تاريخ الميلاد: {birth}\n"
        f"⚧ الجنس:         {r['gender'] or '—'}\n"
        f"🏫 الكلية:        {r['faculty_name']}\n"
        f"📐 القسم:         {r['department_name']}\n"
        f"📅 سنة الالتحاق:  {r['enrollment_year'] or '—'}\n"
        f"📚 السنة الدراسية: السنة {r['study_year']}\n"
        f"💬 Telegram:     @{r['telegram_username'] or '—'}\n"
    )


def fmt_gpa_detail(data: dict) -> str:
    overall = data["overall"]
    by_sem  = data["by_semester"]
    dist    = data["distribution"]

    total_enrolled = overall["total_enrolled"] if overall else 0
    graded         = overall["graded_courses"] if overall else 0

    # مواد مسجلة بس مفيش درجات بعد
    if total_enrolled == 0:
        return "📭 لا توجد مواد مسجلة بعد."

    if graded == 0:
        return (
            f"📊 *تفاصيل GPA*\n\n"
            f"📚 مواد مسجلة: *{total_enrolled}*\n"
            f"⏳ لا توجد درجات مسجّلة بعد.\n"
            f"_ستظهر تفاصيل GPA بعد رصد الدرجات._"
        )

    gpa_val = float(overall["overall_gpa"])
    if gpa_val >= 3.7:
        label = "ممتاز 🏆"
    elif gpa_val >= 3.0:
        label = "جيد جداً ⭐"
    elif gpa_val >= 2.0:
        label = "جيد 👍"
    else:
        label = "مقبول ⚠️"

    lines = [
        "📊 *تفاصيل GPA*\n",
        f"🎯 GPA الإجمالي: *{gpa_val:.2f} / 4.00*  —  {label}",
        f"📚 مواد مسجلة: *{total_enrolled}*  |  مقيّمة: *{graded}*\n",
    ]

    if by_sem:
        lines.append("*GPA لكل فصل:*")
        for r in by_sem:
            if r["graded"] == 0:
                lines.append(f"  📅 {r['semester']}  —  {r['total']} مادة  ⏳ لا درجات بعد")
                continue
            bar_len = int(float(r["avg_gpa"]) / 4.0 * 10)
            bar     = "█" * bar_len + "░" * (10 - bar_len)
            lines.append(
                f"  📅 {r['semester']}\n"
                f"     {bar} *{r['avg_gpa']}*"
                f"  ({r['graded']}/{r['total']} مواد)"
            )
        lines.append("")

    if dist:
        lines.append("*توزيع الدرجات:*")
        for r in dist:
            lines.append(f"  • {r['grade']:3s} — {r['cnt']} مادة")

    return "\n".join(lines)


async def require_student(
    update: Update, ctx: ContextTypes.DEFAULT_TYPE
) -> Optional[asyncpg.Record]:
    s = await db_student_by_tg(update.effective_user.id)
    if not s or not s["is_active"]:
        await update.effective_message.reply_text(
            "👋 حسابك غير مربوط بعد.\nأرسل /start للتسجيل."
        )
        return None
    return s


#  /start — ONBOARDING

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    s = await db_student_by_tg(update.effective_user.id)
    if s and s["is_active"]:
        await db_upsert_state(update.effective_chat.id, s["student_id"], "MAIN_MENU")
        # أرسل الأزرار الدائمة أولاً ثم القائمة الـ Inline
        await update.message.reply_text(
            f"👋 أهلاً *{s['student_name']}*! الأزرار جاهزة 👇",
            parse_mode="Markdown",
            reply_markup=user_keyboard(),
        )
        await update.message.reply_text("📋 القائمة:", reply_markup=main_menu_kb())
        return ConversationHandler.END

    await db_upsert_state(update.effective_chat.id, None, "AWAITING_ID")
    await update.message.reply_text(
        "🎓 *مرحباً بك في بوت الجامعة!*\n\nأرسل رقمك القومي لربط حسابك:",
        parse_mode="Markdown",
    )
    return AWAIT_NID

async def recv_national_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    t0   = time.monotonic()
    nid  = update.message.text.strip()
    user = update.effective_user
    s    = await db_student_by_nid(nid)

    if not s:
        await update.message.reply_text(
            "❌ الرقم القومي غير موجود.\n"
            "تحقق وأعد الإرسال أو تواصل مع الإدارة."
        )
        return AWAIT_NID

    if not s["is_active"]:
        await update.message.reply_text("⚠️ هذا الحساب موقوف. تواصل مع الإدارة.")
        return AWAIT_NID

    await db_link_telegram(s["student_id"], user.id, user.username or "")
    await db_upsert_state(update.effective_chat.id, s["student_id"], "MAIN_MENU")

    reply = f"✅ تم الربط بنجاح! أهلاً *{s['student_name']}* 🎉"
    await update.message.reply_text(
        reply, parse_mode="Markdown", reply_markup=user_keyboard()
    )
    await update.message.reply_text("📋 القائمة:", reply_markup=main_menu_kb())

    await db_log(
        update.effective_chat.id, user.username or "",
        s["student_id"], "/start", "[nid_hidden]", reply,
        int((time.monotonic() - t0) * 1000),
    )
    return ConversationHandler.END


#  STANDALONE COMMANDS

async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = await require_student(update, ctx)
    if not s:
        return
    await update.message.reply_text("📋 القائمة:", reply_markup=main_menu_kb())

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "🤖 *أوامر البوت:*\n\n"
        "📅 /schedule   — جدولك الأسبوعي\n"
        "✅ /attendance — سجل الحضور\n"
        "🎓 /grades     — درجاتك وGPA\n"
        "📚 /courses    — موادك المسجلة\n"
        "⭐ /feedback   — تقييم أستاذ\n"
        "📋 /menu       — القائمة الرئيسية\n"
        "❌ /cancel     — إلغاء العملية الحالية\n\n"
        "_يمكنك استخدام الأزرار أسفل الشاشة مباشرةً_ 👇",
        parse_mode="Markdown",
    )

async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    t0 = time.monotonic()
    s  = await require_student(update, ctx)
    if not s:
        return
    rows = await db_schedule(update.effective_user.id)
    text = fmt_schedule(rows)
    await update.effective_message.reply_text(text, parse_mode="Markdown")
    await db_log(
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
    await db_log(
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
    await db_log(
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
            "📭 لا توجد مواد مسجلة حالياً.\n"
            "اضغط ➕ تسجيل مادة لإضافة مواد."
        )
        return

    lines = ["📚 *موادي المسجلة:*\n"]
    for r in courses:
        # أيقونة تُشير هل حجز سيكشن أم لا
        session_status = "✅ محجوز" if r["has_session"] else "⚠️ لم تحجز سيكشن بعد"
        lines.append(
            f"• *{r['course_code']}* — {r['course_name']}\n"
            f"  📅 {r['semester']} | سنة {r['study_year']} "
            f"| {r['credit_hours']} ساعات\n"
            f"  {session_status}"
        )

    lines.append(
        "\n_💡 المواد التي عليها ⚠️ لن تظهر في الجدول أو الدرجات "
        "حتى تحجز سيكشن من 📋 حجز سيكشن_"
    )
    await update.effective_message.reply_text(
        "\n".join(lines), parse_mode="Markdown"
    )

async def cmd_profile(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = await require_student(update, ctx)
    if not s:
        return
    row  = await db_student_profile(s["student_id"])
    text = fmt_profile(row) if row else "❌ تعذّر جلب البيانات."
    await update.effective_message.reply_text(text, parse_mode="Markdown")


async def cmd_gpa_detail(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    s = await require_student(update, ctx)
    if not s:
        return
    data = await db_gpa_detail(s["student_id"])
    text = fmt_gpa_detail(data)
    await update.effective_message.reply_text(text, parse_mode="Markdown")


async def cmd_enroll_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    s = await require_student(update, ctx)
    if not s:
        return ConversationHandler.END

    sessions = await db_available_sessions(s["student_id"])
    if not sessions:
        await update.effective_message.reply_text(
            "📭 لا توجد جلسات متاحة للحجز حالياً.\n"
            "ربما أنت مسجّل في كل الجلسات المتاحة."
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
        "📋 *حجز سيكشن جديد*\n\nاختر الجلسة التي تريد التسجيل فيها:",
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
    success    = await db_enroll_session(ctx.user_data["enroll_sid"], session_id)

    if success:
        await q.edit_message_text(
            "✅ تم الحجز بنجاح!\n"
            "اضغط 📅 جدولي لمشاهدة موعد السيكشن."
        )
    else:
        await q.edit_message_text("⚠️ أنت مسجّل في هذه الجلسة مسبقاً.")

    return ConversationHandler.END


async def cmd_course_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
    s = await require_student(update, ctx)
    if not s:
        return ConversationHandler.END

    courses = await db_available_courses(s["student_id"])
    if not courses:
        await update.effective_message.reply_text(
            "📭 لا توجد مواد إضافية متاحة للتسجيل في قسمك."
        )
        return ConversationHandler.END

    ctx.user_data["course_sid"] = s["student_id"]
    buttons = []
    for r in courses:
        label = (
            f"{r['course_code']} | {r['course_name']} | "
            f"سنة {r['study_year']} | {r['semester']} | "
            f"{r['credit_hours']} ساعات"
        )
        buttons.append([InlineKeyboardButton(label, callback_data=f"creg_{r['course_id']}")])
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
    success   = await db_register_course(ctx.user_data["course_sid"], course_id)

    if success:
        await q.edit_message_text(
            "✅ تم تسجيل المادة بنجاح!\n"
            "اضغط 📚 موادي لمشاهدة قائمة موادك."
        )
    else:
        await q.edit_message_text("⚠️ أنت مسجّل في هذه المادة مسبقاً.")
    return ConversationHandler.END


async def text_router(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    dispatch = {
        "📅 جدولي":          cmd_schedule,
        "✅ الحضور":          cmd_attendance,
        "🎓 درجاتي":         cmd_grades,
        "📊 تفاصيل GPA":     cmd_gpa_detail,    # ← جديد
        "📚 موادي":           cmd_courses,
        "👤 بياناتي":         cmd_profile,        # ← جديد
        "📋 حجز سيكشن":      cmd_enroll_start,   # ← جديد
        "➕ تسجيل مادة":      cmd_course_start,   # ← جديد
        "⭐ تقييم أستاذ":    feedback_start,
        "ℹ️ مساعدة":         cmd_help,
    }
    fn = dispatch.get(update.message.text)
    if fn:
        await fn(update, ctx)


#  /feedback — MULTI-STEP CONVERSATION

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

    await db_upsert_state(update.effective_chat.id, s["student_id"], "AWAITING_FEEDBACK_SESSION")
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

    await db_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "AWAITING_FEEDBACK_RATING")
    await q.edit_message_text(
        "📊 اختر تقييمك من 1 إلى 5:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("⭐ 1",     callback_data="fbrate_1"),
                InlineKeyboardButton("⭐⭐ 2",   callback_data="fbrate_2"),
                InlineKeyboardButton("⭐⭐⭐ 3", callback_data="fbrate_3"),
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
    await db_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "AWAITING_FEEDBACK_COMMENT")
    await q.edit_message_text(
        f"اخترت: {'⭐' * ctx.user_data['fb_rating']}\n\n"
        "✍️ أضف تعليقاً أو أرسل /skip:"
    )
    return FB_COMMENT

async def feedback_comment(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    comment = (
        "" if update.message.text.strip() == "/skip"
        else update.message.text.strip()
    )
    await db_save_feedback(
        ctx.user_data["fb_sid"],
        ctx.user_data["fb_session_id"],
        ctx.user_data["fb_professor_id"],
        ctx.user_data["fb_rating"],
        comment,
    )
    await db_upsert_state(update.effective_chat.id, ctx.user_data["fb_sid"], "MAIN_MENU")
    ctx.user_data.clear()
    await update.message.reply_text(
        "🎉 شكراً! تم حفظ تقييمك بنجاح.",
        reply_markup=user_keyboard(),
    )
    return ConversationHandler.END


#  CALLBACK ROUTER (Inline buttons)

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
    await db_upsert_state(
        update.effective_chat.id,
        st["student_id"] if st else None, "IDLE",
    )
    ctx.user_data.clear()
    await update.message.reply_text("❌ تم الإلغاء.", reply_markup=user_keyboard())
    return ConversationHandler.END

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception:", exc_info=ctx.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(
            "⚠️ حدث خطأ. حاول مجدداً أو أرسل /menu."
        )


#  APP WIRING

async def post_init(app: Application) -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL,
        ssl="require",                      # ← مطلوب لـ Supabase
        min_size=2,
        max_size=5,
        max_inactive_connection_lifetime=300,
    )
    logger.info("✅ Supabase pool ready")

async def post_shutdown(app: Application) -> None:
    if _pool:
        await _pool.close()
        logger.info("🔌 Pool closed.")

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
                # ← استثنِ أزرار ReplyKeyboard من حقل الرقم القومي
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
            FB_SESSION: [
                CallbackQueryHandler(
                    feedback_session,
                    pattern=r"^(fbsess_\d+_\d+|fb_cancel)$",
                )
            ],
            FB_RATING: [
                CallbackQueryHandler(feedback_rating, pattern=r"^fbrate_[1-5]$")
            ],
            FB_COMMENT: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND & ~button_filter,
                    feedback_comment,
                ),
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
            ENROLL_PICK: [
                CallbackQueryHandler(enroll_pick, pattern=r"^(enroll_\d+|enroll_cancel)$")
            ],
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
            COURSE_PICK: [
                CallbackQueryHandler(course_pick, pattern=r"^(creg_\d+|creg_cancel)$")
            ],
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
        CallbackQueryHandler(
            menu_callback,
            pattern=r"^(schedule|attendance|grades|courses)$",
        )
    )
    app.add_handler(MessageHandler(button_filter, text_router))

    app.add_error_handler(on_error)
    return app



build_app().run_polling(
    drop_pending_updates=True,
    allowed_updates=Update.ALL_TYPES,
)
