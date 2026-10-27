import asyncio
import logging
import math
import os
import random
import re
import sys
from datetime import datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import BaseFilter, Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                           KeyboardButton, Message, ReplyKeyboardMarkup)
from sqlalchemy import (BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text, delete,
                        desc, func, or_, select, update)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ════════════════════════ SOZLAMALAR ════════════════════════
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {int(x) for x in re.split(r"[,; ]+", os.getenv("ADMIN_ID", "")) if x.strip().lstrip("-").isdigit()}
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///data/bot.db").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Tashkent"))
CODE_PREFIX = os.getenv("CODE_PREFIX", "KINO").strip().upper() or "KINO"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("kinobot")


def now() -> datetime:
    return datetime.now(TZ).replace(tzinfo=None)


def today_start() -> datetime:
    return now().replace(hour=0, minute=0, second=0, microsecond=0)


def fmt(dt) -> str:
    return dt.strftime("%d.%m.%Y %H:%M") if dt else "—"


def e(v) -> str:
    return escape(str(v)) if v is not None else ""


# ════════════════════════ BAZA MODELLARI ════════════════════════
class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str] = mapped_column(String(256), default="")
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    joined_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    last_active: Mapped[datetime] = mapped_column(DateTime, default=now)


class Movie(Base):
    __tablename__ = "movies"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    file_id: Mapped[str] = mapped_column(String(512))
    file_type: Mapped[str] = mapped_column(String(16), default="video")
    poster_id: Mapped[str | None] = mapped_column(String(512), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    genre: Mapped[str | None] = mapped_column(String(128), nullable=True)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    language: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


class MovieCode(Base):
    __tablename__ = "movie_codes"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    movie_id: Mapped[int | None] = mapped_column(ForeignKey("movies.id", ondelete="SET NULL"),
                                                  unique=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


class MovieRequest(Base):
    __tablename__ = "movie_requests"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    movie_id: Mapped[int | None] = mapped_column(ForeignKey("movies.id", ondelete="SET NULL"),
                                                  nullable=True, index=True)
    code_text: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now, index=True)


engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db() -> None:
    url = make_url(DATABASE_URL)
    if url.get_backend_name() == "sqlite" and url.database and url.database != ":memory:":
        Path(url.database).parent.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# ════════════════════════ CRUD ════════════════════════
async def get_user(s: AsyncSession, tg_id: int):
    return await s.scalar(select(User).where(User.tg_id == tg_id))


async def upsert_user(s: AsyncSession, tg):
    name = " ".join(p for p in (tg.first_name, tg.last_name) if p)
    u = await get_user(s, tg.id)
    if u is None:
        u = User(tg_id=tg.id, username=tg.username, full_name=name, joined_at=now(), last_active=now())
        s.add(u)
        await s.flush()
        return u
    u.username, u.full_name, u.last_active = tg.username, name, now()
    return u


async def scalar_count(s: AsyncSession, q) -> int:
    return await s.scalar(q) or 0


async def get_code(s: AsyncSession, code: str):
    return await s.scalar(select(MovieCode).where(MovieCode.code == code))


async def code_of_movie(s: AsyncSession, movie_id: int):
    return await s.scalar(select(MovieCode).where(MovieCode.movie_id == movie_id))


async def movie_by_code(s: AsyncSession, code: str):
    return await s.scalar(select(Movie).join(MovieCode, MovieCode.movie_id == Movie.id)
                          .where(MovieCode.code == code))


async def search_movies(s: AsyncSession, text: str):
    p = f"%{text.strip()}%"
    q = (select(Movie, MovieCode.code).outerjoin(MovieCode, MovieCode.movie_id == Movie.id)
         .where(or_(Movie.title.ilike(p), MovieCode.code.ilike(p))).order_by(desc(Movie.id)).limit(10))
    return [(m, c) for m, c in (await s.execute(q)).all()]


async def attach_code(s: AsyncSession, obj: MovieCode, movie_id: int) -> None:
    await s.execute(update(MovieCode).where(MovieCode.movie_id == movie_id, MovieCode.id != obj.id)
                    .values(movie_id=None))
    obj.movie_id = movie_id
    await s.flush()


async def top_movies(s: AsyncSession, limit: int = 10):
    cnt = func.count(MovieRequest.id).label("cnt")
    q = (select(Movie.id, Movie.title, MovieCode.code, cnt)
         .join(MovieRequest, MovieRequest.movie_id == Movie.id)
         .outerjoin(MovieCode, MovieCode.movie_id == Movie.id)
         .group_by(Movie.id, Movie.title, MovieCode.code).order_by(desc("cnt")).limit(limit))
    return (await s.execute(q)).all()


def norm_code(raw: str) -> str:
    return raw.strip().upper().replace(" ", "")


def valid_code(code: str) -> bool:
    return bool(re.match(r"^[A-Z0-9_\-]{2,32}$", code))


async def gen_code(s: AsyncSession) -> str:
    digits = 3
    for i in range(200):
        if i and i % 40 == 0:
            digits += 1
        c = f"{CODE_PREFIX}-{random.randint(10 ** (digits - 1), 10 ** digits - 1)}"
        if not await get_code(s, c):
            return c
    raise RuntimeError("Kod yaratib bo'lmadi")


# ════════════════════════ KLAVIATURALAR ════════════════════════
def ikb(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t, callback_data=d) for t, d in row] for row in rows])


def rkb(*rows, placeholder=None) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t) for t in row] for row in rows],
                               resize_keyboard=True, input_field_placeholder=placeholder)


B_CODE, B_TOP, B_HELP, B_ADMIN = "🎬 Kino kodini yuborish", "🔥 Top kinolar", "ℹ️ Yordam", "🛠 Admin panel"
B_ADD, B_EDIT, B_DEL, B_SEARCH = "➕ Kino qo'shish", "✏️ Kino tahrirlash", "🗑 Kino o'chirish", "🔍 Kino qidirish"
B_USERS, B_STATS, B_CODES, B_HOME = "👥 Foydalanuvchilar", "📊 Statistika", "🔑 Kino kodlari", "🏠 Asosiy menyu"

CANCEL = ("❌ Bekor qilish", "cancel")
SKIP = ("⏭ O'tkazib yuborish", "skip")


def user_menu(is_admin=False):
    rows = [[B_CODE], [B_TOP, B_HELP]] + ([[B_ADMIN]] if is_admin else [])
    return rkb(*rows, placeholder="Kino kodini yuboring: KINO-123")


def admin_menu():
    return rkb([B_ADD, B_EDIT], [B_DEL, B_SEARCH], [B_USERS, B_STATS], [B_CODES, B_HOME], placeholder="Admin panel")


def results_kb(rows, prefix):
    btns = [[(f"🎬 {m.title[:40]} · {c or 'kodsiz'}", f"{prefix}:{m.id}")] for m, c in rows]
    return ikb(*btns, [CANCEL])


# ════════════════════════ MATNLAR / YUBORISH ════════════════════════
def movie_info(m: Movie, code, extra=None) -> str:
    lines = [f"🎬 <b>{e(m.title)}</b>", ""]
    if m.genre:
        lines.append(f"🎭 Janr: {e(m.genre)}")
    if m.year:
        lines.append(f"📅 Yil: {m.year}")
    if m.language:
        lines.append(f"🌐 Til: {e(m.language)}")
    if code:
        lines.append(f"🔑 Kod: <code>{e(code)}</code>")
    if m.description:
        lines += ["", f"📝 {e(m.description[:600])}"]
    if extra:
        lines += [""] + extra
    return "\n".join(lines)


async def send_file(msg: Message, m: Movie, caption: str):
    if m.file_type == "document":
        await msg.answer_document(m.file_id, caption=caption)
    else:
        await msg.answer_video(m.file_id, caption=caption)


async def send_movie(msg: Message, m: Movie, code) -> None:
    info = movie_info(m, code)
    short = f"🎬 <b>{e(m.title)}</b>" + (f"\n🔑 <code>{e(code)}</code>" if code else "")
    try:
        if m.poster_id:
            await msg.answer_photo(m.poster_id, caption=info)
            await send_file(msg, m, short)
        else:
            await send_file(msg, m, info)
    except TelegramBadRequest:
        log.exception("Kinoni yuborib bo'lmadi id=%s", m.id)
        await msg.answer("⚠️ Kinoni yuborib bo'lmadi. Admin bilan bog'laning.")


async def send_card(msg: Message, s: AsyncSession, m: Movie) -> None:
    co = await code_of_movie(s, m.id)
    reqs = await scalar_count(s, select(func.count(MovieRequest.id)).where(MovieRequest.movie_id == m.id))
    extra = [f"🆔 ID: {m.id}", f"📆 Qo'shilgan: {fmt(m.created_at)}", f"📥 So'rovlar: {reqs} ta"]
    if not co:
        extra.append("⚠️ Bu kinoga kod biriktirilmagan!")
    text = movie_info(m, co.code if co else None, extra)
    kb = ikb([("🎬 Ko'rish", f"mv:play:{m.id}")],
             [("✏️ Tahrirlash", f"mv:edit:{m.id}"), ("🗑 O'chirish", f"mv:del:{m.id}")])
    if m.poster_id:
        try:
            await msg.answer_photo(m.poster_id, caption=text, reply_markup=kb)
            return
        except TelegramBadRequest:
            pass
    await msg.answer(text, reply_markup=kb)


async def drop_kb(call: CallbackQuery) -> None:
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass


# ════════════════════════ MIDDLEWARE / FILTRLAR ════════════════════════
class DbMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        tg = data.get("event_from_user")
        if tg is None or tg.is_bot:
            return await handler(event, data)
        async with SessionFactory() as s:
            try:
                u = await upsert_user(s, tg)
                data["session"], data["db_user"] = s, u
                if u.is_blocked and tg.id not in ADMIN_IDS:
                    await s.commit()
                    if isinstance(event, CallbackQuery):
                        await event.answer("🚫 Siz bloklangansiz.", show_alert=True)
                    elif isinstance(event, Message):
                        await event.answer("🚫 Siz botdan foydalanishdan bloklangansiz.")
                    return None
                res = await handler(event, data)
                await s.commit()
                return res
            except Exception:
                await s.rollback()
                raise


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        u = getattr(event, "from_user", None)
        return bool(u and u.id in ADMIN_IDS)


async def not_command(message: Message) -> bool:
    return not (message.text and message.text.startswith("/"))


# ════════════════════════ FSM HOLATLARI ════════════════════════
class AddMovie(StatesGroup):
    title = State()
    code = State()
    video = State()
    poster = State()
    description = State()
    genre = State()
    year = State()
    language = State()
    confirm = State()


class Flow(StatesGroup):
    search = State()
    edit = State()
    user_find = State()
    code_create = State()
    code_attach = State()
    code_attach_movie = State()
    code_delete = State()
    code_find = State()


# ════════════════════════ ADMIN ROUTER ════════════════════════
admin = Router(name="admin")
admin.message.filter(IsAdmin())
admin.callback_query.filter(IsAdmin())

BAD_CODE = "⚠️ Kod noto'g'ri. Lotin harf, raqam, «-», «_» (2–32 belgi). Masalan: <code>KINO-123</code>"


# ───── Menyu ─────
@admin.message(Command("admin"))
@admin.message(F.text == B_ADMIN)
async def open_admin(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🛠 <b>Admin panel</b>\n\nKerakli bo'limni tanlang:", reply_markup=admin_menu())


@admin.message(F.text == B_HOME)
async def to_home(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🏠 Asosiy menyu", reply_markup=user_menu(True))


@admin.message(Command("cancel"))
async def cancel_cmd(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("❌ Bekor qilindi.", reply_markup=admin_menu())


@admin.callback_query(F.data == "cancel")
async def cancel_cb(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer("Bekor qilindi")
    await drop_kb(c)
    await c.message.answer("❌ Bekor qilindi.", reply_markup=admin_menu())


@admin.callback_query(F.data == "noop")
async def noop(c: CallbackQuery):
    await c.answer()


@admin.message(F.text == B_ADD)
async def menu_add(m: Message, state: FSMContext):
    await state.clear()
    await go_step(m, state, AddMovie.title)


@admin.message(F.text.in_({B_EDIT, B_DEL, B_SEARCH}))
async def menu_search(m: Message, state: FSMContext):
    await state.clear()
    prompts = {
        B_EDIT: "✏️ <b>Kino tahrirlash</b>\n\nKinoning <b>kodi</b> yoki <b>nomini</b> yuboring:",
        B_DEL: "🗑 <b>Kino o'chirish</b>\n\nKinoning <b>kodi</b> yoki <b>nomini</b> yuboring:",
        B_SEARCH: "🔍 <b>Kino qidirish</b>\n\nKino <b>kodi</b> yoki <b>nomini</b> yuboring:",
    }
    await state.set_state(Flow.search)
    await m.answer(prompts[m.text], reply_markup=ikb([CANCEL]))


@admin.message(F.text == B_USERS)
async def menu_users(m: Message, state: FSMContext, session: AsyncSession):
    await state.clear()
    total = await scalar_count(session, select(func.count(User.id)))
    blocked = await scalar_count(session, select(func.count(User.id)).where(User.is_blocked.is_(True)))
    await m.answer(f"👥 <b>Foydalanuvchilar</b>\n\n👤 Jami: <b>{total}</b>\n🚫 Bloklangan: <b>{blocked}</b>",
                   reply_markup=ikb([("📋 Ro'yxat", "usr:list:0")],
                                    [("🔎 ID / @username bo'yicha topish", "usr:find")]))


@admin.message(F.text == B_STATS)
async def menu_stats(m: Message, state: FSMContext, session: AsyncSession):
    await state.clear()
    since = today_start()
    c = lambda q: scalar_count(session, q)  # noqa: E731
    top = await top_movies(session, 10)
    lines = [
        "📊 <b>Statistika</b>\n",
        f"👥 Jami foydalanuvchilar: <b>{await c(select(func.count(User.id)))}</b>",
        f"🆕 Bugungi yangi foydalanuvchilar: <b>{await c(select(func.count(User.id)).where(User.joined_at >= since))}</b>",
        f"🚫 Bloklanganlar: <b>{await c(select(func.count(User.id)).where(User.is_blocked.is_(True)))}</b>",
        f"🎬 Jami kinolar: <b>{await c(select(func.count(Movie.id)))}</b>",
        f"📥 Bugungi so'rovlar: <b>{await c(select(func.count(MovieRequest.id)).where(MovieRequest.created_at >= since))}</b>",
        f"📈 Jami so'rovlar: <b>{await c(select(func.count(MovieRequest.id)))}</b>",
        "\n🔥 <b>Eng ko'p so'ralgan kinolar:</b>",
    ]
    if top:
        lines += [f"{i}. {e(t)} (<code>{e(co or '-')}</code>) — {n} marta" for i, (_, t, co, n) in enumerate(top, 1)]
    else:
        lines.append("Hozircha ma'lumot yo'q.")
    await m.answer("\n".join(lines))


@admin.message(F.text == B_CODES)
async def menu_codes(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("🔑 <b>Kino kodlari</b>\n\nKod yarating, kinoga biriktiring, o'chiring yoki kod orqali kino toping:",
                   reply_markup=ikb([("🎲 Kod yaratish (avto)", "cd:new"), ("✍️ Kod yaratish (qo'lda)", "cd:custom")],
                                    [("🔗 Kodni kinoga biriktirish", "cd:attach")],
                                    [("🗑 Kodni o'chirish", "cd:del"), ("🔍 Kod orqali topish", "cd:find")],
                                    [("📃 Bo'sh kodlar", "cd:free"), ("📃 Barcha kodlar", "cd:all")]))


# ───── Kino qo'shish ─────
PROMPTS = {
    AddMovie.title.state: ("➕ <b>Yangi kino</b>\n\n1/8 · 📝 Kino nomini yuboring:", ikb([CANCEL])),
    AddMovie.code.state: ("2/8 · 🔑 Kino kodini yuboring (masalan: <code>KINO-123</code>)\nyoki tugmani bosing:",
                          ikb([("🎲 Avto-generatsiya", "gen_code")], [CANCEL])),
    AddMovie.video.state: ("3/8 · 🎞 Kino faylini yuboring (video yoki fayl):", ikb([CANCEL])),
    AddMovie.poster.state: ("4/8 · 🖼 Poster (rasm) yuboring:", ikb([SKIP, CANCEL])),
    AddMovie.description.state: ("5/8 · 📄 Tavsifni yozing (max 600 belgi):", ikb([SKIP, CANCEL])),
    AddMovie.genre.state: ("6/8 · 🎭 Janrni yozing (masalan: Jangari):", ikb([SKIP, CANCEL])),
    AddMovie.year.state: ("7/8 · 📅 Yilni yozing (masalan: 2023):", ikb([SKIP, CANCEL])),
    AddMovie.language.state: ("8/8 · 🌐 Tilni yozing (masalan: O'zbek tilida):", ikb([SKIP, CANCEL])),
}
OPTIONAL = [(AddMovie.poster, "poster_id"), (AddMovie.description, "description"), (AddMovie.genre, "genre"),
            (AddMovie.year, "year"), (AddMovie.language, "language")]
ORDER = [AddMovie.poster, AddMovie.description, AddMovie.genre, AddMovie.year, AddMovie.language, AddMovie.confirm]


async def go_step(msg: Message, state: FSMContext, step: State):
    await state.set_state(step)
    if step == AddMovie.confirm:
        d = await state.get_data()
        desc_ = (d.get("description") or "—")[:300]
        text = ("📋 <b>Ma'lumotlarni tekshiring:</b>\n\n"
                f"📝 Nomi: <b>{e(d['title'])}</b>\n🔑 Kod: <code>{e(d['code'])}</code>\n"
                f"🎞 Fayl: {'video' if d['file_type'] == 'video' else 'hujjat'} ✅\n"
                f"🖼 Poster: {'bor ✅' if d.get('poster_id') else 'yoq'}\n"
                f"🎭 Janr: {e(d.get('genre') or '—')}\n📅 Yil: {d.get('year') or '—'}\n"
                f"🌐 Til: {e(d.get('language') or '—')}\n📄 Tavsif: {e(desc_)}")
        kb = ikb([("✅ Tasdiqlash", "add:ok"), CANCEL])
        if d.get("poster_id"):
            await msg.answer_photo(d["poster_id"], caption=text, reply_markup=kb)
        else:
            await msg.answer(text, reply_markup=kb)
        return
    text, kb = PROMPTS[step.state]
    await msg.answer(text, reply_markup=kb)


@admin.message(AddMovie.title, F.text, not_command)
async def add_title(m: Message, state: FSMContext):
    t = m.text.strip()
    if not 1 <= len(t) <= 100:
        return await m.answer("⚠️ Nom 1–100 belgi bo'lsin. Qayta yuboring:")
    await state.update_data(title=t)
    await go_step(m, state, AddMovie.code)


@admin.message(AddMovie.code, F.text, not_command)
async def add_code(m: Message, state: FSMContext, session: AsyncSession):
    code = norm_code(m.text)
    if not valid_code(code):
        return await m.answer(BAD_CODE)
    ex = await get_code(session, code)
    if ex and ex.movie_id:
        return await m.answer("⚠️ Bu kod band. Boshqa kod yuboring:")
    await state.update_data(code=code)
    await go_step(m, state, AddMovie.video)


@admin.callback_query(AddMovie.code, F.data == "gen_code")
async def add_gen(c: CallbackQuery, state: FSMContext, session: AsyncSession):
    code = await gen_code(session)
    await state.update_data(code=code)
    await c.answer()
    await drop_kb(c)
    await c.message.answer(f"🎲 Kod yaratildi: <code>{code}</code>")
    await go_step(c.message, state, AddMovie.video)


@admin.message(AddMovie.video, F.video | F.document)
async def add_video(m: Message, state: FSMContext):
    if m.video:
        await state.update_data(file_id=m.video.file_id, file_type="video")
    else:
        await state.update_data(file_id=m.document.file_id, file_type="document")
    await go_step(m, state, AddMovie.poster)


@admin.message(AddMovie.video, not_command)
async def add_video_bad(m: Message):
    await m.answer("⚠️ Iltimos, kino faylini video yoki hujjat sifatida yuboring.")


@admin.message(AddMovie.poster, F.photo)
async def add_poster(m: Message, state: FSMContext):
    await state.update_data(poster_id=m.photo[-1].file_id)
    await go_step(m, state, AddMovie.description)


@admin.message(AddMovie.poster, not_command)
async def add_poster_bad(m: Message):
    await m.answer("⚠️ Rasm (foto) yuboring yoki o'tkazib yuboring.")


@admin.message(AddMovie.description, F.text, not_command)
async def add_desc(m: Message, state: FSMContext):
    t = m.text.strip()
    if len(t) > 600:
        return await m.answer(f"⚠️ Juda uzun ({len(t)}). 600 belgigacha yuboring:")
    await state.update_data(description=t)
    await go_step(m, state, AddMovie.genre)


@admin.message(AddMovie.genre, F.text, not_command)
async def add_genre(m: Message, state: FSMContext):
    t = m.text.strip()
    if len(t) > 60:
        return await m.answer("⚠️ Janr 60 belgidan oshmasin:")
    await state.update_data(genre=t)
    await go_step(m, state, AddMovie.year)


@admin.message(AddMovie.year, F.text, not_command)
async def add_year(m: Message, state: FSMContext):
    t = m.text.strip()
    if not (t.isdigit() and 1888 <= int(t) <= 2100):
        return await m.answer("⚠️ Yilni raqam bilan yozing (masalan: 2023):")
    await state.update_data(year=int(t))
    await go_step(m, state, AddMovie.language)


@admin.message(AddMovie.language, F.text, not_command)
async def add_lang(m: Message, state: FSMContext):
    t = m.text.strip()
    if len(t) > 30:
        return await m.answer("⚠️ Til 30 belgidan oshmasin:")
    await state.update_data(language=t)
    await go_step(m, state, AddMovie.confirm)


@admin.callback_query(StateFilter(AddMovie.poster, AddMovie.description, AddMovie.genre, AddMovie.year,
                                  AddMovie.language), F.data == "skip")
async def add_skip(c: CallbackQuery, state: FSMContext):
    cur = await state.get_state()
    idx = next(i for i, (s_, _) in enumerate(OPTIONAL) if s_.state == cur)
    await state.update_data(**{OPTIONAL[idx][1]: None})
    await c.answer("O'tkazildi")
    await drop_kb(c)
    await go_step(c.message, state, ORDER[idx + 1])


@admin.callback_query(AddMovie.confirm, F.data == "add:ok")
async def add_confirm(c: CallbackQuery, state: FSMContext, session: AsyncSession):
    d = await state.get_data()
    co = await get_code(session, d["code"])
    if co and co.movie_id:
        await c.answer("Bu kod band bo'lib qolgan!", show_alert=True)
        await state.set_state(AddMovie.code)
        return await c.message.answer("⚠️ Kod band. Yangi kod yuboring:", reply_markup=PROMPTS[AddMovie.code.state][1])
    mv = Movie(title=d["title"], file_id=d["file_id"], file_type=d["file_type"], poster_id=d.get("poster_id"),
               description=d.get("description"), genre=d.get("genre"), year=d.get("year"),
               language=d.get("language"))
    session.add(mv)
    await session.flush()
    if co:
        await attach_code(session, co, mv.id)
    else:
        session.add(MovieCode(code=d["code"], movie_id=mv.id))
        await session.flush()
    await state.clear()
    await c.answer("Saqlandi ✅")
    await drop_kb(c)
    await c.message.answer(f"✅ <b>Kino qo'shildi!</b>\n\nFoydalanuvchilar <code>{e(d['code'])}</code> kodini "
                           "yuborib oladi.", reply_markup=admin_menu())
    await send_card(c.message, session, mv)


# ───── Qidirish / ko'rish / o'chirish / tahrirlash ─────
@admin.message(Flow.search, F.text, not_command)
async def do_search(m: Message, state: FSMContext, session: AsyncSession):
    rows = await search_movies(session, m.text)
    if not rows:
        return await m.answer("❌ Topilmadi. Boshqa kod yoki nom yuboring:", reply_markup=ikb([CANCEL]))
    await state.clear()
    if len(rows) == 1:
        return await send_card(m, session, rows[0][0])
    await m.answer(f"🔎 Topildi: <b>{len(rows)}</b> ta. Tanlang:", reply_markup=results_kb(rows, "mv:open"))


@admin.callback_query(F.data.startswith("mv:open:"))
async def mv_open(c: CallbackQuery, state: FSMContext, session: AsyncSession):
    await state.clear()
    mv = await session.get(Movie, int(c.data.split(":")[2]))
    await c.answer()
    if not mv:
        return await c.message.answer("❌ Kino topilmadi.")
    await send_card(c.message, session, mv)


@admin.callback_query(F.data.startswith("mv:play:"))
async def mv_play(c: CallbackQuery, session: AsyncSession):
    mv = await session.get(Movie, int(c.data.split(":")[2]))
    await c.answer()
    if not mv:
        return await c.message.answer("❌ Kino topilmadi.")
    co = await code_of_movie(session, mv.id)
    await send_movie(c.message, mv, co.code if co else None)


@admin.callback_query(F.data.startswith("mv:del:"))
async def mv_del(c: CallbackQuery, session: AsyncSession):
    mv = await session.get(Movie, int(c.data.split(":")[2]))
    await c.answer()
    if not mv:
        return await c.message.answer("❌ Kino topilmadi.")
    await c.message.answer(f"⚠️ <b>{e(mv.title)}</b> o'chirilsinmi?\nKod ham o'chadi. Qaytarib bo'lmaydi.",
                           reply_markup=ikb([("✅ Ha, o'chirish", f"mv:delyes:{mv.id}"),
                                             ("↩️ Yo'q", f"mv:open:{mv.id}")]))


@admin.callback_query(F.data.startswith("mv:delyes:"))
async def mv_delyes(c: CallbackQuery, session: AsyncSession):
    mv = await session.get(Movie, int(c.data.split(":")[2]))
    await c.answer()
    await drop_kb(c)
    if not mv:
        return await c.message.answer("❌ Kino allaqachon o'chirilgan.")
    title = mv.title
    await session.execute(delete(MovieCode).where(MovieCode.movie_id == mv.id))
    await session.execute(update(MovieRequest).where(MovieRequest.movie_id == mv.id).values(movie_id=None))
    await session.delete(mv)
    await session.flush()
    await c.message.answer(f"🗑 <b>{e(title)}</b> o'chirildi.", reply_markup=admin_menu())


@admin.callback_query(F.data.startswith("mv:edit:"))
async def mv_edit(c: CallbackQuery, state: FSMContext):
    await state.clear()
    i = c.data.split(":")[2]
    await c.answer()
    await c.message.answer("✏️ Nimani o'zgartiramiz?", reply_markup=ikb(
        [("📝 Nomi", f"ed:title:{i}"), ("🔑 Kodi", f"ed:code:{i}")],
        [("🎞 Video/fayl", f"ed:video:{i}"), ("🖼 Poster", f"ed:poster:{i}")],
        [("📄 Tavsif", f"ed:description:{i}"), ("🎭 Janr", f"ed:genre:{i}")],
        [("📅 Yil", f"ed:year:{i}"), ("🌐 Til", f"ed:language:{i}")],
        [("⬅️ Orqaga", f"mv:open:{i}")]))


EDIT_PROMPTS = {
    "title": "📝 Yangi nomni yuboring (max 100 belgi):",
    "code": "🔑 Yangi kodni yuboring (masalan: <code>KINO-777</code>):",
    "video": "🎞 Yangi video yoki faylni yuboring:",
    "poster": "🖼 Yangi posterni (rasm) yuboring:",
    "description": "📄 Yangi tavsifni yuboring (max 600). Tozalash: «-»",
    "genre": "🎭 Yangi janrni yuboring. Tozalash: «-»",
    "year": "📅 Yangi yilni yuboring. Tozalash: «-»",
    "language": "🌐 Yangi tilni yuboring. Tozalash: «-»",
}
LIMITS = {"title": 100, "description": 600, "genre": 60, "language": 30}


@admin.callback_query(F.data.startswith("ed:"))
async def ed_field(c: CallbackQuery, state: FSMContext):
    _, field, mid = c.data.split(":")
    await state.set_state(Flow.edit)
    await state.update_data(field=field, movie_id=int(mid))
    await c.answer()
    await c.message.answer(EDIT_PROMPTS[field], reply_markup=ikb([CANCEL]))


@admin.message(Flow.edit, not_command)
async def ed_value(m: Message, state: FSMContext, session: AsyncSession):
    d = await state.get_data()
    field = d["field"]
    mv = await session.get(Movie, d["movie_id"])
    if not mv:
        await state.clear()
        return await m.answer("❌ Kino topilmadi.", reply_markup=admin_menu())
    text = (m.text or "").strip()
    upd = {}
    if field in LIMITS:
        if not text:
            return await m.answer("⚠️ Matn yuboring.")
        if field == "title" and text == "-":
            return await m.answer("⚠️ Nomni tozalab bo'lmaydi.")
        if len(text) > LIMITS[field]:
            return await m.answer(f"⚠️ Juda uzun. Max {LIMITS[field]} belgi.")
        upd[field] = None if text == "-" else text
    elif field == "year":
        if text == "-":
            upd["year"] = None
        elif text.isdigit() and 1888 <= int(text) <= 2100:
            upd["year"] = int(text)
        else:
            return await m.answer("⚠️ Yilni raqam bilan yozing (masalan: 2023).")
    elif field == "code":
        code = norm_code(text)
        if not valid_code(code):
            return await m.answer(BAD_CODE)
        ex = await get_code(session, code)
        if ex and ex.movie_id and ex.movie_id != mv.id:
            return await m.answer("⚠️ Bu kod boshqa kinoga biriktirilgan. Boshqasini yuboring:")
        if ex and ex.movie_id is None:
            await attach_code(session, ex, mv.id)
        elif not ex:
            cur = await code_of_movie(session, mv.id)
            if cur:
                cur.code = code
                await session.flush()
            else:
                session.add(MovieCode(code=code, movie_id=mv.id))
                await session.flush()
    elif field == "video":
        if m.video:
            upd.update(file_id=m.video.file_id, file_type="video")
        elif m.document:
            upd.update(file_id=m.document.file_id, file_type="document")
        else:
            return await m.answer("⚠️ Video yoki fayl yuboring.")
    elif field == "poster":
        if not m.photo:
            return await m.answer("⚠️ Rasm (foto) yuboring.")
        upd["poster_id"] = m.photo[-1].file_id
    for k, v in upd.items():
        setattr(mv, k, v)
    await session.flush()
    await state.clear()
    await m.answer("✅ Yangilandi!", reply_markup=admin_menu())
    await send_card(m, session, mv)


# ───── Foydalanuvchilar ─────
PAGE = 8


async def user_card(s: AsyncSession, u: User):
    reqs = await scalar_count(s, select(func.count(MovieRequest.id)).where(MovieRequest.user_id == u.id))
    text = ("👤 <b>Foydalanuvchi</b>\n\n"
            f"🆔 ID: <code>{u.tg_id}</code>\n📛 Ism: {e(u.full_name) or '—'}\n"
            f"🔗 Username: {'@' + e(u.username) if u.username else '—'}\n"
            f"📆 Qo'shilgan: {fmt(u.joined_at)}\n🕒 Oxirgi faollik: {fmt(u.last_active)}\n"
            f"📥 So'rovlar: {reqs} ta\nHolati: {'🔴 Bloklangan' if u.is_blocked else '🟢 Faol'}")
    toggle = (("✅ Blokdan chiqarish", f"usr:unblock:{u.tg_id}") if u.is_blocked
              else ("🚫 Bloklash", f"usr:block:{u.tg_id}"))
    return text, ikb([toggle], [("🎞 So'ragan kinolari", f"usr:reqs:{u.tg_id}")], [("⬅️ Ro'yxat", "usr:list:0")])


async def safe_edit(c: CallbackQuery, text: str, kb):
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass


@admin.callback_query(F.data.startswith("usr:list:"))
async def usr_list(c: CallbackQuery, session: AsyncSession):
    total = await scalar_count(session, select(func.count(User.id)))
    pages = max(1, math.ceil(total / PAGE))
    page = min(max(int(c.data.split(":")[2]), 0), pages - 1)
    users = list(await session.scalars(select(User).order_by(desc(User.joined_at)).offset(page * PAGE).limit(PAGE)))
    rows = [[(f"{'🔴' if u.is_blocked else '🟢'} {(u.full_name or 'Nomalum')[:25]}"
              f"{' @' + u.username if u.username else ''}", f"usr:view:{u.tg_id}")] for u in users]
    nav = []
    if page > 0:
        nav.append(("⬅️", f"usr:list:{page - 1}"))
    nav.append((f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        nav.append(("➡️", f"usr:list:{page + 1}"))
    await c.answer()
    await safe_edit(c, f"📋 <b>Foydalanuvchilar</b>\nJami: {total} · 🟢 faol · 🔴 bloklangan", ikb(*rows, nav))


@admin.callback_query(F.data.startswith("usr:view:"))
async def usr_view(c: CallbackQuery, session: AsyncSession):
    u = await get_user(session, int(c.data.split(":")[2]))
    await c.answer()
    if not u:
        return await c.message.answer("❌ Topilmadi.")
    text, kb = await user_card(session, u)
    await safe_edit(c, text, kb)


@admin.callback_query(F.data.startswith(("usr:block:", "usr:unblock:")))
async def usr_block(c: CallbackQuery, session: AsyncSession):
    _, action, tg_id = c.data.split(":")
    tg_id = int(tg_id)
    if action == "block" and tg_id in ADMIN_IDS:
        return await c.answer("Adminni bloklab bo'lmaydi!", show_alert=True)
    u = await get_user(session, tg_id)
    if not u:
        return await c.answer("Topilmadi", show_alert=True)
    u.is_blocked = action == "block"
    await session.flush()
    await c.answer("🚫 Bloklandi" if u.is_blocked else "✅ Blokdan chiqarildi")
    text, kb = await user_card(session, u)
    await safe_edit(c, text, kb)


@admin.callback_query(F.data.startswith("usr:reqs:"))
async def usr_reqs(c: CallbackQuery, session: AsyncSession):
    u = await get_user(session, int(c.data.split(":")[2]))
    await c.answer()
    if not u:
        return await c.message.answer("❌ Topilmadi.")
    q = (select(MovieRequest.code_text, MovieRequest.created_at, MovieRequest.movie_id, Movie.title)
         .outerjoin(Movie, Movie.id == MovieRequest.movie_id).where(MovieRequest.user_id == u.id)
         .order_by(desc(MovieRequest.id)).limit(25))
    rows = (await session.execute(q)).all()
    if not rows:
        return await c.message.answer("📭 Bu foydalanuvchi hali kino so'ramagan.")
    lines = [f"🎞 <b>{e(u.full_name)}</b> — oxirgi so'rovlari:\n"]
    for code_text, created, mid, title in rows:
        lines.append(f"• {fmt(created)} — <code>{e(code_text)}</code> — {'✅ ' + e(title) if mid else '❌ topilmadi'}")
    await c.message.answer("\n".join(lines))


@admin.callback_query(F.data == "usr:find")
async def usr_find(c: CallbackQuery, state: FSMContext):
    await state.set_state(Flow.user_find)
    await c.answer()
    await c.message.answer("🔎 Telegram ID yoki @username yuboring:", reply_markup=ikb([CANCEL]))


@admin.message(Flow.user_find, F.text, not_command)
async def usr_find_do(m: Message, state: FSMContext, session: AsyncSession):
    q = m.text.strip()
    if q.lstrip("-").isdigit():
        u = await get_user(session, int(q))
    else:
        u = await session.scalar(select(User).where(func.lower(User.username) == q.lstrip("@").lower()))
    if not u:
        return await m.answer("❌ Topilmadi. Qayta yuboring:", reply_markup=ikb([CANCEL]))
    await state.clear()
    text, kb = await user_card(session, u)
    await m.answer(text, reply_markup=kb)


# ───── Kodlar ─────
@admin.callback_query(F.data == "cd:new")
async def cd_new(c: CallbackQuery, session: AsyncSession):
    code = await gen_code(session)
    session.add(MovieCode(code=code))
    await session.flush()
    await c.answer()
    await c.message.answer(f"✅ Yangi kod: <code>{code}</code>\n\nHozircha kinoga biriktirilmagan. "
                           "«🔗 Kodni kinoga biriktirish» orqali biriktiring.")


@admin.callback_query(F.data == "cd:custom")
async def cd_custom(c: CallbackQuery, state: FSMContext):
    await state.set_state(Flow.code_create)
    await c.answer()
    await c.message.answer("✍️ Yangi kodni yuboring (masalan: <code>KINO-555</code>):", reply_markup=ikb([CANCEL]))


@admin.message(Flow.code_create, F.text, not_command)
async def cd_custom_do(m: Message, state: FSMContext, session: AsyncSession):
    code = norm_code(m.text)
    if not valid_code(code):
        return await m.answer(BAD_CODE)
    if await get_code(session, code):
        return await m.answer("⚠️ Bu kod mavjud. Boshqasini yuboring:")
    session.add(MovieCode(code=code))
    await session.flush()
    await state.clear()
    await m.answer(f"✅ Kod yaratildi: <code>{code}</code> (bo'sh)", reply_markup=admin_menu())


@admin.callback_query(F.data == "cd:attach")
async def cd_attach(c: CallbackQuery, state: FSMContext):
    await state.set_state(Flow.code_attach)
    await c.answer()
    await c.message.answer("🔗 Biriktiriladigan kodni yuboring (bo'sh kod yoki yangi kod):", reply_markup=ikb([CANCEL]))


@admin.message(Flow.code_attach, F.text, not_command)
async def cd_attach_code(m: Message, state: FSMContext, session: AsyncSession):
    code = norm_code(m.text)
    if not valid_code(code):
        return await m.answer(BAD_CODE)
    ex = await get_code(session, code)
    if ex and ex.movie_id:
        return await m.answer("⚠️ Bu kod boshqa kinoga biriktirilgan. Boshqasini yuboring:")
    if not ex:
        session.add(MovieCode(code=code))
        await session.flush()
    await state.update_data(code=code)
    await state.set_state(Flow.code_attach_movie)
    await m.answer(f"🔑 Kod: <code>{code}</code>\n\nEndi kino <b>nomini</b> yuboring:", reply_markup=ikb([CANCEL]))


@admin.message(Flow.code_attach_movie, F.text, not_command)
async def cd_attach_search(m: Message, session: AsyncSession):
    rows = await search_movies(session, m.text)
    if not rows:
        return await m.answer("❌ Topilmadi. Boshqa nom yuboring:", reply_markup=ikb([CANCEL]))
    await m.answer("Kinoni tanlang:", reply_markup=results_kb(rows, "cd:att"))


@admin.callback_query(Flow.code_attach_movie, F.data.startswith("cd:att:"))
async def cd_attach_pick(c: CallbackQuery, state: FSMContext, session: AsyncSession):
    d = await state.get_data()
    mv = await session.get(Movie, int(c.data.split(":")[2]))
    obj = await get_code(session, d.get("code", ""))
    if not mv or not obj:
        return await c.answer("Topilmadi", show_alert=True)
    if obj.movie_id and obj.movie_id != mv.id:
        return await c.answer("Kod band!", show_alert=True)
    old = await code_of_movie(session, mv.id)
    await attach_code(session, obj, mv.id)
    await state.clear()
    await c.answer("Biriktirildi ✅")
    await drop_kb(c)
    note = f"\n(Eski kod <code>{e(old.code)}</code> bo'sh holatga o'tdi.)" if old and old.id != obj.id else ""
    await c.message.answer(f"✅ <code>{e(obj.code)}</code> → <b>{e(mv.title)}</b> biriktirildi.{note}",
                           reply_markup=admin_menu())


@admin.callback_query(F.data == "cd:del")
async def cd_del(c: CallbackQuery, state: FSMContext):
    await state.set_state(Flow.code_delete)
    await c.answer()
    await c.message.answer("🗑 O'chiriladigan kodni yuboring:", reply_markup=ikb([CANCEL]))


@admin.message(Flow.code_delete, F.text, not_command)
async def cd_del_ask(m: Message, state: FSMContext, session: AsyncSession):
    code = norm_code(m.text)
    obj = await get_code(session, code)
    if not obj:
        return await m.answer("❌ Bunday kod yo'q. Qayta yuboring:", reply_markup=ikb([CANCEL]))
    await state.clear()
    warn = ""
    if obj.movie_id:
        mv = await session.get(Movie, obj.movie_id)
        if mv:
            warn = f"\n\n⚠️ Bu kod <b>{e(mv.title)}</b> kinosiga biriktirilgan. O'chirilsa, kino kod orqali topilmaydi."
    await m.answer(f"Kod <code>{e(code)}</code> o'chirilsinmi?{warn}",
                   reply_markup=ikb([("✅ Ha, o'chirish", f"cd:delyes:{code}"), CANCEL]))


@admin.callback_query(F.data.startswith("cd:delyes:"))
async def cd_del_do(c: CallbackQuery, session: AsyncSession):
    code = c.data.split(":", 2)[2]
    obj = await get_code(session, code)
    await c.answer()
    await drop_kb(c)
    if not obj:
        return await c.message.answer("❌ Kod allaqachon o'chirilgan.")
    await session.delete(obj)
    await session.flush()
    await c.message.answer(f"🗑 Kod <code>{e(code)}</code> o'chirildi.", reply_markup=admin_menu())


@admin.callback_query(F.data == "cd:find")
async def cd_find(c: CallbackQuery, state: FSMContext):
    await state.set_state(Flow.code_find)
    await c.answer()
    await c.message.answer("🔍 Kodni yuboring:", reply_markup=ikb([CANCEL]))


@admin.message(Flow.code_find, F.text, not_command)
async def cd_find_do(m: Message, state: FSMContext, session: AsyncSession):
    code = norm_code(m.text)
    obj = await get_code(session, code)
    if not obj:
        return await m.answer("❌ Bunday kod yo'q. Qayta yuboring:", reply_markup=ikb([CANCEL]))
    await state.clear()
    mv = await session.get(Movie, obj.movie_id) if obj.movie_id else None
    if mv:
        await send_card(m, session, mv)
    else:
        await m.answer(f"🔑 <code>{e(code)}</code> — bo'sh kod (kinoga biriktirilmagan).")


@admin.callback_query(F.data.in_({"cd:free", "cd:all"}))
async def cd_list(c: CallbackQuery, session: AsyncSession):
    free = c.data == "cd:free"
    q = select(MovieCode, Movie.title).outerjoin(Movie, Movie.id == MovieCode.movie_id)
    if free:
        q = q.where(MovieCode.movie_id.is_(None))
    rows = (await session.execute(q.order_by(desc(MovieCode.id)).limit(50))).all()
    await c.answer()
    if not rows:
        return await c.message.answer("📭 Kodlar yo'q.")
    lines = ["📃 <b>Bo'sh kodlar</b>\n" if free else "📃 <b>Oxirgi 50 ta kod</b>\n"]
    lines += [f"<code>{e(o.code)}</code> → {e(t) if t else '<i>bo`sh</i>'}" for o, t in rows]
    await c.message.answer("\n".join(lines))


# ════════════════════════ FOYDALANUVCHI ROUTER ════════════════════════
user = Router(name="user")

HELP = ("ℹ️ <b>Yordam</b>\n\n1️⃣ Kino kodini botga yuboring. Masalan: <code>KINO-123</code>\n"
        "2️⃣ Bot sizga shu kodga tegishli kinoni yuboradi.\n\nKino kodlarini kanalimizdan topishingiz mumkin.")


@user.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(f"👋 Assalomu alaykum, <b>{e(m.from_user.first_name)}</b>!\n\n🎬 Kino botiga xush kelibsiz.\n"
                   "Kino kodini yuboring — men sizga kinoni jo'nataman.\n\nMasalan: <code>KINO-123</code>",
                   reply_markup=user_menu(m.from_user.id in ADMIN_IDS))


@user.message(Command("help"))
@user.message(F.text == B_HELP)
async def help_(m: Message):
    await m.answer(HELP)


@user.message(F.text == B_CODE)
async def ask_code(m: Message):
    await m.answer("🔑 Kino kodini yuboring.\nMasalan: <code>KINO-123</code>")


@user.message(F.text == B_TOP)
async def top(m: Message, session: AsyncSession):
    rows = await top_movies(session, 10)
    if not rows:
        return await m.answer("Hozircha ma'lumot yo'q.")
    lines = ["🔥 <b>Eng ko'p so'ralgan kinolar:</b>\n"]
    lines += [f"{i}. {e(t)} — <code>{e(co or '-')}</code>" for i, (_, t, co, _n) in enumerate(rows, 1)]
    await m.answer("\n".join(lines))


@user.message(StateFilter(None), F.text, not_command)
async def handle_code(m: Message, session: AsyncSession, db_user: User):
    code = norm_code(m.text)
    mv = await movie_by_code(session, code) if valid_code(code) else None
    session.add(MovieRequest(user_id=db_user.id, code_text=code[:128], movie_id=mv.id if mv else None))
    await session.flush()
    if mv is None:
        return await m.answer("❌ Bunday kino topilmadi.\nKodni tekshirib, qayta yuboring.")
    await send_movie(m, mv, code)


# ════════════════════════ ISHGA TUSHIRISH ════════════════════════
errors = Router(name="errors")


@errors.errors()
async def on_error(event) -> bool:
    log.error("Xatolik: %s", event.exception, exc_info=event.exception)
    try:
        if event.update.message:
            await event.update.message.answer("⚠️ Xatolik yuz berdi. Keyinroq urinib ko'ring.")
        elif event.update.callback_query:
            await event.update.callback_query.answer("⚠️ Xatolik yuz berdi.", show_alert=True)
    except Exception:
        pass
    return True


async def main() -> None:
    if not BOT_TOKEN or not ADMIN_IDS:
        sys.exit("XATO: BOT_TOKEN va ADMIN_ID o'zgaruvchilari kiritilmagan!")
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.outer_middleware(DbMiddleware())
    dp.callback_query.outer_middleware(DbMiddleware())
    dp.include_routers(errors, admin, user)
    await bot.set_my_commands([BotCommand(command="start", description="Botni ishga tushirish"),
                               BotCommand(command="help", description="Yordam")])
    await bot.delete_webhook(drop_pending_updates=True)
    me = await bot.get_me()
    log.info("Bot ishga tushdi: @%s", me.username)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
