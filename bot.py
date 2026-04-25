import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from typing import Iterable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import ChatMemberUpdated, Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# =========================
# НАСТРОЙКИ БОТА
# =========================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
DB_PATH = os.getenv("DB_PATH", "bot.db").strip()

# Текст автоматической рассылки.
AUTO_MESSAGE = os.getenv(
    "AUTO_MESSAGE",
    "Кому нужна работа на стройке? Пишите в личку."
).replace("\\n", "\n").strip()

if not BOT_TOKEN:
    raise RuntimeError("Укажи BOT_TOKEN в файле .env или в переменных окружения")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


# =========================
# РАБОТА С БАЗОЙ SQLITE
# =========================
def init_db() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                chat_type TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def upsert_chat(chat_id: int, title: str | None, chat_type: str) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO chats (chat_id, title, chat_type, is_active, updated_at)
            VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(chat_id) DO UPDATE SET
                title=excluded.title,
                chat_type=excluded.chat_type,
                is_active=1,
                updated_at=CURRENT_TIMESTAMP
            """,
            (chat_id, title, chat_type),
        )
        conn.commit()


def deactivate_chat(chat_id: int) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "UPDATE chats SET is_active=0, updated_at=CURRENT_TIMESTAMP WHERE chat_id=?",
            (chat_id,),
        )
        conn.commit()


def get_active_chat_ids() -> list[int]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            "SELECT chat_id FROM chats WHERE is_active=1 ORDER BY created_at ASC"
        ).fetchall()
    return [row[0] for row in rows]


def get_chat_rows() -> list[tuple[int, str | None, str, int]]:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute(
            "SELECT chat_id, title, chat_type, is_active FROM chats ORDER BY created_at ASC"
        ).fetchall()
    return rows


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def safe_send(chat_id: int, text: str) -> tuple[bool, str]:
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        await asyncio.sleep(0.08)
        return True, "ok"
    except Exception as e:
        err = str(e)
        err_lower = err.lower()

        if (
            "bot was kicked" in err_lower
            or "chat not found" in err_lower
            or "forbidden" in err_lower
            or "have no rights" in err_lower
        ):
            deactivate_chat(chat_id)

        logger.warning("Не удалось отправить в %s: %s", chat_id, err)
        return False, err


async def broadcast(text: str, chat_ids: Iterable[int]) -> tuple[int, int]:
    ok_count = 0
    fail_count = 0

    for chat_id in chat_ids:
        ok, _ = await safe_send(chat_id, text)
        if ok:
            ok_count += 1
        else:
            fail_count += 1

    return ok_count, fail_count


def format_chats() -> str:
    rows = get_chat_rows()
    if not rows:
        return "База чатов пока пустая."

    lines = ["<b>Подключённые чаты:</b>"]
    for chat_id, title, chat_type, is_active in rows:
        status = "активен" if is_active else "неактивен"
        title = title or "Без названия"
        lines.append(f"• <code>{chat_id}</code> | {title} | {chat_type} | {status}")
    return "\n".join(lines)


async def auto_broadcast_job() -> None:
    chat_ids = get_active_chat_ids()
    if not chat_ids:
        logger.info("Авторассылка пропущена: нет активных чатов")
        return

    ok_count, fail_count = await broadcast(AUTO_MESSAGE, chat_ids)
    logger.info(
        "Авторассылка завершена. Успешно: %s, ошибок: %s",
        ok_count,
        fail_count,
    )


def setup_scheduler() -> None:
    # Каждый день в 07:00
    scheduler.add_job(
        auto_broadcast_job,
        trigger="cron",
        hour=7,
        minute=0,
        id="daily_morning_broadcast",
        replace_existing=True,
    )

    # Каждый день в 18:00
    scheduler.add_job(
        auto_broadcast_job,
        trigger="cron",
        hour=18,
        minute=0,
        id="daily_evening_broadcast",
        replace_existing=True,
    )

    # Каждый день в 23:00
    scheduler.add_job(
        auto_broadcast_job,
        trigger="cron",
        hour=23,
        minute=0,
        id="daily_night_broadcast",
        replace_existing=True,
    )


# =========================
# ПРОВЕРКА ДОСТУПА
# =========================
async def deny_if_not_admin(message: Message) -> bool:
    user = message.from_user
    if not user or not is_admin(user.id):
        await message.answer("У тебя нет доступа к этой команде.")
        return True
    return False


# =========================
# СОБЫТИЯ ГРУПП
# =========================
@dp.my_chat_member()
async def on_bot_added_to_chat(event: ChatMemberUpdated) -> None:
    # Когда бота добавили в группу или дали права, сохраняем чат автоматически.
    chat = event.chat
    new_status = event.new_chat_member.status

    if new_status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR}:
        upsert_chat(chat.id, chat.title, chat.type)
        logger.info("Бот добавлен в чат: %s (%s)", chat.title, chat.id)

    # Если бота удалили или выгнали, помечаем чат как неактивный.
    if new_status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}:
        deactivate_chat(chat.id)
        logger.info("Бот удалён из чата: %s (%s)", chat.title, chat.id)


@dp.message(F.chat.type.in_({"group", "supergroup"}))
async def auto_register_groups(message: Message) -> None:
    # Любое сообщение в группе обновляет информацию о чате в базе.
    chat = message.chat
    upsert_chat(chat.id, chat.title, chat.type)


# =========================
# КОМАНДЫ
# =========================
@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    chat = message.chat
    upsert_chat(chat.id, chat.title, chat.type)

    if chat.type in {"group", "supergroup"}:
        await message.answer("Бот подключён и запомнил этот чат.")
        return

    await message.answer(
        "Бот активен.\n\n"
        "Добавь бота в нужные группы и дай ему право писать сообщения.\n"
        "Он сам запомнит группы без команды /register.\n\n"
        "Авторассылка настроена на 07:00, 18:00 и 23:00."
    )


@dp.message(Command("register"))
async def cmd_register(message: Message) -> None:
    # Оставляем как запасной вариант.
    chat = message.chat
    upsert_chat(chat.id, chat.title, chat.type)
    await message.answer(f"Чат сохранён.\nID: <code>{chat.id}</code>")


@dp.message(Command("unregister"))
async def cmd_unregister(message: Message) -> None:
    deactivate_chat(message.chat.id)
    await message.answer("Этот чат отключён от рассылки.")


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if await deny_if_not_admin(message):
        return

    await message.answer(
        "<b>Команды администратора</b>\n"
        "/chats — список чатов\n"
        "/broadcast ТЕКСТ — отправить во все активные чаты\n"
        "/broadcast_to CHAT_ID ТЕКСТ — отправить в один чат\n"
        "/settext ТЕКСТ — изменить текст авторассылки до следующего перезапуска\n"
        "/myid — показать твой user_id\n\n"
        "<b>Для групп</b>\n"
        "Достаточно просто добавить бота в группу и дать право писать.\n"
        "/register можно не использовать.\n"
        "/unregister — выключить группу из рассылки"
    )


@dp.message(Command("myid"))
async def cmd_myid(message: Message) -> None:
    user = message.from_user
    if not user:
        return
    await message.answer(f"Твой user_id: <code>{user.id}</code>")


@dp.message(Command("chats"))
async def cmd_chats(message: Message) -> None:
    if await deny_if_not_admin(message):
        return
    await message.answer(format_chats())


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, command: CommandObject) -> None:
    if await deny_if_not_admin(message):
        return

    text = (command.args or "").strip()
    if not text:
        await message.answer("Использование: /broadcast ТЕКСТ")
        return

    chat_ids = get_active_chat_ids()
    if not chat_ids:
        await message.answer("Нет активных чатов для рассылки.")
        return

    await message.answer(f"Запускаю рассылку по {len(chat_ids)} чатам...")
    ok_count, fail_count = await broadcast(text, chat_ids)
    await message.answer(f"Готово. Успешно: {ok_count}, ошибок: {fail_count}.")



@dp.message(Command("sendauto"))
async def cmd_sendauto(message: Message) -> None:
    if await deny_if_not_admin(message):
        return

    chat_ids = get_active_chat_ids()
    if not chat_ids:
        await message.answer("Нет активных чатов для рассылки.")
        return

    await message.answer(f"Отправляю текущий AUTO_MESSAGE по {len(chat_ids)} чатам...")
    ok_count, fail_count = await broadcast(AUTO_MESSAGE, chat_ids)
    await message.answer(f"Готово. Успешно: {ok_count}, ошибок: {fail_count}.")

@dp.message(Command("broadcast_to"))
async def cmd_broadcast_to(message: Message, command: CommandObject) -> None:
    if await deny_if_not_admin(message):
        return

    raw = (command.args or "").strip()
    if not raw:
        await message.answer("Использование: /broadcast_to CHAT_ID ТЕКСТ")
        return

    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /broadcast_to CHAT_ID ТЕКСТ")
        return

    chat_id_raw, text = parts
    try:
        chat_id = int(chat_id_raw)
    except ValueError:
        await message.answer("CHAT_ID должен быть числом.")
        return

    ok, err = await safe_send(chat_id, text)
    if ok:
        await message.answer("Сообщение отправлено.")
    else:
        await message.answer(f"Ошибка отправки: <code>{err}</code>")


@dp.message(Command("settext"))
async def cmd_settext(message: Message, command: CommandObject) -> None:
    global AUTO_MESSAGE

    if await deny_if_not_admin(message):
        return

    text = (command.args or "").strip()
    if not text:
        await message.answer("Использование: /settext ТЕКСТ")
        return

    AUTO_MESSAGE = text
    await message.answer(
        "Текст авторассылки обновлён до следующего перезапуска приложения."
    )


async def main() -> None:
    init_db()
    setup_scheduler()
    scheduler.start()

    logger.info("Бот запущен")
    logger.info("Авторассылка активна: 07:00, 18:00, 23:00")
    logger.info("Текущий текст авторассылки: %s", AUTO_MESSAGE)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
