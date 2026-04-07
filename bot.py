import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram import F
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from beer_top import BeerTopService

# Load environment variables
load_dotenv()

# Initialize bot
BOT_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Moscow timezone
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# Store channel IDs for weekly surveys
channels = set()

# Store poll voters: {chat_id: {user_id: set(poll_ids)}}
poll_voters = {}

# Store current poll message IDs: {chat_id: [poll1_id, poll2_id]}
active_polls = {}

# Store users who have started the bot (for DM reminders)
bot_users = set()

beer_top_service = BeerTopService()

HOP_GUIDE_TEXT = """Как читать это быстро
- <code>Simcoe</code> / <code>Chinook</code> / <code>Columbus</code> / <code>Centennial</code> -> жди хвою, смолу, грейпфрут, сухость
- <code>Citra</code> / <code>Mosaic</code> / <code>Galaxy</code> / <code>Azacca</code> -> жди сок, тропики, мягкость, hazy-профиль
- <code>Nelson</code> / <code>Motueka</code> / <code>Riwaka</code> / <code>Nectaron</code> -> жди новозеландский стиль: лайм, passion fruit, виноград, крыжовник
- <code>Sabro</code> -> почти всегда ищи кокос и лайм
- <code>Cascade</code> / <code>Amarillo</code> -> более классический американский цитрус, без такой “смузи-сочности”, как у hazy-хмелей

<code>Simcoe</code>
Профиль: хвоя, смола, грейпфрут
Похоже: лесной, плотный, west coast
Где встречается: West Coast IPA, American IPA, DIPA

<code>Citra</code>
Профиль: лайм, манго, маракуйя
Похоже: сочный, яркий, тропический
Где встречается: NEIPA, Hazy IPA, Pale Ale

<code>Mosaic</code>
Профиль: тропики, ягоды, цитрус
Похоже: Citra, но сложнее и глубже
Где встречается: NEIPA, Hazy Pale Ale, IPA

<code>Amarillo</code>
Профиль: апельсин, мандарин, цветы
Похоже: мягкий оранжевый цитрус
Где встречается: APA, IPA, Blonde / Pale

<code>Centennial</code>
Профиль: грейпфрут, лимон, хвоя
Похоже: классический американский IPA
Где встречается: American IPA, West Coast IPA

<code>Cascade</code>
Профиль: грейпфрут, цветы, трава
Похоже: олдскульный цитрусовый профиль
Где встречается: APA, классический IPA

<code>Chinook</code>
Профиль: сосна, смола, специи
Похоже: Simcoe, но грубее и суше
Где встречается: West Coast IPA, Red IPA

<code>Columbus / CTZ</code>
Профиль: dank, смола, перец
Похоже: грязновато-смолистый, резкий
Где встречается: West Coast IPA, DIPA

<code>Galaxy</code>
Профиль: маракуйя, персик, цитрус
Похоже: очень сочный южный тропик
Где встречается: NEIPA, Hazy IPA

<code>Nelson Sauvin</code>
Профиль: белый виноград, крыжовник
Похоже: винный, необычный, суховатый
Где встречается: NZ Pils, IPA, Hazy IPA

<code>Motueka</code>
Профиль: лайм, цедра, mojito
Похоже: свежий лаймовый профиль
Где встречается: NZ Pils, Saison, IPA

<code>Riwaka</code>
Профиль: маракуйя, грейпфрут, цитрус
Похоже: очень яркий NZ tropical
Где встречается: Hazy IPA, NZ IPA

<code>Nectaron</code>
Профиль: ананас, маракуйя, персик
Похоже: плотный тропический микс
Где встречается: Hazy IPA, NEIPA

<code>Sabro</code>
Профиль: кокос, лайм, древесность
Похоже: экзотика, легко узнаваемый
Где встречается: Hazy IPA, Fruited IPA

<code>Idaho 7</code>
Профиль: абрикос, хвоя, тропики
Похоже: мост между Simcoe и Mosaic
Где встречается: IPA, Hazy IPA

<code>Azacca</code>
Профиль: манго, ананас, цитрус
Похоже: прямой сочный tropical
Где встречается: NEIPA, Pale Ale

<code>El Dorado</code>
Профиль: груша, конфета, тропики
Похоже: мягкий сладковатый фрукт
Где встречается: Hazy IPA, Fruited IPA

<code>Vic Secret</code>
Профиль: ананас, хвоя, маракуйя
Похоже: Galaxy, но суше
Где встречается: IPA, Hazy IPA

<code>Warrior</code>
Профиль: смола, цитрус, горечь
Похоже: чаще база под другие хмели
Где встречается: IPA, DIPA

<code>Apollo</code>
Профиль: резкая смола, цитрус, трава
Похоже: мощный bittering + aroma
Где встречается: DIPA, West Coast IPA"""


async def build_beer_top_message() -> str | None:
    """Build the beer recommendation message shared by surveys and /top_beer."""
    return await beer_top_service.build_message()


async def build_drink_already_message() -> str | None:
    """Build beer recommendations excluding already drunk beers."""
    return await beer_top_service.build_drink_already_message()


async def refresh_beer_cache() -> int:
    """Refresh the persisted beer inventory cache from live sources."""
    return await beer_top_service.refresh_cache()


def build_beer_menu_download() -> tuple[str, str] | None:
    """Build a text export of the cached beer menu."""
    return beer_top_service.build_menu_export()


def build_more_top_categories() -> list[tuple[str, str]]:
    """Build available ranked categories for /more_top selection."""
    return beer_top_service.more_top_categories()


def build_more_top_category_message(category_key: str) -> tuple[str, str] | None:
    """Build extended top message for a selected category."""
    return beer_top_service.more_top_category_message(category_key)


async def send_top_beer_response(message: types.Message):
    """Send the beer recommendation text or a short fallback message."""
    try:
        text = await build_beer_top_message()
    except Exception as e:
        print(f"Error building beer recommendation for chat {message.chat.id}: {e}")
        text = None

    if text is None:
        await message.answer("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.")
        return

    await message.answer(text, parse_mode="HTML")


async def send_refresh_beer_cache_response(message: types.Message):
    if message.chat.type != "private":
        await message.answer("Эта команда доступна только в личке с ботом.")
        return

    try:
        count = await refresh_beer_cache()
    except Exception as e:
        print(f"Error refreshing beer cache for chat {message.chat.id}: {e}")
        await message.answer("Не получилось обновить кэш пива. Попробуй чуть позже.")
        return

    await message.answer(f"Кэш пива обновлен. Сохранено позиций: {count}.")


async def send_drink_already_response(message: types.Message):
    try:
        text = await build_drink_already_message()
    except Exception as e:
        print(f"Error building already-drunk beer recommendation for chat {message.chat.id}: {e}")
        text = None

    if text is None:
        await message.answer("Пока нет готового кэша пива или не удалось прочитать выпитые сорта.")
        return

    await message.answer(text, parse_mode="HTML")


async def send_download_menu_response(message: types.Message):
    export = build_beer_menu_download()
    if export is None:
        await message.answer("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.")
        return

    filename, content = export
    await message.answer_document(
        BufferedInputFile(content.encode("utf-8"), filename=filename)
    )


async def send_more_top_response(message: types.Message):
    categories = build_more_top_categories()
    if not categories:
        await message.answer("Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache.")
        return

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"more_top:{key}")]
            for label, key in categories
        ]
    )
    await message.answer("Выбери категорию:", reply_markup=keyboard)


async def send_hop_guide_response(message: types.Message):
    await message.answer(HOP_GUIDE_TEXT, parse_mode="HTML")


async def send_survey(chat_id: int):
    """Send the survey polls to the specified chat"""
    try:
        # Reset voters for this chat
        poll_voters[chat_id] = {}
        
        # First poll: "Идем в бар на этой неделе?"
        poll1 = await bot.send_poll(
            chat_id=chat_id,
            question="Идем в бар на этой неделе?",
            options=["🟩 Да", "🟥 Нет", "🤷‍♂️ Напишу позже"],
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        
        # Second poll: "Когда тебе удобно?"
        poll2 = await bot.send_poll(
            chat_id=chat_id,
            question="Когда тебе удобно?",
            options=["🟢 Четверг", "🔵 Пятница"],
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        
        # Store poll IDs
        active_polls[chat_id] = [poll1.message_id, poll2.message_id]

    except Exception as e:
        print(f"Error sending survey to chat {chat_id}: {e}")
        channels.discard(chat_id)
        return

    try:
        beer_message = await build_beer_top_message()
        if beer_message:
            await bot.send_message(chat_id=chat_id, text=beer_message, parse_mode="HTML")
    except Exception as e:
        print(f"Error sending beer recommendations to chat {chat_id}: {e}")


def next_cache_refresh_time(now: datetime) -> datetime:
    target = now.replace(hour=8, minute=0, second=0, microsecond=0)
    days_until_wednesday = (2 - now.weekday()) % 7
    if days_until_wednesday == 0 and now >= target:
        days_until_wednesday = 7
    return target + timedelta(days=days_until_wednesday)


@dp.poll_answer()
async def track_poll_answer(poll_answer: types.PollAnswer):
    """Track who voted in polls"""
    chat_id = poll_answer.voter_chat.id
    user_id = poll_answer.voter_user.id
    
    if chat_id not in poll_voters:
        poll_voters[chat_id] = {}
    
    if user_id not in poll_voters[chat_id]:
        poll_voters[chat_id][user_id] = set()
    
    poll_voters[chat_id][user_id].add(poll_answer.poll_id)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Handler for /start command"""
    bot_users.add(message.from_user.id)
    
    # Only respond in private messages
    if message.chat.type == "private":
        await message.answer(
            "Привет! Я бот-опросник.\n\n"
            "Я провожу опросы в каналах и группах.\n"
            "Добавь меня в канал, и я буду проводить опросы каждую среду в 13:00.\n\n"
            "Если ты уже в канале с ботом — просто дождись опроса или используй /poll в канале."
        )


@dp.message(Command("poll"))
async def cmd_poll(message: types.Message):
    """Handler for /poll command - manual survey trigger"""
    # Only work in channels/groups, not in private messages
    if message.chat.type == "private":
        await message.answer(
            "Опросы проводятся только в каналах и группах.\n"
            "Используй эту команду в канале, где нужно провести опрос."
        )
        return
    
    channels.add(message.chat.id)
    await send_survey(message.chat.id)


@dp.message(Command("register"))
async def cmd_register(message: types.Message):
    """Manually register this chat for weekly surveys"""
    # Only work in channels/groups, not in private messages
    if message.chat.type == "private":
        await message.answer(
            "Регистрация работает только в каналах и группах.\n"
            "Используй эту команду в канале."
        )
        return
    
    channels.add(message.chat.id)
    await message.answer("Канал зарегистрирован для еженедельных опросов!")


@dp.message(Command("top_beer"))
async def cmd_top_beer(message: types.Message):
    """Send the current beer recommendation without creating polls."""
    await send_top_beer_response(message)


@dp.message(Command("drink_already"))
async def cmd_drink_already(message: types.Message):
    """Send top beer categories excluding already drunk beers."""
    await send_drink_already_response(message)


@dp.channel_post(Command("top_beer"))
async def cmd_top_beer_channel_post(message: types.Message):
    """Handle /top_beer commands sent as channel posts."""
    await send_top_beer_response(message)


@dp.message(Command("refresh_beer_cache"))
async def cmd_refresh_beer_cache(message: types.Message):
    """Refresh local beer inventory cache from live sources."""
    await send_refresh_beer_cache_response(message)


@dp.message(Command("download_menu"))
async def cmd_download_menu(message: types.Message):
    """Send the current cached beer menu as a .txt document."""
    await send_download_menu_response(message)


@dp.message(Command("hop_guide"))
async def cmd_hop_guide(message: types.Message):
    """Send a hop flavor cheat sheet."""
    await send_hop_guide_response(message)


@dp.message(Command("more_top"))
async def cmd_more_top(message: types.Message):
    """Show available beer categories for extended top lists."""
    await send_more_top_response(message)


@dp.callback_query(F.data.startswith("more_top:"))
async def handle_more_top_category(callback_query: types.CallbackQuery):
    """Send up to 15 beers for the selected top category."""
    category_key = (callback_query.data or "").partition(":")[2]
    result = build_more_top_category_message(category_key)
    await callback_query.answer()
    if result is None:
        await callback_query.message.answer(
            "Пока нет готового кэша пива. Сначала выполни /refresh_beer_cache."
        )
        return

    _, text = result
    await callback_query.message.answer(text, parse_mode="HTML")


@dp.my_chat_member()
async def bot_chat_member_update(update: types.Update):
    """Track when bot is added/removed from channels"""
    new_status = update.my_chat_member.new_chat_member.status
    
    chat_id = update.my_chat_member.chat.id
    chat_type = update.my_chat_member.chat.type
    
    if chat_type in ["channel", "group", "supergroup"]:
        if new_status in ["member", "administrator"]:
            channels.add(chat_id)
            await bot.send_message(
                chat_id=chat_id,
                text="Спасибо, что добавили меня! Опросы будут приходить каждую среду в 13:00 по Москве.\nИспользуйте /poll для запуска опроса прямо сейчас."
            )
        elif new_status in ["left", "kicked"]:
            channels.discard(chat_id)


async def check_voters_and_remind():
    """Check who hasn't voted and send reminders at 19:00 MSK"""
    while True:
        now = datetime.now(MOSCOW_TZ)
        
        # Schedule for 19:00 MSK today
        reminder_time = now.replace(hour=19, minute=0, second=0, microsecond=0)
        
        if now >= reminder_time:
            # Already past 19:00, schedule for tomorrow
            reminder_time += timedelta(days=1)
        
        # Only send reminders on Wednesday
        if reminder_time.weekday() != 2:  # 2 = Wednesday
            days_until_wednesday = (2 - now.weekday()) % 7
            if days_until_wednesday == 0:
                days_until_wednesday = 7
            reminder_time = now.replace(
                hour=19, minute=0, second=0, microsecond=0
            ) + timedelta(days=days_until_wednesday)
        
        sleep_time = (reminder_time - now).total_seconds()
        print(f"Next voter check at {reminder_time.strftime('%Y-%m-%d %H:%M:%S')} MSK")
        await asyncio.sleep(sleep_time)
        
        # Check voters for each channel
        for chat_id in channels:
            if chat_id not in poll_voters:
                continue
            
            voters = poll_voters.get(chat_id, {})
            voted_user_ids = set(voters.keys())
            
            # Get channel members (for groups/channels)
            try:
                members = []
                offset = 0
                while True:
                    chunk = await bot.get_chat_members(
                        chat_id=chat_id,
                        offset=offset,
                        limit=100
                    )
                    if not chunk:
                        break
                    members.extend(chunk)
                    offset += 100
                    if len(chunk) < 100:
                        break
            except Exception as e:
                print(f"Can't get members for chat {chat_id}: {e}")
                continue
            
            # Check each member
            for member in members:
                user_id = member.user.id
                
                # Skip bots
                if member.user.is_bot:
                    continue
                
                # Check if user hasn't voted
                if user_id not in voted_user_ids:
                    # Try to send reminder
                    if user_id in bot_users:
                        try:
                            await bot.send_message(
                                chat_id=user_id,
                                text="Привет! Я всего лишь бот, но даже я вижу, что ты ещё не ответил на опрос по бару."
                            )
                            print(f"Reminder sent to user {user_id}")
                        except Exception as e:
                            print(f"Can't send reminder to user {user_id}: {e}")


async def cache_refresh_scheduler():
    """Refresh beer cache every Wednesday at 08:00 GMT+3 silently."""
    while True:
        now = datetime.now(MOSCOW_TZ)
        refresh_time = next_cache_refresh_time(now)
        sleep_time = (refresh_time - now).total_seconds()
        print(f"Next cache refresh at {refresh_time.strftime('%Y-%m-%d %H:%M:%S')} MSK")
        await asyncio.sleep(sleep_time)

        try:
            count = await refresh_beer_cache()
            print(f"Beer cache auto-refresh completed. Saved entries: {count}")
        except Exception as e:
            print(f"Beer cache auto-refresh failed: {e}")


async def test_scheduler():
    """Schedule test survey - runs 5 seconds after start for testing"""
    now = datetime.now(MOSCOW_TZ)
    print(f"Current time (MSK): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Wait 5 seconds for testing
    sleep_time = 5
    print(f"Test survey will be sent in {sleep_time} seconds...")
    await asyncio.sleep(sleep_time)
    
    # Send test survey to all channels
    print(f"Sending survey to {len(channels)} channel(s)")
    for channel_id in channels:
        await send_survey(channel_id)
    
    print("Test survey sent!")


async def weekly_scheduler():
    """Schedule survey to run every Wednesday at 13:00 Moscow time"""
    while True:
        now = datetime.now(MOSCOW_TZ)
        
        # Calculate next Wednesday at 13:00 Moscow time
        days_until_wednesday = (2 - now.weekday()) % 7
        if days_until_wednesday == 0 and now.hour >= 13:
            days_until_wednesday = 7
        
        next_wednesday = now.replace(
            hour=13, minute=0, second=0, microsecond=0
        ) + timedelta(days=days_until_wednesday)
        
        sleep_time = (next_wednesday - now).total_seconds()
        await asyncio.sleep(sleep_time)
        
        # Send survey to all channels
        for channel_id in channels:
            await send_survey(channel_id)
        
        # Sleep for 1 week after sending
        await asyncio.sleep(7 * 24 * 60 * 60)


# Main function
async def main():
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="poll", description="Запустить опрос вручную"),
        BotCommand(command="top_beer", description="Показать подборку пива"),
        BotCommand(command="drink_already", description="Подборка без уже выпитых сортов"),
        BotCommand(command="more_top", description="Показать больше пива по категориям"),
        BotCommand(command="hop_guide", description="Шпаргалка по хмелям"),
        BotCommand(command="refresh_beer_cache", description="Обновить кэш пива"),
        BotCommand(command="download_menu", description="Скачать меню пива в .txt"),
    ])
    
    # Start the test scheduler
    test_task = asyncio.create_task(test_scheduler())
    
    # Start the weekly scheduler
    weekly_task = asyncio.create_task(weekly_scheduler())
    
    # Start the reminder checker
    reminder_task = asyncio.create_task(check_voters_and_remind())

    # Start cache refresh scheduler
    cache_refresh_task = asyncio.create_task(cache_refresh_scheduler())
    
    print("Bot is running...")
    print("Test survey scheduled for 17:02 MSK today")
    print("Weekly surveys will be sent every Wednesday at 13:00 Moscow time")
    print("Voter reminders will be sent every Wednesday at 19:00 Moscow time")
    print("Beer cache will refresh every Wednesday at 08:00 Moscow time")
    
    try:
        await dp.start_polling(bot)
    finally:
        test_task.cancel()
        weekly_task.cancel()
        reminder_task.cancel()
        cache_refresh_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
