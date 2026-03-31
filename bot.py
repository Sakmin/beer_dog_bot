import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import BotCommand
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


async def build_beer_top_message() -> str | None:
    """Build the beer recommendation message shared by surveys and /top_beer."""
    return await beer_top_service.build_message()


async def build_beer_search_message(query_text: str) -> str | None:
    """Build a tailored beer search result for a free-form user query."""
    return await beer_top_service.search_message(query_text)


async def send_top_beer_response(message: types.Message):
    """Send the beer recommendation text or a short fallback message."""
    try:
        text = await build_beer_top_message()
    except Exception as e:
        print(f"Error building beer recommendation for chat {message.chat.id}: {e}")
        text = None

    if text is None:
        await message.answer("Пока не получилось собрать подборку пива. Попробуй чуть позже.")
        return

    await message.answer(text, parse_mode="HTML")


async def send_search_beer_response(message: types.Message):
    query_text = (message.text or "").partition(" ")[2].strip()
    if not query_text:
        await message.answer(
            "Напиши запрос после команды. Например: /search_beer ne ipa simcoe до 7 градусов"
        )
        return

    try:
        text = await build_beer_search_message(query_text)
    except Exception as e:
        print(f"Error searching beer for chat {message.chat.id}: {e}")
        text = None

    if text is None:
        await message.answer("Пока не получилось подобрать пиво. Попробуй чуть позже.")
        return

    await message.answer(text, parse_mode="HTML")


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


@dp.channel_post(Command("top_beer"))
async def cmd_top_beer_channel_post(message: types.Message):
    """Handle /top_beer commands sent as channel posts."""
    await send_top_beer_response(message)


@dp.message(Command("search_beer"))
async def cmd_search_beer(message: types.Message):
    """Search currently available beer by a free-form query."""
    await send_search_beer_response(message)


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
        BotCommand(command="search_beer", description="Подобрать пиво по запросу"),
    ])
    
    # Start the test scheduler
    test_task = asyncio.create_task(test_scheduler())
    
    # Start the weekly scheduler
    weekly_task = asyncio.create_task(weekly_scheduler())
    
    # Start the reminder checker
    reminder_task = asyncio.create_task(check_voters_and_remind())
    
    print("Bot is running...")
    print("Test survey scheduled for 17:02 MSK today")
    print("Weekly surveys will be sent every Wednesday at 13:00 Moscow time")
    print("Voter reminders will be sent every Wednesday at 19:00 Moscow time")
    
    try:
        await dp.start_polling(bot)
    finally:
        test_task.cancel()
        weekly_task.cancel()
        reminder_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
