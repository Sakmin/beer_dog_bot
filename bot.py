import asyncio
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import BotCommand

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


async def send_survey(chat_id: int):
    """Send the survey polls to the specified chat"""
    try:
        # First poll: "Идем в бар на этой неделе?"
        await bot.send_poll(
            chat_id=chat_id,
            question="Идем в бар на этой неделе?",
            options=["🟩 Да", "🟥 Нет", "🤷‍♂️ Напишу позже"],
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        
        # Second poll: "Когда тебе удобно?"
        await bot.send_poll(
            chat_id=chat_id,
            question="Когда тебе удобно?",
            options=["🟢 Четверг", "🔵 Пятница"],
            is_anonymous=False,
            allows_multiple_answers=False,
        )
    except Exception as e:
        print(f"Error sending survey to chat {chat_id}: {e}")
        channels.discard(chat_id)


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


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Handler for /start command"""
    await message.answer(
        "Привет! Я бот-опросник.\n"
        "Добавьте меня в канал, и я буду проводить опросы каждую среду в 13:00.\n"
        "Используйте /poll для ручного запуска опроса."
    )


@dp.message(Command("poll"))
async def cmd_poll(message: types.Message):
    """Handler for /poll command - manual survey trigger"""
    channels.add(message.chat.id)
    await send_survey(message.chat.id)


@dp.message(Command("register"))
async def cmd_register(message: types.Message):
    """Manually register this chat for weekly surveys"""
    channels.add(message.chat.id)
    await message.answer("Канал зарегистрирован для еженедельных опросов!")


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


# Main function
async def main():
    # Set bot commands
    await bot.set_my_commands([
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="poll", description="Запустить опрос вручную"),
    ])
    
    # Start the test scheduler (17:02 MSK today)
    test_task = asyncio.create_task(test_scheduler())
    
    # Start the weekly scheduler
    weekly_task = asyncio.create_task(weekly_scheduler())
    
    print("Bot is running...")
    print("Test survey scheduled for 17:02 MSK today")
    print("Weekly surveys will be sent every Wednesday at 13:00 Moscow time")
    
    try:
        await dp.start_polling(bot)
    finally:
        test_task.cancel()
        weekly_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
