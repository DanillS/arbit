import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv
import os

# Импортируем наши модули
from monitor import SpreadMonitor
from database import UserDatabase

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Загружаем переменные окружения
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

# Инициализация
bot = Bot(token=TOKEN)
dp = Dispatcher()
db = UserDatabase()
monitor = SpreadMonitor(bot, db)

# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    
    # Добавляем пользователя в базу
    db.add_user(user_id, username)
    
    # Создаем красивое приветствие с доступными парами
    pairs_list = "\n".join([f"• {pair}" for pair in monitor.get_available_pairs()])
    
    welcome_text = (
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        f"Я бот для отслеживания разницы между фьючерсом и спотом.\n"
        f"📊 Как только спред превысит 2%, я пришлю уведомление.\n\n"
        f"📈 Доступные валютные пары:\n{pairs_list}\n\n"
        f"✅ Вы подписаны на уведомления!"
    )
    
    await message.answer(welcome_text)

# Обработчик команды /pairs - показать доступные пары
@dp.message(Command("pairs"))
async def cmd_pairs(message: Message):
    pairs = monitor.get_available_pairs()
    text = "📊 Отслеживаемые пары:\n" + "\n".join([f"• {pair}" for pair in pairs])
    await message.answer(text)

# Обработчик команды /status - показать текущие спреды
@dp.message(Command("status"))
async def cmd_status(message: Message):
    status_text = await monitor.get_current_spreads()
    await message.answer(status_text)

# Обработчик команды /unsubscribe - отписаться
@dp.message(Command("unsubscribe"))
async def cmd_unsubscribe(message: Message):
    user_id = message.from_user.id
    db.remove_user(user_id)
    await message.answer("❌ Вы отписались от уведомлений. Чтобы подписаться снова, нажмите /start")

# Запуск бота и мониторинга
async def main():
    # Запускаем мониторинг в фоне
    asyncio.create_task(monitor.start_monitoring())
    
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())