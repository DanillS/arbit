import os
import sys
import logging
import asyncio
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv
import aiohttp
import ssl
import certifi

# ====== НАСТРОЙКА ЛОГИРОВАНИЯ ======
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ====== ЗАГРУЗКА ПЕРЕМЕННЫХ ======
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

# ====== БАЗА ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ ======
USERS_FILE = "users.json"

class UserDatabase:
    def __init__(self):
        self.users = self.load_users()
        logger.info(f"✅ Загружено {len(self.users)} пользователей")

    def load_users(self):
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, 'r') as f:
                    data = json.load(f)
                    return set(data.get('users', []))
        except:
            pass
        return set()

    def save_users(self):
        with open(USERS_FILE, 'w') as f:
            json.dump({'users': list(self.users)}, f)

    def add_user(self, user_id: int):
        if user_id not in self.users:
            self.users.add(user_id)
            self.save_users()
            logger.info(f"✅ Новый пользователь: {user_id}")

    def get_all_users(self):
        return list(self.users)

# ====== МОНИТОРИНГ СПРЕДОВ ======
class SpreadMonitor:
    def __init__(self, bot, database):
        self.bot = bot
        self.db = database
        self.threshold = 0.1
        self.check_interval = 60
        
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'XAUUSD',
                'spot_url': 'https://www.fxempire.com/api/v1/en/markets/xau-usd/chart',
                'futures_symbol': 'GOLD-6.25',
                'futures_alt': ['GOLD', 'GOLD-12.24', 'GOLD-3.25']
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'XPDUSD',
                'spot_url': 'https://www.fxempire.com/api/v1/en/markets/xpd-usd/chart',
                'futures_symbol': 'PALLAD-6.25',
                'futures_alt': ['PALLAD', 'PALLAD-12.24', 'PALLAD-3.25']
            }
        ]
        
        self.usd_rub_sources = [
            {
                'name': 'CBR',
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js',
                'parser': self._parse_cbr
            },
            {
                'name': 'ExchangeRate',
                'url': 'https://api.exchangerate-api.com/v4/latest/USD',
                'parser': self._parse_exchangerate
            }
        ]
        
        self.last_alerts = {}
        self.current_spreads = {}
        self.last_usd_rub = None
        self.last_usd_rub_time = None
        
        logger.info("✅ SpreadMonitor инициализирован")

    # ✅ ВАЖНО: ЭТОТ МЕТОД НУЖЕН ДЛЯ /start
    def get_assets_names(self):
        """Возвращает список названий активов"""
        return [asset['name'] for asset in self.assets]

    async def _parse_cbr(self, data):
        try:
            return float(data['Valute']['USD']['Value'])
        except:
            return None

    async def _parse_exchangerate(self, data):
        try:
            return float(data['rates']['RUB'])
        except:
            return None

    async def _parse_fxempire(self, data):
        try:
            return float(data['current']['price'])
        except:
            return None

    async def get_usd_rub(self, session):
        if self.last_usd_rub and self.last_usd_rub_time:
            age = (datetime.now() - self.last_usd_rub_time).seconds
            if age < 300:
                return self.last_usd_rub
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        for source in self.usd_rub_sources:
            try:
                async with session.get(source['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        if rate and 50 < rate < 150:
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
            except:
                continue
        return None

    async def get_spot_price(self, session, asset):
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            async with session.get(asset['spot_url'], headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = await self._parse_fxempire(data)
                    if price:
                        if 'XAU' in asset['symbol'] and 2000 < price < 4000:
                            return price
                        elif 'XPD' in asset['symbol'] and 700 < price < 1300:
                            return price
        except:
            pass
        return None

    async def get_futures_price(self, session, asset):
        symbols = [asset['futures_symbol']] + asset.get('futures_alt', [])
        
        for symbol in symbols:
            try:
                url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{symbol}.json"
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'marketdata' in data and 'data' in data['marketdata']:
                            rows = data['marketdata']['data']
                            if rows and len(rows) > 0:
                                for idx in [12, 10, 8]:
                                    if idx < len(rows[0]) and rows[0][idx]:
                                        price = float(rows[0][idx])
                                        if 'GOLD' in asset['symbol'] and 200000 < price < 400000:
                                            return price
                                        elif 'PALLAD' in asset['symbol'] and 50000 < price < 200000:
                                            return price
            except:
                continue
        return None

    async def check_prices(self):
        users = self.db.get_all_users()
        if not users:
            return
        
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            usd_rub = await self.get_usd_rub(session)
            if not usd_rub:
                return
            
            for asset in self.assets:
                try:
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        continue
                    
                    futures_rub = await self.get_futures_price(session, asset)
                    if not futures_rub:
                        continue
                    
                    futures_usd = futures_rub / usd_rub
                    spread = ((futures_usd - spot) / spot) * 100
                    
                    self.current_spreads[asset['name']] = {
                        'name': asset['name'],
                        'spot_usd': round(spot, 2),
                        'futures_rub': round(futures_rub, 2),
                        'usd_rub': round(usd_rub, 2),
                        'spread': round(spread, 2)
                    }
                    
                    if abs(spread) > self.threshold:
                        await self._notify_users(users, self.current_spreads[asset['name']])
                except:
                    continue

    async def _notify_users(self, users, data):
        emoji = "🟢" if data['spread'] > 0 else "🔴"
        msg = (
            f"{emoji} <b>{data['name']}</b>\n"
            f"📈 Спред: {data['spread']:.2f}%\n"
            f"💰 Спот: ${data['spot_usd']}\n"
            f"💎 Фьюч: {data['futures_rub']} RUB\n"
            f"💱 Курс: {data['usd_rub']}"
        )
        
        for user_id in users:
            try:
                await self.bot.send_message(user_id, msg, parse_mode="HTML")
            except:
                continue

    async def get_current_spreads(self):
        if not self.current_spreads:
            return "📊 Нет данных"
        
        lines = ["📊 Текущие спреды:\n"]
        for name, data in self.current_spreads.items():
            lines.append(f"{name}: {data['spread']:.2f}%")
            lines.append(f"   Спот: ${data['spot_usd']}")
            lines.append(f"   Фьюч: {data['futures_rub']} RUB")
            lines.append("")
        return "\n".join(lines)

    async def start_monitoring(self):
        logger.info("🚀 Мониторинг запущен")
        while True:
            try:
                await self.check_prices()
            except:
                pass
            await asyncio.sleep(self.check_interval)

# ====== ИНИЦИАЛИЗАЦИЯ ======
bot = Bot(token=TOKEN)
dp = Dispatcher()
db = UserDatabase()
monitor = SpreadMonitor(bot, db)

# ====== КОМАНДЫ ======
@dp.message(Command("start"))
async def cmd_start(message: Message):
    db.add_user(message.from_user.id)
    # ✅ ИСПОЛЬЗУЕМ ПРАВИЛЬНЫЙ МЕТОД
    assets_list = "\n".join([f"• {name}" for name in monitor.get_assets_names()])
    await message.answer(
        f"🚀 Бот запущен!\n\n"
        f"📊 Отслеживаемые активы:\n{assets_list}\n\n"
        f"📈 Команды:\n"
        f"/status - текущие спреды\n"
        f"/help - помощь"
    )

@dp.message(Command("status"))
async def cmd_status(message: Message):
    status = await monitor.get_current_spreads()
    await message.answer(status, parse_mode="HTML")

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "📚 Доступные команды:\n"
        "/start - запустить бота\n"
        "/status - текущие спреды\n"
        "/help - эта справка"
    )

# ====== ЗАПУСК ======
async def main():
    asyncio.create_task(monitor.start_monitoring())
    logger.info("🚀 Бот запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")