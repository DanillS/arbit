import aiohttp
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional
import ssl
import certifi

class SpreadMonitor:
    def __init__(self, bot, database):
        self.bot = bot
        self.db = database
        self.threshold = 2.0
        self.check_interval = 60
        
        # АКТИВЫ С РАБОЧИМИ API
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'GOLD',
                # Используем разные API для надежности
                'spot_apis': [
                    {
                        'name': 'GoldAPI',
                        'url': 'https://api.gold-api.com/price/XAU',
                        'parser': self._parse_goldapi
                    },
                    {
                        'name': 'MetalPriceAPI',
                        'url': 'https://api.metalpriceapi.com/v1/latest?base=USD&currencies=XAU',
                        'parser': self._parse_metalprice
                    },
                    {
                        'name': 'CurrencyAPI',
                        'url': 'https://cdn.cur.su/api/latest.json',
                        'parser': self._parse_currency_xau
                    }
                ],
                'futures_symbol': 'GOLD-6.25'
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'XPD',
                'spot_apis': [
                    {
                        'name': 'MetalPriceAPI',
                        'url': 'https://api.metalpriceapi.com/v1/latest?base=USD&currencies=XPD',
                        'parser': self._parse_metalprice
                    },
                    {
                        'name': 'CurrencyAPI',
                        'url': 'https://cdn.cur.su/api/latest.json',
                        'parser': self._parse_currency_xpd
                    }
                ],
                'futures_symbol': 'PALLAD-6.25'
            }
        ]
        
        # ИСТОЧНИКИ КУРСА USD/RUB
        self.usd_rub_sources = [
            {
                'name': 'CBR',
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js',
                'parser': self._parse_cbr
            },
            {
                'name': 'CurrencyAPI',
                'url': 'https://cdn.cur.su/api/latest.json',
                'parser': self._parse_currency_usd
            }
        ]
        
        self.last_alerts = {}
        self.alert_cooldown = 3600
        self.current_spreads = {}
        self.last_usd_rub = None
        self.last_usd_rub_time = None
        
        logging.info(f"SpreadMonitor инициализирован")

    # ========== ПРОСТЫЕ ПАРСЕРЫ ==========
    
    async def _parse_cbr(self, data: Dict) -> Optional[float]:
        """Парсит курс ЦБ РФ"""
        try:
            return float(data['Valute']['USD']['Value'])
        except:
            return None

    async def _parse_currency_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с CurrencyAPI"""
        try:
            return float(data['rates']['RUB'])
        except:
            return None

    async def _parse_goldapi(self, data: Dict) -> Optional[float]:
        """Парсит цену золота с GoldAPI"""
        try:
            return float(data['price'])
        except:
            return None

    async def _parse_metalprice(self, data: Dict) -> Optional[float]:
        """Парсит цену с MetalPriceAPI"""
        try:
            if 'rates' in data:
                for key, value in data['rates'].items():
                    return float(value)
            return None
        except:
            return None

    async def _parse_currency_xau(self, data: Dict) -> Optional[float]:
        """Парсит цену золота с CurrencyAPI"""
        try:
            # Цена золота в USD за унцию
            return float(data['rates']['XAU'])
        except:
            return None

    async def _parse_currency_xpd(self, data: Dict) -> Optional[float]:
        """Парсит цену палладия с CurrencyAPI"""
        try:
            # Цена палладия в USD за унцию
            return float(data['rates']['XPD'])
        except:
            return None

    async def get_usd_rub(self, session: aiohttp.ClientSession) -> Optional[float]:
        """Получает курс USD/RUB"""
        if self.last_usd_rub and self.last_usd_rub_time:
            if (datetime.now() - self.last_usd_rub_time).seconds < 300:
                return self.last_usd_rub
        
        for source in self.usd_rub_sources:
            try:
                async with session.get(source['url'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        if rate:
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
            except:
                continue
        
        return None

    async def get_spot_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает спот цену из нескольких API"""
        for api in asset['spot_apis']:
            try:
                async with session.get(api['url'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = await api['parser'](data)
                        if price:
                            logging.info(f"✅ {asset['name']} спот из {api['name']}: ${price}")
                            return price
            except:
                continue
        return None

    async def get_futures_price(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Получает цену фьючерса с MOEX"""
        try:
            url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{symbol}.json"
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'marketdata' in data and 'data' in data['marketdata']:
                        rows = data['marketdata']['data']
                        if rows and len(rows) > 0:
                            # Пробуем получить цену
                            for idx in [12, 10, 8]:
                                if idx < len(rows[0]) and rows[0][idx]:
                                    return float(rows[0][idx])
            return None
        except:
            return None

    async def check_prices(self):
        """Проверяет цены"""
        users = self.db.get_all_users()
        if not users:
            return
        
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Получаем курс
            usd_rub = await self.get_usd_rub(session)
            if not usd_rub:
                logging.error("❌ Нет курса USD/RUB")
                return
            
            for asset in self.assets:
                try:
                    # Спот цена
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        logging.error(f"❌ Нет спота для {asset['name']}")
                        continue
                    
                    # Фьючерс цена
                    futures_rub = await self.get_futures_price(session, asset['futures_symbol'])
                    if not futures_rub:
                        logging.error(f"❌ Нет фьючерса для {asset['name']}")
                        continue
                    
                    # Конвертация и спред
                    futures_usd = futures_rub / usd_rub
                    spread = ((futures_usd - spot) / spot) * 100
                    
                    # Сохраняем
                    self.current_spreads[asset['name']] = {
                        'name': asset['name'],
                        'symbol': asset['symbol'],
                        'spot_usd': round(spot, 2),
                        'futures_rub': round(futures_rub, 2),
                        'futures_usd': round(futures_usd, 2),
                        'usd_rub': round(usd_rub, 2),
                        'spread': round(spread, 2)
                    }
                    
                    # Проверяем порог
                    if abs(spread) > self.threshold:
                        await self.notify_users(users, self.current_spreads[asset['name']])
                        
                except Exception as e:
                    logging.error(f"Ошибка {asset['name']}: {e}")

    async def notify_users(self, users: List[int], data: Dict):
        """Отправляет уведомление"""
        emoji = "🟢" if data['spread'] > 0 else "🔴"
        msg = (
            f"{emoji} <b>{data['name']}</b>\n"
            f"Спред: {data['spread']:.2f}%\n"
            f"Спот: ${data['spot_usd']}\n"
            f"Фьюч: {data['futures_rub']} RUB"
        )
        
        for user_id in users:
            try:
                await self.bot.send_message(user_id, msg, parse_mode="HTML")
            except:
                continue

    async def get_current_spreads(self) -> str:
        """Текущие спреды"""
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
        """Запуск"""
        logging.info("Мониторинг запущен")
        while True:
            try:
                await self.check_prices()
            except Exception as e:
                logging.error(f"Ошибка: {e}")
            await asyncio.sleep(self.check_interval)