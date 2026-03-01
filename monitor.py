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
        
        # АКТИВЫ С ФОРЕКС ИСТОЧНИКАМИ
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'XAUUSD',
                'spot_apis': [
                    {
                        'name': 'FXEmpire',
                        'url': 'https://www.fxempire.com/api/v1/en/markets/xau-usd/chart',
                        'parser': self._parse_fxempire
                    },
                    {
                        'name': 'FXStreet',
                        'url': 'https://www.fxstreet.com/api/rates/real-time/XAUUSD',
                        'parser': self._parse_fxstreet
                    }
                ],
                'futures_symbol': 'GOLD-6.25',
                'futures_alt': ['GOLD', 'GOLD-12.24', 'GOLD-3.25']
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'XPDUSD',
                'spot_apis': [
                    {
                        'name': 'FXEmpire',
                        'url': 'https://www.fxempire.com/api/v1/en/markets/xpd-usd/chart',
                        'parser': self._parse_fxempire
                    },
                    {
                        'name': 'FXStreet',
                        'url': 'https://www.fxstreet.com/api/rates/real-time/XPDUSD',
                        'parser': self._parse_fxstreet
                    }
                ],
                'futures_symbol': 'PALLAD-6.25',
                'futures_alt': ['PALLAD', 'PALLAD-12.24', 'PALLAD-3.25']
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
                'name': 'ExchangeRate',
                'url': 'https://api.exchangerate-api.com/v4/latest/USD',
                'parser': self._parse_exchangerate
            },
            {
                'name': 'CurrencyAPI',
                'url': 'https://cdn.cur.su/api/latest.json',
                'parser': self._parse_currency
            }
        ]
        
        self.last_alerts = {}
        self.alert_cooldown = 3600
        self.current_spreads = {}
        self.last_usd_rub = None
        self.last_usd_rub_time = None
        
        logging.info("✅ SpreadMonitor инициализирован")

    # ========== ПАРСЕРЫ ФОРЕКС ==========
    
    async def _parse_fxempire(self, data: Dict) -> Optional[float]:
        """Парсит FXEmpire"""
        try:
            return float(data['current']['price'])
        except:
            return None

    async def _parse_fxstreet(self, data: Dict) -> Optional[float]:
        """Парсит FXStreet"""
        try:
            return float(data['last'])
        except:
            return None

    # ========== ПАРСЕРЫ КУРСА ==========
    
    async def _parse_cbr(self, data: Dict) -> Optional[float]:
        """Парсит курс ЦБ РФ"""
        try:
            return float(data['Valute']['USD']['Value'])
        except:
            return None

    async def _parse_exchangerate(self, data: Dict) -> Optional[float]:
        """Парсит ExchangeRate-API"""
        try:
            return float(data['rates']['RUB'])
        except:
            return None

    async def _parse_currency(self, data: Dict) -> Optional[float]:
        """Парсит CurrencyAPI"""
        try:
            return float(data['rates']['RUB'])
        except:
            return None

    async def get_usd_rub(self, session: aiohttp.ClientSession) -> Optional[float]:
        """Получает курс USD/RUB"""
        
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

    async def get_spot_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает спот цену с форекса"""
        headers = {'User-Agent': 'Mozilla/5.0'}
        
        for api in asset['spot_apis']:
            try:
                async with session.get(api['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = await api['parser'](data)
                        
                        if price:
                            # Проверка реалистичности
                            if 'XAU' in asset['symbol'] and 2000 < price < 4000:
                                return price
                            elif 'XPD' in asset['symbol'] and 700 < price < 1300:
                                return price
            except:
                continue
        
        return None

    async def get_futures_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает цену фьючерса с MOEX"""
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
                return
            
            for asset in self.assets:
                try:
                    # Спот с форекса
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        continue
                    
                    # Фьючерс с MOEX
                    futures_rub = await self.get_futures_price(session, asset)
                    if not futures_rub:
                        continue
                    
                    # Конвертация и спред
                    futures_usd = futures_rub / usd_rub
                    spread = ((futures_usd - spot) / spot) * 100
                    
                    # Сохраняем
                    self.current_spreads[asset['name']] = {
                        'name': asset['name'],
                        'spot_usd': round(spot, 2),
                        'futures_rub': round(futures_rub, 2),
                        'futures_usd': round(futures_usd, 2),
                        'usd_rub': round(usd_rub, 2),
                        'spread': round(spread, 2)
                    }
                    
                    # Проверяем порог
                    if abs(spread) > self.threshold:
                        await self._notify_users(users, self.current_spreads[asset['name']])
                        
                except Exception as e:
                    continue

    async def _notify_users(self, users: List[int], data: Dict):
        """Отправляет уведомление"""
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
        """Запуск мониторинга"""
        logging.info("🚀 Мониторинг запущен")
        while True:
            try:
                await self.check_prices()
            except:
                pass
            await asyncio.sleep(self.check_interval)