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
        
        # АКТИВЫ
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'GOLD',
                'spot_apis': [
                    {
                        'name': 'MetalPriceAPI',
                        'url': 'https://api.metalpriceapi.com/v1/latest?base=USD&currencies=XAU',
                        'parser': self._parse_metalprice
                    }
                ],
                'futures_symbol': 'GOLD-6.25',
                'futures_alt': ['GOLD', 'GOLD-12.24', 'GOLD-3.25', 'GOLDF']
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'XPD',
                'spot_apis': [
                    {
                        'name': 'MetalPriceAPI',
                        'url': 'https://api.metalpriceapi.com/v1/latest?base=USD&currencies=XPD',
                        'parser': self._parse_metalprice
                    }
                ],
                'futures_symbol': 'PALLAD-6.25',
                'futures_alt': ['PALLAD', 'PALLAD-12.24', 'PALLAD-3.25', 'PALLADF']
            }
        ]
        
        # ИСТОЧНИКИ КУРСА
        self.usd_rub_sources = [
            {
                'name': 'ExchangeRate-API',
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
        
        logging.info(f"SpreadMonitor инициализирован")

    # ========== ПАРСЕРЫ КУРСА ==========
    
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

    # ========== ПАРСЕРЫ МЕТАЛЛОВ ==========
    
    async def _parse_metalprice(self, data: Dict) -> Optional[float]:
        """Парсит MetalPriceAPI"""
        try:
            if 'rates' in data:
                for key, value in data['rates'].items():
                    return float(value)
            return None
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
                logging.info(f"Пробую курс из {source['name']}...")
                async with session.get(source['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        if rate and 50 < rate < 150:
                            logging.info(f"✅ Курс: {rate}")
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
            except:
                continue
        
        logging.error("❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ КУРС")
        return None

    async def get_spot_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает спот цену металла"""
        for api in asset['spot_apis']:
            try:
                logging.info(f"Пробую {asset['name']} из {api['name']}...")
                async with session.get(api['url'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = await api['parser'](data)
                        
                        if price:
                            # Проверяем реалистичность
                            if asset['name'] == 'ЗОЛОТО' and 2000 < price < 4000:
                                logging.info(f"✅ {asset['name']}: ${price}")
                                return price
                            elif asset['name'] == 'ПАЛЛАДИЙ' and 700 < price < 1300:
                                logging.info(f"✅ {asset['name']}: ${price}")
                                return price
                            else:
                                logging.warning(f"Странная цена {asset['name']}: ${price}")
                                return None
                    else:
                        logging.warning(f"{api['name']} статус {resp.status}")
            except Exception as e:
                logging.warning(f"Ошибка {api['name']}: {e}")
        
        logging.error(f"❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ {asset['name']}")
        return None

    async def get_futures_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает цену фьючерса с MOEX"""
        
        symbols = [asset['futures_symbol']] + asset.get('futures_alt', [])
        
        for symbol in symbols:
            try:
                url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{symbol}.json"
                logging.info(f"Пробую фьючерс {symbol}...")
                
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        if 'marketdata' in data and 'data' in data['marketdata']:
                            rows = data['marketdata']['data']
                            if rows and len(rows) > 0:
                                for idx in [12, 10, 8, 4]:
                                    if idx < len(rows[0]) and rows[0][idx]:
                                        price = float(rows[0][idx])
                                        # Проверяем реалистичность
                                        if asset['name'] == 'ЗОЛОТО' and 200000 < price < 400000:
                                            logging.info(f"✅ {symbol}: {price} RUB")
                                            return price
                                        elif asset['name'] == 'ПАЛЛАДИЙ' and 50000 < price < 200000:
                                            logging.info(f"✅ {symbol}: {price} RUB")
                                            return price
            except Exception as e:
                logging.warning(f"Ошибка {symbol}: {e}")
        
        logging.error(f"❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ ФЬЮЧЕРС {asset['name']}")
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
                logging.error("❌ НЕТ КУРСА - ПРОПУСКАЕМ ЦИКЛ")
                return
            
            logging.info(f"💱 Курс USD/RUB: {usd_rub}")
            
            for asset in self.assets:
                try:
                    # Спот
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        continue
                    
                    # Фьючерс
                    futures_rub = await self.get_futures_price(session, asset)
                    if not futures_rub:
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
                    
                    logging.info(f"📊 {asset['name']}: спред {spread:.2f}%")
                    
                    # Проверяем порог
                    if abs(spread) > self.threshold:
                        await self.notify_users(users, self.current_spreads[asset['name']])
                        
                except Exception as e:
                    logging.error(f"Ошибка обработки {asset['name']}: {e}")

    async def notify_users(self, users: List[int], data: Dict):
        """Отправляет уведомление"""
        emoji = "🟢" if data['spread'] > 0 else "🔴"
        
        msg = (
            f"{emoji} <b>{data['name']}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Спред: <b>{data['spread']:.2f}%</b>\n"
            f"💰 Спот: ${data['spot_usd']:,.2f}\n"
            f"💎 Фьюч: {data['futures_rub']:,.2f} RUB\n"
            f"💱 Курс: {data['usd_rub']}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        
        for user_id in users:
            try:
                await self.bot.send_message(user_id, msg, parse_mode="HTML")
                logging.info(f"✅ Уведомление отправлено {user_id}")
            except:
                continue

    async def get_current_spreads(self) -> str:
        """Текущие спреды"""
        if not self.current_spreads:
            return "📊 Данные загружаются..."
        
        lines = ["📊 <b>Текущие спреды:</b>\n"]
        for name, data in self.current_spreads.items():
            lines.append(f"{name}: <b>{data['spread']:.2f}%</b>")
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
                logging.error(f"Ошибка в цикле: {e}")
            await asyncio.sleep(self.check_interval)