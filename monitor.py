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
                        'name': 'GoldAPI',
                        'url': 'https://api.gold-api.com/price/XAU',
                        'parser': self._parse_goldapi
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
                    }
                ],
                'futures_symbol': 'PALLAD-6.25'
            }
        ]
        
        # ИСТОЧНИКИ КУРСА USD/RUB (ПРОВЕРЕННЫЕ И РАБОЧИЕ)
        self.usd_rub_sources = [
            {
                'name': 'CBR (ЦБ РФ)',
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js',
                'parser': self._parse_cbr
            },
            {
                'name': 'ExchangeRate-API',
                'url': 'https://api.exchangerate-api.com/v4/latest/USD',
                'parser': self._parse_exchangerate
            },
            {
                'name': 'CurrencyAPI',
                'url': 'https://cdn.cur.su/api/latest.json',
                'parser': self._parse_currency
            },
            {
                'name': 'Yahoo Finance',
                'url': 'https://query1.finance.yahoo.com/v8/finance/chart/USDRUB=X',
                'parser': self._parse_yahoo
            }
        ]
        
        self.last_alerts = {}
        self.alert_cooldown = 3600
        self.current_spreads = {}
        self.last_usd_rub = None
        self.last_usd_rub_time = None
        
        logging.info(f"SpreadMonitor инициализирован")

    # ========== ПАРСЕРЫ КУРСА ==========
    
    async def _parse_cbr(self, data: Dict) -> Optional[float]:
        """Парсит курс ЦБ РФ"""
        try:
            return float(data['Valute']['USD']['Value'])
        except Exception as e:
            logging.debug(f"CBR parse error: {e}")
            return None

    async def _parse_exchangerate(self, data: Dict) -> Optional[float]:
        """Парсит ExchangeRate-API"""
        try:
            return float(data['rates']['RUB'])
        except Exception as e:
            logging.debug(f"ExchangeRate parse error: {e}")
            return None

    async def _parse_currency(self, data: Dict) -> Optional[float]:
        """Парсит CurrencyAPI"""
        try:
            return float(data['rates']['RUB'])
        except Exception as e:
            logging.debug(f"CurrencyAPI parse error: {e}")
            return None

    async def _parse_yahoo(self, data: Dict) -> Optional[float]:
        """Парсит Yahoo Finance"""
        try:
            return float(data['chart']['result'][0]['meta']['regularMarketPrice'])
        except Exception as e:
            logging.debug(f"Yahoo parse error: {e}")
            return None

    # ========== ПАРСЕРЫ МЕТАЛЛОВ ==========
    
    async def _parse_goldapi(self, data: Dict) -> Optional[float]:
        """Парсит GoldAPI"""
        try:
            return float(data['price'])
        except Exception as e:
            logging.debug(f"GoldAPI parse error: {e}")
            return None

    async def _parse_metalprice(self, data: Dict) -> Optional[float]:
        """Парсит MetalPriceAPI"""
        try:
            if 'rates' in data:
                for key, value in data['rates'].items():
                    return float(value)
            return None
        except Exception as e:
            logging.debug(f"MetalPriceAPI parse error: {e}")
            return None

    async def get_usd_rub(self, session: aiohttp.ClientSession) -> Optional[float]:
        """Получает курс USD/RUB из нескольких источников"""
        
        # Проверяем кэш
        if self.last_usd_rub and self.last_usd_rub_time:
            age = (datetime.now() - self.last_usd_rub_time).seconds
            if age < 300:  # 5 минут
                logging.info(f"Использую кэшированный курс: {self.last_usd_rub}")
                return self.last_usd_rub
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        # Пробуем каждый источник
        for source in self.usd_rub_sources:
            try:
                logging.info(f"Пробую получить курс из {source['name']}...")
                
                async with session.get(source['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        
                        if rate and 50 < rate < 150:  # Проверка на реалистичность
                            logging.info(f"✅ Курс из {source['name']}: {rate}")
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
                        else:
                            logging.warning(f"Курс из {source['name']} нереалистичный: {rate}")
                    else:
                        logging.warning(f"{source['name']} вернул статус {resp.status}")
                        
            except asyncio.TimeoutError:
                logging.warning(f"Таймаут при запросе к {source['name']}")
            except Exception as e:
                logging.warning(f"Ошибка при получении курса из {source['name']}: {e}")
            
            await asyncio.sleep(1)  # Пауза между запросами
        
        # Если ничего не сработало - используем запасной вариант
        logging.error("❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ КУРС ИЗ API, ИСПОЛЬЗУЮ ЗАПАСНОЙ")
        
        # Запасной вариант - парсинг HTML ЦБ РФ (на всякий случай)
        try:
            async with session.get('https://www.cbr.ru/scripts/XML_daily.asp', timeout=10) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    # Простой парсинг - ищем USD
                    import re
                    match = re.search(r'USD.*?<Value>([0-9,]+)</Value>', html)
                    if match:
                        rate = float(match.group(1).replace(',', '.'))
                        if 50 < rate < 150:
                            logging.info(f"✅ Запасной курс из HTML ЦБ: {rate}")
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
        except Exception as e:
            logging.error(f"Ошибка запасного парсинга: {e}")
        
        return None

    async def get_spot_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает спот цену металла"""
        for api in asset['spot_apis']:
            try:
                logging.info(f"Пробую получить {asset['name']} из {api['name']}...")
                
                async with session.get(api['url'], timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = await api['parser'](data)
                        
                        if price:
                            logging.info(f"✅ {asset['name']} из {api['name']}: ${price}")
                            return price
                    else:
                        logging.warning(f"{api['name']} вернул статус {resp.status}")
                        
            except Exception as e:
                logging.warning(f"Ошибка {api['name']}: {e}")
            
            await asyncio.sleep(1)
        
        logging.error(f"❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ {asset['name']}")
        return None

    async def get_futures_price(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """Получает цену фьючерса с MOEX"""
        try:
            url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{symbol}.json"
            logging.info(f"Пробую получить фьючерс {symbol}...")
            
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if 'marketdata' in data and 'data' in data['marketdata']:
                        rows = data['marketdata']['data']
                        if rows and len(rows) > 0:
                            # Пробуем разные индексы
                            for idx in [12, 10, 8, 4]:
                                if idx < len(rows[0]) and rows[0][idx]:
                                    price = float(rows[0][idx])
                                    logging.info(f"✅ Фьючерс {symbol}: {price} RUB")
                                    return price
                    
                    logging.warning(f"Нет данных для {symbol}")
                else:
                    logging.warning(f"MOEX вернул {resp.status} для {symbol}")
                    
        except Exception as e:
            logging.warning(f"Ошибка MOEX для {symbol}: {e}")
        
        return None

    async def check_prices(self):
        """Проверяет цены"""
        users = self.db.get_all_users()
        if not users:
            return
        
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # 1. Получаем курс (ОБЯЗАТЕЛЬНО)
            usd_rub = await self.get_usd_rub(session)
            if not usd_rub:
                logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА: НЕТ КУРСА USD/RUB")
                return
            
            logging.info(f"✅ Текущий курс USD/RUB: {usd_rub}")
            
            # 2. Получаем цены металлов
            for asset in self.assets:
                try:
                    # Спот цена
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        continue
                    
                    # Фьючерс цена
                    futures_rub = await self.get_futures_price(session, asset['futures_symbol'])
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
                    
                    logging.info(f"✅ {asset['name']}: спред {spread:.2f}%")
                    
                    # Проверяем порог
                    if abs(spread) > self.threshold:
                        await self.notify_users(users, self.current_spreads[asset['name']])
                        
                except Exception as e:
                    logging.error(f"Ошибка обработки {asset['name']}: {e}")

    async def notify_users(self, users: List[int], data: Dict):
        """Отправляет уведомление"""
        emoji = "🟢" if data['spread'] > 0 else "🔴"
        direction = "Фьючерс дороже" if data['spread'] > 0 else "Спот дороже"
        
        msg = (
            f"{emoji} <b>{data['name']}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Спред: <b>{data['spread']:.2f}%</b>\n"
            f"📊 {direction}\n"
            f"💰 Спот: ${data['spot_usd']:,.2f}\n"
            f"💎 Фьючерс: {data['futures_rub']:,.2f} RUB\n"
            f"💱 Курс: {data['usd_rub']}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        
        for user_id in users:
            try:
                await self.bot.send_message(user_id, msg, parse_mode="HTML")
                logging.info(f"Уведомление отправлено {user_id}")
            except Exception as e:
                logging.error(f"Ошибка отправки {user_id}: {e}")

    async def get_current_spreads(self) -> str:
        """Текущие спреды"""
        if not self.current_spreads:
            return "📊 Данные загружаются..."
        
        lines = ["📊 <b>Текущие спреды:</b>\n"]
        for name, data in self.current_spreads.items():
            emoji = "✅" if abs(data['spread']) < 2 else "⚠️"
            lines.append(f"{emoji} {name}: <b>{data['spread']:.2f}%</b>")
            lines.append(f"   Спот: ${data['spot_usd']:,.2f}")
            lines.append(f"   Фьюч: {data['futures_rub']:,.2f} RUB")
            lines.append("")
        
        lines.append(f"🔄 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
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