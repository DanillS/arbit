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
        self.threshold = 2.0  # Порог в процентах
        self.check_interval = 60  # Проверка каждые 60 секунд
        
        # СПИСОК ОТСЛЕЖИВАЕМЫХ АКТИВОВ (металлы) с правильными тикерами
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'GOLD',
                'spot_url': 'https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF',
                'futures_symbol': 'GOLD-6.25',
                'futures_alt_symbols': ['GOLD', 'GOLDF', 'GOLD-12.24', 'GOLD-3.25', 'GOLD-6.25', 'GOLD-9.25']
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'PALLAD',
                'spot_url': 'https://query1.finance.yahoo.com/v8/finance/chart/PA%3DF',
                'futures_symbol': 'PALLAD-6.25',
                'futures_alt_symbols': ['PALLAD', 'PALLADF', 'PALLAD-12.24', 'PALLAD-3.25', 'PALLAD-6.25', 'PALLAD-9.25']
            }
        ]
        
        # 5 ИСТОЧНИКОВ КУРСА USD/RUB для надежности
        self.usd_rub_sources = [
            {
                'name': 'Yahoo Finance',
                'url': 'https://query1.finance.yahoo.com/v8/finance/chart/USDRUB=X',
                'parser': self._parse_yahoo_usd
            },
            {
                'name': 'CBR (ЦБ РФ)',
                'url': 'https://www.cbr-xml-daily.ru/daily_json.js',
                'parser': self._parse_cbr_usd
            },
            {
                'name': 'CurrencyAPI',
                'url': 'https://cdn.cur.su/api/latest.json',
                'parser': self._parse_currencies_usd
            },
            {
                'name': 'ExchangeRate-API',
                'url': 'https://api.exchangerate-api.com/v4/latest/USD',
                'parser': self._parse_exchangerate_usd
            },
            {
                'name': 'MOEX',
                'url': 'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM.json',
                'parser': self._parse_moex_usd
            }
        ]
        
        # Хранилище последних значений
        self.last_alerts = {}
        self.alert_cooldown = 3600
        self.current_spreads = {}
        self.last_usd_rub = None
        self.last_usd_rub_time = None
        
        logging.info(f"SpreadMonitor инициализирован с {len(self.assets)} активами")

    def get_available_pairs(self) -> List[str]:
        return [asset['name'] for asset in self.assets]

    # ========== ПАРСЕРЫ КУРСА ДОЛЛАРА ==========
    
    async def _parse_yahoo_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с Yahoo Finance через quote"""
        try:
            # Пробуем новый способ через quote
            if 'chart' in data and 'result' in data['chart']:
                result = data['chart']['result']
                if result and len(result) > 0:
                    # Пробуем через indicators
                    if 'indicators' in result[0] and 'quote' in result[0]['indicators']:
                        quote = result[0]['indicators']['quote']
                        if quote and len(quote) > 0:
                            if 'close' in quote[0] and quote[0]['close']:
                                # Берем последнее закрытие
                                prices = quote[0]['close']
                                if prices and len(prices) > 0:
                                    rate = float(prices[-1])
                                    if 50 < rate < 150:
                                        return rate
                    
                    # Запасной вариант через meta
                    if 'meta' in result[0] and 'regularMarketPrice' in result[0]['meta']:
                        rate = float(result[0]['meta']['regularMarketPrice'])
                        if 50 < rate < 150:
                            return rate
            return None
        except Exception as e:
            logging.debug(f"Ошибка парсинга Yahoo USD: {e}")
            return None

    async def _parse_cbr_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с ЦБ РФ"""
        try:
            if 'Valute' in data and 'USD' in data['Valute']:
                rate = float(data['Valute']['USD']['Value'])
                if 50 < rate < 150:
                    return rate
            return None
        except Exception as e:
            logging.debug(f"Ошибка парсинга ЦБ: {e}")
            return None

    async def _parse_currencies_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с CurrencyAPI"""
        try:
            if 'rates' in data and 'RUB' in data['rates']:
                rate = float(data['rates']['RUB'])
                if 50 < rate < 150:
                    return rate
            return None
        except Exception as e:
            logging.debug(f"Ошибка парсинга CurrencyAPI: {e}")
            return None

    async def _parse_exchangerate_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с ExchangeRate-API"""
        try:
            if 'rates' in data and 'RUB' in data['rates']:
                rate = float(data['rates']['RUB'])
                if 50 < rate < 150:
                    return rate
            return None
        except Exception as e:
            logging.debug(f"Ошибка парсинга ExchangeRate: {e}")
            return None

    async def _parse_moex_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс доллара с MOEX"""
        try:
            if 'marketdata' not in data or 'data' not in data['marketdata']:
                return None
                
            data_rows = data['marketdata']['data']
            if not data_rows or len(data_rows) == 0:
                return None
            
            for row in data_rows:
                if len(row) > 20:
                    for idx in [12, 10, 8, 4]:
                        if idx < len(row) and row[idx] is not None:
                            try:
                                price = float(row[idx])
                                if 50 < price < 150:
                                    return price
                            except:
                                continue
            return None
        except Exception as e:
            logging.error(f"Ошибка парсинга MOEX: {e}")
            return None

    async def get_usd_rub_rate(self, session: aiohttp.ClientSession) -> Optional[float]:
        """Пробует получить курс доллара из 5 источников"""
        
        if self.last_usd_rub and self.last_usd_rub_time:
            age = (datetime.now() - self.last_usd_rub_time).seconds
            if age < 300:
                logging.info(f"Использую кэшированный курс: {self.last_usd_rub}")
                return self.last_usd_rub
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        for source in self.usd_rub_sources:
            try:
                logging.info(f"Пробую получить курс из {source['name']}...")
                
                async with session.get(source['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        
                        if rate and rate > 0:
                            logging.info(f"✅ Курс из {source['name']}: {rate}")
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            return rate
                    else:
                        logging.warning(f"{source['name']} вернул статус {resp.status}")
                        
            except asyncio.TimeoutError:
                logging.warning(f"Таймаут при запросе к {source['name']}")
            except Exception as e:
                logging.warning(f"Ошибка при получении курса из {source['name']}: {e}")
            
            await asyncio.sleep(1)
        
        logging.error("❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ КУРС ДОЛЛАРА")
        return None

    # ========== ПОЛУЧЕНИЕ ЦЕН МЕТАЛЛОВ ==========
    
    async def get_yahoo_price(self, session: aiohttp.ClientSession, url: str, metal_name: str) -> Optional[float]:
        """Получает цену металла с Yahoo Finance через quote"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    # Парсим ответ Yahoo через indicators
                    if 'chart' in data and 'result' in data['chart']:
                        result = data['chart']['result']
                        if result and len(result) > 0:
                            # Основной способ через quote
                            if 'indicators' in result[0] and 'quote' in result[0]['indicators']:
                                quote = result[0]['indicators']['quote']
                                if quote and len(quote) > 0:
                                    if 'close' in quote[0] and quote[0]['close']:
                                        prices = quote[0]['close']
                                        if prices and len(prices) > 0:
                                            price = float(prices[-1])
                                            # Проверка реалистичности
                                            if 'GOLD' in url and price > 1000:
                                                logging.info(f"✅ {metal_name} спот (через quote): ${price}")
                                                return price
                                            if 'PALLAD' in url and price > 500:
                                                logging.info(f"✅ {metal_name} спот (через quote): ${price}")
                                                return price
                            
                            # Запасной способ через meta
                            if 'meta' in result[0] and 'regularMarketPrice' in result[0]['meta']:
                                price = float(result[0]['meta']['regularMarketPrice'])
                                if 'GOLD' in url and price > 1000:
                                    logging.info(f"✅ {metal_name} спот (через meta): ${price}")
                                    return price
                                if 'PALLAD' in url and price > 500:
                                    logging.info(f"✅ {metal_name} спот (через meta): ${price}")
                                    return price
                    
                    logging.error(f"❌ Не удалось найти цену {metal_name} в ответе Yahoo")
                    return None
                else:
                    logging.error(f"Yahoo вернул статус {resp.status} для {metal_name}")
                    return None
        except Exception as e:
            logging.error(f"Ошибка Yahoo Finance для {metal_name}: {e}")
            return None

    async def get_moex_futures_price(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[float]:
        """Получает цену фьючерса с MOEX, пробуя разные тикеры"""
        
        symbols_to_try = [asset['futures_symbol']] + asset.get('futures_alt_symbols', [])
        symbols_to_try = list(dict.fromkeys(symbols_to_try))  # Убираем дубликаты
        
        for symbol in symbols_to_try:
            try:
                url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{symbol}.json"
                logging.info(f"Пробую тикер MOEX для {asset['name']}: {symbol}")
                
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        
                        if 'marketdata' in data and 'data' in data['marketdata']:
                            marketdata = data['marketdata']['data']
                            if marketdata and len(marketdata) > 0:
                                first_row = marketdata[0]
                                
                                for idx in [12, 10, 8, 4]:
                                    if idx < len(first_row) and first_row[idx] is not None:
                                        try:
                                            price = float(first_row[idx])
                                            
                                            if 'GOLD' in asset['symbol'] and 100000 < price < 500000:
                                                logging.info(f"✅ {asset['name']} фьючерс по {symbol}: {price} RUB")
                                                return price
                                            elif 'PALLAD' in asset['symbol'] and 50000 < price < 300000:
                                                logging.info(f"✅ {asset['name']} фьючерс по {symbol}: {price} RUB")
                                                return price
                                        except:
                                            continue
                            else:
                                logging.warning(f"Тикер {symbol} не найден или пуст")
                    else:
                        logging.warning(f"MOEX вернул {resp.status} для {symbol}")
                        
            except Exception as e:
                logging.warning(f"Ошибка при запросе {symbol}: {e}")
            
            await asyncio.sleep(0.5)
        
        logging.error(f"❌ Не удалось найти фьючерс для {asset['name']}")
        return None

    async def get_prices(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[Dict]:
        """Получает цены и считает спред"""
        try:
            # 1. Получаем курс доллара
            usd_rub = await self.get_usd_rub_rate(session)
            if not usd_rub:
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить курс USD/RUB',
                    'timestamp': datetime.now().isoformat()
                }
            
            # 2. Получаем спот цену с Yahoo
            spot_price_usd = await self.get_yahoo_price(session, asset['spot_url'], asset['name'])
            if not spot_price_usd:
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить спот цену',
                    'timestamp': datetime.now().isoformat()
                }
            
            # 3. Получаем фьючерс цену с MOEX
            futures_price_rub = await self.get_moex_futures_price(session, asset)
            if not futures_price_rub:
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить фьючерс цену',
                    'timestamp': datetime.now().isoformat()
                }
            
            # 4. Конвертируем фьючерс в доллары
            futures_price_usd = futures_price_rub / usd_rub
            
            # 5. Считаем спред
            spread = ((futures_price_usd - spot_price_usd) / spot_price_usd) * 100
            
            result = {
                'name': asset['name'],
                'symbol': asset['symbol'],
                'spot_usd': round(spot_price_usd, 2),
                'futures_rub': round(futures_price_rub, 2),
                'futures_usd': round(futures_price_usd, 2),
                'usd_rub': round(usd_rub, 2),
                'spread': round(spread, 2),
                'timestamp': datetime.now().isoformat()
            }
            
            logging.info(f"✅ Успешно получены данные для {asset['name']}: спред {spread:.2f}%")
            return result
            
        except Exception as e:
            logging.error(f"Ошибка получения данных для {asset['name']}: {e}")
            return {
                'name': asset['name'],
                'symbol': asset['symbol'],
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    async def check_all_pairs(self):
        """Проверяет все активы"""
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)

            async with aiohttp.ClientSession(connector=connector) as session:
                tasks = [self.get_prices(session, asset) for asset in self.assets]
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            valid_results = []
            error_results = []
            
            for r in results:
                if isinstance(r, dict):
                    if 'error' in r:
                        error_results.append(r)
                        logging.error(f"Ошибка для {r['name']}: {r['error']}")
                    else:
                        valid_results.append(r)
                        self.current_spreads[r['name']] = r
            
            if valid_results:
                await self.check_thresholds(valid_results)
                logging.info(f"✅ Успешно получены данные для {len(valid_results)} активов")
            
            if error_results and len(error_results) == len(self.assets):
                logging.error("❌ ВСЕ АКТИВЫ С ОШИБКАМИ!")
                
        except Exception as e:
            logging.error(f"Критическая ошибка в check_all_pairs: {e}")

    async def check_thresholds(self, results: List[Dict]):
        """Проверяет превышение порога"""
        users = self.db.get_all_users()
        if not users:
            return
        
        for result in results:
            spread = result.get('spread')
            if spread and abs(spread) > self.threshold:
                await self.notify_users(users, result)

    async def notify_users(self, users: List[int], spread_data: Dict):
        """Отправляет уведомление"""
        name = spread_data['name']
        spread = spread_data['spread']
        
        emoji = "🟢" if spread > 0 else "🔴"
        direction = "Фьючерс дороже спота" if spread > 0 else "Спот дороже фьючерса"
        
        message = (
            f"{emoji} <b>Сигнал по {name}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Спред: <b>{spread:.2f}%</b>\n"
            f"📊 {direction}\n"
            f"💰 Спот: ${spread_data['spot_usd']:,.2f}\n"
            f"💎 Фьючерс: {spread_data['futures_rub']:,.2f} RUB (${spread_data['futures_usd']:,.2f})\n"
            f"💱 Курс USD/RUB: {spread_data['usd_rub']}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"#metals #{spread_data['symbol']}"
        )
        
        for user_id in users:
            try:
                key = (user_id, name)
                now = datetime.now().timestamp()
                
                if key in self.last_alerts:
                    if now - self.last_alerts[key] < self.alert_cooldown:
                        continue
                
                await self.bot.send_message(user_id, message, parse_mode="HTML")
                self.last_alerts[key] = now
                logging.info(f"Уведомление отправлено {user_id} по {name} ({spread:.2f}%)")
                
            except Exception as e:
                logging.error(f"Ошибка отправки {user_id}: {e}")

    async def get_current_spreads(self) -> str:
        """Возвращает строку с текущими спредами"""
        if not self.current_spreads:
            return "📊 Данные еще не загружены. Попробуйте через минуту."
        
        lines = ["📊 <b>Текущие спреды (металлы):</b>\n"]
        
        for name, data in self.current_spreads.items():
            if 'error' in data:
                lines.append(f"❌ {name}: {data['error']}")
                continue
                
            spread = data['spread']
            emoji = "✅" if abs(spread) < 2 else "⚠️" if abs(spread) < 3 else "🚨"
            lines.append(f"{emoji} {name}: <b>{spread:.2f}%</b>")
            lines.append(f"   Спот: ${data['spot_usd']:,.2f}")
            lines.append(f"   Фьючерс: {data['futures_rub']:,.2f} RUB (${data['futures_usd']:,.2f})")
            lines.append(f"   Курс USD/RUB: {data['usd_rub']}")
            lines.append("")
        
        lines.append(f"🔄 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
        
        return "\n".join(lines)

    async def start_monitoring(self):
        """Запускает бесконечный цикл мониторинга"""
        logging.info("Мониторинг металлов запущен")
        
        while True:
            try:
                await self.check_all_pairs()
            except Exception as e:
                logging.error(f"Ошибка в цикле мониторинга: {e}")
            
            await asyncio.sleep(self.check_interval)