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
        self.threshold = 0.1  # Порог в процентах
        self.check_interval = 60  # Проверка каждые 60 секунд
        
        # СПИСОК ОТСЛЕЖИВАЕМЫХ АКТИВОВ (металлы)
        self.assets = [
            {
                'name': 'ЗОЛОТО',
                'symbol': 'GOLD',
                'spot_url': 'https://query1.finance.yahoo.com/v8/finance/chart/GC%3DF',
                'futures_symbol': 'GOLD-6.25'
            },
            {
                'name': 'ПАЛЛАДИЙ',
                'symbol': 'PALLAD',
                'spot_url': 'https://query1.finance.yahoo.com/v8/finance/chart/PA%3DF',
                'futures_symbol': 'PALLAD-6.25'
            }
        ]
        
        # 5 ИСТОЧНИКОВ КУРСА USD/RUB для надежности
        self.usd_rub_sources = [
            {
                'name': 'MOEX',
                'url': 'https://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities/USD000UTSTOM.json',
                'parser': self._parse_moex_usd
            },
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
    
    async def _parse_moex_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с MOEX"""
        try:
            return float(data['marketdata']['data'][0][12])
        except:
            return None

    async def _parse_yahoo_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с Yahoo Finance"""
        try:
            return float(data['chart']['result'][0]['meta']['regularMarketPrice'])
        except:
            return None

    async def _parse_cbr_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с ЦБ РФ"""
        try:
            return float(data['Valute']['USD']['Value'])
        except:
            return None

    async def _parse_currencies_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с CurrencyAPI"""
        try:
            # Там курс RUB за 1 USD
            return float(data['rates']['RUB'])
        except:
            return None

    async def _parse_exchangerate_usd(self, data: Dict) -> Optional[float]:
        """Парсит курс с ExchangeRate-API"""
        try:
            return float(data['rates']['RUB'])
        except:
            return None

    async def get_usd_rub_rate(self, session: aiohttp.ClientSession) -> Optional[float]:
        """Пробует получить курс доллара из 5 источников"""
        
        # Проверяем, не обновляли ли курс в последние 5 минут
        if self.last_usd_rub and self.last_usd_rub_time:
            age = (datetime.now() - self.last_usd_rub_time).seconds
            if age < 300:  # 5 минут
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
            
            await asyncio.sleep(1)  # Пауза между запросами
        
        # Если ни один источник не сработал
        logging.error("❌ НЕ УДАЛОСЬ ПОЛУЧИТЬ КУРС ДОЛЛАРА НИ ИЗ ОДНОГО ИСТОЧНИКА")
        return None

    # ========== ПОЛУЧЕНИЕ ЦЕН ==========
    
    async def get_yahoo_price(self, session: aiohttp.ClientSession, url: str) -> Optional[float]:
        """Получает цену металла с Yahoo Finance"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data['chart']['result'][0]['meta']['regularMarketPrice'])
                else:
                    logging.error(f"Yahoo вернул статус {resp.status} для {url}")
                    return None
        except Exception as e:
            logging.error(f"Ошибка Yahoo Finance: {e}")
            return None

    async def get_moex_price(self, session: aiohttp.ClientSession, security: str) -> Optional[float]:
        """Получает цену фьючерса с MOEX"""
        try:
            url = f"https://iss.moex.com/iss/engines/futures/markets/forts/boards/forts/securities/{security}.json"
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data['marketdata']['data'][0][12])
                else:
                    logging.error(f"MOEX вернул статус {resp.status} для {security}")
                    return None
        except Exception as e:
            logging.error(f"Ошибка MOEX для {security}: {e}")
            return None

    async def get_prices(self, session: aiohttp.ClientSession, asset: Dict) -> Optional[Dict]:
        """Получает цены и считает спред"""
        try:
            # СНАЧАЛА получаем курс доллара (это важно!)
            usd_rub = await self.get_usd_rub_rate(session)
            if not usd_rub:
                # Если курса нет - возвращаем ошибку
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить курс USD/RUB',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Получаем спот цену с Yahoo (доллары)
            spot_price_usd = await self.get_yahoo_price(session, asset['spot_url'])
            if not spot_price_usd:
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить спот цену',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Получаем фьючерс цену с MOEX (рубли)
            futures_price_rub = await self.get_moex_price(session, asset['futures_symbol'])
            if not futures_price_rub:
                return {
                    'name': asset['name'],
                    'symbol': asset['symbol'],
                    'error': 'Не удалось получить фьючерс цену',
                    'timestamp': datetime.now().isoformat()
                }
            
            # Конвертируем фьючерс в доллары
            futures_price_usd = futures_price_rub / usd_rub
            
            # Считаем спред в процентах
            spread = ((futures_price_usd - spot_price_usd) / spot_price_usd) * 100
            
            return {
                'name': asset['name'],
                'symbol': asset['symbol'],
                'spot_usd': round(spot_price_usd, 2),
                'futures_rub': round(futures_price_rub, 2),
                'futures_usd': round(futures_price_usd, 2),
                'usd_rub': round(usd_rub, 2),
                'spread': round(spread, 2),
                'timestamp': datetime.now().isoformat()
            }
            
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
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.get_prices(session, asset) for asset in self.assets]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем результаты
        valid_results = []
        for r in results:
            if isinstance(r, dict):
                valid_results.append(r)
                # Если есть ошибка - логируем
                if 'error' in r:
                    logging.error(f"Ошибка для {r['name']}: {r['error']}")
                else:
                    self.current_spreads[r['name']] = r
        
        # Проверяем пороги
        await self.check_thresholds(valid_results)

    async def check_thresholds(self, results: List[Dict]):
        """Проверяет превышение порога"""
        users = self.db.get_all_users()
        if not users:
            return
        
        for result in results:
            # Пропускаем результаты с ошибками
            if 'error' in result:
                continue
                
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
            lines.append(f"   Спот: ${data['spot_usd']} | Фьюч: {data['futures_rub']} RUB")
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