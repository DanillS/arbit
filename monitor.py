import os
import logging
import asyncio
import json
from datetime import datetime
import aiohttp
import ssl
import certifi

logger = logging.getLogger(__name__)

class SpreadMonitor:
    def __init__(self, bot, database):
        self.bot = bot
        self.db = database
        self.threshold = 2.0  # Порог спреда 2%
        self.check_interval = 60  # Проверка каждую минуту
        
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
            if age < 300:  # Кэш на 5 минут
                return self.last_usd_rub
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        
        for source in self.usd_rub_sources:
            try:
                async with session.get(source['url'], headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = await source['parser'](data)
                        if rate and 50 < rate < 150:
                            self.last_usd_rub = rate
                            self.last_usd_rub_time = datetime.now()
                            logger.info(f"Курс USD/RUB: {rate}")
                            return rate
            except Exception as e:
                logger.error(f"Ошибка получения курса из {source['name']}: {e}")
                continue
        return None

    async def get_spot_price(self, session, asset):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        try:
            async with session.get(asset['spot_url'], headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = await self._parse_fxempire(data)
                    if price:
                        # Проверка на разумные цены
                        if 'XAU' in asset['symbol'] and 2000 < price < 4000:
                            logger.info(f"Спот {asset['name']}: ${price}")
                            return price
                        elif 'XPD' in asset['symbol'] and 700 < price < 1300:
                            logger.info(f"Спот {asset['name']}: ${price}")
                            return price
        except Exception as e:
            logger.error(f"Ошибка получения спота {asset['name']}: {e}")
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
                                # Пробуем разные индексы для цены
                                for idx in [12, 10, 8]:
                                    if idx < len(rows[0]) and rows[0][idx]:
                                        price = float(rows[0][idx])
                                        # Проверка на разумные цены в рублях
                                        if 'GOLD' in asset['symbol'] and 200000 < price < 400000:
                                            logger.info(f"Фьючерс {asset['name']}: {price} RUB")
                                            return price
                                        elif 'PALLAD' in asset['symbol'] and 50000 < price < 200000:
                                            logger.info(f"Фьючерс {asset['name']}: {price} RUB")
                                            return price
            except Exception as e:
                logger.error(f"Ошибка получения фьючерса {symbol}: {e}")
                continue
        return None

    async def check_prices(self):
        """Проверяет цены и отправляет уведомления"""
        users = self.db.get_all_users()
        if not users:
            logger.info("Нет подписанных пользователей")
            return
        
        logger.info("Проверка цен...")
        
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            usd_rub = await self.get_usd_rub(session)
            if not usd_rub:
                logger.error("Не удалось получить курс USD/RUB")
                return
            
            for asset in self.assets:
                try:
                    spot = await self.get_spot_price(session, asset)
                    if not spot:
                        logger.warning(f"Не удалось получить спот для {asset['name']}")
                        continue
                    
                    futures_rub = await self.get_futures_price(session, asset)
                    if not futures_rub:
                        logger.warning(f"Не удалось получить фьючерс для {asset['name']}")
                        continue
                    
                    futures_usd = futures_rub / usd_rub
                    spread = ((futures_usd - spot) / spot) * 100
                    
                    logger.info(f"{asset['name']}: спред {spread:.2f}%")
                    
                    self.current_spreads[asset['name']] = {
                        'name': asset['name'],
                        'spot_usd': round(spot, 2),
                        'futures_rub': round(futures_rub, 2),
                        'usd_rub': round(usd_rub, 2),
                        'spread': round(spread, 2)
                    }
                    
                    # Проверяем порог и отправляем уведомления
                    if abs(spread) > self.threshold:
                        # Проверяем, не отправляли ли уже уведомление за последние 30 минут
                        last_alert = self.last_alerts.get(asset['name'])
                        if not last_alert or (datetime.now() - last_alert).seconds > 1800:
                            await self._notify_users(users, self.current_spreads[asset['name']])
                            self.last_alerts[asset['name']] = datetime.now()
                            
                except Exception as e:
                    logger.error(f"Ошибка при обработке {asset['name']}: {e}")
                    continue

    async def _notify_users(self, users, data):
        """Отправляет уведомления всем пользователям"""
        emoji = "🟢" if data['spread'] > 0 else "🔴"
        msg = (
            f"{emoji} <b>{data['name']}</b>\n"
            f"📈 Спред: {data['spread']:.2f}%\n"
            f"💰 Спот: ${data['spot_usd']}\n"
            f"💎 Фьюч: {data['futures_rub']} RUB\n"
            f"💱 Курс: {data['usd_rub']} RUB/USD"
        )
        
        for user_id in users:
            try:
                await self.bot.send_message(user_id, msg, parse_mode="HTML")
                logger.info(f"Уведомление отправлено пользователю {user_id}")
            except Exception as e:
                logger.error(f"Ошибка отправки пользователю {user_id}: {e}")

    async def get_current_spreads(self):
        """Возвращает текущие спреды для команды /status"""
        if not self.current_spreads:
            return "📊 Нет данных о спредах. Попробуйте позже."
        
        lines = ["📊 <b>Текущие спреды:</b>\n"]
        for name, data in self.current_spreads.items():
            emoji = "🟢" if data['spread'] > 0 else "🔴"
            lines.append(f"{emoji} <b>{name}</b>: {data['spread']:.2f}%")
            lines.append(f"   💰 Спот: ${data['spot_usd']}")
            lines.append(f"   💎 Фьюч: {data['futures_rub']} RUB")
            lines.append(f"   💱 Курс: {data['usd_rub']} RUB/USD")
            lines.append("")
        return "\n".join(lines)

    async def start_monitoring(self):
        """Запускает бесконечный цикл мониторинга"""
        logger.info("🚀 Мониторинг запущен")
        while True:
            try:
                await self.check_prices()
            except Exception as e:
                logger.error(f"Ошибка в цикле мониторинга: {e}")
            await asyncio.sleep(self.check_interval)