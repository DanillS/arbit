import aiohttp
import asyncio
import logging
from datetime import datetime
from typing import Dict, List
import ssl
import certifi

class SpreadMonitor:
    def __init__(self, bot, database):
        self.bot = bot
        self.db = database
        self.threshold = 1.5  # Порог в процентах
        self.check_interval = 60  # Проверка каждые 60 секунд
        
        # Список отслеживаемых пар
        self.pairs = [
            "BTCUSDT",
            "ETHUSDT", 
            "BNBUSDT",
            "SOLUSDT",
            "XRPUSDT",
            "ADAUSDT",
            "DOGEUSDT",
            "DOTUSDT"
        ]
        
        # Хранилище последних значений для избежания спама
        self.last_alerts = {}  # (user_id, pair) -> timestamp
        self.alert_cooldown = 3600  # Не спамить чаще 1 часа по одной паре
        
        # Кэш текущих спредов
        self.current_spreads = {}
        
        logging.info(f"SpreadMonitor инициализирован с {len(self.pairs)} парами")

    def get_available_pairs(self) -> List[str]:
        """Возвращает список доступных пар"""
        return self.pairs

    async def get_prices(self, session: aiohttp.ClientSession, symbol: str) -> Dict:
        """Получает цены спота и фьючерса для одной пары"""
        try:
            # Спот цена
            spot_url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            # Фьючерс цена
            futures_url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
            
            async with session.get(spot_url) as spot_resp:
                spot_data = await spot_resp.json()
            
            async with session.get(futures_url) as fut_resp:
                fut_data = await fut_resp.json()
            
            spot_price = float(spot_data['price'])
            futures_price = float(fut_data['price'])
            
            # Расчет спреда в процентах
            spread = ((futures_price - spot_price) / spot_price) * 100
            
            return {
                'symbol': symbol,
                'spot': spot_price,
                'futures': futures_price,
                'spread': round(spread, 2),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logging.error(f"Ошибка получения данных для {symbol}: {e}")
            return None

    async def check_all_pairs(self):
        """Проверяет все пары и отправляет уведомления"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.get_prices(session, pair) for pair in self.pairs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем успешные результаты
        valid_results = []
        for r in results:
            if isinstance(r, dict) and r is not None:
                valid_results.append(r)
            
            # Обновляем кэш
            for result in valid_results:
                self.current_spreads[result['symbol']] = result
            
            # Проверяем пороги и отправляем уведомления
            await self.check_thresholds(valid_results)

    async def check_thresholds(self, results: List[Dict]):
        """Проверяет превышение порога и отправляет уведомления"""
        users = self.db.get_all_users()
        
        if not users:
            return
        
        for result in results:
            symbol = result['symbol']
            spread = result['spread']
            
            # Проверяем только спред больше порога (по модулю или только положительный?)
            if abs(spread) > self.threshold:
                await self.notify_users(users, result)

    async def notify_users(self, users: List[int], spread_data: Dict):
        """Отправляет уведомление всем пользователям"""
        symbol = spread_data['symbol']
        spread = spread_data['spread']
        
        # Эмодзи в зависимости от знака спреда
        emoji = "🟢" if spread > 0 else "🔴"
        direction = "Контанго (фьюч дороже)" if spread > 0 else "Бэквордация (спот дороже)"
        
        message = (
            f"{emoji} <b>Сигнал по {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📈 Спред: <b>{spread:.2f}%</b>\n"
            f"📊 Направление: {direction}\n"
            f"💰 Спот: ${spread_data['spot']:,.2f}\n"
            f"💎 Фьючерс: ${spread_data['futures']:,.2f}\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"#arbitrage #{symbol}"
        )
        
        for user_id in users:
            try:
                # Проверяем кулдаун для этой пары и пользователя
                key = (user_id, symbol)
                now = datetime.now().timestamp()
                
                if key in self.last_alerts:
                    if now - self.last_alerts[key] < self.alert_cooldown:
                        continue  # Пропускаем, если еще не прошло время
                
                await self.bot.send_message(user_id, message, parse_mode="HTML")
                self.last_alerts[key] = now
                
            except Exception as e:
                logging.error(f"Не удалось отправить сообщение пользователю {user_id}: {e}")

    async def get_current_spreads(self) -> str:
        """Возвращает строку с текущими спредами"""
        if not self.current_spreads:
            return "📊 Данные еще не загружены. Попробуйте через минуту."
        
        lines = ["📊 <b>Текущие спреды:</b>\n"]
        
        for symbol, data in self.current_spreads.items():
            spread = data['spread']
            emoji = "✅" if abs(spread) < 2 else "⚠️" if abs(spread) < 3 else "🚨"
            lines.append(f"{emoji} {symbol}: <b>{spread:.2f}%</b>")
        
        lines.append(f"\n🔄 Обновлено: {datetime.now().strftime('%H:%M:%S')}")
        
        return "\n".join(lines)

    async def start_monitoring(self):
        """Запускает бесконечный цикл мониторинга"""
        logging.info("Мониторинг спредов запущен")
        
        while True:
            try:
                await self.check_all_pairs()
            except Exception as e:
                logging.error(f"Ошибка в цикле мониторинга: {e}")
            
            await asyncio.sleep(self.check_interval)