"""
Исправленная система рассылок для работы в Docker
Полная замена файла broadcast.py
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import pytz
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import get_all_users, get_completed_users, get_uncompleted_users, log_broadcast

logger = logging.getLogger(__name__)

class BroadcastScheduler:
    def __init__(self, bot: Bot):
        self.bot = bot
        # Указываем время в московском часовом поясе
        self.timezone = pytz.timezone('Europe/Moscow')
        # Дата вебинара: 3 августа 2025, 12:00 МСК
        self.webinar_date = self.timezone.localize(datetime(2025, 8, 3, 12, 0))
        self.running = False
        
        # Флаги отправленных рассылок (для предотвращения дублирования)
        self.sent_broadcasts = set()
        
        logger.info(f"📅 Вебинар запланирован на: {self.webinar_date}")
    
    async def start_scheduler(self):
        """Запуск планировщика рассылок"""
        self.running = True
        logger.info("📡 Планировщик рассылок запущен")
        
        while self.running:
            try:
                await self.check_and_send_broadcasts()
                # Проверяем каждые 5 минут
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"❌ Ошибка в планировщике: {e}")
                # При ошибке ждем 10 минут
                await asyncio.sleep(600)
    
    def stop_scheduler(self):
        """Остановка планировщика"""
        self.running = False
        logger.info("⏹ Планировщик рассылок остановлен")
    
    def get_moscow_time(self) -> datetime:
        """Получить текущее время в Москве"""
        return datetime.now(self.timezone)
    
    async def check_and_send_broadcasts(self):
        """Проверка времени и отправка рассылок"""
        now = self.get_moscow_time()
        
        # Временные точки для рассылок
        broadcast_schedule = {
            # За неделю до вебинара
            'week_before': self.webinar_date - timedelta(days=7),
            # За 3 дня
            'three_days': self.webinar_date - timedelta(days=3),
            # За день
            'one_day': self.webinar_date - timedelta(days=1),
            # В день вебинара
            'three_hours': self.webinar_date - timedelta(hours=3),
            'two_hours': self.webinar_date - timedelta(hours=2),
            'one_hour': self.webinar_date - timedelta(hours=1),
            'fifteen_minutes': self.webinar_date - timedelta(minutes=15),
            'webinar_start': self.webinar_date,
        }
        
        # Проверяем каждое время рассылки
        for broadcast_id, broadcast_time in broadcast_schedule.items():
            # Проверяем, нужно ли отправить рассылку (в пределах 5 минут)
            time_diff = abs((now - broadcast_time).total_seconds())
            
            if time_diff < 300 and broadcast_id not in self.sent_broadcasts:  # 5 минут
                logger.info(f"⏰ Время для рассылки: {broadcast_id}")
                await self.send_broadcast_by_type(broadcast_id)
                self.sent_broadcasts.add(broadcast_id)
    
    async def send_broadcast_by_type(self, broadcast_type: str):
        """Отправка рассылки по типу"""
        broadcast_functions = {
            'week_before': self.send_week_reminder,
            'three_days': self.send_three_days_reminder,
            'one_day': self.send_day_reminder,
            'three_hours': self.send_three_hours_reminder,
            'two_hours': self.send_two_hours_reminder,
            'one_hour': self.send_hour_reminder,
            'fifteen_minutes': self.send_fifteen_minutes_reminder,
            'webinar_start': self.send_start_reminder,
        }
        
        function = broadcast_functions.get(broadcast_type)
        if function:
            await function()
        else:
            logger.warning(f"⚠️ Неизвестный тип рассылки: {broadcast_type}")
    
    def get_diagnostic_keyboard(self):
        """Клавиатура для прохождения диагностики"""
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Пройти диагностику", callback_data="start_diagnostic")],
            [InlineKeyboardButton(text="✅ Уже пройдено", callback_data="already_completed")]
        ])
    
    async def send_week_reminder(self):
        """Рассылка за неделю до вебинара"""
        text = """📌 Осталась ровно неделя до вебинара «Умный кардиочекап» с Дианой Новиковой и Еленой Удачкиной.

📅 Вебинар «Умный Кардиочекап» пройдёт **3 августа в 12:00 МСК**.

✔️ Получите список приоритетных анализов и обследований, учитывающих ваш возраст, образ жизни, наследственность и симптомы, чтобы достоверно оценить возможные риски ССЗ и грамотно определить необходимые для здоровья шаги и их последовательность.

✔️Узнаете, какие конкретные шаги доказанно помогают сохранить здоровье сердца и сосудов, чтобы не стать жертвой раннего инфаркта или инсульта и быть активными долгие годы.

✔️ Будете знать, как разумно заботиться о здоровье: научитесь выделять из потока информации только важное, будете уверены в своих действиях и перестанете переживать, что упускаете что-то значимое для себя и близких.

Вы получите конкретные шаги и алгоритмы для грамотной профилактической диагностики сердечно-сосудистой системы, чтобы сохранить сердце здоровым, а жизнь долгой и активной — для себя и своих близких

📍 Всё будет здесь, в боте — записи, ссылки, необходимые материалы и бонусы.

Подготовка уже началась! Не забудьте пройти диагностику и опрос, если ещё этого не сделали. Это важно ― так вы сможете извлечь максимум пользы из вебинара и получить бонусы 🎁"""
        
        await self.broadcast_to_users(text, self.get_diagnostic_keyboard(), "week_before")
    
    async def send_three_days_reminder(self):
        """Рассылка за 3 дня до вебинара"""
        text = """🔹 🗓️ До вебинара «Умный Кардиочекап» осталось 3 дня.

Это не просто лекция. Это чёткий пошаговый алгоритм диагностики, выявления рисков и предупреждения инфаркта, инсульта и других ССЗ ― своевременно и с минимальными затратами.

Вебинар пройдёт **3 августа в 12:00 (по Москве)**. Мы пришлём ссылку за 1 день и в день эфира.

📩 Если ещё не прошли диагностику — сейчас самое время.

Ссылка на эфир будет здесь, в боте."""
        
        await self.broadcast_to_users(text, self.get_diagnostic_keyboard(), "three_days")
    
    async def send_day_reminder(self):
        """Рассылка за день до вебинара"""
        text = """🔹 🫀 Уже завтра — вебинар, после которого у вас будет на руках маршрутная карта, чтобы помочь вам сохранить сердце здоровым, а жизнь долгой и полноценной — для себя и своих близких.

📅 **3 августа, 12:00 МСК**

Ждем вас завтра на встрече, приглашайте к экранам своих родных и близких ❤️

**Что важно сделать перед вебинаром?**
✔️ Подготовьте анализы (если есть)
✔️ Пройдите диагностику, если ещё не успели ― так вы сможете применить знания на практике и сразу получить результат, а не просто послушать и забыть

**Чтобы получить максимум пользы от вебинара, подготовьте:**
✔️измерительную ленту
✔️тонометр (если есть)
✔️ручку и блокнот или телефон, чтобы делать заметки
✔️результаты базовых анализов (если сдавали)
✔️ответы тестов из бота
✔️стакан с любимым напитком 😉

⏰ Завтра утром пришлю ссылку. Ничего не пропустите."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Пройти диагностику", callback_data="start_diagnostic")],
            [InlineKeyboardButton(text="✅ Диагностика пройдена", callback_data="already_completed")]
        ])
        
        await self.broadcast_to_users(text, keyboard, "one_day")
    
    async def send_three_hours_reminder(self):
        """Рассылка за 3 часа до вебинара"""
        text = """🔸 📲 Вебинар через 3 часа

Сегодня — день, когда вы получите общую картину состояния сердца и сосудов и системное представление о том, насколько вы защищены от инфаркта и инсульта, чтобы выстроить эффективную стратегию действий для сохранения молодости сердца и сосудов.

🕛 **Вебинар начнётся в 12:00 по МСК.**

За 2,5 часа научимся рассчитывать риски, поговорим о разборе анализов и выстроим готовый маршрут — научимся оценивать риски сердечно-сосудистых заболеваний и преждевременных инфарктов и инсультов, разберём ключевые анализы и выстроим пошаговый маршрут к сохранению сердца здоровым.

**Чтобы извлечь максимум пользы из вебинара, подготовьте:**
✔️измерительную ленту
✔️тонометр (если есть)
✔️ручку и блокнот или телефон, чтобы делать заметки
✔️результаты базовых анализов (если сдавали)
✔️ответы тестов из бота
✔️стакан с любимым напитком 😉"""
        
        await self.broadcast_to_users(text, target_audience="all", broadcast_type="three_hours")
    
    async def send_two_hours_reminder(self):
        """Рассылка за 2 часа до вебинара"""
        text = """🔸 📲 2 часа до вебинара «Умный кардиочекап»

✅ **Готовый маршрут диагностики:** получите чёткий список критически важных обследований и анализов, нужных именно вам

✅ **Пошаговый алгоритм действий:** сдав минимум анализов, оцените свои риски (явные и скрытые) и получите от врачей маршрутную карту действий, которые приведут к результату

✅ **Как не потратить лишнего:** узнаете, как стабилизировать состояние и вовремя остановить прогрессирование заболеваний без ненужных обследований, бесполезных препаратов и бесконечных походов по врачам

**Не пропустите ‼️**"""
        
        await self.broadcast_to_users(text, target_audience="all", broadcast_type="two_hours")
    
    async def send_hour_reminder(self):
        """Рассылка за час до вебинара"""
        text = """🔸 **Ссылка на вебинар «Умный кардиочекап»**

🕛 **Начало — через час, в 12:00 МСК**

🔗 **Ссылка на эфир:** https://your-webinar-link.com"""
        
        await self.broadcast_to_users(text, target_audience="all", broadcast_type="one_hour")
    
    async def send_fifteen_minutes_reminder(self):
        """Рассылка за 15 минут до вебинара"""
        text = """🔸 **Через 15 минут — старт 🚀**

Вебинар «Умный Кардиочекап» начинается в ровно в **12:00 МСК**

🔗 **Присоединиться:** https://your-webinar-link.com"""
        
        await self.broadcast_to_users(text, target_audience="all", broadcast_type="fifteen_minutes")
    
    async def send_start_reminder(self):
        """Рассылка в момент начала вебинара"""
        text = """🔸 **Мы начали!**

Вебинар в прямом эфире. Подключайтесь сейчас — идёт обсуждение ключевых тем:

🔗 **Ссылка на эфир:** https://your-webinar-link.com

**Сегодня вы:**
✔️ Рассчитаете риски сердечно-сосудистых заболеваний и вероятность преждевременных инфарктов и инсультов
✔️ Поймёте, какие анализы и когда сдавать
✔️ Получите важную информацию для выстраивания пошаговой стратегии сохранения здоровья сердца"""
        
        await self.broadcast_to_users(text, target_audience="all", broadcast_type="webinar_start")
    
    async def broadcast_to_users(self, text: str, keyboard: Optional[InlineKeyboardMarkup] = None, 
                                target_audience: str = "all", broadcast_type: str = ""):
        """Отправка сообщения пользователям"""
        try:
            # Получаем список пользователей
            if target_audience == "completed":
                users = await get_completed_users()
            elif target_audience == "uncompleted":
                users = await get_uncompleted_users()
            else:
                users = await get_all_users()
            
            total_users = len(users)
            sent_count = 0
            error_count = 0
            
            logger.info(f"📤 Начинаю рассылку для {total_users} пользователей (тип: {broadcast_type})")
            
            for user in users:
                try:
                    if keyboard:
                        await self.bot.send_message(
                            user.telegram_id, 
                            text, 
                            parse_mode="Markdown",
                            reply_markup=keyboard
                        )
                    else:
                        await self.bot.send_message(
                            user.telegram_id, 
                            text, 
                            parse_mode="Markdown"
                        )
                    sent_count += 1
                    
                    # Задержка между отправками для избежания лимитов
                    await asyncio.sleep(0.05)  # 50ms между сообщениями
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Ошибка отправки пользователю {user.telegram_id}: {e}")
            
            # Логируем результат рассылки
            await log_broadcast(
                broadcast_type=broadcast_type,
                message_text=text,
                target_audience=target_audience,
                total_users=total_users,
                sent_count=sent_count,
                error_count=error_count
            )
            
            logger.info(f"✅ Рассылка завершена. Отправлено: {sent_count}/{total_users}, ошибок: {error_count}")
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при рассылке: {e}")

# Дополнительные функции для административной рассылки

async def send_custom_broadcast(bot: Bot, message_text: str, user_filter: str = "all"):
    """Отправка произвольной рассылки через админку"""
    try:
        if user_filter == "completed":
            users = await get_completed_users()
        elif user_filter == "uncompleted":
            users = await get_uncompleted_users()
        else:
            users = await get_all_users()
        
        total_users = len(users)
        sent_count = 0
        error_count = 0
        
        logger.info(f"📤 Отправка кастомной рассылки для {total_users} пользователей")
        
        for user in users:
            try:
                await bot.send_message(
                    user.telegram_id,
                    message_text,
                    parse_mode="Markdown"
                )
                sent_count += 1
                await asyncio.sleep(0.05)
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Ошибка отправки пользователю {user.telegram_id}: {e}")
        
        # Логируем результат
        await log_broadcast(
            broadcast_type="custom_admin",
            message_text=message_text,
            target_audience=user_filter,
            total_users=total_users,
            sent_count=sent_count,
            error_count=error_count
        )
        
        return {"sent": sent_count, "errors": error_count, "total": total_users}
        
    except Exception as e:
        logger.error(f"❌ Ошибка при произвольной рассылке: {e}")
        return {"sent": 0, "errors": 1, "total": 0}

async def test_broadcast_system():
    """Тестирование системы рассылок"""
    logger.info("🧪 Тестирование системы рассылок...")
    
    # Проверяем наличие пользователей
    try:
        all_users = await get_all_users()
        completed_users = await get_completed_users()
        uncompleted_users = await get_uncompleted_users()
        
        logger.info(f"📊 Статистика пользователей:")
        logger.info(f"   Всего: {len(all_users)}")
        logger.info(f"   Завершили диагностику: {len(completed_users)}")
        logger.info(f"   Не завершили: {len(uncompleted_users)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        return False