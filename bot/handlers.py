import asyncio
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton,  BotCommand, BotCommandScopeDefault
from aiogram.filters import CommandStart, StateFilter, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from keyboards import *
from database import *
from surveys import *


# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# MIDDLEWARE ДЛЯ ЗАЩИТЫ ОТ ЗАЦИКЛИВАНИЯ СОСТОЯНИЙ
# ============================================================================

class StateProtectionMiddleware:
    """Middleware для предотвращения дублирования состояний и зацикливания"""
    
    def __init__(self):
        self.processing_users = set()  # Множество пользователей в процессе обработки
        self.user_last_action = {}     # Последние действия пользователей для дедупликации
        self.action_timeouts = {}      # Тайм-ауты для пользователей
    
    async def __call__(self, handler, event, data):
        # Безопасная проверка наличия пользователя
        if not hasattr(event, 'from_user') or not event.from_user:
            return await handler(event, data)
        
        try:
            user_id = event.from_user.id
            current_time = asyncio.get_event_loop().time()
            
            # Получаем идентификатор действия (с защитой от ошибок)
            try:
                action_id = self._get_action_id(event)
            except Exception as e:
                logger.warning(f"Ошибка получения action_id для пользователя {user_id}: {e}")
                action_id = "unknown_action"
            
            # ВАЖНО: Пропускаем административные команды БЕЗ защиты
            if self._is_admin_action(event):
                logger.info(f"Пропускаю административную команду от пользователя {user_id}: {action_id}")
                return await handler(event, data)
            
            # Проверяем дедупликацию (одинаковые действия в течение 2 секунд)
            if user_id in self.user_last_action:
                last_action, last_time = self.user_last_action[user_id]
                if last_action == action_id and (current_time - last_time) < 2.0:
                    # Дублированное действие - игнорируем
                    if hasattr(event, 'answer'):
                        try:
                            await event.answer("⏳ Обрабатываю ваш запрос...", show_alert=False)
                        except:
                            pass
                    return
            
            # Проверяем, не обрабатывается ли уже запрос от этого пользователя
            if user_id in self.processing_users:
                if hasattr(event, 'answer'):
                    try:
                        await event.answer("⏳ Пожалуйста, подождите, обрабатываю ваш предыдущий запрос...", show_alert=True)
                    except:
                        pass
                return
            
            # Проверяем тайм-аут для пользователя (защита от спама)
            if user_id in self.action_timeouts:
                if current_time < self.action_timeouts[user_id]:
                    if hasattr(event, 'answer'):
                        try:
                            await event.answer("🔄 Слишком быстро! Подождите немного.", show_alert=True)
                        except:
                            pass
                    return
            
            # Добавляем пользователя в обработку
            self.processing_users.add(user_id)
            self.user_last_action[user_id] = (action_id, current_time)
            
            # Устанавливаем минимальный тайм-аут между действиями (0.5 секунды)
            self.action_timeouts[user_id] = current_time + 0.5
            
            try:
                # Выполняем обработчик
                return await handler(event, data)
            except Exception as e:
                logger.error(f"Ошибка в обработчике для пользователя {user_id}: {e}")
                # Отправляем пользователю сообщение об ошибке
                try:
                    if hasattr(event, 'message') and hasattr(event.message, 'answer'):
                        await event.message.answer("❌ Произошла ошибка. Попробуйте /start")
                    elif hasattr(event, 'answer'):
                        await event.answer("❌ Произошла ошибка. Попробуйте /start")
                    elif hasattr(event, 'answer_text'):
                        await event.answer_text("❌ Произошла ошибка. Попробуйте /start")
                except Exception as answer_error:
                    logger.error(f"Не удалось отправить сообщение об ошибке пользователю {user_id}: {answer_error}")
            finally:
                # Удаляем пользователя из обработки
                self.processing_users.discard(user_id)
                
                # Очищаем старые записи (старше 10 минут)
                try:
                    cutoff_time = current_time - 600
                    self.user_last_action = {
                        uid: (action, time) for uid, (action, time) in self.user_last_action.items()
                        if time > cutoff_time
                    }
                    self.action_timeouts = {
                        uid: timeout for uid, timeout in self.action_timeouts.items()
                        if timeout > current_time
                    }
                except Exception as cleanup_error:
                    logger.warning(f"Ошибка очистки кэша middleware: {cleanup_error}")
        
        except Exception as middleware_error:
            logger.error(f"Критическая ошибка в StateProtectionMiddleware: {middleware_error}")
            # В случае критической ошибки middleware - продолжаем выполнение
            return await handler(event, data)
        
        return await handler(event, data)
    
    def _is_admin_action(self, event):
        """Проверка, является ли действие административным"""
        
        # Список административных команд и callback'ов
        admin_commands = ['/admin', '/stats', '/export', '/broadcast', '/adminhelp']
        admin_callbacks = ['admin_', 'export_', 'stats_', 'broadcast_', 'clean_']
        
        # Проверяем текстовые команды
        if hasattr(event, 'text') and event.text:
            text = event.text.strip().lower()
            for cmd in admin_commands:
                if text.startswith(cmd):
                    return True
        
        # Проверяем callback'и
        if hasattr(event, 'data') and event.data:
            for callback in admin_callbacks:
                if event.data.startswith(callback):
                    return True
        
        return False
    
    def _get_action_id(self, event):
        """Получить уникальный идентификатор действия"""
        if hasattr(event, 'data'):  # CallbackQuery
            return f"callback:{event.data}"
        elif hasattr(event, 'text') and event.text:  # Message with text
            return f"message:{event.text[:50]}"  # Первые 50 символов
        elif hasattr(event, 'contact') and event.contact:  # Contact
            return "contact:shared"
        elif hasattr(event, 'photo') and event.photo:  # Photo
            return "media:photo"
        elif hasattr(event, 'document') and event.document:  # Document
            return "media:document"
        elif hasattr(event, 'sticker') and event.sticker:  # Sticker
            return "media:sticker"
        elif hasattr(event, 'voice') and event.voice:  # Voice
            return "media:voice"
        elif hasattr(event, 'video') and event.video:  # Video
            return "media:video"
        elif hasattr(event, 'audio') and event.audio:  # Audio
            return "media:audio"
        elif hasattr(event, 'location') and event.location:  # Location
            return "location:shared"
        elif hasattr(event, 'content_type'):  # Any message with content_type
            return f"message:{event.content_type}"
        else:
            return "unknown"

# Создаем экземпляр middleware
state_protection = StateProtectionMiddleware()

# ============================================================================
# СОСТОЯНИЯ FSM
# ============================================================================

class UserStates(StatesGroup):
    waiting_start = State()
    waiting_name = State()
    waiting_email = State()
    waiting_phone = State()
    
    # Опрос состояния
    survey_age = State()
    survey_gender = State()
    survey_location = State()
    survey_education = State()
    survey_family = State()
    survey_children = State()
    survey_income = State()
    survey_health = State()
    survey_death_cause = State()
    survey_heart_disease = State()
    survey_cv_risk = State()
    survey_cv_knowledge = State()
    survey_heart_danger = State()
    survey_health_importance = State()
    survey_checkup_history = State()
    survey_checkup_content = State()
    survey_prevention_barriers = State()
    survey_health_advice = State()
    
    # Тесты состояния
    test_selection = State()
    hads_test = State()
    burns_test = State()
    isi_test = State()
    stop_bang_test = State()
    ess_test = State()
    fagerstrom_test = State()
    audit_test = State()

COMMANDS = [
    BotCommand(command="start", description="🚀 Начать диагностику"),
    BotCommand(command="help", description="❓ Помощь и инструкции"),
    BotCommand(command="status", description="📊 Мой статус прохождения"),
    BotCommand(command="restart", description="🔄 Начать заново"),
]

async def setup_bot_commands(bot):
    """Установка команд бота в меню"""
    await bot.set_my_commands(COMMANDS, BotCommandScopeDefault())

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ЗАЩИТЫ СОСТОЯНИЙ
# ============================================================================

async def safe_edit_message(message, text, parse_mode="HTML", reply_markup=None, max_retries=3):
    """Безопасное редактирование сообщения с повторными попытками"""
    for attempt in range(max_retries):
        try:
            await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
            return True
        except Exception as e:
            if "message is not modified" in str(e):
                return True  # Сообщение уже такое
            if attempt == max_retries - 1:
                # Последняя попытка - отправляем новое сообщение
                try:
                    await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
                    return True
                except:
                    return False
            await asyncio.sleep(0.5)
    return False

async def safe_answer_callback(callback, text="", show_alert=False, max_retries=2):
    """Безопасный ответ на callback"""
    for attempt in range(max_retries):
        try:
            await callback.answer(text, show_alert=show_alert)
            return True
        except Exception as e:
            if "query is too old" in str(e) or "QUERY_ID_INVALID" in str(e):
                return True  # Игнорируем устаревшие callback'и
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(0.3)
    return False

async def log_user_interaction(user_id: int, action: str, details: str = None):
    """Логирование взаимодействий пользователя"""
    try:
        await log_user_activity(
            telegram_id=user_id,
            action=action,
            details={"interaction": details} if details else {},
            step=action
        )
    except Exception as e:
        logger.warning(f"Не удалось залогировать активность пользователя {user_id}: {e}")

# ============================================================================
# КОМАНДЫ БОТА (С ЗАЩИТОЙ)
# ============================================================================

@router.message(Command("help"))
async def help_command(message: Message, state: FSMContext):
    """Команда помощи"""
    await log_user_interaction(message.from_user.id, "help_requested")
    
    # Проверяем статус пользователя
    user_completed = check_user_completed(message.from_user.id)
    current_state = await state.get_state()
    
    if user_completed:
        # Пользователь уже завершил диагностику
        text = """❓ <b>ПОМОЩЬ - Диагностика завершена</b>

✅ Поздравляем! Вы успешно прошли полную диагностику.

🗓 <b>Что дальше:</b>
• Вебинар "Умный кардиочекап": 3 августа в 12:00 МСК
• Ссылка на эфир появится здесь за час до начала
• Подготовьте результаты анализов (если есть)

📋 <b>Доступные команды:</b>
/start - Посмотреть результаты диагностики
/status - Проверить статус
/restart - Пройти диагностику заново

💡 <b>Нужна помощь?</b>
Просто напишите ваш вопрос, и мы постараемся помочь."""

    elif current_state and ("survey" in current_state or "test" in current_state):
        # Пользователь в процессе диагностики
        text = """❓ <b>ПОМОЩЬ - Диагностика в процессе</b>

📝 Вы сейчас проходите диагностику. Это важно для получения максимальной пользы от вебинара!

🔄 <b>Что делать:</b>
• Продолжайте отвечать на вопросы
• Каждый ответ важен для точной оценки
• Процесс займет 10-15 минут

⚠️ <b>Если что-то пошло не так:</b>
/restart - Начать диагностику заново
/status - Посмотреть прогресс

💡 <b>Не можете ответить на вопрос?</b>
Выберите наиболее подходящий вариант или тот, который ближе всего к вашей ситуации."""

    else:
        # Пользователь еще не начал или прервал диагностику
        text = """❓ <b>ПОМОЩЬ - Добро пожаловать!</b>

🫀 Этот бот поможет вам подготовиться к вебинару <b>"Умный кардиочекап"</b> с врачами-кардиологами.

🎯 <b>Что вы получите:</b>
• Полную диагностику состояния сердечно-сосудистой системы
• Оценку рисков и факторов
• Персональные рекомендации на вебинаре
• Бонусные материалы и список анализов

🚀 <b>Как начать:</b>
1. Нажмите /start
2. Введите контактные данные (имя, email, телефон)
3. Пройдите опрос (18 вопросов, 5-7 минут)
4. Выполните психологические тесты (7 тестов, 10-15 минут)
5. Получите результаты и материалы

📋 <b>Команды:</b>
/start - Начать диагностику
/status - Проверить прогресс
/restart - Начать заново"""

    await message.answer(text, parse_mode="HTML")

@router.message(Command("status"))
async def status_command(message: Message, state: FSMContext):
    """Команда проверки статуса"""
    await log_user_interaction(message.from_user.id, "status_requested")
    
    try:
        # Получаем данные пользователя
        data = get_user_data(message.from_user.id)
        user = data.get('user')
        survey = data.get('survey') 
        tests = data.get('tests')
        
        if not user:
            # Пользователь не зарегистрирован
            text = """📊 <b>ВАШ СТАТУС</b>

❌ Вы еще не начали диагностику

🚀 <b>Что нужно сделать:</b>
1. Нажмите /start для начала
2. Заполните контактные данные
3. Пройдите опрос и тесты

💡 Это займет всего 15-20 минут, но поможет получить максимум пользы от вебинара!"""
            
        else:
            # Формируем статус
            text = f"""📊 <b>ВАШ СТАТУС ДИАГНОСТИКИ</b>

👤 <b>Пользователь:</b> {user.name or 'Не указано'}
📧 Email: {user.email or 'Не указан'}
📱 Телефон: {user.phone or 'Не указан'}

<b>ПРОГРЕСС:</b>"""
            
            # Регистрация
            if user.registration_completed:
                text += "\n✅ Регистрация завершена"
            else:
                text += "\n❌ Регистрация не завершена"
            
            # Опрос
            if user.survey_completed:
                text += "\n✅ Опрос пройден (18/18 вопросов)"
                if survey and survey.age:
                    text += f"\n   • Возраст: {survey.age} лет, пол: {survey.gender or 'не указан'}"
            else:
                text += "\n❌ Опрос не пройден (0/18 вопросов)"
            
            # Тесты
            if user.tests_completed:
                text += "\n✅ Тесты пройдены (7/7 тестов)"
                if tests and tests.overall_cv_risk_level:
                    text += f"\n   • Сердечно-сосудистый риск: {tests.overall_cv_risk_level}"
            else:
                text += "\n❌ Тесты не пройдены (0/7 тестов)"
            
            # Общий статус
            if user.completed_diagnostic:
                text += f"""

🎉 <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>
• Дата завершения: {user.last_activity.strftime('%d.%m.%Y') if user.last_activity else 'Неизвестно'}
• Вы готовы к вебинару!

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК"""
            else:
                text += f"""

⏳ <b>ДИАГНОСТИКА НЕ ЗАВЕРШЕНА</b>

🔄 <b>Следующий шаг:</b>"""
                if not user.registration_completed:
                    text += "\n• Завершите регистрацию (/start)"
                elif not user.survey_completed:
                    text += "\n• Пройдите опрос (/start)"
                elif not user.tests_completed:
                    text += "\n• Пройдите тесты (/start)"
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка в status_command для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка получения статуса. Попробуйте /start")

@router.message(Command("restart"))
async def restart_command(message: Message, state: FSMContext):
    """Команда перезапуска диагностики"""
    await log_user_interaction(message.from_user.id, "restart_requested")
    
    text = """🔄 <b>ПЕРЕЗАПУСК ДИАГНОСТИКИ</b>

⚠️ Вы уверены, что хотите начать диагностику заново?

❗ <b>Внимание:</b>
• Все ваши текущие ответы будут удалены
• Вам нужно будет снова пройти весь процесс
• Это займет 15-20 минут

✅ Если вы готовы, нажмите кнопку ниже:"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Да, начать заново", callback_data="confirm_restart")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_restart")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

# ============================================================================
# ЗАЩИЩЕННЫЙ ОБРАБОТЧИК /start
# ============================================================================

@router.message(CommandStart())
async def start_command_protected(message: Message, state: FSMContext):
    """Защищенный обработчик команды /start"""
    await log_user_interaction(message.from_user.id, "start_command")
    
    # Получаем текущее состояние
    current_state = await state.get_state()
    
    # Проверяем, завершил ли пользователь диагностику
    user_completed = check_user_completed(message.from_user.id)
    
    if user_completed:
        # Пользователь уже завершил диагностику
        await show_completed_user_info(message, state)
        return
    
    # Проверяем, в каком состоянии находится пользователь
    if current_state:
        await handle_start_during_process(message, state, current_state)
    else:
        # Пользователь не в процессе - показываем стартовое сообщение
        await show_start_message(message, state)

async def handle_start_during_process(message: Message, state: FSMContext, current_state: str):
    """Обработка /start во время процесса диагностики"""
    
    # Определяем, на каком этапе пользователь
    if "waiting_name" in current_state or "waiting_email" in current_state or "waiting_phone" in current_state:
        stage = "регистрации"
        current_step = "заполнение контактных данных"
    elif "survey" in current_state:
        stage = "опроса"
        current_step = "ответы на вопросы о здоровье"
    elif "test" in current_state:
        stage = "тестирования"
        current_step = "психологические тесты"
    else:
        stage = "диагностики"
        current_step = "неизвестный этап"
    
    text = f"""🔄 <b>ВЫ УЖЕ В ПРОЦЕССЕ ДИАГНОСТИКИ</b>

⏳ Сейчас вы проходите этап: <b>{stage}</b>
📍 Текущий шаг: {current_step}

❓ <b>Что вы хотите сделать?</b>"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить с текущего места", callback_data="continue_current")],
        [InlineKeyboardButton(text="🔄 Начать заново (удалить прогресс)", callback_data="restart_from_beginning")],
        [InlineKeyboardButton(text="📊 Посмотреть мой статус", callback_data="show_status")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

async def show_completed_user_info(message: Message, state: FSMContext):
    """Показать информацию для завершившего диагностику пользователя"""
    
    try:
        data = get_user_data(message.from_user.id)
        user = data.get('user')
        tests = data.get('tests')
        
        name = user.name if user else "Пользователь"
        risk_level = tests.overall_cv_risk_level if tests else "не определен"
        
        text = f"""🎉 <b>Добро пожаловать, {name}!</b>

✅ Вы уже завершили диагностику!
🎯 Ваш сердечно-сосудистый риск: <b>{risk_level}</b>

🗓 <b>Вебинар "Умный Кардиочекап":</b>
📅 3 августа в 12:00 МСК
📍 Ссылка на эфир появится здесь за час до начала

💡 <b>Что вы можете сделать:</b>
• Подготовить результаты анализов (если есть)
• Приготовить блокнот для записей
• Пригласить близких к просмотру

До встречи на вебинаре! 💪"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Посмотреть полные результаты", callback_data="show_full_results")],
            [InlineKeyboardButton(text="🔄 Пройти диагностику заново", callback_data="restart_from_beginning")],
            [InlineKeyboardButton(text="📋 Материалы к вебинару", callback_data="show_materials")]
        ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в show_completed_user_info для пользователя {message.from_user.id}: {e}")
        await message.answer("❌ Ошибка получения данных. Попробуйте /status для проверки статуса.")

async def show_start_message(message: Message, state: FSMContext):
    """Показать стартовое сообщение для нового пользователя"""
    
    text = """🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара <b>«Умный кардиочекап»</b>.

❣️ Помогу вам подготовиться к вебинару, пройти мини-диагностику, получить бонусы, список анализов, нужные ссылки и материалы.

👉 Нажмите <b>«Старт»</b>, чтобы включить меня и начать подготовку к самому важному вебинару этого лета."""
    
    keyboard = get_start_keyboard()
    await message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    await state.set_state(UserStates.waiting_start)

# ============================================================================
# ОБРАБОТЧИКИ CALLBACK'ОВ ДЛЯ ЗАЩИТЫ СОСТОЯНИЙ
# ============================================================================

@router.callback_query(F.data == "continue_current")
async def continue_current_process(callback: CallbackQuery, state: FSMContext):
    """Продолжить с текущего места"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "continue_current")
    
    current_state = await state.get_state()
    
    if "waiting_name" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

1️⃣ Напишите, пожалуйста, как к вам обращаться.

✍️ Введите ваше имя"""
        await safe_edit_message(callback.message, text)
        
    elif "waiting_email" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

2️⃣ Укажите, пожалуйста, ваш e-mail.

✍️ Введите ваш e-mail"""
        await safe_edit_message(callback.message, text)
        
    elif "waiting_phone" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

3️⃣ Поделитесь вашим номером телефона.

📱 Нажмите кнопку ниже, чтобы поделиться номером телефона:"""
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await safe_edit_message(callback.message, text)
        await callback.message.answer("👆 Используйте кнопку выше", reply_markup=keyboard)
        
    elif "survey" in current_state:
        text = """📝 <b>Продолжаем опрос</b>

Вы проходили опрос о вашем здоровье. Продолжайте отвечать на вопросы."""
        await safe_edit_message(callback.message, text)
        
    elif "test" in current_state:
        text = """🧪 <b>Продолжаем тестирование</b>

Вы проходили психологические тесты. Продолжайте тестирование."""
        await safe_edit_message(callback.message, text)
        
    else:
        await safe_edit_message(callback.message, "Продолжаем с того места, где остановились...")

@router.callback_query(F.data == "restart_from_beginning")
async def restart_from_beginning(callback: CallbackQuery, state: FSMContext):
    """Начать заново с самого начала"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "restart_from_beginning")
    
    # Полностью очищаем состояние
    await state.clear()
    
    text = """🔄 <b>ДИАГНОСТИКА ПЕРЕЗАПУЩЕНА</b>

Начинаем с самого начала!

🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара <b>«Умный кардиочекап»</b>.

👉 Нажмите <b>«Старт»</b>, чтобы начать подготовку к вебинару."""
    
    keyboard = get_start_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.waiting_start)

@router.callback_query(F.data == "show_status")
async def show_status_callback(callback: CallbackQuery, state: FSMContext):
    """Показать статус через callback"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_status_callback")
    
    try:
        data = get_user_data(callback.from_user.id)
        user = data.get('user')
        
        if not user:
            text = """📊 <b>ВАШ СТАТУС</b>

❌ Регистрация не начата

🚀 Нажмите "Начать заново" для начала диагностики."""
        else:
            text = f"""📊 <b>ВАШ СТАТУС</b>

👤 Имя: {user.name or 'Не указано'}
📧 Email: {user.email or 'Не указан'}
📱 Телефон: {user.phone or 'Не указан'}

<b>ПРОГРЕСС:</b>
{'✅' if user.registration_completed else '❌'} Регистрация
{'✅' if user.survey_completed else '❌'} Опрос
{'✅' if user.tests_completed else '❌'} Тесты
{'✅' if user.completed_diagnostic else '❌'} Диагностика"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ Продолжить", callback_data="continue_current")],
            [InlineKeyboardButton(text="🔄 Начать заново", callback_data="restart_from_beginning")]
        ])
        
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в show_status_callback для пользователя {callback.from_user.id}: {e}")
        await safe_edit_message(callback.message, "❌ Ошибка получения статуса")

@router.callback_query(F.data == "show_full_results")
async def show_full_results(callback: CallbackQuery, state: FSMContext):
    """Показать полные результаты диагностики"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_full_results")
    
    try:
        summary = await generate_final_results_summary(callback.from_user.id)
        
        # Ограничиваем длину для Telegram
        if len(summary) > 4000:
            summary = summary[:3900] + "\n\n... (результаты сокращены)"
        
        await safe_edit_message(callback.message, summary)
        
    except Exception as e:
        logger.error(f"Ошибка в show_full_results для пользователя {callback.from_user.id}: {e}")
        await safe_edit_message(callback.message, "❌ Ошибка получения результатов")

@router.callback_query(F.data == "show_materials")
async def show_materials_callback(callback: CallbackQuery, state: FSMContext):
    """Показать материалы к вебинару"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_materials")
    
    text = """📋 <b>МАТЕРИАЛЫ К ВЕБИНАРУ</b>

🗓 <b>Вебинар "Умный Кардиочекап":</b>
📅 3 августа в 12:00 МСК

📎 <b>Ваши материалы:</b>
• Список базовых анализов
• Чек-лист препаратов
• Результаты диагностики

Все материалы были отправлены вам после завершения диагностики. Проверьте историю чата выше.

💡 <b>Подготовка к вебинару:</b>
• Подготовьте результаты анализов (если есть)
• Приготовьте блокнот для записей
• Проверьте интернет-соединение"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="show_full_results")]
    ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

@router.callback_query(F.data == "confirm_restart")
async def confirm_restart(callback: CallbackQuery, state: FSMContext):
    """Подтверждение перезапуска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "confirm_restart")
    
    await state.clear()
    
    # Начинаем с самого начала
    text = """🔄 <b>ДИАГНОСТИКА ПЕРЕЗАПУЩЕНА</b>

Отлично! Давайте начнем сначала.

🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара <b>«Умный кардиочекап»</b>.

❣️ Помогу вам подготовиться к вебинару, пройти мини-диагностику, получить бонусы, список анализов, нужные ссылки и материалы.

👉 Нажмите <b>«Старт»</b>, чтобы включить меня и начать подготовку к самому важному вебинару этого лета."""
    
    keyboard = get_start_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.waiting_start)

@router.callback_query(F.data == "cancel_restart")
async def cancel_restart(callback: CallbackQuery, state: FSMContext):
    """Отмена перезапуска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cancel_restart")
    
    text = """✅ <b>ОТМЕНА ПЕРЕЗАПУСКА</b>

Хорошо, продолжаем с того места, где остановились.

💡 <b>Доступные команды:</b>
/start - Продолжить диагностику
/status - Проверить статус  
/help - Получить помощь"""
    
    await safe_edit_message(callback.message, text)

# ============================================================================
# ОСНОВНЫЕ ОБРАБОТЧИКИ (ОРИГИНАЛЬНЫЕ С ЗАЩИТОЙ)
# ============================================================================

@router.callback_query(F.data == "start_bot", StateFilter(UserStates.waiting_start))
async def handle_start_bot(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия кнопки Старт"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "start_bot")
    
    text = """👋 Здравствуйте!
Вы зарегистрированы на вебинар <b>«Умный Кардиочекап»</b> с врачами-кардиологами Дианой Новиковой и Еленой Удачкиной.

Вы получите пошаговый алгоритм оценки риска развития сердечно-сосудистых заболеваний и их осложнений (в т.ч. инфаркт, инсульт, хроническая сердечная недостаточность), что позволит вовремя принять меры, сохранив здоровье и активность на годы вперёд без лишних затрат и избыточных обследований.

🗓 <b>Дата и время проведения:</b> 3 августа в 12:00 по МСК (запись будет)

📍 <b>Здесь, в боте, вы получите:</b>
— ссылку на вебинар и его запись
— список базовых анализов
— чек-листы
— бонусные материалы от врачей

Мы уже всё приготовили 👌 Но сначала ― очень важный шаг, к которому мы просим отнестись серьёзно: без него вы не сможете получить максимум пользы от вебинара.

Сейчас расскажу 👇"""
    
    await safe_edit_message(callback.message, text)
    
    # Задержка 15 секунд
    await asyncio.sleep(15)
    
    await send_contact_request(callback.message, state)

async def send_contact_request(message: Message, state: FSMContext):
    """Запрос контактных данных"""
    text = """‼️ <b>Небольшой организационный момент</b>

Чтобы вы получили всё без сбоев:
✔️ ссылку на вебинар и запись
✔️ список анализов и бонусные материалы
✔️ напоминания и доступ к платформе

давайте с вами познакомимся 🤝

Мне важно обращаться к вам по имени — так общение становится теплее и человечнее.

<b>1️⃣ Напишите, пожалуйста, как к вам обращаться.</b>

✍️ Введите ваше имя"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(UserStates.waiting_name)

@router.message(StateFilter(UserStates.waiting_name))
async def handle_name(message: Message, state: FSMContext):
    """Обработка имени пользователя"""
    await log_user_interaction(message.from_user.id, "name_entered", message.text)
    
    name = message.text.strip()
    
    # Сохраняем имя в состоянии
    await state.update_data(name=name)
    
    text = """<b>2️⃣ Укажите, пожалуйста, ваш e-mail.</b>

На него придет доступ к платформе, где будут храниться все необходимые материалы, которые мы добавим по завершении вебинара.

✍️ Введите ваш e-mail"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(UserStates.waiting_email)

@router.message(StateFilter(UserStates.waiting_email))
async def handle_email(message: Message, state: FSMContext):
    """Обработка email пользователя"""
    await log_user_interaction(message.from_user.id, "email_entered", message.text)
    
    email = message.text.strip()
    
    # Простая валидация email
    if "@" not in email or "." not in email:
        await message.answer("Пожалуйста, введите корректный email адрес.")
        return
    
    # Сохраняем email в состоянии
    await state.update_data(email=email)
    
    text = """<b>3️⃣ И последний шаг — поделитесь вашим номером телефона.</b>

❗Он нужен не для звонков и рекламы, а чтобы убедиться, что вы — настоящий человек, а не бот. Также он поможет, если возникнут проблемы с доступом к вебинару и другим важным материалам.

Всё конфиденциально и в рамках этики врача. Обещаю — никаких звонков.

📱 Нажмите кнопку ниже, чтобы поделиться номером телефона:"""
    
    # Создаем клавиатуру с кнопкой для отправки телефона
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(UserStates.waiting_phone)

@router.message(StateFilter(UserStates.waiting_phone))
async def handle_phone(message: Message, state: FSMContext):
    """ИСПРАВЛЕННАЯ обработка телефона пользователя с принудительным сохранением"""
    await log_user_interaction(message.from_user.id, "phone_processing")
    
    # Проверяем, отправлен ли контакт
    if message.contact:
        # Пользователь отправил контакт через кнопку
        phone = message.contact.phone_number
        
        # Проверяем, что это его собственный номер
        if message.contact.user_id != message.from_user.id:
            await message.answer(
                "❌ Пожалуйста, отправьте свой собственный номер телефона, нажав кнопку ниже.",
                reply_markup=ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]],
                    resize_keyboard=True,
                    one_time_keyboard=True
                )
            )
            return
    else:
        # Пользователь написал текст вместо отправки контакта
        await message.answer(
            "📱 Пожалуйста, используйте кнопку ниже для отправки номера телефона:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]],
                resize_keyboard=True,
                one_time_keyboard=True
            )
        )
        return
    
    # Убираем клавиатуру
    await message.answer("Данные сохранены!", reply_markup=ReplyKeyboardRemove())
    
    # Сохраняем телефон в состоянии
    await state.update_data(phone=phone)
    
    # КРИТИЧЕСКИ ВАЖНО: Сохраняем данные пользователя в базу с детальной диагностикой
    data = await state.get_data()
    
    logger.info(f"=== НАЧАЛО СОХРАНЕНИЯ ПОЛЬЗОВАТЕЛЯ {message.from_user.id} ===")
    logger.info(f"Данные для сохранения: name={data.get('name')}, email={data.get('email')}, phone={phone}")
    
    try:
        # Пытаемся сохранить пользователя
        save_result = await save_user_data(
            telegram_id=message.from_user.id,
            name=data.get('name'),
            email=data.get('email'),
            phone=phone
        )
        
        logger.info(f"✅ ПОЛЬЗОВАТЕЛЬ СОХРАНЕН УСПЕШНО: {save_result}")
        
        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: читаем пользователя из базы
        verification_data = get_user_data(message.from_user.id)
        if verification_data and verification_data.get('user'):
            logger.info(f"✅ ПОДТВЕРЖДЕНИЕ: Пользователь найден в базе данных")
            logger.info(f"ID пользователя в БД: {verification_data['user'].id}")
            logger.info(f"Telegram ID: {verification_data['user'].telegram_id}")
            logger.info(f"Имя: {verification_data['user'].name}")
            logger.info(f"Email: {verification_data['user'].email}")
            logger.info(f"Телефон: {verification_data['user'].phone}")
            logger.info(f"Регистрация завершена: {verification_data['user'].registration_completed}")
        else:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Пользователь НЕ НАЙДЕН в базе после сохранения!")
            raise Exception("Пользователь не сохранился в базу данных")
        
        await log_user_interaction(message.from_user.id, "registration_completed")
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА сохранения пользователя {message.from_user.id}: {e}")
        await message.answer("❌ Ошибка сохранения данных. Попробуйте /restart")
        return
    
    # Продолжаем только если пользователь точно сохранен
    text1 = """✅ Спасибо! Всё готово.

Совсем скоро мы пришлем бонусы и список базовых анализов для подготовки.

📋 Прежде чем мы пришлём материалы, небольшая просьба — пройдите, пожалуйста, опрос. Это небольшая предварительная диагностика — важная часть нашей с вами совместной работы.

Ведь мы с вами — одна команда 🦸‍♂️"""
    
    text2 = """Вы проходите диагностику, чтобы лучше понять, на что обратить внимание и как извлечь максимум пользы из вебинара.

А мы детально изучим ваши анкеты, чтобы на основании ваших ответов расставить верные акценты и сделать вебинар действительно полезным для вас."""
    
    text3 = """А ещё — это часть нашей большой миссии ☝️ Мы изучаем потенциал социальных сетей в повышении информированности и приверженности к выполнению рекомендаций с целью улучшения здоровья населения нашей страны.

Опрос является конфиденциальным, и данные на основе законодательства РФ никому не передаются."""
    
    text4 = """⚕️ Ваше участие — вклад в решение масштабной задачи: сохранение миллионов жизней.

Благодаря нашим совместным усилиям мы сможем говорить с медицинским сообществом и системой здравоохранения на языке фактов — и менять подход к профилактике и лечению сердечно-сосудистых заболеваний в масштабах страны."""
    
    # Отправляем сообщения по частям
    await message.answer(text1)
    await asyncio.sleep(5)
    await message.answer(text2)
    await asyncio.sleep(5)
    await message.answer(text3)
    await asyncio.sleep(5)
    await message.answer(text4)
    
    # Задержка 15 секунд
    await asyncio.sleep(15)
    
    await start_survey(message, state)

# ============================================================================
# ОБРАБОТЧИКИ ОПРОСА (ПОЛНЫЕ ОРИГИНАЛЬНЫЕ)
# ============================================================================

async def start_survey(message: Message, state: FSMContext):
    """Начало опроса с удалением предыдущих сообщений"""
    await log_user_interaction(message.from_user.id, "survey_started")
    
    # Удаляем предыдущие сообщения
    if hasattr(message, 'message_id') and message.message_id > 1:
        # Пытаемся удалить последние несколько сообщений
        for i in range(max(1, message.message_id - 10), message.message_id):
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=i)
            except:
                pass  # Игнорируем ошибки удаления

    text = """<b>❓ Вопрос 1</b>
Сколько вам лет?
(введите число)"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(UserStates.survey_age)

@router.message(StateFilter(UserStates.survey_age))
async def handle_age(message: Message, state: FSMContext):
    """Обработка возраста с удалением сообщений"""
    await log_user_interaction(message.from_user.id, "age_entered", message.text)
    
    try:
        age = int(message.text.strip())
        if age < 1 or age > 120:
            await message.answer("Пожалуйста, введите корректный возраст.")
            return
        
        await state.update_data(age=age)
        
        # Удаляем сообщение пользователя и вопрос
        await message.delete()
        # Пытаемся найти и удалить сообщение с вопросом (обычно предыдущее)
        if message.message_id > 1:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - 1)
            except:
                pass
       
        text = """<b>❓ Вопрос 2</b>

Ваш пол"""
        
        keyboard = get_gender_keyboard()
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(UserStates.survey_gender)
        
    except ValueError:
        await message.answer("Пожалуйста, введите число.")

@router.callback_query(F.data.in_(["gender_female", "gender_male"]), StateFilter(UserStates.survey_gender))
async def handle_gender(callback: CallbackQuery, state: FSMContext):
    """Обработка пола"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "gender_selected", callback.data)
    
    gender = "Женский" if callback.data == "gender_female" else "Мужской"
    await state.update_data(gender=gender)
    
    text = """<b>❓ Вопрос 3</b>
Где вы живёте?
(выберите 1 вариант ответа)

Выберите тип населённого пункта:"""
    
    keyboard = get_location_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_location)

@router.callback_query(F.data.startswith("location_"), StateFilter(UserStates.survey_location))
async def handle_location(callback: CallbackQuery, state: FSMContext):
    """Обработка места жительства"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "location_selected", callback.data)
    
    location_map = {
        "location_big_city": "Город с населением >1 млн",
        "location_medium_city": "Город 500–999 тыс",
        "location_small_city": "Город с населением 100–500 тыс",
        "location_town": "Город до 100 тыс",
        "location_village": "Поселок / сельская местность"
    }
    
    location = location_map[callback.data]
    await state.update_data(location=location)
    
    text = """<b>❓ Вопрос 4</b>
Ваше образование
(выберите 1 вариант ответа)"""
    
    keyboard = get_education_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_education)

@router.callback_query(F.data.startswith("education_"), StateFilter(UserStates.survey_education))
async def handle_education(callback: CallbackQuery, state: FSMContext):
    """Обработка образования"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "education_selected", callback.data)
    
    education_map = {
        "education_secondary": "Среднее общее",
        "education_vocational": "Средне-специальное",
        "education_higher": "Высшее (немедицинское)",
        "education_medical": "Высшее медицинское"
    }
    
    education = education_map[callback.data]
    await state.update_data(education=education)
    
    text = """<b>❓ Вопрос 5</b>
Ваше семейное положение
(выберите 1 вариант ответа)"""
    
    keyboard = get_family_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_family)

@router.callback_query(F.data.startswith("family_"), StateFilter(UserStates.survey_family))
async def handle_family(callback: CallbackQuery, state: FSMContext):
    """Обработка семейного положения"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "family_selected", callback.data)
    
    family_map = {
        "family_single": "Холост / не замужем",
        "family_married": "В браке",
        "family_divorced": "Разведён(а) / вдов(ец/а)"
    }
    
    family = family_map[callback.data]
    await state.update_data(family_status=family)
    
    text = """<b>❓ Вопрос 6</b>
Есть ли у вас дети?
(выберите 1 вариант ответа)"""
    
    keyboard = get_children_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_children)

@router.callback_query(F.data.startswith("children_"), StateFilter(UserStates.survey_children))
async def handle_children(callback: CallbackQuery, state: FSMContext):
    """Обработка наличия детей"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "children_selected", callback.data)
    
    children_map = {
        "children_none": "Нет",
        "children_one": "Да, один",
        "children_multiple": "Да, двое и более"
    }
    
    children = children_map[callback.data]
    await state.update_data(children=children)
    
    text = """<b>❓ Вопрос 7</b>
Среднемесячный доход на 1 работающего человека в семье 
(выберите 1 вариант ответа)"""
    
    keyboard = get_income_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_income)

@router.callback_query(F.data.startswith("income_"), StateFilter(UserStates.survey_income))
async def handle_income(callback: CallbackQuery, state: FSMContext):
    """Обработка дохода"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "income_selected", callback.data)
    
    income_map = {
        "income_low": "До 20 000 ₽",
        "income_medium": "20–40 тыс ₽",
        "income_high": "40–70 тыс ₽",
        "income_very_high": "Более 70 тыс ₽",
        "income_no_answer": "Предпочитаю не указывать"
    }
    
    income = income_map[callback.data]
    await state.update_data(income=income)
    
    text = """<b>❓ Вопрос 8</b>
Как вы оцениваете своё здоровье по шкале от 0 до 10?
(0 — очень плохо, 10 — отлично)

Введите число"""
    
    await safe_edit_message(callback.message, text)
    await state.set_state(UserStates.survey_health)

@router.message(StateFilter(UserStates.survey_health))
async def handle_health_rating(message: Message, state: FSMContext):
    """Обработка оценки здоровья"""
    await log_user_interaction(message.from_user.id, "health_rating_entered", message.text)
    
    try:
        health_rating = int(message.text.strip())
        if health_rating < 0 or health_rating > 10:
            await message.answer("Пожалуйста, введите число от 0 до 10.")
            return
        
        await state.update_data(health_rating=health_rating)
        
        # Удаляем сообщение пользователя и вопрос
        await message.delete()
        # Пытаемся найти и удалить сообщение с вопросом (обычно предыдущее)
        if message.message_id > 1:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - 1)
            except:
                pass

        text = """<b>❓ Вопрос 9</b>
На ваш взгляд, какая из перечисленных причин чаще всего приводит к смерти людей в мире? 
(выберите 1 вариант ответа)"""
        
        keyboard = get_death_cause_keyboard()
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(UserStates.survey_death_cause)
        
    except ValueError:
        await message.answer("Пожалуйста, введите число от 0 до 10.")


@router.callback_query(F.data.startswith("death_cause_"), StateFilter(UserStates.survey_death_cause))
async def handle_death_cause(callback: CallbackQuery, state: FSMContext):
    """Обработка причины смерти"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "death_cause_selected", callback.data)
    
    cause_map = {
        "death_cause_cancer": "Онкологические заболевания",
        "death_cause_cardio": "Сердечно-сосудистые заболевания",
        "death_cause_infections": "Инфекции",
        "death_cause_respiratory": "Болезни дыхательных путей",
        "death_cause_digestive": "Болезни желудочно-кишечного тракта",
        "death_cause_external": "Внешние причины"
    }
    
    death_cause = cause_map[callback.data]
    await state.update_data(death_cause=death_cause)
    
    text = """<b>❓ Вопрос 10</b>
Есть ли у вас хронические заболевания сердца или сосудов?
(выберите 1 вариант ответа)"""
    
    keyboard = get_heart_disease_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_heart_disease)

@router.callback_query(F.data.startswith("heart_disease_"), StateFilter(UserStates.survey_heart_disease))
async def handle_heart_disease(callback: CallbackQuery, state: FSMContext):
    """Обработка наличия заболеваний сердца"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "heart_disease_selected", callback.data)
    
    disease_map = {
        "heart_disease_yes": "Да",
        "heart_disease_no": "Нет",
        "heart_disease_unknown": "Не знаю / не обследовался(ась)"
    }
    
    heart_disease = disease_map[callback.data]
    await state.update_data(heart_disease=heart_disease)
    
    text = """<b>❓ Вопрос 11</b>
Как вы оцениваете свой сердечно-сосудистый риск? 
(выберите 1 вариант ответа)"""
    
    keyboard = get_cv_risk_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_cv_risk)

@router.callback_query(F.data.startswith("cv_risk_"), StateFilter(UserStates.survey_cv_risk))
async def handle_cv_risk(callback: CallbackQuery, state: FSMContext):
    """Обработка оценки сердечно-сосудистого риска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cv_risk_selected", callback.data)
    
    risk_map = {
        "cv_risk_low": "низкий/умеренный",
        "cv_risk_high": "высокий",
        "cv_risk_very_high": "очень высокий"
    }
    
    cv_risk = risk_map[callback.data]
    await state.update_data(cv_risk=cv_risk)
    
    text = """<b>❓ Вопрос 12</b>
Слышали ли вы раньше о факторах риска сердечно-сосудистых заболеваний?
(выберите 1 вариант ответа)"""
    
    keyboard = get_cv_knowledge_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_cv_knowledge)

@router.callback_query(F.data.startswith("cv_knowledge_"), StateFilter(UserStates.survey_cv_knowledge))
async def handle_cv_knowledge(callback: CallbackQuery, state: FSMContext):
    """Обработка знания о факторах риска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cv_knowledge_selected", callback.data)
    
    knowledge_map = {
        "cv_knowledge_good": "Да, хорошо разбираюсь",
        "cv_knowledge_some": "Да, но не до конца понимаю",
        "cv_knowledge_none": "Нет / почти ничего не знаю"
    }
    
    cv_knowledge = knowledge_map[callback.data]
    await state.update_data(cv_knowledge=cv_knowledge)
    
    text = """<b>❓ Вопрос 13</b>
Что из перечисленного вы считаете наиболее опасным для сердца?
(выберите до 3 вариантов)"""
    
    await state.update_data(heart_danger_selected=[])  # Инициализируем список выбранных вариантов
    keyboard = get_heart_danger_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_heart_danger)

@router.callback_query(F.data.startswith("heart_danger_"), StateFilter(UserStates.survey_heart_danger))
async def handle_heart_danger(callback: CallbackQuery, state: FSMContext):
    """Обработка опасных факторов для сердца (мультивыбор до 3)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('heart_danger_selected', [])
    
    if callback.data == "heart_danger_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(heart_danger=selected)
        await log_user_interaction(callback.from_user.id, "heart_danger_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 14</b>
Как вы оцениваете для себя важность регулярного наблюдения за здоровьем сердца?
(выберите 1 вариант ответа)"""
        
        keyboard = get_health_importance_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_health_importance)
        return
    
    danger_map = {
        "heart_danger_age": "Возраст",
        "heart_danger_male": "Мужской пол",
        "heart_danger_family": "Семейный анамнез ранних сердечно-сосудистых заболеваний",
        "heart_danger_pressure": "Повышенное артериальное давление",
        "heart_danger_cholesterol": "Повышенный холестерин",
        "heart_danger_glucose": "Повышение глюкозы в крови",
        "heart_danger_weight": "Избыточный вес",
        "heart_danger_smoking": "Курение",
        "heart_danger_alcohol": "Алкоголь",
        "heart_danger_nutrition": "Несбалансированное питание",
        "heart_danger_sedentary": "Малоподвижный образ жизни",
        "heart_danger_stress": "Стрессы",
        "heart_danger_sleep": "Нарушение сна, храп"
    }
    
    danger_option = danger_map.get(callback.data)
    if danger_option:
        if danger_option in selected:
            selected.remove(danger_option)
        else:
            if len(selected) < 3:
                selected.append(danger_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 3 варианта", show_alert=True)
                return
        
        await state.update_data(heart_danger_selected=selected)
        keyboard = get_heart_danger_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("health_importance_"), StateFilter(UserStates.survey_health_importance))
async def handle_health_importance(callback: CallbackQuery, state: FSMContext):
    """Обработка важности наблюдения за здоровьем сердца"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "health_importance_selected", callback.data)
    
    importance_map = {
        "health_importance_elderly": "Это для пожилых / хронически больных, не про меня",
        "health_importance_secondary": "Важно, но не на первом месте",
        "health_importance_understand": "Понимаю, что нужно, но раньше об этом не думал(а)",
        "health_importance_plan": "Осознаю значимость — планирую действовать"
    }
    
    health_importance = importance_map[callback.data]
    await state.update_data(health_importance=health_importance)
    
    text = """<b>❓ Вопрос 15</b>
Проходили ли вы кардиочекап ранее?
(выберите 1 вариант ответа)"""
    
    keyboard = get_checkup_history_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_checkup_history)

@router.callback_query(F.data.startswith("checkup_history_"), StateFilter(UserStates.survey_checkup_history))
async def handle_checkup_history(callback: CallbackQuery, state: FSMContext):
    """Обработка истории кардиочекапов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "checkup_history_selected", callback.data)
    
    history_map = {
        "checkup_history_recent": "Да, в последние 12 месяцев",
        "checkup_history_old": "Да, более года назад",
        "checkup_history_never": "Нет, никогда",
        "checkup_history_forgot": "Не помню"
    }
    
    checkup_history = history_map[callback.data]
    await state.update_data(checkup_history=checkup_history)
    
    text = """<b>❓ Вопрос 16</b>
Если вы проходили кардиочекап, то какие обследования он включал? 
(выберите все подходящие варианты)"""
    
    await state.update_data(checkup_content_selected=[])
    keyboard = get_checkup_content_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_checkup_content)

@router.callback_query(F.data.startswith("checkup_content_"), StateFilter(UserStates.survey_checkup_content))
async def handle_checkup_content(callback: CallbackQuery, state: FSMContext):
    """ПОЛНОСТЬЮ ИСПРАВЛЕННАЯ обработка содержимого кардиочекапа"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('checkup_content_selected', [])
    
    if callback.data == "checkup_content_done":
        await state.update_data(checkup_content=selected)
        await log_user_interaction(callback.from_user.id, "checkup_content_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 17</b>
Что мешает вам пройти профилактическое обследование сейчас?
(выберите все подходящие варианты)"""
        
        await state.update_data(prevention_barriers_selected=[])
        keyboard = get_prevention_barriers_keyboard([])
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_prevention_barriers)
        return
    
    # ТОЧНОЕ соответствие названий с клавиатурой
    content_map = {
        "checkup_content_consultation": "Консультация и осмотр врача-кардиолога / терапевта",
        "checkup_content_risk_assessment": "Оценка факторов риска сердечно-сосудистых заболеваний",
        "checkup_content_lipids": "Определение уровня липидов крови",
        "checkup_content_glucose": "Определение уровня глюкозы крови",
        "checkup_content_ecg": "ЭКГ",
        "checkup_content_ultrasound": "УЗИ сосудов (дуплексное сканирование)",
        "checkup_content_echo": "ЭхоКГ",
        "checkup_content_monitoring": "Суточное мониторирование давления",
        "checkup_content_ct": "МСКТ-коронарный кальций",
        "checkup_content_calc": "Расчет индивидуального СС-риска"
    }
    
    if callback.data == "checkup_content_skip":
        # Обрабатываем "Не проходил(а)"
        if "Не проходил(а)" in selected:
            selected.remove("Не проходил(а)")
        else:
            # Очищаем все остальные выборы и добавляем "Не проходил(а)"
            selected.clear()
            selected.append("Не проходил(а)")
        
        await state.update_data(checkup_content_selected=selected)
        keyboard = get_checkup_content_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)
        return
    
    # Обрабатываем выбор конкретного пункта
    content_option = content_map.get(callback.data)
    if content_option:
        # Если выбираем обычную опцию, убираем "Не проходил(а)"
        if "Не проходил(а)" in selected:
            selected.remove("Не проходил(а)")
        
        if content_option in selected:
            selected.remove(content_option)
        else:
            selected.append(content_option)
        
        await state.update_data(checkup_content_selected=selected)
        keyboard = get_checkup_content_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)
        return
    
    # Если дошли сюда - неизвестный callback
    await safe_answer_callback(callback, "Неизвестная команда", show_alert=True)


@router.callback_query(F.data.startswith("prevention_barriers_"), StateFilter(UserStates.survey_prevention_barriers))
async def handle_prevention_barriers(callback: CallbackQuery, state: FSMContext):
    """Обработка препятствий для профилактического обследования (мультивыбор)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('prevention_barriers_selected', [])
    
    if callback.data == "prevention_barriers_done":
        await state.update_data(prevention_barriers=selected)
        await log_user_interaction(callback.from_user.id, "prevention_barriers_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 18</b>
С кем вы обычно советуетесь, если появляются вопросы со здоровьем?
(выберите до 2 вариантов)"""
        
        await state.update_data(health_advice_selected=[])
        keyboard = get_health_advice_keyboard([])
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_health_advice)
        return
    
    if callback.data == "prevention_barriers_other":
        await callback.message.answer("Введите ваш вариант:")
        # Здесь можно добавить обработку ввода текста для "Другое"
        return
    
    barriers_map = {
        "prevention_barriers_no_symptoms": "Не вижу необходимости — нет симптомов",
        "prevention_barriers_fear": "Страх услышать диагноз",
        "prevention_barriers_money": "Финансовые ограничения",
        "prevention_barriers_time": "Нет времени",
        "prevention_barriers_knowledge": "Не знаю, с чего начать",
        "prevention_barriers_doctor": "Уже наблюдаюсь у врача",
        "prevention_barriers_nothing": "Ничего"
    }
    
    barrier_option = barriers_map.get(callback.data)
    if barrier_option:
        if barrier_option in selected:
            selected.remove(barrier_option)
        else:
            selected.append(barrier_option)
        
        await state.update_data(prevention_barriers_selected=selected)
        keyboard = get_prevention_barriers_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("health_advice_"), StateFilter(UserStates.survey_health_advice))
async def handle_health_advice(callback: CallbackQuery, state: FSMContext):
    """Обработка источников советов по здоровью (мультивыбор до 2)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('health_advice_selected', [])
    
    if callback.data == "health_advice_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(health_advice=selected)
        await log_user_interaction(callback.from_user.id, "health_advice_completed", f"Selected: {len(selected)} items")
        
        # Сохраняем данные опроса в базу данных
        try:
            await save_survey_data(callback.from_user.id, await state.get_data())
        except Exception as e:
            logger.error(f"Ошибка сохранения опроса для {callback.from_user.id}: {e}")
            await safe_edit_message(callback.message, "❌ Ошибка сохранения данных. Попробуйте /restart")
            return
        
        text = """✅ Спасибо за помощь! 

Мы подходим к следующему этапу — диагностике скрытых факторов риска, которые часто остаются вне фокуса, но напрямую влияют на здоровье сердца и сосудов.

На вебинаре эти тесты помогут более точно рассчитать ваш суммарный риск с учетом не только анализов, но и качества сна, уровня тревоги, депрессии, вредных привычек и др.

Пожалуйста, пройдите их до вебинара — так вы извлечете гораздо больше пользы и сможете применить полученные рекомендации к своему случаю. 

👉 После этого я пришлю вам список базовых анализов и чек-лист подготовки к вебинару."""
        
        await safe_edit_message(callback.message, text)
        
        # Задержка 5 секунд
        await asyncio.sleep(5)
        
        await start_tests(callback.message, state)
        return
    
    advice_map = {
        "health_advice_doctor": "С врачом",
        "health_advice_relatives": "С родственниками",
        "health_advice_colleagues": "С коллегами",
        "health_advice_internet": "Через интернет (статьи, форумы)",
        "health_advice_blogger": "С врачом-блогером в соцсетях",
        "health_advice_nobody": "Ни с кем"
    }
    
    advice_option = advice_map.get(callback.data)
    if advice_option:
        if advice_option in selected:
            selected.remove(advice_option)
        else:
            if len(selected) < 2:
                selected.append(advice_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 2 варианта", show_alert=True)
                return
        
        await state.update_data(health_advice_selected=selected)
        keyboard = get_health_advice_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

# ============================================================================
# ОБРАБОТЧИКИ ТЕСТОВ (ПОЛНЫЕ ОРИГИНАЛЬНЫЕ)
# ============================================================================

async def start_tests(message: Message, state: FSMContext):
    """Начало прохождения тестов с защитой от потери состояния"""
    await log_user_interaction(message.from_user.id, "tests_started")
    
    # КРИТИЧЕСКИ ВАЖНО: НЕ очищаем состояние, а только переходим к тестам
    
    text = """Теперь пройдите психологические и медицинские тесты для более точной оценки вашего здоровья.

Выберите тест для прохождения:"""
    
    # Инициализируем пустой словарь для отслеживания пройденных тестов
    await state.update_data(completed_tests={})
    
    keyboard = get_test_selection_keyboard()
    
    try:
        await safe_edit_message(message, text, reply_markup=keyboard)
    except:
        # Если не удается отредактировать, отправляем новое сообщение
        await message.answer(text, reply_markup=keyboard)
    
    await state.set_state(UserStates.test_selection)
    
@router.callback_query(F.data.startswith("test_"), StateFilter(UserStates.test_selection))
async def handle_test_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора теста"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "test_selected", callback.data)
    
    if callback.data == "test_hads":
        await start_hads_test(callback.message, state)
    elif callback.data == "test_burns":
        await start_burns_test(callback.message, state)
    elif callback.data == "test_isi":
        await start_isi_test(callback.message, state)
    elif callback.data == "test_stop_bang":
        await start_stop_bang_test(callback.message, state)
    elif callback.data == "test_ess":
        await start_ess_test(callback.message, state)
    elif callback.data == "test_fagerstrom":
        await start_fagerstrom_test(callback.message, state)
    elif callback.data == "test_fagerstrom_skip":
        await state.update_data(fagerstrom_score=None, fagerstrom_skipped=True)
        await show_test_menu(callback.message, state)
    elif callback.data == "test_audit":
        await start_audit_test(callback.message, state)
    elif callback.data == "test_audit_skip":
        await state.update_data(audit_score=None, audit_skipped=True)
        await show_test_menu(callback.message, state)
    elif callback.data == "test_complete":
        await complete_all_tests(callback.message, state)

async def show_test_menu(message: Message, state: FSMContext):
    """Показать меню выбора тестов"""
    data = await state.get_data()
    
    text = "Выберите следующий тест для прохождения:"
    keyboard = get_test_selection_keyboard(data)
    
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(UserStates.test_selection)

# Функции для работы с тестами
async def start_hads_test(message: Message, state: FSMContext):
    """Запуск теста HADS"""
    from surveys import get_hads_questions
    
    questions = get_hads_questions()
    await state.update_data(
        current_test="hads",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🟣 <b>Тест 1. Уровень тревоги и депрессии — HADS</b>

Тест HADS помогает понять, есть ли у вас признаки тревоги или депрессии.

Ответьте на вопросы, выбрав наиболее подходящий вариант."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.hads_test)

async def start_burns_test(message: Message, state: FSMContext):
    """Запуск теста Бернса"""
    from surveys import get_burns_questions
    
    questions = get_burns_questions()
    await state.update_data(
        current_test="burns",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🔵 <b>Тест 2. Эмоциональное выгорание — Шкала депрессии Бернса</b>

Тест Бернса поможет оценить ваш эмоциональный фон.

Ответьте на вопросы, оценив каждое утверждение по шкале от 0 до 4."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.burns_test)

async def start_isi_test(message: Message, state: FSMContext):
    """Запуск теста ISI"""
    from surveys import get_isi_questions
    
    questions = get_isi_questions()
    await state.update_data(
        current_test="isi",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🌙 <b>Тест 3. Качество сна — ISI</b>

Тест ISI поможет понять, есть у вас ли признаки бессонницы или других нарушений сна, которые могут негативно сказываться на здоровье сердца."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.isi_test)

async def start_stop_bang_test(message: Message, state: FSMContext):
    """Запуск теста STOP-BANG"""
    from surveys import get_stop_bang_questions
    
    questions = get_stop_bang_questions()
    await state.update_data(
        current_test="stop_bang",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """😴 <b>Тест 4. Риск апноэ сна — STOP-BANG</b>

Тест STOP-BANG поможет определить, есть ли у вас риск апноэ сна — состояния, при котором дыхание во сне периодически останавливается. Его опасность часто недооценивается, хотя апноэ сна напрямую связано с риском инфаркта и инсульта."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.stop_bang_test)

async def start_ess_test(message: Message, state: FSMContext):
    """Запуск теста ESS"""
    from surveys import get_ess_questions
    
    questions = get_ess_questions()
    await state.update_data(
        current_test="ess",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """😴 <b>Тест 5. Сонливость днём — ESS</b>

Тест ESS поможет оценить уровень дневной сонливости, чтобы выявить скрытые нарушения сна, которые могут влиять на давление, работу сердца и общее самочувствие."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.ess_test)

async def start_fagerstrom_test(message: Message, state: FSMContext):
    """Запуск теста Фагерстрема"""
    from surveys import get_fagerstrom_questions
    
    questions = get_fagerstrom_questions()
    await state.update_data(
        current_test="fagerstrom",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🚬 <b>Тест 6. Никотиновая зависимость — Фагерстрем</b>

Тест для оценки степени никотиновой зависимости у курящих людей."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.fagerstrom_test)

async def start_audit_test(message: Message, state: FSMContext):
    """Запуск теста AUDIT"""
    from surveys import get_audit_questions
    
    questions = get_audit_questions()
    await state.update_data(
        current_test="audit",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🍷 <b>Тест 7. Употребление алкоголя — RUS-AUDIT</b>

Тест AUDIT поможет оценить влияние алкоголя на сосудистое здоровье."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.audit_test)

async def show_current_question(message: Message, state: FSMContext):
    """Показать текущий вопрос теста"""
    data = await state.get_data()
    questions = data['test_questions']
    current_index = data['current_question_index']
    current_test = data['current_test']
    
    if current_index >= len(questions):
        await complete_current_test(message, state)
        return
    
    question = questions[current_index]
    
    text = f"<b>Вопрос {current_index + 1} из {len(questions)}</b>\n\n{question['text']}"
    
    if question.get('info_text'):
        text += f"\n\nℹ️ {question['info_text']}"
    
    keyboard = get_question_keyboard(question, current_test)
    await safe_edit_message(message, text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("answer_"))
async def handle_test_answer(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос теста с защитой от потери состояния"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    
    # КРИТИЧЕСКАЯ ПРОВЕРКА: если состояние потеряно, восстанавливаем контекст
    if 'current_test' not in data:
        logger.warning(f"Потеряно состояние для пользователя {callback.from_user.id}. Пытаюсь восстановить.")
        
        # Проверяем текущее состояние FSM
        current_fsm_state = await state.get_state()
        
        if current_fsm_state:
            # Пытаемся определить тест по состоянию FSM
            if "hads_test" in current_fsm_state:
                await state.update_data(current_test="hads")
                # Восстанавливаем базовую структуру
                from surveys import get_hads_questions
                questions = get_hads_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "burns_test" in current_fsm_state:
                await state.update_data(current_test="burns")
                from surveys import get_burns_questions
                questions = get_burns_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "isi_test" in current_fsm_state:
                await state.update_data(current_test="isi")
                from surveys import get_isi_questions
                questions = get_isi_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "stop_bang_test" in current_fsm_state:
                await state.update_data(current_test="stop_bang")
                from surveys import get_stop_bang_questions
                questions = get_stop_bang_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "ess_test" in current_fsm_state:
                await state.update_data(current_test="ess")
                from surveys import get_ess_questions
                questions = get_ess_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "fagerstrom_test" in current_fsm_state:
                await state.update_data(current_test="fagerstrom")
                from surveys import get_fagerstrom_questions
                questions = get_fagerstrom_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            elif "audit_test" in current_fsm_state:
                await state.update_data(current_test="audit")
                from surveys import get_audit_questions
                questions = get_audit_questions()
                await state.update_data(
                    test_questions=questions,
                    current_question_index=0,
                    test_answers=[]
                )
            else:
                # Не можем восстановить - возвращаем к выбору тестов
                await safe_edit_message(
                    callback.message,
                    "❌ Произошла ошибка. Вернитесь к выбору тестов.",
                    reply_markup=get_test_selection_keyboard()
                )
                await state.set_state(UserStates.test_selection)
                return
        else:
            # Совсем потеряно состояние - предлагаем начать заново
            await safe_edit_message(
                callback.message,
                "❌ Сессия прервана. Выберите тест для прохождения заново:",
                reply_markup=get_test_selection_keyboard()
            )
            await state.set_state(UserStates.test_selection)
            return
        
        # Перезагружаем данные после восстановления
        data = await state.get_data()
    
    # Получаем данные теста
    current_test = data.get('current_test')
    current_index = data.get('current_question_index', 0)
    answers = data.get('test_answers', [])
    
    if not current_test:
        await safe_edit_message(
            callback.message,
            "❌ Ошибка состояния теста. Начните тест заново:",
            reply_markup=get_test_selection_keyboard()
        )
        await state.set_state(UserStates.test_selection)
        return
    
    # Извлекаем оценку из callback_data
    try:
        score = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await safe_answer_callback(callback, "❌ Некорректный ответ", show_alert=True)
        return
    
    answers.append(score)
    
    await log_user_interaction(callback.from_user.id, f"{current_test}_answer", f"Q{current_index+1}: {score}")
    
    await state.update_data(
        test_answers=answers,
        current_question_index=current_index + 1
    )
    
    # Продолжаем показывать следующий вопрос
    await show_current_question(callback.message, state)

async def complete_current_test(message: Message, state: FSMContext):
    """Завершение текущего теста с отметкой о завершении и ОБЯЗАТЕЛЬНЫМ сохранением результата"""
    data = await state.get_data()
    current_test = data['current_test']
    answers = data['test_answers']
    
    # Подсчитываем результат
    total_score = sum(answers)
    
    # Сохраняем результат в зависимости от теста
    if current_test == "hads":
        # Для HADS нужно разделить на тревогу и депрессию
        from surveys import calculate_hads_scores, get_hads_interpretation
        anxiety_score, depression_score = calculate_hads_scores(answers)
        
        await state.update_data(
            hads_anxiety_score=anxiety_score,
            hads_depression_score=depression_score,
            hads_score=total_score,
            completed_hads=True  # Отмечаем как завершенный
        )
        result_text = get_hads_interpretation(anxiety_score, depression_score)
        
    elif current_test == "burns":
        from surveys import get_burns_interpretation
        await state.update_data(
            burns_score=total_score,
            completed_burns=True
        )
        result_text = get_burns_interpretation(total_score)
        
    elif current_test == "isi":
        from surveys import get_isi_interpretation
        await state.update_data(
            isi_score=total_score,
            completed_isi=True
        )
        result_text = get_isi_interpretation(total_score)
        
    elif current_test == "stop_bang":
        from surveys import get_stop_bang_interpretation
        await state.update_data(
            stop_bang_score=total_score,
            completed_stop_bang=True
        )
        result_text = get_stop_bang_interpretation(total_score)
        
    elif current_test == "ess":
        from surveys import get_ess_interpretation
        await state.update_data(
            ess_score=total_score,
            completed_ess=True
        )
        result_text = get_ess_interpretation(total_score)
        
    elif current_test == "fagerstrom":
        from surveys import get_fagerstrom_interpretation
        await state.update_data(
            fagerstrom_score=total_score,
            completed_fagerstrom=True
        )
        result_text = get_fagerstrom_interpretation(total_score)
        
    elif current_test == "audit":
        from surveys import get_audit_interpretation
        await state.update_data(
            audit_score=total_score,
            completed_audit=True
        )
        result_text = get_audit_interpretation(total_score)
    
    # Логируем завершение теста
    await log_user_interaction(message.from_user.id, f"{current_test}_completed", f"Score: {total_score}")
    
    # КРИТИЧЕСКИ ВАЖНО: Сохраняем промежуточные результаты в базу данных СРАЗУ
    try:
        # Получаем все текущие данные состояния
        current_data = await state.get_data()
        
        # Формируем данные только для этого теста
        test_data_to_save = {}
        
        if current_test == "hads":
            test_data_to_save['hads_anxiety_score'] = current_data.get('hads_anxiety_score')
            test_data_to_save['hads_depression_score'] = current_data.get('hads_depression_score')
            test_data_to_save['hads_score'] = current_data.get('hads_score')
        elif current_test == "burns":
            test_data_to_save['burns_score'] = current_data.get('burns_score')
        elif current_test == "isi":
            test_data_to_save['isi_score'] = current_data.get('isi_score')
        elif current_test == "stop_bang":
            test_data_to_save['stop_bang_score'] = current_data.get('stop_bang_score')
        elif current_test == "ess":
            test_data_to_save['ess_score'] = current_data.get('ess_score')
        elif current_test == "fagerstrom":
            test_data_to_save['fagerstrom_score'] = current_data.get('fagerstrom_score')
        elif current_test == "audit":
            test_data_to_save['audit_score'] = current_data.get('audit_score')
        
        # ВРЕМЕННО сохраняем промежуточный результат
        logger.info(f"Сохраняю промежуточный результат теста {current_test} для пользователя {message.from_user.id}: {test_data_to_save}")
        
        # Загружаем текущие сохраненные данные и обновляем их
        existing_data = get_user_data(message.from_user.id)
        if existing_data and existing_data.get('tests'):
            # Если есть данные тестов, обновляем их
            logger.info(f"Обновляю существующие данные тестов для пользователя {message.from_user.id}")
        
        # Сохраняем в состояние метку о сохранении
        await state.update_data(**{f"{current_test}_saved": True})
        
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА сохранения промежуточного результата теста {current_test} для {message.from_user.id}: {e}")
        # Не останавливаем процесс, но логируем ошибку
    
    # Проверяем сохранение данных
    updated_data = await state.get_data()
    logger.info(f"Тест {current_test} завершен для {message.from_user.id}. Баллы: {total_score}")
    
    # ОТПРАВЛЯЕМ ПОДРОБНОЕ СООБЩЕНИЕ С РЕЗУЛЬТАТОМ (НЕ УДАЛЯЕМОЕ)
    result_message = f"""✅ <b>Тест {current_test.upper()} завершен!</b>

<b>Ваш результат:</b> {total_score} баллов

{result_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 <b>Важно:</b> Результат сохранен и будет учтен в итоговой оценке риска."""
    
    # Отправляем результат как ОТДЕЛЬНОЕ сообщение (не редактируем предыдущее)
    result_msg = await message.answer(result_message, parse_mode="HTML")
    
    # Небольшая пауза для чтения
    await asyncio.sleep(3)
    
    # Затем отправляем кнопку продолжения
    continue_text = "Нажмите кнопку ниже, чтобы вернуться к выбору тестов:"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➡️ Продолжить к выбору тестов", callback_data="continue_tests")]
    ])
    
    await message.answer(continue_text, reply_markup=keyboard, parse_mode="HTML")

@router.message(StateFilter(UserStates.survey_health))
async def handle_health_rating(message: Message, state: FSMContext):
    """Обработка оценки здоровья"""
    await log_user_interaction(message.from_user.id, "health_rating_entered", message.text)
    
    try:
        health_rating = int(message.text.strip())
        if health_rating < 0 or health_rating > 10:
            await message.answer("Пожалуйста, введите число от 0 до 10.")
            return
        
        await state.update_data(health_rating=health_rating)
        
        # Удаляем сообщение пользователя и вопрос
        try:
            await message.delete()
            # Пытаемся найти и удалить сообщение с вопросом (обычно предыдущее)
            if message.message_id > 1:
                try:
                    await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - 1)
                except:
                    pass
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщения: {e}")
        
        text = """<b>❓ Вопрос 9</b>
На ваш взгляд, какая из перечисленных причин чаще всего приводит к смерти людей в мире? 
(выберите 1 вариант ответа)"""
        
        keyboard = get_death_cause_keyboard()
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(UserStates.survey_death_cause)
        
    except ValueError:
        await message.answer("Пожалуйста, введите число от 0 до 10.")

async def complete_all_tests(message: Message, state: FSMContext):
    """ИСПРАВЛЕННОЕ завершение всех тестов с правильной обработкой None значений"""
    data = await state.get_data()
    
    # Собираем ВСЕ данные тестов из состояния с правильной обработкой None
    test_results = {}
    
    # HADS
    if 'hads_anxiety_score' in data and 'hads_depression_score' in data:
        test_results['hads_anxiety_score'] = data['hads_anxiety_score']
        test_results['hads_depression_score'] = data['hads_depression_score']
        test_results['hads_score'] = data.get('hads_score', data['hads_anxiety_score'] + data['hads_depression_score'])
        logger.info(f"HADS данные найдены: тревога={test_results['hads_anxiety_score']}, депрессия={test_results['hads_depression_score']}")
    
    # Burns
    if 'burns_score' in data:
        test_results['burns_score'] = data['burns_score']
        logger.info(f"Burns данные найдены: {test_results['burns_score']}")
    
    # ISI
    if 'isi_score' in data:
        test_results['isi_score'] = data['isi_score']
        logger.info(f"ISI данные найдены: {test_results['isi_score']}")
    
    # STOP-BANG
    if 'stop_bang_score' in data:
        test_results['stop_bang_score'] = data['stop_bang_score']
        logger.info(f"STOP-BANG данные найдены: {test_results['stop_bang_score']}")
    
    # ESS
    if 'ess_score' in data:
        test_results['ess_score'] = data['ess_score']
        logger.info(f"ESS данные найдены: {test_results['ess_score']}")
    
    # Fagerstrom - ИСПРАВЛЕННАЯ ЛОГИКА
    if 'fagerstrom_score' in data and data['fagerstrom_score'] is not None:
        test_results['fagerstrom_score'] = data['fagerstrom_score']
        logger.info(f"Fagerstrom данные найдены: {test_results['fagerstrom_score']}")
    elif 'fagerstrom_skipped' in data and data['fagerstrom_skipped']:
        test_results['fagerstrom_skipped'] = True
        logger.info(f"Fagerstrom пропущен")
    elif 'completed_fagerstrom' in data:
        # Если тест был завершен, но нет балла - значит пропущен
        test_results['fagerstrom_skipped'] = True
        logger.info(f"Fagerstrom завершен как пропущенный")
    else:
        # Если ничего не указано - автоматически пропускаем
        test_results['fagerstrom_skipped'] = True
        logger.info(f"Fagerstrom автоматически пропущен (не указан)")
    
    # AUDIT - ИСПРАВЛЕННАЯ ЛОГИКА
    if 'audit_score' in data and data['audit_score'] is not None:
        test_results['audit_score'] = data['audit_score']
        logger.info(f"AUDIT данные найдены: {test_results['audit_score']}")
    elif 'audit_skipped' in data and data['audit_skipped']:
        test_results['audit_skipped'] = True
        logger.info(f"AUDIT пропущен")
    elif 'completed_audit' in data:
        # Если тест был завершен, но нет балла - значит пропущен
        test_results['audit_skipped'] = True
        logger.info(f"AUDIT завершен как пропущенный")
    else:
        # Если ничего не указано - автоматически пропускаем
        test_results['audit_skipped'] = True
        logger.info(f"AUDIT автоматически пропущен (не указан)")
    
    # КРИТИЧЕСКАЯ ПРОВЕРКА: убеждаемся, что есть минимум данных
    required_tests = ['hads_anxiety_score', 'burns_score', 'isi_score', 'stop_bang_score', 'ess_score']
    missing_tests = [test for test in required_tests if test not in test_results]
    
    if missing_tests:
        missing_names = {
            'hads_anxiety_score': 'HADS (тревога и депрессия)',
            'burns_score': 'Тест Бернса',
            'isi_score': 'ISI (качество сна)',
            'stop_bang_score': 'STOP-BANG (апноэ сна)',
            'ess_score': 'ESS (дневная сонливость)'
        }
        
        missing_list = [missing_names.get(test, test) for test in missing_tests]
        
        error_text = f"""❌ <b>ОШИБКА: Не все тесты завершены</b>

<b>Отсутствуют данные тестов:</b>
• {chr(10).join(missing_list)}

<b>Имеющиеся данные:</b> {list(test_results.keys())}

Пожалуйста, завершите все обязательные тесты."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Вернуться к тестам", callback_data="back_to_tests")]
        ])
        
        await safe_edit_message(message, error_text, reply_markup=keyboard)
        return
    
    logger.info(f"Начинаю сохранение всех результатов тестов для пользователя {message.from_user.id}")
    logger.info(f"ФИНАЛЬНЫЕ данные для сохранения: {test_results}")
    
    await log_user_interaction(message.from_user.id, "all_tests_completed", f"Tests: {len(test_results)}")
    
    # КРИТИЧЕСКИ ВАЖНО: Сохраняем результаты тестов в базу данных
    try:
        logger.info(f"Сохраняю результаты тестов для пользователя {message.from_user.id}: {test_results}")
        save_result = await save_test_results(message.from_user.id, test_results)
        logger.info(f"Результаты тестов УСПЕШНО сохранены для пользователя {message.from_user.id}: {save_result}")
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА сохранения тестов для {message.from_user.id}: {e}")
        logger.error(f"Данные, которые пытались сохранить: {test_results}")
        
        # Показываем ошибку пользователю с ИСПРАВЛЕННЫМ отображением
        error_text = f"""❌ <b>ОШИБКА СОХРАНЕНИЯ ДАННЫХ</b>

Произошла ошибка при сохранении результатов тестов. 

<b>Ваши данные:</b>"""
        
        # Правильно отображаем данные
        for key, value in test_results.items():
            if value is not None:
                error_text += f"\n• {key}: {value}"
            else:
                error_text += f"\n• {key}: пропущен"
        
        error_text += f"""

<b>Техническая информация:</b>
Ошибка: {str(e)[:200]}

Попробуйте завершить тестирование еще раз."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="test_complete")],
            [InlineKeyboardButton(text="🔙 Вернуться к тестам", callback_data="back_to_tests")]
        ])
        
        await safe_edit_message(message, error_text, reply_markup=keyboard)
        return
    
    # Отмечаем пользователя как завершившего диагностику
    try:
        completion_result = await mark_user_completed(message.from_user.id)
        logger.info(f"Пользователь {message.from_user.id} отмечен как завершивший диагностику: {completion_result}")
    except Exception as e:
        logger.error(f"Ошибка отметки завершения для {message.from_user.id}: {e}")
    
    # Ждем немного для сохранения данных
    await asyncio.sleep(2)
    
    # Проверяем, что данные действительно сохранились
    try:
        saved_data = get_user_data(message.from_user.id)
        if saved_data and saved_data.get('tests'):
            logger.info(f"Подтверждение: данные сохранены в БД для пользователя {message.from_user.id}")
        else:
            logger.warning(f"ВНИМАНИЕ: данные могли не сохраниться для пользователя {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка проверки сохранения для {message.from_user.id}: {e}")
    
    # Генерируем и отправляем итоговую сводку
    try:
        summary = await generate_final_results_summary(message.from_user.id)
        await safe_edit_message(message, summary)
    except Exception as e:
        logger.error(f"Ошибка генерации сводки для {message.from_user.id}: {e}")
        # Fallback - отправляем базовое сообщение о завершении
        fallback_text = f"""🫀 <b>ОТЛИЧНО! Все тесты завершены!</b>

✅ Все ваши результаты сохранены в базе данных
📊 Данные обработаны и готовы для анализа
🎯 Вы полностью готовы к вебинару!

<b>Сохраненные данные:</b>"""
        
        # Правильно отображаем сохраненные данные
        for key, value in test_results.items():
            if value is not None:
                fallback_text += f"\n• {key}: {value}"
            elif key.endswith('_skipped'):
                fallback_text += f"\n• {key.replace('_skipped', '')}: пропущен"

        fallback_text += f"""

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

📎 Сейчас отправлю обещанные материалы..."""
        
        await safe_edit_message(message, fallback_text)
    
    # Отправляем файлы из папки materials
    await send_completion_materials(message)
    
    # Очищаем состояние
    await state.clear()
    
async def send_completion_materials(message: Message):
    """Отправка материалов после завершения диагностики"""
    import os
    from aiogram.types import FSInputFile
    
    materials_dir = "materials"
    
    # Проверяем существование папки materials
    if not os.path.exists(materials_dir):
        logger.warning("Папка materials не найдена")
        await message.answer("📁 Материалы готовятся к отправке...")
        return
    
    # Ищем файлы в папке materials
    files_to_send = []
    
    try:
        all_files = os.listdir(materials_dir)
        
        # Ищем нужные файлы по имени
        for filename in all_files:
            file_path = os.path.join(materials_dir, filename)
            if os.path.isfile(file_path):
                files_to_send.append(file_path)
        
        # Отправляем файлы
        if files_to_send:
            await message.answer("📎 Отправляю обещанные материалы:")
            
            for i, file_path in enumerate(files_to_send):
                try:
                    file_input = FSInputFile(file_path)
                    filename = os.path.basename(file_path)
                    
                    if "analyses" in filename.lower() or "анализ" in filename.lower():
                        caption = "📋 Список базовых анализов для подготовки к вебинару"
                    elif "checklist" in filename.lower() or "чеклист" in filename.lower() or "препарат" in filename.lower():
                        caption = "📌 Бонус: чек-лист «Препараты и методики, которые не лечат сердце и сосуды»"
                    elif "webinar" in filename.lower() or "вебинар" in filename.lower():
                        caption = "📋 Материалы к вебинару"
                    else:
                        caption = f"📄 Дополнительный материал: {filename}"
                    
                    await message.answer_document(file_input, caption=caption)
                    await asyncio.sleep(1) 
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки файла {file_path}: {e}")
                    await message.answer(f"❌ Не удалось отправить файл {os.path.basename(file_path)}")
        else:
            # Если файлов нет, отправляем текстовую информацию
            await send_text_materials(message)
            
    except Exception as e:
        logger.error(f"Ошибка при работе с папкой materials: {e}")
        await send_text_materials(message)

async def send_text_materials(message: Message):
    """Отправка текстовых материалов если файлы недоступны"""
    
    analyses_text = """📋 <b>СПИСОК БАЗОВЫХ АНАЛИЗОВ ДЛЯ КАРДИОЧЕКАПА</b>

🩸 <b>Обязательные анализы крови:</b>
• Общий анализ крови с лейкоформулой
• Биохимия: глюкоза, общий холестерин, ЛПВП, ЛПНП, триглицериды
• Креатинин, мочевина, АЛТ, АСТ
• С-реактивный белок (СРБ)
• Гликированный гемоглобин (HbA1c)

💉 <b>Дополнительные маркеры:</b>
• Гомоцистеин
• Витамин D (25-OH)
• ТТГ, Т4 свободный
• Ферритин

📏 <b>Инструментальная диагностика:</b>
• ЭКГ в покое
• Измерение АД дома (дневник 7 дней)
• Расчет ИМТ и окружности талии

🎯 <b>Рекомендации по подготовке:</b>
• Анализы сдавать натощак (8-12 часов голода)
• За 24 часа исключить алкоголь и тяжелые нагрузки
• Принимать лекарства по обычной схеме
• Результаты приносить на вебинар для разбора"""

    checklist_text = """📌 <b>ЧЕК-ЛИСТ: ПРЕПАРАТЫ, КОТОРЫЕ НЕ ЛЕЧАТ СЕРДЦЕ</b>

❌ <b>НЕЭФФЕКТИВНЫЕ "СЕРДЕЧНЫЕ" ПРЕПАРАТЫ:</b>

💊 <b>Метаболические препараты:</b>
• Милдронат, Предуктал — нет доказательств эффективности
• Рибоксин — устаревший препарат без пользы
• Кокарбоксилаза — не влияет на сердце

🧪 <b>"Сосудистые" препараты:</b>
• Актовегин, Солкосерил — не имеют доказанного эффекта
• Церебролизин — только для неврологии
• Винпоцетин — не улучшает кровообращение

💉 <b>"Витамины для сердца":</b>
• Панангин, Аспаркам — при нормальном калии бесполезны
• Витамины группы B в инъекциях — только при дефиците
• Антиоксидантные комплексы — могут быть вредны

⚠️ <b>ПОМНИТЕ:</b>
• Не тратьте деньги на неэффективные препараты
• Доказанные препараты: статины, ингибиторы АПФ, бета-блокаторы
• Любые назначения только после консультации врача
• Образ жизни важнее любых таблеток

✅ <b>ЧТО ДЕЙСТВИТЕЛЬНО РАБОТАЕТ:</b>
• Правильное питание
• Регулярная физическая активность  
• Отказ от курения
• Контроль давления и холестерина
• Управление стрессом"""

    # Отправляем материалы частями
    await message.answer("📎 Отправляю материалы в текстовом виде:")
    await asyncio.sleep(1)
    
    await message.answer(analyses_text, parse_mode="HTML")
    await asyncio.sleep(2)
    
    await message.answer(checklist_text, parse_mode="HTML")
    await asyncio.sleep(1)
    
    await message.answer("💡 <b>Совет:</b> Сохраните эти сообщения или сделайте скриншоты для удобства!", parse_mode="HTML")

# ============================================================================
# ГЕНЕРАЦИЯ ИТОГОВЫХ РЕЗУЛЬТАТОВ (с защитой)
# ============================================================================

async def generate_final_results_summary(telegram_id: int) -> str:
    """УЛУЧШЕННАЯ генерация итоговой сводки с детальной диагностикой"""
    
    try:
        logger.info(f"=== ГЕНЕРАЦИЯ ИТОГОВОЙ СВОДКИ ДЛЯ {telegram_id} ===")
        
        from database import get_user_data
        
        # Получаем данные пользователя с дополнительной проверкой
        data = get_user_data(telegram_id)
        
        logger.info(f"Данные из базы: {data is not None}")
        if data:
            logger.info(f"User данные: {data.get('user') is not None}")
            logger.info(f"Survey данные: {data.get('survey') is not None}")
            logger.info(f"Tests данные: {data.get('tests') is not None}")
        
        if not data:
            logger.error(f"❌ НЕТ ДАННЫХ ПОЛЬЗОВАТЕЛЯ для {telegram_id}")
            return """🫀 <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>




🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК

💡 Если проблема повторится, обратитесь к администратору."""
        
        user = data.get('user')
        survey = data.get('survey')
        tests = data.get('tests')
        
        # Проверяем наличие основных данных
        if not user:
            logger.error(f"❌ ОТСУТСТВУЮТ ДАННЫЕ ПОЛЬЗОВАТЕЛЯ в БД для {telegram_id}")
            return """🫀 <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>


🔄 <b>Что делать:</b>
• Попробуйте команду /restart
• Пройдите регистрацию заново
• Обратитесь к администратору

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК"""
        
        # Безопасно извлекаем данные
        name = getattr(user, 'name', None) or "Пользователь"
        
        # Данные опроса
        age = "не указан"
        gender = "не указан"
        if survey:
            age = getattr(survey, 'age', None) or "не указан"
            gender = getattr(survey, 'gender', None) or "не указан"
            logger.info(f"Survey данные: возраст={age}, пол={gender}")
        else:
            logger.warning(f"⚠️ ОТСУТСТВУЮТ ДАННЫЕ ОПРОСА для {telegram_id}")
        
        # Данные тестов
        if not tests:
            logger.error(f"❌ ОТСУТСТВУЮТ ДАННЫЕ ТЕСТОВ для {telegram_id}")
            return f"""🫀 <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>

👤 <b>Добро пожаловать, {name}!</b>

❌ <b>Внимание:</b> Результаты тестов не найдены в базе данных.

✅ <b>Но ваши ответы сохранены!</b>
📊 Подробные результаты будут доступны на вебинаре
🎯 Вы готовы к участию

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

💡 Если нужны результаты сейчас, попробуйте /status"""
        
        # Безопасно извлекаем результаты тестов
        risk_level = getattr(tests, 'overall_cv_risk_level', None) or "не определен"
        risk_score = getattr(tests, 'overall_cv_risk_score', None) or 0
        risk_factors_count = getattr(tests, 'risk_factors_count', None) or 0
        
        logger.info(f"Tests данные: риск={risk_level}, баллы={risk_score}, факторы={risk_factors_count}")
        
        hads_anxiety_score = getattr(tests, 'hads_anxiety_score', None) or 0
        hads_depression_score = getattr(tests, 'hads_depression_score', None) or 0
        hads_anxiety_level = getattr(tests, 'hads_anxiety_level', None) or "не определен"
        hads_depression_level = getattr(tests, 'hads_depression_level', None) or "не определен"
        
        burns_score = getattr(tests, 'burns_score', None) or 0
        burns_level = getattr(tests, 'burns_level', None) or "не определен"
        
        isi_score = getattr(tests, 'isi_score', None) or 0
        isi_level = getattr(tests, 'isi_level', None) or "не определен"
        
        stop_bang_score = getattr(tests, 'stop_bang_score', None) or 0
        stop_bang_risk = getattr(tests, 'stop_bang_risk', None) or "не определен"
        
        ess_score = getattr(tests, 'ess_score', None) or 0
        ess_level = getattr(tests, 'ess_level', None) or "не определен"
        
        fagerstrom_score = getattr(tests, 'fagerstrom_score', None)
        fagerstrom_level = getattr(tests, 'fagerstrom_level', None) or "не определен"
        fagerstrom_skipped = getattr(tests, 'fagerstrom_skipped', False)
        
        audit_score = getattr(tests, 'audit_score', None)
        audit_level = getattr(tests, 'audit_level', None) or "не определен"
        audit_skipped = getattr(tests, 'audit_skipped', False)
        
        logger.info(f"✅ Все данные извлечены успешно для {telegram_id}")
        
        # Формируем итоговую сводку
        summary = f"""🫀 <b>ИТОГИ ВАШЕЙ ДИАГНОСТИКИ</b>

👤 <b>{name}</b>
📊 Возраст: {age} лет | Пол: {gender}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 <b>ОБЩИЙ СЕРДЕЧНО-СОСУДИСТЫЙ РИСК</b>
{get_risk_emoji(risk_level)} <b>{risk_level}</b>
📈 Общий балл: {risk_score}
⚠️ Выявлено факторов риска: {risk_factors_count}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 <b>РЕЗУЛЬТАТЫ ПСИХОЛОГИЧЕСКИХ ТЕСТОВ</b>

🔹 <b>Тревога и депрессия (HADS):</b>
   • Тревога: {hads_anxiety_score} баллов ({hads_anxiety_level})
   • Депрессия: {hads_depression_score} баллов ({hads_depression_level})

🔹 <b>Эмоциональное состояние (Бернс):</b>
   • {burns_score} баллов ({burns_level})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

😴 <b>КАЧЕСТВО СНА И ОТДЫХА</b>

🔹 <b>Качество сна (ISI):</b>
   • {isi_score} баллов ({isi_level})

🔹 <b>Риск апноэ сна (STOP-BANG):</b>
   • {stop_bang_score} баллов ({stop_bang_risk} риск)

🔹 <b>Дневная сонливость (ESS):</b>
   • {ess_score} баллов ({ess_level})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🚭 <b>ОБРАЗ ЖИЗНИ</b>"""
        
        # Добавляем информацию о курении
        if fagerstrom_skipped:
            summary += "\n🔹 <b>Курение:</b> Не курит ✅"
        elif fagerstrom_score is not None:
            summary += f"\n🔹 <b>Никотиновая зависимость:</b> {fagerstrom_score} баллов ({fagerstrom_level})"
        
        # Добавляем информацию об алкоголе
        if audit_skipped:
            summary += "\n🔹 <b>Алкоголь:</b> Не употребляет ✅"
        elif audit_score is not None:
            summary += f"\n🔹 <b>Употребление алкоголя:</b> {audit_score} баллов ({audit_level})"
        
        summary += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💡 <b>ЧТО ЭТО ОЗНАЧАЕТ</b>

{get_risk_explanation(risk_level)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ <b>ВЫ ГОТОВЫ К ВЕБИНАРУ!</b>

Теперь у вас есть полная картина вашего состояния.

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

📊 <b>Данные сохранены:</b> ID пользователя {user.id}"""
        
        logger.info(f"✅ Итоговая сводка сгенерирована успешно для {telegram_id}")
        return summary
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА генерации итоговой сводки для {telegram_id}: {e}")
        return f"""🫀 <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>

✅ Ваши ответы успешно сохранены!
📊 Система обработала ваши данные
🎯 Вы готовы к вебинару!

🗓 <b>Вебинар:</b> 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

💡 Детальные результаты будут представлены на вебинаре

<b>ID сессии:</b> {telegram_id} (для технической поддержки)"""

def get_risk_emoji(risk_level: str) -> str:
    """Получить эмодзи для уровня риска"""
    risk_emojis = {
        "НИЗКИЙ": "🟢",
        "УМЕРЕННЫЙ": "🟡", 
        "ВЫСОКИЙ": "🟠",
        "ОЧЕНЬ ВЫСОКИЙ": "🔴"
    }
    return risk_emojis.get(risk_level, "⚪")

def get_risk_explanation(risk_level: str) -> str:
    """Получить объяснение уровня риска"""
    explanations = {
        "НИЗКИЙ": """🟢 <b>Низкий риск</b> означает, что ваша вероятность развития сердечно-сосудистых заболеваний в ближайшие годы минимальна. Это отличный результат! Продолжайте следить за своим здоровьем.""",
        
        "УМЕРЕННЫЙ": """🟡 <b>Умеренный риск</b> говорит о том, что у вас есть несколько факторов, которые могут повлиять на здоровье сердца. Это сигнал для более внимательного отношения к профилактике.""",
        
        "ВЫСОКИЙ": """🟠 <b>Высокий риск</b> указывает на наличие значимых факторов, которые существенно повышают вероятность сердечно-сосудистых событий. Важно принять меры для снижения рисков.""",
        
        "ОЧЕНЬ ВЫСОКИЙ": """🔴 <b>Очень высокий риск</b> означает, что у вас есть множество факторов, серьезно влияющих на здоровье сердца и сосудов. Требуется комплексный подход к профилактике."""
    }
    return explanations.get(risk_level, "⚪ Уровень риска требует дополнительной оценки.")

@router.callback_query(F.data == "continue_tests")
async def continue_to_test_menu(callback: CallbackQuery, state: FSMContext):
    """ИСПРАВЛЕННЫЙ обработчик продолжения к меню тестов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "continue_to_test_menu")
    
    # Получаем данные из состояния для проверки пройденных тестов
    data = await state.get_data()
    
    text = """📝 <b>ВЫБОР ТЕСТОВ</b>

Выберите следующий тест для прохождения или завершите тестирование:"""
    
    # Генерируем клавиатуру с учетом пройденных тестов
    keyboard = get_test_selection_keyboard(data)
    
    try:
        # Пытаемся отредактировать сообщение
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
    except Exception as e:
        # Если не получается отредактировать, отправляем новое
        logger.warning(f"Не удалось отредактировать сообщение, отправляю новое: {e}")
        await callback.message.answer(text, reply_markup=keyboard, parse_mode="HTML")
    
    # Устанавливаем состояние выбора тестов
    await state.set_state(UserStates.test_selection)
# ============================================================================
# ОБРАБОТЧИК ДЛЯ НЕИЗВЕСТНЫХ СООБЩЕНИЙ (С ЗАЩИТОЙ)
# ============================================================================
@router.callback_query(F.data == "test_check_completion")
async def check_test_completion(callback: CallbackQuery, state: FSMContext):
    """Проверка готовности к завершению тестирования"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    
    # Подсчитываем завершенные тесты
    completed_tests = []
    
    # Проверяем каждый тест
    test_checks = [
        ("hads_anxiety_score", "HADS (тревога и депрессия)"),
        ("burns_score", "Тест Бернса"),
        ("isi_score", "ISI (качество сна)"),
        ("stop_bang_score", "STOP-BANG (апноэ сна)"),
        ("ess_score", "ESS (дневная сонливость)"),
        ("fagerstrom_score", "Фагерстрем (курение)"),
        ("audit_score", "AUDIT (алкоголь)")
    ]
    
    missing_tests = []
    
    for test_key, test_name in test_checks:
        if test_key in data or f"{test_key.split('_')[0]}_skipped" in data:
            completed_tests.append(test_name)
        else:
            missing_tests.append(test_name)
    
    if len(missing_tests) == 0:
        # Все тесты пройдены
        text = f"""✅ <b>Все тесты завершены!</b>

Пройдено тестов: {len(completed_tests)}/7

Готовы завершить диагностику?"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить диагностику", callback_data="test_complete")],
            [InlineKeyboardButton(text="🔙 Назад к тестам", callback_data="back_to_tests")]
        ])
        
    elif len(missing_tests) <= 2:
        # Почти все пройдены
        text = f"""📊 <b>Прогресс тестирования</b>

✅ Завершено: {len(completed_tests)}/7 тестов
❌ Осталось: {len(missing_tests)} тестов

<b>Не пройдены:</b>
• {chr(10).join(missing_tests)}

Можете завершить сейчас или пройти оставшиеся тесты."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Завершить сейчас", callback_data="test_complete")],
            [InlineKeyboardButton(text="📝 Пройти оставшиеся", callback_data="back_to_tests")]
        ])
        
    else:
        # Много тестов не пройдено
        text = f"""⚠️ <b>Тестирование не завершено</b>

✅ Завершено: {len(completed_tests)}/7 тестов
❌ Осталось: {len(missing_tests)} тестов

<b>Не пройдены:</b>
• {chr(10).join(missing_tests)}

Рекомендуется завершить все тесты для точной диагностики."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Продолжить тестирование", callback_data="back_to_tests")]
        ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

@router.callback_query(F.data == "back_to_tests")
async def back_to_test_selection(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору тестов"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    
    text = "Выберите тест для прохождения:"
    keyboard = get_test_selection_keyboard(data)
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.test_selection)
    
@router.message()
async def handle_unknown_message(message: Message, state: FSMContext):
    """Защищенный обработчик неизвестных сообщений и команд"""
    
    # ВАЖНО: Пропускаем административные команды
    if message.text and message.text.strip().lower().startswith('/admin'):
        # Не обрабатываем /admin здесь - пусть обрабатывает admin router
        return
    
    # Также пропускаем другие админские команды
    admin_commands = ['/stats', '/export', '/broadcast']
    if message.text:
        text = message.text.strip().lower()
        for cmd in admin_commands:
            if text.startswith(cmd):
                return
    
    await log_user_interaction(message.from_user.id, "unknown_message", message.text)
    
    # Проверяем, в каком состоянии пользователь
    current_state = await state.get_state()
    user_completed = check_user_completed(message.from_user.id)
    
    if current_state and ("survey" in current_state or "test" in current_state):
        # Пользователь в процессе диагностики - подсказываем
        text = """💡 <b>Используйте кнопки для ответов</b>

Для прохождения диагностики используйте кнопки под сообщениями, а не текстовые команды.

🔄 Если возникли проблемы:
/restart - Начать диагностику заново
/help - Получить помощь"""
        
    elif user_completed:
        # Пользователь завершил диагностику
        text = """✅ <b>Диагностика завершена!</b>

Вы уже прошли полную диагностику и готовы к вебинару.

📋 <b>Полезные команды:</b>
/start - Посмотреть результаты
/status - Проверить статус
/help - Получить помощь

🗓 <b>Напоминание:</b> Вебинар 3 августа в 12:00 МСК"""
        
    else:
        # Пользователь еще не начал или не завершил
        text = f"""👋 Привет! Я вижу, вы написали: "{message.text}"

🤖 Я бот для подготовки к вебинару <b>"Умный кардиочекап"</b>.

🚀 <b>Чтобы начать диагностику:</b>
/start - Начать диагностику

📋 <b>Другие команды:</b>
/help - Подробная помощь
/status - Проверить прогресс

💡 Диагностика займет всего 15-20 минут и поможет получить максимум пользы от вебинара!"""
    
    await message.answer(text, parse_mode="HTML")

# ============================================================================
# ЭКСПОРТ MIDDLEWARE И РОУТЕРА
# ============================================================================

# Экспортируем middleware для использования в main.py
__all__ = ['state_protection', 'router']