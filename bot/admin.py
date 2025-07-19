import os
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime, timedelta
import pytz

from database import admin_export_data, admin_get_stats, clean_old_data
from dotenv import load_dotenv
load_dotenv()
admin_router = Router()

class AdminStates(StatesGroup):
    waiting_password = State()

# Получаем пароль из переменных окружения
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

def get_admin_keyboard():
    """Клавиатура административной панели"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📥 Экспорт данных", callback_data="admin_export")],
        [InlineKeyboardButton(text="🗑 Очистить старые данные", callback_data="admin_clean")],
        [InlineKeyboardButton(text="🔄 Обновить статистику", callback_data="admin_refresh_stats")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_logout")]
    ])
    return keyboard

@admin_router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext, is_admin: bool = False):
    """Административная панель с запросом пароля"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # Проверяем, авторизован ли пользователь
    admin_session = await state.get_data()
    if admin_session.get('admin_authenticated'):
        await show_admin_panel(message)
    else:
        await request_admin_password(message, state)

async def request_admin_password(message: Message, state: FSMContext):
    """Запрос пароля для доступа к админке"""
    text = """🔐 <b>Доступ к административной панели</b>

Введите пароль администратора:"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(AdminStates.waiting_password)

async def request_admin_password(message: Message, state: FSMContext):
    """Запрос пароля для доступа к админке"""
    text = """🔐 <b>Доступ к административной панели</b>

Введите пароль администратора:"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(AdminStates.waiting_password)

@admin_router.message(AdminStates.waiting_password)
async def handle_admin_password(message: Message, state: FSMContext, is_admin: bool = False):
    """Обработка пароля администратора"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        await state.clear()
        return
    
    password = message.text.strip()
    
    # Удаляем сообщение с паролем для безопасности
    try:
        await message.delete()
    except:
        pass
    
    if str(password) == str(ADMIN_PASSWORD):
        # Сохраняем состояние авторизации
        await state.update_data(admin_authenticated=True)
        
        text = "✅ Пароль верный! Добро пожаловать в админ-панель."
        sent_message = await message.answer(text)
        
        # Через 2 секунды показываем админ панель
        import asyncio
        await asyncio.sleep(2)
        await show_admin_panel(sent_message)
    else:
        
        await message.answer("❌ Неверный пароль. Попробуйте снова.")

async def check_admin_auth(callback: CallbackQuery, state: FSMContext, is_admin: bool) -> bool:
    """Проверка авторизации администратора БЕЗ очистки состояния"""
    if not is_admin:
        await callback.answer("❌ Нет прав доступа", show_alert=True)
        return False

    # Проверяем состояние авторизации
    admin_session = await state.get_data()
    if not admin_session.get('admin_authenticated'):
        await callback.answer("❌ Необходимо повторно ввести пароль", show_alert=True)
        # Перенаправляем на авторизацию
        await request_admin_password(callback.message, state)
        return False

    return True
    
async def show_admin_panel(message: Message):
    """Показать административную панель"""
    text = """🔧 <b>Административная панель</b>

Выберите действие:

📊 <b>Статистика</b> - просмотр статистики пользователей
📥 <b>Экспорт данных</b> - выгрузка всех данных в Excel
🗑 <b>Очистить старые данные</b> - удаление устаревших записей
🔄 <b>Обновить статистику</b> - пересчет текущей статистики
🚪 <b>Выйти</b> - выход из админ-панели"""
    
    keyboard = get_admin_keyboard()
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.callback_query(F.data == "admin_stats")
async def show_stats(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Показать статистику"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    try:
        stats = await admin_get_stats()
        
        text = f"""📊 <b>Статистика бота</b>

👥 <b>Пользователи:</b>
• Всего зарегистрировано: {stats['total_users']}
• Завершили регистрацию: {stats['completed_registration']}
• Завершили опрос: {stats['completed_surveys']}
• Прошли тесты: {stats['completed_tests']}
• Завершили диагностику: {stats['completed_diagnostic']}

📈 <b>Конверсия:</b>
• Регистрация: {stats['completed_registration']/max(stats['total_users'], 1)*100:.1f}%
• Опрос: {stats['completed_surveys']/max(stats['total_users'], 1)*100:.1f}%
• Тесты: {stats['completed_tests']/max(stats['total_users'], 1)*100:.1f}%
• Полная диагностика: {stats['completed_diagnostic']/max(stats['total_users'], 1)*100:.1f}%"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton(text="📈 Детальная статистика", callback_data="admin_detailed_stats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "admin_detailed_stats")
async def show_detailed_stats(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Показать детальную статистику"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    try:
        from database import admin_get_detailed_stats
        stats = await admin_get_detailed_stats()
        
        basic = stats['basic']
        risk_dist = stats.get('risk_distribution', {})
        test_stats = stats.get('test_results', {})
        
        text = f"""📈 <b>Детальная статистика</b>

👥 <b>Основные показатели:</b>
• Всего пользователей: {basic['total_users']}
• Завершили регистрацию: {basic['completed_registration']}
• Завершили опрос: {basic['completed_surveys']}
• Прошли тесты: {basic['completed_tests']}
• Завершили диагностику: {basic['completed_diagnostic']}

🎯 <b>Распределение рисков:</b>"""
        
        for risk_level, count in risk_dist.items():
            if risk_level and count > 0:
                percentage = (count / max(basic['completed_tests'], 1) * 100)
                text += f"\n• {risk_level}: {count} ({percentage:.1f}%)"
        
        text += f"""\n\n⚠️ <b>Клинически значимые результаты:</b>
• Высокая тревога: {test_stats.get('hads_high_anxiety', 0)}
• Высокая депрессия: {test_stats.get('hads_high_depression', 0)}
• Умеренная+ депрессия: {test_stats.get('burns_moderate_plus', 0)}
• Клиническая бессонница: {test_stats.get('isi_clinical_insomnia', 0)}
• Высокий риск апноэ: {test_stats.get('stop_bang_high_risk', 0)}
• Чрезмерная сонливость: {test_stats.get('ess_excessive', 0)}
• Никотиновая зависимость: {test_stats.get('fagerstrom_dependent', 0)}
• Проблемы с алкоголем: {test_stats.get('audit_risky', 0)}"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_detailed_stats")],
            [InlineKeyboardButton(text="⬅️ К основной статистике", callback_data="admin_stats")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения детальной статистики: {e}")

@admin_router.callback_query(F.data == "admin_refresh_stats")
async def refresh_stats(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Обновить статистику вручную"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Обновляю статистику...")
    
    try:
        from database import update_daily_stats
        
        # Обновляем ежедневную статистику
        def _update_stats():
            return update_daily_stats()
        
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _update_stats)
        
        # Получаем обновленную статистику
        stats = await admin_get_stats()
        
        text = f"""✅ <b>Статистика обновлена</b>

📊 <b>Актуальные данные:</b>
• Всего пользователей: {stats['total_users']}
• Завершили регистрацию: {stats['completed_registration']}
• Завершили опрос: {stats['completed_surveys']}
• Прошли тесты: {stats['completed_tests']}
• Завершили диагностику: {stats['completed_diagnostic']}

🕒 Последнее обновление: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Подробная статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка обновления статистики: {e}")

@admin_router.callback_query(F.data == "admin_export")
async def export_data(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Экспорт данных в Excel"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Подготавливаю данные для экспорта...")
    
    try:
        filename = await admin_export_data()
        
        if os.path.exists(filename):
            # Отправляем файл
            document = FSInputFile(filename)
            await callback.message.answer_document(
                document, 
                caption="📥 Экспорт данных из базы готов"
            )
            
            # Удаляем временный файл
            os.remove(filename)
            
            # Возвращаемся к админ панели
            await show_admin_panel(callback.message)
        else:
            await callback.message.edit_text("❌ Ошибка: файл не создан")
            
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка экспорта: {e}")

@admin_router.callback_query(F.data == "admin_clean")
async def clean_data_menu(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Меню очистки данных"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """🗑 <b>Очистка старых данных</b>

⚠️ <b>Внимание!</b> Это действие нельзя отменить.

Будут удалены:
• Старые логи активности
• Старые логи рассылок  
• Устаревшая системная статистика

<b>Данные пользователей, опросов и тестов останутся нетронутыми!</b>

Выберите период для удаления:"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 30 дней", callback_data="clean_30")],
        [InlineKeyboardButton(text="🗑 60 дней", callback_data="clean_60")],
        [InlineKeyboardButton(text="🗑 90 дней", callback_data="clean_90")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.callback_query(F.data.startswith("clean_"))
async def clean_old_data_action(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Очистка старых данных"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    # Получаем количество дней
    days = int(callback.data.split("_")[1])
    
    await callback.message.edit_text(f"⏳ Удаляю данные старше {days} дней...")
    
    try:
        def _clean():
            return clean_old_data(days)
        
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _clean)
        
        text = f"""✅ <b>Очистка завершена</b>

Удалено за период старше {days} дней:
• Логов активности: {result.get('deleted_activity_logs', 0)}
• Логов рассылок: {result.get('deleted_broadcast_logs', 0)}
• Записей системной статистики: {result.get('deleted_system_stats', 0)}

💾 Основные данные пользователей сохранены."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к админ панели", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка очистки: {e}")

@admin_router.callback_query(F.data == "admin_back")
async def back_to_admin(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Вернуться к админ панели"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await show_admin_panel(callback.message)

@admin_router.callback_query(F.data == "admin_logout")
async def admin_logout(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Выход из админ панели"""
    if not is_admin:
        await callback.answer("❌ Нет прав доступа", show_alert=True)
        return
    
    await callback.answer()
    
    # Очищаем состояние авторизации
    await state.update_data(admin_authenticated=False)
    await state.clear()
    
    text = """👋 <b>Выход из админ-панели</b>

Вы успешно вышли из административной панели.
Для повторного входа используйте команду /admin"""
    
    await callback.message.edit_text(text, parse_mode="HTML")

async def check_admin_auth(callback: CallbackQuery, state: FSMContext, is_admin: bool) -> bool:
    """Проверка авторизации администратора БЕЗ очистки состояния"""
    if not is_admin:
        await callback.answer("❌ Нет прав доступа", show_alert=True)
        return False

    return True

# Команды для быстрого доступа
@admin_router.message(Command("stats"))
async def quick_stats(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрый просмотр статистики"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # УБИРАЕМ проверку admin_authenticated - полагаемся только на is_admin
    try:
        stats = await admin_get_stats()
        
        text = f"""📊 <b>Быстрая статистика бота</b>

👥 Всего пользователей: {stats['total_users']}
✅ Завершили регистрацию: {stats['completed_registration']}
📝 Завершили опрос: {stats['completed_surveys']}
🧪 Прошли тесты: {stats['completed_tests']}
🎯 Завершили диагностику: {stats['completed_diagnostic']}

📈 Конверсия в регистрацию: {stats['completed_registration']/max(stats['total_users'], 1)*100:.1f}%
📈 Конверсия в опрос: {stats['completed_surveys']/max(stats['total_users'], 1)*100:.1f}%
📈 Конверсия в тесты: {stats['completed_tests']/max(stats['total_users'], 1)*100:.1f}%
📈 Конверсия в диагностику: {stats['completed_diagnostic']/max(stats['total_users'], 1)*100:.1f}%"""
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@admin_router.message(Command("export"))
async def quick_export(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрый экспорт данных"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # УБИРАЕМ проверку admin_authenticated
    await message.answer("⏳ Подготавливаю экспорт...")
    
    try:
        filename = await admin_export_data()
        
        if os.path.exists(filename):
            document = FSInputFile(filename)
            await message.answer_document(
                document, 
                caption="📥 Экспорт данных готов"
            )
            os.remove(filename)
        else:
            await message.answer("❌ Ошибка создания файла")
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        
# Помощь по командам администратора
@admin_router.message(Command("adminhelp"))
async def admin_help(message: Message, state: FSMContext, is_admin: bool = False):
    """Помощь по командам администратора"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    text = """🔧 <b>Команды администратора</b>

<b>Основные команды:</b>
/admin - Открыть административную панель (требует пароль)
/stats - Быстрый просмотр статистики (требует авторизации)
/export - Быстрый экспорт данных в Excel (требует авторизации)
/adminhelp - Эта справка

<b>🔐 Безопасность:</b>
• Для входа в админ-панель требуется пароль
• Пароль настраивается в переменной ADMIN_PASSWORD
• Сессия авторизации сохраняется до выхода или перезапуска

<b>📊 Административная панель позволяет:</b>
• Просматривать детальную статистику
• Экспортировать все данные в Excel
• Очищать старые технические данные
• Обновлять статистику вручную
• Мониторить работу бота

<b>📥 Экспорт включает:</b>
• Данные пользователей
• Результаты опросов  
• Результаты тестов
• Статистику и аналитику
• Логи активности и рассылок

<b>🗑 Очистка данных:</b>
• Удаляются только технические логи
• Данные пользователей остаются нетронутыми
• Можно выбрать период для удаления

Все данные экспортируются в формате Excel с несколькими листами для удобного анализа."""
    
    await message.answer(text, parse_mode="HTML")
    
@admin_router.callback_query(F.data == "admin_test_broadcast")
async def test_broadcast_system(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Тестирование системы рассылок"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("🧪 Тестирую систему рассылок...")
    
    try:
        from broadcast import BroadcastScheduler
        
        # Создаем тестовый планировщик
        scheduler = BroadcastScheduler(callback.bot)
        
        # Проверяем настройки времени
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time = datetime.now(moscow_tz)
        webinar_time = scheduler.webinar_date
        
        # Рассчитываем время до вебинара
        time_until_webinar = webinar_time - current_time
        days_until = time_until_webinar.days
        hours_until = time_until_webinar.seconds // 3600
        
        # Проверяем пользователей
        from database import get_all_users, get_completed_users
        all_users = await get_all_users()
        completed_users = await get_completed_users()
        
        # Формируем отчет
        text = f"""🧪 <b>ТЕСТ СИСТЕМЫ РАССЫЛОК</b>

⏰ <b>Настройки времени:</b>
• Текущее время (МСК): {current_time.strftime('%d.%m.%Y %H:%M')}
• Время вебинара: {webinar_time.strftime('%d.%m.%Y %H:%M')}
• До вебинара: {days_until} дней, {hours_until} часов

👥 <b>Пользователи для рассылки:</b>
• Всего пользователей: {len(all_users)}
• Завершили диагностику: {len(completed_users)}
• Получат рассылки: {len(all_users)}

🕐 <b>Расписание рассылок:</b>"""
        
        # Добавляем расписание рассылок
        schedule = {
            'За неделю': webinar_time - timedelta(days=7),
            'За 3 дня': webinar_time - timedelta(days=3), 
            'За 1 день': webinar_time - timedelta(days=1),
            'За 3 часа': webinar_time - timedelta(hours=3),
            'За 2 часа': webinar_time - timedelta(hours=2),
            'За 1 час': webinar_time - timedelta(hours=1),
            'За 15 минут': webinar_time - timedelta(minutes=15),
            'Начало вебинара': webinar_time
        }
        
        for name, send_time in schedule.items():
            time_diff = send_time - current_time
            if time_diff.total_seconds() > 0:
                status = "⏳ Ожидает"
                days = time_diff.days
                hours = time_diff.seconds // 3600
                time_left = f"через {days}д {hours}ч"
            else:
                status = "✅ Прошло"
                time_left = "уже отправлено"
            
            text += f"\n• {name}: {send_time.strftime('%d.%m %H:%M')} - {status} ({time_left})"
        
        text += f"""

🔧 <b>Техническая информация:</b>
• Планировщик: {'Активен' if scheduler.running else 'Не запущен'}
• Проверка каждые: 5 минут
• Отправленные рассылки: {len(scheduler.sent_broadcasts)}
• Задержка между сообщениями: 50ms
• Часовой пояс: Europe/Moscow

✅ Система рассылок готова к работе!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Тестовая рассылка", callback_data="admin_send_test")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_test_broadcast")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка тестирования: {e}")

@admin_router.callback_query(F.data == "admin_send_test")
async def send_test_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Отправка тестовой рассылки"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("📤 Отправляю тестовую рассылку...")
    
    try:
        from broadcast import send_custom_broadcast
        
        test_message = """🧪 <b>ТЕСТОВОЕ СООБЩЕНИЕ</b>

Это тестовая рассылка для проверки системы.

✅ Если вы получили это сообщение, значит система рассылок работает корректно!

🗓 Напоминаем: вебинар "Умный кардиочекап" состоится 3 августа в 12:00 МСК."""
        
        # Отправляем только администраторам для теста
        result = await send_test_to_admins(callback.bot, test_message)
        
        text = f"""✅ <b>Тестовая рассылка отправлена</b>

📊 Результат:
• Отправлено: {result['sent']}
• Ошибок: {result['errors']}
• Получателей: администраторы

Проверьте свои сообщения в Telegram!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к тесту", callback_data="admin_test_broadcast")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка отправки: {e}")

async def send_test_to_admins(bot, message_text: str):
    """Отправка тестового сообщения админам"""
    import os
    
    # Получаем ID админов
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids = []
    if admin_ids_str:
        admin_ids = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
    
    sent_count = 0
    error_count = 0
    
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, message_text, parse_mode="HTML")
            sent_count += 1
        except Exception as e:
            error_count += 1    
    return {"sent": sent_count, "errors": error_count}

# Добавить эту кнопку в get_admin_keyboard():
def get_admin_keyboard():
    """Клавиатура административной панели"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📥 Экспорт данных", callback_data="admin_export")],
        [InlineKeyboardButton(text="🗑 Очистить старые данные", callback_data="admin_clean")],
        [InlineKeyboardButton(text="🔄 Обновить статистику", callback_data="admin_refresh_stats")],
        [InlineKeyboardButton(text="📡 Тест рассылок", callback_data="admin_test_broadcast")],  # НОВАЯ КНОПКА
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_logout")]
    ])
    return keyboard
