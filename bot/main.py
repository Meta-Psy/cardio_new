import asyncio
import logging
import os
import sys
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.session.aiohttp import AiohttpSession
import aiohttp

from handlers import router, state_protection
from database import init_db, ensure_database_exists, fix_incomplete_records, validate_data_integrity
from admin import admin_router
from broadcast import BroadcastScheduler
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования без эмодзи для совместимости с Windows
def setup_logging():
    """Настройка логирования с учетом кодировки системы"""
    
    # Определяем формат без эмодзи
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Настройка для файла
    file_handler = logging.FileHandler('bot.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Настройка для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Настройка корневого логгера
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler],
        format=log_format
    )

setup_logging()
logger = logging.getLogger(__name__)

# Получаем переменные из .env
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
PROXY_URL = os.getenv("PROXY_URL") 

# ID администраторов
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = []
if ADMIN_IDS_STR:
    try:
        ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]
    except ValueError:
        logger.warning("Некорректный формат ADMIN_IDS в .env файле")

class AdminMiddleware:
    """Middleware для проверки прав администратора"""
    
    def __init__(self, admin_ids):
        self.admin_ids = admin_ids
    
    async def __call__(self, handler, event, data):
        if hasattr(event, 'from_user') and event.from_user:
            data['is_admin'] = event.from_user.id in self.admin_ids
        else:
            data['is_admin'] = False
        return await handler(event, data)

async def create_bot_with_proxy():
    """Создание бота с прокси (если нужно)"""
    
    if not PROXY_URL:
        # Если прокси не нужен, создаем обычного бота
        return Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
        )
    
    try:
        logger.info(f"Настройка бота с прокси: {PROXY_URL}")
        
        # Создаем connector для прокси
        connector = aiohttp.TCPConnector()
        
        # Создаем timeout
        timeout = aiohttp.ClientTimeout(total=60, connect=15)
        
        # Создаем сессию с прокси
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
        
        # Оборачиваем в AiohttpSession
        aiogram_session = AiohttpSession(session)
        
        # Создаем бота
        bot = Bot(
            token=BOT_TOKEN,
            session=aiogram_session,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
        )
        
        return bot
        
    except Exception as e:
        logger.error(f"Ошибка создания бота с прокси: {e}")
        logger.info("Переключаюсь на обычного бота без прокси")
        
        # Fallback на обычного бота
        return Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
        )

async def create_bot_with_retry():
    """Создание бота с настройками для retry"""
    
    try:
        # Выбираем тип создания бота в зависимости от настроек
        if PROXY_URL:
            return await create_bot_with_proxy()
        else:
            # Создаем простого бота без дополнительных настроек
            bot = Bot(
                token=BOT_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
            )
            return bot
    
    except Exception as e:
        logger.error(f"Ошибка создания бота: {e}")
        # Fallback - создаем простого бота
        return Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
        )
        
async def setup_commands(bot):
    """Настройка команд бота"""
    try:
        from aiogram.types import BotCommand, BotCommandScopeDefault
        
        commands = [
            BotCommand(command="start", description="🚀 Начать диагностику"),
            BotCommand(command="help", description="❓ Помощь и инструкции"),
            BotCommand(command="status", description="📊 Мой статус прохождения"),
            BotCommand(command="restart", description="🔄 Начать заново"),
        ]
        
        await bot.set_my_commands(commands, BotCommandScopeDefault())
        logger.info("УСПЕХ: Команды бота настроены")
    except Exception as e:
        logger.warning(f"Не удалось настроить команды: {e}")

async def test_bot_connection(bot: Bot, max_retries: int = 3):
    """Тестирование подключения к Telegram с retry"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Попытка подключения к Telegram ({attempt + 1}/{max_retries})...")
            me = await bot.get_me()
            logger.info(f"УСПЕХ: Подключение к Telegram. Бот: @{me.username}")
            return True
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} неудачна: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"Жду {wait_time} секунд перед следующей попыткой...")
                await asyncio.sleep(wait_time)
    
    logger.error("Все попытки подключения исчерпаны!")
    return False

async def startup_checks():
    """Проверки при запуске бота"""
    logger.info("Выполняю проверки при запуске...")
    
    # Проверка переменных окружения
    missing_vars = []
    if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
        missing_vars.append("BOT_TOKEN")
    if not ADMIN_PASSWORD or ADMIN_PASSWORD == "your_secure_admin_password_here":
        missing_vars.append("ADMIN_PASSWORD")
    if not ADMIN_IDS:
        missing_vars.append("ADMIN_IDS")
    
    if missing_vars:
        logger.error(f"ОШИБКА: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        logger.error("Создайте файл .env и укажите недостающие переменные")
        return False
    
    # Проверка базы данных
    try:
        if not ensure_database_exists():
            logger.error("ОШИБКА: Не удалось инициализировать базу данных!")
            return False
        
        init_db()
        logger.info("УСПЕХ: База данных инициализирована")
        
        # Исправляем неполные записи
        try:
            fixed_data = fix_incomplete_records()
            if fixed_data['fixed_records'] > 0:
                logger.info(f"ИСПРАВЛЕНО: {fixed_data['fixed_records']} неполных записей")
        except Exception as e:
            logger.warning(f"Не удалось исправить записи: {e}")
        
        # Проверяем целостность данных
        try:
            integrity_check = validate_data_integrity()
            if not integrity_check['healthy']:
                logger.warning(f"ВНИМАНИЕ: Обнаружены проблемы с данными: {'; '.join(integrity_check['issues'])}")
            else:
                logger.info("УСПЕХ: Целостность данных проверена")
        except Exception as e:
            logger.warning(f"Не удалось проверить целостность: {e}")
            
    except Exception as e:
        logger.error(f"ОШИБКА при работе с базой данных: {e}")
        return False
    
    # Создаем папку для материалов
    try:
        os.makedirs("materials", exist_ok=True)
        logger.info("УСПЕХ: Папка materials создана/проверена")
    except Exception as e:
        logger.warning(f"Не удалось создать папку materials: {e}")
    
    return True

async def main():
    """Основная функция запуска бота с интеграцией защиты состояний"""
    
    logger.info("Запуск бота кардиочекапа с защитой от зацикливания...")
    
    # Выполняем проверки при запуске
    if not await startup_checks():
        logger.error("КРИТИЧЕСКАЯ ОШИБКА: Проверки при запуске не пройдены. Завершение работы.")
        return
    
    # Создание бота
    bot = None
    try:
        bot = await create_bot_with_retry()
        logger.info("УСПЕХ: Бот создан")
        
        # Тестируем подключение
        if not await test_bot_connection(bot):
            logger.error("ОШИБКА: Не удалось подключиться к Telegram API")
            logger.error("Проверьте интернет-подключение или используйте VPN")
            return
        
        # Настройка команд
        await setup_commands(bot)
        
        # Создаем диспетчер
        storage = MemoryStorage()
        dp = Dispatcher(storage=storage)
        
        # ============================================================================
        # ИНТЕГРАЦИЯ MIDDLEWARE ДЛЯ ЗАЩИТЫ ОТ ЗАЦИКЛИВАНИЯ
        # ============================================================================
        
        # КРИТИЧЕСКИ ВАЖНО: Регистрируем middleware защиты состояний ПЕРВЫМ
        # Это обеспечивает обработку всех запросов через защиту от дублирования
        logger.info("Регистрация middleware защиты состояний...")
        dp.message.middleware(state_protection)
        dp.callback_query.middleware(state_protection)
        logger.info("УСПЕХ: Middleware защиты состояний зарегистрирован")
        
        # Регистрация административного middleware
        admin_middleware = AdminMiddleware(ADMIN_IDS)
        dp.message.middleware(admin_middleware)
        dp.callback_query.middleware(admin_middleware)
        logger.info("УСПЕХ: Административный middleware зарегистрирован")
        
        # Регистрация роутеров (ПОРЯДОК ВАЖЕН!)
        if ADMIN_IDS:
            dp.include_router(admin_router)  # ПЕРВЫМ - админский роутер
            logger.info("УСПЕХ: Административный роутер подключен")
        
        dp.include_router(router)  # ВТОРЫМ - основной роутер
        
        logger.info("УСПЕХ: Диспетчер настроен с защитой состояний")
        
    except Exception as e:
        logger.error(f"ОШИБКА создания бота: {e}")
        return
    
    # Запуск планировщика рассылок
    scheduler = None
    scheduler_task = None
    
    try:
        if ADMIN_IDS:
            scheduler = BroadcastScheduler(bot)
            logger.info("УСПЕХ: Планировщик рассылок создан")
    except Exception as e:
        logger.warning(f"Ошибка создания планировщика: {e}")
    
    # Выводим информацию о конфигурации
    logger.info("Конфигурация бота:")
    logger.info(f"   Администраторы: {len(ADMIN_IDS)} пользователей")
    logger.info(f"   Пароль админки: {'установлен' if ADMIN_PASSWORD else 'НЕ УСТАНОВЛЕН'}")
    logger.info(f"   Планировщик рассылок: {'включен' if scheduler else 'отключен'}")
    logger.info(f"   Прокси: {'используется' if PROXY_URL else 'не используется'}")
    logger.info(f"   Защита состояний: ВКЛЮЧЕНА")
    logger.info(f"   Middleware: StateProtection -> AdminMiddleware -> Handlers")
    
    # Запуск бота
    logger.info("Запуск polling с защитой от зацикливания...")
    
    try:
        # Запускаем планировщик в фоне
        if scheduler:
            scheduler_task = asyncio.create_task(scheduler.start_scheduler())
            logger.info("ЗАПУЩЕН: Планировщик рассылок")
        
        # Статистика защиты состояний
        def log_protection_stats():
            processing_count = len(state_protection.processing_users)
            cache_size = len(state_protection.user_last_action)
            timeout_size = len(state_protection.action_timeouts)
            
            if processing_count > 0 or cache_size > 50:
                logger.info(f"Защита состояний: обрабатывается {processing_count} пользователей, "
                           f"кэш {cache_size} записей, тайм-ауты {timeout_size}")
        
        # Периодическое логирование статистики (каждые 5 минут)
        async def stats_logger():
            while True:
                await asyncio.sleep(300)  # 5 минут
                log_protection_stats()
        
        stats_task = asyncio.create_task(stats_logger())
        
        # Запускаем поллинг
        await dp.start_polling(
            bot,
            handle_signals=True,
            drop_pending_updates=True
        )
        
    except KeyboardInterrupt:
        logger.info("ОСТАНОВКА: Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"ОШИБКА при работе бота: {e}")
        # Логируем состояние защиты при ошибке
        logger.error(f"Состояние защиты: {len(state_protection.processing_users)} активных пользователей")
    finally:
        # Останавливаем планировщик
        if scheduler:
            scheduler.stop_scheduler()
            logger.info("ОСТАНОВЛЕН: Планировщик рассылок")
            
        if scheduler_task:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Останавливаем логирование статистики
        if 'stats_task' in locals():
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass
        
        # Финальная статистика защиты
        final_processing = len(state_protection.processing_users)
        final_cache = len(state_protection.user_last_action)
        logger.info(f"ФИНАЛЬНАЯ СТАТИСТИКА: {final_processing} активных пользователей, "
                   f"{final_cache} записей в кэше")
        
        # Очищаем состояние защиты
        state_protection.processing_users.clear()
        state_protection.user_last_action.clear()
        state_protection.action_timeouts.clear()
        logger.info("ОЧИЩЕНО: Состояние защиты сброшено")
        
        # Закрываем сессию бота
        if bot:
            try:
                await bot.session.close()
                logger.info("ЗАКРЫТО: Сессия бота")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии сессии бота: {e}")
        
        logger.info("ЗАВЕРШЕНО: Бот корректно завершен с защитой состояний")

def check_environment():
    """Проверка окружения перед запуском"""
    if not os.path.exists('.env'):
        print("ОШИБКА: Файл .env не найден!")
        print("Создайте файл .env на основе .env.example")
        print("\nПример содержимого .env:")
        print("BOT_TOKEN=your_bot_token_here")
        print("ADMIN_IDS=123456789")
        print("ADMIN_PASSWORD=your_password_here")
        return False
    
    return True

def print_startup_banner():
    """Красивый баннер при запуске"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                   КАРДИОЧЕКАП БОТ v2.0                      ║
║                  С ЗАЩИТОЙ ОТ ЗАЦИКЛИВАНИЯ                  ║
╠══════════════════════════════════════════════════════════════╣
║  • Middleware защиты состояний                              ║
║  • Дедупликация действий пользователей                      ║
║  • Защита от спама и переполнения                           ║
║  • Безопасное редактирование сообщений                      ║
║  • Логирование всех взаимодействий                          ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)

if __name__ == "__main__":
    try:
        # Проверяем окружение
        if not check_environment():
            exit(1)
        
        # Показываем баннер
        print_startup_banner()
        
        # Запускаем бота
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("ОСТАНОВКА: Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА при запуске бота: {e}")
        print(f"Ошибка: {e}")
        exit(1)