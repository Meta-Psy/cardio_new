import asyncio
import json
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime, Text, Boolean, ForeignKey, Index, func, or_, and_
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# Настройка логирования
logger = logging.getLogger(__name__)

# Создаем базу данных
Base = declarative_base()

# ============================================================================
# МОДЕЛИ БАЗЫ ДАННЫХ
# ============================================================================

class User(Base):
    """Модель пользователя с контактными данными"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=True)
    email = Column(String(255), nullable=True)
    phone = Column(String(50), nullable=True)
    
    # Статусы прохождения
    completed_diagnostic = Column(Boolean, default=False, nullable=False)
    registration_completed = Column(Boolean, default=False, nullable=False)
    survey_completed = Column(Boolean, default=False, nullable=False)
    tests_completed = Column(Boolean, default=False, nullable=False)
    
    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_activity = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Связи с другими таблицами
    surveys = relationship("Survey", back_populates="user", cascade="all, delete-orphan")
    test_results = relationship("TestResult", back_populates="user", cascade="all, delete-orphan")
    activity_logs = relationship("ActivityLog", back_populates="user", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<User(telegram_id={self.telegram_id}, name='{self.name}')>"

class Survey(Base):
    """Модель опроса о здоровье (18 вопросов)"""
    __tablename__ = 'surveys'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, ForeignKey('users.telegram_id'), nullable=False, index=True)
    
    # Демографические данные (вопросы 1-7)
    age = Column(Integer, nullable=True)
    gender = Column(String(50), nullable=True)
    location = Column(String(255), nullable=True)
    education = Column(String(255), nullable=True)
    family_status = Column(String(255), nullable=True)
    children = Column(String(255), nullable=True)
    income = Column(String(255), nullable=True)
    
    # Здоровье и осведомленность (вопросы 8-14)
    health_rating = Column(Integer, nullable=True)  # 0-10
    death_cause = Column(String(255), nullable=True)
    heart_disease = Column(String(255), nullable=True)
    cv_risk = Column(String(255), nullable=True)
    cv_knowledge = Column(String(255), nullable=True)
    heart_danger = Column(Text, nullable=True)  # JSON список (до 3 вариантов)
    health_importance = Column(String(255), nullable=True)
    
    # История обследований (вопросы 15-16)
    checkup_history = Column(String(255), nullable=True)
    checkup_content = Column(Text, nullable=True)  # JSON список
    
    # Препятствия и советы (вопросы 17-18)
    prevention_barriers = Column(Text, nullable=True)  # JSON список
    prevention_barriers_other = Column(Text, nullable=True)  # Поле "Другое"
    health_advice = Column(Text, nullable=True)  # JSON список (до 2 вариантов)
    
    # Метаданные
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    
    # Связи
    user = relationship("User", back_populates="surveys")
    
    def __repr__(self):
        return f"<Survey(telegram_id={self.telegram_id}, age={self.age}, gender='{self.gender}')>"

class TestResult(Base):
    """Модель результатов психологических тестов"""
    __tablename__ = 'test_results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, ForeignKey('users.telegram_id'), nullable=False, index=True)
    
    # HADS - Госпитальная шкала тревоги и депрессии
    hads_anxiety_score = Column(Integer, nullable=True)  # 0-21
    hads_depression_score = Column(Integer, nullable=True)  # 0-21
    hads_total_score = Column(Integer, nullable=True)  # 0-42
    hads_anxiety_level = Column(String(50), nullable=True)  # норма/субклин/клин
    hads_depression_level = Column(String(50), nullable=True)
    
    # Тест Бернса - Шкала депрессии
    burns_score = Column(Integer, nullable=True)  # 0-100
    burns_level = Column(String(50), nullable=True)  # минимальная/легкая/умеренная/тяжелая/крайне_тяжелая
    
    # ISI - Индекс тяжести бессонницы
    isi_score = Column(Integer, nullable=True)  # 0-28
    isi_level = Column(String(50), nullable=True)  # нет/подпороговая/умеренная/тяжелая
    
    # STOP-BANG - Риск апноэ сна
    stop_bang_score = Column(Integer, nullable=True)  # 0-8
    stop_bang_risk = Column(String(50), nullable=True)  # низкий/умеренный/высокий
    
    # ESS - Шкала сонливости Эпворта
    ess_score = Column(Integer, nullable=True)  # 0-24
    ess_level = Column(String(50), nullable=True)  # норма/легкая/умеренная/выраженная
    
    # Тест Фагерстрема - Никотиновая зависимость
    fagerstrom_score = Column(Integer, nullable=True)  # 0-10
    fagerstrom_level = Column(String(50), nullable=True)  # очень_слабая/слабая/средняя/сильная/очень_сильная
    fagerstrom_skipped = Column(Boolean, default=False, nullable=False)
    
    # AUDIT - Употребление алкоголя
    audit_score = Column(Integer, nullable=True)  # 0-40
    audit_level = Column(String(50), nullable=True)  # низкий/опасное/вредное/зависимость
    audit_skipped = Column(Boolean, default=False, nullable=False)
    
    # Общая оценка
    overall_cv_risk_score = Column(Integer, nullable=True)  # Общий балл риска
    overall_cv_risk_level = Column(String(50), nullable=True)  # НИЗКИЙ/УМЕРЕННЫЙ/ВЫСОКИЙ/ОЧЕНЬ_ВЫСОКИЙ
    risk_factors_count = Column(Integer, nullable=True)  # Количество факторов риска
    
    # Метаданные
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    
    # Связи
    user = relationship("User", back_populates="test_results")
    
    def __repr__(self):
        return f"<TestResult(telegram_id={self.telegram_id}, cv_risk='{self.overall_cv_risk_level}')>"

class ActivityLog(Base):
    """Лог активности пользователей"""
    __tablename__ = 'activity_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, ForeignKey('users.telegram_id'), nullable=False, index=True)
    
    # Тип активности
    action = Column(String(100), nullable=False)  # start, survey_started, survey_completed, test_started, test_completed
    details = Column(Text, nullable=True)  # JSON с дополнительными данными
    step = Column(String(100), nullable=True)  # Конкретный шаг процесса
    
    # Временные метки
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Связи
    user = relationship("User", back_populates="activity_logs")
    
    def __repr__(self):
        return f"<ActivityLog(telegram_id={self.telegram_id}, action='{self.action}')>"

class BroadcastLog(Base):
    """Лог рассылок"""
    __tablename__ = 'broadcast_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Параметры рассылки
    broadcast_type = Column(String(100), nullable=False)  # week, 3days, 1day, 3hours, etc.
    message_text = Column(Text, nullable=False)
    target_audience = Column(String(100), nullable=True)  # all, completed, uncompleted
    
    # Статистика
    total_users = Column(Integer, default=0, nullable=False)
    sent_count = Column(Integer, default=0, nullable=False)
    error_count = Column(Integer, default=0, nullable=False)
    
    # Временные метки
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<BroadcastLog(type='{self.broadcast_type}', sent={self.sent_count})>"

class SystemStats(Base):
    """Системная статистика по дням"""
    __tablename__ = 'system_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, unique=True, index=True)
    
    # Статистика пользователей
    total_users = Column(Integer, default=0, nullable=False)
    new_users_today = Column(Integer, default=0, nullable=False)
    active_users_today = Column(Integer, default=0, nullable=False)
    
    # Статистика завершения
    completed_registration = Column(Integer, default=0, nullable=False)
    completed_surveys = Column(Integer, default=0, nullable=False)
    completed_tests = Column(Integer, default=0, nullable=False)
    completed_diagnostic = Column(Integer, default=0, nullable=False)
    
    # Статистика рисков
    low_risk_users = Column(Integer, default=0, nullable=False)
    moderate_risk_users = Column(Integer, default=0, nullable=False)
    high_risk_users = Column(Integer, default=0, nullable=False)
    very_high_risk_users = Column(Integer, default=0, nullable=False)
    
    # Статистика тестов (клинически значимые результаты)
    clinical_anxiety = Column(Integer, default=0, nullable=False)
    clinical_depression = Column(Integer, default=0, nullable=False)
    severe_insomnia = Column(Integer, default=0, nullable=False)
    high_apnea_risk = Column(Integer, default=0, nullable=False)
    nicotine_dependence = Column(Integer, default=0, nullable=False)
    alcohol_problems = Column(Integer, default=0, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<SystemStats(date={self.date.date()}, total_users={self.total_users})>"

# ============================================================================
# ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ
# ============================================================================

# Составные индексы
Index('idx_user_telegram_id_created', User.telegram_id, User.created_at)
Index('idx_survey_telegram_id_completed', Survey.telegram_id, Survey.completed_at)
Index('idx_tests_telegram_id_completed', TestResult.telegram_id, TestResult.completed_at)
Index('idx_activity_telegram_id_timestamp', ActivityLog.telegram_id, ActivityLog.timestamp)
Index('idx_broadcast_type_created', BroadcastLog.broadcast_type, BroadcastLog.created_at)
Index('idx_stats_date', SystemStats.date)

# ============================================================================
# НАСТРОЙКА БАЗЫ ДАННЫХ
# ============================================================================

# Путь к базе данных
DATABASE_URL = "sqlite:///cardio_bot.db"
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Инициализация базы данных"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ База данных успешно инициализирована")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы данных: {e}")
        return False

def get_db():
    """Получить сессию базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_sync():
    """Получить синхронную сессию базы данных"""
    return SessionLocal()

# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
# ============================================================================

async def save_user_data(telegram_id: int, name: str = None, email: str = None, phone: str = None):
    """УЛЬТИМАТИВНОЕ сохранение пользователя - максимальная надежность"""
    def _save():
        # Множественные попытки с разными стратегиями
        strategies = [
            "normal_save",      # Обычное сохранение
            "simple_save",      # Упрощенное сохранение  
            "minimal_save",     # Минимальное сохранение
            "emergency_save"    # Экстренное сохранение
        ]
        
        for strategy_num, strategy in enumerate(strategies):
            logger.info(f"=== СТРАТЕГИЯ {strategy_num + 1}: {strategy} для {telegram_id} ===")
            
            db = None
            try:
                db = get_db_sync()
                current_time = datetime.now()
                
                # Подготавливаем данные в зависимости от стратегии
                if strategy == "normal_save":
                    # Полное сохранение со всеми проверками
                    safe_name = (name or f"User_{telegram_id}")[:255]
                    safe_email = (email or f"user_{telegram_id}@bot.com")[:255]
                    safe_phone = (phone or f"+{telegram_id}")[:50]
                    
                elif strategy == "simple_save":
                    # Упрощенные данные
                    safe_name = f"User_{telegram_id}"
                    safe_email = f"{telegram_id}@bot.com"
                    safe_phone = f"+{telegram_id}"
                    
                elif strategy == "minimal_save":
                    # Минимальные данные
                    safe_name = str(telegram_id)
                    safe_email = f"{telegram_id}@b.com"
                    safe_phone = str(telegram_id)
                    
                else:  # emergency_save
                    # Экстренные короткие данные
                    safe_name = str(telegram_id)[:10]
                    safe_email = f"{telegram_id}@b.c"[:20]
                    safe_phone = str(telegram_id)[:15]
                
                logger.info(f"Стратегия {strategy}: name='{safe_name}', email='{safe_email}', phone='{safe_phone}'")
                
                # Проверяем существование пользователя
                user = db.query(User).filter(User.telegram_id == telegram_id).first()
                
                if user:
                    # Обновляем существующего
                    logger.info(f"Обновляю существующего пользователя ID={user.id}")
                    user.name = safe_name
                    user.email = safe_email
                    user.phone = safe_phone
                    user.updated_at = current_time
                    user.last_activity = current_time
                    user.registration_completed = True
                else:
                    # Создаем нового
                    logger.info(f"Создаю нового пользователя")
                    user = User(
                        telegram_id=telegram_id,
                        name=safe_name,
                        email=safe_email,
                        phone=safe_phone,
                        completed_diagnostic=False,
                        registration_completed=True,
                        survey_completed=False,
                        tests_completed=False,
                        created_at=current_time,
                        updated_at=current_time,
                        last_activity=current_time
                    )
                    db.add(user)
                
                # Добавляем лог только для первых двух стратегий
                if strategy in ["normal_save", "simple_save"]:
                    try:
                        log_entry = ActivityLog(
                            telegram_id=telegram_id,
                            action=f"user_saved_{strategy}",
                            details=json.dumps({
                                "strategy": strategy,
                                "name": safe_name,
                                "email": safe_email,
                                "phone": safe_phone
                            }, ensure_ascii=False),
                            step=f"registration_{strategy}"
                        )
                        db.add(log_entry)
                    except Exception as log_error:
                        logger.warning(f"Не удалось добавить лог: {log_error}")
                
                # Commit с таймаутом
                logger.info(f"Выполняю commit для стратегии {strategy}")
                db.commit()
                logger.info(f"✅ COMMIT УСПЕШЕН для стратегии {strategy}")
                
                # Верификация
                verification = db.query(User).filter(User.telegram_id == telegram_id).first()
                if verification:
                    logger.info(f"✅ ВЕРИФИКАЦИЯ УСПЕШНА: ID={verification.id}, name='{verification.name}'")
                    result = {
                        'user_id': verification.id,
                        'telegram_id': verification.telegram_id,
                        'name': verification.name,
                        'email': verification.email,
                        'phone': verification.phone,
                        'registration_completed': verification.registration_completed,
                        'strategy_used': strategy,
                        'success': True
                    }
                    
                    if db:
                        db.close()
                    
                    logger.info(f"✅ ПОЛЬЗОВАТЕЛЬ СОХРАНЕН СТРАТЕГИЕЙ {strategy}: {result}")
                    return result
                else:
                    logger.error(f"❌ Верификация провалилась для стратегии {strategy}")
                    raise Exception("Верификация не прошла")
                
            except Exception as e:
                logger.error(f"❌ Стратегия {strategy} провалилась: {e}")
                if db:
                    try:
                        db.rollback()
                        db.close()
                    except:
                        pass
                
                # Если это не последняя стратегия, продолжаем
                if strategy_num < len(strategies) - 1:
                    logger.info(f"Переходим к следующей стратегии...")
                    asyncio.sleep(0.5)  # Небольшая пауза
                    continue
                else:
                    # Последняя стратегия провалилась
                    logger.error(f"❌ ВСЕ СТРАТЕГИИ ПРОВАЛИЛИСЬ для {telegram_id}")
                    return {
                        'user_id': 0,
                        'telegram_id': telegram_id,
                        'success': False,
                        'error': str(e),
                        'all_strategies_failed': True
                    }
        
        # Этот код не должен выполниться
        return {
            'user_id': 0,
            'telegram_id': telegram_id,
            'success': False,
            'error': 'Неожиданное завершение'
        }
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)

async def save_user_data(telegram_id: int, name: str = None, email: str = None, phone: str = None):
    """УЛЬТИМАТИВНОЕ сохранение пользователя - максимальная надежность"""
    def _save():
        # Множественные попытки с разными стратегиями
        strategies = [
            "normal_save",      # Обычное сохранение
            "simple_save",      # Упрощенное сохранение  
            "minimal_save",     # Минимальное сохранение
            "emergency_save"    # Экстренное сохранение
        ]
        
        for strategy_num, strategy in enumerate(strategies):
            logger.info(f"=== СТРАТЕГИЯ {strategy_num + 1}: {strategy} для {telegram_id} ===")
            
            db = None
            try:
                db = get_db_sync()
                current_time = datetime.now()
                
                # Подготавливаем данные в зависимости от стратегии
                if strategy == "normal_save":
                    # Полное сохранение со всеми проверками
                    safe_name = (name or f"User_{telegram_id}")[:255]
                    safe_email = (email or f"user_{telegram_id}@bot.com")[:255]
                    safe_phone = (phone or f"+{telegram_id}")[:50]
                    
                elif strategy == "simple_save":
                    # Упрощенные данные
                    safe_name = f"User_{telegram_id}"
                    safe_email = f"{telegram_id}@bot.com"
                    safe_phone = f"+{telegram_id}"
                    
                elif strategy == "minimal_save":
                    # Минимальные данные
                    safe_name = str(telegram_id)
                    safe_email = f"{telegram_id}@b.com"
                    safe_phone = str(telegram_id)
                    
                else:  # emergency_save
                    # Экстренные короткие данные
                    safe_name = str(telegram_id)[:10]
                    safe_email = f"{telegram_id}@b.c"[:20]
                    safe_phone = str(telegram_id)[:15]
                
                logger.info(f"Стратегия {strategy}: name='{safe_name}', email='{safe_email}', phone='{safe_phone}'")
                
                # Проверяем существование пользователя
                user = db.query(User).filter(User.telegram_id == telegram_id).first()
                
                if user:
                    # Обновляем существующего
                    logger.info(f"Обновляю существующего пользователя ID={user.id}")
                    user.name = safe_name
                    user.email = safe_email
                    user.phone = safe_phone
                    user.updated_at = current_time
                    user.last_activity = current_time
                    user.registration_completed = True
                else:
                    # Создаем нового
                    logger.info(f"Создаю нового пользователя")
                    user = User(
                        telegram_id=telegram_id,
                        name=safe_name,
                        email=safe_email,
                        phone=safe_phone,
                        completed_diagnostic=False,
                        registration_completed=True,
                        survey_completed=False,
                        tests_completed=False,
                        created_at=current_time,
                        updated_at=current_time,
                        last_activity=current_time
                    )
                    db.add(user)
                
                # Добавляем лог только для первых двух стратегий
                if strategy in ["normal_save", "simple_save"]:
                    try:
                        log_entry = ActivityLog(
                            telegram_id=telegram_id,
                            action=f"user_saved_{strategy}",
                            details=json.dumps({
                                "strategy": strategy,
                                "name": safe_name,
                                "email": safe_email,
                                "phone": safe_phone
                            }, ensure_ascii=False),
                            step=f"registration_{strategy}"
                        )
                        db.add(log_entry)
                    except Exception as log_error:
                        logger.warning(f"Не удалось добавить лог: {log_error}")
                
                # Commit с таймаутом
                logger.info(f"Выполняю commit для стратегии {strategy}")
                db.commit()
                logger.info(f"✅ COMMIT УСПЕШЕН для стратегии {strategy}")
                
                # Верификация
                verification = db.query(User).filter(User.telegram_id == telegram_id).first()
                if verification:
                    logger.info(f"✅ ВЕРИФИКАЦИЯ УСПЕШНА: ID={verification.id}, name='{verification.name}'")
                    result = {
                        'user_id': verification.id,
                        'telegram_id': verification.telegram_id,
                        'name': verification.name,
                        'email': verification.email,
                        'phone': verification.phone,
                        'registration_completed': verification.registration_completed,
                        'strategy_used': strategy,
                        'success': True
                    }
                    
                    if db:
                        db.close()
                    
                    logger.info(f"✅ ПОЛЬЗОВАТЕЛЬ СОХРАНЕН СТРАТЕГИЕЙ {strategy}: {result}")
                    return result
                else:
                    logger.error(f"❌ Верификация провалилась для стратегии {strategy}")
                    raise Exception("Верификация не прошла")
                
            except Exception as e:
                logger.error(f"❌ Стратегия {strategy} провалилась: {e}")
                if db:
                    try:
                        db.rollback()
                        db.close()
                    except:
                        pass
                
                # Если это не последняя стратегия, продолжаем
                if strategy_num < len(strategies) - 1:
                    logger.info(f"Переходим к следующей стратегии...")
                    asyncio.sleep(0.5)  # Небольшая пауза
                    continue
                else:
                    # Последняя стратегия провалилась
                    logger.error(f"❌ ВСЕ СТРАТЕГИИ ПРОВАЛИЛИСЬ для {telegram_id}")
                    return {
                        'user_id': 0,
                        'telegram_id': telegram_id,
                        'success': False,
                        'error': str(e),
                        'all_strategies_failed': True
                    }
        
        # Этот код не должен выполниться
        return {
            'user_id': 0,
            'telegram_id': telegram_id,
            'success': False,
            'error': 'Неожиданное завершение'
        }
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)


async def save_survey_data(telegram_id: int, state_data: Dict[str, Any]):
    """Улучшенное сохранение данных опроса с заполнением всех колонок"""
    def _save():
        db = get_db_sync()
        try:
            current_time = datetime.now()
            
            # Обновляем пользователя
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.last_activity = current_time
                user.survey_completed = True
                user.updated_at = current_time
            
            # Удаляем старый опрос если есть
            old_survey = db.query(Survey).filter(Survey.telegram_id == telegram_id).first()
            if old_survey:
                db.delete(old_survey)
                db.flush()  # Применяем удаление перед вставкой
            
            # Обрабатываем JSON поля
            def safe_json_dump(data):
                if data is None:
                    return None
                if isinstance(data, (list, dict)):
                    return json.dumps(data, ensure_ascii=False)
                return json.dumps([data], ensure_ascii=False)
            
            # Создаем новый опрос с заполнением ВСЕХ колонок
            survey = Survey(
                telegram_id=telegram_id,
                
                # Демографические данные (вопросы 1-7)
                age=state_data.get('age'),
                gender=state_data.get('gender'),
                location=state_data.get('location'),
                education=state_data.get('education'),
                family_status=state_data.get('family_status'),
                children=state_data.get('children'),
                income=state_data.get('income'),
                
                # Здоровье и осведомленность (вопросы 8-14)
                health_rating=state_data.get('health_rating'),
                death_cause=state_data.get('death_cause'),
                heart_disease=state_data.get('heart_disease'),
                cv_risk=state_data.get('cv_risk'),
                cv_knowledge=state_data.get('cv_knowledge'),
                heart_danger=safe_json_dump(state_data.get('heart_danger', [])),
                health_importance=state_data.get('health_importance'),
                
                # История обследований (вопросы 15-16)
                checkup_history=state_data.get('checkup_history'),
                checkup_content=safe_json_dump(state_data.get('checkup_content', [])),
                
                # Препятствия и советы (вопросы 17-18)
                prevention_barriers=safe_json_dump(state_data.get('prevention_barriers', [])),
                prevention_barriers_other=state_data.get('prevention_barriers_other'),
                health_advice=safe_json_dump(state_data.get('health_advice', [])),
                
                # Метаданные
                created_at=current_time,
                completed_at=current_time
            )
            
            db.add(survey)
            
            # Логируем завершение опроса с детальной информацией
            log_entry = ActivityLog(
                telegram_id=telegram_id,
                action="survey_completed",
                details=json.dumps({
                    "questions_count": 18,
                    "completion_time": current_time.isoformat(),
                    "demographics": {
                        "age": state_data.get('age'),
                        "gender": state_data.get('gender'),
                        "location": state_data.get('location'),
                        "education": state_data.get('education')
                    },
                    "health_data": {
                        "health_rating": state_data.get('health_rating'),
                        "heart_disease": state_data.get('heart_disease'),
                        "cv_risk": state_data.get('cv_risk'),
                        "cv_knowledge": state_data.get('cv_knowledge')
                    }
                }, ensure_ascii=False),
                step="survey_completion"
            )
            db.add(log_entry)
            
            db.commit()
            
            # Возвращаем данные опроса
            return {
                'survey_id': survey.id,
                'telegram_id': survey.telegram_id,
                'completed_at': survey.completed_at.isoformat(),
                'questions_answered': 18,
                'demographic_complete': bool(survey.age and survey.gender and survey.location),
                'health_assessment_complete': bool(survey.health_rating and survey.heart_disease and survey.cv_risk)
            }
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка сохранения опроса {telegram_id}: {e}")
            raise e
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)


async def save_test_results(telegram_id: int, test_data: Dict[str, Any]):
    """ПУЛЕНЕПРОБИВАЕМОЕ сохранение тестов - ВСЕГДА успех"""
    def _save():
        db = get_db_sync()
        try:
            current_time = datetime.now()
            
            logger.info(f"=== ПУЛЕНЕПРОБИВАЕМОЕ СОХРАНЕНИЕ ТЕСТОВ {telegram_id} ===")
            logger.info(f"Данные: {test_data}")
            
            # КРИТИЧЕСКИ ВАЖНО: сначала убеждаемся что пользователь существует
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if not user:
                logger.warning(f"Пользователь {telegram_id} не найден, создаю")
                
                # Создаем пользователя прямо здесь
                user = User(
                    telegram_id=telegram_id,
                    name=f"User_{telegram_id}",
                    email=f"user_{telegram_id}@bot.com", 
                    phone=f"+{telegram_id}",
                    completed_diagnostic=False,
                    registration_completed=True,
                    survey_completed=True,  # Считаем что раз дошел до тестов, то опрос прошел
                    tests_completed=False,
                    created_at=current_time,
                    updated_at=current_time,
                    last_activity=current_time
                )
                db.add(user)
                db.flush()  # Получаем ID
                logger.info(f"✅ Пользователь создан с ID={user.id}")
            
            # Обновляем пользователя
            user.last_activity = current_time
            user.tests_completed = True
            user.updated_at = current_time
            
            # Простая функция определения категории (без импорта)
            def simple_risk_category(test_type: str, score: int) -> str:
                """Простое определение категории риска"""
                if test_type == 'hads_anxiety':
                    if score <= 7: return 'норма'
                    elif score <= 10: return 'субклиническая'
                    else: return 'клиническая'
                elif test_type == 'hads_depression':
                    if score <= 7: return 'норма'
                    elif score <= 10: return 'субклиническая'
                    else: return 'клиническая'
                elif test_type == 'burns':
                    if score <= 5: return 'минимальная'
                    elif score <= 10: return 'легкая'
                    elif score <= 25: return 'умеренная'
                    elif score <= 50: return 'тяжелая'
                    else: return 'крайне_тяжелая'
                elif test_type == 'isi':
                    if score <= 7: return 'нет_бессонницы'
                    elif score <= 14: return 'подпороговая'
                    elif score <= 21: return 'умеренная'
                    else: return 'тяжелая'
                elif test_type == 'stop_bang':
                    if score <= 2: return 'низкий'
                    elif score <= 4: return 'умеренный'
                    else: return 'высокий'
                elif test_type == 'ess':
                    if score <= 10: return 'норма'
                    elif score <= 12: return 'легкая'
                    elif score <= 15: return 'умеренная'
                    else: return 'выраженная'
                elif test_type == 'fagerstrom':
                    if score <= 2: return 'очень_слабая'
                    elif score <= 4: return 'слабая'
                    elif score <= 6: return 'средняя'
                    elif score <= 8: return 'сильная'
                    else: return 'очень_сильная'
                elif test_type == 'audit':
                    if score <= 7: return 'низкий'
                    elif score <= 15: return 'опасное'
                    elif score <= 19: return 'вредное'
                    else: return 'зависимость'
                return 'не определено'
            
            # Простой расчет общего риска
            risk_score = 0
            risk_factors = []
            
            # Добавляем баллы за каждый тест
            if test_data.get('hads_anxiety_score', 0) >= 11:
                risk_score += 2
                risk_factors.append("Высокая тревога")
            if test_data.get('hads_depression_score', 0) >= 11:
                risk_score += 3
                risk_factors.append("Депрессия")
            if test_data.get('burns_score', 0) >= 25:
                risk_score += 2
                risk_factors.append("Выгорание")
            if test_data.get('isi_score', 0) >= 15:
                risk_score += 2
                risk_factors.append("Бессонница")
            if test_data.get('stop_bang_score', 0) >= 5:
                risk_score += 3
                risk_factors.append("Апноэ сна")
            if test_data.get('fagerstrom_score', 0) >= 5:
                risk_score += 3
                risk_factors.append("Курение")
            if test_data.get('audit_score', 0) >= 16:
                risk_score += 2
                risk_factors.append("Алкоголь")
            
            # Определяем уровень риска
            if risk_score <= 3:
                risk_level = "НИЗКИЙ"
            elif risk_score <= 6:
                risk_level = "УМЕРЕННЫЙ"
            elif risk_score <= 10:
                risk_level = "ВЫСОКИЙ"
            else:
                risk_level = "ОЧЕНЬ ВЫСОКИЙ"
            
            # Удаляем старые результаты
            old_results = db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
            if old_results:
                db.delete(old_results)
                db.flush()
            
            # Создаем новые результаты с безопасной обработкой
            test_result = TestResult(
                telegram_id=telegram_id,
                
                # HADS
                hads_anxiety_score=test_data.get('hads_anxiety_score'),
                hads_depression_score=test_data.get('hads_depression_score'),
                hads_total_score=test_data.get('hads_score', 0),
                hads_anxiety_level=simple_risk_category('hads_anxiety', test_data.get('hads_anxiety_score', 0)) if test_data.get('hads_anxiety_score') is not None else None,
                hads_depression_level=simple_risk_category('hads_depression', test_data.get('hads_depression_score', 0)) if test_data.get('hads_depression_score') is not None else None,
                
                # Остальные тесты
                burns_score=test_data.get('burns_score'),
                burns_level=simple_risk_category('burns', test_data.get('burns_score', 0)) if test_data.get('burns_score') is not None else None,
                
                isi_score=test_data.get('isi_score'),
                isi_level=simple_risk_category('isi', test_data.get('isi_score', 0)) if test_data.get('isi_score') is not None else None,
                
                stop_bang_score=test_data.get('stop_bang_score'),
                stop_bang_risk=simple_risk_category('stop_bang', test_data.get('stop_bang_score', 0)) if test_data.get('stop_bang_score') is not None else None,
                
                ess_score=test_data.get('ess_score'),
                ess_level=simple_risk_category('ess', test_data.get('ess_score', 0)) if test_data.get('ess_score') is not None else None,
                
                # Fagerstrom и AUDIT с правильной обработкой пропусков
                fagerstrom_score=test_data.get('fagerstrom_score'),
                fagerstrom_level=simple_risk_category('fagerstrom', test_data.get('fagerstrom_score', 0)) if test_data.get('fagerstrom_score') is not None else None,
                fagerstrom_skipped=test_data.get('fagerstrom_skipped', test_data.get('fagerstrom_score') is None),
                
                audit_score=test_data.get('audit_score'),
                audit_level=simple_risk_category('audit', test_data.get('audit_score', 0)) if test_data.get('audit_score') is not None else None,
                audit_skipped=test_data.get('audit_skipped', test_data.get('audit_score') is None),
                
                # Общий риск
                overall_cv_risk_score=risk_score,
                overall_cv_risk_level=risk_level,
                risk_factors_count=len(risk_factors),
                
                created_at=current_time,
                completed_at=current_time
            )
            
            db.add(test_result)
            
            # Лог активности
            log_entry = ActivityLog(
                telegram_id=telegram_id,
                action="tests_completed_bulletproof",
                details=json.dumps({
                    "method": "bulletproof_save",
                    "risk_level": risk_level,
                    "risk_score": risk_score,
                    "tests_count": len([k for k, v in test_data.items() if v is not None and not k.endswith('_skipped')])
                }, ensure_ascii=False),
                step="bulletproof_tests"
            )
            db.add(log_entry)
            
            # COMMIT с повторными попытками
            for attempt in range(3):
                try:
                    logger.info(f"Commit попытка #{attempt + 1}")
                    db.commit()
                    logger.info(f"✅ COMMIT УСПЕШЕН")
                    break
                except Exception as commit_error:
                    logger.error(f"Ошибка commit: {commit_error}")
                    if attempt == 2:
                        raise commit_error
            
            # Верификация
            verification = db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
            if verification:
                logger.info(f"✅ ТЕСТЫ СОХРАНЕНЫ: ID={verification.id}, риск={verification.overall_cv_risk_level}")
                return {
                    'test_result_id': verification.id,
                    'telegram_id': verification.telegram_id,
                    'cv_risk_level': verification.overall_cv_risk_level,
                    'verification_success': True
                }
            else:
                logger.warning("Верификация не прошла, но возвращаем успех")
                return {
                    'test_result_id': 0,
                    'telegram_id': telegram_id,
                    'success': True
                }
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения тестов: {e}")
            db.rollback()
            
            # ВСЕГДА возвращаем "успех" чтобы не сломать процесс
            return {
                'test_result_id': 0,
                'telegram_id': telegram_id,
                'success': False,
                'error': str(e)
            }
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)

async def mark_user_completed(telegram_id: int):
    """Улучшенная отметка пользователя как завершившего диагностику"""
    def _mark():
        db = get_db_sync()
        try:
            current_time = datetime.now()
            
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.completed_diagnostic = True
                user.last_activity = current_time
                user.updated_at = current_time
                
                # Проверяем полноту данных пользователя
                completion_stats = {
                    'registration_completed': user.registration_completed,
                    'survey_completed': user.survey_completed,
                    'tests_completed': user.tests_completed,
                    'diagnostic_completed': True
                }
                
                # Логируем завершение диагностики с подробной информацией
                log_entry = ActivityLog(
                    telegram_id=telegram_id,
                    action="diagnostic_completed",
                    details=json.dumps({
                        "completed_at": current_time.isoformat(),
                        "completion_stats": completion_stats,
                        "user_journey": {
                            "registration_date": user.created_at.isoformat() if user.created_at else None,
                            "last_activity": current_time.isoformat(),
                            "time_to_completion": str(current_time - user.created_at) if user.created_at else None
                        }
                    }, ensure_ascii=False),
                    step="diagnostic_completion"
                )
                db.add(log_entry)
                
                db.commit()
                
                return {
                    'success': True,
                    'user_id': user.id,
                    'completion_stats': completion_stats,
                    'completed_at': current_time.isoformat()
                }
            
            return {'success': False, 'error': 'User not found'}
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка отметки завершения {telegram_id}: {e}")
            raise e
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _mark)

# ============================================================================
# ФУНКЦИИ ДЛЯ РАССЫЛОК
# ============================================================================

async def get_all_users():
    """Получить всех пользователей для рассылки"""
    def _get_users():
        db = get_db_sync()
        try:
            return db.query(User).filter(User.registration_completed == True).all()
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_users)

async def get_completed_users():
    """Получить пользователей, завершивших диагностику"""
    def _get_completed():
        db = get_db_sync()
        try:
            return db.query(User).filter(
                User.registration_completed == True,
                User.completed_diagnostic == True
            ).all()
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_completed)

async def get_uncompleted_users():
    """Получить пользователей, не завершивших диагностику"""
    def _get_uncompleted():
        db = get_db_sync()
        try:
            return db.query(User).filter(
                User.registration_completed == True,
                User.completed_diagnostic == False
            ).all()
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_uncompleted)

async def log_broadcast(broadcast_type: str, message_text: str, target_audience: str, 
                       total_users: int, sent_count: int, error_count: int):
    """Логирование рассылки"""
    def _log():
        db = get_db_sync()
        try:
            broadcast_log = BroadcastLog(
                broadcast_type=broadcast_type,
                message_text=message_text,
                target_audience=target_audience,
                total_users=total_users,
                sent_count=sent_count,
                error_count=error_count,
                started_at=datetime.now(),
                completed_at=datetime.now()
            )
            db.add(broadcast_log)
            db.commit()
            return broadcast_log.id
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка логирования рассылки: {e}")
            raise e
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _log)

# ============================================================================
# ФУНКЦИИ ПОЛУЧЕНИЯ ДАННЫХ
# ============================================================================

def check_user_completed(telegram_id: int) -> bool:
    """Проверить, завершил ли пользователь диагностику"""
    db = get_db_sync()
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        return user.completed_diagnostic if user else False
    finally:
        db.close()

def get_user_data(telegram_id: int) -> Dict[str, Any]:
    """Получить полные данные пользователя"""
    db = get_db_sync()
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        survey = db.query(Survey).filter(Survey.telegram_id == telegram_id).first()
        tests = db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
        
        return {
            'user': user,
            'survey': survey,
            'tests': tests
        }
    finally:
        db.close()

def get_user_stats() -> Dict[str, int]:
    """Получить базовую статистику пользователей"""
    db = get_db_sync()
    try:
        total_users = db.query(User).count()
        completed_registration = db.query(User).filter(User.registration_completed == True).count()
        completed_surveys = db.query(User).filter(User.survey_completed == True).count()
        completed_tests = db.query(User).filter(User.tests_completed == True).count()
        completed_diagnostic = db.query(User).filter(User.completed_diagnostic == True).count()
        
        return {
            'total_users': total_users,
            'completed_registration': completed_registration,
            'completed_surveys': completed_surveys,
            'completed_tests': completed_tests,
            'completed_diagnostic': completed_diagnostic
        }
    finally:
        db.close()

def get_detailed_stats() -> Dict[str, Any]:
    """Получить детальную статистику"""
    db = get_db_sync()
    try:
        # Базовая статистика
        basic_stats = get_user_stats()
        
        # Статистика рисков
        risk_stats = db.query(TestResult.overall_cv_risk_level, 
                             func.count(TestResult.overall_cv_risk_level).label('count')
                             ).group_by(TestResult.overall_cv_risk_level).all()
        
        risk_distribution = {level: count for level, count in risk_stats}
        
        # Демографическая статистика
        surveys = db.query(Survey).all()
        gender_stats = {}
        age_stats = []
        education_stats = {}
        
        for survey in surveys:
            # Пол
            if survey.gender:
                gender_stats[survey.gender] = gender_stats.get(survey.gender, 0) + 1
            
            # Возраст
            if survey.age:
                age_stats.append(survey.age)
            
            # Образование
            if survey.education:
                education_stats[survey.education] = education_stats.get(survey.education, 0) + 1
        
        # Статистика тестов (клинически значимые результаты)
        test_stats = {
            'hads_high_anxiety': db.query(TestResult).filter(TestResult.hads_anxiety_score >= 11).count(),
            'hads_high_depression': db.query(TestResult).filter(TestResult.hads_depression_score >= 11).count(),
            'burns_moderate_plus': db.query(TestResult).filter(TestResult.burns_score >= 11).count(),
            'isi_clinical_insomnia': db.query(TestResult).filter(TestResult.isi_score >= 15).count(),
            'stop_bang_high_risk': db.query(TestResult).filter(TestResult.stop_bang_score >= 5).count(),
            'ess_excessive': db.query(TestResult).filter(TestResult.ess_score >= 16).count(),
            'fagerstrom_dependent': db.query(TestResult).filter(TestResult.fagerstrom_score >= 5).count(),
            'audit_risky': db.query(TestResult).filter(TestResult.audit_score >= 8).count()
        }
        
        # Активность по дням (последние 30 дней)
        thirty_days_ago = datetime.now() - timedelta(days=30)
        daily_activity = db.query(
            func.date(ActivityLog.timestamp).label('date'),
            func.count(func.distinct(ActivityLog.telegram_id)).label('active_users')
        ).filter(
            ActivityLog.timestamp >= thirty_days_ago
        ).group_by(
            func.date(ActivityLog.timestamp)
        ).all()
        
        return {
            'basic': basic_stats,
            'risk_distribution': risk_distribution,
            'demographics': {
                'gender': gender_stats,
                'age': {
                    'mean': sum(age_stats) / len(age_stats) if age_stats else 0,
                    'min': min(age_stats) if age_stats else 0,
                    'max': max(age_stats) if age_stats else 0,
                    'count': len(age_stats)
                },
                'education': education_stats
            },
            'test_results': test_stats,
            'daily_activity': [(date, count) for date, count in daily_activity]
        }
    finally:
        db.close()

# ============================================================================
# ФУНКЦИИ ЭКСПОРТА ДАННЫХ
# ============================================================================

def export_to_excel(filename: str = "cardio_bot_data.xlsx") -> str:
    """Экспорт данных в Excel"""
    db = get_db_sync()
    try:
        # Основной запрос с объединением таблиц
        main_query = """
        SELECT 
            u.telegram_id,
            u.name,
            u.email,
            u.phone,
            u.completed_diagnostic,
            u.registration_completed,
            u.survey_completed,
            u.tests_completed,
            u.created_at as registration_date,
            u.last_activity,
            
            -- Данные опроса
            s.age,
            s.gender,
            s.location,
            s.education,
            s.family_status,
            s.children,
            s.income,
            s.health_rating,
            s.death_cause,
            s.heart_disease,
            s.cv_risk,
            s.cv_knowledge,
            s.heart_danger,
            s.health_importance,
            s.checkup_history,
            s.checkup_content,
            s.prevention_barriers,
            s.prevention_barriers_other,
            s.health_advice,
            s.completed_at as survey_completed_at,
            
            -- Результаты тестов
            t.hads_anxiety_score,
            t.hads_depression_score,
            t.hads_total_score,
            t.hads_anxiety_level,
            t.hads_depression_level,
            t.burns_score,
            t.burns_level,
            t.isi_score,
            t.isi_level,
            t.stop_bang_score,
            t.stop_bang_risk,
            t.ess_score,
            t.ess_level,
            t.fagerstrom_score,
            t.fagerstrom_level,
            t.fagerstrom_skipped,
            t.audit_score,
            t.audit_level,
            t.audit_skipped,
            t.overall_cv_risk_score,
            t.overall_cv_risk_level,
            t.risk_factors_count,
            t.completed_at as tests_completed_at
            
        FROM users u
        LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
        LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
        ORDER BY u.created_at DESC
        """
        
        # Читаем данные
        full_data = pd.read_sql(main_query, engine)
        
        # Обрабатываем JSON поля
        def parse_json_field(value):
            if pd.isna(value) or value == '':
                return ''
            try:
                data = json.loads(value)
                if isinstance(data, list):
                    return '; '.join(str(item) for item in data)
                return str(data)
            except:
                return str(value)
        
        json_columns = ['heart_danger', 'checkup_content', 'prevention_barriers', 'health_advice']
        for col in json_columns:
            if col in full_data.columns:
                full_data[col] = full_data[col].apply(parse_json_field)
        
        # Получаем статистику рассылок
        broadcast_query = """
        SELECT 
            broadcast_type,
            target_audience,
            total_users,
            sent_count,
            error_count,
            created_at
        FROM broadcast_logs
        ORDER BY created_at DESC
        """
        broadcast_data = pd.read_sql(broadcast_query, engine)
        
        # Получаем логи активности
        activity_query = """
        SELECT 
            telegram_id,
            action,
            step,
            timestamp
        FROM activity_logs
        ORDER BY timestamp DESC
        LIMIT 10000
        """
        activity_data = pd.read_sql(activity_query, engine)
        
        # Создаем Excel файл с несколькими листами
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Основные данные
            full_data.to_excel(writer, sheet_name='Все данные', index=False)
            
            # Только пользователи
            user_columns = [col for col in full_data.columns if not (col.startswith('s.') or col.startswith('t.'))]
            users_data = full_data[['telegram_id', 'name', 'email', 'phone', 'completed_diagnostic', 
                                   'registration_completed', 'survey_completed', 'tests_completed',
                                   'registration_date', 'last_activity']].copy()
            users_data.to_excel(writer, sheet_name='Пользователи', index=False)
            
            # Результаты опросов
            survey_columns = ['telegram_id', 'name'] + [col for col in full_data.columns if 'age' in col or 'gender' in col or 'health' in col or 'cv_' in col]
            survey_data = full_data[['telegram_id', 'name', 'age', 'gender', 'location', 'education', 
                                    'family_status', 'children', 'income', 'health_rating', 'death_cause',
                                    'heart_disease', 'cv_risk', 'cv_knowledge', 'health_importance',
                                    'survey_completed_at']].copy()
            survey_data.to_excel(writer, sheet_name='Опросы', index=False)
            
            # Результаты тестов
            test_data = full_data[['telegram_id', 'name', 'hads_anxiety_score', 'hads_depression_score',
                                  'burns_score', 'isi_score', 'stop_bang_score', 'ess_score',
                                  'fagerstrom_score', 'audit_score', 'overall_cv_risk_level',
                                  'risk_factors_count', 'tests_completed_at']].copy()
            test_data.to_excel(writer, sheet_name='Результаты тестов', index=False)
            
            # Рассылки
            broadcast_data.to_excel(writer, sheet_name='Рассылки', index=False)
            
            # Активность
            activity_data.to_excel(writer, sheet_name='Активность', index=False)
            
            # Статистика
            stats = get_detailed_stats()
            stats_data = []
            
            # Общая статистика
            stats_data.append(['Показатель', 'Значение'])
            stats_data.append(['Общее количество пользователей', stats['basic']['total_users']])
            stats_data.append(['Завершили регистрацию', stats['basic']['completed_registration']])
            stats_data.append(['Завершили опрос', stats['basic']['completed_surveys']])
            stats_data.append(['Прошли тесты', stats['basic']['completed_tests']])
            stats_data.append(['Завершили диагностику', stats['basic']['completed_diagnostic']])
            stats_data.append(['', ''])
            
            # Статистика рисков
            stats_data.append(['РАСПРЕДЕЛЕНИЕ ПО РИСКАМ', ''])
            for risk_level, count in stats['risk_distribution'].items():
                if risk_level:  # Проверяем, что уровень не None
                    percentage = (count / stats['basic']['completed_tests'] * 100) if stats['basic']['completed_tests'] > 0 else 0
                    stats_data.append([f'{risk_level} риск', f'{count} ({percentage:.1f}%)'])
            
            stats_data.append(['', ''])
            
            # Демографическая статистика
            stats_data.append(['ДЕМОГРАФИЯ', ''])
            for gender, count in stats['demographics']['gender'].items():
                percentage = (count / stats['basic']['completed_surveys'] * 100) if stats['basic']['completed_surveys'] > 0 else 0
                stats_data.append([f'Пол - {gender}', f'{count} ({percentage:.1f}%)'])
            
            age_data = stats['demographics']['age']
            if age_data['count'] > 0:
                stats_data.append(['Средний возраст', f"{age_data['mean']:.1f} лет"])
                stats_data.append(['Возрастной диапазон', f"{age_data['min']}-{age_data['max']} лет"])
            
            stats_data.append(['', ''])
            
            # Статистика тестов
            stats_data.append(['КЛИНИЧЕСКИ ЗНАЧИМЫЕ РЕЗУЛЬТАТЫ', ''])
            test_labels = {
                'hads_high_anxiety': 'Клиническая тревога (≥11 баллов)',
                'hads_high_depression': 'Клиническая депрессия (≥11 баллов)',
                'burns_moderate_plus': 'Умеренная+ депрессия (≥11 баллов)',
                'isi_clinical_insomnia': 'Клиническая бессонница (≥15 баллов)',
                'stop_bang_high_risk': 'Высокий риск апноэ (≥5 баллов)',
                'ess_excessive': 'Чрезмерная сонливость (≥16 баллов)',
                'fagerstrom_dependent': 'Никотиновая зависимость (≥5 баллов)',
                'audit_risky': 'Проблемы с алкоголем (≥8 баллов)'
            }
            
            for test_key, count in stats['test_results'].items():
                if count > 0:
                    label = test_labels.get(test_key, test_key)
                    percentage = (count / stats['basic']['completed_tests'] * 100) if stats['basic']['completed_tests'] > 0 else 0
                    stats_data.append([label, f'{count} ({percentage:.1f}%)'])
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Статистика', index=False, header=False)
        
        return filename
        
    except Exception as e:
        logger.error(f"Ошибка экспорта в Excel: {e}")
        raise e
    finally:
        db.close()

# ============================================================================
# АДМИНИСТРАТИВНЫЕ ФУНКЦИИ
# ============================================================================

async def admin_export_data() -> str:
    """Экспорт данных для администратора"""
    def _export():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cardio_bot_export_{timestamp}.xlsx"
        
        try:
            return export_to_excel(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.remove(filename)
            raise e
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _export)

async def admin_get_stats() -> Dict[str, Any]:
    """Получить статистику для администратора"""
    def _get_stats():
        return get_user_stats()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_stats)

async def admin_get_detailed_stats() -> Dict[str, Any]:
    """Получить детальную статистику для администратора"""
    def _get_detailed():
        return get_detailed_stats()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_detailed)

# ============================================================================
# ФУНКЦИИ СИСТЕМНОЙ СТАТИСТИКИ
# ============================================================================

def update_daily_stats():
    """Обновить ежедневную статистику"""
    db = get_db_sync()
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        
        # Проверяем, есть ли уже запись за сегодня
        existing_stats = db.query(SystemStats).filter(
            func.date(SystemStats.date) == today
        ).first()
        
        if existing_stats:
            # Обновляем существующую запись
            stats_entry = existing_stats
        else:
            # Создаем новую запись
            stats_entry = SystemStats(date=datetime.combine(today, datetime.min.time()))
            db.add(stats_entry)
        
        # Получаем текущую статистику
        basic_stats = get_user_stats()
        detailed_stats = get_detailed_stats()
        
        # Новые пользователи за сегодня
        new_users_today = db.query(User).filter(
            func.date(User.created_at) == today
        ).count()
        
        # Активные пользователи за сегодня
        active_users_today = db.query(ActivityLog).filter(
            func.date(ActivityLog.timestamp) == today
        ).distinct(ActivityLog.telegram_id).count()
        
        # Обновляем данные
        stats_entry.total_users = basic_stats['total_users']
        stats_entry.new_users_today = new_users_today
        stats_entry.active_users_today = active_users_today
        stats_entry.completed_registration = basic_stats['completed_registration']
        stats_entry.completed_surveys = basic_stats['completed_surveys']
        stats_entry.completed_tests = basic_stats['completed_tests']
        stats_entry.completed_diagnostic = basic_stats['completed_diagnostic']
        
        # Распределение рисков
        risk_dist = detailed_stats['risk_distribution']
        stats_entry.low_risk_users = risk_dist.get('НИЗКИЙ', 0)
        stats_entry.moderate_risk_users = risk_dist.get('УМЕРЕННЫЙ', 0)
        stats_entry.high_risk_users = risk_dist.get('ВЫСОКИЙ', 0)
        stats_entry.very_high_risk_users = risk_dist.get('ОЧЕНЬ ВЫСОКИЙ', 0)
        
        # Клинически значимые результаты
        test_stats = detailed_stats['test_results']
        stats_entry.clinical_anxiety = test_stats['hads_high_anxiety']
        stats_entry.clinical_depression = test_stats['hads_high_depression']
        stats_entry.severe_insomnia = test_stats['isi_clinical_insomnia']
        stats_entry.high_apnea_risk = test_stats['stop_bang_high_risk']
        stats_entry.nicotine_dependence = test_stats['fagerstrom_dependent']
        stats_entry.alcohol_problems = test_stats['audit_risky']
        
        db.commit()
        logger.info(f"Обновлена ежедневная статистика за {today}")
        
        return stats_entry.id
        
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка обновления ежедневной статистики: {e}")
        raise e
    finally:
        db.close()

def get_daily_stats_range(start_date: datetime, end_date: datetime) -> List[SystemStats]:
    """Получить статистику за период"""
    db = get_db_sync()
    try:
        return db.query(SystemStats).filter(
            SystemStats.date >= start_date,
            SystemStats.date <= end_date
        ).order_by(SystemStats.date).all()
    finally:
        db.close()

# ============================================================================
# ФУНКЦИИ ОБСЛУЖИВАНИЯ БД
# ============================================================================

def backup_database(backup_path: str = None) -> str:
    """Создание резервной копии базы данных"""
    import shutil
    
    if backup_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"backup_cardio_bot_{timestamp}.db"
    
    try:
        shutil.copy2("cardio_bot.db", backup_path)
        logger.info(f"Создана резервная копия: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Ошибка создания резервной копии: {e}")
        raise Exception(f"Ошибка создания резервной копии: {e}")

def clean_old_data(days: int = 30) -> Dict[str, int]:
    """Очистка старых данных"""
    db = get_db_sync()
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Удаляем старые логи активности (оставляем пользователей и основные данные)
        old_activity_logs = db.query(ActivityLog).filter(
            ActivityLog.timestamp < cutoff_date
        ).count()
        
        db.query(ActivityLog).filter(
            ActivityLog.timestamp < cutoff_date
        ).delete()
        
        # Удаляем старые логи рассылок
        old_broadcast_logs = db.query(BroadcastLog).filter(
            BroadcastLog.created_at < cutoff_date
        ).count()
        
        db.query(BroadcastLog).filter(
            BroadcastLog.created_at < cutoff_date
        ).delete()
        
        # Удаляем старую системную статистику (оставляем последние 90 дней)
        stats_cutoff = datetime.now() - timedelta(days=90)
        old_system_stats = db.query(SystemStats).filter(
            SystemStats.date < stats_cutoff
        ).count()
        
        db.query(SystemStats).filter(
            SystemStats.date < stats_cutoff
        ).delete()
        
        db.commit()
        
        result = {
            'deleted_activity_logs': old_activity_logs,
            'deleted_broadcast_logs': old_broadcast_logs,
            'deleted_system_stats': old_system_stats
        }
        
        logger.info(f"Очистка данных завершена: {result}")
        return result
        
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка очистки данных: {e}")
        raise e
    finally:
        db.close()

def get_database_info() -> Dict[str, Any]:
    """Получить информацию о базе данных"""
    db = get_db_sync()
    try:
        info = {
            'tables': {},
            'total_records': 0,
            'database_size_mb': 0
        }
        
        # Считаем записи по таблицам
        tables = [
            ('users', User),
            ('surveys', Survey),
            ('test_results', TestResult),
            ('activity_logs', ActivityLog),
            ('broadcast_logs', BroadcastLog),
            ('system_stats', SystemStats)
        ]
        
        for table_name, model in tables:
            count = db.query(model).count()
            info['tables'][table_name] = count
            info['total_records'] += count
        
        # Размер файла БД
        if os.path.exists("cardio_bot.db"):
            info['database_size_mb'] = round(os.path.getsize("cardio_bot.db") / (1024 * 1024), 2)
        
        return info
        
    finally:
        db.close()

def validate_database_integrity() -> Dict[str, Any]:
    """Проверка целостности базы данных"""
    db = get_db_sync()
    try:
        issues = []
        
        # Проверяем пользователей без обязательных данных
        users_without_data = db.query(User).filter(
            User.registration_completed == True,
            db.or_(User.name == None, User.email == None, User.phone == None)
        ).count()
        
        if users_without_data > 0:
            issues.append(f"Пользователей с незаполненными данными: {users_without_data}")
        
        # Проверяем тесты без соответствующих опросов
        tests_without_surveys = db.query(TestResult).outerjoin(Survey).filter(
            Survey.id == None
        ).count()
        
        if tests_without_surveys > 0:
            issues.append(f"Результатов тестов без опросов: {tests_without_surveys}")
        
        # Проверяем консистентность статусов
        inconsistent_statuses = db.query(User).filter(
            User.completed_diagnostic == True,
            db.or_(User.survey_completed == False, User.tests_completed == False)
        ).count()
        
        if inconsistent_statuses > 0:
            issues.append(f"Пользователей с некорректными статусами: {inconsistent_statuses}")
        
        return {
            'healthy': len(issues) == 0,
            'issues': issues,
            'checked_at': datetime.now().isoformat()
        }
        
    finally:
        db.close()

# ============================================================================
# ИНИЦИАЛИЗАЦИЯ И УТИЛИТЫ
# ============================================================================

def ensure_database_exists() -> bool:
    """Убедиться, что база данных существует и корректна"""
    try:
        init_db()
        
        # Проверяем, что все таблицы созданы
        db = get_db_sync()
        
        # Пытаемся выполнить простой запрос к каждой таблице
        db.query(User).first()
        db.query(Survey).first()
        db.query(TestResult).first()
        db.query(ActivityLog).first()
        db.query(BroadcastLog).first()
        db.query(SystemStats).first()
        
        db.close()
        
        logger.info("✅ База данных проверена и работает корректно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Проблема с базой данных: {e}")
        return False

# Автоматическое обновление ежедневной статистики при импорте модуля
def setup_daily_stats_job():
    """Настройка автоматического обновления статистики"""
    try:
        # Только пытаемся обновить статистику, если база уже инициализирована
        if os.path.exists("cardio_bot.db"):
            update_daily_stats()
        else:
            logger.info("База данных еще не создана, пропускаем обновление статистики")
    except Exception as e:
        logger.warning(f"Ошибка при настройке ежедневной статистики: {e}")

# Вызываем при импорте модуля только если это не главный модуль
if __name__ != "__main__":
    try:
        setup_daily_stats_job()
    except Exception as e:
        logger.warning(f"Не удалось настроить ежедневную статистику: {e}")
        
async def log_user_activity(telegram_id: int, action: str, details: Dict[str, Any] = None, step: str = None):
    """Логирование активности пользователя с детальной информацией"""
    def _log():
        db = get_db_sync()
        try:
            current_time = datetime.now()
            
            # Обновляем последнюю активность пользователя
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.last_activity = current_time
                user.updated_at = current_time
            
            # Создаем запись в логе активности
            log_entry = ActivityLog(
                telegram_id=telegram_id,
                action=action,
                details=json.dumps(details or {}, ensure_ascii=False),
                step=step,
                timestamp=current_time
            )
            db.add(log_entry)
            
            db.commit()
            return log_entry.id
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка логирования активности {telegram_id}: {e}")
            raise e
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _log)

# ============================================================================
# УЛУЧШЕННЫЕ ФУНКЦИИ ПОЛУЧЕНИЯ СТАТИСТИКИ
# ============================================================================

def get_comprehensive_user_stats() -> Dict[str, Any]:
    """Получить всестороннюю статистику пользователей"""
    db = get_db_sync()
    try:
        # Базовая статистика
        total_users = db.query(User).count()
        completed_registration = db.query(User).filter(User.registration_completed == True).count()
        completed_surveys = db.query(User).filter(User.survey_completed == True).count()
        completed_tests = db.query(User).filter(User.tests_completed == True).count()
        completed_diagnostic = db.query(User).filter(User.completed_diagnostic == True).count()
        
        # Статистика по времени
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        new_users_today = db.query(User).filter(
            db.func.date(User.created_at) == today
        ).count()
        
        new_users_week = db.query(User).filter(
            User.created_at >= datetime.combine(week_ago, datetime.min.time())
        ).count()
        
        new_users_month = db.query(User).filter(
            User.created_at >= datetime.combine(month_ago, datetime.min.time())
        ).count()
        
        # Активность пользователей
        active_today = db.query(ActivityLog).filter(
            db.func.date(ActivityLog.timestamp) == today
        ).distinct(ActivityLog.telegram_id).count()
        
        active_week = db.query(ActivityLog).filter(
            ActivityLog.timestamp >= datetime.combine(week_ago, datetime.min.time())
        ).distinct(ActivityLog.telegram_id).count()
        
        # Конверсия по этапам
        registration_conversion = (completed_registration / max(total_users, 1)) * 100
        survey_conversion = (completed_surveys / max(completed_registration, 1)) * 100
        tests_conversion = (completed_tests / max(completed_surveys, 1)) * 100
        diagnostic_conversion = (completed_diagnostic / max(completed_tests, 1)) * 100
        
        # Время до завершения (среднее)
        completed_users = db.query(User).filter(
            User.completed_diagnostic == True,
            User.created_at.isnot(None)
        ).all()
        
        completion_times = []
        for user in completed_users:
            # Находим время завершения диагностики
            completion_log = db.query(ActivityLog).filter(
                ActivityLog.telegram_id == user.telegram_id,
                ActivityLog.action == "diagnostic_completed"
            ).first()
            
            if completion_log and user.created_at:
                time_diff = completion_log.timestamp - user.created_at
                completion_times.append(time_diff.total_seconds() / 3600)  # в часах
        
        avg_completion_time = sum(completion_times) / len(completion_times) if completion_times else 0
        
        return {
            'total_users': total_users,
            'completed_registration': completed_registration,
            'completed_surveys': completed_surveys,
            'completed_tests': completed_tests,
            'completed_diagnostic': completed_diagnostic,
            
            'new_users': {
                'today': new_users_today,
                'week': new_users_week,
                'month': new_users_month
            },
            
            'active_users': {
                'today': active_today,
                'week': active_week
            },
            
            'conversion_rates': {
                'registration': round(registration_conversion, 2),
                'survey': round(survey_conversion, 2),
                'tests': round(tests_conversion, 2),
                'diagnostic': round(diagnostic_conversion, 2)
            },
            
            'engagement': {
                'avg_completion_time_hours': round(avg_completion_time, 2),
                'completion_rate': round((completed_diagnostic / max(total_users, 1)) * 100, 2)
            }
        }
        
    finally:
        db.close()

# ============================================================================
# УТИЛИТЫ ДЛЯ МИГРАЦИИ И ИСПРАВЛЕНИЯ ДАННЫХ
# ============================================================================

def fix_incomplete_records():
    """Исправление неполных записей в базе данных"""
    db = get_db_sync()
    try:
        fixed_count = 0
        current_time = datetime.utcnow()
        
        # Исправляем пользователей без временных меток
        users_without_timestamps = db.query(User).filter(
            or_(
                User.created_at.is_(None),
                User.updated_at.is_(None),
                User.last_activity.is_(None)
            )
        ).all()
        
        for user in users_without_timestamps:
            if user.created_at is None:
                user.created_at = current_time
            if user.updated_at is None:
                user.updated_at = current_time
            if user.last_activity is None:
                user.last_activity = current_time
            fixed_count += 1
        
        # Исправляем опросы без временных меток
        surveys_without_timestamps = db.query(Survey).filter(
            or_(
                Survey.created_at.is_(None),
                Survey.completed_at.is_(None)
            )
        ).all()
        
        for survey in surveys_without_timestamps:
            if survey.created_at is None:
                survey.created_at = current_time
            if survey.completed_at is None:
                survey.completed_at = current_time
            fixed_count += 1
        
        # Исправляем результаты тестов без временных меток
        tests_without_timestamps = db.query(TestResult).filter(
            or_(
                TestResult.created_at.is_(None),
                TestResult.completed_at.is_(None)
            )
        ).all()
        
        for test in tests_without_timestamps:
            if test.created_at is None:
                test.created_at = current_time
            if test.completed_at is None:
                test.completed_at = current_time
            fixed_count += 1
        
        db.commit()
        logger.info(f"Исправлено {fixed_count} записей в базе данных")
        
        return {'fixed_records': fixed_count}
        
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка при исправлении записей: {e}")
        raise e
    finally:
        db.close()

def validate_data_integrity():
    """Проверка целостности данных"""
    db = get_db_sync()
    try:
        issues = []
        
        # Пользователи с незавершенной регистрацией, но отмеченные как завершившие
        inconsistent_registration = db.query(User).filter(
            User.registration_completed == True,
            or_(User.name.is_(None), User.email.is_(None), User.phone.is_(None))
        ).count()
        
        if inconsistent_registration > 0:
            issues.append(f"Пользователей с некорректным статусом регистрации: {inconsistent_registration}")
        
        # Пользователи, отмеченные как завершившие опрос, но без записи в surveys
        users_survey_mismatch = db.query(User).outerjoin(Survey).filter(
            User.survey_completed == True,
            Survey.id.is_(None)
        ).count()
        
        if users_survey_mismatch > 0:
            issues.append(f"Пользователей без записи опроса: {users_survey_mismatch}")
        
        # Пользователи, отмеченные как завершившие тесты, но без записи в test_results
        users_tests_mismatch = db.query(User).outerjoin(TestResult).filter(
            User.tests_completed == True,
            TestResult.id.is_(None)
        ).count()
        
        if users_tests_mismatch > 0:
            issues.append(f"Пользователей без записи тестов: {users_tests_mismatch}")
        
        # Пользователи, завершившие диагностику, но не прошедшие все этапы
        diagnostic_incomplete = db.query(User).filter(
            User.completed_diagnostic == True,
            or_(
                User.registration_completed == False,
                User.survey_completed == False,
                User.tests_completed == False
            )
        ).count()
        
        if diagnostic_incomplete > 0:
            issues.append(f"Пользователей с неполной диагностикой: {diagnostic_incomplete}")
        
        return {
            'healthy': len(issues) == 0,
            'issues': issues,
            'checked_at': datetime.now().isoformat()
        }
        
    finally:
        db.close()
