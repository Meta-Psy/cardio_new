import pandas as pd
import sys
import os
import json
from datetime import datetime
from bot.database import (
    get_db_sync, User, Survey, TestResult, ActivityLog,
    init_db, ensure_database_exists
)

def import_users_from_excel(excel_file: str):
    """Импорт пользователей из Excel файла"""
    
    print(f"Загружаю данные из {excel_file}...")
    
    try:
        # Читаем Excel файл
        df = pd.read_excel(excel_file, sheet_name=0)  # Первый лист
        print(f"Найдено строк в Excel: {len(df)}")
        
        # Показываем колонки
        print("Колонки в файле:")
        for i, col in enumerate(df.columns):
            print(f"  {i}: {col}")
        
        db = get_db_sync()
        imported = 0
        
        for index, row in df.iterrows():
            try:
                # Извлекаем основные данные пользователя
                telegram_id = int(row.get('telegram_id', 0))
                if telegram_id == 0:
                    print(f"Пропускаю строку {index}: нет telegram_id")
                    continue
                
                name = str(row.get('name', '')).strip() if pd.notna(row.get('name')) else None
                email = str(row.get('email', '')).strip() if pd.notna(row.get('email')) else None
                phone = str(row.get('phone', '')).strip() if pd.notna(row.get('phone')) else None
                
                # Статусы
                completed_diagnostic = bool(row.get('completed_diagnostic', False))
                registration_completed = bool(row.get('registration_completed', False))
                survey_completed = bool(row.get('survey_completed', False))
                tests_completed = bool(row.get('tests_completed', False))
                
                # Проверяем логику статусов
                if name and email and phone:
                    registration_completed = True
                
                # Временные метки
                current_time = datetime.utcnow()
                created_at = current_time
                if pd.notna(row.get('registration_date')):
                    try:
                        created_at = pd.to_datetime(row.get('registration_date'))
                    except:
                        pass
                
                # Создаем пользователя
                user = User(
                    telegram_id=telegram_id,
                    name=name,
                    email=email,
                    phone=phone,
                    completed_diagnostic=completed_diagnostic,
                    registration_completed=registration_completed,
                    survey_completed=survey_completed,
                    tests_completed=tests_completed,
                    created_at=created_at,
                    updated_at=current_time,
                    last_activity=current_time
                )
                
                # Удаляем существующего пользователя если есть
                existing = db.query(User).filter(User.telegram_id == telegram_id).first()
                if existing:
                    db.delete(existing)
                    db.flush()
                
                db.add(user)
                
                # Импортируем опрос если есть данные
                if survey_completed and import_survey_data(db, telegram_id, row):
                    print(f"  + Импортирован опрос для {name or telegram_id}")
                
                # Импортируем тесты если есть данные
                if tests_completed and import_test_data(db, telegram_id, row):
                    print(f"  + Импортированы тесты для {name or telegram_id}")
                
                db.commit()
                imported += 1
                print(f"✅ Импортирован пользователь: {name or telegram_id} (ID: {telegram_id})")
                
            except Exception as e:
                print(f"❌ Ошибка импорта строки {index}: {e}")
                db.rollback()
                continue
        
        db.close()
        print(f"\n🎉 Импорт завершен! Импортировано пользователей: {imported}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

def import_survey_data(db, telegram_id: int, row) -> bool:
    """Импорт данных опроса"""
    try:
        # Основные демографические данные
        age = int(row.get('age', 0)) if pd.notna(row.get('age')) else None
        gender = str(row.get('gender', '')).strip() if pd.notna(row.get('gender')) else None
        location = str(row.get('location', '')).strip() if pd.notna(row.get('location')) else None
        education = str(row.get('education', '')).strip() if pd.notna(row.get('education')) else None
        
        # Дополнительные поля опроса
        family_status = str(row.get('family_status', '')).strip() if pd.notna(row.get('family_status')) else None
        children = str(row.get('children', '')).strip() if pd.notna(row.get('children')) else None
        income = str(row.get('income', '')).strip() if pd.notna(row.get('income')) else None
        
        # Здоровье
        health_rating = int(row.get('health_rating', 0)) if pd.notna(row.get('health_rating')) else None
        death_cause = str(row.get('death_cause', '')).strip() if pd.notna(row.get('death_cause')) else None
        heart_disease = str(row.get('heart_disease', '')).strip() if pd.notna(row.get('heart_disease')) else None
        cv_risk = str(row.get('cv_risk', '')).strip() if pd.notna(row.get('cv_risk')) else None
        cv_knowledge = str(row.get('cv_knowledge', '')).strip() if pd.notna(row.get('cv_knowledge')) else None
        health_importance = str(row.get('health_importance', '')).strip() if pd.notna(row.get('health_importance')) else None
        
        # История обследований
        checkup_history = str(row.get('checkup_history', '')).strip() if pd.notna(row.get('checkup_history')) else None
        
        # JSON поля (если есть в Excel)
        def safe_json_field(value):
            if pd.isna(value) or value == '':
                return None
            if isinstance(value, str):
                # Если уже JSON - возвращаем как есть
                if value.startswith('[') or value.startswith('{'):
                    return value
                # Иначе делаем список
                return json.dumps([value], ensure_ascii=False)
            return json.dumps([str(value)], ensure_ascii=False)
        
        heart_danger = safe_json_field(row.get('heart_danger'))
        checkup_content = safe_json_field(row.get('checkup_content'))
        prevention_barriers = safe_json_field(row.get('prevention_barriers'))
        health_advice = safe_json_field(row.get('health_advice'))
        
        # Проверяем, есть ли минимальные данные для создания опроса
        if not (age or gender or health_rating):
            return False
        
        # Удаляем существующий опрос
        existing_survey = db.query(Survey).filter(Survey.telegram_id == telegram_id).first()
        if existing_survey:
            db.delete(existing_survey)
            db.flush()
        
        # Создаем опрос
        survey = Survey(
            telegram_id=telegram_id,
            age=age,
            gender=gender,
            location=location,
            education=education,
            family_status=family_status,
            children=children,
            income=income,
            health_rating=health_rating,
            death_cause=death_cause,
            heart_disease=heart_disease,
            cv_risk=cv_risk,
            cv_knowledge=cv_knowledge,
            heart_danger=heart_danger,
            health_importance=health_importance,
            checkup_history=checkup_history,
            checkup_content=checkup_content,
            prevention_barriers=prevention_barriers,
            prevention_barriers_other=None,
            health_advice=health_advice,
            created_at=datetime.utcnow(),
            completed_at=datetime.utcnow()
        )
        
        db.add(survey)
        return True
        
    except Exception as e:
        print(f"Ошибка импорта опроса для {telegram_id}: {e}")
        return False

def import_test_data(db, telegram_id: int, row) -> bool:
    """Импорт результатов тестов"""
    try:
        # Извлекаем данные тестов
        test_fields = {
            'hads_anxiety_score': 'hads_anxiety',
            'hads_depression_score': 'hads_depression', 
            'burns_score': 'burns_score',
            'isi_score': 'isi_score',
            'stop_bang_score': 'stop_bang_score',
            'ess_score': 'ess_score',
            'fagerstrom_score': 'fagerstrom_score',
            'audit_score': 'audit_score',
        }
        
        # Проверяем наличие данных тестов
        has_test_data = False
        test_data = {}
        
        for db_field, excel_field in test_fields.items():
            value = row.get(excel_field)
            if pd.notna(value) and value != '':
                test_data[db_field] = int(float(value))
                has_test_data = True
            else:
                test_data[db_field] = None
        
        if not has_test_data:
            return False
        
        # Рассчитываем общий HADS score
        hads_total = 0
        if test_data['hads_anxiety_score'] and test_data['hads_depression_score']:
            hads_total = test_data['hads_anxiety_score'] + test_data['hads_depression_score']
        
        # Определяем уровни рисков (упрощенно)
        def get_risk_level(test_type, score):
            if score is None:
                return None
            
            if test_type in ['hads_anxiety_score', 'hads_depression_score']:
                if score <= 7: return 'норма'
                elif score <= 10: return 'субклиническая'
                else: return 'клиническая'
            elif test_type == 'burns_score':
                if score <= 5: return 'минимальная'
                elif score <= 10: return 'легкая'
                elif score <= 25: return 'умеренная'
                else: return 'тяжелая'
            elif test_type == 'isi_score':
                if score <= 7: return 'нет_бессонницы'
                elif score <= 14: return 'подпороговая'
                else: return 'умеренная'
            elif test_type == 'stop_bang_score':
                if score <= 2: return 'низкий'
                elif score <= 4: return 'умеренный'
                else: return 'высокий'
            
            return 'норма'
        
        # Рассчитываем общий риск (упрощенно)
        risk_score = 0
        if test_data['hads_anxiety_score'] and test_data['hads_anxiety_score'] >= 8:
            risk_score += 1
        if test_data['hads_depression_score'] and test_data['hads_depression_score'] >= 8:
            risk_score += 2
        if test_data['burns_score'] and test_data['burns_score'] >= 11:
            risk_score += 1
        if test_data['stop_bang_score'] and test_data['stop_bang_score'] >= 3:
            risk_score += 2
        
        if risk_score <= 1:
            overall_risk = 'НИЗКИЙ'
        elif risk_score <= 3:
            overall_risk = 'УМЕРЕННЫЙ'
        elif risk_score <= 5:
            overall_risk = 'ВЫСОКИЙ'
        else:
            overall_risk = 'ОЧЕНЬ ВЫСОКИЙ'
        
        # Удаляем существующие результаты
        existing_tests = db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
        if existing_tests:
            db.delete(existing_tests)
            db.flush()
        
        # Создаем результаты тестов
        test_result = TestResult(
            telegram_id=telegram_id,
            hads_anxiety_score=test_data['hads_anxiety_score'],
            hads_depression_score=test_data['hads_depression_score'],
            hads_total_score=hads_total,
            hads_anxiety_level=get_risk_level('hads_anxiety_score', test_data['hads_anxiety_score']),
            hads_depression_level=get_risk_level('hads_depression_score', test_data['hads_depression_score']),
            burns_score=test_data['burns_score'],
            burns_level=get_risk_level('burns_score', test_data['burns_score']),
            isi_score=test_data['isi_score'],
            isi_level=get_risk_level('isi_score', test_data['isi_score']),
            stop_bang_score=test_data['stop_bang_score'],
            stop_bang_risk=get_risk_level('stop_bang_score', test_data['stop_bang_score']),
            ess_score=test_data['ess_score'],
            ess_level=get_risk_level('ess_score', test_data['ess_score']),
            fagerstrom_score=test_data['fagerstrom_score'],
            fagerstrom_level=get_risk_level('fagerstrom_score', test_data['fagerstrom_score']),
            fagerstrom_skipped=test_data['fagerstrom_score'] is None,
            audit_score=test_data['audit_score'],
            audit_level=get_risk_level('audit_score', test_data['audit_score']),
            audit_skipped=test_data['audit_score'] is None,
            overall_cv_risk_score=risk_score,
            overall_cv_risk_level=overall_risk,
            risk_factors_count=risk_score,
            created_at=datetime.utcnow(),
            completed_at=datetime.utcnow()
        )
        
        db.add(test_result)
        return True
        
    except Exception as e:
        print(f"Ошибка импорта тестов для {telegram_id}: {e}")
        return False

def main():
    """Основная функция"""
    if len(sys.argv) != 2:
        print("Использование: python import_from_excel.py data.xlsx")
        return
    
    excel_file = sys.argv[1]
    
    if not os.path.exists(excel_file):
        print(f"Файл {excel_file} не найден!")
        return
    
    # Инициализируем БД
    print("Инициализирую базу данных...")
    ensure_database_exists()
    init_db()
    
    # Импортируем данные
    import_users_from_excel(excel_file)
    
    print("\n✅ Импорт завершен!")

if __name__ == "__main__":
    main()