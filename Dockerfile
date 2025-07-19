# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта
COPY . .

# Создаем директорию для базы данных
RUN mkdir -p /app/data

# Указываем переменные окружения
ENV PYTHONPATH=/app
ENV BOT_TOKEN=""
ENV ADMIN_IDS=""

# Экспонируем порт (если понадобится веб-интерфейс)
EXPOSE 8000

# Команда для запуска бота
CMD ["python", "main.py"]