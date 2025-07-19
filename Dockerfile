FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем содержимое папки bot прямо в /app
COPY bot/ .

# Копируем остальные папки, если нужно
COPY materials ./materials

ENV PYTHONPATH=/app

CMD ["python", "main.py"]