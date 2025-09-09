# PostgreSQL Query Guardian
Функционал

- Анализ запроса: оценка стоимости, метрик, блокировок.
- Рекомендации: индексы, настройки, переписывание, секционирование.
- История: хранение анализов, просмотр статистики.
- Аналитика: тепловая карта проблем, топ-таблицы, топ-проблемы.
- Информация о БД: структура, индексы, размеры, автообслуживание.
- Обратная связь: WebSocket, логирование, уведомления.
- Интеграция: CLI для CI/CD, REST API.

Быстрый старт

▌1. Клонирование репозитория
bash
git clone https://github.com/your-team/hacaton-pg-guardian.git
cd hacaton-pg-guardian
▌2. Установка зависимостей
bash
python -m venv venv
source venv/bin/activate # или venv\Scripts\activate на Windows
pip install -r requirements.txt
▌3. Запуск backend (FastAPI)
bash
uvicorn main:hacaton --reload --host 0.0.0.0 --port 8000
▌4. Запуск frontend
Откройте любой HTML-файл из папки frontend/ в браузере (например, index.html). 
Для работы с API убедитесь, что backend запущен и доступен по адресу http://localhost:8000.
▌5. CLI для CI/CD
bash
python -m cli.guard --query "SELECT * FROM big_table" --output json
▌6. Подключение к БД

На главной странице веб-интерфейса нажмите «Подключиться к БД» и введите параметры PostgreSQL.

▌API

- POST /analyze — анализ запроса, получение метрик и рекомендаций.
- GET /history — история анализов.
- GET /dbinfo — информация о базе данных.
- GET /heatmap — аналитика по проблемам.
- POST /check_connection — проверка подключения к БД.
- POST /rules/upload — загрузка кастомных YAML-правил.

▌Технологии

- Backend: Python, FastAPI, psycopg2
- Frontend: HTML, CSS, JS (vanilla)
- База данных: PostgreSQL
- Интеграция: REST API
