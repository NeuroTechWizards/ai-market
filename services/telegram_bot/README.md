# Telegram Bot for RFSD

Бот-интерфейс для взаимодействия с RFSD Backend.

## Установка

1. Создайте `.env` файл в этой папке:
   ```
   TELEGRAM_BOT_TOKEN=ваш_токен
   RFSD_BACKEND_URL=http://127.0.0.1:8000
   ```

2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```

## Запуск

1. Убедитесь, что backend запущен:
   ```bash
   cd ../rfsd_backend
   uvicorn app.main:app --reload --port 8000
   ```

2. Запустите бота:
   ```bash
   python -m app.main
   ```
