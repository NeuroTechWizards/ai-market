# RFSD Backend

Backend сервис для Russian Financial Statements Database (RFSD).

## Установка

```bash
cd services/rfsd_backend
pip install -r requirements.txt
```

## Запуск локально

**Важно:** Команды нужно выполнять из директории `services/rfsd_backend`:

```bash
cd services/rfsd_backend
uvicorn app.main:app --reload --port 8000
```

Сервис будет доступен по адресу: http://localhost:8000

## Quick Test

Быстрая проверка работоспособности всех endpoints:

1. Перейдите в директорию сервиса и запустите его в одном терминале:
   ```bash
   cd services/rfsd_backend
   uvicorn app.main:app --reload --port 8000
   ```

2. В другом терминале (также из `services/rfsd_backend`) выполните тестовый скрипт:
   ```powershell
   cd services/rfsd_backend
   .\scripts\test_endpoints.ps1
   ```

Скрипт проверит:
- `/health` — проверка здоровья сервиса
- `/rfsd/sample` — получение сэмпла данных (3 строки)
- `/rfsd/company_timeseries` — поиск компании по ИНН

## API

### Health Check

```bash
GET /health
```

Возвращает:
```json
{
  "status": "ok"
}
```

## Документация API

После запуска сервиса доступна интерактивная документация:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
