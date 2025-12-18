import asyncio
import os
from openai import AsyncOpenAI
import sys

# Пытаемся найти .env вручную, если библиотека не подгрузила
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

async def check():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # Попробуем найти в settings, если запуск из модуля
        print("❌ OPENAI_API_KEY не найден в переменных окружения.")
        print("Убедитесь, что вы добавили его в .env файл.")
        return

    print(f"🔑 Ключ найден: {api_key[:8]}...{api_key[-4:]}")
    print("⏳ Пробую сделать тестовый запрос (gpt-4o-mini)...")

    client = AsyncOpenAI(api_key=api_key)
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Test connection. Reply 'OK'."}],
            max_tokens=10
        )
        print(f"✅ УСПЕХ! Ответ API: {response.choices[0].message.content}")
        print("Баланс положительный, ключ рабочий.")
    except Exception as e:
        print("\n❌ ОШИБКА API:")
        print(f"{e}")
        
        err_str = str(e)
        if "insufficient_quota" in err_str:
            print("\n💰 ДИАГНОЗ: Закончились деньги на счету (Insufficient Quota).")
            print("Нужно пополнить баланс на https://platform.openai.com/")
        elif "rate_limit" in err_str:
            print("\n🚦 ДИАГНОЗ: Превышен лимит запросов (Rate Limit).")
        elif "invalid_api_key" in err_str or "Incorrect API key" in err_str:
            print("\n🔑 ДИАГНОЗ: Неверный ключ API.")

if __name__ == "__main__":
    try:
        asyncio.run(check())
    except KeyboardInterrupt:
        pass
