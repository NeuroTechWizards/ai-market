import logging
import asyncio
import os
import getpass
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

from .settings import settings
from .router import router

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я RFSD бот. Пришлите ИНН (10/12 цифр) и запрос.\n"
        "Например: 'ИНН 7722514880 xlsx' (полный отчет) или 'ИНН 7722514880 выручка'."
    )

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_text = update.message.text
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    
    result = await router.route_message(user_text)
    
    if result["type"] == "text":
        await update.message.reply_text(result["content"])
    elif result["type"] == "document":
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=result["content"],
            filename=result["filename"],
            caption=result.get("caption", "")
        )

async def runner():
    if not settings.TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN не найден в переменных окружения (.env).")
        try:
            token_input = getpass.getpass("Введите TELEGRAM_BOT_TOKEN: ").strip()
            if token_input:
                settings.TELEGRAM_BOT_TOKEN = token_input
                os.environ["TELEGRAM_BOT_TOKEN"] = token_input
            else:
                print("Токен не введен. Запуск невозможен.")
                return
        except Exception as e:
            print(f"Ошибка ввода: {e}")
            return

    application = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build()
    
    application.add_handler(CommandHandler('start', start_cmd))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_text))
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    logger.info("Bot polling started")
    
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("Stopping bot...")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Bot stopped")

def main():
    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
