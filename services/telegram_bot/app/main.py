import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

from .settings import settings
from .router import router

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=(
            "Привет! Я бот для работы с базой фин. отчетности (RFSD).\n\n"
            "Примеры команд:\n"
            "1. 'ИНН 7722514880' -> получу базовые данные за 5 лет\n"
            "2. 'ИНН 7722514880 выручка' -> только динамика выручки\n"
            "3. 'ИНН 7722514880 xlsx' -> скачаю полный Excel-профиль\n"
        )
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user_text = update.message.text
    
    # Отправляем "печатает...", чтобы пользователь видел реакцию
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    
    # Роутинг
    result = await router.route_message(user_text)
    
    if result["type"] == "text":
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=result["content"]
        )
    elif result["type"] == "document":
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=result["content"],
            filename=result["filename"],
            caption=result.get("caption", "")
        )

def main():
    if not settings.TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN not found in .env")
        return

    application = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build()
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    
    print("Bot started... (Polling)")
    application.run_polling()

if __name__ == '__main__':
    main()
