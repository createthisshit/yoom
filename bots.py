import asyncio
import logging
import sys
import time
from urllib.parse import urlencode

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

# Токен твоего Telegram-бота
TOKEN = '8195156027:AAFmGf_dltQ9ETpswU3U4UTuWv8eRPS16fU'

# Настройки YooMoney
YOOMONEY_RECEIVER = "4100118178122985"  # номер кошелька или идентификатор
YOOMONEY_BASE_URL = "https://yoomoney.ru/quickpay/confirm.xml"
PAYMENT_SUM = 200  # сумма платежа (можно менять по необходимости)

# Инициализация бота и диспетчера
bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

def generate_payment_link(chat_id: int, sum_value: int) -> str:
    """
    Формирует ссылку для оплаты через YooMoney с уникальным параметром label.
    Для уникальности label используется комбинация chat_id и текущей временной метки.
    """
    unique_label = f"{chat_id}_{int(time.time())}"
    params = {
        "receiver": YOOMONEY_RECEIVER,
        "quickpay-form": "shop",
        "targets": "Подписка",  # описание платежа
        "paymentType": "SB",     # способ оплаты (можно изменить, если нужно)
        "sum": sum_value,
        "label": unique_label
    }
    return f"{YOOMONEY_BASE_URL}?{urlencode(params)}"

@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    При команде /start бот генерирует ссылку на оплату через YooMoney.
    В ссылке в параметре label содержится уникальный идентификатор, включающий chat_id пользователя.
    """
    chat_id = message.chat.id
    payment_link = generate_payment_link(chat_id, PAYMENT_SUM)
    text = (
        "Чтобы активировать подписку, перейдите по следующей ссылке для оплаты:\n"
        f"<a href='{payment_link}'>Оплатить подписку</a>\n\n"
        "После оплаты вы получите уведомление в боте."
    )
    await message.answer(text, disable_web_page_preview=True)

async def main() -> None:
    # Запускаем бота
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
