import asyncio
import logging
import sys
import os
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery
import requests

# Получаем токен из переменных окружения
TOKEN = os.getenv("8195156027:AAFmGf_dltQ9ETpswU3U4UTuWv8eRPS16fU")
YOOMONEY_WALLET = "4100118178122985"  # Укажи свой YooMoney кошелек
YOOMONEY_API_KEY = os.getenv("JBv8vNP6BqehGUYQuF2tTelW")  # Ключ API YooMoney

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
CURRENCY = "XTR"

# Telegram Stars (как у тебя было раньше)
@dp.message(F.text == "/pay_stars")
async def pay_with_stars(message: Message):
    await message.answer_invoice(
        title="Подписка на 30 дней",
        description="Оплатить и получить ссылку",
        payload="access_to_private",
        currency="XTR",
        prices=[LabeledPrice(label="XTR", amount=350)]
    )

@dp.pre_checkout_query()
async def pre_checkout_handler(event: PreCheckoutQuery):
    await event.answer(True)

@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    link = await bot.create_chat_invite_link(-1002291268265, member_limit=1)
    await message.answer(f"Твоя ссылка:\n{link.invite_link}")

# YooMoney (новая функция)
@dp.message(F.text == "/pay_yoomoney")
async def pay_with_yoomoney(message: Message):
    amount = "100.00"  # Укажи нужную сумму
    payment_link = f"https://yoomoney.ru/quickpay/confirm.xml?receiver={YOOMONEY_WALLET}&sum={amount}&quickpay-form=shop&paymentType=AC"
    await message.answer(f"Оплатите по ссылке: {payment_link}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
