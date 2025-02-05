import asyncio
import logging
import sys
from aiogram import Bot, Dispatcher, html, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import ReplyKeyboardMarkup, ReplyKeyboardRemove,KeyboardButton
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import Message, LabeledPrice, PreCheckoutQuery

# Bot token can be obtained via https://t.me/BotFather
TOKEN = '8195156027:AAFmGf_dltQ9ETpswU3U4UTuWv8eRPS16fU'
# All handlers should be attached to the Router (or Dispatcher)
bot = Bot(token=TOKEN, default=ParseMode.HTML)
dp = Dispatcher()
CURRENCY = 'XTR'


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer_invoice(
        title="Подписка на 30 дней",
        description="Оплатить и получить ссылку",
        payload="access_to_private",
        currency="XTR",
        prices=[LabeledPrice(label="XTR", amount=1)]
    )
@dp.pre_checkout_query()
async def pre_checkout_handler(event: PreCheckoutQuery) -> None:
    await event.answer(True)

@dp.message(F.successful_payment)
async def successful_payment(message: Message) -> None:
    link = await bot.create_chat_invite_link(-1002291268265, member_limit=1)
    await message.answer(f"Твоя ссылка:\n{link.invite_link}")

async def main() -> None:
    # Initialize Bot instance with default bot properties which will be passed to all API calls
    bot = Bot(token='8195156027:AAFmGf_dltQ9ETpswU3U4UTuWv8eRPS16fU', default=DefaultBotProperties(parse_mode=ParseMode.HTML))

    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
