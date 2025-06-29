import logging
import sys
import uuid
import hashlib
import requests
import asyncio
import os
import qrcode
import base64
from io import BytesIO
from typing import Dict, Optional, Tuple
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web, ClientSession
import asyncpg
from config import fetch_bot_settings
import time
import random
import string

# Настройка логирования
def generate_log_id() -> str:
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

class LogIdFilter(logging.Filter):
    def __init__(self, log_id: str):
        super().__init__()
        self.log_id = log_id

    def filter(self, record):
        record.log_id = self.log_id
        return True

log_id = generate_log_id()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(log_id)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)
logger.addFilter(LogIdFilter(log_id))
logger.info(f"Starting subscription bot [{log_id}]")

# Конфигурации
PAYMENT_STORE = "/store_payment"
YOOMONEY_HOOK = "/yoomoney_hook"
HEALTH_CHECK = "/status"
WEBHOOK_BASE = "/bot_hook"
DB_URL = os.getenv("DB_URL", "postgresql://postgres.iylthyqzwovudjcyfubg:Alex4382!@aws-0-eu-central-1.pooler.supabase.com:6543/postgres")
HOST_URL = os.getenv("HOST_URL", "https://yoom-production.up.railway.app")
TON_ADDRESS = "UQBLNUOpN5B0q_M2xukAB5MsfSCUsdE6BkXHO6ndogQDi5_6"
BTC_ADDRESS = "bc1q5xq9m473r8nnkx799ztcrwfqs0555fs3ulw9vr"
USDT_ADDRESS = "TQzs3V6QHdXb3CtNPYK9iPWuvvrYCPt6vE"
PAYPAL_EMAIL = "nemillingsuppay@gmail.com"
ENV = os.getenv("ENV", "railway")
logger.debug(f"Platform: {ENV} [{log_id}]")

# Кэш для курсов
crypto_cache: Dict[str, Optional[Tuple[float, float, float]]] = {"prices": None, "timestamp": 0}
CACHE_TIMEOUT = 300

async def get_db_pool() -> asyncpg.Pool:
    try:
        pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=10)
        logger.info(f"Database pool created [{log_id}]")
        return pool
    except Exception as e:
        logger.error(f"Failed to create database pool [{log_id}]: {e}")
        sys.exit(1)

async def setup_database(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        try:
            for bot_key in SETTINGS:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS payments_{bot_key}
                    (label TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, payment_type TEXT)
                """)
                await conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_payments_{bot_key}_label ON payments_{bot_key} (label)
                """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_languages
                (user_id TEXT PRIMARY KEY, language TEXT NOT NULL)
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS webhooks
                (bot_key TEXT PRIMARY KEY, webhook_url TEXT NOT NULL)
            """)
            logger.info(f"Database setup for {len(SETTINGS)} bots [{log_id}]")
        except Exception as e:
            logger.error(f"Database setup error [{log_id}]: {e}")
            sys.exit(1)

def get_usd_from_rub(rub_amount: float) -> float:
    try:
        usd_rate = random.uniform(95.0, 105.0)
        logger.debug(f"Converting {rub_amount} RUB to USD at rate {usd_rate} [{log_id}]")
        return rub_amount / usd_rate
    except Exception as e:
        logger.error(f"Error converting RUB to USD [{log_id}]: {e}")
        return rub_amount / 100.0

def get_crypto_prices() -> Tuple[float, float, float]:
    global crypto_cache
    current_time = time.time()
    if crypto_cache["prices"] and (current_time - crypto_cache["timestamp"]) < CACHE_TIMEOUT:
        logger.debug(f"Using cached crypto prices [{log_id}]")
        return crypto_cache["prices"]
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network,bitcoin,tether&vs_currencies=usd",
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        crypto_cache["prices"] = (
            data["the-open-network"]["usd"],
            data["bitcoin"]["usd"],
            data["tether"]["usd"]
        )
        crypto_cache["timestamp"] = current_time
        logger.debug(f"Crypto prices updated: {crypto_cache['prices']} [{log_id}]")
        return crypto_cache["prices"]
    except Exception as e:
        logger.error(f"Error fetching crypto prices [{log_id}]: {e}")
        return 5.0, 60000.0, 1.0

def generate_qr_code(data: str) -> Optional[str]:
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill="black", back_color="white")
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        qr_base64 = base64.b64encode(buffered.getvalue()).decode()
        logger.debug(f"QR code generated for data: {data} [{log_id}]")
        return qr_base64
    except Exception as e:
        logger.error(f"Error generating QR code [{log_id}]: {e}")
        return None

# Загрузка конфигураций ботов
SETTINGS = fetch_bot_settings()
logger.info(f"Configuring {len(SETTINGS)} bots [{log_id}]")
bot_instances: Dict[str, Bot] = {}
dispatchers: Dict[str, Dispatcher] = {}

for bot_key, cfg in SETTINGS.items():
    try:
        logger.debug(f"Initializing bot {bot_key} with token {cfg['TOKEN'][:10]}... [{log_id}]")
        bot_instances[bot_key] = Bot(token=cfg["TOKEN"])
        dispatchers[bot_key] = Dispatcher(bot_instances[bot_key])
        logger.info(f"Bot {bot_key} initialized [{log_id}]")
    except Exception as e:
        logger.error(f"Error initializing bot {bot_key} [{log_id}]: {e}")
        sys.exit(1)

def create_follow_button() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("Follow", callback_data="follow"))
    return keyboard

def create_language_buttons() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("English", callback_data="lang_en"),
        InlineKeyboardButton("Русский", callback_data="lang_ru"),
        InlineKeyboardButton("Українська", callback_data="lang_uk"),
        InlineKeyboardButton("Türkçe", callback_data="lang_tr"),
        InlineKeyboardButton("हिन्दी", callback_data="lang_hi")
    )
    return keyboard

def create_payment_buttons(user_id: str, language: str) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup()
    buttons = {
        "ru": [
            ("ЮMoney", f"yoomoney_{user_id}"),
            ("TON", f"ton_{user_id}"),
            ("BTC", f"btc_{user_id}"),
            ("USDT TRC20", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ],
        "en": [
            ("TON", f"ton_{user_id}"),
            ("BTC", f"btc_{user_id}"),
            ("USDT TRC20", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ],
        "uk": [
            ("TON", f"ton_{user_id}"),
            ("BTC", f"btc_{user_id}"),
            ("USDT TRC20", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ],
        "tr": [
            ("TON", f"ton_{user_id}"),
            ("BTC", f"btc_{user_id}"),
            ("USDT TRC20", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ],
        "hi": [
            ("TON", f"ton_{user_id}"),
            ("BTC", f"btc_{user_id}"),
            ("USDT TRC20", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ]
    }
    for text, callback in buttons.get(language, buttons["en"]):
        keyboard.add(InlineKeyboardButton(text, callback_data=callback))
    return keyboard

async def get_user_language(pool: asyncpg.Pool, user_id: str) -> str:
    async with pool.acquire() as conn:
        try:
            language = await conn.fetchval(
                "SELECT language FROM user_languages WHERE user_id = $1", user_id
            )
            language = language or "en"
            logger.debug(f"User {user_id} language: {language} [{log_id}]")
            return language
        except Exception as e:
            logger.error(f"Error fetching language for {user_id} [{log_id}]: {e}")
            return "en"

async def save_user_language(pool: asyncpg.Pool, user_id: str, language: str):
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO user_languages (user_id, language) VALUES ($1, $2) "
                "ON CONFLICT (user_id) DO UPDATE SET language = $2",
                user_id, language
            )
            logger.debug(f"Language {language} saved for user {user_id} [{log_id}]")
        except Exception as e:
            logger.error(f"Error saving language for {user_id} [{log_id}]: {e}")

async def handle_crypto_or_paypal_payment(
    cb: types.CallbackQuery,
    bot_key: str,
    payment_type: str,
    pool: asyncpg.Pool,
    address: Optional[str] = None,
    price_index: Optional[int] = None,
    decimals: int = 2
):
    try:
        user_id = cb.data.split("_")[1]
        chat_id = cb.message.chat.id
        bot = bot_instances[bot_key]
        cfg = SETTINGS[bot_key]
        language = await get_user_language(pool, user_id)
        await bot.answer_callback_query(cb.id)
        logger.info(f"[{bot_key}] Payment {payment_type} selected by user {user_id} [{log_id}]")

        payment_id = str(uuid.uuid4())
        price = cfg["PRICE"]["ru"] if language == "ru" else cfg["PRICE"][language]
        price = price * 0.5  # 50% discount
        usd_amount = get_usd_from_rub(price) if language == "ru" else price

        if payment_type in ["ton", "btc", "usdt"]:
            ton_price, btc_price, usdt_price = get_crypto_prices()
            prices = {"ton": ton_price, "btc": btc_price, "usdt": usdt_price}
            amount = usd_amount / prices[payment_type]
            amount = round(amount, 4 if payment_type == "ton" else 8 if payment_type == "btc" else 2)
            addresses = {"ton": TON_ADDRESS, "btc": BTC_ADDRESS, "usdt": USDT_ADDRESS}
            address = addresses[payment_type]
            qr_data = f"{payment_type}://{address}?amount={amount}" if payment_type != "usdt" else address
            qr_base64 = generate_qr_code(qr_data)
            if qr_base64:
                qr_bytes = base64.b64decode(qr_base64)
                await bot.send_photo(
                    chat_id,
                    photo=qr_bytes,
                    caption=f"{address}",
                    protect_content=True,
                    has_spoiler=True if bot_key in ["bot10", "bot11", "bot13"] else False
                )
            else:
                await bot.send_message(chat_id, f"{address}", protect_content=True)
        else:  # PayPal
            amount = price
            currency = "RUB" if language == "ru" else "USD"

        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES ($1, $2, $3, $4)",
                payment_id, user_id, "pending", payment_type
            )
        logger.info(f"[{bot_key}] Payment {payment_id} ({payment_type}) saved for {user_id} [{log_id}]")

        prompt = {
            "en": f"Send {amount:.{decimals}f} {payment_type.upper()} to {address or PAYPAL_EMAIL}" +
                  (f". Include Telegram ID ({user_id}) in note." if payment_type == "paypal" else ""),
            "ru": f"Отправьте {amount:.{decimals}f} {payment_type.upper()} на {address or PAYPAL_EMAIL}" +
                  (f". Укажите Telegram ID ({user_id}) в заметке." if payment_type == "paypal" else ""),
            "uk": f"Надішліть {amount:.{decimals}f} {payment_type.upper()} на {address or PAYPAL_EMAIL}" +
                  (f". Вкажіть Telegram ID ({user_id}) у примітці." if payment_type == "paypal" else ""),
            "tr": f"{amount:.{decimals}f} {payment_type.upper()} adresine {address or PAYPAL_EMAIL} gönderin" +
                  (f". Notta Telegram ID ({user_id}) belirtin." if payment_type == "paypal" else ""),
            "hi": f"{amount:.{decimals}f} {payment_type.upper()} को {address or PAYPAL_EMAIL} पर भेजें" +
                  (f". नोट में Telegram ID ({user_id}) शामिल करें।" if payment_type == "paypal" else "")
        }
        await bot.send_message(chat_id, prompt[language], protect_content=True)
        logger.info(f"[{bot_key}] {payment_type} instructions sent to {user_id} (protect_content=True) [{log_id}]")
    except Exception as e:
        logger.error(f"[{bot_key}] Error processing {payment_type} for {user_id} [{log_id}]: {e}")
        await bot_instances[bot_key].send_message(chat_id, "Payment error.", protect_content=True)

def register_handlers(dp: Dispatcher, bot_key: str, pool: asyncpg.Pool):
    @dp.message_handler(commands=["start"])
    async def initiate_banner(msg: types.Message):
        try:
            user_id = str(msg.from_user.id)
            chat_id = msg.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            logger.info(f"[{bot_key}] /start command from {user_id} [{log_id}]")

            keyboard = create_follow_button()
            caption = {
                "en": "Welcome! Click Follow to continue.",
                "ru": "Добро пожаловать! Нажмите Подписаться, чтобы продолжить.",
                "uk": "Ласкаво просимо! Натисніть Підписатися, щоб продовжити.",
                "tr": "Hoş geldiniz! Devam etmek için Takip Et'e tıklayın.",
                "hi": "स्वागत है! जारी रखने के लिए फॉलो करें पर क्लिक करें।"
            }.get(await get_user_language(pool, user_id), "en")

            await bot.send_photo(
                chat_id,
                photo=cfg.get("START_PHOTO", "https://via.placeholder.com/800x400.png?text=Welcome+Banner"),
                caption=caption,
                reply_markup=keyboard,
                protect_content=True,
                has_spoiler=True if bot_key in ["bot10", "bot11", "bot13"] else False
            )
            logger.info(f"[{bot_key}] Banner sent to {user_id} (protect_content=True, has_spoiler={bot_key in ['bot10', 'bot11', 'bot13']}) [{log_id}]")
        except Exception as e:
            logger.error(f"[{bot_key}] Error in /start for {user_id} [{log_id}]: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Error.", protect_content=True)

    @dp.callback_query_handler(lambda c: c.data == "follow")
    async def handle_follow_choice(cb: types.CallbackQuery):
        try:
            user_id = str(cb.from_user.id)
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_key}] Follow button pressed by {user_id} [{log_id}]")

            keyboard = create_language_buttons()
            welcome_text = {
                "en": "Please select your language:",
                "ru": "Выберите язык:",
                "uk": "Оберіть мову:",
                "tr": "Lütfen dilinizi seçin:",
                "hi": "कृपया अपनी भाषा चुनें:"
            }.get(await get_user_language(pool, user_id), "en")
            await bot.send_message(
                chat_id,
                welcome_text,
                reply_markup=keyboard,
                protect_content=True
            )
            logger.info(f"[{bot_key}] Language selection sent to {user_id} (protect_content=True) [{log_id}]")
        except Exception as e:
            logger.error(f"[{bot_key}] Error processing Follow for {user_id} [{log_id}]: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Error.", protect_content=True)

    @dp.callback_query_handler(lambda c: c.data.startswith("lang_"))
    async def handle_language_choice(cb: types.CallbackQuery):
        try:
            user_id = str(cb.from_user.id)
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = cb.data.split("_")[1]
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_key}] Language {language} selected by {user_id} [{log_id}]")

            await save_user_language(pool, user_id, language)
            keyboard = create_payment_buttons(user_id, language)
            price = cfg["PRICE"][language] * 0.5  # 50% discount
            original_price = cfg["PRICE"][language]
            welcome_msg = cfg["DESCRIPTION"][language].format(price=price, original_price=original_price)
            currency = "RUB" if language == "ru" else "USD"
            payment_prompt = {
                "en": f"{welcome_msg}\n\nChoose payment method for {price} {currency}:",
                "ru": f"{welcome_msg}\n\nВыберите способ оплаты для {price} {currency}:",
                "uk": f"{welcome_msg}\n\nОберіть спосіб оплати для {price} {currency}:",
                "tr": f"{welcome_msg}\n\n{price} {currency} için ödeme yöntemi seçin:",
                "hi": f"{welcome_msg}\n\n{price} {currency} के लिए भुगतान विधि चुनें:"
            }
            await bot.send_message(chat_id, payment_prompt[language], reply_markup=keyboard, protect_content=True)
            logger.info(f"[{bot_key}] Payment options sent to {user_id} in {language} (protect_content=True) [{log_id}]")
        except Exception as e:
            logger.error(f"[{bot_key}] Error selecting language for {user_id} [{log_id}]: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Error selecting language.", protect_content=True)

    @dp.callback_query_handler(lambda c: c.data.startswith("yoomoney_"))
    async def handle_yoomoney_choice(cb: types.CallbackQuery):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = await get_user_language(pool, user_id)
            await bot.answer_callback_query(cb.id)
            logger.info(f"[{bot_key}] YooMoney selected by {user_id} [{log_id}]")

            payment_id = str(uuid.uuid4())
            price = cfg["PRICE"]["ru"] * 0.5  # 50% discount
            payment_data = {
                "quickpay-form": "shop",
                "paymentType": "AC",
                "targets": f"Subscription {user_id} ({bot_key})",
                "sum": price,
                "label": payment_id,
                "receiver": cfg["YOOMONEY_WALLET"],
                "successURL": f"https://t.me/{(await bot.get_me()).username}"
            }
            payment_link = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(payment_data)}"

            async with pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                    "VALUES ($1, $2, $3, $4)",
                    payment_id, user_id, "pending", "yoomoney"
                )
            logger.info(f"[{bot_key}] Payment {payment_id} (yoomoney) saved for {user_id} [{log_id}]")

            keyboard = InlineKeyboardMarkup()
            keyboard.add(InlineKeyboardButton("Оплатить сейчас", url=payment_link))
            await bot.send_message(chat_id, "Перейдите для оплаты через ЮMoney:", reply_markup=keyboard, protect_content=True)
            logger.info(f"[{bot_key}] YooMoney link sent to {user_id} (protect_content=True) [{log_id}]")
        except Exception as e:
            logger.error(f"[{bot_key}] Error processing YooMoney for {user_id} [{log_id}]: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error.", protect_content=True)

    @dp.callback_query_handler(lambda c: c.data.startswith(("ton_", "btc_", "usdt_", "paypal_")))
    async def handle_payment_choice(cb: types.CallbackQuery):
        payment_type = cb.data.split("_")[0]
        params = {
            "ton": (TON_ADDRESS, 0, 4),
            "btc": (BTC_ADDRESS, 1, 8),
            "usdt": (USDT_ADDRESS, 2, 2),
            "paypal": (None, None, 2)
        }
        address, price_index, decimals = params[payment_type]
        await handle_crypto_or_paypal_payment(cb, bot_key, payment_type, pool, address, price_index, decimals)

def check_yoomoney_webhook(data: Dict, bot_key: str) -> bool:
    try:
        params = [
            data.get("notification_type", ""),
            data.get("operation_id", ""),
            data.get("amount", ""),
            data.get("currency", ""),
            data.get("datetime", ""),
            data.get("sender", ""),
            data.get("codepro", ""),
            SETTINGS[bot_key]["NOTIFICATION_SECRET"],
            data.get("label", "")
        ]
        computed_hash = hashlib.sha1("&".join(str(p) for p in params).encode()).hexdigest()
        logger.debug(f"[{bot_key}] YooMoney webhook check: computed_hash={computed_hash}, received_hash={data.get('sha1_hash')} [{log_id}]")
        return computed_hash == data.get("sha1_hash")
    except Exception as e:
        logger.error(f"[{bot_key}] Error checking YooMoney webhook [{log_id}]: {e}")
        return False

async def process_yoomoney_webhook(req: web.Request) -> web.Response:
    try:
        data = await req.post()
        payment_id = data.get("label")
        logger.debug(f"Received YooMoney webhook: {dict(data)} [{log_id}]")
        if not payment_id:
            logger.error(f"Missing label in YooMoney webhook [{log_id}]")
            return web.Response(status=400)
        bot_key = await locate_bot_by_payment(payment_id, pool)
        if not bot_key:
            logger.error(f"Bot not found for payment {payment_id} [{log_id}]")
            return web.Response(status=400)
        if not check_yoomoney_webhook(data, bot_key):
            logger.error(f"[{bot_key}] Invalid YooMoney webhook hash [{log_id}]")
            return web.Response(status=400)
        if data.get("notification_type") in ["p2p-incoming", "card-incoming"]:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(
                    f"SELECT user_id, status FROM payments_{bot_key} WHERE label = $1", payment_id
                )
                if result:
                    user_id, status = result
                    if status == "success":
                        logger.info(f"[{bot_key}] Payment {payment_id} already processed for {user_id} [{log_id}]")
                        return web.Response(status=200)
                    language = await get_user_language(pool, user_id)
                    await conn.execute(
                        f"UPDATE payments_{bot_key} SET status = $1 WHERE label = $2", "success", payment_id
                    )
                    invite = await generate_channel_invite(bot_key, user_id)
                    success_msg = {
                        "en": f"Payment confirmed! Join the private channel: {invite}",
                        "ru": f"Платеж подтвержден! Присоединяйтесь к приватному каналу: {invite}",
                        "uk": f"Платіж підтверджено! Долучайтесь до приватного каналу: {invite}",
                        "tr": f"Ödeme onaylandı! Özel kanala katıl: {invite}",
                        "hi": f"भुगतान की पुष्टि हो गई! निजी चैनल में शामिल हों: {invite}"
                    }
                    error_msg = {
                        "en": "Invite error.",
                        "ru": "Ошибка приглашения.",
                        "uk": "Помилка запрошення.",
                        "tr": "Davet hatası.",
                        "hi": "निमंत्रण त्रुटि।"
                    }
                    await bot_instances[bot_key].send_message(
                        user_id,
                        success_msg[language] if invite else error_msg[language],
                        protect_content=True
                    )
                    logger.info(f"[{bot_key}] Payment {payment_id} confirmed, invite sent to {user_id} [{log_id}]")
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Error processing YooMoney webhook [{log_id}]: {e}")
        return web.Response(status=500)

async def store_payment(req: web.Request, bot_key: str, pool: asyncpg.Pool) -> web.Response:
    try:
        data = await req.json()
        payment_id = data.get("label")
        user_id = data.get("user_id")
        payment_type = data.get("payment_type", "unknown")
        logger.debug(f"[{bot_key}] Store payment request: {data} [{log_id}]")
        if not payment_id or not user_id:
            logger.error(f"[{bot_key}] Missing label or user_id in /store_payment [{log_id}]")
            return web.Response(status=400)
        async with pool.acquire() as conn:
            await conn.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES ($1, $2, $3, $4) ON CONFLICT (label) DO UPDATE SET user_id = $2, status = $3",
                payment_id, user_id, "pending", payment_type
            )
        logger.info(f"[{bot_key}] Payment {payment_id} ({payment_type}) saved for {user_id} [{log_id}]")
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[{bot_key}] Error storing payment [{log_id}]: {e}")
        return web.Response(status=500)

async def check_status(req: web.Request) -> web.Response:
    logger.info(f"[{ENV}] Server status check [{log_id}]")
    return web.Response(status=200, text=f"Active with {len(SETTINGS)} bots")

async def process_bot_webhook(req: web.Request, bot_key: str) -> web.Response:
    try:
        if bot_key not in dispatchers:
            logger.error(f"[{bot_key}] Bot not found in dispatchers [{log_id}]")
            return web.Response(status=400)
        bot = bot_instances[bot_key]
        dp = dispatchers[bot_key]
        update = await req.json()
        logger.debug(f"[{bot_key}] Received webhook: {update} [{log_id}]")
        update_obj = types.Update(**update)
        await dp.process_update(update_obj)
        logger.info(f"[{bot_key}] Webhook processed for {bot_key} [{log_id}]")
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[{bot_key}] Error processing webhook: {e} [{log_id}]")
        return web.Response(status=500)

async def configure_webhooks(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        for bot_key, bot in bot_instances.items():
            try:
                hook_url = f"{HOST_URL}{WEBHOOK_BASE}/{bot_key}"
                saved_hook = await conn.fetchval(
                    "SELECT webhook_url FROM webhooks WHERE bot_key = $1", bot_key
                )
                if saved_hook == hook_url:
                    logger.info(f"[{bot_key}] Webhook already set in DB: {hook_url} [{log_id}]")
                    continue
                webhook_info = await bot.get_webhook_info()
                logger.debug(f"[{bot_key}] Current webhook: {webhook_info.url} [{log_id}]")
                if webhook_info.url != hook_url:
                    await bot.delete_webhook(drop_pending_updates=True)
                    await bot.set_webhook(hook_url)
                    await conn.execute(
                        "INSERT INTO webhooks (bot_key, webhook_url) VALUES ($1, $2) "
                        "ON CONFLICT (bot_key) DO UPDATE SET webhook_url = $2",
                        bot_key, hook_url
                    )
                    logger.info(f"[{bot_key}] Webhook set: {hook_url} [{log_id}]")
                else:
                    logger.info(f"[{bot_key}] Webhook already set: {hook_url} [{log_id}]")
            except Exception as e:
                logger.error(f"[{bot_key}] Error setting webhook: {e} [{log_id}]")
                raise

async def keep_alive():
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(f"{HOST_URL}/status") as response:
                    logger.info(f"Keep-alive ping to /status: {response.status} [{log_id}]")
            except Exception as e:
                logger.error(f"Keep-alive ping error: {e} [{log_id}]")
            await asyncio.sleep(300)  # Ping every 5 minutes

async def launch_server():
    pool = await get_db_pool()
    await setup_database(pool)
    for bot_key, dp in dispatchers.items():
        register_handlers(dp, bot_key, pool)
    try:
        await configure_webhooks(pool)
        app = web.Application()
        app.router.add_post("/", lambda req: web.Response(status=200, text="OK"))
        app.router.add_post(YOOMONEY_HOOK, process_yoomoney_webhook)
        app.router.add_get(HEALTH_CHECK, check_status)
        app.router.add_post(HEALTH_CHECK, check_status)
        for bot_key in SETTINGS:
            app.router.add_post(f"{YOOMONEY_HOOK}/{bot_key}", lambda req, bot_key=bot_key: process_yoomoney_webhook(req))
            app.router.add_post(f"{PAYMENT_STORE}/{bot_key}", lambda req, bot_key=bot_key: store_payment(req, bot_key, pool))
            app.router.add_post(f"{WEBHOOK_BASE}/{bot_key}", lambda req, bot_key=bot_key: process_bot_webhook(req, bot_key))
        port = int(os.getenv("PORT", 8080))
        logger.info(f"Starting server on port {port} with {len(SETTINGS)} bots [{log_id}]")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"Server started on port {port} [{log_id}]")
        asyncio.create_task(keep_alive())  # Start keep-alive task
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Critical server startup error: {e} [{log_id}]")
        sys.exit(1)
    finally:
        await pool.close()

async def locate_bot_by_payment(payment_id: str, pool: asyncpg.Pool) -> Optional[str]:
    async with pool.acquire() as conn:
        try:
            for bot_key in SETTINGS:
                result = await conn.fetchval(
                    f"SELECT user_id FROM payments_{bot_key} WHERE label = $1", payment_id
                )
                if result:
                    logger.debug(f"Found bot {bot_key} for payment {payment_id} [{log_id}]")
                    return bot_key
            logger.error(f"Bot not found for payment {payment_id} [{log_id}]")
            return None
        except Exception as e:
            logger.error(f"Error locating payment {payment_id} [{log_id}]: {e}")
            return None

async def generate_channel_invite(bot_key: str, user_id: str) -> Optional[str]:
    try:
        cfg = SETTINGS[bot_key]
        bot = bot_instances[bot_key]
        bot_member = await bot.get_chat_member(chat_id=cfg["PRIVATE_CHANNEL_ID"], user_id=(await bot.get_me()).id)
        if not bot_member.can_invite_users:
            logger.error(f"[{bot_key}] No invite permissions for {user_id} [{log_id}]")
            return None
        for _ in range(3):
            try:
                invite = await bot.create_chat_invite_link(
                    chat_id=cfg["PRIVATE_CHANNEL_ID"], member_limit=1, name=f"user_{user_id}"
                )
                logger.info(f"[{bot_key}] Invite created for {user_id}: {invite.invite_link} [{log_id}]")
                return invite.invite_link
            except Exception as e:
                logger.error(f"[{bot_key}] Error creating invite for {user_id} (retry): {e} [{log_id}]")
                await asyncio.sleep(1)
        return None
    except Exception as e:
        logger.error(f"[{bot_key}] Error generating invite for {user_id} [{log_id}]: {e}")
        return None

if __name__ == "__main__":
    asyncio.run(launch_server())
