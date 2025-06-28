import logging
import sys
import uuid
import psycopg2
import hashlib
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
from urllib.parse import urlencode
import asyncio
import os
import qrcode
import base64
from io import BytesIO
from config import fetch_bot_settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)
log.info("Запуск бота подписки")

# Определение путей и базы данных
PAYMENT_STORE = "/store_payment"
YOOMONEY_HOOK = "/yoomoney_hook"
HEALTH_CHECK = "/status"
WEBHOOK_BASE = "/bot_hook"
DB_URL = "postgresql://postgres.iylthyqzwovudjcyfubg:Alex4382!@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
HOST_URL = os.getenv("HOST_URL", "https://web-production-5c4ed.up.railway.app")
TON_ADDRESS = "UQBLNUOpN5B0q_M2xukAB5MsfSCUsdE6BkXHO6ndogQDi5_6"
BTC_ADDRESS = "bc1q5xq9m473r8nnkx799ztcrwfqs0555fs3ulw9vr"
USDT_ADDRESS = "TQzs3V6QHdXb3CtNPYK9iPWuvvrYCPt6vE"
PAYPAL_EMAIL = "nemillingsuppay@gmail.com"

# Окружение
ENV = "railway"
log.info(f"Платформа: {ENV}")

# Конвертация RUB в USD
def get_usd_from_rub(rub_amount):
    try:
        usd_rate = 100.0  # Фиксированный курс: 1 USD = 100 RUB
        return rub_amount / usd_rate
    except Exception as e:
        log.error(f"Ошибка конвертации RUB в USD: {e}")
        return rub_amount / 100.0

# Получение курса криптовалют
def get_crypto_prices():
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network,bitcoin,tether&vs_currencies=usd",
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        return (
            data["the-open-network"]["usd"],
            data["bitcoin"]["usd"],
            data["tether"]["usd"]
        )
    except Exception as e:
        log.error(f"Ошибка получения курса: {e}")
        return 5.0, 60000.0, 1.0

# Генерация QR-кода
def generate_qr_code(data):
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill="black", back_color="white")
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode()
    except Exception as e:
        log.error(f"Ошибка генерации QR-кода: {e}")
        return None

# Загрузка конфигураций ботов
SETTINGS = fetch_bot_settings()
log.info(f"Настройка {len(SETTINGS)} ботов")
bot_instances = {}
dispatchers = {}

for bot_key, cfg in SETTINGS.items():
    try:
        log.info(f"Инициализация бота {bot_key}")
        bot_instances[bot_key] = Bot(token=cfg["TOKEN"])
        dispatchers[bot_key] = Dispatcher(bot_instances[bot_key])
        log.info(f"Бот {bot_key} инициализирован")
    except Exception as e:
        log.error(f"Ошибка инициализации бота {bot_key}: {e}")
        sys.exit(1)

# Инициализация базы данных
def setup_database():
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        for bot_key in SETTINGS:
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS payments_{bot_key} "
                "(label TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL, payment_type TEXT)"
            )
            cursor.execute(
                f"ALTER TABLE payments_{bot_key} ADD COLUMN IF NOT EXISTS payment_type TEXT"
            )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS user_languages "
            "(user_id TEXT PRIMARY KEY, language TEXT NOT NULL)"
        )
        conn.commit()
        conn.close()
        log.info("База данных настроена")
    except Exception as e:
        log.error(f"Ошибка базы данных: {e}")
        sys.exit(1)

setup_database()

# Кнопки выбора языка
def create_language_buttons():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("English", callback_data="lang_en"),
        InlineKeyboardButton("Русский", callback_data="lang_ru"),
        InlineKeyboardButton("Українська", callback_data="lang_uk"),
        InlineKeyboardButton("Türkçe", callback_data="lang_tr"),
        InlineKeyboardButton("हिन्दी", callback_data="lang_hi")
    )
    return keyboard

# Кнопки оплаты (ЮMoney только для ru)
def create_payment_buttons(user_id, language):
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
            ("USDT", f"usdt_{user_id}"),
            ("PayPal", f"paypal_{user_id}")
        ]
    }
    for text, callback in buttons.get(language, buttons["en"]):
        keyboard.add(InlineKeyboardButton(text, callback_data=callback))
    return keyboard

# Получение языка пользователя
def get_user_language(user_id):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT language FROM user_languages WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else "en"
    except Exception as e:
        log.error(f"Ошибка получения языка: {e}")
        return "en"

# Сохранение языка пользователя
def save_user_language(user_id, language):
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO user_languages (user_id, language) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET language = %s",
            (user_id, language, language)
        )
        conn.commit()
        conn.close()
        log.info(f"Язык {language} сохранен для пользователя {user_id}")
    except Exception as e:
        log.error(f"Ошибка сохранения языка для пользователя {user_id}: {e}")

# Обработчики команд
for bot_key, dp in dispatchers.items():
    @dp.message_handler(commands=["start"])
    async def initiate_language_selection(msg: types.Message, bot_key=bot_key):
        try:
            user_id = str(msg.from_user.id)
            chat_id = msg.chat.id
            bot = bot_instances[bot_key]
            log.info(f"[{bot_key}] Команда /start от пользователя {user_id}")

            keyboard = create_language_buttons()
            await bot.send_message(
                chat_id,
                "Please select your language:\nВыберите язык:\nОберіть мову:\nLütfen dilinizi seçin:\nकृपया अपनी भाषा चुनें:",
                reply_markup=keyboard
            )
            log.info(f"[{bot_key}] Отправлен выбор языка пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка /start: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Error. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("lang_"))
    async def handle_language_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = str(cb.from_user.id)
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = cb.data.split("_")[1]
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран язык {language} пользователем {user_id}")

            save_user_language(user_id, language)
            keyboard = create_payment_buttons(user_id, language)
            price = cfg["PRICE"][language]
            welcome_msg = cfg["DESCRIPTION"][language].format(price=price)
            currency = "RUB" if language == "ru" else "USD"
            payment_prompt = {
                "en": f"{welcome_msg}\n\nChoose payment method for {price} {currency}:",
                "ru": f"{welcome_msg}\n\nВыберите способ оплаты для {price} {currency}:",
                "uk": f"{welcome_msg}\n\nОберіть спосіб оплати для {price} {currency}:",
                "tr": f"{welcome_msg}\n\n{price} {currency} için ödeme yöntemi seçin:",
                "hi": f"{welcome_msg}\n\n{price} {currency} के लिए भुगतान विधि चुनें:"
            }
            await bot.send_message(
                chat_id,
                payment_prompt[language],
                reply_markup=keyboard
            )
            log.info(f"[{bot_key}] Отправлены варианты оплаты на {language} пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка выбора языка: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Error selecting language. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("yoomoney_"))
    async def handle_yoomoney_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = get_user_language(user_id)
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран ЮMoney пользователем {user_id}")

            payment_id = str(uuid.uuid4())
            price = cfg["PRICE"]["ru"]  # ЮMoney всегда в RUB
            payment_data = {
                "quickpay-form": "shop",
                "paymentType": "AC",
                "targets": f"Subscription for user {user_id}",
                "sum": price,
                "label": payment_id,
                "receiver": cfg["YOOMONEY_WALLET"],
                "successURL": f"https://t.me/{(await bot.get_me()).username}"
            }
            payment_link = f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(payment_data)}"

            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "yoomoney")
            )
            conn.commit()
            conn.close()
            log.info(f"[{bot_key}] Сохранен платеж {payment_id} для пользователя {user_id}")

            keyboard = InlineKeyboardMarkup()
            button_text = {
                "ru": "Оплатить сейчас"
            }
            keyboard.add(InlineKeyboardButton(button_text["ru"], url=payment_link))
            prompt = {
                "ru": "Перейдите для оплаты через ЮMoney:"
            }
            await bot.send_message(chat_id, prompt["ru"], reply_markup=keyboard)
            log.info(f"[{bot_key}] Ссылка ЮMoney отправлена пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка ЮMoney: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("ton_"))
    async def handle_ton_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = get_user_language(user_id)
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран TON пользователем {user_id}")

            payment_id = str(uuid.uuid4())
            ton_price, _, _ = get_crypto_prices()
            price = cfg["PRICE"]["ru"] if language == "ru" else cfg["PRICE"][language]
            usd_amount = get_usd_from_rub(price) if language == "ru" else price
            amount_ton = round(usd_amount / ton_price, 4)
            nano_ton = int(amount_ton * 1e9)

            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "ton")
            )
            conn.commit()
            conn.close()
            log.info(f"[{bot_key}] Сохранен TON платеж {payment_id} для пользователя {user_id}")

            qr_data = f"ton://transfer/{TON_ADDRESS}?amount={nano_ton}"
            qr_base64 = generate_qr_code(qr_data)
            if qr_base64:
                qr_bytes = base64.b64decode(qr_base64)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=f"{TON_ADDRESS}")
            else:
                await bot.send_message(chat_id, f"{TON_ADDRESS}")

            prompt = {
                "en": f"Pay: {amount_ton:.4f} TON",
                "ru": f"Оплатите: {amount_ton:.4f} TON",
                "uk": f"Сплатіть: {amount_ton:.4f} TON",
                "tr": f"Öde: {amount_ton:.4f} TON",
                "hi": f"भुगतान करें: {amount_ton:.4f} TON"
            }
            await bot.send_message(chat_id, prompt[language])
            log.info(f"[{bot_key}] Отправлен TON адрес и сумма пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка TON: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("btc_"))
    async def handle_btc_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = get_user_language(user_id)
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран BTC пользователем {user_id}")

            payment_id = str(uuid.uuid4())
            _, btc_price, _ = get_crypto_prices()
            price = cfg["PRICE"]["ru"] if language == "ru" else cfg["PRICE"][language]
            usd_amount = get_usd_from_rub(price) if language == "ru" else price
            amount_btc = f"{usd_amount / btc_price:.8f}".rstrip("0")

            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "btc")
            )
            conn.commit()
            conn.close()
            log.info(f"[{bot_key}] Сохранен BTC платеж {payment_id} для пользователя {user_id}")

            qr_data = f"bitcoin:{BTC_ADDRESS}?amount={amount_btc}"
            qr_base64 = generate_qr_code(qr_data)
            if qr_base64:
                qr_bytes = base64.b64decode(qr_base64)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=f"{BTC_ADDRESS}")
            else:
                await bot.send_message(chat_id, f"{BTC_ADDRESS}")

            prompt = {
                "en": f"Pay: {amount_btc} BTC",
                "ru": f"Оплатите: {amount_btc} BTC",
                "uk": f"Сплатіть: {amount_btc} BTC",
                "tr": f"Öde: {amount_btc} BTC",
                "hi": f"भुगतान करें: {amount_btc} BTC"
            }
            await bot.send_message(chat_id, prompt[language])
            log.info(f"[{bot_key}] Отправлен BTC адрес и сумма пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка BTC: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("usdt_"))
    async def handle_usdt_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = get_user_language(user_id)
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран USDT TRC20 пользователем {user_id}")

            payment_id = str(uuid.uuid4())
            _, _, usdt_price = get_crypto_prices()
            price = cfg["PRICE"]["ru"] if language == "ru" else cfg["PRICE"][language]
            usd_amount = get_usd_from_rub(price) if language == "ru" else price
            amount_usdt = round(usd_amount / usdt_price, 2)

            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "usdt")
            )
            conn.commit()
            conn.close()
            log.info(f"[{bot_key}] Сохранен USDT платеж {payment_id} для пользователя {user_id}")

            qr_base64 = generate_qr_code(USDT_ADDRESS)
            if qr_base64:
                qr_bytes = base64.b64decode(qr_base64)
                await bot.send_photo(chat_id, photo=qr_bytes, caption=f"{USDT_ADDRESS}")
            else:
                await bot.send_message(chat_id, f"{USDT_ADDRESS}")

            prompt = {
                "en": f"Pay: {amount_usdt:.2f} USDT TRC20",
                "ru": f"Оплатите: {amount_usdt:.2f} USDT TRC20",
                "uk": f"Сплатіть: {amount_usdt:.2f} USDT TRC20",
                "tr": f"Öde: {amount_usdt:.2f} USDT TRC20",
                "hi": f"भुगतान करें: {amount_usdt:.2f} USDT TRC20"
            }
            await bot.send_message(chat_id, prompt[language])
            log.info(f"[{bot_key}] Отправлен USDT адрес и сумма пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка USDT: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error. Try again.")

    @dp.callback_query_handler(lambda c: c.data.startswith("paypal_"))
    async def handle_paypal_choice(cb: types.CallbackQuery, bot_key=bot_key):
        try:
            user_id = cb.data.split("_")[1]
            chat_id = cb.message.chat.id
            bot = bot_instances[bot_key]
            cfg = SETTINGS[bot_key]
            language = get_user_language(user_id)
            await bot.answer_callback_query(cb.id)
            log.info(f"[{bot_key}] Выбран PayPal пользователем {user_id}")

            payment_id = str(uuid.uuid4())
            price = cfg["PRICE"]["ru"] if language == "ru" else cfg["PRICE"][language]
            currency = "RUB" if language == "ru" else "USD"

            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(
                f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
                "VALUES (%s, %s, %s, %s)",
                (payment_id, user_id, "pending", "paypal")
            )
            conn.commit()
            conn.close()
            log.info(f"[{bot_key}] Сохранен PayPal платеж {payment_id} для пользователя {user_id}")

            prompt = {
                "en": f"Please send {price} {currency} via PayPal to {PAYPAL_EMAIL}. Include your Telegram ID ({user_id}) in the payment note.",
                "ru": f"Пожалуйста, отправьте {price} {currency} через PayPal на {PAYPAL_EMAIL}. Укажите ваш Telegram ID ({user_id}) в заметке к платежу.",
                "uk": f"Будь ласка, надішліть {price} {currency} через PayPal на {PAYPAL_EMAIL}. Вкажіть ваш Telegram ID ({user_id}) у примітці до платежу.",
                "tr": f"Lütfen {price} {currency} tutarını PayPal üzerinden {PAYPAL_EMAIL} adresine gönderin. Ödeme notuna Telegram ID'nizi ({user_id}) ekleyin.",
                "hi": f"कृपया {price} {currency} को PayPal के माध्यम से {PAYPAL_EMAIL} पर भेजें। भुगतान नोट में अपना Telegram ID ({user_id}) शामिल करें।"
            }
            await bot.send_message(chat_id, prompt[language])
            log.info(f"[{bot_key}] Отправлены инструкции PayPal пользователю {user_id}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка PayPal: {e}")
            await bot_instances[bot_key].send_message(chat_id, "Payment error. Try again.")

# Временный обработчик корневого пути
async def handle_root(req):
    log.info(f"[{ENV}] Запрос на корневой путь")
    return web.Response(status=200, text="OK")

# Проверка вебхука ЮMoney
def check_yoomoney_webhook(data, bot_key):
    try:
        cfg = SETTINGS[bot_key]
        params = [
            data.get("notification_type", ""),
            data.get("operation_id", ""),
            data.get("amount", ""),
            data.get("currency", ""),
            data.get("datetime", ""),
            data.get("sender", ""),
            data.get("codepro", ""),
            cfg["NOTIFICATION_SECRET"],
            data.get("label", "")
        ]
        computed_hash = hashlib.sha1("&".join(params).encode()).hexdigest()
        return computed_hash == data.get("sha1_hash")
    except Exception as e:
        log.error(f"[{bot_key}] Ошибка проверки ЮMoney: {e}")
        return False

# Генерация приглашения
async def generate_channel_invite(bot_key, user_id):
    try:
        cfg = SETTINGS[bot_key]
        bot = bot_instances[bot_key]
        language = get_user_language(user_id)
        bot_member = await bot.get_chat_member(chat_id=cfg["PRIVATE_CHANNEL_ID"], user_id=(await bot.get_me()).id)
        if not bot_member.can_invite_users:
            log.error(f"[{bot_key}] Нет прав на приглашения")
            return None
        for _ in range(3):
            try:
                invite = await bot.create_chat_invite_link(
                    chat_id=cfg["PRIVATE_CHANNEL_ID"], member_limit=1, name=f"user_{user_id}"
                )
                log.info(f"[{bot_key}] Приглашение для {user_id}: {invite.invite_link}")
                return invite.invite_link
            except:
                await asyncio.sleep(1)
        return None
    except Exception as e:
        log.error(f"[{bot_key}] Ошибка приглашения: {e}")
        return None

# Поиск бота
def locate_bot_by_payment(payment_id):
    try:
        for bot_key in SETTINGS:
            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(f"SELECT user_id FROM payments_{bot_key} WHERE label = %s", (payment_id,))
            result = cursor.fetchone()
            conn.close()
            if result:
                return bot_key
        return None
    except Exception as e:
        log.error(f"Ошибка поиска платежа: {e}")
        return None

# Обработчик ЮMoney
async def process_yoomoney_webhook(req):
    try:
        data = await req.post()
        payment_id = data.get("label")
        if not payment_id:
            return web.Response(status=400)
        bot_key = locate_bot_by_payment(payment_id)
        if not bot_key or not check_yoomoney_webhook(data, bot_key):
            return web.Response(status=400)
        if data.get("notification_type") in ["p2p-incoming", "card-incoming"]:
            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(f"SELECT user_id FROM payments_{bot_key} WHERE label = %s", (payment_id,))
            result = cursor.fetchone()
            if result:
                user_id = result[0]
                language = get_user_language(user_id)
                cursor.execute(f"UPDATE payments_{bot_key} SET status = %s WHERE label = %s", ("success", payment_id))
                conn.commit()
                invite = await generate_channel_invite(bot_key, user_id)
                success_msg = {
                    "en": f"Payment confirmed! Channel: {invite}",
                    "ru": f"Платеж подтвержден! Канал: {invite}",
                    "uk": f"Платіж підтверджено! Канал: {invite}",
                    "tr": f"Ödeme onaylandı! Kanal: {invite}",
                    "hi": f"भुगतान की पुष्टि हो गई! चैनल: {invite}"
                }
                error_msg = {
                    "en": "Invite error. Contact @YourSupportHandle.",
                    "ru": "Ошибка приглашения. Свяжитесь с @YourSupportHandle.",
                    "uk": "Помилка запрошення. Зверніться до @YourSupportHandle.",
                    "tr": "Davet hatası. @YourSupportHandle ile iletişime geçin.",
                    "hi": "निमंत्रण त्रुटि। @YourSupportHandle से संपर्क करें।"
                }
                await bot_instances[bot_key].send_message(
                    user_id,
                    success_msg[language] if invite else error_msg[language]
                )
            conn.close()
        return web.Response(status=200)
    except Exception as e:
        log.error(f"Ошибка вебхука ЮMoney: {e}")
        return web.Response(status=500)

# Хранение платежей
async def store_payment(req, bot_key):
    try:
        data = await req.json()
        payment_id = data.get("label")
        user_id = data.get("user_id")
        payment_type = data.get("payment_type", "unknown")
        if not payment_id or not user_id:
            return web.Response(status=400)
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO payments_{bot_key} (label, user_id, status, payment_type) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (label) DO UPDATE SET user_id = %s, status = %s",
            (payment_id, user_id, "pending", payment_type, user_id, "pending")
        )
        conn.commit()
        conn.close()
        log.info(f"[{bot_key}] Платеж {payment_id} сохранен для пользователя {user_id} (тип: {payment_type})")
        return web.Response(status=200)
    except Exception as e:
        log.error(f"[{bot_key}] Ошибка сохранения платежа: {e}")
        return web.Response(status=500)

# Проверка состояния
async def check_status(req):
    log.info(f"[{ENV}] Запрос состояния сервера")
    return web.Response(status=200, text=f"Активно с {len(SETTINGS)} ботами")

# Обработчик бота
async def process_bot_webhook(req, bot_key):
    try:
        if bot_key not in dispatchers:
            log.error(f"[{bot_key}] Бот не найден в dispatchers")
            return web.Response(status=400)
        bot = bot_instances[bot_key]
        dp = dispatchers[bot_key]
        Bot.set_current(bot)
        dp.set_current(dp)
        update = await req.json()
        update_obj = types.Update(**update)
        asyncio.create_task(dp.process_update(update_obj))
        log.info(f"[{bot_key}] Обработан вебхук")
        return web.Response(status=200)
    except Exception as e:
        log.error(f"[{bot_key}] Ошибка вебхука: {e}")
        return web.Response(status=500)

# Настройка вебхуков
async def configure_webhooks():
    for bot_key in bot_instances:
        try:
            bot = bot_instances[bot_key]
            hook_url = f"{HOST_URL}{WEBHOOK_BASE}/{bot_key}"
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.set_webhook(hook_url)
            log.info(f"[{bot_key}] Вебхук успешно установлен: {hook_url}")
        except Exception as e:
            log.error(f"[{bot_key}] Ошибка установки вебхука: {e}")

# Запуск сервера
async def launch_server():
    try:
        await configure_webhooks()
        app = web.Application()
        app.router.add_post("/", handle_root)
        app.router.add_post(YOOMONEY_HOOK, process_yoomoney_webhook)
        app.router.add_get(HEALTH_CHECK, check_status)
        app.router.add_post(HEALTH_CHECK, check_status)
        for bot_key in SETTINGS:
            app.router.add_post(f"{YOOMONEY_HOOK}/{bot_key}", lambda req, bot_key=bot_key: process_yoomoney_webhook(req))
            app.router.add_post(f"{PAYMENT_STORE}/{bot_key}", lambda req, bot_key=bot_key: store_payment(req, bot_key))
            app.router.add_post(f"{WEBHOOK_BASE}/{bot_key}", lambda req, bot_key=bot_key: process_bot_webhook(req, bot_key))
        port = int(os.getenv("PORT", 8000))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log.info(f"Сервер успешно запущен на порту {port} с {len(SETTINGS)} ботами")
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        log.error(f"Ошибка запуска сервера: {e}")
        sys.exit(1)

# Запуск приложения
if __name__ == "__main__":
    asyncio.run(launch_server())
