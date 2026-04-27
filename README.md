# Fandogh Bridge

پل پیام دوطرفه بین ربات تلگرام و بازوی بله با پایتون (`asyncio`) با UI مرحله‌ای و دکمه‌ای.

## قابلیت‌ها

- ثبت‌نام مرحله‌ای: پذیرش مقررات + ثبت شماره موبایل
- پذیرش مقررات با دکمه شیشه‌ای (inline)
- منوی دکمه‌ای یکسان روی تلگرام و بله
- اتصال به مخاطب با `فندق‌آیدی` یا شماره یا `@username`
- ذخیره مخاطب با نام دلخواه (مخاطبین فندقی)
- مدیریت مخاطب با دکمه شیشه‌ای: اتصال، بلاک/آنبلاک، مشاهده پروفایل، حذف
- لیست مخاطبین صفحه‌بندی‌شده (۵ مورد در هر صفحه) با لینک `/contact_<id>`
- درخواست اطلاع‌رسانی به ادمین برای مخاطب‌های ثبت‌نام‌نشده (شماره و/یا نام کاربری + توضیح)
- اگر مخاطبِ درخواست‌شده ثبت‌نام کند، به درخواست‌دهنده اعلان داده می‌شود
- رله دوطرفه `text`, `photo`, `voice`
- دکمه شیشه‌ای زیر هر پیام دریافتی برای `اتصال/پاسخ`
- اعلان مشاهده پیام با دکمه `👁️ مشاهده شد` و ارسال تایید برای فرستنده
- صف retry با backoff تا 24 ساعت
- کاتالوگ فروش JSON برای تعریف بسته‌ها، سقف مصرف و روش‌های پرداخت
- نمایش اعتبار باقی‌مانده و خرید بسته از داخل ربات
- نمایش تاریخچه خرید/پرداخت با وضعیت هر سفارش
- پرداخت داخل تلگرام با Telegram Stars (ثبت در کانال بررسی ادمین + تایید/رد)
- پرداخت TON خودکار در تلگرام با Crypto Pay API (شارژ خودکار پس از پرداخت موفق)
- پرداخت کیف‌پولی مستقیم در بله (Bale Wallet Invoice) (ثبت در کانال بررسی ادمین + تایید/رد)
- ثبت پرداخت دستی کارت‌به‌کارت و TON با ارسال رسید و تایید ادمین در کانال
- خرید بسته برای خود یا دیگران (با شماره، @username یا فندق‌آیدی)
- نمایش قیمت‌ها هم‌زمان به USDT، تومان و TON
- نرخ USDT ثابت و قابل تنظیم از `.env` (بدون نمایش نرخ تبدیل به کاربر)
- گزینه جداگانه برای ارتباط با پشتیبانی
- بهبود UI تلگرام با دکمه‌های رنگی (Button Style)، منوی Commands و لیست Commands
- دیتابیس عملیاتی روی MySQL 8
- پنل حرفه‌ای MySQL با phpMyAdmin
- لاگ مرکزی روی Elasticsearch + Kibana + Fluent Bit
- داشبورد آماده Kibana برای فعالیت کاربر و خطاهای ارسال

## Quick Start

1. مقداردهی توکن‌ها در `.env`
2. تنظیم کاتالوگ فروش در `config/sales_catalog.json`
3. اجرای محلی:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m bridge.main
```

اجرای توسعه با auto-reload:

```bash
source .venv/bin/activate
python -m bridge.dev
```

در این حالت با تغییر فایل‌های `src`, `.env`, `pyproject.toml`, `requirements.txt` ربات به‌صورت خودکار stop/start می‌شود.

4. اجرای Docker:

```bash
docker compose up --build -d
```

پس از بالا آمدن سرویس‌ها:

- MySQL Panel (phpMyAdmin): `http://127.0.0.1:18082`
- Elasticsearch API: `http://127.0.0.1:19200`
- Kibana: `http://127.0.0.1:15601`
- Kibana Login: `KIBANA_VIEWER_USERNAME` / `KIBANA_VIEWER_PASSWORD`

داشبورد آماده Kibana:

- `Fandogh - User Activity Dashboard`

اگر قبلاً SQLite داشته‌اید و می‌خواهید داده‌ها را به MySQL منتقل کنید:

```bash
source .venv/bin/activate
PYTHONPATH=src python scripts/migrate_sqlite_to_mysql.py --sqlite ./app_data/bridge.db
```

## نکات کانفیگ

- `ADMIN_IDS` برای اطلاع‌رسانی ادمین:
  - نمونه: `telegram:123456789,bale:987654321`
- تنظیمات لاگ و audit:
  - `LOG_FORMAT=json` (یا `plain`)
  - `HTTPX_LOG_LEVEL=WARNING` (پیش‌فرض: لاگ‌های شبکه خام tokenدار وارد ES نشوند)
  - `LOG_RETENTION_DAYS=30`
  - `LOG_CLEANUP_INTERVAL_SEC=3600`
  - `AUDIT_EVENTS_ENABLED=true`
  - `AUDIT_CAPTURE_FULL_TEXT=true`
- تنظیمات MySQL:
  - `MYSQL_ROOT_PASSWORD`
  - `MYSQL_DATABASE`
  - `MYSQL_USER`
  - `MYSQL_PASSWORD`
  - `DB_URL=mysql+aiomysql://...`
- تنظیمات Elastic/Kibana:
  - `ELASTIC_PASSWORD`
  - `KIBANA_SYSTEM_PASSWORD`
  - `ELASTIC_INGEST_USERNAME`
  - `ELASTIC_INGEST_PASSWORD`
  - `KIBANA_VIEWER_USERNAME`
  - `KIBANA_VIEWER_PASSWORD`
  - `KIBANA_ENCRYPTION_KEY`
- `TELEGRAM_ADMIN_CHANNEL_ID` برای ارسال اعلان به کانال تلگرام:
  - برای کانال خصوصی باید `chat_id` عددی (مثل `-100...`) تنظیم شود.
  - اگر مقدار را به شکل `100...` بگذارید، برنامه خودش به `-100...` تبدیل می‌کند.
  - لینک دعوت خصوصی (`https://t.me/+...`) به‌تنهایی برای `sendMessage` کافی نیست.
  - روش‌های پرداخت دستی فقط وقتی نمایش داده می‌شوند که این کانال تنظیم شده باشد.
- `BALE_WALLET_PROVIDER_TOKEN` برای پرداخت رسمی کیف پول بله (در `.env` تنظیم شود).
- تنظیمات بهبود UI تلگرام:
  - `TELEGRAM_ENABLE_BUTTON_STYLES=false` (پیش‌فرض بدون رنگ؛ ساختار دکمه‌ها حفظ می‌شود)
  - `TELEGRAM_BUTTON_STYLE_MODE=none` (برای فعال‌سازی رنگ بعدا: `auto`)
  - `TELEGRAM_SET_COMMANDS_ON_START=true` (ثبت خودکار `/commands` در استارت سرویس)
  - `TELEGRAM_SET_MENU_BUTTON_ON_START=true` (تنظیم منوی پایین ورودی روی حالت Commands)
- پرداخت TON خودکار تلگرام:
  - `TELEGRAM_TON_PAY_ENABLED=true`
  - `TELEGRAM_TON_PAY_API_TOKEN=<token-from-@CryptoBot>`
  - `TELEGRAM_TON_PAY_API_BASE_URL=https://pay.crypt.bot/api`
  - `TELEGRAM_TON_PAY_ASSET=TON`
  - `TELEGRAM_TON_PAY_POLL_INTERVAL_SEC=15`
  - اگر این مقادیر تنظیم نشوند، گزینه پرداخت TON خودکار در منو نمایش داده نمی‌شود.
- تبدیل قیمت USDT به TON برای نمایش:
  - `TON_RATE_API_ENABLED=true`
  - `TON_RATE_API_URL=https://data-api.binance.vision/api/v3/ticker/price`
  - `TON_RATE_API_SYMBOL=TONUSDT`
  - `TON_RATE_API_CACHE_SEC=300`
  - `TON_RATE_API_TIMEOUT_SEC=8`
- `SALES_CONFIG_PATH` مسیر فایل JSON فروش و اعتبار را مشخص می‌کند.
- فایل `config/sales_catalog.json` شامل این موارد است:
  - قواعد اعتبار: هر پیام متنی یا هر چند ثانیه ویس و سقف حجم عکس
  - بسته‌ها فقط با `price_usd` ذخیره می‌شوند
  - قیمت Stars به‌صورت داینامیک محاسبه می‌شود: هر `100 ⭐ = 1.5 USDT`
  - قیمت کیف پول بله از نرخ ثابت USDT در `.env` محاسبه می‌شود (`USDT_FIXED_TOMAN_RATE`) و در متن قیمت کاربر نمایش جداگانه ندارد
  - روش‌های پرداخت و مشخصات کارت/شبا/کیف پول TON
  - شناسه و متن پشتیبانی
- برای دریافت callback دکمه‌های شیشه‌ای:
  - `TELEGRAM_ALLOWED_UPDATES=["message","callback_query","pre_checkout_query"]`
  - `BALE_ALLOWED_UPDATES=["message","callback_query","pre_checkout_query"]`
- مسیرها:
  - `DB_URL=mysql+aiomysql://fandogh_app:...@mysql:3306/fandogh`
  - `MEDIA_TMP_DIR=./app_tmp_media`
  - `SALES_CONFIG_PATH=./config/sales_catalog.json`
- تنظیمات watcher توسعه:
  - `DEV_WATCH_COMMAND`
  - `DEV_WATCH_PATHS`
  - `DEV_WATCH_IGNORE_DIRS`
  - `DEV_WATCH_DEBOUNCE_MS`
  - `DEV_WATCH_POLL_DELAY_MS`
  - `DEV_WATCH_FORCE_POLLING`
  - `DEV_WATCH_TERM_TIMEOUT_SEC`
  - `DEV_WATCH_RUST_TIMEOUT_MS`

## Testing

```bash
source .venv/bin/activate
pytest -q
```
