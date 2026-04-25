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
- درخواست اطلاع‌رسانی به ادمین برای مخاطب‌های ثبت‌نام‌نشده (شماره + توضیح)
- رله دوطرفه `text`, `photo`, `voice`
- دکمه شیشه‌ای زیر هر پیام دریافتی برای `اتصال/پاسخ`
- صف retry با backoff تا 24 ساعت

## Quick Start

1. مقداردهی توکن‌ها در `.env`
2. اجرای محلی:

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

3. اجرای Docker:

```bash
docker compose up --build -d
```

## نکات کانفیگ

- `ADMIN_IDS` برای اطلاع‌رسانی ادمین:
  - نمونه: `telegram:123456789,bale:987654321`
- `TELEGRAM_ADMIN_CHANNEL_ID` برای ارسال اعلان به کانال تلگرام:
  - برای کانال خصوصی باید `chat_id` عددی (مثل `-100...`) تنظیم شود.
  - اگر مقدار را به شکل `100...` بگذارید، برنامه خودش به `-100...` تبدیل می‌کند.
  - لینک دعوت خصوصی (`https://t.me/+...`) به‌تنهایی برای `sendMessage` کافی نیست.
- برای دریافت callback دکمه‌های شیشه‌ای:
  - `TELEGRAM_ALLOWED_UPDATES=["message","callback_query"]`
  - `BALE_ALLOWED_UPDATES=["message","callback_query"]`
- مسیرها:
  - `DB_URL=sqlite+aiosqlite:///./app_data/bridge.db`
  - `MEDIA_TMP_DIR=./app_tmp_media`
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
