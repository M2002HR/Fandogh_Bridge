# Fandogh Bridge

پل پیام دوطرفه بین ربات تلگرام و بازوی بله با پایتون (`asyncio`) با UI مرحله‌ای و دکمه‌ای.

## قابلیت‌ها

- ثبت‌نام مرحله‌ای: پذیرش مقررات + ثبت شماره موبایل
- منوی دکمه‌ای یکسان روی تلگرام و بله
- اتصال به مخاطب با `bridge_id` یا شماره یا `@username`
- ذخیره مخاطب با نام دلخواه (مخاطبین فندقی)
- مدیریت مخاطب: اتصال، بلاک/آنبلاک، مشاهده پروفایل، حذف
- درخواست اطلاع‌رسانی به ادمین برای مخاطب‌های ثبت‌نام‌نشده
- رله دوطرفه `text`, `photo`, `voice`
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

3. اجرای Docker:

```bash
docker compose up --build -d
```

## نکات کانفیگ

- `ADMIN_IDS` برای اطلاع‌رسانی ادمین:
  - نمونه: `telegram:123456789,bale:987654321`
- مسیرها:
  - `DB_URL=sqlite+aiosqlite:///./app_data/bridge.db`
  - `MEDIA_TMP_DIR=./app_tmp_media`

## Testing

```bash
source .venv/bin/activate
pytest -q
```
