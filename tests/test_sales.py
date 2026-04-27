from pathlib import Path

from bridge.sales import load_sales_catalog


def test_load_sales_catalog_and_rules() -> None:
    catalog = load_sales_catalog(str(Path(__file__).resolve().parents[1] / "config" / "sales_catalog.json"))

    assert catalog.version == 1
    assert len(catalog.packages) == 4
    assert catalog.package_by_id("starter-package") is not None
    assert catalog.package_by_id("max-package") is not None
    assert catalog.payment_method("telegram_stars") is not None
    assert catalog.usdt_to_toman_rate == 150000
    assert catalog.usdt_rate_channel == ""
    assert catalog.usdt_rate_cache_sec == 180
    assert catalog.usdt_rate_refresh_hour_local == 11
    assert catalog.usdt_rate_timezone == "Asia/Tehran"
    assert catalog.support_contact_id == "@fandogh_manager"
    assert catalog.rules.starter_credits.text_units == 2
    assert catalog.rules.starter_credits.voice_minutes == 1
    assert catalog.rules.starter_credits.photo_count == 0
    assert catalog.payment_method("bale_wallet") is not None
    assert catalog.rules.text_units_for_length(0) == 0
    assert catalog.rules.text_units_for_length(200) == 1
    assert catalog.rules.text_units_for_length(201) == 1
    assert catalog.rules.text_is_unlimited_per_message is True
    assert catalog.rules.voice_units_for_seconds(60) == 1
    assert catalog.rules.voice_units_for_seconds(61) == 2
    assert catalog.rules.photo_max_file_bytes == 10 * 1024 * 1024
