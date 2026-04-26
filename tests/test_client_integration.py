from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from bridge.platforms.client import BotApiClient
from bridge.types import Platform


@pytest.mark.asyncio
async def test_bot_api_client_methods(tmp_path) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))

        url = str(request.url)
        if url.endswith("/getUpdates"):
            return httpx.Response(200, json={"ok": True, "result": [{"update_id": 1}]})
        if url.endswith("/getFile"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"file_path": "abc/file.ogg", "file_size": 12}},
            )
        if "/file/bot" in url:
            return httpx.Response(200, content=b"voice-bytes")
        if url.endswith("/sendMessage"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 11}})
        if url.endswith("/sendPhoto"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 12}})
        if url.endswith("/sendVoice"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 13}})
        if url.endswith("/answerCallbackQuery"):
            return httpx.Response(200, json={"ok": True, "result": True})
        if url.endswith("/answerPreCheckoutQuery"):
            payload = json.loads(request.content.decode("utf-8"))
            assert payload["pre_checkout_query_id"] == "pcq-id"
            assert payload["ok"] is True
            return httpx.Response(200, json={"ok": True, "result": True})
        if url.endswith("/sendInvoice"):
            payload = json.loads(request.content.decode("utf-8"))
            assert payload["currency"] == "XTR"
            assert "provider_token" not in payload
            assert payload["prices"] == [{"label": "Starter", "amount": 250}]
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 14}})
        return httpx.Response(404, json={"ok": False})

    transport = httpx.MockTransport(handler)
    client = BotApiClient(
        platform=Platform.TELEGRAM,
        token="token",
        api_base_url="https://api.telegram.org",
        file_base_url="https://api.telegram.org/file",
        timeout_sec=10,
    )
    client.client = httpx.AsyncClient(transport=transport)

    updates = await client.get_updates(offset=None, timeout=20, allowed_updates=["message"])
    assert updates == [{"update_id": 1}]

    file_meta = await client.get_file("file-id")
    assert file_meta["file_path"] == "abc/file.ogg"

    output = tmp_path / "downloaded.ogg"
    await client.download_file("abc/file.ogg", output)
    assert output.exists()
    assert output.read_bytes() == b"voice-bytes"

    await client.send_message("1", "hello")

    photo = tmp_path / "x.jpg"
    photo.write_bytes(b"img")
    await client.send_photo("1", photo_path=photo, caption="cap")

    voice = tmp_path / "x.ogg"
    voice.write_bytes(b"voice")
    await client.send_voice("1", voice_path=voice)
    await client.answer_callback_query("cb-id")
    await client.answer_pre_checkout_query("pcq-id", ok=True)
    await client.send_invoice(
        chat_id="1",
        title="Starter",
        description="Starter package",
        payload="stars:starter-100:1:abc123",
        currency="XTR",
        prices=[{"label": "Starter", "amount": 250}],
    )

    await client.aclose()
    assert any(url.endswith("/getUpdates") for url in calls)
    assert any(url.endswith("/sendMessage") for url in calls)
    assert any(url.endswith("/answerPreCheckoutQuery") for url in calls)
    assert any(url.endswith("/sendInvoice") for url in calls)
