from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

from bridge.types import Platform, PlatformApiError

logger = logging.getLogger(__name__)


class BotApiClient:
    def __init__(
        self,
        platform: Platform,
        token: str,
        api_base_url: str,
        file_base_url: str,
        timeout_sec: float = 40.0,
    ) -> None:
        self.platform = platform
        self.token = token
        self.api_base_url = api_base_url.rstrip("/")
        self.file_base_url = file_base_url.rstrip("/")
        timeout = httpx.Timeout(connect=15.0, read=timeout_sec, write=30.0, pool=30.0)
        self.client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self.client.aclose()

    def _method_url(self, method: str) -> str:
        return f"{self.api_base_url}/bot{self.token}/{method}"

    def file_url(self, file_path: str) -> str:
        return f"{self.file_base_url}/bot{self.token}/{file_path}"

    async def get_updates(self, offset: int | None, timeout: int, allowed_updates: list[str]) -> list[dict]:
        payload = {
            "offset": offset,
            "timeout": timeout,
            "allowed_updates": allowed_updates,
        }
        response = await self._post("getUpdates", json=payload)
        if not isinstance(response, list):
            raise PlatformApiError(f"{self.platform.value}:getUpdates invalid response type")
        return response

    async def send_message(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict | None = None,
        reply_to_message_id: int | None = None,
    ) -> dict:
        payload: dict = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if reply_to_message_id and reply_to_message_id > 0:
            payload["reply_to_message_id"] = reply_to_message_id
        result = await self._post("sendMessage", json=payload)
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:sendMessage invalid response")
        return result

    async def send_photo(
        self,
        chat_id: str,
        photo_file_id: str | None = None,
        photo_path: Path | None = None,
        caption: str | None = None,
        reply_markup: dict | None = None,
    ) -> dict:
        if photo_path is None and photo_file_id is None:
            raise ValueError("photo_path or photo_file_id must be provided")

        if photo_path is not None:
            data: dict[str, str] = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
            with photo_path.open("rb") as fh:
                files = {"photo": (photo_path.name, fh, "application/octet-stream")}
                result = await self._post("sendPhoto", data=data, files=files)
                if not isinstance(result, dict):
                    raise PlatformApiError(f"{self.platform.value}:sendPhoto invalid response")
                return result

        payload = {"chat_id": chat_id, "photo": photo_file_id}
        if caption:
            payload["caption"] = caption
        if reply_markup:
            payload["reply_markup"] = reply_markup
        result = await self._post("sendPhoto", json=payload)
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:sendPhoto invalid response")
        return result

    async def send_voice(
        self,
        chat_id: str,
        voice_file_id: str | None = None,
        voice_path: Path | None = None,
        caption: str | None = None,
        reply_markup: dict | None = None,
    ) -> dict:
        if voice_path is None and voice_file_id is None:
            raise ValueError("voice_path or voice_file_id must be provided")

        if voice_path is not None:
            data: dict[str, str] = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption
            if reply_markup:
                data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
            with voice_path.open("rb") as fh:
                files = {"voice": (voice_path.name, fh, "audio/ogg")}
                result = await self._post("sendVoice", data=data, files=files)
                if not isinstance(result, dict):
                    raise PlatformApiError(f"{self.platform.value}:sendVoice invalid response")
                return result

        payload = {"chat_id": chat_id, "voice": voice_file_id}
        if caption:
            payload["caption"] = caption
        if reply_markup:
            payload["reply_markup"] = reply_markup
        result = await self._post("sendVoice", json=payload)
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:sendVoice invalid response")
        return result

    async def get_file(self, file_id: str) -> dict:
        result = await self._post("getFile", json={"file_id": file_id})
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:getFile invalid response")
        return result

    async def get_chat(self, chat_id: str) -> dict:
        result = await self._post("getChat", json={"chat_id": chat_id})
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:getChat invalid response")
        return result

    async def get_user_profile_photos(self, user_id: str, limit: int = 1) -> dict:
        result = await self._post(
            "getUserProfilePhotos",
            json={"user_id": int(user_id), "offset": 0, "limit": limit},
        )
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:getUserProfilePhotos invalid response")
        return result

    async def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> dict:
        payload: dict = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        result = await self._post("answerCallbackQuery", json=payload)
        if not isinstance(result, bool):
            # Telegram/Bale typically return bool; normalize for callers.
            return {"ok": bool(result)}
        return {"ok": result}

    async def answer_pre_checkout_query(self, pre_checkout_query_id: str, ok: bool, error_message: str | None = None) -> dict:
        payload: dict = {"pre_checkout_query_id": pre_checkout_query_id, "ok": ok}
        if error_message:
            payload["error_message"] = error_message
        result = await self._post("answerPreCheckoutQuery", json=payload)
        if not isinstance(result, bool):
            return {"ok": bool(result)}
        return {"ok": result}

    async def set_my_commands(
        self,
        commands: list[dict[str, str]],
        *,
        language_code: str | None = None,
        scope: dict | None = None,
    ) -> dict:
        payload: dict = {"commands": commands}
        if language_code:
            payload["language_code"] = language_code
        if scope:
            payload["scope"] = scope
        result = await self._post("setMyCommands", json=payload)
        if not isinstance(result, bool):
            return {"ok": bool(result)}
        return {"ok": result}

    async def set_chat_menu_button(self, chat_id: str | None = None, menu_button: dict | None = None) -> dict:
        payload: dict = {}
        if chat_id is not None and chat_id != "":
            payload["chat_id"] = int(chat_id) if str(chat_id).lstrip("-").isdigit() else str(chat_id)
        if menu_button:
            payload["menu_button"] = menu_button
        result = await self._post("setChatMenuButton", json=payload)
        if not isinstance(result, bool):
            return {"ok": bool(result)}
        return {"ok": result}

    async def send_invoice(
        self,
        *,
        chat_id: str,
        title: str,
        description: str,
        payload: str,
        currency: str | None = None,
        prices: list[dict],
        provider_token: str | None = None,
    ) -> dict:
        data = {
            "chat_id": chat_id,
            "title": title,
            "description": description,
            "payload": payload,
            "prices": prices,
        }
        if currency:
            data["currency"] = currency
        if provider_token:
            data["provider_token"] = provider_token
        result = await self._post("sendInvoice", json=data)
        if not isinstance(result, dict):
            raise PlatformApiError(f"{self.platform.value}:sendInvoice invalid response")
        return result

    async def download_file(self, file_path: str, output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        url = self.file_url(file_path)
        async with self.client.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise PlatformApiError(f"{self.platform.value} file download failed: {resp.status_code}")
            with output_path.open("wb") as fh:
                async for chunk in resp.aiter_bytes():
                    fh.write(chunk)
        return output_path

    async def _post(
        self,
        method: str,
        *,
        json: dict | None = None,
        data: dict | None = None,
        files: dict | None = None,
    ) -> object:
        url = self._method_url(method)
        response = await self.client.post(url, json=json, data=data, files=files)
        if response.status_code != 200:
            raise PlatformApiError(f"{self.platform.value}:{method} HTTP {response.status_code} {response.text}")

        body = response.json()
        if not body.get("ok"):
            description = body.get("description", "unknown API error")
            raise PlatformApiError(f"{self.platform.value}:{method} {description}")
        return body["result"]
