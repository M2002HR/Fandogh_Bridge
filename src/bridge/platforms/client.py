from __future__ import annotations

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
        self.client = httpx.AsyncClient(timeout=timeout_sec)

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

    async def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> dict:
        payload: dict = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup
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
    ) -> dict:
        if photo_path is None and photo_file_id is None:
            raise ValueError("photo_path or photo_file_id must be provided")

        if photo_path is not None:
            data: dict[str, str] = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption
            with photo_path.open("rb") as fh:
                files = {"photo": (photo_path.name, fh, "application/octet-stream")}
                result = await self._post("sendPhoto", data=data, files=files)
                if not isinstance(result, dict):
                    raise PlatformApiError(f"{self.platform.value}:sendPhoto invalid response")
                return result

        payload = {"chat_id": chat_id, "photo": photo_file_id}
        if caption:
            payload["caption"] = caption
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
    ) -> dict:
        if voice_path is None and voice_file_id is None:
            raise ValueError("voice_path or voice_file_id must be provided")

        if voice_path is not None:
            data: dict[str, str] = {"chat_id": chat_id}
            if caption:
                data["caption"] = caption
            with voice_path.open("rb") as fh:
                files = {"voice": (voice_path.name, fh, "audio/ogg")}
                result = await self._post("sendVoice", data=data, files=files)
                if not isinstance(result, dict):
                    raise PlatformApiError(f"{self.platform.value}:sendVoice invalid response")
                return result

        payload = {"chat_id": chat_id, "voice": voice_file_id}
        if caption:
            payload["caption"] = caption
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
    ) -> dict | list[dict]:
        url = self._method_url(method)
        response = await self.client.post(url, json=json, data=data, files=files)
        if response.status_code != 200:
            raise PlatformApiError(f"{self.platform.value}:{method} HTTP {response.status_code} {response.text}")

        body = response.json()
        if not body.get("ok"):
            description = body.get("description", "unknown API error")
            raise PlatformApiError(f"{self.platform.value}:{method} {description}")
        return body["result"]
