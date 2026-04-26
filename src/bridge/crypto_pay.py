from __future__ import annotations

from typing import Any

import httpx


class CryptoPayError(Exception):
    pass


class CryptoPayClient:
    def __init__(self, *, api_token: str, base_url: str, timeout_sec: int = 12) -> None:
        self.api_token = api_token.strip()
        self.base_url = base_url.rstrip("/")
        timeout = httpx.Timeout(connect=10.0, read=float(timeout_sec), write=10.0, pool=10.0)
        self.client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        await self.client.aclose()

    async def create_invoice(
        self,
        *,
        amount_usd: float,
        payload: str,
        description: str,
        paid_btn_url: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "currency_type": "fiat",
            "fiat": "USD",
            "accepted_assets": "TON",
            "amount": f"{amount_usd:.2f}",
            "description": description[:1024],
            "payload": payload,
            "allow_comments": False,
            "allow_anonymous": True,
        }
        if paid_btn_url:
            body["paid_btn_name"] = "openBot"
            body["paid_btn_url"] = paid_btn_url
        result = await self._call("createInvoice", body)
        if not isinstance(result, dict):
            raise CryptoPayError("createInvoice invalid response payload")
        return result

    async def get_invoices(self, invoice_ids: list[int]) -> list[dict[str, Any]]:
        ids = [str(x) for x in invoice_ids if x > 0]
        if not ids:
            return []
        result = await self._call("getInvoices", {"invoice_ids": ",".join(ids)})
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
        if isinstance(result, dict):
            items = result.get("items")
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        raise CryptoPayError("getInvoices invalid response payload")

    async def _call(self, method: str, body: dict[str, Any]) -> Any:
        if not self.api_token:
            raise CryptoPayError("missing Crypto Pay token")
        url = f"{self.base_url}/{method}"
        headers = {
            "Crypto-Pay-API-Token": self.api_token,
            "Content-Type": "application/json",
        }
        response = await self.client.post(url, headers=headers, json=body)
        if response.status_code != 200:
            raise CryptoPayError(f"{method} HTTP {response.status_code}: {response.text[:500]}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise CryptoPayError(f"{method} invalid JSON body")
        if not payload.get("ok"):
            raise CryptoPayError(f"{method} failed: {payload.get('error') or payload}")
        return payload.get("result")
