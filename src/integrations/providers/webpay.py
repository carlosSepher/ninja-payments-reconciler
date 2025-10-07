from __future__ import annotations

import os
import time
from typing import Any, Dict

import httpx

from .base import ProviderCallLog, ProviderClient, ProviderStatusResult, mask_sensitive_headers


class WebpayProvider:
    name = "webpay"

    def __init__(
        self,
        *,
        status_url_template: str | None = None,
        api_key_id: str | None = None,
        api_key_secret: str | None = None,
        commerce_code: str | None = None,
    ) -> None:
        self.status_url_template = status_url_template or os.getenv(
            "WEBPAY_STATUS_URL_TEMPLATE", "https://webpay.transbank.cl/rest/transactions/{token}"
        )
        self.api_key_id = api_key_id or os.getenv("WEBPAY_API_KEY_ID")
        self.api_key_secret = api_key_secret or os.getenv("WEBPAY_API_KEY_SECRET")
        self.commerce_code = commerce_code or os.getenv("WEBPAY_COMMERCE_CODE")

    def status(self, token: str) -> tuple[ProviderStatusResult, ProviderCallLog]:
        url = self.status_url_template.format(token=token)
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
        }
        if self.api_key_id:
            headers["Tbk-Api-Key-Id"] = self.api_key_id
        if self.api_key_secret:
            headers["Tbk-Api-Key-Secret"] = self.api_key_secret
        if self.commerce_code:
            headers["Tbk-Commerce-Code"] = self.commerce_code

        start = time.monotonic()
        error_message: str | None = None
        response_status: int | None = None
        response_headers: Dict[str, Any] | None = None
        response_body: Dict[str, Any] | None = None
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(url, headers=headers)
            response_status = resp.status_code
            response_headers = dict(resp.headers)
            if resp.headers.get("content-type", "").startswith("application/json"):
                response_body = resp.json()
            else:
                response_body = {"raw": resp.text}
        except httpx.HTTPError as exc:  # pragma: no cover - network
            error_message = str(exc)
            response_body = None
            response_headers = None
            resp = None  # type: ignore[assignment]

        latency_ms = int((time.monotonic() - start) * 1000)

        provider_status = None
        mapped_status = None
        if response_body and isinstance(response_body, dict):
            provider_status = str(response_body.get("status")) if response_body.get("status") else None
            mapped_status = self._map_status(provider_status)

        result = ProviderStatusResult(
            provider_status=provider_status,
            mapped_status=mapped_status,
            response_code=response_status,
            payload=response_body,
        )
        log = ProviderCallLog(
            request_url=url,
            request_headers=mask_sensitive_headers(headers),
            request_body=None,
            response_status=response_status,
            response_headers=mask_sensitive_headers(response_headers or {}),
            response_body=response_body,
            error_message=error_message,
            latency_ms=latency_ms,
        )
        return result, log

    @staticmethod
    def _map_status(provider_status: str | None) -> str | None:
        mapping = {
            "AUTHORIZED": "AUTHORIZED",
            "FAILED": "FAILED",
            "REJECTED": "FAILED",
            "REVERSED": "CANCELED",
            "NULLIFIED": "CANCELED",
            "PENDING": "PENDING",
            "INITIALIZED": "PENDING",
        }
        if provider_status is None:
            return None
        return mapping.get(provider_status.upper(), None)


def create() -> ProviderClient:
    return WebpayProvider()
