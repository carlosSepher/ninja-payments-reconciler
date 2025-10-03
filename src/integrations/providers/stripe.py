from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict

import httpx

from .base import ProviderCallLog, ProviderClient, ProviderStatusResult, mask_sensitive_headers


class StripeProvider:
    name = "stripe"

    def __init__(self, *, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("STRIPE_API_KEY")
        self.base_url = os.getenv("STRIPE_API_BASE", "https://api.stripe.com")

    def status(self, token: str) -> tuple[ProviderStatusResult, ProviderCallLog]:
        url = f"{self.base_url}/v1/payment_intents/{token}"
        headers: Dict[str, str] = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if not self.api_key:
            error = "Stripe API key is not configured"
            result = ProviderStatusResult(None, None, None, None)
            log = ProviderCallLog(
                request_url=url,
                request_headers=headers,
                request_body=None,
                response_status=None,
                response_headers=None,
                response_body=None,
                error_message=error,
                latency_ms=0,
            )
            return result, log

        start = time.monotonic()
        error_message: str | None = None
        response_status: int | None = None
        response_headers: Dict[str, Any] | None = None
        response_body: Dict[str, Any] | None = None
        try:
            with httpx.Client(timeout=10) as client:
                resp = client.get(url, auth=(self.api_key, ""), headers=headers)
            response_status = resp.status_code
            response_headers = dict(resp.headers)
            response_body = resp.json()
        except httpx.HTTPError as exc:  # pragma: no cover - network
            error_message = str(exc)
            resp = None  # type: ignore[assignment]

        latency_ms = int((time.monotonic() - start) * 1000)

        provider_status = None
        mapped_status = None
        if response_body:
            provider_status = response_body.get("status")
            mapped_status = self._map_status(provider_status)

        result = ProviderStatusResult(
            provider_status=provider_status,
            mapped_status=mapped_status,
            response_code=response_status,
            payload=response_body,
        )
        log = ProviderCallLog(
            request_url=url,
            request_headers=mask_sensitive_headers({**headers, "Authorization": f"Basic {base64.b64encode(f'{self.api_key}:'.encode()).decode()}"})
            if self.api_key
            else headers,
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
        if provider_status is None:
            return None
        mapping = {
            "succeeded": "AUTHORIZED",
            "processing": "TO_CONFIRM",
            "requires_payment_method": "FAILED",
            "requires_action": "TO_CONFIRM",
            "requires_capture": "AUTHORIZED",
            "canceled": "CANCELED",
        }
        return mapping.get(provider_status.lower(), None)


def create() -> ProviderClient:
    return StripeProvider()
