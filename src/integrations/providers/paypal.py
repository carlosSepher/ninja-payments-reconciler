from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict

import httpx

from .base import ProviderCallLog, ProviderClient, ProviderStatusResult, mask_sensitive_headers


class PayPalProvider:
    name = "paypal"

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self.client_id = client_id or os.getenv("PAYPAL_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("PAYPAL_CLIENT_SECRET")
        self.base_url = base_url or os.getenv("PAYPAL_BASE_URL", "https://api.paypal.com")

    def status(self, token: str) -> tuple[ProviderStatusResult, ProviderCallLog]:
        url = f"{self.base_url}/v2/checkout/orders/{token}"
        headers: Dict[str, str] = {
            "Content-Type": "application/json",
        }

        start = time.monotonic()
        error_message: str | None = None
        response_status: int | None = None
        response_headers: Dict[str, Any] | None = None
        response_body: Dict[str, Any] | None = None

        auth_header: Dict[str, str] | None = None
        try:
            access_token = self._fetch_access_token()
            if access_token:
                headers["Authorization"] = f"Bearer {access_token}"
                auth_header = {"Authorization": headers["Authorization"]}
        except httpx.HTTPError as exc:  # pragma: no cover - network
            error_message = f"token_error: {exc}"  # type: ignore[str-format]
        except ValueError as exc:
            error_message = str(exc)

        if error_message is None:
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

        latency_ms = int((time.monotonic() - start) * 1000)

        provider_status = None
        mapped_status = None
        if response_body and isinstance(response_body, dict):
            provider_status = response_body.get("status")
            mapped_status = self._map_status(provider_status)

        result = ProviderStatusResult(
            provider_status=provider_status,
            mapped_status=mapped_status,
            response_code=response_status,
            payload=response_body,
        )
        merged_headers = {**headers}
        if auth_header:
            merged_headers.update(auth_header)
        log = ProviderCallLog(
            request_url=url,
            request_headers=mask_sensitive_headers(merged_headers),
            request_body=None,
            response_status=response_status,
            response_headers=mask_sensitive_headers(response_headers or {}),
            response_body=response_body,
            error_message=error_message,
            latency_ms=latency_ms,
        )
        return result, log

    def _fetch_access_token(self) -> str:
        if not self.client_id or not self.client_secret:
            raise ValueError("PayPal credentials are not configured")
        token_url = f"{self.base_url}/v1/oauth2/token"
        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        with httpx.Client(timeout=10) as client:
            response = client.post(token_url, headers=headers, data={"grant_type": "client_credentials"})
        response.raise_for_status()
        data = response.json()
        return data["access_token"]

    @staticmethod
    def _map_status(provider_status: str | None) -> str | None:
        if provider_status is None:
            return None
        mapping = {
            "COMPLETED": "AUTHORIZED",
            "APPROVED": "TO_CONFIRM",
            "CREATED": "PENDING",
            "VOIDED": "CANCELED",
            "PAYER_ACTION_REQUIRED": "TO_CONFIRM",
        }
        return mapping.get(provider_status.upper(), None)


def create() -> ProviderClient:
    return PayPalProvider()
