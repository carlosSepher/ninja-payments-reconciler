from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict

import httpx

from .providers.base import mask_sensitive_headers


@dataclass(slots=True)
class CrmResponse:
    status_code: int
    headers: Dict[str, Any]
    body: Dict[str, Any] | None
    crm_id: str | None
    latency_ms: int


class CRMClient:
    def __init__(
        self,
        *,
        base_url: str,
        pagar_path: str,
        bearer_token: str | None,
        timeout_seconds: int,
        log_requests: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.pagar_path = pagar_path
        self.bearer_token = bearer_token
        self.timeout_seconds = timeout_seconds
        self.log_requests = log_requests

    @property
    def endpoint(self) -> str:
        return f"{self.base_url}{self.pagar_path}"

    def send(
        self, payload: Dict[str, Any]
    ) -> tuple[CrmResponse, Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any], str | None]:
        url = self.endpoint
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        start = time.monotonic()
        response_headers: Dict[str, Any] | None = None
        response_body: Dict[str, Any] | None = None
        status_code: int = 0
        crm_id: str | None = None
        error_message: str | None = None
        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.post(url, headers=headers, json=payload)
            status_code = response.status_code
            response_headers = dict(response.headers)
            if response.headers.get("content-type", "").startswith("application/json"):
                response_body = response.json()
            else:
                response_body = {"raw": response.text}
            if isinstance(response_body, dict):
                crm_id = response_body.get("id")
        except httpx.HTTPError as exc:  # pragma: no cover - network
            error_message = str(exc)

        latency_ms = int((time.monotonic() - start) * 1000)
        if response_body is not None:
            response_payload = response_body
        elif error_message is not None:
            response_payload = {"error": error_message}
        else:
            response_payload = {"status_code": status_code}
        crm_response = CrmResponse(
            status_code=status_code,
            headers=response_headers or {},
            body=response_payload,
            crm_id=crm_id,
            latency_ms=latency_ms,
        )
        masked_request_headers = mask_sensitive_headers(headers)
        masked_response_headers = mask_sensitive_headers(response_headers or {})
        return (
            crm_response,
            masked_request_headers,
            payload,
            masked_response_headers,
            response_payload,
            error_message,
        )
