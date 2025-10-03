from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Protocol

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ProviderStatusResult:
    provider_status: str | None
    mapped_status: str | None
    response_code: int | None
    payload: Dict[str, Any] | None


@dataclass(slots=True)
class ProviderCallLog:
    request_url: str
    request_headers: Dict[str, Any]
    request_body: Dict[str, Any] | None
    response_status: int | None
    response_headers: Dict[str, Any] | None
    response_body: Dict[str, Any] | None
    error_message: str | None
    latency_ms: int | None


class ProviderClient(Protocol):
    name: str

    def status(self, token: str) -> tuple[ProviderStatusResult, ProviderCallLog]:  # pragma: no cover
        ...


def mask_sensitive_headers(headers: dict[str, Any]) -> dict[str, Any]:
    masked: dict[str, Any] = {}
    for key, value in headers.items():
        if key.lower() in {"authorization", "tbk-api-key-secret", "x-api-key"}:
            masked[key] = "***"
        else:
            masked[key] = value
    return masked
