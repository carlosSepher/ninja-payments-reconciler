from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict

from ..db import Database
from ..integrations.providers.base import ProviderClient
from ..repositories import crm_repo, payments_repo
from ..services.crm_payloads import build_payload
from ..settings import Settings

LOGGER = logging.getLogger(__name__)


class PspPoller:
    def __init__(
        self,
        *,
        db: Database,
        settings: Settings,
        providers: Dict[str, ProviderClient],
    ) -> None:
        self._db = db
        self._settings = settings
        self._providers = providers
        self._heartbeat_at: datetime | None = None

    async def run(self) -> None:
        while True:
            if not self._settings.reconcile_enabled:
                await asyncio.sleep(self._settings.reconcile_interval_seconds)
                continue
            await asyncio.to_thread(self._process_once)
            await asyncio.sleep(self._settings.reconcile_interval_seconds)

    def _process_once(self) -> None:
        stats = {
            "payments": 0,
            "updated": 0,
            "failed": 0,
            "skipped": 0,
        }
        with self._db.connection() as conn:
            payments = payments_repo.select_payments_for_reconciliation(
                conn,
                providers=self._settings.reconcile_polling_providers,
                batch_size=self._settings.reconcile_batch_size,
            )
            now = datetime.now(timezone.utc)
            for payment in payments:
                stats["payments"] += 1
                provider = self._providers.get(payment.provider)
                if not provider:
                    LOGGER.warning("No provider client configured for %s", payment.provider)
                    stats["skipped"] += 1
                    continue

                attempt_index = payment.attempts
                if attempt_index >= len(self._settings.reconcile_attempt_offsets):
                    payments_repo.mark_attempts_exhausted(conn, payment_id=payment.id)
                    stats["failed"] += 1
                    continue

                due_at = payment.created_at + timedelta(
                    seconds=self._settings.reconcile_attempt_offsets[attempt_index]
                )
                if now < due_at:
                    stats["skipped"] += 1
                    continue

                result, call_log = provider.status(payment.token)
                payments_repo.record_provider_event(
                    conn,
                    payment_id=payment.id,
                    provider=payment.provider,
                    request_url=call_log.request_url,
                    request_headers=call_log.request_headers,
                    request_body=call_log.request_body,
                    response_status=call_log.response_status,
                    response_headers=call_log.response_headers,
                    response_body=call_log.response_body,
                    error_message=call_log.error_message,
                    latency_ms=call_log.latency_ms,
                )

                success = call_log.error_message is None and result.provider_status is not None
                payments_repo.record_status_check(
                    conn,
                    payment_id=payment.id,
                    provider=payment.provider,
                    success=success,
                    provider_status=result.provider_status,
                    mapped_status=result.mapped_status,
                    response_code=result.response_code,
                    raw_payload=result.payload,
                    error_message=call_log.error_message,
                )

                if result.mapped_status is None:
                    if attempt_index + 1 >= len(self._settings.reconcile_attempt_offsets):
                        payments_repo.mark_attempts_exhausted(conn, payment_id=payment.id)
                        stats["failed"] += 1
                    continue

                if result.mapped_status == payment.status:
                    continue

                status_reason = payment.status_reason
                if result.mapped_status in {"AUTHORIZED", "FAILED", "CANCELED", "REFUNDED"}:
                    status_reason = "provider reconciliation update"

                payments_repo.update_payment_status(
                    conn,
                    payment_id=payment.id,
                    new_status=result.mapped_status,
                    status_reason=status_reason,
                )
                stats["updated"] += 1

                if result.mapped_status == "AUTHORIZED":
                    payload = build_payload(payment, "PAYMENT_APPROVED")
                    crm_repo.enqueue_crm_operation(
                        conn,
                        payment_id=payment.id,
                        operation="PAYMENT_APPROVED",
                        payload=payload,
                    )


            cutoff = now - timedelta(minutes=self._settings.abandoned_timeout_minutes)
            abandoned_payments = payments_repo.find_abandoned_payments(
                conn, cutoff=cutoff, limit=self._settings.reconcile_batch_size
            )
            for abandoned in abandoned_payments:
                payments_repo.update_payment_status(
                    conn,
                    payment_id=abandoned.id,
                    new_status="ABANDONED",
                    status_reason="abandoned timeout",
                )
                payload = build_payload(abandoned, "ABANDONED_CART")
                crm_repo.enqueue_crm_operation(
                    conn,
                    payment_id=abandoned.id,
                    operation="ABANDONED_CART",
                    payload=payload,
                )
                stats.setdefault("abandoned", 0)
                stats["abandoned"] += 1

            self._emit_runtime_log(conn, stats)

    def _emit_runtime_log(self, conn, stats: Dict[str, int]) -> None:
        now = datetime.now(timezone.utc)
        if self._heartbeat_at and now < self._heartbeat_at:
            return
        self._heartbeat_at = now + timedelta(seconds=self._settings.heartbeat_interval_seconds)
        payments_repo.log_service_runtime_event(
            conn,
            event_type="HEARTBEAT",
            payload={"psp_poller": stats},
        )
