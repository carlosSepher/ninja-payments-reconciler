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
        LOGGER.info(f"PSP Poller loop started - providers: {list(self._providers.keys())}")
        while True:
            if not self._settings.reconcile_enabled:
                LOGGER.debug("Reconciliation is disabled, sleeping...")
                await asyncio.sleep(self._settings.reconcile_interval_seconds)
                continue
            try:
                await asyncio.to_thread(self._process_once)
            except Exception as exc:
                LOGGER.exception("Error in PSP poller loop: %s", exc)
            await asyncio.sleep(self._settings.reconcile_interval_seconds)

    def _process_once(self) -> None:
        stats = {
            "payments": 0,
            "updated": 0,
            "failed": 0,
            "skipped": 0,
        }
        LOGGER.debug("PSP Poller: Starting processing cycle")

        with self._db.connection() as conn:
            payments = payments_repo.select_payments_for_reconciliation(
                conn,
                providers=self._settings.reconcile_polling_providers,
                batch_size=self._settings.reconcile_batch_size,
            )
            LOGGER.info(f"PSP Poller: Found {len(payments)} payments to reconcile")

            now = datetime.now(timezone.utc)
            for payment in payments:
                stats["payments"] += 1
                provider = self._providers.get(payment.provider)
                if not provider:
                    LOGGER.warning(
                        f"PSP Poller: No provider client configured for {payment.provider}, "
                        f"payment_id={payment.id}"
                    )
                    stats["skipped"] += 1
                    continue

                attempt_index = payment.attempts
                if attempt_index >= len(self._settings.reconcile_attempt_offsets):
                    payments_repo.mark_attempts_exhausted(conn, payment_id=payment.id)
                    payload = build_payload(payment, "ABANDONED_CART")
                    crm_repo.enqueue_crm_operation(
                        conn,
                        payment_id=payment.id,
                        operation="ABANDONED_CART",
                        payload=payload,
                    )
                    stats.setdefault("abandoned", 0)
                    stats["abandoned"] += 1
                    stats["failed"] += 1
                    LOGGER.warning(
                        f"PSP Poller: Attempts exhausted for payment_id={payment.id}, "
                        f"provider={payment.provider}, attempts={attempt_index}"
                    )
                    continue

                due_at = payment.created_at + timedelta(
                    seconds=self._settings.reconcile_attempt_offsets[attempt_index]
                )
                if now < due_at:
                    stats["skipped"] += 1
                    LOGGER.debug(
                        f"PSP Poller: Skipping payment_id={payment.id}, "
                        f"not yet due (due_at={due_at.isoformat()})"
                    )
                    continue

                LOGGER.debug(
                    f"PSP Poller: Checking status for payment_id={payment.id}, "
                    f"provider={payment.provider}, token={payment.token}, "
                    f"attempt={attempt_index + 1}"
                )

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

                if call_log.error_message:
                    LOGGER.error(
                        f"PSP Poller: ✗ Error checking payment_id={payment.id}, "
                        f"provider={payment.provider}, error={call_log.error_message}"
                    )

                if result.mapped_status is None:
                    if attempt_index + 1 >= len(self._settings.reconcile_attempt_offsets):
                        payments_repo.mark_attempts_exhausted(conn, payment_id=payment.id)
                        payload = build_payload(payment, "ABANDONED_CART")
                        crm_repo.enqueue_crm_operation(
                            conn,
                            payment_id=payment.id,
                            operation="ABANDONED_CART",
                            payload=payload,
                        )
                        stats.setdefault("abandoned", 0)
                        stats["abandoned"] += 1
                        stats["failed"] += 1
                        LOGGER.warning(
                            f"PSP Poller: No mapped status and attempts exhausted for "
                            f"payment_id={payment.id}, provider_status={result.provider_status}"
                        )
                    else:
                        LOGGER.debug(
                            f"PSP Poller: No mapped status yet for payment_id={payment.id}, "
                            f"will retry later"
                        )
                    continue

                if result.mapped_status == payment.status:
                    LOGGER.debug(
                        f"PSP Poller: No status change for payment_id={payment.id}, "
                        f"status={payment.status}"
                    )
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
                LOGGER.info(
                    f"PSP Poller: ✓ Status updated for payment_id={payment.id}, "
                    f"provider={payment.provider}, {payment.status} → {result.mapped_status}, "
                    f"provider_status={result.provider_status}"
                )

                if result.mapped_status == "AUTHORIZED":
                    payload = build_payload(payment, "PAYMENT_APPROVED")
                    crm_repo.enqueue_crm_operation(
                        conn,
                        payment_id=payment.id,
                        operation="PAYMENT_APPROVED",
                        payload=payload,
                    )
                    LOGGER.info(
                        f"PSP Poller: Enqueued CRM notification for payment_id={payment.id}, "
                        f"operation=PAYMENT_APPROVED"
                    )

            cutoff = now - timedelta(minutes=self._settings.abandoned_timeout_minutes)
            abandoned_payments = payments_repo.find_abandoned_payments(
                conn, cutoff=cutoff, limit=self._settings.reconcile_batch_size
            )

            if abandoned_payments:
                LOGGER.info(f"PSP Poller: Found {len(abandoned_payments)} abandoned payments")

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
                LOGGER.info(
                    f"PSP Poller: Marked payment_id={abandoned.id} as ABANDONED, "
                    f"enqueued CRM notification"
                )

            self._emit_runtime_log(conn, stats)
            LOGGER.info(
                f"PSP Poller: Cycle completed - "
                f"payments={stats['payments']}, updated={stats['updated']}, "
                f"failed={stats['failed']}, skipped={stats['skipped']}, "
                f"abandoned={stats.get('abandoned', 0)}"
            )

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
        LOGGER.debug(f"PSP Poller: Heartbeat recorded - {stats}")
