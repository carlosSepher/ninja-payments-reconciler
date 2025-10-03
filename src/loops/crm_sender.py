from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict

from ..db import Database
from ..integrations.crm_client import CRMClient
from ..repositories import crm_repo, payments_repo
from ..settings import Settings


class CrmSender:
    def __init__(
        self,
        *,
        db: Database,
        settings: Settings,
        client: CRMClient,
    ) -> None:
        self._db = db
        self._settings = settings
        self._client = client
        self._heartbeat_at: datetime | None = None

    async def run(self) -> None:
        while True:
            if not self._settings.crm_enabled:
                await asyncio.sleep(self._settings.reconcile_interval_seconds)
                continue
            await asyncio.to_thread(self._process_once)
            await asyncio.sleep(self._settings.reconcile_interval_seconds)

    def _process_once(self) -> None:
        stats = {"sent": 0, "failed": 0, "retried": 0}
        with self._db.connection() as conn:
            reactivated = crm_repo.reactivate_failed_items(
                conn, limit=self._settings.reconcile_batch_size
            )
            if reactivated:
                stats["retried"] += reactivated
            queue_items = crm_repo.fetch_pending_crm_items(
                conn, limit=self._settings.reconcile_batch_size
            )
            now = datetime.now(timezone.utc)
            for item in queue_items:
                response, req_headers, req_body, resp_headers, resp_body, error_message = self._client.send(
                    item.payload
                )
                crm_repo.record_crm_event(
                    conn,
                    payment_id=item.payment_id,
                    operation=item.operation,
                    request_url=self._client.endpoint,
                    request_headers=req_headers,
                    request_body=req_body,
                    response_status=response.status_code,
                    response_headers=resp_headers,
                    response_body=resp_body,
                    error_message=error_message,
                    latency_ms=response.latency_ms,
                )

                if 200 <= response.status_code < 300 and error_message is None:
                    crm_repo.update_crm_item_success(
                        conn,
                        item_id=item.id,
                        response_code=response.status_code,
                        crm_id=response.crm_id,
                    )
                    stats["sent"] += 1
                else:
                    attempts = item.attempts + 1
                    backoff_index = min(attempts - 1, len(self._settings.crm_retry_backoff) - 1)
                    next_attempt = now + timedelta(
                        seconds=self._settings.crm_retry_backoff[backoff_index]
                    )
                    crm_repo.update_crm_item_failure(
                        conn,
                        item_id=item.id,
                        attempts=attempts,
                        next_attempt_at=next_attempt,
                        response_code=response.status_code if response.status_code else None,
                        error_message=error_message or "CRM send failed",
                    )
                    stats["failed"] += 1
            self._emit_runtime_log(conn, stats)

    def _emit_runtime_log(self, conn, stats: Dict[str, int]) -> None:
        now = datetime.now(timezone.utc)
        if self._heartbeat_at and now < self._heartbeat_at:
            return
        self._heartbeat_at = now + timedelta(seconds=self._settings.heartbeat_interval_seconds)
        payments_repo.log_service_runtime_event(
            conn,
            event_type="HEARTBEAT",
            payload={"crm_sender": stats},
        )
