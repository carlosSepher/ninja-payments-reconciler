from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

import psycopg2.extras


@dataclass(slots=True)
class CrmQueueItem:
    id: int
    payment_id: int
    operation: str
    status: str
    attempts: int
    next_attempt_at: datetime | None
    payload: dict


def enqueue_crm_operation(
    conn,
    *,
    payment_id: int,
    operation: str,
    payload: dict,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.crm_push_queue (
                payment_id,
                operation,
                status,
                attempts,
                payload
            ) VALUES (%s, %s, 'PENDING', 0, %s::jsonb)
            ON CONFLICT (payment_id, operation)
            DO UPDATE SET
                status = 'PENDING',
                attempts = 0,
                next_attempt_at = NULL,
                last_attempt_at = NULL,
                response_code = NULL,
                crm_id = NULL,
                last_error = NULL,
                payload = EXCLUDED.payload,
                updated_at = NOW()
            """,
            (
                payment_id,
                operation,
                psycopg2.extras.Json(payload),
            ),
        )


def fetch_pending_crm_items(conn, *, limit: int = 50) -> List[CrmQueueItem]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                id,
                payment_id,
                operation,
                status,
                attempts,
                next_attempt_at,
                payload
            FROM payments.crm_push_queue
            WHERE status = 'PENDING'
              AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    return [
        CrmQueueItem(
            id=row["id"],
            payment_id=row["payment_id"],
            operation=row["operation"],
            status=row["status"],
            attempts=row["attempts"],
            next_attempt_at=row.get("next_attempt_at"),
            payload=row.get("payload") or {},
        )
        for row in rows
    ]


def update_crm_item_success(
    conn,
    *,
    item_id: int,
    response_code: int,
    crm_id: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE payments.crm_push_queue
            SET status = 'SENT',
                response_code = %s,
                crm_id = %s,
                last_error = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (response_code, crm_id, item_id),
        )


def update_crm_item_failure(
    conn,
    *,
    item_id: int,
    attempts: int,
    next_attempt_at: datetime | None,
    response_code: int | None,
    error_message: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE payments.crm_push_queue
            SET status = 'FAILED',
                attempts = %s,
                next_attempt_at = %s,
                last_attempt_at = NOW(),
                response_code = %s,
                last_error = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (attempts, next_attempt_at, response_code, error_message, item_id),
        )


def reset_crm_item_for_retry(
    conn,
    *,
    item_id: int,
    attempts: int,
    next_attempt_at: datetime | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE payments.crm_push_queue
            SET status = 'PENDING',
                attempts = %s,
                next_attempt_at = %s,
                last_attempt_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (attempts, next_attempt_at, item_id),
        )


def record_crm_event(
    conn,
    *,
    payment_id: int,
    operation: str,
    request_url: str,
    request_headers: dict,
    request_body: dict | None,
    response_status: int | None,
    response_headers: dict | None,
    response_body: dict | None,
    error_message: str | None,
    latency_ms: int | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.crm_event_log (
                payment_id,
                operation,
                request_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                error_message,
                latency_ms
            ) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, %s)
            """,
            (
                payment_id,
                operation,
                request_url,
                psycopg2.extras.Json(request_headers),
                psycopg2.extras.Json(request_body) if request_body is not None else None,
                response_status,
                psycopg2.extras.Json(response_headers) if response_headers is not None else None,
                psycopg2.extras.Json(response_body) if response_body is not None else None,
                error_message,
                latency_ms,
            ),
        )



def reactivate_failed_items(conn, *, limit: int = 100) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH moved AS (
                SELECT id
                FROM payments.crm_push_queue
                WHERE status = 'FAILED'
                  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                ORDER BY next_attempt_at NULLS FIRST
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            )
            UPDATE payments.crm_push_queue AS q
            SET status = 'PENDING'
            FROM moved
            WHERE q.id = moved.id
            RETURNING q.id
            """,
            (limit,),
        )
        return cur.rowcount
