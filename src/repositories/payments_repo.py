from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Sequence

import psycopg2.extras


@dataclass(slots=True)
class Payment:
    id: int
    status: str
    provider: str
    token: str
    created_at: datetime
    amount_minor: int
    provider_metadata: dict | None
    context: dict | None
    product_id: int | None
    authorization_code: str | None
    status_reason: str | None
    attempts: int


def select_payments_for_reconciliation(
    conn,
    *,
    providers: Sequence[str],
    batch_size: int,
) -> List[Payment]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                p.id,
                p.status,
                p.provider,
                p.token,
                p.created_at,
                p.amount_minor,
                p.provider_metadata,
                p.context,
                p.product_id,
                p.authorization_code,
                p.status_reason,
                COALESCE(sc.attempts, 0) AS attempts
            FROM payments.payment AS p
            LEFT JOIN (
                SELECT payment_id, COUNT(*) AS attempts
                FROM payments.status_check
                GROUP BY payment_id
            ) AS sc ON sc.payment_id = p.id
            WHERE p.status IN ('PENDING', 'TO_CONFIRM')
              AND p.token IS NOT NULL
              AND p.provider = ANY(%s)
            ORDER BY p.created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (list(providers), batch_size),
        )
        rows = cur.fetchall()

    payments: List[Payment] = []
    for row in rows:
        payments.append(
            Payment(
                id=row["id"],
                status=row["status"],
                provider=row["provider"],
                token=row["token"],
                created_at=row["created_at"],
                amount_minor=row["amount_minor"],
                provider_metadata=row.get("provider_metadata"),
                context=row.get("context"),
                product_id=row.get("product_id"),
                authorization_code=row.get("authorization_code"),
                status_reason=row.get("status_reason"),
                attempts=row.get("attempts", 0),
            )
        )
    return payments


def record_status_check(
    conn,
    *,
    payment_id: int,
    provider: str,
    success: bool,
    provider_status: str | None,
    mapped_status: str | None,
    response_code: int | None,
    raw_payload: dict | None,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.status_check (
                payment_id,
                provider,
                success,
                provider_status,
                mapped_status,
                response_code,
                raw_payload,
                error_message,
                requested_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, NOW())
            """,
            (
                payment_id,
                provider,
                success,
                provider_status,
                mapped_status,
                response_code,
                psycopg2.extras.Json(raw_payload) if raw_payload is not None else None,
                error_message,
            ),
        )


def record_provider_event(
    conn,
    *,
    payment_id: int,
    provider: str,
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
            INSERT INTO payments.provider_event_log (
                payment_id,
                provider,
                request_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                error_message,
                latency_ms
            )
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, %s)
            """,
            (
                payment_id,
                provider,
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


def update_payment_status(
    conn,
    *,
    payment_id: int,
    new_status: str,
    status_reason: str | None,
) -> None:
    timestamp_field: str | None = None
    if new_status == "AUTHORIZED":
        timestamp_field = "first_authorized_at"
    elif new_status == "FAILED":
        timestamp_field = "failed_at"
    elif new_status == "CANCELED":
        timestamp_field = "canceled_at"
    elif new_status == "REFUNDED":
        timestamp_field = "refunded_at"
    elif new_status == "ABANDONED":
        timestamp_field = "abandoned_at"

    set_clauses = ["status = %s", "updated_at = NOW()"]
    params: List[object] = [new_status]
    if status_reason is not None:
        set_clauses.append("status_reason = %s")
        params.append(status_reason)
    if timestamp_field is not None:
        set_clauses.append(f"{timestamp_field} = COALESCE({timestamp_field}, NOW())")

    params.append(payment_id)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE payments.payment
            SET {', '.join(set_clauses)}
            WHERE id = %s
            """,
            params,
        )


def mark_attempts_exhausted(conn, *, payment_id: int) -> None:
    update_payment_status(
        conn,
        payment_id=payment_id,
        new_status="FAILED",
        status_reason="reconcile attempts exhausted",
    )


def find_abandoned_payments(
    conn,
    *,
    cutoff: datetime,
    limit: int = 100,
) -> List[Payment]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                p.id,
                p.status,
                p.provider,
                p.token,
                p.created_at,
                p.amount_minor,
                p.provider_metadata,
                p.context,
                p.product_id,
                p.authorization_code,
                p.status_reason,
                0 AS attempts
            FROM payments.payment AS p
            WHERE p.status = 'PENDING'
              AND p.created_at <= %s
            ORDER BY p.created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (cutoff, limit),
        )
        rows = cur.fetchall()

    return [
        Payment(
            id=row["id"],
            status=row["status"],
            provider=row["provider"],
            token=row["token"],
            created_at=row["created_at"],
            amount_minor=row["amount_minor"],
            provider_metadata=row.get("provider_metadata"),
            context=row.get("context"),
            product_id=row.get("product_id"),
            authorization_code=row.get("authorization_code"),
            status_reason=row.get("status_reason"),
            attempts=row.get("attempts", 0),
        )
        for row in rows
    ]


def log_service_runtime_event(
    conn,
    *,
    event_type: str,
    payload: dict | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.service_runtime_log (
                event_type,
                payload
            ) VALUES (%s, %s::jsonb)
            """,
            (
                event_type,
                psycopg2.extras.Json(payload) if payload is not None else None,
            ),
        )
