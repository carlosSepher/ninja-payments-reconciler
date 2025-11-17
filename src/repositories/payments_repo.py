from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Sequence

from decimal import Decimal, InvalidOperation

import psycopg2.extras


@dataclass(slots=True)
class Payment:
    id: int
    status: str
    provider: str
    token: str
    created_at: datetime
    amount_minor: Decimal
    provider_metadata: dict | None
    context: dict | None
    product_id: int | None
    authorization_code: str | None
    status_reason: str | None
    attempts: int
    payment_order_id: int | None
    order_customer_rut: str | None
    should_notify_crm: bool
    contract_number: int | None
    payment_type: str
    quota_numbers: tuple[int, ...]
    deposit_name: str | None
    deposit_rut: str | None
    currency: str | None
    aux_amount_minor: Decimal | None


@dataclass(slots=True)
class PaymentsMetrics:
    total_payments: int
    authorized_payments: int
    total_amount_minor: Decimal
    total_amount_currency: str | None
    last_payment_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_payments": self.total_payments,
            "authorized_payments": self.authorized_payments,
            "total_amount_minor": float(self.total_amount_minor),
            "total_amount_currency": self.total_amount_currency,
            "last_payment_at": self.last_payment_at.isoformat() if self.last_payment_at else None,
        }


def _normalize_contract_number(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = int(value)
    except (ArithmeticError, TypeError, ValueError):
        return None
    return number if number > 0 else None


def _normalize_payment_type(value: Any) -> str:
    if not value:
        return "contrato"
    normalized = str(value).strip().lower()
    if normalized == "cuota":
        return "cuotas"
    if normalized in {"contrato", "cuotas"}:
        return normalized
    return normalized


def _normalize_quota_numbers(value: Any) -> tuple[int, ...]:
    if not value:
        return tuple()
    numbers: list[int] = []
    if isinstance(value, (list, tuple, set)):
        iterable = value
    else:
        iterable = str(value).strip("{}").split(",")
    for item in iterable:
        try:
            number = int(item)
        except (TypeError, ValueError):
            continue
        if number > 0:
            numbers.append(number)
    return tuple(numbers)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_currency(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip().upper()
    return text or None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def select_payments_for_reconciliation(
    conn,
    *,
    providers: Sequence[str],
    batch_size: int,
) -> List[Payment]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH payment_attempts AS (
                SELECT payment_id, COUNT(*) AS attempts
                FROM payments.status_check
                GROUP BY payment_id
            ),
            payment_orders AS (
                SELECT po.id, po.customer_rut
                FROM payments.payment_order AS po
            )
            SELECT
                p.id,
                p.status::text,
                p.provider::text,
                p.token,
                p.created_at,
                p.amount_minor,
                p.provider_metadata,
                p.context,
                p.product_id,
                p.authorization_code,
                p.status_reason,
                COALESCE(pa.attempts, 0) AS attempts,
                po.id AS payment_order_id,
                po.customer_rut AS order_customer_rut,
                COALESCE(pc.notifica, false) AS should_notify_crm,
                pc.contrato AS contract_number,
                pc.cuotas AS quota_numbers,
                pc.tipo_pago AS payment_type,
                pdi.nombre_depositante AS deposit_name,
                pdi.rut_depositante AS deposit_rut,
                p.currency::text AS currency,
                paa.auxiliar_amount AS aux_amount_minor
            FROM payments.payment AS p
            LEFT JOIN payment_attempts pa ON pa.payment_id = p.id
            LEFT JOIN payment_orders po ON po.id = p.payment_order_id
            LEFT JOIN payments.payment_contract AS pc ON pc.payment_id = p.id
            LEFT JOIN payments.payment_deposit_info AS pdi ON pdi.payment_id = p.id
            LEFT JOIN payments.payment_aux_amount AS paa ON paa.payment_id = p.id
            WHERE p.status::text IN ('PENDING', 'TO_CONFIRM')
              AND p.token IS NOT NULL
              AND p.provider::text = ANY(%s::text[])
            ORDER BY p.created_at ASC
            LIMIT %s
            FOR UPDATE OF p SKIP LOCKED
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
                amount_minor=Decimal(row["amount_minor"]),
                provider_metadata=row.get("provider_metadata"),
                context=row.get("context"),
                product_id=row.get("product_id"),
                authorization_code=row.get("authorization_code"),
                status_reason=row.get("status_reason"),
                attempts=row.get("attempts", 0),
                payment_order_id=row.get("payment_order_id"),
                order_customer_rut=row.get("order_customer_rut"),
                should_notify_crm=row.get("should_notify_crm", False),
                contract_number=_normalize_contract_number(row.get("contract_number")),
                payment_type=_normalize_payment_type(row.get("payment_type")),
                quota_numbers=_normalize_quota_numbers(row.get("quota_numbers")),
                deposit_name=_clean_text(row.get("deposit_name")),
                deposit_rut=_clean_text(row.get("deposit_rut")),
                currency=_normalize_currency(row.get("currency")),
                aux_amount_minor=_decimal_or_none(row.get("aux_amount_minor")),
            )
        )
    return payments


def find_authorized_payments_without_crm(
    conn,
    *,
    limit: int = 100,
) -> List[Payment]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH payment_orders AS (
                SELECT po.id, po.customer_rut
                FROM payments.payment_order AS po
            )
            SELECT
                p.id,
                p.status::text,
                p.provider::text,
                p.token,
                p.created_at,
                p.amount_minor,
                p.provider_metadata,
                p.context,
                p.product_id,
                p.authorization_code,
                p.status_reason,
                po.id AS payment_order_id,
                po.customer_rut AS order_customer_rut,
                COALESCE(pc.notifica, false) AS should_notify_crm,
                pc.contrato AS contract_number,
                pc.cuotas AS quota_numbers,
                pc.tipo_pago AS payment_type,
                pdi.nombre_depositante AS deposit_name,
                pdi.rut_depositante AS deposit_rut,
                p.currency::text AS currency,
                paa.auxiliar_amount AS aux_amount_minor
            FROM payments.payment AS p
            LEFT JOIN payment_orders po ON po.id = p.payment_order_id
            LEFT JOIN payments.payment_contract AS pc ON pc.payment_id = p.id
            LEFT JOIN payments.payment_deposit_info AS pdi ON pdi.payment_id = p.id
            LEFT JOIN payments.payment_aux_amount AS paa ON paa.payment_id = p.id
            LEFT JOIN payments.crm_push_queue AS q
              ON q.payment_id = p.id
             AND q.operation = 'PAYMENT_APPROVED'
            WHERE p.status::text = 'AUTHORIZED'
              AND q.id IS NULL
            ORDER BY p.created_at ASC
            LIMIT %s
            """,
            (limit,),
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
                amount_minor=Decimal(row["amount_minor"]),
                provider_metadata=row.get("provider_metadata"),
                context=row.get("context"),
                product_id=row.get("product_id"),
                authorization_code=row.get("authorization_code"),
                status_reason=row.get("status_reason"),
                attempts=0,
                payment_order_id=row.get("payment_order_id"),
                order_customer_rut=row.get("order_customer_rut"),
                should_notify_crm=row.get("should_notify_crm", False),
                contract_number=_normalize_contract_number(row.get("contract_number")),
                payment_type=_normalize_payment_type(row.get("payment_type")),
                quota_numbers=_normalize_quota_numbers(row.get("quota_numbers")),
                deposit_name=_clean_text(row.get("deposit_name")),
                deposit_rut=_clean_text(row.get("deposit_rut")),
                currency=_normalize_currency(row.get("currency")),
                aux_amount_minor=_decimal_or_none(row.get("aux_amount_minor")),
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
    direction: str = "OUTBOUND",
    operation: str = "STATUS",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.provider_event_log (
                payment_id,
                provider,
                direction,
                operation,
                request_url,
                request_headers,
                request_body,
                response_status,
                response_headers,
                response_body,
                error_message,
                latency_ms
            )
            VALUES (%s, %s::payments.provider_type, %s::payments.direction_type, %s::payments.operation_type, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, %s)
            """,
            (
                payment_id,
                provider,
                direction,
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
    # Note: ABANDONED status does not have a dedicated timestamp column

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
        new_status="ABANDONED",
        status_reason="reconcile attempts exhausted",
    )


def get_payments_metrics(conn) -> PaymentsMetrics:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_payments,
                COUNT(*) FILTER (WHERE status::text = 'AUTHORIZED') AS authorized_payments,
                COALESCE(SUM(amount_minor), 0) AS total_amount_minor,
                MAX(created_at) AS last_payment_at
            FROM payments.payment
            """,
        )
        row = cur.fetchone() or {}

    total_payments = int(row.get("total_payments", 0) or 0)
    authorized_payments = int(row.get("authorized_payments", 0) or 0)
    total_amount_value = row.get("total_amount_minor")
    total_amount_minor = (
        Decimal("0") if total_amount_value in (None, 0) else Decimal(total_amount_value)
    )
    last_payment_at = row.get("last_payment_at")

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT context ->> 'currency' AS currency
            FROM payments.payment
            WHERE context IS NOT NULL
              AND context ->> 'currency' IS NOT NULL
            """,
        )
        currency_rows = cur.fetchall()

    currencies = [row["currency"] for row in currency_rows if row.get("currency")]
    total_amount_currency: str | None = None
    if currencies:
        total_amount_currency = currencies[0] if len(currencies) == 1 else "MIXED"

    return PaymentsMetrics(
        total_payments=total_payments,
        authorized_payments=authorized_payments,
        total_amount_minor=total_amount_minor,
        total_amount_currency=total_amount_currency,
        last_payment_at=last_payment_at,
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
                p.status::text,
                p.provider::text,
                p.token,
                p.created_at,
                p.amount_minor,
                p.provider_metadata,
                p.context,
                p.product_id,
                p.authorization_code,
                p.status_reason,
                0 AS attempts,
                po.id AS payment_order_id,
                po.customer_rut AS order_customer_rut,
                COALESCE(pc.notifica, false) AS should_notify_crm,
                pc.contrato AS contract_number,
                pc.cuotas AS quota_numbers,
                pc.tipo_pago AS payment_type,
                pdi.nombre_depositante AS deposit_name,
                pdi.rut_depositante AS deposit_rut,
                p.currency::text AS currency,
                paa.auxiliar_amount AS aux_amount_minor
            FROM payments.payment AS p
            LEFT JOIN payments.payment_order AS po
              ON po.id = p.payment_order_id
            LEFT JOIN payments.payment_contract AS pc
              ON pc.payment_id = p.id
            LEFT JOIN payments.payment_deposit_info AS pdi
              ON pdi.payment_id = p.id
            LEFT JOIN payments.payment_aux_amount AS paa
              ON paa.payment_id = p.id
            WHERE p.status::text = 'PENDING'
              AND p.created_at <= %s
            ORDER BY p.created_at ASC
            FOR UPDATE OF p SKIP LOCKED
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
            amount_minor=Decimal(row["amount_minor"]),
            provider_metadata=row.get("provider_metadata"),
            context=row.get("context"),
            product_id=row.get("product_id"),
            authorization_code=row.get("authorization_code"),
            status_reason=row.get("status_reason"),
            attempts=row.get("attempts", 0),
            payment_order_id=row.get("payment_order_id"),
            order_customer_rut=row.get("order_customer_rut"),
            should_notify_crm=row.get("should_notify_crm", False),
            contract_number=_normalize_contract_number(row.get("contract_number")),
            payment_type=_normalize_payment_type(row.get("payment_type")),
            quota_numbers=_normalize_quota_numbers(row.get("quota_numbers")),
            deposit_name=_clean_text(row.get("deposit_name")),
            deposit_rut=_clean_text(row.get("deposit_rut")),
            currency=_normalize_currency(row.get("currency")),
            aux_amount_minor=_decimal_or_none(row.get("aux_amount_minor")),
        )
        for row in rows
    ]


def log_service_runtime_event(
    conn,
    *,
    event_type: str,
    payload: dict | None = None,
    instance_id: str = "reconciler-1",
) -> None:
    import socket
    import os
    
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO payments.service_runtime_log (
                instance_id,
                host_name,
                process_id,
                event_type,
                payload
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (
                instance_id,
                socket.gethostname(),
                os.getpid(),
                event_type,
                psycopg2.extras.Json(payload) if payload is not None else None,
            ),
        )
