from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from ..repositories.payments_repo import Payment

_AMOUNT_KEYS = (
    "amount_minor",
    "amountMinor",
    "amount",
    "total_amount",
    "totalAmount",
    "total",
)


def _extract_from_dict(data: Any, *keys: str) -> Any:
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key in data:
            return data[key]
    return None


def _sanitize_rut(value: Any) -> str | None:
    if value is None:
        return None
    rut = str(value)
    cleaned = rut.replace(".", "").replace("-", "").strip()
    return cleaned or None


def _truncate_amount_to_str(amount: Any) -> str:
    if isinstance(amount, Decimal):
        truncated = int(amount)
    else:
        try:
            truncated = int(Decimal(str(amount)))
        except (InvalidOperation, ValueError):
            truncated = 0
    return str(truncated)


def _is_non_zero_numeric(value: Any) -> bool:
    if value is None:
        return False
    try:
        return Decimal(str(value)) != 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _find_amount_in_payload(data: Any) -> Any:
    if isinstance(data, dict):
        for key in _AMOUNT_KEYS:
            if key in data and _is_non_zero_numeric(data[key]):
                return data[key]
        for value in data.values():
            found = _find_amount_in_payload(value)
            if found is not None:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_amount_in_payload(item)
            if found is not None:
                return found
    return None


def _resolve_amount(payment: Payment) -> Any:
    currency = (payment.currency or "CLP").upper()
    if currency != "CLP" and payment.aux_amount_minor is not None:
        return payment.aux_amount_minor
    if _is_non_zero_numeric(payment.amount_minor):
        return payment.amount_minor

    for source in (payment.context, payment.provider_metadata):
        found = _find_amount_in_payload(source)
        if found is not None:
            return found
    return payment.amount_minor


def can_notify_crm(payment: Payment) -> bool:
    if not payment.should_notify_crm:
        return False
    currency = (payment.currency or "CLP").upper()
    if currency != "CLP" and payment.aux_amount_minor is None:
        return False
    if payment.payment_type == "cuota":
        return bool(payment.quota_numbers)
    return payment.contract_number is not None


def build_payload(payment: Payment, operation: str) -> Dict[str, Any]:
    context = payment.context or {}
    provider_metadata = payment.provider_metadata or {}

    rut = payment.deposit_rut or payment.order_customer_rut
    if rut is None:
        rut = _extract_from_dict(context, "customer_rut") or _extract_from_dict(
            provider_metadata, "rut"
        )
    rut = _sanitize_rut(rut)

    name = payment.deposit_name or _extract_from_dict(context, "customer_name")
    if name is None:
        name = _extract_from_dict(provider_metadata, "name") or payment.provider
    transaction_id = (
        payment.payment_order_id
        or payment.authorization_code
        or payment.token
        or payment.id
    )
    amount_value = _resolve_amount(payment)
    amount_str = _truncate_amount_to_str(amount_value)
    is_quota_payment = payment.payment_type == "cuota"
    contract_list = None
    quota_list = None
    if is_quota_payment:
        if payment.quota_numbers:
            quota_list = list(payment.quota_numbers)
    else:
        if payment.contract_number is not None:
            contract_list = [payment.contract_number]

    payload: Dict[str, Any] = {
        "rutDepositante": rut,
        "nombreDepositante": name,
        "paymentMethod": payment.provider,
        "transactionId": str(transaction_id) if transaction_id is not None else None,
        "monto": amount_str,
        "listContrato": contract_list,
        "listCuota": quota_list,
    }
    return payload
