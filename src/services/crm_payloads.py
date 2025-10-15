from __future__ import annotations

from typing import Any, Dict

from ..repositories.payments_repo import Payment


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


def build_payload(payment: Payment, operation: str) -> Dict[str, Any]:
    context = payment.context or {}
    provider_metadata = payment.provider_metadata or {}

    rut = (
        payment.order_customer_rut
        or _extract_from_dict(context, "customer_rut")
        or _extract_from_dict(provider_metadata, "rut")
    )
    rut = _sanitize_rut(rut)

    name = (
        _extract_from_dict(context, "customer_name")
        or _extract_from_dict(provider_metadata, "name")
        or payment.provider
    )
    transaction_id = (
        payment.payment_order_id
        or payment.authorization_code
        or payment.token
        or payment.id
    )
    amount_value = float(payment.amount_minor)
    product_id = payment.product_id if isinstance(payment.product_id, int) else None

    payload: Dict[str, Any] = {
        "rutDepositante": rut,
        "nombreDepositante": name,
        "paymentMethod": payment.provider,
        "transactionId": str(transaction_id) if transaction_id is not None else None,
        "monto": amount_value,
        "listContrato": [product_id] if product_id is not None else [],
        "listCuota": None,
    }
    return payload
