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


def build_payload(payment: Payment, operation: str) -> Dict[str, Any]:
    context = payment.context or {}
    provider_metadata = payment.provider_metadata or {}

    rut = _extract_from_dict(context, "customer_rut") or _extract_from_dict(provider_metadata, "rut")
    name = (
        _extract_from_dict(context, "customer_name")
        or _extract_from_dict(provider_metadata, "name")
        or "Pago Ninja"
    )
    transaction_id = payment.authorization_code or payment.token
    amount_str = str(payment.amount_minor)
    product_id = payment.product_id if isinstance(payment.product_id, int) else None

    payload: Dict[str, Any] = {
        "rutDepositante": rut,
        "nombreDepositante": name,
        "paymentMethod": payment.provider,
        "transactionId": transaction_id,
        "monto": amount_str,
        "listContrato": [product_id] if product_id is not None else [],
        "listCuota": None,
        "operation": operation,
        "paymentId": payment.id,
    }
    return payload
