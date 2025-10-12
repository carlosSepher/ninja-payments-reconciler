from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _csv_to_int_list(value: str | List[int] | None, *, default: List[int]) -> List[int]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return [int(v) for v in value]
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _csv_to_str_list(value: str | List[str] | None, *, default: List[str]) -> List[str]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [item.strip() for item in value.split(",") if item.strip()]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=(".env",), env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="ninja-payments-reconciler", alias="APP_NAME")
    app_environment: str = Field(default="local", alias="APP_ENVIRONMENT")
    app_version: str | None = Field(default=None, alias="APP_VERSION")
    database_dsn: str = Field(default="postgresql://localhost/payments", alias="DATABASE_DSN")

    reconcile_enabled: bool = Field(default=True, alias="RECONCILE_ENABLED")
    reconcile_interval_seconds: int = Field(default=15, alias="RECONCILE_INTERVAL_SECONDS")
    reconcile_batch_size: int = Field(default=100, alias="RECONCILE_BATCH_SIZE")
    reconcile_attempt_offsets_raw: str | List[int] | None = Field(
        default=None, alias="RECONCILE_ATTEMPT_OFFSETS"
    )
    reconcile_polling_providers_raw: str | List[str] | None = Field(
        default=None, alias="RECONCILE_POLLING_PROVIDERS"
    )
    abandoned_timeout_minutes: int = Field(default=60, alias="ABANDONED_TIMEOUT_MINUTES")

    crm_enabled: bool = Field(default=True, alias="CRM_ENABLED")
    crm_base_url: str = Field(
        default="http://emprende-crm-prod-b0043c05a756b148.elb.us-east-1.amazonaws.com:8980/unify/inyeccion/contrato/v2",
        alias="CRM_BASE_URL",
    )
    crm_pagar_path: str = Field(default="/pagar", alias="CRM_PAGAR_PATH")
    crm_auth_bearer: str | None = Field(default=None, alias="CRM_AUTH_BEARER")
    crm_timeout_seconds: int = Field(default=10, alias="CRM_TIMEOUT_SECONDS")
    crm_retry_backoff_raw: str | List[int] | None = Field(
        default=None, alias="CRM_RETRY_BACKOFF"
    )
    crm_log_requests: bool = Field(default=True, alias="CRM_LOG_REQUESTS")

    swagger_basic_username: str = Field(default="ninja", alias="SWAGGER_BASIC_USERNAME")
    swagger_basic_password: str = Field(default="reconciler", alias="SWAGGER_BASIC_PASSWORD")

    health_auth_bearer: str | None = Field(default=None, alias="HEALTH_AUTH_BEARER")

    heartbeat_interval_seconds: int = Field(default=60, alias="HEARTBEAT_INTERVAL_SECONDS")

    # Provider credentials
    stripe_api_key: str | None = Field(default=None, alias="STRIPE_API_KEY")
    stripe_api_base: str = Field(default="https://api.stripe.com", alias="STRIPE_API_BASE")
    
    paypal_client_id: str | None = Field(default=None, alias="PAYPAL_CLIENT_ID")
    paypal_client_secret: str | None = Field(default=None, alias="PAYPAL_CLIENT_SECRET")
    paypal_base_url: str = Field(default="https://api-m.sandbox.paypal.com", alias="PAYPAL_BASE_URL")
    
    webpay_status_url_template: str = Field(
        default="https://webpay3gint.transbank.cl/rswebpaytransaction/api/webpay/v1.2/transactions/{token}",
        alias="WEBPAY_STATUS_URL_TEMPLATE"
    )
    webpay_api_key_id: str | None = Field(default=None, alias="WEBPAY_API_KEY_ID")
    webpay_api_key_secret: str | None = Field(default=None, alias="WEBPAY_API_KEY_SECRET")
    webpay_commerce_code: str | None = Field(default=None, alias="WEBPAY_COMMERCE_CODE")

    @property
    def reconcile_attempt_offsets(self) -> List[int]:
        return _csv_to_int_list(
            self.reconcile_attempt_offsets_raw, default=[60, 180, 900, 1800]
        )

    @property
    def reconcile_polling_providers(self) -> List[str]:
        return _csv_to_str_list(
            self.reconcile_polling_providers_raw, default=["webpay", "stripe", "paypal"]
        )

    @property
    def crm_retry_backoff(self) -> List[int]:
        return _csv_to_int_list(self.crm_retry_backoff_raw, default=[60, 300, 1800])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
