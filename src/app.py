from __future__ import annotations

import asyncio
import logging
import os
import secrets
import socket
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import JSONResponse
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)

from .db import create_database
from .integrations.crm_client import CRMClient
from .integrations.providers import paypal, stripe, webpay
from .integrations.providers.base import ProviderClient
from .loops.crm_sender import CrmSender
from .loops.psp_poller import PspPoller
from .repositories import payments_repo
from .settings import get_settings

LOGGER = logging.getLogger(__name__)

_docs_basic_scheme = HTTPBasic()
_bearer_scheme = HTTPBearer(auto_error=False)


def create_app() -> FastAPI:
    # Configurar logging con más detalle
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    settings = get_settings()
    LOGGER.info(f"Starting {settings.app_name}")
    LOGGER.info(f"Reconciliation enabled: {settings.reconcile_enabled}")
    LOGGER.info(f"CRM integration enabled: {settings.crm_enabled}")
    LOGGER.info(f"Polling providers: {settings.reconcile_polling_providers}")

    provider_clients: Dict[str, ProviderClient] = {
        "webpay": webpay.WebpayProvider(
            status_url_template=settings.webpay_status_url_template,
            api_key_id=settings.webpay_api_key_id,
            api_key_secret=settings.webpay_api_key_secret,
            commerce_code=settings.webpay_commerce_code,
        ),
        "stripe": stripe.StripeProvider(
            api_key=settings.stripe_api_key,
            api_base=settings.stripe_api_base,
        ),
        "paypal": paypal.PayPalProvider(
            client_id=settings.paypal_client_id,
            client_secret=settings.paypal_client_secret,
            base_url=settings.paypal_base_url,
        ),
    }

    providers = {
        name: client
        for name, client in provider_clients.items()
        if name in settings.reconcile_polling_providers
    }
    LOGGER.info(f"Configured providers: {list(providers.keys())}")

    crm_client = CRMClient(
        base_url=settings.crm_base_url,
        pagar_path=settings.crm_pagar_path,
        bearer_token=settings.crm_auth_bearer,
        timeout_seconds=settings.crm_timeout_seconds,
        log_requests=settings.crm_log_requests,
    )
    LOGGER.info(f"CRM endpoint: {crm_client.endpoint}")

    app = FastAPI(title=settings.app_name, docs_url=None, redoc_url=None, openapi_url=None)

    app.state.settings = settings
    app.state.providers = providers
    app.state.crm_client = crm_client
    app.state.database = None
    app.state.poller: PspPoller | None = None
    app.state.sender: CrmSender | None = None
    app.state.background_tasks: list[asyncio.Task] = []
    app.state.started_at: datetime | None = datetime.now(timezone.utc)

    @app.on_event("startup")
    async def on_startup() -> None:
        LOGGER.info("=" * 60)
        LOGGER.info("SERVICE STARTUP")
        LOGGER.info("=" * 60)
        
        database = create_database()
        app.state.database = database
        LOGGER.info("Database connection established")
        
        poller = PspPoller(db=database, settings=settings, providers=providers)
        sender = CrmSender(db=database, settings=settings, client=crm_client)
        app.state.poller = poller
        app.state.sender = sender
        
        with database.connection() as conn:
            payments_repo.log_service_runtime_event(
                conn,
                event_type="STARTUP",
                payload={"app": settings.app_name},
            )
        
        app.state.background_tasks.append(asyncio.create_task(poller.run(), name="psp_poller"))
        app.state.background_tasks.append(asyncio.create_task(sender.run(), name="crm_sender"))
        LOGGER.info("Background tasks started: psp_poller, crm_sender")
        LOGGER.info("=" * 60)

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        LOGGER.info("=" * 60)
        LOGGER.info("SERVICE SHUTDOWN")
        LOGGER.info("=" * 60)
        
        for task in app.state.background_tasks:
            task.cancel()
            LOGGER.info(f"Cancelled task: {task.get_name()}")
        
        for task in app.state.background_tasks:
            with suppress(asyncio.CancelledError):
                await task
        
        database = app.state.database
        if database is not None:
            with database.connection() as conn:
                payments_repo.log_service_runtime_event(
                    conn,
                    event_type="SHUTDOWN",
                    payload={"app": settings.app_name},
                )
            database.close()
            LOGGER.info("Database connection closed")
        
        LOGGER.info("=" * 60)
        LOGGER.info("=" * 60)

    def _validate_swagger_credentials(
        credentials: HTTPBasicCredentials = Depends(_docs_basic_scheme),
    ) -> HTTPBasicCredentials:
        expected_user = settings.swagger_basic_username
        expected_password = settings.swagger_basic_password
        is_user_valid = secrets.compare_digest(credentials.username, expected_user)
        is_password_valid = secrets.compare_digest(credentials.password, expected_password)
        if not (is_user_valid and is_password_valid):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect basic auth credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials

    def _verify_health_auth(
        credentials: HTTPAuthorizationCredentials | None,
    ) -> None:
        expected_token = settings.health_auth_bearer
        if not expected_token:
            return
        if credentials is None or credentials.scheme.lower() != "bearer":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        provided_token = credentials.credentials.strip()
        if provided_token != expected_token:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/health/metrics")
    async def health_metrics(
        credentials: HTTPAuthorizationCredentials = Security(_bearer_scheme),
    ) -> dict[str, Any]:
        _verify_health_auth(credentials)

        now = datetime.now(timezone.utc)
        started_at: datetime | None = app.state.started_at
        uptime_seconds = int((now - started_at).total_seconds()) if started_at else 0

        database_summary: dict[str, Any] = {"connected": False, "schema": None}
        payments_summary: dict[str, Any] = {
            "total_payments": 0,
            "authorized_payments": 0,
            "total_amount_minor": 0,
            "total_amount_currency": None,
            "last_payment_at": None,
        }
        status_label = "ok"

        database = app.state.database
        if database is not None:
            schema_name: str | None = None
            try:
                with database.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT current_schema()")
                        schema_row = cur.fetchone()
                        if schema_row:
                            schema_name = schema_row[0]
                    metrics = payments_repo.get_payments_metrics(conn)
                database_summary = {"connected": True, "schema": schema_name}
                payments_summary = metrics.to_dict()
            except Exception as exc:  # pragma: no cover - defensive
                status_label = "degraded"
                LOGGER.exception("Health metrics probe failed: %s", exc)
        else:
            status_label = "degraded"

        response: dict[str, Any] = {
            "status": status_label,
            "timestamp": now.isoformat(),
            "uptime_seconds": uptime_seconds,
            "service": {
                "default_provider": settings.reconcile_polling_providers[0]
                if settings.reconcile_polling_providers
                else None,
                "environment": settings.app_environment,
                "version": settings.app_version,
                "host": socket.gethostname(),
                "pid": os.getpid(),
            },
            "database": database_summary,
            "payments": payments_summary,
        }

        return response

    @app.get("/openapi.json", include_in_schema=False)
    async def custom_openapi(
        _: HTTPBasicCredentials = Depends(_validate_swagger_credentials),
    ) -> JSONResponse:
        return JSONResponse(app.openapi())

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger_ui(
        _: HTTPBasicCredentials = Depends(_validate_swagger_credentials),
    ):
        return get_swagger_ui_html(
            openapi_url="/openapi.json",
            title=f"{settings.app_name} - Swagger UI",
            swagger_ui_parameters={"persistAuthorization": True},
        )

    @app.get("/docs/oauth2-redirect", include_in_schema=False)
    async def swagger_ui_redirect(
        _: HTTPBasicCredentials = Depends(_validate_swagger_credentials),
    ):
        return get_swagger_ui_oauth2_redirect_html()

    return app


app = create_app()
