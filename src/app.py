from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import Dict

from fastapi import FastAPI

from .db import create_database
from .integrations.crm_client import CRMClient
from .integrations.providers import paypal, stripe, webpay
from .integrations.providers.base import ProviderClient
from .loops.crm_sender import CrmSender
from .loops.psp_poller import PspPoller
from .repositories import payments_repo
from .settings import get_settings

LOGGER = logging.getLogger(__name__)


def create_app() -> FastAPI:
    logging.basicConfig(level=logging.INFO)
    settings = get_settings()

    provider_clients: Dict[str, ProviderClient] = {
        "webpay": webpay.create(),
        "stripe": stripe.create(),
        "paypal": paypal.create(),
    }

    providers = {
        name: client
        for name, client in provider_clients.items()
        if name in settings.reconcile_polling_providers
    }

    crm_client = CRMClient(
        base_url=settings.crm_base_url,
        pagar_path=settings.crm_pagar_path,
        bearer_token=settings.crm_auth_bearer,
        timeout_seconds=settings.crm_timeout_seconds,
        log_requests=settings.crm_log_requests,
    )

    app = FastAPI(title=settings.app_name)

    app.state.settings = settings
    app.state.providers = providers
    app.state.crm_client = crm_client
    app.state.database = None
    app.state.poller: PspPoller | None = None
    app.state.sender: CrmSender | None = None
    app.state.background_tasks: list[asyncio.Task] = []

    @app.on_event("startup")
    async def on_startup() -> None:
        LOGGER.info("Starting service")
        database = create_database()
        app.state.database = database
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

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        LOGGER.info("Shutting down service")
        for task in app.state.background_tasks:
            task.cancel()
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

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
