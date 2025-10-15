# Ninja Payments Reconciler

Standalone FastAPI service that keeps your payment records in sync with external PSPs (Stripe, Webpay, PayPal) and forwards state changes to an internal CRM. The application runs two background loops — one for PSP reconciliation and another for CRM delivery — while exposing a small HTTP surface for health checks and metrics.

## Key Responsibilities

- Poll PSPs for status updates on `PENDING` / `TO_CONFIRM` payments and update the local ledger.
- Persist full provider call logs and status check history for auditability.
- Queue and push CRM notifications when payments transition to final states.
- Provide operational visibility with ready-to-use health and metrics endpoints.

## Architecture Overview

```mermaid
flowchart LR
    subgraph Service
        FastAPI[FastAPI app]
        Poller[PspPoller loop]
        Sender[CrmSender loop]
    end

    FastAPI -->|/health, /api/v1/health/metrics| Ops[Operators & Monitoring]
    Poller -->|status() calls| Providers[(PSP APIs)]
    Poller -->|select / update| DB[(PostgreSQL)]
    Sender -->|enqueue CRM payload| DB
    Sender -->|send CRM request| CRM((CRM REST API))
    CRM -->|response| Sender
    FastAPI -. access .-> DB

    classDef loop fill:#f3f7ff,stroke:#1a56db,stroke-width:1px;
    classDef external fill:#fff5f5,stroke:#e53e3e,stroke-width:1px;
    class Poller,Sender loop;
    class Providers,CRM external;
```

### Background Loops

```mermaid
sequenceDiagram
    participant Poller as PSP Poller
    participant DB as Postgres
    participant PSP as PSP API

    loop every reconcile_interval_seconds
        Poller->>DB: select_payments_for_reconciliation()
        alt payments available
            Poller->>PSP: GET status(token)
            PSP-->>Poller: JSON payload
            Poller->>DB: record_provider_event()
            Poller->>DB: record_status_check()
            opt mapped status differs
                Poller->>DB: update_payment_status()
                Poller->>DB: enqueue_crm_operation()
            end
        else none ready
            Poller->>Poller: sleep
        end
    end
```

```mermaid
sequenceDiagram
    participant Sender as CRM Sender
    participant DB as Postgres
    participant CRM as CRM API

    loop every reconcile_interval_seconds
        Sender->>DB: reactivate_failed_items()
        Sender->>DB: fetch_pending_crm_items()
        alt item available
            Sender->>CRM: POST payload
            CRM-->>Sender: HTTP response
            alt success
                Sender->>DB: update_crm_item_success()
            else failure
                Sender->>DB: update_crm_item_failure()
            end
            Sender->>DB: record_crm_event()
        else queue empty
            Sender->>Sender: sleep
        end
    end
```

### Data Model (simplified)

```mermaid
erDiagram
    payment ||--o{ status_check : "history"
    payment ||--o{ provider_event_log : "request log"
    payment ||--o{ crm_push_queue : "pending CRM pushes"
    payment ||--o{ crm_event_log : "CRM call log"

    payment {
        integer id PK
        varchar status
        varchar provider
        varchar token
        numeric(18,2) amount_minor
        jsonb context
        timestamp created_at
        timestamp updated_at
    }

    status_check {
        integer id PK
        integer payment_id FK
        varchar provider_status
        varchar mapped_status
        integer response_code
        jsonb raw_payload
        timestamp requested_at
    }

    provider_event_log {
        integer id PK
        integer payment_id FK
        text request_url
        jsonb request_headers
        jsonb response_body
        text error_message
        integer latency_ms
        timestamp created_at
    }

    crm_push_queue {
        integer id PK
        integer payment_id FK
        varchar status
        integer attempts
        jsonb payload
        timestamp next_attempt_at
    }

    crm_event_log {
        integer id PK
        integer payment_id FK
        jsonb request_headers
        jsonb response_body
        text error_message
        timestamp created_at
    }
```

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL 13+
- PSP credentials for the providers you intend to reconcile (Stripe, Webpay, PayPal)

### Bootstrap

```bash
git clone <repository-url>
cd ninja-payments-reconciler
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Swagger UI Access

- Navigate to `http://<host>:8300/docs` and authenticate with basic credentials `ninja / reconciler`.
- Use the `Authorize` button within Swagger to provide the bearer token required by endpoints such as `/api/v1/health/metrics`.

```bash
# Prepare database schema
psql -h <host> -U <user> -d <db> -f scripts/create_tables.sql

# Copy & configure environment
cp .env.example .env
```

Edit `.env` with your credentials. A minimal configuration looks like:

```dotenv
APP_NAME=ninja-payments-reconciler
APP_ENVIRONMENT=local
APP_VERSION=1
APP_PORT=8300
DATABASE_DSN=postgresql://ninja:ninjapass@localhost:5442/ninja_payments
RECONCILE_POLLING_PROVIDERS=webpay,stripe,paypal
STRIPE_API_KEY=sk_test_...
PAYPAL_CLIENT_ID=...
PAYPAL_CLIENT_SECRET=...
WEBPAY_API_KEY_ID=...
WEBPAY_API_KEY_SECRET=...
HEALTH_AUTH_BEARER=replace-me
```

### Running the Service

```bash
./run.sh           # uses APP_PORT / PORT (defaults to 8001)
# or
uvicorn src.app:app --host 0.0.0.0 --port 8300 --log-level info
```

Both background loops spawn automatically on startup. Logs are written to stdout.

### Docker Deployment

- Ajusta `REPO_URL` dentro de `scripts/deploy_ec2.sh` o expórtalo antes de ejecutar el script en la EC2.
- Verifica que Docker esté instalado y que exista la red `pg16_default` con el contenedor de PostgreSQL accesible como `pg16`.
- Ejecuta `sudo bash scripts/deploy_ec2.sh main` para clonar/pullear el repo, regenerar `.env`, construir la imagen y levantar el contenedor en el puerto `8200`.
- Nginx puede enrutar `/reconciler/` a `http://127.0.0.1:8200/` según la configuración provista.
- Revisa logs con `docker logs -f ninja-payments-reconciler` y status con `docker ps`.

### Nginx

- Copia `deploy/nginx.conf` a `/etc/nginx/sites-available/ninja-payments-reconciler.conf` y crea el enlace simbólico en `sites-enabled`.
- Valida sintaxis con `sudo nginx -t` y recarga con `sudo systemctl reload nginx`.
- El bloque `/reconciler/` direcciona al contenedor FastAPI en `127.0.0.1:8200`.

### Stopping the Service

- Send `CTRL+C` or terminate the process. The shutdown handler cancels background tasks and records a `SHUTDOWN` entry in `payments.service_runtime_log`.

## Configuration Reference

| Variable | Default | Description |
| --- | --- | --- |
| `APP_NAME` | `ninja-payments-reconciler` | Service identifier used in logs. |
| `APP_ENVIRONMENT` | `local` | Free-form environment label exposed in metrics. |
| `APP_VERSION` | `None` | Optional semantic version string. |
| `APP_PORT` | `8001` | Preferred HTTP port (used by `run.sh`). |
| `DATABASE_DSN` | `postgresql://localhost/payments` | PostgreSQL connection string. |
| `RECONCILE_ENABLED` | `true` | Toggle the PSP poller loop. |
| `RECONCILE_INTERVAL_SECONDS` | `15` | Sleep time between poller iterations. |
| `RECONCILE_ATTEMPT_OFFSETS` | `60,180,900,1800` | Delay (in seconds) before each retry attempt per payment. |
| `RECONCILE_BATCH_SIZE` | `100` | Max number of payments processed per poller cycle. |
| `RECONCILE_POLLING_PROVIDERS` | `webpay,stripe,paypal` | Ordered list of providers to reconcile. |
| `ABANDONED_TIMEOUT_MINUTES` | `60` | Age threshold to mark `PENDING` payments as `ABANDONED`. |
| `CRM_ENABLED` | `true` | Toggle the CRM sender loop. |
| `CRM_BASE_URL` / `CRM_PAGAR_PATH` | — | CRM endpoint configuration. |
| `CRM_AUTH_BEARER` | `None` | Optional bearer token for CRM calls. |
| `CRM_RETRY_BACKOFF` | `60,300,1800` | Backoff schedule (seconds) for CRM retries. |
| `HEALTH_AUTH_BEARER` | `None` | When set, `/api/v1/health/metrics` requires `Authorization: Bearer <token>`. |
| `STRIPE_API_KEY` / `STRIPE_API_BASE` | — | Stripe credentials and base URL. |
| `PAYPAL_CLIENT_ID` / `PAYPAL_CLIENT_SECRET` / `PAYPAL_BASE_URL` | — | PayPal configuration (OAuth + Orders API). |
| `WEBPAY_*` | — | Webpay credentials and status endpoint template. |
| `HEARTBEAT_INTERVAL_SECONDS` | `60` | Interval to emit heartbeat events to `service_runtime_log`. |

## Health & Metrics Endpoints

### Liveness

```bash
curl http://127.0.0.1:8300/health
# {"status": "ok"}
```

### Detailed Metrics

```bash
curl \
  -H "Authorization: Bearer $HEALTH_AUTH_BEARER" \
  http://127.0.0.1:8300/api/v1/health/metrics
```

Response schema:

```json
{
  "status": "ok | degraded",
  "timestamp": "RFC3339",
  "uptime_seconds": 123,
  "service": {
    "default_provider": "stripe",
    "environment": "local",
    "version": "1",
    "host": "hostname",
    "pid": 1234
  },
  "database": {
    "connected": true,
    "schema": "payments"
  },
  "payments": {
    "total_payments": 26,
    "authorized_payments": 11,
    "total_amount_minor": 316597.50,
    "total_amount_currency": "MIXED",
    "last_payment_at": "2025-10-06T01:39:13.263489+00:00"
  }
}
```

The endpoint downgrades `status` to `degraded` when the database is unavailable or metrics collection fails.

## Payment Reconciliation Logic

- Payments eligible for reconciliation are loaded with a `FOR UPDATE SKIP LOCKED` query to safely distribute work across multiple instances.
- The retry index is derived from the number of entries in `payments.status_check` for the payment; once all offsets are exhausted, the payment transitions to `ABANDONED` with reason `reconcile attempts exhausted`.
- Stripe integration transparently handles checkout session tokens (`cs_...`) and payment intent client secrets by normalizing the identifier before hitting `/v1/payment_intents` or `/v1/checkout/sessions` with the necessary `expand[]` parameter.
- Every provider call is persisted to `payments.provider_event_log`, including masked headers and timing, to aid debugging.

## CRM Delivery Logic

- Items in `payments.crm_push_queue` may be in one of three states: `PENDING`, `FAILED`, or `SENT`.
- Failed items are automatically reactivated once their `next_attempt_at` is past due, with retries scheduled according to `CRM_RETRY_BACKOFF`.
- Each HTTP interaction against the CRM is logged to `payments.crm_event_log` with full request/response context.

## Development & Testing

```bash
# Type checking / linting (if configured in requirements-dev)
ruff check .
ruff format .

# Run unit tests (pytest config not provided but reserved)
pytest

# Quick syntax check without cache permissions issues
PYTHONPYCACHEPREFIX=.pyc_cache python3 -m compileall src
```

## Operational Tips

- **Adjusting retry cadence**: set `RECONCILE_ATTEMPT_OFFSETS=0,60,180,900` to perform the first PSP status check immediately after creation.
- **Inspecting queue state**: query `payments.crm_push_queue` filtering by `status` to see pending or failed CRM pushes.
- **Manual CRM log entry**: you can insert synthetic rows into `payments.crm_event_log` to document skipped pushes.
- **Scaling horizontally**: multiple instances can run concurrently; the DB `SKIP LOCKED` queries ensure work is partitioned without conflicts.

## License

Proprietary. All rights reserved.
