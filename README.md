# Ninja Payments Reconciler

Standalone reconciliation service for PSP polling and CRM integration.

## Prerequisites

- Python 3.10 or higher
- PostgreSQL database
- Payment provider credentials (Webpay, Stripe, PayPal)

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd ninja-payments-reconciler
```

### 2. Create and activate virtual environment

```bash
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or
venv\Scripts\activate  # On Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

For development dependencies:

```bash
pip install -r requirements-dev.txt
```

### 4. Configure environment variables

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` with your actual credentials and configuration.

### 5. Initialize the database

Run the SQL scripts to create the necessary tables:

```bash
psql -h localhost -U ninja -d ninja_payments -f scripts/create_tables.sql
```

## Running the Service

Start the reconciliation service:

```bash
python -m src.app
```

Or with uvicorn:

```bash
uvicorn src.app:app --reload
```

## Project Structure

```
src/
├── app.py                  # Main application entry point
├── db.py                   # Database connection management
├── settings.py             # Configuration settings
├── integrations/
│   ├── crm_client.py       # CRM HTTP client
│   └── providers/          # Payment provider integrations
│       ├── base.py
│       ├── paypal.py
│       ├── stripe.py
│       └── webpay.py
├── loops/
│   ├── crm_sender.py       # CRM synchronization loop
│   └── psp_poller.py       # Payment provider polling loop
├── repositories/
│   ├── crm_repo.py         # CRM queue database operations
│   └── payments_repo.py    # Payment database operations
└── services/
    └── crm_payloads.py     # CRM payload builders
```

## Development

### Running tests

```bash
pytest
```

### Code formatting and linting

```bash
ruff check .
ruff format .
```

## Configuration

Key environment variables:

- `DATABASE_DSN`: PostgreSQL connection string
- `RECONCILE_ENABLED`: Enable/disable reconciliation polling
- `RECONCILE_POLLING_PROVIDERS`: Comma-separated list of providers to poll
- `CRM_ENABLED`: Enable/disable CRM integration
- `CRM_BASE_URL`: CRM API base URL
- `CRM_AUTH_BEARER`: CRM authentication token

See `.env.example` for complete configuration options.

## License

Proprietary
