-- Crear el schema si no existe
CREATE SCHEMA IF NOT EXISTS payments;

-- Tabla principal de pagos
CREATE TABLE IF NOT EXISTS payments.payment (
    id SERIAL PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    token VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    amount_minor INTEGER NOT NULL,
    provider_metadata JSONB,
    context JSONB,
    product_id INTEGER,
    authorization_code VARCHAR(255),
    status_reason TEXT,
    first_authorized_at TIMESTAMP,
    failed_at TIMESTAMP,
    canceled_at TIMESTAMP,
    refunded_at TIMESTAMP,
    abandoned_at TIMESTAMP
);

-- Índices para la tabla payment
CREATE INDEX IF NOT EXISTS idx_payment_status ON payments.payment(status);
CREATE INDEX IF NOT EXISTS idx_payment_provider ON payments.payment(provider);
CREATE INDEX IF NOT EXISTS idx_payment_created_at ON payments.payment(created_at);
CREATE INDEX IF NOT EXISTS idx_payment_token ON payments.payment(token) WHERE token IS NOT NULL;

-- Tabla de verificación de estados
CREATE TABLE IF NOT EXISTS payments.status_check (
    id SERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL REFERENCES payments.payment(id),
    provider VARCHAR(50) NOT NULL,
    success BOOLEAN NOT NULL,
    provider_status VARCHAR(100),
    mapped_status VARCHAR(50),
    response_code INTEGER,
    raw_payload JSONB,
    error_message TEXT,
    requested_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices para status_check
CREATE INDEX IF NOT EXISTS idx_status_check_payment_id ON payments.status_check(payment_id);
CREATE INDEX IF NOT EXISTS idx_status_check_requested_at ON payments.status_check(requested_at);

-- Tabla de log de eventos del proveedor
CREATE TABLE IF NOT EXISTS payments.provider_event_log (
    id SERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL REFERENCES payments.payment(id),
    provider VARCHAR(50) NOT NULL,
    request_url TEXT NOT NULL,
    request_headers JSONB NOT NULL,
    request_body JSONB,
    response_status INTEGER,
    response_headers JSONB,
    response_body JSONB,
    error_message TEXT,
    latency_ms INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices para provider_event_log
CREATE INDEX IF NOT EXISTS idx_provider_event_log_payment_id ON payments.provider_event_log(payment_id);
CREATE INDEX IF NOT EXISTS idx_provider_event_log_created_at ON payments.provider_event_log(created_at);

-- Tabla de cola de envío al CRM
CREATE TABLE IF NOT EXISTS payments.crm_push_queue (
    id SERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL REFERENCES payments.payment(id),
    operation VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP,
    last_attempt_at TIMESTAMP,
    response_code INTEGER,
    crm_id VARCHAR(255),
    last_error TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_crm_push_queue_payment_operation UNIQUE (payment_id, operation)
);

-- Índices para crm_push_queue
CREATE INDEX IF NOT EXISTS idx_crm_push_queue_status ON payments.crm_push_queue(status);
CREATE INDEX IF NOT EXISTS idx_crm_push_queue_next_attempt ON payments.crm_push_queue(next_attempt_at) WHERE next_attempt_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_crm_push_queue_created_at ON payments.crm_push_queue(created_at);

-- Tabla de log de eventos del CRM
CREATE TABLE IF NOT EXISTS payments.crm_event_log (
    id SERIAL PRIMARY KEY,
    payment_id INTEGER NOT NULL REFERENCES payments.payment(id),
    operation VARCHAR(50) NOT NULL,
    request_url TEXT NOT NULL,
    request_headers JSONB NOT NULL,
    request_body JSONB,
    response_status INTEGER,
    response_headers JSONB,
    response_body JSONB,
    error_message TEXT,
    latency_ms INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índices para crm_event_log
CREATE INDEX IF NOT EXISTS idx_crm_event_log_payment_id ON payments.crm_event_log(payment_id);
CREATE INDEX IF NOT EXISTS idx_crm_event_log_created_at ON payments.crm_event_log(created_at);

-- Tabla de log de eventos del servicio
CREATE TABLE IF NOT EXISTS payments.service_runtime_log (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Índice para service_runtime_log
CREATE INDEX IF NOT EXISTS idx_service_runtime_log_event_type ON payments.service_runtime_log(event_type);
CREATE INDEX IF NOT EXISTS idx_service_runtime_log_created_at ON payments.service_runtime_log(created_at);

-- Mensaje de confirmación
DO $$
BEGIN
    RAISE NOTICE 'Database schema created successfully!';
END $$;
