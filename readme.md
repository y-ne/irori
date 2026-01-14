## banned machine-doll

Queue system implementation which only tapped to postgres.

NOTE: please do remember that currently Irori is just a PET. if there's any suggestion feel free to open a PR.

irori.sql

```sql
CREATE TABLE IF NOT EXISTS irori (
    id BIGSERIAL PRIMARY KEY,
    payload JSONB,

    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    errors JSONB[] DEFAULT '{}',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT irori_queue_status_check
        CHECK (status IN ('pending', 'processing', 'success', 'failed'))
);
```

```sql
CREATE INDEX IF NOT EXISTS idx_irori_pending ON irori (next_retry_at)
WHERE status = 'pending';
```
