banned machine-doll

this queue system only tapped to postgresql.

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

    CONSTRAINT notify_queue_status_check
        CHECK (status IN ('pending', 'processing', 'success', 'failed'))
);
```
