-- Fact table denormalizada: 1 fila por detalle, con info de transacción y usuario.
-- Es la base sobre la que se construyen todos los KPIs.

CREATE OR REPLACE TABLE gold_transactions_enriched AS
SELECT
    d.detail_id,
    t.transaction_id,
    t.user_id,
    u.name              AS user_name,
    u.email             AS user_email,
    t.amount,
    t.status,
    t.created_at         AS transaction_at,
    t.created_at::DATE   AS transaction_date,
    d.payment_method,
    d.channel,
    d.processing_time_ms,
    t._pipeline_run_id
FROM read_parquet('data/silver/transactions.parquet') t
INNER JOIN read_parquet('data/silver/users.parquet') u
    ON t.user_id = u.user_id
INNER JOIN read_parquet('data/silver/transaction_details.parquet') d
    ON t.transaction_id = d.transaction_id;
