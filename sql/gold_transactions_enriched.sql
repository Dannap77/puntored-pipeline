-- Es la base sobre la que se construyen todos los KPI

CREATE OR REPLACE TABLE gold_transactions_enriched AS
SELECT
    d.detail_id,
    t.transaction_id,
    t.user_id,
    u.name  AS user_name,
    u.email AS user_email,
    t.amount,
    t.status,
    t.created_at  AS transaction_at,
    t.created_at::DATE AS transaction_date,
    d.payment_method,
    d.channel,
    d.processing_time_ms,
    t._ingested_at
FROM read_parquet('data/silver/transactions.parquet') t
INNER JOIN read_parquet('data/silver/users.parquet') u
    ON t.user_id = u.user_id
INNER JOIN read_parquet('data/silver/transaction_details.parquet') d
    ON t.transaction_id = d.transaction_id;
