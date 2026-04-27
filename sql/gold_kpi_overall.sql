-- KPIs globales del negocio.

CREATE OR REPLACE TABLE gold_kpi_overall AS
SELECT
    COUNT(DISTINCT transaction_id)                                          AS total_transactions,
    COUNT(DISTINCT user_id)                                                 AS total_users,
    SUM(amount)                                                             AS total_amount,
    AVG(amount)                                                             AS avg_amount,
    SUM(CASE WHEN status = 'success' THEN amount ELSE 0 END)                AS total_amount_success,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id)                                    AS success_rate,
    AVG(processing_time_ms)                                                 AS avg_processing_time_ms,
    MIN(transaction_at)                                                     AS first_transaction_at,
    MAX(transaction_at)                                                     AS last_transaction_at
FROM gold_transactions_enriched;
