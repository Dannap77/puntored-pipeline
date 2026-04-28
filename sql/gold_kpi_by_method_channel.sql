-- Cruce método de pago × canal.

CREATE OR REPLACE TABLE gold_kpi_by_method_channel AS
SELECT
    payment_method,
    channel,
    COUNT(DISTINCT transaction_id)    AS total_transactions,
    SUM(amount)   AS total_amount,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id)  AS success_rate,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id) AS failure_rate,
    AVG(processing_time_ms)  AS avg_processing_time_ms
FROM gold_transactions_enriched
GROUP BY payment_method, channel
ORDER BY failure_rate DESC, total_transactions DESC;
