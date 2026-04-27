-- Serie temporal diaria para detectar tendencias y estacionalidad.

CREATE OR REPLACE TABLE gold_kpi_by_day AS
SELECT
    transaction_date,
    COUNT(DISTINCT transaction_id)                                          AS total_transactions,
    SUM(amount)                                                             AS total_amount,
    SUM(CASE WHEN status = 'success' THEN amount ELSE 0 END)                AS revenue,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id)                                    AS success_rate,
    AVG(processing_time_ms)                                                 AS avg_processing_time_ms
FROM gold_transactions_enriched
GROUP BY transaction_date
ORDER BY transaction_date;
