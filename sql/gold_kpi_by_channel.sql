-- KPIs por canal: volumen, tasa de éxito y tiempo de procesamiento.

CREATE OR REPLACE TABLE gold_kpi_by_channel AS
SELECT
    channel,
    COUNT(DISTINCT transaction_id)                                          AS total_transactions,
    SUM(amount)                                                             AS total_amount,
    AVG(amount)                                                             AS avg_ticket,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id)                                    AS success_rate,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id)                                    AS failure_rate,
    AVG(processing_time_ms)                                                 AS avg_processing_time_ms,
    MEDIAN(processing_time_ms)                                              AS median_processing_time_ms
FROM gold_transactions_enriched
GROUP BY channel
ORDER BY total_amount DESC;
