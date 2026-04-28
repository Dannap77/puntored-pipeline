-- KPIs agregados por usuario para segmentación y ranking.

CREATE OR REPLACE TABLE gold_kpi_by_user AS
SELECT
    user_id,
    user_name,
    user_email,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(amount) AS total_amount,
    AVG(amount)  AS avg_ticket,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
        / COUNT(DISTINCT transaction_id) AS success_rate,
    SUM(CASE WHEN status = 'success' THEN amount ELSE 0 END) AS revenue,
    MIN(transaction_at) AS first_transaction_at,
    MAX(transaction_at) AS last_transaction_at,
    DATEDIFF(
        'day',
        MIN(transaction_at),
        MAX(transaction_at)
    )  AS lifetime_days
FROM gold_transactions_enriched
GROUP BY user_id, user_name, user_email;
