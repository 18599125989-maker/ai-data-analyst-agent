WITH active_subs AS (
    SELECT
        sub_id,
        tenant_id,
        plan_tier,
        start_date,
        end_date,
        mrr,
        status
    FROM fact_subscription
    WHERE status = 'active'
      AND start_date <= DATE '2025-10-31'
      AND COALESCE(end_date, DATE '9999-12-31') >= DATE '2025-10-01'
)

SELECT
    a.plan_tier,
    COUNT(*) AS active_subscription_count,
    ROUND(SUM(a.mrr), 2) AS total_mrr,
    ROUND(SUM(a.mrr / NULLIF(p.monthly_price, 0)), 2) AS estimated_seats,
    ROUND(SUM((a.mrr / NULLIF(p.monthly_price, 0)) * p.monthly_price), 2) AS nominal_revenue
FROM active_subs a
LEFT JOIN dim_plan p
    ON a.plan_tier = p.plan_tier
GROUP BY a.plan_tier
ORDER BY total_mrr DESC;
