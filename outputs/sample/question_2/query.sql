SELECT
    STRFTIME(DATE_TRUNC('month', register_at), '%Y-%m') AS register_month,
    COUNT(*) AS new_users,
    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_users,
    SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive_users,
    SUM(CASE WHEN status = 'suspended' THEN 1 ELSE 0 END) AS suspended_users
FROM dim_user
WHERE register_at >= TIMESTAMP '2025-04-01'
  AND register_at <  TIMESTAMP '2026-04-01'
GROUP BY register_month
ORDER BY register_month;
