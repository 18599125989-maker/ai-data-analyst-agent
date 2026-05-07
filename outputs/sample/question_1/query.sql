-- Question 1: 企业画像分布

-- 1. 按国家统计企业数
SELECT
    country,
    COUNT(*) AS tenant_count
FROM dim_tenant
GROUP BY country
ORDER BY tenant_count DESC, country;

-- 2. 按规模统计企业数和占比
SELECT
    size_tier,
    COUNT(*) AS tenant_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM dim_tenant
GROUP BY size_tier
ORDER BY tenant_count DESC, size_tier;

-- 3. 国家 × 行业二维分布
SELECT
    country,
    industry,
    COUNT(*) AS tenant_count
FROM dim_tenant
GROUP BY country, industry
ORDER BY country, industry;
