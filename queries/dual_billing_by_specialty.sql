-- Dual-billing participation rate and Medicaid spend by specialty
-- Shows which specialties have the highest share of providers billing both programs
-- and how much Medicaid paid those dual billers in 2022

SELECT
    provider_specialty,
    count(DISTINCT provider_npi)                                AS total_providers,
    sum(CASE WHEN bills_both_programs THEN 1 ELSE 0 END)        AS dual_billing_providers,
    round(
        sum(CASE WHEN bills_both_programs THEN 1 ELSE 0 END)
        / count(DISTINCT provider_npi) * 100, 1
    )                                                           AS dual_billing_pct,
    round(sum(CASE WHEN bills_both_programs
        THEN medicaid_total_paid ELSE 0 END), 2)                AS total_medicaid_paid,
    round(avg(CASE WHEN bills_both_programs
        THEN medicaid_total_paid END), 2)                       AS avg_medicaid_paid_per_dual_biller
FROM dbw_cms_medallion_dev.gold.agg_provider_spending
WHERE provider_specialty IS NOT NULL
GROUP BY provider_specialty
HAVING count(DISTINCT provider_npi) >= 100
ORDER BY dual_billing_pct DESC
LIMIT 30
