-- Medicare reimbursement rates by state
-- Compares average Medicare payment and submitted charge across states
-- to surface geographic variation in billing and reimbursement

SELECT
    provider_state,
    count(DISTINCT provider_npi)                AS provider_count,
    round(avg(medicare_avg_payment_amt), 2)     AS avg_medicare_payment,
    round(avg(medicare_avg_submitted_charge), 2)AS avg_submitted_charge,
    round(
        avg(medicare_avg_submitted_charge)
        - avg(medicare_avg_payment_amt), 2
    )                                           AS avg_charge_payment_gap,
    sum(medicare_total_beneficiaries)           AS total_beneficiaries,
    sum(CASE WHEN bills_both_programs THEN 1 ELSE 0 END) AS dual_billing_providers
FROM dbw_cms_medallion_dev.gold.agg_provider_spending
WHERE provider_state IS NOT NULL
GROUP BY provider_state
ORDER BY avg_medicare_payment DESC
