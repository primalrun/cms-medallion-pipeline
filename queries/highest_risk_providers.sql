-- Highest-risk dual-billing providers (risk_score = 3)
-- Providers flagged on all three dimensions: high volume, high charge, high Medicaid spend
-- Ordered by charge_ratio_vs_specialty to surface the most aggressive billers first

SELECT
    provider_npi,
    provider_specialty,
    provider_state,
    hcpcs_code,
    hcpcs_description,
    medicare_total_services,
    specialty_p95_services,
    round(medicare_total_services / specialty_p95_services, 2)  AS volume_ratio_vs_p95,
    medicare_avg_submitted_charge,
    specialty_avg_submitted_charge,
    charge_ratio_vs_specialty,
    medicaid_total_paid,
    medicaid_months_billed,
    risk_score
FROM dbw_cms_medallion_dev.gold.fraud_risk_indicators
WHERE risk_score = 3
ORDER BY charge_ratio_vs_specialty DESC
LIMIT 50
