-- Most common HCPCS codes among highest-risk dual-billing providers
-- Identifies which procedures are most frequently associated with risk_score = 3
-- Useful for targeting audits at specific procedure types

SELECT
    hcpcs_code,
    hcpcs_description,
    count(DISTINCT provider_npi)            AS provider_count,
    count(DISTINCT provider_specialty)      AS specialty_count,
    round(avg(charge_ratio_vs_specialty), 2)AS avg_charge_ratio,
    round(avg(medicaid_total_paid), 2)      AS avg_medicaid_paid,
    sum(medicare_total_services)            AS total_medicare_services
FROM dbw_cms_medallion_dev.gold.fraud_risk_indicators
WHERE risk_score = 3
GROUP BY hcpcs_code, hcpcs_description
ORDER BY provider_count DESC
LIMIT 30
