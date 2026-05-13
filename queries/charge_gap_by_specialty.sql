-- Charge-to-payment gap by specialty
-- Shows how much more each specialty bills vs. what Medicare actually pays
-- High gap may indicate aggressive billing practices or complex negotiation dynamics

SELECT
    provider_specialty,
    count(DISTINCT hcpcs_code)              AS distinct_hcpcs_count,
    count(DISTINCT provider_count)          AS provider_count,
    round(avg(avg_submitted_charge), 2)     AS avg_submitted_charge,
    round(avg(avg_medicare_payment_amt), 2) AS avg_medicare_payment_amt,
    round(avg(avg_charge_to_payment_gap), 2)AS avg_charge_to_payment_gap,
    round(
        avg(avg_charge_to_payment_gap)
        / nullif(avg(avg_medicare_payment_amt), 0) * 100, 1
    )                                       AS gap_pct_of_payment
FROM dbw_cms_medallion_dev.gold.agg_specialty_utilization
WHERE provider_specialty IS NOT NULL
GROUP BY provider_specialty
ORDER BY avg_charge_to_payment_gap DESC
LIMIT 30
