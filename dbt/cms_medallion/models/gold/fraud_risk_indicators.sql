{{ config(materialized='table', file_format='delta') }}

-- Gold: Fraud and waste risk indicators for dual-billing providers (2022)
-- Grain: one row per provider NPI + HCPCS code
-- Flags providers with unusual billing patterns across Medicare and Medicaid

with overlap as (
    select
        provider_npi,
        hcpcs_code,
        hcpcs_description,
        provider_specialty,
        provider_state,
        medicare_total_services,
        medicare_total_beneficiaries,
        medicare_avg_submitted_charge,
        medicare_avg_payment_amt,
        medicaid_total_paid,
        medicaid_total_patients,
        medicaid_total_claim_lines,
        medicaid_months_billed
    from {{ source('silver', 'provider_overlap') }}
),

-- Compute specialty-level benchmarks for anomaly detection
specialty_benchmarks as (
    select
        provider_specialty,
        hcpcs_code,
        avg(medicare_avg_submitted_charge)          as specialty_avg_submitted_charge,
        stddev(medicare_avg_submitted_charge)       as specialty_stddev_submitted_charge,
        avg(medicare_total_services)                as specialty_avg_services,
        percentile_approx(medicare_total_services, 0.95)
                                                    as specialty_p95_services
    from overlap
    group by provider_specialty, hcpcs_code
)

select
    o.provider_npi,
    o.hcpcs_code,
    o.hcpcs_description,
    o.provider_specialty,
    o.provider_state,
    o.medicare_total_services,
    o.medicare_avg_submitted_charge,
    o.medicare_avg_payment_amt,
    o.medicaid_total_paid,
    o.medicaid_total_patients,
    o.medicaid_months_billed,
    sb.specialty_avg_submitted_charge,
    sb.specialty_avg_services,
    sb.specialty_p95_services,

    -- Charge ratio: how much more than specialty average this provider charges
    round(
        o.medicare_avg_submitted_charge / nullif(sb.specialty_avg_submitted_charge, 0),
        2
    )                                               as charge_ratio_vs_specialty,

    -- Volume flag: provider services exceed specialty 95th percentile
    case when o.medicare_total_services > sb.specialty_p95_services
        then true else false
    end                                             as high_volume_flag,

    -- High charge flag: provider charges >2x specialty average
    case when o.medicare_avg_submitted_charge
             > 2 * sb.specialty_avg_submitted_charge
        then true else false
    end                                             as high_charge_flag,

    -- Dual program high spend: significant Medicaid spend alongside Medicare
    case when o.medicaid_total_paid > 50000
        then true else false
    end                                             as high_medicaid_spend_flag,

    -- Composite risk score (0-3): count of risk flags triggered
    (
        case when o.medicare_total_services > sb.specialty_p95_services    then 1 else 0 end
      + case when o.medicare_avg_submitted_charge
                 > 2 * sb.specialty_avg_submitted_charge                   then 1 else 0 end
      + case when o.medicaid_total_paid > 50000                            then 1 else 0 end
    )                                               as risk_score,

    2022                                            as snapshot_year
from overlap o
left join specialty_benchmarks sb
    on o.provider_specialty = sb.provider_specialty
   and o.hcpcs_code = sb.hcpcs_code
