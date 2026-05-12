{{ config(materialized='table', file_format='delta') }}

-- Gold: Provider-level spending aggregation across Medicare and Medicaid (2022)
-- Grain: one row per provider NPI

with medicare as (
    select
        provider_npi,
        first(provider_specialty)                       as provider_specialty,
        first(provider_state)                           as provider_state,
        sum(total_services)                             as medicare_total_services,
        sum(total_beneficiaries)                        as medicare_total_beneficiaries,
        avg(avg_medicare_payment_amt)                   as medicare_avg_payment_amt,
        avg(avg_submitted_charge)                       as medicare_avg_submitted_charge,
        count(distinct hcpcs_code)                      as medicare_distinct_hcpcs_count
    from {{ source('silver', 'medicare') }}
    group by provider_npi
),

medicaid as (
    select
        provider_npi,
        sum(total_paid)                                 as medicaid_total_paid,
        sum(total_patients)                             as medicaid_total_patients,
        sum(total_claim_lines)                          as medicaid_total_claim_lines,
        count(distinct hcpcs_code)                      as medicaid_distinct_hcpcs_count,
        count(distinct claim_month)                     as medicaid_months_active
    from {{ source('silver', 'medicaid') }}
    group by provider_npi
)

select
    m.provider_npi,
    m.provider_specialty,
    m.provider_state,
    m.medicare_total_services,
    m.medicare_total_beneficiaries,
    m.medicare_avg_payment_amt,
    m.medicare_avg_submitted_charge,
    m.medicare_distinct_hcpcs_count,
    md.medicaid_total_paid,
    md.medicaid_total_patients,
    md.medicaid_total_claim_lines,
    md.medicaid_distinct_hcpcs_count,
    md.medicaid_months_active,
    case when md.provider_npi is not null then true else false end as bills_both_programs,
    2022                                                           as snapshot_year
from medicare m
left join medicaid md on m.provider_npi = md.provider_npi
