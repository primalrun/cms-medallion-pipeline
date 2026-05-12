{{ config(materialized='table', file_format='delta') }}

-- Gold: Specialty-level utilization and payment aggregation (2022)
-- Grain: one row per provider specialty + HCPCS code

select
    provider_specialty,
    hcpcs_code,
    hcpcs_description,
    count(distinct provider_npi)            as provider_count,
    sum(total_beneficiaries)                as total_beneficiaries,
    sum(total_services)                     as total_services,
    avg(avg_medicare_payment_amt)           as avg_medicare_payment_amt,
    avg(avg_submitted_charge)               as avg_submitted_charge,
    avg(avg_submitted_charge)
        - avg(avg_medicare_payment_amt)     as avg_charge_to_payment_gap,
    2022                                    as snapshot_year
from {{ source('silver', 'medicare') }}
where provider_specialty is not null
  and hcpcs_code is not null
group by
    provider_specialty,
    hcpcs_code,
    hcpcs_description
