# CMS Medallion Pipeline — Internals

Technical reference covering process flow, data grain decisions, dependency management, and gold layer table design.

---

## Pipeline Process Flow

```
bronze_medicare  ──────────────────────────────────────────┐
  CMS API → paginated 5k-row batches → Delta (ADLS)        │
  OPTIMIZE to compact small files                           │
                                                            ├──▶ silver_transform ──▶ silver_dq ──▶ gold_models
bronze_medicaid  ──────────────────────────────────────────┘
  dbutils.fs.cp (HHS URL → ADLS staging)                   
  Spark batch read parquet → Delta                          
  OPTIMIZE                                                  
```

Tasks run on a single-node Databricks cluster (Standard_D4s_v3) under Unity Catalog `SINGLE_USER` mode. The workflow is defined as a Databricks Job with explicit `depends_on` task keys — the two bronze tasks run in parallel; silver_transform fans in from both.

---

## Bronze Layer

### `ingest_medicare.py`
Ingests Medicare Provider Utilization and Payment Data (2022) from the CMS open data REST API.

The API returns JSON in pages. Rather than accumulating all pages in driver memory (which caused OOM at ~3M rows), each 5,000-row page is written directly to a Delta table:
- First batch: `mode="overwrite"` with `overwriteSchema=true` — creates the table fresh
- Subsequent batches: `mode="append"` — no schema negotiation needed

After all pages are written, `OPTIMIZE` compacts the thousands of small Delta files produced by append writes into a small number of larger Parquet files.

Two audit columns are added at ingest: `_ingested_at` (timestamp) and `_source` (literal string identifying the dataset version).

**Result**: 9,660,647 rows across ~1,933 API pages.

### `ingest_medicaid_fraud.py`
Ingests Medicaid Provider Spending data (2018–2024) released by HHS as a single large Parquet file (238M rows).

Unity Catalog blocks writes to local `/tmp` — only paths under `/Workspace` or ADLS external locations are permitted. Reading the full 238M-row file into Python memory via PyArrow also caused OOM. The solution:

1. `dbutils.fs.cp(SOURCE_URL, staging_path)` — Databricks copies the file directly from the public HHS Azure Blob URL to an ADLS staging path, bypassing the driver entirely
2. `spark.read.parquet(staging_path)` — Spark reads it distributed from ADLS
3. Write to Delta bronze path, then `dbutils.fs.rm(staging_path)` to clean up

`OPTIMIZE` runs after write to compact files.

**Result**: 238,015,729 rows.

---

## Silver Layer

### `transform_providers.py`
Produces three silver Delta tables.

#### `silver/medicare`
Column rename and type casting from raw CMS field names (e.g. `Rndrng_NPI` → `provider_npi`, `Avg_Mdcr_Pymt_Amt` → `avg_medicare_payment_amt`). String columns containing numeric values are cast to `DoubleType` or `IntegerType`. Rows with null NPI or HCPCS are filtered out. A `snapshot_year = 2022` column is added.

**Grain**: one row per provider (`Rndrng_NPI`) + HCPCS code + place of service. This is the raw grain from CMS — a provider billing the same HCPCS from multiple office locations produces multiple rows.

When aggregated for the overlap join (see `silver/provider_overlap` below), all billing measures (`total_services`, `total_beneficiaries`, payment amounts) are summed or averaged across locations so no activity is lost. Provider metadata (`provider_specialty`, `provider_state`) uses `first()` — picking one representative value rather than exploding the grain — since a provider's specialty and state are stable attributes unlikely to vary meaningfully across locations.

#### `silver/medicaid`
Raw Medicaid data has one row per billing provider + servicing provider + HCPCS + claim month. The servicing provider is the individual who performed the service; the billing provider is the entity that submitted the claim. For cross-program analysis the relevant identity is the billing provider.

The table is filtered to `CLAIM_FROM_MONTH` between `2022-01` and `2022-12` (aligning with the Medicare snapshot year), then aggregated to billing provider + HCPCS + claim month grain — summing `total_patients`, `total_claim_lines`, and `total_paid` across all servicing providers.

**Grain**: one row per billing `provider_npi` + `hcpcs_code` + `claim_month`.

**Result**: 20,290,000+ rows (one year of Medicaid billing activity).

#### `silver/provider_overlap`
Identifies providers who billed both Medicare and Medicaid in 2022 for the same HCPCS code. Produced by an inner join on `provider_npi + hcpcs_code`.

Before joining, both sides are aggregated to provider + HCPCS grain:

- **Medicaid**: sum `total_patients`, `total_claim_lines`, `total_paid` across months; count distinct `claim_month` as `medicaid_months_billed`
- **Medicare**: sum `total_beneficiaries` and `total_services`; average `avg_submitted_charge` and `avg_medicare_payment_amt` — all billing measures are preserved across locations. `first()` is used for `provider_specialty` and `provider_state` — a provider may bill the same HCPCS from multiple locations, producing multiple specialty/state values per NPI+HCPCS; `first()` picks one representative value rather than exploding rows. No billing activity is lost by this choice.

**Result**: ~240,000 rows (87,589 distinct overlapping NPIs × their shared HCPCS codes).

### `data_quality.py`
Runs after `transform_providers.py`. Validates all three silver tables before gold models execute.

Checks are classified as **critical** (pipeline-halting) or **non-critical** (logged, pipeline continues):

| Table | Check | Critical |
|---|---|---|
| medicare | provider_npi not null | Yes |
| medicare | hcpcs_code not null | Yes |
| medicare | total_services, avg_payment, avg_charge ≥ 0 | Yes |
| medicare | snapshot_year = 2022 | Yes |
| medicare | provider_npi is 10 digits | Yes |
| medicaid | provider_npi, hcpcs_code not null | Yes |
| medicaid | total_patients ≥ 0 | Yes |
| medicaid | claim_year = 2022 | Yes |
| medicaid | total_paid ≥ 0 | No — 520 legitimate claim reversals |
| medicaid | claim_month format (YYYY-MM) | No |
| overlap | npi, hcpcs not null | Yes |
| overlap | medicare_total_services, avg_payment ≥ 0 | Yes |
| overlap | snapshot_year = 2022 | Yes |
| overlap | medicaid_total_paid ≥ 0 | No — legitimate reversals |

Critical failures raise `ValueError`, which Databricks Workflow treats as task failure and halts downstream tasks. Non-critical failures are printed for visibility but do not block gold.

---

## Gold Layer

Gold models are run by `run_gold_models.py` via `spark.sql()` using `CREATE OR REPLACE TABLE ... USING DELTA AS`. The same SQL is also maintained as dbt models under `dbt/cms_medallion/models/gold/` for local development with `dbt run`.

All three tables write to the `dbw_cms_medallion_dev.gold` schema (Unity Catalog).

### `agg_provider_spending`

**Purpose**: Provider-level spending rollup across both programs.

**Source tables**: `silver.medicare` (left), `silver.medicaid` (right)

**Join**: `LEFT JOIN` on `provider_npi` — all Medicare providers are included; Medicaid columns are null for providers not in Medicaid.

**Medicare aggregation** (before join): grouped by `provider_npi`, using `first()` for specialty and state, `sum()` for services and beneficiaries, `avg()` for payment amounts, `count(DISTINCT hcpcs_code)` for breadth.

**Medicaid aggregation** (before join): grouped by `provider_npi`, summing totals and counting distinct HCPCS codes and active months.

**Key columns**:
- `medicare_avg_payment_amt` — average Medicare reimbursement per service across this provider's HCPCS codes
- `medicare_avg_submitted_charge` — what the provider billed; gap vs. payment_amt indicates billing aggressiveness
- `medicaid_total_paid` — total Medicaid payments to this provider in 2022
- `bills_both_programs` — true when the provider has a Medicaid match
- `medicare_distinct_hcpcs_count` / `medicaid_distinct_hcpcs_count` — breadth of service mix

**Useful for**: charge-vs-payment gap analysis by specialty/state; dual-program participation rates; Medicare reimbursement rate comparisons.

**Note**: Total Medicare dollars collected cannot be derived from this table — `medicare_avg_payment_amt` is an average of averages across HCPCS codes, not a per-claim figure. Provider-level totals would need to be computed from `silver.medicare` directly before the averaging aggregation. `fraud_risk_indicators` has per-HCPCS service counts but only covers dual-billing providers, not all Medicare providers.

---

### `agg_specialty_utilization`

**Purpose**: Procedure-level utilization benchmarks by specialty.

**Source table**: `silver.medicare` only

**Grain**: one row per `provider_specialty + hcpcs_code + hcpcs_description`

**Key columns**:
- `provider_count` — how many distinct providers in this specialty billed this HCPCS code
- `total_beneficiaries`, `total_services` — aggregate volume
- `avg_medicare_payment_amt` — average Medicare reimbursement for this procedure in this specialty
- `avg_submitted_charge` — average billed amount
- `avg_charge_to_payment_gap` — `avg_submitted_charge - avg_medicare_payment_amt`; how much more providers charge vs. what Medicare pays

**Useful for**: identifying which procedures have the largest charge-to-payment gaps; benchmarking a specific provider's rates against their specialty peers (used internally by `fraud_risk_indicators`); spotting HCPCS codes billed by unusually many or few providers within a specialty.

---

### `fraud_risk_indicators`

**Purpose**: Risk scoring for providers who bill both Medicare and Medicaid, comparing their behavior against specialty/HCPCS benchmarks.

**Source table**: `silver.provider_overlap` (providers appearing in both programs)

**Grain**: one row per `provider_npi + hcpcs_code` — same as provider_overlap

**Benchmarks computed inline** (CTE `specialty_benchmarks`): grouped by `provider_specialty + hcpcs_code`, computing:
- `specialty_avg_submitted_charge` and `specialty_stddev_submitted_charge`
- `specialty_avg_services` and `specialty_p95_services` (95th percentile via `percentile_approx`)

**Risk flags** (each boolean):
| Flag | Condition |
|---|---|
| `high_volume_flag` | `medicare_total_services > specialty_p95_services` |
| `high_charge_flag` | `medicare_avg_submitted_charge > 2 × specialty_avg_submitted_charge` |
| `high_medicaid_spend_flag` | `medicaid_total_paid > $50,000` |

**`risk_score`**: sum of the three flags (0–3). A score of 3 means the provider is a high-volume, high-charge, high-Medicaid-spend outlier for that procedure within their specialty.

**`charge_ratio_vs_specialty`**: `medicare_avg_submitted_charge / specialty_avg_submitted_charge`, rounded to 2 decimal places. A ratio of 2.5 means the provider bills 2.5× the specialty average for that code.

**Useful for**: ranking dual-billing providers by composite risk; filtering to `risk_score = 3` for highest-priority review candidates; analyzing whether high-charge providers are also high-volume, or whether the signals are independent.

---

## Dependency Management

### Infrastructure — Terraform

All Azure and Databricks resources are provisioned by Terraform (`terraform/`). A single `terraform apply` creates or updates:

| Resource | Purpose |
|---|---|
| `azurerm_resource_group` | Container for all Azure resources |
| `azurerm_storage_account` | ADLS Gen2 storage (`cmsdevadls`) |
| `azurerm_storage_data_lake_gen2_filesystem` × 3 | `bronze`, `silver`, `gold` containers |
| `azurerm_databricks_workspace` | Premium workspace (required for Unity Catalog) |
| `azurerm_databricks_access_connector` | Managed identity for ADLS authentication |
| `azurerm_role_assignment` × 2 | `Storage Blob Data Contributor` for access connector + current user |
| `databricks_storage_credential` | Unity Catalog credential backed by access connector |
| `databricks_external_location` × 3 | Unity Catalog external locations for bronze/silver/gold |
| `databricks_cluster` | Single-node dev cluster, `SINGLE_USER` mode, 30-min auto-terminate |
| `databricks_secret_scope` + `databricks_secret` | Scope `cms`, key `databricks_pat` (PAT for dbt local dev) |
| `databricks_workspace_file` × 5 | dbt project files pushed to Databricks workspace |
| `databricks_job` | 5-task workflow with email failure alerts |

**Providers**: `hashicorp/azurerm ~> 3.110` and `databricks/databricks ~> 1.50`. The Databricks provider authenticates to the workspace via `azure_workspace_resource_id` (using the Azure CLI session — no separate Databricks token needed for Terraform).

Sensitive values (`databricks_pat`, `subscription_id`) live in `terraform.tfvars` which is gitignored. `terraform.tfvars.example` is committed as a template.

### ADLS Authentication — Managed Identity

Notebooks do not use storage account keys or SAS tokens. ADLS Gen2 access flows through Unity Catalog external locations, which are backed by a Databricks Access Connector with a system-assigned managed identity. The managed identity holds `Storage Blob Data Contributor` on the storage account. When a notebook reads or writes an `abfss://` path registered as an external location, Unity Catalog authorizes the access transparently.

This means notebooks reference ADLS paths directly (e.g. `abfss://bronze@cmsdevadls.dfs.core.windows.net/medicare`) with no credential configuration in the notebook itself.

### Secret Management

The Databricks PAT is stored in secret scope `cms`, key `databricks_pat`, provisioned by Terraform from `var.databricks_pat`. It is used by `run_dbt.py` (local dev reference notebook) for dbt-databricks authentication. The workflow notebook `run_gold_models.py` does not need it — it runs Spark SQL directly on the cluster.

### dbt — Local Development

dbt models live in `dbt/cms_medallion/models/gold/`. A Python virtual environment under `dbt/.venv/` holds `dbt-databricks` and dependencies. Connection is configured via `~/.dbt/profiles.yml` (gitignored), targeting the SQL warehouse HTTP path and authenticating with the PAT.

`dbt_project.yml` configures all gold models to materialize as Delta tables in the `gold` schema of the `dbw_cms_medallion_dev` catalog. `models/gold/sources.yml` declares the silver tables as dbt sources pointing at `dbw_cms_medallion_dev.default` (the Unity Catalog schema where silver Delta tables are registered).

The `dbt/` directory is committed for reference and local iteration. The Databricks Workflow does not run dbt — it runs `run_gold_models.py` instead, which contains the same SQL executed via `spark.sql()`. This avoids the `dbt-databricks` / Databricks internal protobuf version conflict that occurs when installing dbt inside a running notebook.

### Workflow Task Dependencies

```
bronze_medicare  ─┐
                  ├──▶ silver_transform ──▶ silver_dq ──▶ gold_models
bronze_medicaid  ─┘
```

Defined in Terraform as `databricks_job` with `depends_on { task_key = "..." }` blocks. Databricks Workflow enforces execution order and halts downstream tasks on any upstream failure. Email failure alerts are sent to the configured address on any task failure.
