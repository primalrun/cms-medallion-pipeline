# Command Line Steps

Chronological record of terminal commands used to build and deploy the CMS Medallion Pipeline.

---

## 1. Azure CLI Setup

```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Log in to Azure
az login

# Get subscription ID
az account show --query id -o tsv
```

---

## 2. Project Folder Structure

```bash
mkdir -p ~/cms-medallion-pipeline/{notebooks/{bronze,silver,gold},dbt,terraform,docs}
```

---

## 3. Terraform — Infrastructure Provisioning

```bash
cd ~/cms-medallion-pipeline/terraform

# Copy example vars and fill in subscription_id, databricks_pat, alert_email
cp terraform.tfvars.example terraform.tfvars

# Initialize Terraform (downloads azurerm and databricks providers)
terraform init

# Preview changes
terraform plan

# Provision all Azure and Databricks resources
terraform apply
```

Resources provisioned: resource group, ADLS Gen2 storage account, bronze/silver/gold containers, Databricks Premium workspace, access connector (managed identity), role assignments, Unity Catalog storage credential, external locations, single-node cluster, secret scope + PAT secret, dbt workspace files, cms-medallion-pipeline workflow job.

```bash
# After updating workflow task from run_dbt → run_gold_models
terraform apply
```

---

## 4. dbt — Local Development Setup

```bash
cd ~/cms-medallion-pipeline/dbt

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dbt-databricks
pip install dbt-databricks

# Initialize dbt project (prompted for host, http_path, catalog, schema, threads)
dbt init cms_medallion

# Verify connection
cd cms_medallion
dbt debug

# Run gold models against Databricks SQL warehouse
dbt run
```

---

## 5. Databricks Notebooks — Manual Steps

Notebooks were imported manually into the Databricks workspace via the UI, maintaining folder structure:

```
/Users/<your-email>/
  bronze/
    ingest_medicare
    ingest_medicaid_fraud
  silver/
    transform_providers
    data_quality
  gold/
    run_gold_models
```

Notebooks were run individually in order to validate each layer before running the full workflow:
1. `bronze/ingest_medicare` — 9,660,647 rows ingested
2. `bronze/ingest_medicaid_fraud` — 238,015,729 rows ingested
3. `silver/transform_providers` — medicare, medicaid, provider_overlap silver tables created
4. `silver/data_quality` — all critical DQ checks passed
5. `gold/run_gold_models` — agg_provider_spending, agg_specialty_utilization, fraud_risk_indicators created

---

## 6. Full Workflow Run

Triggered from the Databricks Jobs UI:
- Job: `cms-medallion-pipeline`
- All 5 tasks completed successfully
- Runtime: 4h 34m

---

## 7. Git — Repository Setup

```bash
cd ~/cms-medallion-pipeline

# Initialize repo and rename default branch
git init
git branch -m main

# Stage project files (excluding venv, tfstate, tfvars, dbt target)
git add .gitignore README.md INTERNALS.md docs/ notebooks/ terraform/ \
    dbt/cms_medallion/dbt_project.yml dbt/cms_medallion/models/ \
    dbt/cms_medallion/analyses/.gitkeep dbt/cms_medallion/macros/.gitkeep \
    dbt/cms_medallion/seeds/.gitkeep dbt/cms_medallion/snapshots/.gitkeep \
    dbt/cms_medallion/tests/.gitkeep dbt/cms_medallion/.gitignore \
    dbt/cms_medallion/README.md

# Initial commit
git commit -m "Initial commit: end-to-end CMS medallion pipeline on Azure Databricks"

# Add GitHub remote and push
git remote add origin https://github.com/primalrun/cms-medallion-pipeline.git
git push -u origin main
```

---

## 8. Ongoing Git Workflow

```bash
# Stage and commit changes
git add <file>
git commit -m "description"
git push
```
