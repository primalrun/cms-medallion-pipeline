locals {
  name_suffix = "${var.project}-${var.environment}"
  # Storage account names: lowercase, no hyphens, max 24 chars
  storage_name = replace("cms${var.environment}adls", "-", "")
}

# ── Current user (for role assignment) ───────────────────────────────────────

data "azurerm_client_config" "current" {}

# ── Resource Group ────────────────────────────────────────────────────────────

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_suffix}"
  location = var.location
}

# ── ADLS Gen2 ─────────────────────────────────────────────────────────────────

resource "azurerm_storage_account" "this" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.this.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.this.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.this.id
}

# ── Databricks Workspace ──────────────────────────────────────────────────────

resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${local.name_suffix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium"
}

# ── Databricks Access Connector (managed identity for ADLS auth) ──────────────
# Unity Catalog uses this instead of storage account keys

resource "azurerm_databricks_access_connector" "this" {
  name                = "ac-${local.name_suffix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "access_connector_adls" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "current_user_adls" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ── Unity Catalog Storage Credential + External Locations ─────────────────────

data "databricks_current_user" "me" {}

resource "databricks_storage_credential" "adls" {
  name = "adls-credential"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }
  depends_on = [azurerm_databricks_workspace.this]
}

resource "databricks_external_location" "bronze" {
  name            = "bronze"
  url             = "abfss://bronze@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.id
}

resource "databricks_external_location" "silver" {
  name            = "silver"
  url             = "abfss://silver@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.id
}

resource "databricks_external_location" "gold" {
  name            = "gold"
  url             = "abfss://gold@${azurerm_storage_account.this.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.id
}

# ── Databricks Cluster ────────────────────────────────────────────────────────

resource "databricks_cluster" "dev" {
  cluster_name            = "cms-dev-cluster"
  spark_version           = "15.4.x-scala2.12"
  node_type_id            = "Standard_D4s_v3"
  autotermination_minutes = 30
  num_workers             = 0
  data_security_mode      = "SINGLE_USER"
  single_user_name        = data.databricks_current_user.me.user_name

  spark_conf = {
    "spark.master"                     = "local[*]"
    "spark.databricks.cluster.profile" = "singleNode"
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

# ── PAT Secret (used by run_dbt notebook in workflow) ─────────────────────────

resource "databricks_secret_scope" "cms" {
  name = "cms"
}

resource "databricks_secret" "databricks_pat" {
  key          = "databricks_pat"
  string_value = var.databricks_pat
  scope        = databricks_secret_scope.cms.id
}

# ── dbt Project Files → Databricks Workspace ──────────────────────────────────

locals {
  dbt_ws_root = "/Users/${data.databricks_current_user.me.user_name}/dbt_project/cms_medallion"
}

resource "databricks_workspace_file" "dbt_project_yml" {
  source = "${path.module}/../dbt/cms_medallion/dbt_project.yml"
  path   = "${local.dbt_ws_root}/dbt_project.yml"
}

resource "databricks_workspace_file" "dbt_agg_provider_spending" {
  source = "${path.module}/../dbt/cms_medallion/models/gold/agg_provider_spending.sql"
  path   = "${local.dbt_ws_root}/models/gold/agg_provider_spending.sql"
}

resource "databricks_workspace_file" "dbt_agg_specialty_utilization" {
  source = "${path.module}/../dbt/cms_medallion/models/gold/agg_specialty_utilization.sql"
  path   = "${local.dbt_ws_root}/models/gold/agg_specialty_utilization.sql"
}

resource "databricks_workspace_file" "dbt_fraud_risk_indicators" {
  source = "${path.module}/../dbt/cms_medallion/models/gold/fraud_risk_indicators.sql"
  path   = "${local.dbt_ws_root}/models/gold/fraud_risk_indicators.sql"
}

resource "databricks_workspace_file" "dbt_sources_yml" {
  source = "${path.module}/../dbt/cms_medallion/models/gold/sources.yml"
  path   = "${local.dbt_ws_root}/models/gold/sources.yml"
}

# ── Databricks Workflow ────────────────────────────────────────────────────────

resource "databricks_job" "pipeline" {
  name = "cms-medallion-pipeline"

  task {
    task_key            = "bronze_medicare"
    existing_cluster_id = databricks_cluster.dev.cluster_id
    notebook_task {
      notebook_path = "/Users/${data.databricks_current_user.me.user_name}/bronze/ingest_medicare"
    }
  }

  task {
    task_key            = "bronze_medicaid"
    existing_cluster_id = databricks_cluster.dev.cluster_id
    notebook_task {
      notebook_path = "/Users/${data.databricks_current_user.me.user_name}/bronze/ingest_medicaid_fraud"
    }
  }

  task {
    task_key            = "silver_transform"
    existing_cluster_id = databricks_cluster.dev.cluster_id
    depends_on { task_key = "bronze_medicare" }
    depends_on { task_key = "bronze_medicaid" }
    notebook_task {
      notebook_path = "/Users/${data.databricks_current_user.me.user_name}/silver/transform_providers"
    }
  }

  task {
    task_key            = "silver_dq"
    existing_cluster_id = databricks_cluster.dev.cluster_id
    depends_on { task_key = "silver_transform" }
    notebook_task {
      notebook_path = "/Users/${data.databricks_current_user.me.user_name}/silver/data_quality"
    }
  }

  task {
    task_key            = "gold_models"
    existing_cluster_id = databricks_cluster.dev.cluster_id
    depends_on { task_key = "silver_dq" }
    notebook_task {
      notebook_path = "/Users/${data.databricks_current_user.me.user_name}/gold/run_gold_models"
    }
  }

  email_notifications {
    on_failure = [var.alert_email]
  }
}
