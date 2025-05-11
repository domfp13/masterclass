resource "azurerm_resource_group" "dbx_managed_rg" {
  name     = "databricks-mc-managed-rg"
  location = "West Europe"
}

resource "azurerm_resource_group" "dbx_rg" {
  name     = "databricks-mc-rg"
  location = "West Europe"
}

resource "azurerm_databricks_workspace" "databricks_ws" {
  name                = "databricks-mc-ws"
  resource_group_name = azurerm_resource_group.example.name
  location            = azurerm_resource_group.example.location

  managed_resource_group_name = azurerm_resource_group.dbx_managed_rg.name
  sku                 = "standard"

  tags = {
    Environment = "Production"
  }
}
