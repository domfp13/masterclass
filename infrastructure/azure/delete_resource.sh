# login to Azure
az login

# list all subscriptions
az account list --output table

# set the subscription
az account set --subscription <subscription_id>

# listing all resource groups
az group list --output table

az databricks workspace delete \
  --resource-group <resource_group_name> \
  --name <databricks-workspace_name> \
  --yes

# delete resource group without waiting for the operation to complete
az group delete --name "<resource_group_name>" --yes --no-wait

# delete resource group but wait for the operation to complete
az group delete --name "<resource_group_name>" --yes