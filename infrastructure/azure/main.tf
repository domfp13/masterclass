terraform {
  cloud {
    organization = "infoinsights"
    workspaces {
      name = "azure-masterclass"
    }    
  }

  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "4.28.0"
    }
  }
}

provider "azurerm" {
  # Configuration options
  features {

  }
}