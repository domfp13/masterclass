terraform {
  cloud {
    organization = "infoinsights"

    workspaces {
      name = "aws-masterclass"
    }
  }
  required_providers {
    random = {
      source  = "hashicorp/random"
      version = "3.5"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "5.39.0"
    }
  }
}

# Specify the AWS provider
provider "aws" {
  region = "us-east-1"
}
