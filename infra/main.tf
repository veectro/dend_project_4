terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.74"
    }
  }

  cloud {
    organization = "veectro"
    workspaces {
      name = "udacity-dend-4"
    }
  }

  required_version = ">= 0.14.9"
}


provider "aws" {
  profile = "udacity"
  region  = "us-west-2"
}
