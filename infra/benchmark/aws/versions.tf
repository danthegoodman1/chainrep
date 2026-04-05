terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      app        = "chainrep-bench"
      managed_by = "terraform"
      run_id     = var.run_id
    }
  }
}
