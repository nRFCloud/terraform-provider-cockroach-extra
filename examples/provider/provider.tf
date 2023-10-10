terraform {
  required_providers {
    cockroach-extra = {
      source = "registry.terraform.io/nrfcloud/cockroach-extra"
    }
  }
}

provider "cockroach-extra" {
  api_key = "<YOU API KEY>"
}