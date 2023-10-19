terraform {
  required_providers {
    cockroach-extra = {
      source = "registry.terraform.io/nrfcloud/cockroach-extra"
    }
  }
}

resource "cockroach-extra_migration" "cool-migration" {
  cluster_id           = "29fd13f0-32a0-42e6-bf51-e7c3c5bbd03e"
  migrations_url = "file://migrations"
  destroy_mode         = "drop"
  database             = "migration-test"
  version              = 20231019210913
}

provider "cockroach-extra" {
  #  api_key = "<YOU API KEY>"
}