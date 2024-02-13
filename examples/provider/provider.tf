terraform {
  required_providers {
    cockroach-extra = {
      source = "registry.terraform.io/nrfcloud/cockroach-extra"
    }
  }
}

resource "cockroach-extra_cluster_setting" "set_issuers" {
  cluster_id    = "a09677a4-5497-4c7a-af42-b8932dafb3a3"
  setting_name  = "server.jwt_authentication.issuers"
  setting_value = "other"
}

provider "cockroach-extra" {
  #  api_key = "<YOU API KEY>"
}