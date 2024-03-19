terraform {
  required_providers {
    cockroach-extra = {
      source = "registry.terraform.io/nrfcloud/cockroach-extra"
    }
  }
}

resource "cockroach-extra_external_connection" "test_kafka" {
  cluster_id = "7e200f32-5273-47bc-abcd-0dc248586b32"
  name       = "test_kafka"
  uri        = "kafka://cluster-0.kafka.svc:9092"
}

resource "cockroach-extra_changefeed" "test_changefeed" {
  cluster_id = "7e200f32-5273-47bc-abcd-0dc248586b32"
  sink_uri   = cockroach-extra_external_connection.test_kafka.ref_uri
  target     = ["defaultdb.public.test"]
  options = {
    full_table_name = true
    on_error        = "fail"
    metrics_label   = "test"
    format          = "json"
  }
}

provider "cockroach-extra" {
  #  api_key = "<YOU API KEY>"
}