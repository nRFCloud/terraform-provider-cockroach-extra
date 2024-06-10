terraform {
  required_providers {
    cockroach-extra = {
      source = "registry.terraform.io/nrfcloud/cockroach-extra"
    }
  }
}

resource "cockroach-extra_external_connection" "test_kafka" {
  cluster_id = "a32a668e-14bf-11ef-8257-765575d27eeb"
  name       = "test_kafka"
  uri        = "kafka://cluster-0.kafka.svc:9092"
}

resource "cockroach-extra_persistent_cursor" "test_cursor" {
  cluster_id = "a32a668e-14bf-11ef-8257-765575d27eeb"
  key        = "test_cursor2"
}

resource "cockroach-extra_changefeed" "test_changefeed" {
  cluster_id        = "a32a668e-14bf-11ef-8257-765575d27eeb"
  sink_uri          = cockroach-extra_external_connection.test_kafka.ref_uri
  persistent_cursor = cockroach-extra_persistent_cursor.test_cursor.id
  select            = "SELECT * FROM defaultdb.public.test"
  #   target     = ["defaultdb.public.test"]
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