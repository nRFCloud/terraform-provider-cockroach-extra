resource "cockroach-extra_migration" "cool-migration" {
  cluster_id           = "29fd13f0-32a0-42e6-bf51-e7c3c5bbd03e"
  migrations_directory = "/Users/john/workspace/terraform-provider-cockroach-extra/examples/provider/migrations"
  destroy_mode         = "drop"
  database             = "defaultdb"
  version              = "20231019025716"
}