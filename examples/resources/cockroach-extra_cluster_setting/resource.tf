resource "cockroach-extra_cluster_setting" "set_issuers" {
  cluster_id    = "29fd13f0-32a0-42e6-bf51-e7c3c5bbd03e"
  setting_name  = "server.jwt_authentication.issuers"
  setting_value = "other"
}