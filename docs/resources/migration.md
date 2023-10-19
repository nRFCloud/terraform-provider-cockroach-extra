---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "cockroach-extra_migration Resource - terraform-provider-cockroach-extra"
subcategory: ""
description: |-
  
---

# cockroach-extra_migration (Resource)





<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `cluster_id` (String) Cluster ID
- `database` (String) Database to apply the migration to
- `destroy_mode` (String) What to do when the resource is destroyed. 'noop' will do nothing, 'drop' will drop the database, 'down' will run all down migrations
- `migrations_directory` (String) Path to the migrations directory
- `version` (Number) What migration version should be applied. This should be the migration id number (integer prefix of the filename).

### Read-Only

- `id` (String) The ID of this resource.