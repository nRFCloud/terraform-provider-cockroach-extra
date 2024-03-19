---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "cockroach-extra_changefeed Resource - terraform-provider-cockroach-extra"
subcategory: ""
description: |-
  Cluster setting
---

# cockroach-extra_changefeed (Resource)

Cluster setting



<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `cluster_id` (String) Cluster ID
- `sink_uri` (String) URI of the sink where the changefeed will send the changes

### Optional

- `initial_scan_on_update` (Boolean) Initial scan on update
- `options` (Attributes) Options for the changefeed.
Documentation for the options can be found [here](https://www.cockroachlabs.com/docs/stable/create-changefeed#options) (see [below for nested schema](#nestedatt--options))
- `select` (String) SQL query that the changefeed will use to filter the watched tables.
**Note:** Using this option will prevent updating any properties of the changefeed.,
- `target` (List of String) List of tables that the changefeed will watch

### Read-Only

- `id` (Number) Changefeed job ID

<a id="nestedatt--options"></a>
### Nested Schema for `options`

Optional:

- `avro_schema_prefix` (String) Avro schema prefix
- `compression` (String) Compression
- `confluent_schema_registry` (String) Confluent schema registry address for avro
- `cursor` (String) Cursor
- `diff` (Boolean) Diff
- `end_time` (String) End time
- `envelope` (String) Envelope
- `execution_locality` (String) Execution locality
- `format` (String) Format
- `full_table_name` (Boolean) Full table name
- `gc_protect_expires_after` (String) GC protect expires after
- `initial_scan` (String) Initial scan
- `kafka_sink_config` (String) Kafka sink config
- `key_column` (String) Key column
- `key_in_value` (Boolean) Key in value
- `lagging_ranges_polling_interval` (String) Lagging ranges polling interval
- `lagging_ranges_threshold` (String) Lagging ranges threshold
- `metrics_label` (String) Metrics label
- `min_checkpoint_frequency` (String) Min checkpoint frequency
- `mvcc_timestamp` (String) MVCC timestamp
- `on_error` (String) On error
- `protect_data_from_gc_on_pause` (Boolean) Protect data from GC on pause
- `resolved` (String) Resolved
- `schema_change_events` (String) Schema change events
- `schema_change_policy` (String) Schema change policy
- `split_column_families` (Boolean) Split column families
- `topic_in_value` (Boolean) Topic in value
- `unordered` (Boolean) Unordered
- `updated` (Boolean) Updated
- `virtual_columns` (String) Virtual columns
- `webhook_auth_header` (String) Webhook auth header
- `webhook_sink_config` (String) Webhook sink config