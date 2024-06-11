package resources

import (
	"context"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/jackc/pgx"
	"github.com/lib/pq"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
)

var _ resource.Resource = &ChangefeedResource{}

//var _ resource.ResourceWithImportState = &ChangefeedResource{}

func NewChangefeedResource() resource.Resource {
	return &ChangefeedResource{}
}

type ChangefeedResource struct {
	client *ccloud.CcloudClient
}

type ChangefeedResourceModel struct {
	ClusterId           types.String `tfsdk:"cluster_id"`
	Id                  types.String `tfsdk:"id"`
	JobId               types.Int64  `tfsdk:"job_id"`
	Target              types.List   `tfsdk:"target"`
	Select              types.String `tfsdk:"select"`
	SinkUri             types.String `tfsdk:"sink_uri"`
	InitialScanOnUpdate types.Bool   `tfsdk:"initial_scan_on_update"`
	Status              types.String `tfsdk:"status"`
	PersistentCursor    types.String `tfsdk:"persistent_cursor"`
	Options             struct {
		AvroSchemaPrefix             types.String `tfsdk:"avro_schema_prefix"`
		Compression                  types.String `tfsdk:"compression"`
		ConfluentSchemaRegistry      types.String `tfsdk:"confluent_schema_registry"`
		Cursor                       types.String `tfsdk:"cursor"`
		Diff                         types.Bool   `tfsdk:"diff"`
		EndTime                      types.String `tfsdk:"end_time"`
		Envelope                     types.String `tfsdk:"envelope"`
		ExecutionLocality            types.String `tfsdk:"execution_locality"`
		Format                       types.String `tfsdk:"format"`
		FullTableName                types.Bool   `tfsdk:"full_table_name"`
		GcProtectExpiresAfter        types.String `tfsdk:"gc_protect_expires_after"`
		InitialScan                  types.String `tfsdk:"initial_scan"`
		KafkaSinkConfig              types.String `tfsdk:"kafka_sink_config"`
		KeyColumn                    types.String `tfsdk:"key_column"`
		KeyInValue                   types.Bool   `tfsdk:"key_in_value"`
		LaggingRangesThreshold       types.String `tfsdk:"lagging_ranges_threshold"`
		LaggingRangesPollingInterval types.String `tfsdk:"lagging_ranges_polling_interval"`
		MetricsLabel                 types.String `tfsdk:"metrics_label"`
		MinCheckpointFrequency       types.String `tfsdk:"min_checkpoint_frequency"`
		MvccTimestamp                types.Bool   `tfsdk:"mvcc_timestamp"`
		OnError                      types.String `tfsdk:"on_error"`
		ProtectDataFromGcOnPause     types.Bool   `tfsdk:"protect_data_from_gc_on_pause"`
		Resolved                     types.String `tfsdk:"resolved"`
		SchemaChangeEvents           types.String `tfsdk:"schema_change_events"`
		SchemaChangePolicy           types.String `tfsdk:"schema_change_policy"`
		SplitColumnFamilies          types.Bool   `tfsdk:"split_column_families"`
		TopicInValue                 types.Bool   `tfsdk:"topic_in_value"`
		Unordered                    types.Bool   `tfsdk:"unordered"`
		Updated                      types.Bool   `tfsdk:"updated"`
		VirtualColumns               types.String `tfsdk:"virtual_columns"`
		WebhookAuthHeader            types.String `tfsdk:"webhook_auth_header"`
		WebhookSinkConfig            types.String `tfsdk:"webhook_sink_config"`
	} `tfsdk:"options"`
}

func (r *ChangefeedResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_changefeed"
}

func getChangefeedId(clusterId string, jobId int64) string {
	return fmt.Sprintf("changefeed|%s|%d", clusterId, jobId)
}

func (r *ChangefeedResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Cluster setting",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				Computed: true,
				Optional: false,
				Required: false,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"persistent_cursor": schema.StringAttribute{
				Optional: true,
				Required: false,
				Validators: []validator.String{
					stringvalidator.ConflictsWith(path.MatchRoot("options").AtName("cursor")),
				},
				MarkdownDescription: `
Id of a persistent cursor resource.
If set, the changefeed will use this cursor to resume from.
`,
			},

			"job_id": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "Changefeed job ID",
				Optional:            false,
				Required:            false,
				PlanModifiers: []planmodifier.Int64{
					int64planmodifier.UseStateForUnknown(),
				},
			},
			"target": schema.ListAttribute{
				ElementType:         types.StringType,
				MarkdownDescription: "List of tables that the changefeed will watch",
				Required:            false,
				Optional:            true,
				Validators: []validator.List{
					listvalidator.ExactlyOneOf(path.MatchRoot("select")),
					listvalidator.UniqueValues(),
					listvalidator.ValueStringsAre(
						stringvalidator.RegexMatches(
							regexp.MustCompile(`^[a-zA-Z0-9_]+?\.[a-zA-Z0-9_]+?\.[a-zA-Z0-9_]+?$`),
							"Table names must be fully qualified",
						),
					),
				},
			},
			"select": schema.StringAttribute{
				MarkdownDescription: `
SQL query that the changefeed will use to filter the watched tables.
**Note:** Using this option will prevent updating any properties of the changefeed.,
`,
				Required: false,
				Optional: true,
				Validators: []validator.String{
					stringvalidator.ExactlyOneOf(path.MatchRoot("target")),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"sink_uri": schema.StringAttribute{
				MarkdownDescription: "URI of the sink where the changefeed will send the changes",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplaceIf(func(ctx context.Context, req planmodifier.StringRequest, resp *stringplanmodifier.RequiresReplaceIfFuncResponse) {
						var data ChangefeedResourceModel
						resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
						resp.RequiresReplace = !data.Select.IsNull()
					}, "", ""),
				},
			},
			"status": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Status of the changefeed job",
				Optional:            false,
				Required:            false,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplaceIf(func(ctx context.Context, req planmodifier.StringRequest, resp *stringplanmodifier.RequiresReplaceIfFuncResponse) {
						resp.RequiresReplace = !(req.StateValue.ValueString() == "paused" || req.StateValue.ValueString() == "running")
					},
						"", ""),
				},
				Validators: []validator.String{
					stringvalidator.OneOf("running", "paused", "canceling", "canceled", "failed", "succeeded"),
				},
			},
			"options": schema.SingleNestedAttribute{
				Optional: true,
				Required: false,
				PlanModifiers: []planmodifier.Object{
					objectplanmodifier.RequiresReplaceIf(func(ctx context.Context, req planmodifier.ObjectRequest, resp *objectplanmodifier.RequiresReplaceIfFuncResponse) {
						var data ChangefeedResourceModel
						resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
						resp.RequiresReplace = !data.Select.IsNull()
					},
						"Changefeeds with queries cannot be updated",
						"Changing"),
				},
				MarkdownDescription: `
Options for the changefeed.
Documentation for the options can be found [here](https://www.cockroachlabs.com/docs/stable/create-changefeed#options)
`,
				Attributes: map[string]schema.Attribute{
					"avro_schema_prefix": schema.StringAttribute{
						MarkdownDescription: "Avro schema prefix",
						Required:            false,
						Optional:            true,
					},
					"compression": schema.StringAttribute{
						MarkdownDescription: "Compression",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("gzip", "zstd"),
						},
					},
					"confluent_schema_registry": schema.StringAttribute{
						MarkdownDescription: "Confluent schema registry address for avro",
						Required:            false,
						Optional:            true,
					},
					"cursor": schema.StringAttribute{
						MarkdownDescription: "Cursor",
						Required:            false,
						Optional:            false,
						Computed:            true,
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.UseStateForUnknown(),
						},
					},
					"diff": schema.BoolAttribute{
						MarkdownDescription: "Diff",
						Required:            false,
						Optional:            true,
					},
					"end_time": schema.StringAttribute{
						MarkdownDescription: "End time",
						Required:            false,
						Optional:            true,
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.RequiresReplace(),
						},
					},
					"envelope": schema.StringAttribute{
						MarkdownDescription: "Envelope",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("wrapped", "bare", "key_only", "row"),
						},
					},
					"execution_locality": schema.StringAttribute{
						MarkdownDescription: "Execution locality",
						Required:            false,
						Optional:            true,
					},
					"format": schema.StringAttribute{
						MarkdownDescription: "Format",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("json", "avro", "csv", "parquet"),
						},
					},
					"full_table_name": schema.BoolAttribute{
						MarkdownDescription: "Full table name",
						Required:            false,
						Optional:            true,
						PlanModifiers: []planmodifier.Bool{
							boolplanmodifier.RequiresReplace(),
						},
					},
					"gc_protect_expires_after": schema.StringAttribute{
						MarkdownDescription: "GC protect expires after",
						Required:            false,
						Optional:            true,
					},
					"initial_scan": schema.StringAttribute{
						MarkdownDescription: "Initial scan",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("yes", "no", "only"),
						},
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.RequiresReplace(),
						},
					},
					"kafka_sink_config": schema.StringAttribute{
						MarkdownDescription: "Kafka sink config",
						Required:            false,
						Optional:            true,
					},
					"key_column": schema.StringAttribute{
						MarkdownDescription: "Key column",
						Required:            false,
						Optional:            true,
					},
					"key_in_value": schema.BoolAttribute{
						MarkdownDescription: "Key in value",
						Required:            false,
						Optional:            true,
					},
					"lagging_ranges_threshold": schema.StringAttribute{
						MarkdownDescription: "Lagging ranges threshold",
						Required:            false,
						Optional:            true,
					},
					"lagging_ranges_polling_interval": schema.StringAttribute{
						MarkdownDescription: "Lagging ranges polling interval",
						Required:            false,
						Optional:            true,
					},
					"metrics_label": schema.StringAttribute{
						MarkdownDescription: "Metrics label",
						Required:            false,
						Optional:            true,
					},
					"min_checkpoint_frequency": schema.StringAttribute{
						MarkdownDescription: "Min checkpoint frequency",
						Required:            false,
						Optional:            true,
					},
					"mvcc_timestamp": schema.BoolAttribute{
						MarkdownDescription: "MVCC timestamp",
						Required:            false,
						Optional:            true,
					},
					"on_error": schema.StringAttribute{
						MarkdownDescription: "On error",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("pause", "fail"),
						},
					},
					"protect_data_from_gc_on_pause": schema.BoolAttribute{
						MarkdownDescription: "Protect data from GC on pause",
						Required:            false,
						Optional:            true,
					},
					"resolved": schema.StringAttribute{
						MarkdownDescription: "Resolved",
						Required:            false,
						Optional:            true,
					},
					"schema_change_events": schema.StringAttribute{
						MarkdownDescription: "Schema change events",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("default", "column_changes"),
						},
					},
					"schema_change_policy": schema.StringAttribute{
						MarkdownDescription: "Schema change policy",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("backfill", "no_backfill", "stop"),
						},
					},
					"split_column_families": schema.BoolAttribute{
						MarkdownDescription: "Split column families",
						Required:            false,
						Optional:            true,
					},
					"topic_in_value": schema.BoolAttribute{
						MarkdownDescription: "Topic in value",
						Required:            false,
						Optional:            true,
					},
					"unordered": schema.BoolAttribute{
						MarkdownDescription: "Unordered",
						Required:            false,
						Optional:            true,
					},
					"updated": schema.BoolAttribute{
						MarkdownDescription: "Updated",
						Required:            false,
						Optional:            true,
					},
					"virtual_columns": schema.StringAttribute{
						MarkdownDescription: "Virtual columns",
						Required:            false,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("null", "omitted"),
						},
					},
					"webhook_auth_header": schema.StringAttribute{
						MarkdownDescription: "Webhook auth header",
						Required:            false,
						Optional:            true,
					},
					"webhook_sink_config": schema.StringAttribute{
						MarkdownDescription: "Webhook sink config",
						Required:            false,
						Optional:            true,
					},
				},
			},
			"initial_scan_on_update": schema.BoolAttribute{
				MarkdownDescription: "Initial scan on update",
				Required:            false,
				Optional:            true,
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}

func (r *ChangefeedResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*ccloud.CcloudClient)

	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type",
			fmt.Sprintf("Expected *CcloudClient, got: %T. Please report this issue to the provider developers.", req.ProviderData))
		return
	}

	r.client = client
}

func (r *ChangefeedResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data ChangefeedResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	var cursorKey string
	// Check if the persistent_cursor is set
	if !data.PersistentCursor.IsNull() {
		_, cursorKey = ParseCursorId(data.PersistentCursor.ValueString())
		cursorValue, err := GetCursor(ctx, r.client, data.ClusterId.ValueString(), cursorKey)

		if err != nil {
			resp.Diagnostics.AddError("Unable to get persistent cursor", err.Error())
			return
		}

		if cursorValue.OffsetCursor != nil {
			data.Options.Cursor = types.StringValue(*cursorValue.OffsetCursor)
		}

		if cursorValue.InUse {
			resp.Diagnostics.AddError("Persistent cursor is in use", "The persistent cursor is currently in use by another job")
			return
		}
	}

	// Iterate through the keys of the options struct and build a string of options ex: SET option1 = value1, option2 = value2
	options := []string{}
	optionsObjVal := reflect.ValueOf(data.Options)
	for i := 0; i < optionsObjVal.NumField(); i++ {
		value := optionsObjVal.Field(i).Interface()
		// get tfsdk tag
		tag := optionsObjVal.Type().Field(i).Tag.Get("tfsdk")

		// Check if the value is a bool or string
		switch v := value.(type) {
		case types.Bool:
			if !v.IsNull() {
				options = append(options, tag)
			}
		case types.String:
			if !v.IsNull() {
				// If the value is a string, sanitize it and add it to the options string
				options = append(options, fmt.Sprintf("%s=%s", tag, pq.QuoteLiteral(v.ValueString())))
			}
		}
	}
	optionsString := ""
	if len(options) > 0 {
		optionsString = fmt.Sprintf("WITH %s", strings.Join(options, ", "))
	}

	query := ""

	if !data.Target.IsNull() {
		targetStringList := make([]string, len(data.Target.Elements()))
		data.Target.ElementsAs(ctx, &targetStringList, false)
		targetString := strings.Join(targetStringList, ", ")

		query = fmt.Sprintf("CREATE CHANGEFEED FOR %s INTO '%s' %s", targetString, data.SinkUri.ValueString(), optionsString)
	}

	if !data.Select.IsNull() {
		query = fmt.Sprintf("CREATE CHANGEFEED INTO '%s' %s AS %s", data.SinkUri.ValueString(), optionsString, data.Select.ValueString())

	}

	tflog.Info(ctx, fmt.Sprintf("Creating changefeed with query: %s", query))

	jobId, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*int64, error) {
		var jobId int64
		err := db.QueryRow(query).Scan(&jobId)

		if err != nil {
			return nil, err
		}

		// wait for job to be running
		err = waitForJobStatus(db, jobId, "running")
		return &jobId, err
	})

	if err != nil {
		// if the error contains "after replica gc threshold", then the cursor is too old
		if strings.Contains(err.Error(), "after replica GC threshold") {
			resp.Diagnostics.AddError("Unable to create changefeed job with expired cursor", err.Error())
			return
		}
		resp.Diagnostics.AddError("Unable to create changefeed job", err.Error())
		return
	}

	data.JobId = types.Int64Value(*jobId)
	data.Id = types.StringValue(getChangefeedId(data.ClusterId.ValueString(), *jobId))
	data.Status = types.StringValue("running")

	err = UpdateCursorJobId(ctx, r.client, data.ClusterId.ValueString(), cursorKey, jobId)
	if err != nil {
		resp.Diagnostics.AddError("Unable to update cursor job ID", err.Error())
		_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
			_, err := db.Exec(fmt.Sprintf("CANCEL JOB %d", data.JobId.ValueInt64()))

			if err != nil {
				return nil, err
			}

			err = waitForJobStatus(db, data.JobId.ValueInt64(), "canceled")

			return nil, err
		})
		if err != nil {
			resp.Diagnostics.AddError("Unable to cancel job", err.Error())
		}
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func removeQuotes(s string) string {
	return strings.Trim(strings.Trim(s, "\""), "'")
}

func (r *ChangefeedResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data ChangefeedResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Info(ctx, fmt.Sprintf("Reading changefeed with job ID: %d", data.JobId.ValueInt64()))

	changefeedInfo, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*struct {
		uri            string
		statement      string
		status         string
		fullTableNames []string
		highWaterMark  float64
	}, error) {
		var statement string
		var status string
		var uri string
		var fullTableNames []string
		var highWaterMark float64
		err := db.QueryRow(fmt.Sprintf("SELECT description, status, sink_uri, full_table_names, high_water_timestamp from [SHOW CHANGEFEED JOB %d]", data.JobId.ValueInt64())).
			Scan(&statement, &status, &uri, &fullTableNames, &highWaterMark)
		if err != nil {
			return nil, err
		}

		result := struct {
			uri            string
			statement      string
			status         string
			fullTableNames []string
			highWaterMark  float64
		}{
			uri:            uri,
			statement:      statement,
			status:         status,
			fullTableNames: fullTableNames,
			highWaterMark:  highWaterMark,
		}
		return &result, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to read changefeed job", err.Error())
		return
	}

	if changefeedInfo.status == "failed" || changefeedInfo.status == "canceled" || changefeedInfo.status == "canceling" {
		resp.Diagnostics.AddError("Changefeed job in unexpected state", fmt.Sprintf("Changefeed job is in state: %s", changefeedInfo.status))
		return
	}

	tflog.Info(ctx, fmt.Sprintf("Changefeed statement: %s", changefeedInfo.statement))

	// Parse the statement to get the target and select
	// ex: CREATE CHANGEFEED FOR table1, table2 INTO 'sink' WITH option1 = value1, option2
	// ex: CREATE CHANGEFEED INTO 'sink' WITH option1 = value1, option2 = value2 AS SELECT * FROM table1
	optionsRaw := ""

	// Match prefix case-insensitive to detect the type of changefeed
	if strings.HasPrefix(changefeedInfo.statement, "CREATE CHANGEFEED FOR") {
		re := regexp.MustCompile(`(?i)create changefeed for[\s\S]+?into[\s\S]+?(?:with([\s\S]+?))?$`)
		match := re.FindStringSubmatch(changefeedInfo.statement)
		if len(match) != 2 {
			resp.Diagnostics.AddError("Unable to parse changefeed statement", "Unable to parse changefeed statement")
			return
		}
		optionsRaw = strings.TrimSpace(match[1])
		targets := make([]attr.Value, len(changefeedInfo.fullTableNames))
		for i, target := range changefeedInfo.fullTableNames {
			targets[i] = types.StringValue(target)
		}
		data.Target, _ = types.ListValue(types.StringType, targets)
	}

	if strings.HasPrefix(changefeedInfo.statement, "CREATE CHANGEFEED INTO") {
		// Parse the statement with regex to extract the sink URI and options
		re := regexp.MustCompile(`(?i)create changefeed into[\s\S]+?(?:with([\s\S]+?))?as([\s\S]+?)$`)
		match := re.FindStringSubmatch(changefeedInfo.statement)
		if len(match) != 3 {
			resp.Diagnostics.AddError("Unable to parse changefeed statement", "Unable to parse changefeed statement")
			return
		}
		optionsRaw = strings.TrimSpace(match[1])
		//data.Select = types.StringValue(strings.TrimSpace(match[2]))
	}

	if !CompareURLs(data.SinkUri.ValueString(), changefeedInfo.uri) {
		data.SinkUri = types.StringValue(changefeedInfo.uri)
	}

	// Parse the options
	options := strings.Split(strings.Trim(strings.Trim(optionsRaw, "("), ")"), ",")
	for _, option := range options {
		var key string
		var value string

		// Split the option into key and value
		optionParts := strings.SplitN(option, "=", 2)
		key = strings.TrimSpace(optionParts[0])
		if len(optionParts) > 1 {
			value = removeQuotes(strings.TrimSpace(optionParts[1]))
		}

		// Set the value of the option in the data struct
		switch key {
		case "avro_schema_prefix":
			data.Options.AvroSchemaPrefix = types.StringValue(value)
		case "compression":
			data.Options.Compression = types.StringValue(value)
		case "confluent_schema_registry":
			data.Options.ConfluentSchemaRegistry = types.StringValue(value)
		case "cursor":
			data.Options.Cursor = types.StringValue(value)
		case "diff":
			data.Options.Diff = types.BoolValue(true)
		case "end_time":
			data.Options.EndTime = types.StringValue(value)
		case "envelope":
			data.Options.Envelope = types.StringValue(value)
		case "execution_locality":
			data.Options.ExecutionLocality = types.StringValue(value)
		case "format":
			data.Options.Format = types.StringValue(value)
		case "full_table_name":
			data.Options.FullTableName = types.BoolValue(true)
		case "gc_protect_expires_after":
			data.Options.GcProtectExpiresAfter = types.StringValue(value)
		case "initial_scan":
			data.Options.InitialScan = types.StringValue(value)
		case "kafka_sink_config":
			data.Options.KafkaSinkConfig = types.StringValue(value)
		case "key_column":
			data.Options.KeyColumn = types.StringValue(value)
		case "key_in_value":
			data.Options.KeyInValue = types.BoolValue(true)
		case "lagging_ranges_threshold":
			data.Options.LaggingRangesThreshold = types.StringValue(value)
		case "lagging_ranges_polling_interval":
			data.Options.LaggingRangesPollingInterval = types.StringValue(value)
		case "metrics_label":
			data.Options.MetricsLabel = types.StringValue(value)
		case "min_checkpoint_frequency":
			data.Options.MinCheckpointFrequency = types.StringValue(value)
		case "mvcc_timestamp":
			data.Options.MvccTimestamp = types.BoolValue(true)
		case "on_error":
			data.Options.OnError = types.StringValue(value)
		case "protect_data_from_gc_on_pause":
			data.Options.ProtectDataFromGcOnPause = types.BoolValue(true)
		case "resolved":
			data.Options.Resolved = types.StringValue(value)
		case "schema_change_events":
			data.Options.SchemaChangeEvents = types.StringValue(value)
		case "schema_change_policy":
			data.Options.SchemaChangePolicy = types.StringValue(value)
		case "split_column_families":
			data.Options.SplitColumnFamilies = types.BoolValue(true)
		case "topic_in_value":
			data.Options.TopicInValue = types.BoolValue(true)
		case "unordered":
			data.Options.Unordered = types.BoolValue(true)
		case "updated":
			data.Options.Updated = types.BoolValue(true)
		case "virtual_columns":
			data.Options.VirtualColumns = types.StringValue(value)
		case "webhook_auth_header":
			data.Options.WebhookAuthHeader = types.StringValue(value)
		case "webhook_sink_config":
			data.Options.WebhookSinkConfig = types.StringValue(value)
		}
	}

	data.Status = types.StringValue(changefeedInfo.status)
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func stringListDelta(source []string, target []string) (added []string, removed []string) {
	sourceMap := make(map[string]bool)
	for _, s := range source {
		sourceMap[s] = true
	}
	for _, t := range target {
		if _, ok := sourceMap[t]; !ok {
			added = append(added, t)
		}
	}
	targetMap := make(map[string]bool)
	for _, t := range target {
		targetMap[t] = true
	}
	for _, s := range source {
		if _, ok := targetMap[s]; !ok {
			removed = append(removed, s)
		}
	}
	return
}

func (r *ChangefeedResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	bannedOptionUpdates := []string{
		"end_time",
		"full_table_name",
		"initial_scan",
	}

	var data ChangefeedResourceModel
	var stateData ChangefeedResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &stateData)...)

	data.Id = stateData.Id

	if resp.Diagnostics.HasError() {
		return
	}

	stateStatus := stateData.Status.ValueString()

	if stateStatus == "canceled" || stateStatus == "failed" || stateStatus == "succeeded" || stateStatus == "canceling" {
		resp.Diagnostics.AddError("Unable to update changefeed", "Changefeed is not running")
		return
	}

	if !stateData.Select.IsNull() {
		resp.Diagnostics.AddError("Unable to update changefeed", "Cannot update changefeed with select statement")
		return
	}

	if !data.PersistentCursor.Equal(stateData.PersistentCursor) {
		var err error
		if data.PersistentCursor.IsNull() {
			_, cursorKey := ParseCursorId(stateData.PersistentCursor.ValueString())
			err = UpdateCursorJobId(ctx, r.client, data.ClusterId.ValueString(), cursorKey, nil)
		} else {
			_, cursorKey := ParseCursorId(data.PersistentCursor.ValueString())
			err = UpdateCursorJobId(ctx, r.client, data.ClusterId.ValueString(), cursorKey, data.JobId.ValueInt64Pointer())
		}

		if err != nil {
			resp.Diagnostics.AddError("Unable to update cursor job ID", err.Error())
			return

		}
	}

	// Build the options string
	var setList []string
	var unsetList []string
	optionsObjVal := reflect.ValueOf(data.Options)
	stateOptionsVal := reflect.ValueOf(stateData.Options)
	for i := 0; i < optionsObjVal.NumField(); i++ {
		value, ok := optionsObjVal.Field(i).Interface().(attr.Value)
		if !ok {
			resp.Diagnostics.AddError("Unable to update changefeed", "Unable to parse options")
			return
		}

		stateValue, ok := stateOptionsVal.Field(i).Interface().(attr.Value)
		if !ok {
			resp.Diagnostics.AddError("Unable to update changefeed", "Unable to parse options")
			return
		}

		// get tfsdk tag
		tag := optionsObjVal.Type().Field(i).Tag.Get("tfsdk")

		if slices.Contains(bannedOptionUpdates, tag) {
			if !value.Equal(stateValue) {
				resp.Diagnostics.AddError("Unable to update changefeed", fmt.Sprintf("Cannot update %s option. old: %s new: %s", tag, stateValue.String(), value.String()))
				return
			}
			continue
		}

		if tag == "cursor" {
			data.Options.Cursor = stateData.Options.Cursor
			continue
		}

		if value.Equal(stateValue) {
			continue
		}

		if value.IsNull() {
			unsetList = append(unsetList, tag)
		} else {
			// Check if the value is a bool or string
			switch v := value.(type) {
			case types.Bool:
				setList = append(setList, tag)
			case types.String:
				setList = append(setList, fmt.Sprintf("%s=%s", tag, pq.QuoteLiteral(v.ValueString())))
			}
		}
	}

	if !data.SinkUri.Equal(stateData.SinkUri) {
		setList = append(setList, fmt.Sprintf("sink='%s'", data.SinkUri.ValueString()))
	}

	var setStatement string
	var unsetStatement string

	if len(setList) > 0 {
		setStatement = fmt.Sprintf("SET %s", strings.Join(setList, ", "))
	}

	if len(unsetList) > 0 {
		unsetStatement = fmt.Sprintf("UNSET %s", strings.Join(unsetList, ", "))
	}

	// Get the added and removed targets
	targets := make([]string, len(data.Target.Elements()))
	data.Target.ElementsAs(ctx, &targets, false)
	stateTargets := make([]string, len(stateData.Target.Elements()))
	stateData.Target.ElementsAs(ctx, &stateTargets, false)
	addedTargets, removedTargets := stringListDelta(stateTargets, targets)

	addTargetStatement := ""
	removeTargetStatement := ""

	if len(addedTargets) > 0 {
		addTargetStatement = fmt.Sprintf("ADD %s", strings.Join(addedTargets, ", "))
		if data.InitialScanOnUpdate.IsNull() || !data.InitialScanOnUpdate.ValueBool() {
			addTargetStatement += " WITH no_initial_scan"
		} else {
			addTargetStatement += " WITH initial_scan"
		}
	}

	if len(removedTargets) > 0 {
		removeTargetStatement = fmt.Sprintf("DROP %s", strings.Join(removedTargets, ", "))
	}

	query := fmt.Sprintf("ALTER CHANGEFEED %d %s %s %s %s", data.JobId.ValueInt64(), addTargetStatement, removeTargetStatement, setStatement, unsetStatement)

	tflog.Info(ctx, fmt.Sprintf("Updating changefeed with query: %s", query))

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("PAUSE JOB %d WITH REASON='Terraform Update'", data.JobId.ValueInt64()))
		if err != nil {
			return nil, err
		}
		// Wait until the job is paused
		err = waitForJobStatus(db, data.JobId.ValueInt64(), "paused")
		if err != nil {
			return nil, err
		}

		_, err = db.Exec(query)
		if err != nil {
			return nil, err
		}
		_, err = db.Exec(fmt.Sprintf("RESUME JOB %d", data.JobId.ValueInt64()))
		if err != nil {
			return nil, err

		}
		// Wait until the job is running
		err = waitForJobStatus(db, data.JobId.ValueInt64(), "running")

		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to update changefeed", err.Error())
		return
	}

	data.Status = types.StringValue("running")

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func waitForJobStatus(db *pgx.ConnPool, jobId int64, status string) error {
	return retry.Do(
		func() error {
			var jobStatus string
			err := db.QueryRow(fmt.Sprintf("SELECT status FROM [SHOW CHANGEFEED JOB %d]", jobId)).Scan(&jobStatus)
			if err != nil {
				return err
			}
			if jobStatus != status {
				return fmt.Errorf("job status never reached %s current status: %s", status, jobStatus)
			}
			return nil
		},
		retry.Attempts(20),
		retry.Delay(time.Second*2),
	)
}

func (r *ChangefeedResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data ChangefeedResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	status := data.Status.ValueString()

	if status == "running" || status == "paused" {

		_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
			_, err := db.Exec(fmt.Sprintf("CANCEL JOB %d", data.JobId.ValueInt64()))

			if err != nil {
				return nil, err
			}

			err = waitForJobStatus(db, data.JobId.ValueInt64(), "canceled")

			return nil, err
		})

		if err != nil {
			resp.Diagnostics.AddError("Unable to cancel job", err.Error())
			return
		}

	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
