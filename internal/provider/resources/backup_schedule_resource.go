package resources

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework-validators/boolvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/jackc/pgx"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
	"strings"
)

type BackupScheduleResource struct {
	client *ccloud.CcloudClient
}

type BackupScheduleResourceModel struct {
	ClusterId            types.String `tfsdk:"cluster_id"`
	Label                types.String `tfsdk:"label"`
	Id                   types.String `tfsdk:"id"`
	Tables               types.List   `tfsdk:"tables"`
	FullClusterBackup    types.Bool   `tfsdk:"full_cluster_backup"`
	Databases            types.List   `tfsdk:"databases"`
	Location             types.String `tfsdk:"location"`
	Schedule             types.String `tfsdk:"recurring"`
	FullBackupSchedule   types.String `tfsdk:"full_backup_schedule"`
	RevisionHistory      types.Bool   `tfsdk:"revision_history"`
	EncryptionPassphrase types.String `tfsdk:"encryption_passphrase"`
	//ExecutionLocality     types.Map    `tfsdk:"execution_locality"`
	KmsUri                types.String `tfsdk:"kms_uri"`
	IncrementalLocation   types.String `tfsdk:"incremental_location"`
	FirstRun              types.String `tfsdk:"first_run"`
	OnExecutionFailure    types.String `tfsdk:"on_execution_failure"`
	OnPreviousRunning     types.String `tfsdk:"on_previous_running"`
	IgnoreExistingBackups types.Bool   `tfsdk:"ignore_existing_backups"`
}

func (r *BackupScheduleResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_backup_schedule"
}

func (r *BackupScheduleResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Backup schedule",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
			},
			"label": schema.StringAttribute{
				MarkdownDescription: "Name for the backup schedule",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"full_cluster_backup": schema.BoolAttribute{
				MarkdownDescription: "Backup the entire cluster",
				Optional:            true,
				Validators: []validator.Bool{
					boolvalidator.ExactlyOneOf(
						path.MatchRelative().AtName("tables"),
						path.MatchRelative().AtName("databases"),
						path.MatchRelative().AtName("full_cluster_backup"),
					),
				},
			},
			"tables": schema.ListAttribute{
				MarkdownDescription: "Tables to backup",
				Optional:            true,
				ElementType:         types.StringType,
			},
			"databases": schema.ListAttribute{
				MarkdownDescription: "Databases to backup",
				Optional:            true,
				ElementType:         types.StringType,
			},
			"location": schema.StringAttribute{
				MarkdownDescription: "URI for the location to store backups",
				Required:            true,
				Sensitive:           true,
			},
			"recurring": schema.StringAttribute{
				MarkdownDescription: "Schedule for recurring backups",
				Required:            true,
			},
			"full_backup_schedule": schema.StringAttribute{
				MarkdownDescription: "Schedule for full backups",
				Optional:            true,
			},
			"revision_history": schema.BoolAttribute{
				MarkdownDescription: "Whether to keep revision history",
				Optional:            true,
			},
			"encryption_passphrase": schema.StringAttribute{
				MarkdownDescription: "Passphrase for encryption",
				Optional:            true,
				Sensitive:           true,
			},
			//"execution_locality": schema.MapAttribute{
			//	MarkdownDescription: "Execution locality",
			//	Optional:            true,
			//	ElementType:         types.StringType,
			//},
			"kms_uri": schema.StringAttribute{
				MarkdownDescription: "KMS URI",
				Optional:            true,
				Sensitive:           true,
			},
			"incremental_location": schema.StringAttribute{
				MarkdownDescription: "Separate location for incremental backups",
				Optional:            true,
			},
			"first_run": schema.StringAttribute{
				MarkdownDescription: "Time of first backup",
				Optional:            true,
			},
			"on_execution_failure": schema.StringAttribute{
				MarkdownDescription: "Action to take on execution failure",
				Optional:            true,
				Validators: []validator.String{
					stringvalidator.OneOf("retry", "reschedule", "pause"),
				},
			},
			"on_previous_running": schema.StringAttribute{
				MarkdownDescription: "Action to take if previous backup is still running",
				Optional:            true,
				Validators: []validator.String{
					stringvalidator.OneOf("start", "skip", "wait"),
				},
			},
			"ignore_existing_backups": schema.BoolAttribute{
				MarkdownDescription: "Ignore existing backups",
				Optional:            true,
			},
			"id": schema.StringAttribute{
				Computed: true,
				Required: false,
				Optional: false,
			},
		},
	}
}

func (r *BackupScheduleResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*ccloud.CcloudClient)

	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected provider data type",
			fmt.Sprintf("Expected *CcloudClient, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
	}

	r.client = client
}

func (r *BackupScheduleResource) Create(ctx context.Context, req resource.CreateRequest, resp resource.CreateResponse) {
	var data BackupScheduleResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Build Header
	header := fmt.Sprintf("CREATE SCHEDULE IF NOT EXISTS %s FOR BACKUP", pgx.Identifier{data.Label.ValueString()}.Sanitize())

	// Build Target
	var target string
	if !data.FullClusterBackup.IsNull() && data.FullClusterBackup.ValueBool() {
		target = ""
	} else if !data.Tables.IsNull() {
		var tables []string
		data.Tables.ElementsAs(ctx, &tables, false)
		for i, table := range tables {
			tables[i] = pgx.Identifier{table}.Sanitize()
		}
		target = fmt.Sprintf("TABLE %s", strings.Join(tables, ","))
	} else if !data.Databases.IsNull() {
		var databases []string
		data.Databases.ElementsAs(ctx, &databases, false)
		for i, database := range databases {
			databases[i] = pgx.Identifier{database}.Sanitize()
		}
		target = fmt.Sprintf("DATABASE %s", strings.Join(databases, ","))
	}

	// Build location
	location := fmt.Sprintf("LOCATION '%s'", pgx.Identifier{data.Location.ValueString()}.Sanitize())

	// Build backup options
	var optionsSet []string = []string{
		"detached",
	}
	if !data.RevisionHistory.IsNull() && data.RevisionHistory.ValueBool() {
		optionsSet = append(optionsSet, "revision_history")
	}
	if !data.EncryptionPassphrase.IsNull() {
		optionsSet = append(optionsSet, fmt.Sprintf("encryption_passphrase='%s'", pgx.Identifier{data.EncryptionPassphrase.ValueString()}.Sanitize()))
	}
	if !data.KmsUri.IsNull() {
		optionsSet = append(optionsSet, fmt.Sprintf("kms_uri='%s'", pgx.Identifier{data.KmsUri.ValueString()}.Sanitize()))
	}
	if !data.IncrementalLocation.IsNull() {
		optionsSet = append(optionsSet, fmt.Sprintf("incremental_location='%s'", pgx.Identifier{data.IncrementalLocation.ValueString()}.Sanitize()))
	}
	backupOptions := fmt.Sprintf("WITH %s", strings.Join(optionsSet, ", "))

	// Build schedule
	schedule := fmt.Sprintf("SCHEDULE %s", pgx.Identifier{data.Schedule.ValueString()}.Sanitize())
	if !data.FullBackupSchedule.IsNull() {
		schedule = fmt.Sprintf("%s FULL BACKUP %s", schedule, pgx.Identifier{data.FullBackupSchedule.ValueString()}.Sanitize())
	}

	// Build schedule options
	var scheduleOptionsSet []string
	if !data.FirstRun.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("first_run='%s'", pgx.Identifier{data.FirstRun.ValueString()}.Sanitize()))
	}
	if !data.OnExecutionFailure.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("on_execution_failure='%s'", pgx.Identifier{data.OnExecutionFailure.ValueString()}.Sanitize()))
	}
	if !data.OnPreviousRunning.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("on_previous_running='%s'", pgx.Identifier{data.OnPreviousRunning.ValueString()}.Sanitize()))
	}
	if !data.IgnoreExistingBackups.IsNull() && data.IgnoreExistingBackups.ValueBool() {
		scheduleOptionsSet = append(scheduleOptionsSet, "ignore_existing_backups")
	}
	scheduleOptions := ""
	if len(scheduleOptionsSet) > 0 {
		scheduleOptions = fmt.Sprintf("WITH SCHEDULE OPTIONS %s", strings.Join(scheduleOptionsSet, ", "))
	}

	// Build query
	query := fmt.Sprintf("%s %s %s %s %s %s", header, target, location, backupOptions, schedule, scheduleOptions)

	scheduleId, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*string, error) {
		var scheduleId string
		err := db.QueryRow(query, nil).Scan(&scheduleId)
		return &scheduleId, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to create backup schedule", err.Error())
		return
	}

	data.Id = types.StringValue(*scheduleId)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// TODO: Implement the rest of backup management
