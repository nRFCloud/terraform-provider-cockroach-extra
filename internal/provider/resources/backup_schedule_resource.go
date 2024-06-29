package resources

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/parser"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/hashicorp/terraform-plugin-framework-validators/boolvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/listvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/objectvalidator"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/jackc/pgx"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
	"regexp"
	"strings"
)

var _ resource.Resource = &BackupScheduleResource{}

func NewBackupScheduleResource() resource.Resource {
	return &BackupScheduleResource{}
}

type BackupScheduleResource struct {
	client *ccloud.CcloudClient
}

type BackupScheduleResourceModel struct {
	ClusterId types.String `tfsdk:"cluster_id"`
	Label     types.String `tfsdk:"label"`
	Id        types.String `tfsdk:"id"`
	Location  types.String `tfsdk:"location"`
	Recurring types.String `tfsdk:"recurring"`
	//ExecutionLocality     types.Map    `tfsdk:"execution_locality"`
	Target *struct {
		Tables            types.List `tfsdk:"tables"`
		Databases         types.List `tfsdk:"databases"`
		FullClusterBackup types.Bool `tfsdk:"full_cluster_backup"`
	} `tfsdk:"target"`
	ScheduleOptions *struct {
		FirstRun              types.String `tfsdk:"first_run"`
		OnExecutionFailure    types.String `tfsdk:"on_execution_failure"`
		OnPreviousRunning     types.String `tfsdk:"on_previous_running"`
		IgnoreExistingBackups types.Bool   `tfsdk:"ignore_existing_backups"`
	} `tfsdk:"schedule_options"`
	FullBackupScheduleId        types.Int64 `tfsdk:"full_backup_schedule_id"`
	IncrementalBackupScheduleId types.Int64 `tfsdk:"incremental_backup_schedule_id"`
	BackupOptions               *struct {
		Kms                       types.String `tfsdk:"kms"`
		EncryptionPassphrase      types.String `tfsdk:"encryption_passphrase"`
		RevisionHistory           types.Bool   `tfsdk:"revision_history"`
		FullBackupFrequency       types.String `tfsdk:"full_backup_frequency"`
		IncrementalBackupLocation types.String `tfsdk:"incremental_backup_location"`
	} `tfsdk:"backup_options"`
}

func (r *BackupScheduleResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_backup_schedule"
}

func getBackupScheduleId(clusterId string, label string) string {
	return fmt.Sprintf("backup_schedule|%s|%s", clusterId, label)
}

func (r *BackupScheduleResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Backup schedule",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "ID of the backup schedule",
				Computed:            true,
				Required:            false,
				Optional:            false,
			},
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"label": schema.StringAttribute{
				MarkdownDescription: "Label for the backup schedule",
				Required:            true,
			},
			"location": schema.StringAttribute{
				MarkdownDescription: "Location for the backup",
				Required:            true,
				Sensitive:           true,
			},
			"recurring": schema.StringAttribute{
				MarkdownDescription: "Recurring schedule",
				Required:            true,
				Validators: []validator.String{
					stringvalidator.Any(
						stringvalidator.OneOf("@daily", "@hourly", "@weekly"),
						CronExpressionValidator(),
					),
				},
			},
			"target": schema.SingleNestedAttribute{
				MarkdownDescription: "Backup target",
				Validators: []validator.Object{
					objectvalidator.AtLeastOneOf(
						path.MatchRelative().AtName("tables"),
						path.MatchRelative().AtName("databases"),
						path.MatchRelative().AtName("full_cluster_backup"),
					),
				},
				PlanModifiers: []planmodifier.Object{
					objectplanmodifier.RequiresReplace(),
				},
				Required: true,
				Attributes: map[string]schema.Attribute{
					"tables": schema.ListAttribute{
						MarkdownDescription: "Tables to backup",
						Optional:            true,
						ElementType:         types.StringType,
						PlanModifiers: []planmodifier.List{
							listplanmodifier.RequiresReplace(),
						},
						Validators: []validator.List{
							listvalidator.ConflictsWith(
								path.MatchRoot("target").AtName("databases"),
								path.MatchRoot("target").AtName("full_cluster_backup"),
							),
							listvalidator.ValueStringsAre(
								stringvalidator.RegexMatches(
									regexp.MustCompile(`^[a-zA-Z0-9_]+?\.[a-zA-Z0-9_]+?\.[a-zA-Z0-9_]+?$`),
									"Table names must be fully qualified",
								),
							),
						},
					},
					"databases": schema.ListAttribute{
						MarkdownDescription: "Databases to backup",
						Optional:            true,
						ElementType:         types.StringType,
						PlanModifiers: []planmodifier.List{
							listplanmodifier.RequiresReplace(),
						},
						Validators: []validator.List{
							listvalidator.ConflictsWith(
								path.MatchRoot("target").AtName("tables"),
								path.MatchRoot("target").AtName("full_cluster_backup"),
							),
						},
					},
					"full_cluster_backup": schema.BoolAttribute{
						MarkdownDescription: "Backup the full cluster",
						Optional:            true,
						PlanModifiers: []planmodifier.Bool{
							boolplanmodifier.RequiresReplace(),
						},
						Validators: []validator.Bool{
							boolvalidator.ConflictsWith(
								path.MatchRoot("target").AtName("tables"),
								path.MatchRoot("target").AtName("databases"),
							),
						},
					},
				},
			},
			"schedule_options": schema.SingleNestedAttribute{
				MarkdownDescription: "Backup schedule options",
				Required:            false,
				Optional:            true,
				//Default to empty object
				Computed: true,
				Default: objectdefault.StaticValue(types.ObjectValueMust(
					map[string]attr.Type{
						"first_run":               types.StringType,
						"on_execution_failure":    types.StringType,
						"on_previous_running":     types.StringType,
						"ignore_existing_backups": types.BoolType,
					},
					map[string]attr.Value{
						"first_run":               types.StringNull(),
						"on_execution_failure":    types.StringValue("reschedule"),
						"on_previous_running":     types.StringValue("wait"),
						"ignore_existing_backups": types.BoolValue(false),
					},
				)),
				Attributes: map[string]schema.Attribute{
					"first_run": schema.StringAttribute{
						MarkdownDescription: "When should the first run be scheduled",
						Optional:            true,
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.UseStateForUnknown(),
							stringplanmodifier.RequiresReplaceIfConfigured(),
						},
					},
					"on_execution_failure": schema.StringAttribute{
						MarkdownDescription: "What to do on execution failure",
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("retry", "reschedule", "pause"),
						},
						Default:  stringdefault.StaticString("reschedule"),
						Computed: true,
					},
					"on_previous_running": schema.StringAttribute{
						MarkdownDescription: "What to do if the previous run is still running",
						Computed:            true,
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.OneOf("skip", "wait", "start"),
						},
						Default: stringdefault.StaticString("wait"),
					},
					"ignore_existing_backups": schema.BoolAttribute{
						MarkdownDescription: "Ignore existing backups",
						Optional:            true,
						PlanModifiers: []planmodifier.Bool{
							boolplanmodifier.RequiresReplaceIfConfigured(),
							boolplanmodifier.UseStateForUnknown(),
						},
					},
				},
			},
			"full_backup_schedule_id": schema.Int64Attribute{
				MarkdownDescription: "Schedule ID for full backups",
				Optional:            false,
				Required:            false,
				Computed:            true,
			},
			"incremental_backup_schedule_id": schema.Int64Attribute{
				MarkdownDescription: "Schedule ID for incremental backups",
				Optional:            false,
				Required:            false,
				Computed:            true,
			},
			"backup_options": schema.SingleNestedAttribute{
				MarkdownDescription: "Backup options",
				Optional:            true,
				Computed:            true,
				Default: objectdefault.StaticValue(
					types.ObjectValueMust(
						map[string]attr.Type{
							"revision_history":            types.BoolType,
							"full_backup_frequency":       types.StringType,
							"incremental_backup_location": types.StringType,
							"encryption_passphrase":       types.StringType,
							"kms":                         types.StringType,
						},
						map[string]attr.Value{
							"revision_history":            types.BoolValue(true),
							"full_backup_frequency":       types.StringValue("always"),
							"incremental_backup_location": types.StringNull(),
							"encryption_passphrase":       types.StringNull(),
							"kms":                         types.StringNull(),
						},
					),
				),
				Attributes: map[string]schema.Attribute{
					"kms": schema.StringAttribute{
						MarkdownDescription: "KMS URI",
						Optional:            true,
					},
					"encryption_passphrase": schema.StringAttribute{
						MarkdownDescription: "Encryption passphrase",
						Optional:            true,
						Sensitive:           true,
					},
					"revision_history": schema.BoolAttribute{
						MarkdownDescription: "Enable revision history",
						Optional:            true,
						Default:             booldefault.StaticBool(true),
						Computed:            true,
					},
					"full_backup_frequency": schema.StringAttribute{
						MarkdownDescription: "Frequency that full backups should be taken",
						Optional:            true,
						Validators: []validator.String{
							stringvalidator.Any(
								stringvalidator.OneOf("@daily", "@weekly", "@hourly", "always"),
								CronExpressionValidator(),
							),
						},
						Default:  stringdefault.StaticString("always"),
						Computed: true,
					},
					"incremental_backup_location": schema.StringAttribute{
						MarkdownDescription: "Explicit separate location for incremental backups",
						Optional:            true,
					},
				},
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

func (r *BackupScheduleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data BackupScheduleResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Check if backup schedule with the given label already exists
	// "IF NOT EXISTS" should prevent duplication, but an explicit check prevents confusion
	scheduleExists, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		var exists bool
		err := db.QueryRow("SELECT EXISTS(SELECT * FROM [SHOW schedules for backup ] WHERE label = $1)", data.Label.ValueString()).Scan(&exists)
		return &exists, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to check if backup schedule exists", err.Error())
		return
	}

	if *scheduleExists {
		resp.Diagnostics.AddError("Backup schedule with the given label already exists", "")
		return
	}

	header := fmt.Sprintf("CREATE SCHEDULE IF NOT EXISTS %s FOR BACKUP", pgx.Identifier{data.Label.ValueString()}.Sanitize())

	// Build Target
	var target string
	if !data.Target.FullClusterBackup.IsNull() && data.Target.FullClusterBackup.ValueBool() {
		target = ""
	} else if !data.Target.Tables.IsNull() {
		var tables []string
		data.Target.Tables.ElementsAs(ctx, &tables, false)
		target = fmt.Sprintf("TABLE %s", strings.Join(tables, ","))
	} else if !data.Target.Databases.IsNull() {
		var databases []string
		data.Target.Databases.ElementsAs(ctx, &databases, false)
		target = fmt.Sprintf("DATABASE %s", strings.Join(databases, ","))
	}

	tflog.Debug(ctx, fmt.Sprintf("Target: %s", target))

	location := fmt.Sprintf("INTO %s", SanatizeValue(data.Location.ValueString()))

	backupOptionsSet := []string{}

	tflog.Debug(ctx, fmt.Sprintf("data.BackupOptions %v", data.BackupOptions))

	if !data.BackupOptions.RevisionHistory.IsNull() && data.BackupOptions.RevisionHistory.ValueBool() {
		backupOptionsSet = append(backupOptionsSet, "revision_history")
	}

	if !data.BackupOptions.EncryptionPassphrase.IsNull() {
		backupOptionsSet = append(backupOptionsSet, fmt.Sprintf("encryption_passphrase=%s", SanatizeValue(data.BackupOptions.EncryptionPassphrase.ValueString())))
	}

	if !data.BackupOptions.Kms.IsNull() {
		backupOptionsSet = append(backupOptionsSet, fmt.Sprintf("kms=%s", SanatizeValue(data.BackupOptions.Kms.ValueString())))
	}

	//backupOptions := fmt.Sprintf("WITH %s", strings.Join(backupOptionsSet, ", "))
	backupOptions := ""
	if len(backupOptionsSet) > 0 {
		backupOptions = fmt.Sprintf("WITH %s", strings.Join(backupOptionsSet, ", "))
	}

	tflog.Debug(ctx, fmt.Sprintf("Backup options: %s", backupOptions))

	scheduleOptionsSet := []string{}

	if !data.ScheduleOptions.FirstRun.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("first_run=%s", SanatizeValue(data.ScheduleOptions.FirstRun.ValueString())))
	}

	if !data.ScheduleOptions.OnExecutionFailure.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("on_execution_failure=%s", SanatizeValue(data.ScheduleOptions.OnExecutionFailure.ValueString())))
	}

	if !data.ScheduleOptions.OnPreviousRunning.IsNull() {
		scheduleOptionsSet = append(scheduleOptionsSet, fmt.Sprintf("on_previous_running=%s", SanatizeValue(data.ScheduleOptions.OnPreviousRunning.ValueString())))
	}

	if !data.ScheduleOptions.IgnoreExistingBackups.IsNull() && data.ScheduleOptions.IgnoreExistingBackups.ValueBool() {
		scheduleOptionsSet = append(scheduleOptionsSet, "ignore_existing_backups")
	}

	//scheduleOptions := fmt.Sprintf("WITH SCHEDULE OPTIONS %s", strings.Join(scheduleOptionsSet, ", "))
	scheduleOptions := ""
	if len(scheduleOptionsSet) > 0 {
		scheduleOptions = fmt.Sprintf("WITH SCHEDULE OPTIONS %s", strings.Join(scheduleOptionsSet, ", "))
	}

	tflog.Debug(ctx, fmt.Sprintf("Schedule options: %s", scheduleOptions))

	recurring := fmt.Sprintf("RECURRING %s", SanatizeValue(data.Recurring.ValueString()))

	var fullBackupSchedule string
	if data.BackupOptions.FullBackupFrequency.ValueString() == "always" {
		fullBackupSchedule = "FULL BACKUP ALWAYS"
	} else {
		fullBackupSchedule = fmt.Sprintf("FULL BACKUP %s", SanatizeValue(data.BackupOptions.FullBackupFrequency.ValueString()))
	}

	createScheduleQuery := fmt.Sprintf("%s %s %s %s %s %s %s", header, target, location, backupOptions, recurring, fullBackupSchedule, scheduleOptions)

	fullQuery := fmt.Sprintf("WITH x as (%s) select schedule_id, strpos(backup_stmt, 'BACKUP INTO LATEST') = 1 as is_incremental from x", createScheduleQuery)

	type scheduleIdSet struct {
		fullBackupId        *int64
		incrementalBackupId *int64
	}

	tflog.Debug(ctx, fmt.Sprintf("Creating backup schedule: %s", fullQuery))

	scheduleIds, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*scheduleIdSet, error) {
		schedules := scheduleIdSet{}
		rows, err := db.Query(fullQuery)
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		for rows.Next() {
			var scheduleId int64
			var isIncremental bool

			err = rows.Scan(&scheduleId, &isIncremental)
			if err != nil {
				return nil, err
			}
			if isIncremental {
				schedules.incrementalBackupId = &scheduleId
			} else {
				schedules.fullBackupId = &scheduleId
			}
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}

		return &schedules, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to create backup schedule", err.Error())
		return
	}

	data.Id = types.StringValue(getBackupScheduleId(data.ClusterId.ValueString(), data.Label.ValueString()))
	if scheduleIds.incrementalBackupId != nil {
		data.IncrementalBackupScheduleId = types.Int64Value(*scheduleIds.incrementalBackupId)
	} else {
		data.IncrementalBackupScheduleId = types.Int64Null()
	}
	data.FullBackupScheduleId = types.Int64Value(*scheduleIds.fullBackupId)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BackupScheduleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data BackupScheduleResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	type scheduleInfo struct {
		id                 int64
		label              string
		recurrence         string
		onPreviousRunning  string
		onExecutionFailure string
		command            *tree.Backup
		backupType         string
	}

	type scheduleSet struct {
		fullBackup        *scheduleInfo
		incrementalBackup *scheduleInfo
	}

	schedules, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*scheduleSet, error) {
		schedules := scheduleSet{}
		rows, err := db.Query("SELECT id, label, recurrence, on_previous_running, on_execution_failure, command, backup_type FROM [SHOW SCHEDULES FOR BACKUP] WHERE label = $1", data.Label.ValueString())
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var scheduleId int64
			var label, recurrence, onPreviousRunning, onExecutionFailure, command, backupType string

			err = rows.Scan(&scheduleId, &label, &recurrence, &onPreviousRunning, &onExecutionFailure, &command, &backupType)
			if err != nil {
				return nil, err
			}
			parsedCommand, err := parser.ParseOne(command)
			if err != nil {
				return nil, err
			}
			backupCommand, ok := parsedCommand.AST.(*tree.Backup)
			if !ok {
				return nil, fmt.Errorf("Unable to parse backup command")
			}

			if backupType == "FULL" {
				schedules.fullBackup = &scheduleInfo{
					id:                 scheduleId,
					label:              label,
					recurrence:         recurrence,
					onPreviousRunning:  onPreviousRunning,
					onExecutionFailure: onExecutionFailure,
					command:            backupCommand,
					backupType:         backupType,
				}
			} else {
				schedules.incrementalBackup = &scheduleInfo{
					id:                 scheduleId,
					label:              label,
					recurrence:         recurrence,
					onPreviousRunning:  onPreviousRunning,
					onExecutionFailure: onExecutionFailure,
					command:            backupCommand,
					backupType:         backupType,
				}
			}
		}
		return &schedules, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to read backup schedule", err.Error())
		return
	}

	if schedules.fullBackup == nil {
		resp.State.RemoveResource(ctx)
		return
	}

	data.Id = types.StringValue(schedules.fullBackup.label)
	if schedules.incrementalBackup != nil {
		data.Recurring = types.StringValue(schedules.incrementalBackup.recurrence)
	} else {
		data.Recurring = types.StringValue(schedules.fullBackup.recurrence)
	}

	data.ScheduleOptions.OnPreviousRunning = types.StringValue(strings.ToLower(schedules.fullBackup.onPreviousRunning))

	mapOnExecutionFailure := map[string]string{
		"PAUSE_SCHED": "pause",
		"RETRY_SOON":  "retry",
		"RETRY_SCHED": "reschedule",
	}

	if val, ok := mapOnExecutionFailure[schedules.fullBackup.onExecutionFailure]; ok {
		data.ScheduleOptions.OnExecutionFailure = types.StringValue(val)
	} else {
		data.ScheduleOptions.OnExecutionFailure = types.StringValue("retry")
	}

	data.FullBackupScheduleId = types.Int64Value(schedules.fullBackup.id)

	if schedules.incrementalBackup == nil {
		data.BackupOptions.FullBackupFrequency = types.StringValue("always")
	} else {
		data.BackupOptions.FullBackupFrequency = types.StringValue(schedules.fullBackup.recurrence)
	}

	newBackupLocation := strings.Trim(schedules.fullBackup.command.To[0].String(), "'")
	if !CompareURLs(newBackupLocation, data.Location.ValueString()) {
		data.Location = types.StringValue(newBackupLocation)
	}

	switch true {
	case schedules.fullBackup.command.Targets == nil:
		data.Target.FullClusterBackup = types.BoolValue(true)
	case len(schedules.fullBackup.command.Targets.Tables.TablePatterns) > 0:
		tables := []attr.Value{}
		for _, table := range schedules.fullBackup.command.Targets.Tables.TablePatterns {
			tables = append(tables, types.StringValue(table.String()))
		}
		tableListValue, _ := types.ListValue(types.StringType, tables)
		data.Target.Tables = tableListValue
	case len(schedules.fullBackup.command.Targets.Databases) > 0:
		databases := []attr.Value{}
		for _, database := range schedules.fullBackup.command.Targets.Databases {
			databases = append(databases, types.StringValue(database.String()))
		}
		databaseListValue, _ := types.ListValue(types.StringType, databases)
		data.Target.Databases = databaseListValue
	}

	if schedules.fullBackup.command.Options.EncryptionKMSURI != nil && schedules.fullBackup.command.Options.EncryptionKMSURI[0] != nil {
		data.BackupOptions.Kms = types.StringValue(schedules.fullBackup.command.Options.EncryptionKMSURI[0].String())
	}

	if (schedules.fullBackup.command.Options.EncryptionPassphrase != nil && data.BackupOptions.EncryptionPassphrase.IsNull()) ||
		(schedules.fullBackup.command.Options.EncryptionPassphrase == nil && !data.BackupOptions.EncryptionPassphrase.IsNull()) {
		data.BackupOptions.EncryptionPassphrase = types.StringValue(schedules.fullBackup.command.Options.EncryptionPassphrase.String())
	}

	if schedules.fullBackup.command.Options.CaptureRevisionHistory != nil {
		db := tree.MustBeDBool(schedules.fullBackup.command.Options.CaptureRevisionHistory)
		data.BackupOptions.RevisionHistory = types.BoolValue(bool(db))
	}

	if schedules.incrementalBackup != nil {
		data.IncrementalBackupScheduleId = types.Int64Value(schedules.incrementalBackup.id)
		if schedules.incrementalBackup.command.Options.IncrementalStorage != nil && schedules.incrementalBackup.command.Options.IncrementalStorage[0] != nil {
			data.BackupOptions.IncrementalBackupLocation = types.StringValue(schedules.incrementalBackup.command.Options.IncrementalStorage[0].String())
		} else {
			data.BackupOptions.IncrementalBackupLocation = types.StringNull()
		}
	} else {
		data.IncrementalBackupScheduleId = types.Int64Null()
		data.BackupOptions.IncrementalBackupLocation = types.StringNull()
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *BackupScheduleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan BackupScheduleResourceModel
	var state BackupScheduleResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)

	if resp.Diagnostics.HasError() {
		return
	}

	header := fmt.Sprintf("ALTER BACKUP SCHEDULE %d", state.FullBackupScheduleId.ValueInt64())

	updateSet := []string{}

	if !plan.Label.Equal(state.Label) {
		updateSet = append(updateSet, fmt.Sprintf("SET LABEL %s", SanatizeValue(plan.Label.ValueString())))
	}

	if !plan.Location.Equal(state.Location) {
		updateSet = append(updateSet, fmt.Sprintf("SET INTO %s", SanatizeValue(plan.Location.ValueString())))
	}

	if !plan.Recurring.Equal(state.Recurring) {
		updateSet = append(updateSet, fmt.Sprintf("SET RECURRING %s", SanatizeValue(plan.Recurring.ValueString())))
	}

	if !plan.BackupOptions.FullBackupFrequency.Equal(state.BackupOptions.FullBackupFrequency) {
		if plan.BackupOptions.FullBackupFrequency.ValueString() == "always" {
			updateSet = append(updateSet, "SET FULL BACKUP ALWAYS")
		} else {
			updateSet = append(updateSet, fmt.Sprintf("SET FULL BACKUP %s", SanatizeValue(plan.BackupOptions.FullBackupFrequency.ValueString())))
		}
	}

	updateSet = append(updateSet, fmt.Sprintf("SET WITH revision_history=%t", plan.BackupOptions.RevisionHistory.ValueBool()))

	if !plan.BackupOptions.EncryptionPassphrase.Equal(state.BackupOptions.EncryptionPassphrase) {
		if plan.BackupOptions.EncryptionPassphrase.IsNull() {
			updateSet = append(updateSet, "UNSET WITH encryption_passphrase")
		} else {
			updateSet = append(updateSet, fmt.Sprintf("SET WITH encryption_passphrase=%s", SanatizeValue(plan.BackupOptions.EncryptionPassphrase.ValueString())))
		}
	}

	if !plan.BackupOptions.Kms.Equal(state.BackupOptions.Kms) {
		if plan.BackupOptions.Kms.IsNull() {
			updateSet = append(updateSet, "UNSET WITH kms")
		} else {
			updateSet = append(updateSet, fmt.Sprintf("SET WITH kms=%s", SanatizeValue(plan.BackupOptions.Kms.ValueString())))
		}
	}

	if !plan.BackupOptions.IncrementalBackupLocation.Equal(state.BackupOptions.IncrementalBackupLocation) {
		if plan.BackupOptions.IncrementalBackupLocation.IsNull() {
			updateSet = append(updateSet, "UNSET WITH incremental_backup_location")
		} else {
			updateSet = append(updateSet, fmt.Sprintf("SET WITH incremental_backup_location=%s", SanatizeValue(plan.BackupOptions.IncrementalBackupLocation.ValueString())))
		}
	}

	if !plan.ScheduleOptions.OnExecutionFailure.Equal(state.ScheduleOptions.OnExecutionFailure) {
		updateSet = append(updateSet, fmt.Sprintf("SET SCHEDULE OPTION on_execution_failure=%s", SanatizeValue(plan.ScheduleOptions.OnExecutionFailure.ValueString())))
	}

	if !plan.ScheduleOptions.OnPreviousRunning.Equal(state.ScheduleOptions.OnPreviousRunning) {
		updateSet = append(updateSet, fmt.Sprintf("SET SCHEDULE OPTION on_previous_running=%s", SanatizeValue(plan.ScheduleOptions.OnPreviousRunning.ValueString())))
	}

	alterScheduleQuery := fmt.Sprintf("%s %s", header, strings.Join(updateSet, ", "))

	fullQuery := fmt.Sprintf("WITH x as (%s) select schedule_id, strpos(backup_stmt, 'BACKUP INTO LATEST') = 1 as is_incremental from x", alterScheduleQuery)

	type scheduleIdSet struct {
		fullBackupId        *int64
		incrementalBackupId *int64
	}

	tflog.Debug(ctx, fmt.Sprintf("Updating backup schedule: %s", fullQuery))

	scheduleIds, err := ccloud.SqlConWithTempUser(ctx, r.client, plan.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*scheduleIdSet, error) {
		schedules := scheduleIdSet{}
		rows, err := db.Query(fullQuery)
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		for rows.Next() {
			var scheduleId int64
			var isIncremental bool

			err = rows.Scan(&scheduleId, &isIncremental)
			if err != nil {
				return nil, err
			}
			if isIncremental {
				schedules.incrementalBackupId = &scheduleId
			} else {
				schedules.fullBackupId = &scheduleId
			}
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}

		return &schedules, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to update backup schedule", err.Error())
		return
	}

	plan.FullBackupScheduleId = types.Int64Value(*scheduleIds.fullBackupId)
	if scheduleIds.incrementalBackupId != nil {
		plan.IncrementalBackupScheduleId = types.Int64Value(*scheduleIds.incrementalBackupId)
	} else {
		plan.IncrementalBackupScheduleId = types.Int64Null()
	}
	plan.Id = types.StringValue(getBackupScheduleId(plan.ClusterId.ValueString(), plan.Label.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *BackupScheduleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data BackupScheduleResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		_, err := db.Exec("drop schedules with x as (show schedules for backup) select id from x where label = $1", data.Label.ValueString())
		if err != nil {
			return nil, err
		}
		return nil, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to delete backup schedule", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
