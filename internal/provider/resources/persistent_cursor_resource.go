package resources

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/jackc/pgx"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
	"strings"
)

var _ resource.Resource = &PersistentCursorResource{}

func NewPersistentCursorResource() resource.Resource {
	return &PersistentCursorResource{}
}

type PersistentCursorResource struct {
	client *ccloud.CcloudClient
}

type PersistentCursorResourceModel struct {
	ClusterId     types.String `tfsdk:"cluster_id"`
	Key           types.String `tfsdk:"key"`
	ResumeOffset  types.Int64  `tfsdk:"resume_offset"`
	Id            types.String `tfsdk:"id"`
	LastUsedJobId types.Int64  `tfsdk:"last_used_job_id"`
	HighWaterMark types.String `tfsdk:"value"`
	Ref           types.String `tfsdk:"ref"`
}

func (r *PersistentCursorResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_persistent_cursor"
}

func (r *PersistentCursorResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: `
Create a persistent cursor.
This can be used with a changefeed to preserve the state of the cursor across restarts.
If the cursor falls behind the gc window (job was in a failed state for too long), it will expire and changefeeds will not be able to resume from it.
`,
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"key": schema.StringAttribute{
				MarkdownDescription: "Unique key that identifies this cursor",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"resume_offset": schema.Int64Attribute{
				MarkdownDescription: `
Add an offset in seconds for changefeed resumption.
Useful for skipping over whatever caused the error.
`,
				Default:  int64default.StaticInt64(0),
				Computed: true,
				Optional: true,
			},
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Persistent cursor ID",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"last_used_job_id": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "ID of the last job that used this cursor",
			},
			"value": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Current timestamp of the cursor",
			},
			"ref": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Reference to the cursor",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}

}

const persistentCursorTable = "persistent_cursors"

func (r *PersistentCursorResource) ensureCursorTable(ctx context.Context, data *PersistentCursorResourceModel) error {
	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT count(*) FROM information_schema.tables WHERE table_name = '%s'", persistentCursorTable)).Scan(&count); err != nil {
			return nil, err
		}
		if count == 1 {
			return nil, nil
		}

		if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (key STRING PRIMARY KEY, resume_offset INT, last_used_job_id INT)", persistentCursorTable)); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

type CursorValue struct {
	Cursor       *string
	OffsetCursor *string
	Offset       *int64
	Exists       bool
	LastJobId    *int64
}

func GetCursor(ctx context.Context, client *ccloud.CcloudClient, clusterId string, key string) (*CursorValue, error) {
	return ccloud.SqlConWithTempUser(ctx, client, clusterId, "defaultdb", func(db *pgx.ConnPool) (*CursorValue, error) {
		var cursor, cursorOffset *string
		var lastJobId, offset *int64
		err := db.QueryRow(fmt.Sprintf(`
SELECT high_water_timestamp::string as cursor,
       resume_offset,
(high_water_timestamp::decimal + (resume_offset::decimal * 1000000))::string as offset_high_water_timestamp,
last_used_job_id last_used_job_id
from %s ct
left outer join [show changefeed jobs] as jobs on jobs.job_id = ct.last_used_job_id
where key = $1
`, persistentCursorTable), key).Scan(&cursor, &offset, &cursorOffset, &lastJobId)

		if errors.Is(err, pgx.ErrNoRows) {
			return &CursorValue{
				Exists: false,
			}, nil
		}

		if err != nil {
			return nil, err
		}

		return &CursorValue{
			Cursor:       cursor,
			OffsetCursor: cursorOffset,
			Exists:       true,
			Offset:       offset,
			LastJobId:    lastJobId,
		}, nil
	})
}

func UpdateCursorJobId(ctx context.Context, client *ccloud.CcloudClient, clusterId string, key string, jobId *int64) error {
	_, err := ccloud.SqlConWithTempUser(ctx, client, clusterId, "defaultdb", func(db *pgx.ConnPool) (_ *interface{}, err error) {
		tx, err := db.Begin()
		if err != nil {
			return nil, err
		}
		var currentJobId *int64
		var status *string
		var returnedKey *string
		defer func() {
			r := tx.Rollback()
			if r != nil {
				err = r
			}
		}()
		err = tx.QueryRow(fmt.Sprintf("select key, last_used_job_id, (select status from [show changefeed jobs] where job_id = last_used_job_id) from %s where key =$1 for update", persistentCursorTable), key).Scan(
			&returnedKey, &currentJobId, &status)

		if err != nil {
			return nil, err
		}

		if currentJobId != nil && !(*status == "canceled" || *status == "failed" || *status == "succeeded" || *status == "canceling") {
			return nil, fmt.Errorf("cursor is still in use by job %d", *currentJobId)
		}

		// check if there are any other cursors that are in use by the same job
		var otherCursorCount int
		err = tx.QueryRow(fmt.Sprintf("select count(*) from %s where last_used_job_id = $1 and key != $2", persistentCursorTable), jobId, key).Scan(&otherCursorCount)
		if err != nil {
			return nil, err
		}

		if otherCursorCount > 0 {
			return nil, fmt.Errorf("Job cannot use multiple cursors")
		}

		_, err = tx.Exec(fmt.Sprintf("UPDATE %s SET last_used_job_id = $1 WHERE key = $2", persistentCursorTable), jobId, key)

		if err != nil {
			return nil, err
		}
		return nil, tx.Commit()
	})
	return err
}

func ParseCursorId(cursorId string) (clusterId, key string) {
	parts := strings.Split(cursorId, "|")
	if len(parts) != 3 || parts[0] != "cursor" {
		panic(fmt.Sprintf("Unable to parse cursor ID %s", cursorId))
	}
	return parts[1], parts[2]
}

func (r *PersistentCursorResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *PersistentCursorResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data PersistentCursorResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.ResumeOffset.IsNull() {
		data.ResumeOffset = types.Int64Value(0)
	}

	if err := r.ensureCursorTable(ctx, &data); err != nil {
		resp.Diagnostics.AddError("Unable to create persistent cursor table", err.Error())
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (key, resume_offset) VALUES ($1, $2)", persistentCursorTable), data.Key.ValueString(), data.ResumeOffset.ValueInt64())
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to create persistent cursor", err.Error())
		return
	}

	data.Id = types.StringValue(fmt.Sprintf("cursor|%s|%s", data.ClusterId.ValueString(), data.Key.ValueString()))
	data.Ref = data.Id
	data.HighWaterMark = types.StringNull()
	data.LastUsedJobId = types.Int64Null()

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *PersistentCursorResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data PersistentCursorResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	cursorValue, err := GetCursor(ctx, r.client, data.ClusterId.ValueString(), data.Key.ValueString())

	if err != nil {
		resp.Diagnostics.AddError("Unable to read persistent cursor", err.Error())
		return
	}

	if !cursorValue.Exists {
		resp.Diagnostics.AddError("Persistent cursor not found", fmt.Sprintf("Cursor with key %s not found", data.Key.ValueString()))
		return
	}
	data.ResumeOffset = types.Int64Value(*cursorValue.Offset)

	if cursorValue.LastJobId != nil {
		data.LastUsedJobId = types.Int64Value(*cursorValue.LastJobId)
	}
	if cursorValue.OffsetCursor != nil {
		data.HighWaterMark = types.StringValue(*cursorValue.OffsetCursor)
	}

	data.Ref = data.Id

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *PersistentCursorResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data PersistentCursorResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("UPDATE %s SET resume_offset = $1 WHERE key = $2", persistentCursorTable), data.ResumeOffset.ValueInt64(), data.Key.ValueString())
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to update persistent cursor", err.Error())
		return
	}

	data.Ref = data.Id

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *PersistentCursorResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data PersistentCursorResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, fmt.Sprintf("Deleting persistent cursor %s for cluster %s", data.Key, data.ClusterId))
	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE key = $1", persistentCursorTable), data.Key.ValueString())
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to reset cluster setting", err.Error())
		return
	}
}
