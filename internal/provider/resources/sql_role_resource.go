package resources

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/jackc/pgx"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
)

var _ resource.Resource = &SqlRoleResource{}

func NewSqlRoleResource() resource.Resource {
	return &SqlRoleResource{}
}

type SqlRoleResource struct {
	client *ccloud.CcloudClient
}

type SqlRoleResourceModel struct {
	ClusterId types.String `tfsdk:"cluster_id"`
	RoleName  types.String `tfsdk:"name"`
	Id        types.String `tfsdk:"id"`
}

func (r *SqlRoleResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_sql_role"
}

func (r *SqlRoleResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Create a SQL role",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Username",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				Computed: true,
				Required: false,
				Optional: false,
			},
		},
	}
}

func (r *SqlRoleResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*ccloud.CcloudClient)

	if !ok {
		resp.Diagnostics.AddError("invalid provider data", "invalid provider data")
		return
	}

	r.client = client
}

func getSqlRoleId(clusterId string, username string) string {
	return fmt.Sprintf("role|%s|%s", clusterId, username)
}

func (r *SqlRoleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data SqlRoleResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", pgx.Identifier{data.RoleName.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("error creating role", err.Error())
		return
	}

	data.Id = types.StringValue(getSqlRoleId(data.ClusterId.ValueString(), data.RoleName.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlRoleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data SqlRoleResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	exists, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		var result bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)", data.RoleName.ValueString()).Scan(&result)
		return &result, err
	})

	if err != nil {
		if errors.Is(err, &ccloud.CockroachCloudClusterNotReadyError{}) || errors.Is(err, &ccloud.CockroachCloudClusterNotFoundError{}) {
			*exists = false
		} else {
			resp.Diagnostics.AddError("error checking role", err.Error())
			return
		}
	}
	if !*exists {
		resp.State.RemoveResource(ctx)
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlRoleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError("updating sql roles is not supported", "updating sql roles is not supported")
}

func (r *SqlRoleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data SqlRoleResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("DROP ROLE %s", pgx.Identifier{data.RoleName.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("error deleting role", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}
