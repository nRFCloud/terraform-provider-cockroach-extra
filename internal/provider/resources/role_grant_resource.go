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

var _ resource.Resource = &RoleGrantResource{}

func NewRoleGrantResource() resource.Resource {
	return &RoleGrantResource{}
}

type RoleGrantResource struct {
	client *ccloud.CcloudClient
}

type RoleGrantResourceModel struct {
	ClusterId types.String `tfsdk:"cluster_id"`
	Username  types.String `tfsdk:"user_name"`
	Role      types.String `tfsdk:"role_name"`
	Id        types.String `tfsdk:"id"`
}

func (r *RoleGrantResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_role_grant"
}

func (r *RoleGrantResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Grant a role to a user",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"user_name": schema.StringAttribute{
				MarkdownDescription: "Username",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"role_name": schema.StringAttribute{
				MarkdownDescription: "Role",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				MarkdownDescription: "ID",
				Computed:            true,
				Required:            false,
				Optional:            false,
			},
		},
	}
}

func (r *RoleGrantResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*ccloud.CcloudClient)

	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type", fmt.Sprintf("Expected *CcloudClient, got: %T. Please report this issue to the provider developers.", req.ProviderData))
		return
	}

	r.client = client
}

func getRoleGrantId(clusterId string, username string, role string) string {
	return "role_grant|" + clusterId + "|" + username + "|" + role
}

func (r *RoleGrantResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data RoleGrantResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("GRANT %s TO %s", pgx.Identifier{data.Role.ValueString()}.Sanitize(), pgx.Identifier{data.Username.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Failed to grant role", err.Error())
		return
	}

	data.Id = types.StringValue(getRoleGrantId(data.ClusterId.ValueString(), data.Username.ValueString(), data.Role.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *RoleGrantResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data RoleGrantResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	result, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		// If the role is not found, the query will return an empty row
		var result bool
		var response int
		err := db.QueryRow(fmt.Sprintf("select 1 from [show grants on role %s] where member=$1", pgx.Identifier{data.Role.ValueString()}.Sanitize()), data.Username.ValueString()).Scan(&response)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		result = !errors.Is(err, pgx.ErrNoRows)
		return &result, nil
	})

	if err != nil && !errors.Is(err, &ccloud.CockroachCloudClusterNotReadyError{}) && !errors.Is(err, &ccloud.CockroachCloudClusterNotFoundError{}) {
		resp.Diagnostics.AddError("Failed to read role", err.Error())
		return
	}

	if !*result {
		resp.State.RemoveResource(ctx)
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// Update Role grants should never be updated in place, as they are immutable.
// Throw an error if the user tries to do so.
func (r *RoleGrantResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data RoleGrantResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)

	resp.Diagnostics.AddError("Role grants cannot be updated in place", "Role grants cannot be updated in place. Please delete the resource and recreate it.")
}

func (r *RoleGrantResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data RoleGrantResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.Role.IsNull() || data.Username.IsNull() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("REVOKE %s FROM %s", pgx.Identifier{data.Role.ValueString()}.Sanitize(), pgx.Identifier{data.Username.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Failed to revoke role", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
