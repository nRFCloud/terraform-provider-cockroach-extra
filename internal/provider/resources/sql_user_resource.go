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
	"strings"
)

var _ resource.Resource = &SqlUserResource{}

func NewSqlUserResource() resource.Resource {
	return &SqlUserResource{}
}

type SqlUserResource struct {
	client *ccloud.CcloudClient
}

func buildSqlUserId(clusterId string, username string) string {
	return fmt.Sprintf("user|%s|%s", clusterId, username)
}

func parseSqlUserId(id string) (clusterId string, username string, err error) {
	parts := strings.Split(id, "|")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("invalid user resource ID")
	}
	if parts[0] != "user" {
		return "", "", fmt.Errorf("resource id must start with 'user'")
	}
	return parts[1], parts[2], nil
}

type SqlUserResourceModel struct {
	ClusterId types.String `tfsdk:"cluster_id"`
	Username  types.String `tfsdk:"name"`
	Password  types.String `tfsdk:"password"`
	Id        types.String `tfsdk:"id"`
}

func (r *SqlUserResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_sql_user"
}

func (r *SqlUserResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Create a SQL user",
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
			"password": schema.StringAttribute{
				MarkdownDescription: "Password",
				Required:            false,
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

func (r *SqlUserResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *SqlUserResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data SqlUserResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		if data.Password.IsNull() {
			_, err := db.Exec(fmt.Sprintf("CREATE USER %s", pgx.Identifier{data.Username.ValueString()}.Sanitize()))
			return nil, err
		} else {
			_, err := db.Exec(fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", pgx.Identifier{data.Username.ValueString()}.Sanitize()), data.Password.ValueString())
			return nil, err
		}
	})

	if err != nil {
		resp.Diagnostics.AddError("error creating user", err.Error())
		return
	}

	data.Id = types.StringValue(buildSqlUserId(data.ClusterId.ValueString(), data.Username.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlUserResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data SqlUserResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	exists, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		var result bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)", data.Username.ValueString()).Scan(&result)
		return &result, err
	})

	if err != nil {
		if errors.Is(err, &ccloud.CockroachCloudClusterNotReadyError{}) || errors.Is(err, &ccloud.CockroachCloudClusterNotFoundError{}) {
			*exists = false
		} else {
			resp.Diagnostics.AddError("error checking user", err.Error())
			return
		}
	}

	if !*exists {
		resp.State.RemoveResource(ctx)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlUserResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data SqlUserResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("ALTER USER %s WITH PASSWORD $1", pgx.Identifier{data.Username.ValueString()}.Sanitize()), data.Password.ValueString())
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("error updating user", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlUserResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data SqlUserResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("REVOKE ALL ON * FROM %s", pgx.Identifier{data.Username.ValueString()}.Sanitize()))

		if err != nil {
			return nil, err
		}

		_, err = db.Exec(fmt.Sprintf("DROP USER %s", pgx.Identifier{data.Username.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("error deleting user", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *SqlUserResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	clusterId, username, err := parseSqlUserId(req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Invalid sql user resource ID", err.Error())
		return
	}

	exists, err := ccloud.SqlConWithTempUser(ctx, r.client, clusterId, "defaultdb", func(db *pgx.ConnPool) (*bool, error) {
		var result bool
		err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)", username).Scan(&result)
		return &result, err
	})

	if err != nil {
		if errors.Is(err, &ccloud.CockroachCloudClusterNotReadyError{}) || errors.Is(err, &ccloud.CockroachCloudClusterNotFoundError{}) {
			*exists = false
		} else {
			resp.Diagnostics.AddError("error importing user", err.Error())
			return
		}
	}

	if !*exists {
		resp.Diagnostics.AddError("Failed to import user", fmt.Sprintf("User with username: '%s' does not exist", username))
		return
	}

	var data SqlUserResourceModel
	data.ClusterId = types.StringValue(clusterId)
	data.Username = types.StringValue(username)
	data.Password = types.StringValue("")
	data.Id = types.StringValue(req.ID)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
