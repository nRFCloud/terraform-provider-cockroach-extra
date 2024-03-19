package resources

import (
	"context"
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

var _ resource.Resource = &ExternalConnectionResource{}
var _ resource.ResourceWithImportState = &ExternalConnectionResource{}

func NewExternalConnectionResource() resource.Resource {
	return &ExternalConnectionResource{}
}

type ExternalConnectionResource struct {
	client *ccloud.CcloudClient
}

type ExternalConnectionResourceModel struct {
	ClusterId                types.String `tfsdk:"cluster_id"`
	ConnectionName           types.String `tfsdk:"name"`
	ConnectionUri            types.String `tfsdk:"uri"`
	ExternalConnectionRefUri types.String `tfsdk:"ref_uri"`
	Id                       types.String `tfsdk:"id"`
}

func (r *ExternalConnectionResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_external_connection"
}

func buildExternalConnectionId(clusterId string, connectionName string) string {
	return clusterId + "|" + connectionName
}

func parseExternalConnectionId(id string) (clusterId string, connectionName string, err error) {
	parts := strings.Split(id, "|")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid external connection ID")
	}
	return parts[0], parts[1], nil
}

func (r *ExternalConnectionResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "External connection",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Connection name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"uri": schema.StringAttribute{
				MarkdownDescription: "Connection URI",
				Required:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				Computed: true,
				Required: false,
				Optional: false,
			},
			"ref_uri": schema.StringAttribute{
				Computed: true,
				Required: false,
				Optional: false,
			},
		},
	}
}

func getExternalConnectionUri(connectionName string) string {
	return fmt.Sprintf("external://%s", connectionName)
}

func (r *ExternalConnectionResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *ExternalConnectionResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data ExternalConnectionResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	connectionUri := data.ConnectionUri.ValueString()
	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION %s as %s", pgx.Identifier{data.ConnectionName.ValueString()}.Sanitize(), pgx.Identifier{connectionUri}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to create external connection", err.Error())
		return
	}

	data.Id = types.StringValue(buildExternalConnectionId(data.ClusterId.ValueString(), data.ConnectionName.ValueString()))
	data.ExternalConnectionRefUri = types.StringValue(getExternalConnectionUri(data.ConnectionName.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

/**
 * Parse the CREATE EXTERNAL CONNECTION statement to extract the connection name and URI
 * ex: CREATE EXTERNAL CONNECTION myconn AS 'postgresql://user:password@host:port/dbname'
 */
func (r *ExternalConnectionResource) parseCreateConnectionStatement(connectionStatement string) (connectionName string, connectionUri string, err error) {
	// remove the CREATE EXTERNAL CONNECTION prefix
	connectionStatement = strings.TrimPrefix(connectionStatement, "CREATE EXTERNAL CONNECTION ")

	// split the statement into the connection name and URI
	parts := strings.Split(connectionStatement, " AS ")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("unable to parse connection statement")
	}

	return strings.Trim(strings.TrimSpace(parts[0]), "'"), strings.Trim(parts[1], "'"), nil
}

func (r *ExternalConnectionResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data ExternalConnectionResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	exConnStatement, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*string, error) {
		var connectionStatement string
		err := db.QueryRow(fmt.Sprintf("SHOW CREATE EXTERNAL CONNECTION %s", pgx.Identifier{data.ConnectionName.ValueString()}.Sanitize())).Scan(nil, &connectionStatement)
		if err != nil {
			return nil, err
		}
		return &connectionStatement, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to read external connection", err.Error())
		return
	}

	connectionName, connectionUri, err := r.parseCreateConnectionStatement(*exConnStatement)

	if err != nil {
		resp.Diagnostics.AddError("Unable to parse external connection statement", err.Error())
		return
	}

	data.ConnectionName = types.StringValue(connectionName)
	data.ConnectionUri = types.StringValue(connectionUri)
	data.ExternalConnectionRefUri = types.StringValue(getExternalConnectionUri(data.ConnectionName.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ExternalConnectionResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data ExternalConnectionResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), "defaultdb", func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("DROP EXTERNAL CONNECTION %s", pgx.Identifier{data.ConnectionName.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to drop external connection", err.Error())
		return
	}
}

func (r *ExternalConnectionResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError("Updating external connections is not supported", "Updating external connections is not supported")
}

func (r *ExternalConnectionResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	clusterId, connectionName, err := parseExternalConnectionId(req.ID)
	if err != nil {
		resp.Diagnostics.AddError("Invalid external connection ID", err.Error())
		return
	}

	exConnStatement, err := ccloud.SqlConWithTempUser(ctx, r.client, clusterId, "defaultdb", func(db *pgx.ConnPool) (*string, error) {
		var connectionStatement string
		err := db.QueryRow(fmt.Sprintf("SHOW CREATE EXTERNAL CONNECTION %s", pgx.Identifier{connectionName}.Sanitize())).Scan(&connectionStatement)
		if err != nil {
			return nil, err
		}
		return &connectionStatement, nil
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to read external connection", err.Error())
		return
	}

	connectionName, connectionUri, err := r.parseCreateConnectionStatement(*exConnStatement)

	if err != nil {
		resp.Diagnostics.AddError("Unable to parse external connection statement", err.Error())
		return
	}

	var data ExternalConnectionResourceModel
	data.ClusterId = types.StringValue(clusterId)
	data.ConnectionName = types.StringValue(connectionName)
	data.ConnectionUri = types.StringValue(connectionUri)
	data.ExternalConnectionRefUri = types.StringValue(getExternalConnectionUri(data.ConnectionName.ValueString()))
	data.Id = types.StringValue(req.ID)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

}
