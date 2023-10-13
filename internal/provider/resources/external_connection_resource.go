package resources

// TODO: Implement the external connection resource

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
)

type ExternalConnectionResource struct {
	client *ccloud.CcloudClient
}

type ExternalConnectionResourceModel struct {
	ClusterId      types.String `tfsdk:"cluster_id"`
	ConnectionName types.String `tfsdk:"connection_name"`
	ConnectionUri  types.String `tfsdk:"connection_uri"`
	Id             types.String `tfsdk:"id"`
}

func (r *ExternalConnectionResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_external_connection"
}

func buildExternalConnectionId(clusterId string, connectionName string) string {
	return clusterId + "|" + connectionName
}

//func parseExternalConnectionId(id string) (clusterId string, connectionName string) {
//	parts := strings.Split(id, "|")
//	return parts[0], parts[1]
//}

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
			"connection_name": schema.StringAttribute{
				MarkdownDescription: "Connection name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"connection_uri": schema.StringAttribute{
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
		},
	}
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

func (r *ExternalConnectionResource) Create(ctx context.Context, req resource.CreateRequest, resp resource.CreateResponse) {
	var data ExternalConnectionResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), func(db *pgx.ConnPool) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION %s as $1", pgx.Identifier{data.ConnectionName.ValueString()}.Sanitize()), data.ConnectionUri.ValueString())
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to create external connection", err.Error())
		return
	}

	data.Id = types.StringValue(buildExternalConnectionId(data.ClusterId.ValueString(), data.ConnectionName.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ExternalConnectionResource) Read(ctx context.Context, req resource.ReadRequest, resp resource.ReadResponse) {
	var data ExternalConnectionResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

}
