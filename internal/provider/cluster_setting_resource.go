package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/jackc/pgx"
	"strings"
)

var _ resource.Resource = &ClusterSettingResource{}
var _ resource.ResourceWithImportState = &ClusterSettingResource{}

func NewClusterSettingResource() resource.Resource {
	return &ClusterSettingResource{}
}

type ClusterSettingResource struct {
	client *CcloudClient
}

type ClusterSettingResourceModel struct {
	ClusterId    types.String `tfsdk:"cluster_id"`
	SettingName  types.String `tfsdk:"setting_name"`
	SettingValue types.String `tfsdk:"setting_value"`
	Id           types.String `tfsdk:"id"`
}

func (r *ClusterSettingResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_cluster_setting"
}

func (r *ClusterSettingResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
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
			"setting_name": schema.StringAttribute{
				MarkdownDescription: "Setting name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"setting_value": schema.StringAttribute{
				MarkdownDescription: "Setting value",
				Required:            true,
			},
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Cluster setting ID",
				Optional:            false,
				Required:            false,
			},
		},
	}
}

func (r *ClusterSettingResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*CcloudClient)

	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type",
			fmt.Sprintf("Expected *CcloudClient, got: %T. Please report this issue to the provider developers.", req.ProviderData))
		return
	}

	r.client = client
}

func (r *ClusterSettingResource) setClusterSetting(ctx context.Context, clusterId string, settingName string, settingValue string) error {
	_, err := SqlConWithTempUser(ctx, r.client, clusterId, func(db *pgx.Conn) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("SET CLUSTER SETTING %s = $1", pgx.Identifier{settingName}.Sanitize()), settingValue)
		return nil, err
	})

	return err
}

func (r *ClusterSettingResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data ClusterSettingResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Debug(ctx, fmt.Sprintf("Setting cluster setting %s with value %s for cluster %s", data.SettingName, data.SettingValue, data.ClusterId))
	err := r.setClusterSetting(ctx, data.ClusterId.ValueString(), data.SettingName.ValueString(), data.SettingValue.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to set cluster setting", err.Error())
		return
	}

	data.Id = types.StringValue(fmt.Sprintf("%s|%s", data.ClusterId.ValueString(), data.SettingName.ValueString()))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ClusterSettingResource) getClusterSetting(ctx context.Context, clusterId string, settingName string) (*string, error) {
	return SqlConWithTempUser(ctx, r.client, clusterId, func(db *pgx.Conn) (*string, error) {
		var value string

		// Funky query that casts the resulting type to a string
		err := db.QueryRow(fmt.Sprintf("with x as (show cluster setting %s) select value::TEXT from x as t(value)", pgx.Identifier{settingName}.Sanitize())).Scan(&value)

		if err != nil {
			return nil, err
		}
		return &value, nil
	})
}

func (r *ClusterSettingResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data ClusterSettingResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	settingRow, err := r.getClusterSetting(ctx, data.ClusterId.ValueString(), data.SettingName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Unable to get cluster setting", err.Error())
		return
	}

	data.SettingValue = types.StringValue(*settingRow)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ClusterSettingResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data ClusterSettingResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	data.Id = types.StringValue(fmt.Sprintf("%s|%s", data.ClusterId.ValueString(), data.SettingName.ValueString()))

	if resp.Diagnostics.HasError() {
		return
	}

	err := r.setClusterSetting(ctx, data.ClusterId.ValueString(), data.SettingName.ValueString(), data.SettingValue.ValueString())

	if err != nil {
		resp.Diagnostics.AddError("Unable to set cluster setting", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ClusterSettingResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data ClusterSettingResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	_, err := SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), func(db *pgx.Conn) (*interface{}, error) {
		_, err := db.Exec(fmt.Sprintf("RESET CLUSTER SETTING %s", pgx.Identifier{data.SettingName.ValueString()}.Sanitize()))
		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Unable to reset cluster setting", err.Error())
		return
	}
}

func (r *ClusterSettingResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	var data ClusterSettingResourceModel

	idParts := strings.Split(req.ID, "|")
	if len(idParts) != 2 {
		resp.Diagnostics.AddError("Invalid ID", fmt.Sprintf("Expected ID to be in the format <cluster_id>|<setting_name>, got: %s", req.ID))
		return
	}
	clusterId := idParts[0]
	settingName := idParts[1]

	settingRow, err := r.getClusterSetting(ctx, clusterId, settingName)

	if err != nil {
		resp.Diagnostics.AddError("Unable to get cluster setting", err.Error())
		return
	}

	data = ClusterSettingResourceModel{
		ClusterId:    types.StringValue(clusterId),
		SettingName:  types.StringValue(settingName),
		SettingValue: types.StringValue(*settingRow),
		Id:           types.StringValue(req.ID),
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}
}
