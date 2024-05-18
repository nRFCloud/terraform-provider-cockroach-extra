package provider

import (
	"context"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/resources"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure CockroachExtraProvider satisfies various provider interfaces.
var _ provider.Provider = &CockroachExtraProvider{}
var _ provider.ProviderWithMetaSchema

// CockroachExtraProvider defines the provider implementation.
type CockroachExtraProvider struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// CockroachExtraProviderModel describes the provider data model.
type CockroachExtraProviderModel struct {
	ApiKey types.String `tfsdk:"api_key"`
}

func (p *CockroachExtraProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "cockroach-extra"
	resp.Version = p.version
}

func (p *CockroachExtraProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"api_key": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Cockroach Cloud API key",
				Sensitive:           true,
			},
		},
	}
}

func (p *CockroachExtraProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data CockroachExtraProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	// Configuration values are now available.
	// if data.Endpoint.IsNull() { /* ... */ }
	if data.ApiKey.IsUnknown() {
		resp.Diagnostics.AddAttributeError(
			path.Root("api_key"),
			"Unknown Cockroach Cloud api key",
			"Please set the Cockroach Cloud api key in the provider configuration block or COCKROACH_API_KEY.",
		)
		return
	}

	apiKey := os.Getenv("COCKROACH_API_KEY")

	if !data.ApiKey.IsNull() {
		apiKey = data.ApiKey.ValueString()
	}

	if apiKey == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("api_key"),
			"Unknown Cockroach Cloud api key",
			"Please set the Cockroach Cloud api key in the provider configuration block or COCKROACH_API_KEY.",
		)
		return
	}

	// Example client configuration for data sources and resources
	client := ccloud.NewCcloudClient(ctx, apiKey)
	resp.DataSourceData = client
	resp.ResourceData = client
}

func (p *CockroachExtraProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		resources.NewClusterSettingResource,
		resources.NewRoleGrantResource,
		resources.NewSqlUserResource,
		resources.NewSqlRoleResource,
		resources.NewMigrationResource,
		resources.NewExternalConnectionResource,
		resources.NewChangefeedResource,
		resources.NewPersistentCursorResource,
	}
}

func (p *CockroachExtraProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		//NewExampleDataSource,
	}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &CockroachExtraProvider{
			version: version,
		}
	}
}
