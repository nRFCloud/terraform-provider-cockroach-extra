package resources

import (
	"context"
	"errors"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/cockroachdb"
	"github.com/golang-migrate/migrate/source"
	"github.com/golang-migrate/migrate/source/file"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
	"github.com/nrfcloud/terraform-provider-cockroach-extra/internal/provider/ccloud"
)

type MigrationResource struct {
	client *ccloud.CcloudClient
}

type MigrationResourceModel struct {
	ClusterId           types.String `tfsdk:"cluster_id"`
	Database            types.String `tfsdk:"database"`
	MigrationsDirectory types.String `tfsdk:"migrations_directory"`
	DestroyMode         types.String `tfsdk:"destroy_mode"`
	Version             types.Int64  `tfsdk:"version"`
	Id                  types.String `tfsdk:"id"`
}

var _ resource.Resource = &MigrationResource{}

func NewMigrationResource() resource.Resource {
	return &MigrationResource{}
}

func (r *MigrationResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*ccloud.CcloudClient)

	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type", "The provider data was not of the expected type")
		return
	}

	r.client = client
}

func (r *MigrationResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				MarkdownDescription: "Cluster ID",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database": schema.StringAttribute{
				MarkdownDescription: "Database to apply the migration to",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"migrations_directory": schema.StringAttribute{
				MarkdownDescription: "Path to the migrations directory",
				Required:            true,
			},
			"destroy_mode": schema.StringAttribute{
				MarkdownDescription: "What to do when the resource is destroyed. 'noop' will do nothing, 'drop' will drop the database, 'down' will run all down migrations",
				Validators: []validator.String{
					stringvalidator.OneOf("noop", "drop", "down"),
				},
				Required: true,
			},
			"version": schema.Int64Attribute{
				MarkdownDescription: "What migration version should be applied. This should be the migration id number (integer prefix of the filename).",
				Optional:            false,
				Required:            true,
			},
			"id": schema.StringAttribute{
				Computed: true,
				Optional: false,
				Required: false,
			},
		},
	}
}

func (r *MigrationResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_migration"
}

func getSourceDriver(path string) (source.Driver, error) {
	driver := file.File{}
	return driver.Open(path)
}

func (r *MigrationResource) runMigrations(ctx context.Context, data *MigrationResourceModel) (*uint, error) {
	return ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), data.Database.ValueString(), func(db *pgx.ConnPool) (*uint, error) {
		stdDb := stdlib.OpenDBFromPool(db)
		driver, err := cockroachdb.WithInstance(stdDb, &cockroachdb.Config{})
		if err != nil {
			return nil, err
		}

		sourceDriver, err := getSourceDriver(data.MigrationsDirectory.ValueString())

		if err != nil {
			return nil, err
		}

		defer sourceDriver.Close()

		migrator, err := migrate.NewWithInstance(data.MigrationsDirectory.ValueString(), sourceDriver, data.Database.ValueString(), driver)

		if err != nil {
			return nil, err
		}

		err = migrator.Migrate(uint(data.Version.ValueInt64()))
		if err != nil {
			return nil, err
		}

		version, _, err := migrator.Version()

		return &version, err
	})
}

func (r *MigrationResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data MigrationResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	version, err := r.runMigrations(ctx, &data)

	if err != nil {
		resp.Diagnostics.AddError("Error running migrations", err.Error())
		return
	}

	data.Version = types.Int64Value(int64(*version))
	data.Id = types.StringValue(data.ClusterId.ValueString() + "|" + data.Database.ValueString() + "|migrations")

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *MigrationResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data MigrationResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	sourceDriver, err := getSourceDriver(data.MigrationsDirectory.ValueString())

	if err != nil {
		resp.Diagnostics.AddError("Error reading migrations", err.Error())
		return
	}
	defer sourceDriver.Close()

	remoteVersion, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), data.Database.ValueString(), func(db *pgx.ConnPool) (*uint, error) {
		dbDriver, err := cockroachdb.WithInstance(stdlib.OpenDBFromPool(db), &cockroachdb.Config{})
		if err != nil {
			return nil, err
		}
		migrator, err := migrate.NewWithInstance(data.MigrationsDirectory.ValueString(), sourceDriver, data.Database.ValueString(), dbDriver)
		if err != nil {
			return nil, err
		}
		version, _, err := migrator.Version()
		return &version, err
	})

	if err != nil {
		if !errors.Is(err, migrate.ErrNilVersion) {
			resp.Diagnostics.AddError("Error reading migrations", err.Error())
			return
		}
	}

	if remoteVersion != nil {
		data.Version = types.Int64Value(int64(*remoteVersion))
	} else {
		data.Version = types.Int64Value(int64(0))
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *MigrationResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data MigrationResourceModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	version, err := r.runMigrations(ctx, &data)

	if err != nil {
		resp.Diagnostics.AddError("Error running migrations", err.Error())
		return
	}

	data.Version = types.Int64Value(int64(*version))

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}

func (r *MigrationResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data MigrationResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.DestroyMode.ValueString() == "noop" {
		resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
		return
	}

	_, err := ccloud.SqlConWithTempUser(ctx, r.client, data.ClusterId.ValueString(), data.Database.ValueString(), func(db *pgx.ConnPool) (res *interface{}, err error) {
		stdDb := stdlib.OpenDBFromPool(db)
		driver, err := cockroachdb.WithInstance(stdDb, &cockroachdb.Config{})
		if err != nil {
			return nil, err
		}

		sourceDriver, err := getSourceDriver(data.MigrationsDirectory.ValueString())

		defer func(sourceDriver source.Driver) {
			err := sourceDriver.Close()
			if err != nil {
				return
			}
		}(sourceDriver)

		if err != nil {
			return nil, err
		}

		migrator, err := migrate.NewWithInstance(data.MigrationsDirectory.ValueString(), sourceDriver, data.Database.ValueString(), driver)
		if err != nil {
			return nil, err
		}
		if data.DestroyMode.ValueString() == "drop" {
			_, err = db.Exec("DROP DATABASE IF EXISTS " + pgx.Identifier{data.Database.ValueString()}.Sanitize())
			if err != nil {
				return nil, err
			}
			_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + pgx.Identifier{data.Database.ValueString()}.Sanitize())
		} else {
			err = migrator.Down()
		}

		return nil, err
	})

	if err != nil {
		resp.Diagnostics.AddError("Error running migrations", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, data)...)
}
