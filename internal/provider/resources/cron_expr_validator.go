package resources

import (
	"context"
	"github.com/gorhill/cronexpr"
	"github.com/hashicorp/terraform-plugin-framework-validators/helpers/validatordiag"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
)

type cronExpressionValidator struct{}

func (v cronExpressionValidator) Description(ctx context.Context) string {
	return v.MarkdownDescription(ctx)
}

func (v cronExpressionValidator) MarkdownDescription(_ context.Context) string {
	return "value must be a valid cron expression"
}

func (v cronExpressionValidator) ValidateString(ctx context.Context, request validator.StringRequest, response *validator.StringResponse) {
	if request.ConfigValue.IsNull() || request.ConfigValue.IsUnknown() {
		return
	}

	value := request.ConfigValue

	if _, err := cronexpr.Parse(value.ValueString()); err != nil {
		response.Diagnostics.Append(validatordiag.InvalidAttributeValueDiagnostic(
			request.Path,
			v.Description(ctx),
			value.String(),
		))
	}
}

func CronExpressionValidator() validator.String {
	return cronExpressionValidator{}
}
