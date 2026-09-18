package httpapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/serviceapi"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"

	"github.com/labstack/echo/v4"
)

type createBuildRequest struct {
	SourceURI      string
	SourceDigest   string
	Targets        []string
	Options        kovav1.KovaBuildOptions
	IdempotencyKey string
}

func buildRequestFromJSON(c echo.Context) (createBuildRequest, error) {
	var body serviceapi.CreateBuildRequest
	decoder := json.NewDecoder(c.Request().Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return createBuildRequest{}, fmt.Errorf("decode request: %w", err)
	}
	body.SourceURI = strings.TrimSpace(body.SourceURI)
	body.SourceDigest = strings.TrimSpace(body.SourceDigest)
	if err := sourcebundle.Validate(body.SourceURI, body.SourceDigest); err != nil {
		return createBuildRequest{}, err
	}
	normalizedTargets, err := buildcontract.NormalizeTargets(body.Targets)
	if err != nil {
		return createBuildRequest{}, err
	}
	body.Targets = normalizedTargets
	format := strings.ToLower(strings.TrimSpace(body.Format))
	if format == "" {
		format = "oci"
	}
	if _, err := source.ParseBuildFormats(format); err != nil {
		return createBuildRequest{}, err
	}
	if err := buildcontract.ValidateConcurrency(body.Concurrency, len(body.Targets)); err != nil {
		return createBuildRequest{}, err
	}
	if body.Timeout < 0 {
		return createBuildRequest{}, fmt.Errorf("timeout must be non-negative")
	}
	if len(body.IdempotencyKey) > 256 {
		return createBuildRequest{}, fmt.Errorf("idempotency_key is too long")
	}
	for _, value := range body.Variables {
		if _, err := source.ParseBuildVariables([]string{value}); err != nil {
			return createBuildRequest{}, err
		}
	}
	oomCooldown := ""
	if strings.TrimSpace(body.OOMCooldown) != "" {
		duration, err := time.ParseDuration(body.OOMCooldown)
		if err != nil || duration < 0 {
			return createBuildRequest{}, fmt.Errorf("oom_cooldown must be a non-negative duration")
		}
		oomCooldown = body.OOMCooldown
	}
	return createBuildRequest{
		SourceURI: body.SourceURI, SourceDigest: body.SourceDigest,
		Targets: body.Targets, IdempotencyKey: strings.TrimSpace(body.IdempotencyKey),
		Options: kovav1.KovaBuildOptions{
			Format: format, Concurrency: body.Concurrency, Timeout: body.Timeout,
			OOMCooldown: oomCooldown, FailFast: body.FailFast, Verbose: body.Verbose,
			Vars: append([]string(nil), body.Variables...),
		},
	}, nil
}
