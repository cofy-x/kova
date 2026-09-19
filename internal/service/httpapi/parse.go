package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/buildcontract"
	"github.com/cofy-x/kova/internal/source"
	"github.com/cofy-x/kova/internal/sourcebundle"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/labstack/echo/v4"
)

const maxCreateBuildRequestBytes = 1 << 20

type createBuildRequest struct {
	SourceURI      string
	SourceDigest   string
	Targets        []string
	Options        kovav1.KovaBuildOptions
	IdempotencyKey string
}

func buildRequestFromJSON(c echo.Context) (createBuildRequest, error) {
	mediaType, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return createBuildRequest{}, fmt.Errorf("content type must be application/json")
	}
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, maxCreateBuildRequestBytes)
	var body apiv1.CreateBuildRequest
	decoder := json.NewDecoder(c.Request().Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		return createBuildRequest{}, fmt.Errorf("decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return createBuildRequest{}, fmt.Errorf("request body must contain one JSON object")
	}
	if body.SourceURI != strings.TrimSpace(body.SourceURI) || body.SourceDigest != strings.TrimSpace(body.SourceDigest) {
		return createBuildRequest{}, fmt.Errorf("source_uri and source_digest must not contain surrounding whitespace")
	}
	if utf8.RuneCountInString(body.SourceURI) > apiv1.MaxSourceURILength {
		return createBuildRequest{}, fmt.Errorf("source_uri exceeds %d characters", apiv1.MaxSourceURILength)
	}
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
	if body.IdempotencyKey != strings.TrimSpace(body.IdempotencyKey) {
		return createBuildRequest{}, fmt.Errorf("idempotency_key must not contain surrounding whitespace")
	}
	if utf8.RuneCountInString(body.IdempotencyKey) > apiv1.MaxIdempotencyKeyLength {
		return createBuildRequest{}, fmt.Errorf("idempotency_key is too long")
	}
	if len(body.Variables) > apiv1.MaxBuildVariables {
		return createBuildRequest{}, fmt.Errorf("variables must contain at most %d entries", apiv1.MaxBuildVariables)
	}
	for _, value := range body.Variables {
		if utf8.RuneCountInString(value) > apiv1.MaxBuildVariableLength {
			return createBuildRequest{}, fmt.Errorf("variable exceeds %d characters", apiv1.MaxBuildVariableLength)
		}
	}
	if _, err := source.ParseBuildVariables(body.Variables); err != nil {
		return createBuildRequest{}, fmt.Errorf("variables must use unique KOVA_NAME=value entries")
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
		Targets: body.Targets, IdempotencyKey: body.IdempotencyKey,
		Options: kovav1.KovaBuildOptions{
			Format: format, Concurrency: body.Concurrency, Timeout: body.Timeout,
			OOMCooldown: oomCooldown, FailFast: body.FailFast, Verbose: body.Verbose,
			Vars: append([]string(nil), body.Variables...),
		},
	}, nil
}
