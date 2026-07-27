package httpapi

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/source"

	"github.com/labstack/echo/v4"
)

var sha256Pattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type createBuildRequest struct {
	Options        kovav1.KovaBuildOptions
	SourceDigest   string
	IdempotencyKey string
}

func buildRequestFromMultipart(c echo.Context) (createBuildRequest, error) {
	form, err := c.MultipartForm()
	if err != nil {
		return createBuildRequest{}, err
	}
	allowed := map[string]bool{
		"format": true, "target": true, "concurrency": true, "timeout": true, "retry": true,
		"fail-fast": true, "skip-fail": true, "verbose": true, "oom-cooldown": true, "var": true,
		"formats": true, "source_digest": true, "idempotency_key": true,
	}
	var request createBuildRequest
	for key, vals := range form.Value {
		if !allowed[key] {
			return createBuildRequest{}, fmt.Errorf("unsupported form field %q", key)
		}
		if key != "var" && len(vals) > 1 {
			return createBuildRequest{}, fmt.Errorf("form field %q must be specified at most once", key)
		}
		for _, value := range vals {
			switch key {
			case "source_digest":
				request.SourceDigest = strings.TrimSpace(value)
			case "idempotency_key":
				request.IdempotencyKey = strings.TrimSpace(value)
			case "formats":
				if form.Value["format"] != nil {
					return createBuildRequest{}, fmt.Errorf("form fields format and formats are mutually exclusive")
				}
				normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), " ", "")
				if normalized != "oci,nydus" && normalized != "nydus,oci" {
					return createBuildRequest{}, fmt.Errorf("form field %q must be oci,nydus", key)
				}
				request.Options.Format = "both"
			default:
				if err := setBuildOption(&request.Options, key, value); err != nil {
					return createBuildRequest{}, err
				}
			}
		}
	}
	if request.SourceDigest != "" && !sha256Pattern.MatchString(request.SourceDigest) {
		return createBuildRequest{}, fmt.Errorf("form field source_digest must be a lowercase sha256 digest")
	}
	if strings.TrimSpace(request.Options.Target) == "" {
		return createBuildRequest{}, fmt.Errorf("form field target is required")
	}
	if request.IdempotencyKey != "" {
		if request.SourceDigest == "" {
			return createBuildRequest{}, fmt.Errorf("idempotency_key requires source_digest")
		}
		if len(request.IdempotencyKey) > 256 {
			return createBuildRequest{}, fmt.Errorf("form field idempotency_key is too long")
		}
	}
	return request, nil
}

func setBuildOption(opts *kovav1.KovaBuildOptions, key string, value string) error {
	switch key {
	case "format":
		format := source.NormalizeBuildFormatValue(value)
		if _, err := source.ParseBuildFormats(format); err != nil {
			return err
		}
		opts.Format = format
	case "target":
		opts.Target = value
	case "concurrency":
		n, err := formInt(value, key, 1)
		if err != nil {
			return err
		}
		opts.Concurrency = n
	case "timeout":
		n, err := formInt(value, key, 0)
		if err != nil {
			return err
		}
		opts.Timeout = n
	case "retry":
		n, err := formInt(value, key, 0)
		if err != nil {
			return err
		}
		opts.Retry = n
	case "oom-cooldown":
		if strings.TrimSpace(value) != "" {
			d, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("form field %q must be a duration", key)
			}
			if d < 0 {
				return fmt.Errorf("form field %q must be greater than or equal to 0s", key)
			}
		}
		opts.OOMCooldown = value
	case "fail-fast":
		v, err := formBool(value, key)
		if err != nil {
			return err
		}
		opts.FailFast = v
	case "skip-fail":
		v, err := formBool(value, key)
		if err != nil {
			return err
		}
		opts.SkipFail = v
	case "verbose":
		v, err := formBool(value, key)
		if err != nil {
			return err
		}
		opts.Verbose = v
	case "var":
		if _, err := source.ParseBuildVariables([]string{value}); err != nil {
			return err
		}
		opts.Vars = append(opts.Vars, value)
	}
	return nil
}

func formInt(value string, key string, min int) (int, error) {
	if strings.TrimSpace(value) == "" {
		return 0, fmt.Errorf("form field %q must be an integer", key)
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("form field %q must be an integer", key)
	}
	if n < min {
		return 0, fmt.Errorf("form field %q must be greater than or equal to %d", key, min)
	}
	return n, nil
}

func formBool(value string, key string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("form field %q must be a boolean", key)
	}
}
