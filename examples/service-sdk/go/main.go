package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	"github.com/cofy-x/kova/pkg/client"
)

const receiptSchema = "kova.seed-build-receipt/v1"

var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type settings struct {
	serviceURL   string
	token        string
	sourceURI    string
	sourceDigest string
	target       string
	platform     apiv1.Platform
	idempotency  string
	receiptPath  string
	recipeDigest string
	targetRole   string
	format       string
	waitTimeout  time.Duration
	pollInterval time.Duration
	caFile       string
	insecure     bool
}

type receiptOutput struct {
	Role           string         `json:"role"`
	Platform       apiv1.Platform `json:"platform"`
	Format         string         `json:"format"`
	Image          string         `json:"image"`
	ManifestDigest string         `json:"manifest_digest"`
	ImmutableRef   string         `json:"immutable_ref"`
}

type receipt struct {
	Schema         string                 `json:"schema"`
	RecipeDigest   string                 `json:"recipe_digest"`
	SourceURI      string                 `json:"source_uri"`
	SourceDigest   string                 `json:"source_digest"`
	KovaBuildID    string                 `json:"kova_build_id"`
	KovaVersion    string                 `json:"kova_version"`
	IdempotencyKey string                 `json:"idempotency_key"`
	Status         apiv1.JobStatus        `json:"status"`
	FailureCode    apiv1.BuildFailureCode `json:"failure_code,omitempty"`
	Outputs        []receiptOutput        `json:"outputs"`
}

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := loadSettings()
	if err != nil {
		return fail(1, "configuration error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kova, err := client.New(client.Config{
		BaseURL:  cfg.serviceURL,
		Token:    cfg.token,
		CAFile:   cfg.caFile,
		Insecure: cfg.insecure,
	})
	if err != nil {
		return fail(1, "create Kova client: %v", sanitize(err.Error(), cfg.token))
	}

	version, err := kova.Version(ctx)
	if err != nil {
		return reportError(err, cfg.token)
	}
	if version.APIVersion != apiv1.APIVersion {
		return fail(1, "Kova Service API %q is incompatible with this example", sanitize(version.APIVersion, cfg.token))
	}
	if err := kova.Ready(ctx); err != nil {
		return reportError(err, cfg.token)
	}
	created, err := kova.CreateBuild(ctx, apiv1.CreateBuildRequest{
		SourceURI:      cfg.sourceURI,
		SourceDigest:   cfg.sourceDigest,
		Targets:        []apiv1.TargetSpec{{Target: cfg.target, Platform: cfg.platform}},
		Format:         cfg.format,
		Concurrency:    1,
		IdempotencyKey: cfg.idempotency,
	})
	if err != nil {
		return reportError(err, cfg.token)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, cfg.waitTimeout)
	terminal, waitErr := kova.WaitBuild(waitCtx, created.ID, cfg.pollInterval)
	cancelWait()
	if waitErr != nil && terminal.ID == "" {
		if errors.Is(waitErr, context.DeadlineExceeded) {
			return fail(2, "timed out waiting for Kova build")
		}
		if errors.Is(waitErr, context.Canceled) {
			return fail(2, "wait for Kova build was cancelled")
		}
		return reportError(waitErr, cfg.token)
	}
	if !isTerminal(terminal.Status) {
		return fail(1, "Kova build returned unexpected status %q", terminal.Status)
	}

	results, err := kova.GetResults(ctx, terminal.ID)
	if err != nil {
		return reportError(err, cfg.token)
	}
	if err := validateResults(results, terminal.ID, cfg); err != nil {
		return fail(1, "invalid build results: %s", sanitize(err.Error(), cfg.token))
	}
	outputs, err := receiptOutputs(results.Outputs, cfg.targetRole, cfg.platform)
	if err != nil {
		return fail(1, "invalid verified output: %s", sanitize(err.Error(), cfg.token))
	}
	if terminal.Status == apiv1.JobStatusSucceeded && len(outputs) == 0 {
		return fail(1, "invalid build results: successful build has no verified outputs")
	}
	if terminal.Status == apiv1.JobStatusSucceeded {
		if err := validateSuccessfulFormats(outputs, cfg.format); err != nil {
			return fail(1, "invalid build results: %s", sanitize(err.Error(), cfg.token))
		}
	}
	idempotencyKey := results.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = cfg.idempotency
	}
	buildReceipt := receipt{
		Schema:         receiptSchema,
		RecipeDigest:   cfg.recipeDigest,
		SourceURI:      results.SourceURI,
		SourceDigest:   results.SourceDigest,
		KovaBuildID:    terminal.ID,
		KovaVersion:    version.Version,
		IdempotencyKey: idempotencyKey,
		Status:         terminal.Status,
		FailureCode:    terminal.FailureCode,
		Outputs:        outputs,
	}
	if err := writeReceipt(cfg.receiptPath, buildReceipt); err != nil {
		return fail(1, "write caller-owned receipt: %s", sanitize(err.Error(), cfg.token))
	}

	if terminal.Status != apiv1.JobStatusSucceeded {
		return fail(3, "Kova build %s ended with status %s (%s); saved %d verified output(s)", terminal.ID, terminal.Status, terminal.FailureCode, len(outputs))
	}
	for _, output := range outputs {
		fmt.Printf("%s %s %s\n", output.Format, output.ManifestDigest, output.ImmutableRef)
	}
	return 0
}

func loadSettings() (settings, error) {
	required := func(name string) (string, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return "", fmt.Errorf("%s is required", name)
		}
		return value, nil
	}

	var cfg settings
	var platform string
	requiredSettings := []struct {
		name        string
		destination *string
	}{
		{name: "KOVA_SERVICE_URL", destination: &cfg.serviceURL},
		{name: "KOVA_SERVICE_TOKEN", destination: &cfg.token},
		{name: "KOVA_SOURCE_URI", destination: &cfg.sourceURI},
		{name: "KOVA_SOURCE_DIGEST", destination: &cfg.sourceDigest},
		{name: "KOVA_TARGET", destination: &cfg.target},
		{name: "KOVA_PLATFORM", destination: &platform},
		{name: "KOVA_IDEMPOTENCY_KEY", destination: &cfg.idempotency},
		{name: "KOVA_RECEIPT_PATH", destination: &cfg.receiptPath},
		{name: "KOVA_RECIPE_DIGEST", destination: &cfg.recipeDigest},
	}
	for _, setting := range requiredSettings {
		value, err := required(setting.name)
		if err != nil {
			return settings{}, err
		}
		*setting.destination = value
	}
	cfg.platform = apiv1.Platform(platform)
	if cfg.platform != apiv1.PlatformLinuxAMD64 && cfg.platform != apiv1.PlatformLinuxARM64 {
		return settings{}, fmt.Errorf("KOVA_PLATFORM must be linux/amd64 or linux/arm64")
	}
	if !digestPattern.MatchString(cfg.sourceDigest) {
		return settings{}, fmt.Errorf("KOVA_SOURCE_DIGEST must be a SHA-256 digest")
	}
	if !digestPattern.MatchString(cfg.recipeDigest) {
		return settings{}, fmt.Errorf("KOVA_RECIPE_DIGEST must be a SHA-256 digest")
	}
	cfg.targetRole = envDefault("KOVA_TARGET_ROLE", "seed")
	cfg.format = envDefault("KOVA_BUILD_FORMAT", "oci")
	if cfg.format != "oci" && cfg.format != "nydus" && cfg.format != "both" {
		return settings{}, fmt.Errorf("KOVA_BUILD_FORMAT must be oci, nydus, or both")
	}
	var err error
	cfg.waitTimeout, err = secondsEnv("KOVA_WAIT_TIMEOUT_SECONDS", 600)
	if err != nil {
		return settings{}, err
	}
	cfg.pollInterval, err = secondsEnv("KOVA_POLL_INTERVAL_SECONDS", 2)
	if err != nil {
		return settings{}, err
	}
	cfg.caFile = strings.TrimSpace(os.Getenv("KOVA_SERVICE_CA_FILE"))
	cfg.insecure, err = boolEnv("KOVA_SERVICE_INSECURE")
	if err != nil {
		return settings{}, err
	}
	return cfg, nil
}

func envDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func secondsEnv(name string, fallback float64) (time.Duration, error) {
	raw := envDefault(name, strconv.FormatFloat(fallback, 'f', -1, 64))
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return 0, fmt.Errorf("%s must be a positive number of seconds", name)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func boolEnv(name string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "", "0", "false", "no", "off":
		return false, nil
	case "1", "true", "yes", "on":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be a boolean", name)
	}
}

func isTerminal(status apiv1.JobStatus) bool {
	return status == apiv1.JobStatusSucceeded || status == apiv1.JobStatusFailed || status == apiv1.JobStatusCancelled
}

func validateResults(results apiv1.BuildResults, buildID string, cfg settings) error {
	if results.ID != buildID {
		return fmt.Errorf("build ID does not match the requested build")
	}
	if results.SourceURI != cfg.sourceURI {
		return fmt.Errorf("source URI does not match the submitted request")
	}
	if results.SourceDigest != cfg.sourceDigest {
		return fmt.Errorf("source digest does not match the submitted request")
	}
	if results.IdempotencyKey != "" && results.IdempotencyKey != cfg.idempotency {
		return fmt.Errorf("idempotency key does not match the submitted request")
	}
	return nil
}

func receiptOutputs(outputs []apiv1.BuildOutput, role string, platform apiv1.Platform) ([]receiptOutput, error) {
	result := make([]receiptOutput, 0, len(outputs))
	seenFormats := make(map[string]struct{}, 2)
	for _, output := range outputs {
		if output.Platform != platform {
			return nil, fmt.Errorf("output %q has platform %q instead of %q", output.Image, output.Platform, platform)
		}
		if output.Format != "oci" && output.Format != "nydus" {
			return nil, fmt.Errorf("output %q has unsupported format %q", output.Image, output.Format)
		}
		if _, exists := seenFormats[output.Format]; exists {
			return nil, fmt.Errorf("multiple %s outputs were returned for one logical target", output.Format)
		}
		seenFormats[output.Format] = struct{}{}
		if !digestPattern.MatchString(output.ManifestDigest) || !strings.HasSuffix(output.ImmutableRef, "@"+output.ManifestDigest) {
			return nil, fmt.Errorf("output %q has inconsistent manifest facts", output.Image)
		}
		result = append(result, receiptOutput{
			Role: role, Platform: output.Platform, Format: output.Format, Image: output.Image,
			ManifestDigest: output.ManifestDigest, ImmutableRef: output.ImmutableRef,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		return outputSortKey(left) < outputSortKey(right)
	})
	return result, nil
}

func validateSuccessfulFormats(outputs []receiptOutput, requested string) error {
	want := map[string]bool{"oci": requested == "oci" || requested == "both", "nydus": requested == "nydus" || requested == "both"}
	got := make(map[string]bool, len(outputs))
	for _, output := range outputs {
		got[output.Format] = true
	}
	for _, format := range []string{"oci", "nydus"} {
		if got[format] != want[format] {
			return fmt.Errorf("successful build output formats do not match requested format %q", requested)
		}
	}
	return nil
}

func outputSortKey(output receiptOutput) string {
	formatRank := "1"
	if output.Format == "oci" {
		formatRank = "0"
	}
	return strings.Join([]string{output.Role, string(output.Platform), formatRank, output.Image, output.ManifestDigest, output.ImmutableRef}, "\x00")
}

func writeReceipt(path string, value receipt) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	encoder := json.NewEncoder(temporary)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func reportError(err error, token string) int {
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		return fail(1, "Kova API error: HTTP %d code=%s retryable=%t retry_after=%s message=%s", apiErr.StatusCode, apiErr.Code, apiErr.Retryable, apiErr.RetryAfter, sanitize(apiErr.Message, token))
	}
	return fail(1, "Kova request failed: %s", sanitize(err.Error(), token))
}

func sanitize(message, token string) string {
	if token == "" {
		return message
	}
	return strings.ReplaceAll(message, token, "[redacted]")
}

func fail(code int, format string, args ...any) int {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	return code
}
