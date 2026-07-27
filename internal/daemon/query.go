package daemon

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cofy-x/kova/internal/batch"
	"github.com/cofy-x/kova/internal/scheduler"
	"github.com/cofy-x/kova/internal/source"
)

func validateQueryKeys(q url.Values, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	for key, values := range q {
		if _, ok := allowedSet[key]; !ok {
			return fmt.Errorf("unsupported query parameter %q", key)
		}
		if key == "var" || key == "target" {
			continue
		}
		if len(values) > 1 {
			return fmt.Errorf("query parameter %q must be specified at most once", key)
		}
	}
	return nil
}

func queryValue(q url.Values, key string) (string, bool, error) {
	values, ok := q[key]
	if !ok || len(values) == 0 {
		return "", false, nil
	}
	if len(values) > 1 {
		return "", false, fmt.Errorf("query parameter %q must be specified at most once", key)
	}
	return values[0], true, nil
}

func queryBoolStrict(q url.Values, key string, def bool) (bool, error) {
	v, ok, err := queryValue(q, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return def, nil
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("query parameter %q must be a boolean", key)
	}
}

func queryIntStrict(q url.Values, key string, def int, min int) (int, error) {
	v, ok, err := queryValue(q, key)
	if err != nil {
		return 0, err
	}
	if !ok || strings.TrimSpace(v) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("query parameter %q must be an integer", key)
	}
	if n < min {
		return 0, fmt.Errorf("query parameter %q must be greater than or equal to %d", key, min)
	}
	return n, nil
}

func queryDurationStrict(q url.Values, key string, def time.Duration, min time.Duration) (time.Duration, error) {
	v, ok, err := queryValue(q, key)
	if err != nil {
		return 0, err
	}
	if !ok || strings.TrimSpace(v) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("query parameter %q must be a duration", key)
	}
	if d < min {
		return 0, fmt.Errorf("query parameter %q must be greater than or equal to %s", key, min)
	}
	return d, nil
}

func buildOptionsFromQuery(q url.Values, defaultAddrs string, resultDB string, logsFile string) (batch.Options, error) {
	if err := validateQueryKeys(q, "addrs", "concurrency", "fail-fast", "format", "oom-cooldown", "timeout", "retry", "verbose", "target", "skip-fail", "var"); err != nil {
		return batch.Options{}, err
	}
	addrsValue, ok, err := queryValue(q, "addrs")
	if err != nil {
		return batch.Options{}, err
	}
	if !ok || strings.TrimSpace(addrsValue) == "" {
		addrsValue = defaultAddrs
	}
	addrs, err := scheduler.ParseAddrs(addrsValue)
	if err != nil {
		return batch.Options{}, err
	}
	concurrency, err := queryIntStrict(q, "concurrency", 1, 1)
	if err != nil {
		return batch.Options{}, err
	}
	timeout, err := queryIntStrict(q, "timeout", 300, 0)
	if err != nil {
		return batch.Options{}, err
	}
	retry, err := queryIntStrict(q, "retry", 0, 0)
	if err != nil {
		return batch.Options{}, err
	}
	oomCooldown, err := queryDurationStrict(q, "oom-cooldown", batch.DefaultBuildkitOOMCooldown, 0)
	if err != nil {
		return batch.Options{}, err
	}
	for _, addr := range addrs {
		if addr != nil {
			addr.Cooldown = oomCooldown
		}
	}
	failFast, err := queryBoolStrict(q, "fail-fast", false)
	if err != nil {
		return batch.Options{}, err
	}
	format, _, err := queryValue(q, "format")
	if err != nil {
		return batch.Options{}, err
	}
	format = source.NormalizeBuildFormatValue(format)
	if _, err := source.ParseBuildFormats(format); err != nil {
		return batch.Options{}, err
	}
	verbose, err := queryBoolStrict(q, "verbose", false)
	if err != nil {
		return batch.Options{}, err
	}
	skipFail, err := queryBoolStrict(q, "skip-fail", false)
	if err != nil {
		return batch.Options{}, err
	}
	target, _, err := queryValue(q, "target")
	if err != nil {
		return batch.Options{}, err
	}
	buildVars, err := source.ParseBuildVariables(q["var"])
	if err != nil {
		return batch.Options{}, err
	}
	return batch.Options{
		Addrs:       addrs,
		AddrsRaw:    addrsValue,
		Concurrency: concurrency,
		Failfast:    failFast,
		BuildFormat: format,
		OOMCooldown: oomCooldown,
		ResultPath:  resultDB,
		LogsPath:    logsFile,
		Vars:        buildVars,
		Timeout:     timeout,
		Retry:       retry,
		Verbose:     verbose,
		Target:      strings.TrimSpace(target),
		SkipFail:    skipFail,
	}, nil
}

func exportOptionsFromQuery(q url.Values, resultDB string) (batch.Options, error) {
	if err := validateQueryKeys(q, "oci", "with-fail", "target"); err != nil {
		return batch.Options{}, err
	}
	oci, err := queryBoolStrict(q, "oci", false)
	if err != nil {
		return batch.Options{}, err
	}
	withFail, err := queryBoolStrict(q, "with-fail", false)
	if err != nil {
		return batch.Options{}, err
	}
	targets, err := trimmedQueryValues(q["target"])
	if err != nil {
		return batch.Options{}, err
	}
	return batch.Options{
		FromResultPath: resultDB,
		OCI:            oci,
		WithFail:       withFail,
		ExportTargets:  targets,
	}, nil
}

func trimmedQueryValues(values []string) ([]string, error) {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value == "" {
			return nil, fmt.Errorf("query parameter %q must not be empty", "target")
		}
		result = append(result, value)
	}
	return result, nil
}

func preheatOptionsFromQuery(q url.Values, resultDB string) (batch.Options, error) {
	if err := validateQueryKeys(q, "target", "dragonfly-scheduler-addr", "concurrency", "interval", "timeout", "fail-fast", "oci", "verbose", "insecure-skip-verify"); err != nil {
		return batch.Options{}, err
	}
	schedulerAddr, ok, err := queryValue(q, "dragonfly-scheduler-addr")
	if err != nil {
		return batch.Options{}, err
	}
	if !ok || strings.TrimSpace(schedulerAddr) == "" {
		return batch.Options{}, fmt.Errorf("dragonfly-scheduler-addr is required")
	}
	concurrency, err := queryIntStrict(q, "concurrency", 1, 1)
	if err != nil {
		return batch.Options{}, err
	}
	interval, err := queryIntStrict(q, "interval", 5, 0)
	if err != nil {
		return batch.Options{}, err
	}
	timeout, err := queryIntStrict(q, "timeout", 5, 0)
	if err != nil {
		return batch.Options{}, err
	}
	failFast, err := queryBoolStrict(q, "fail-fast", false)
	if err != nil {
		return batch.Options{}, err
	}
	oci, err := queryBoolStrict(q, "oci", false)
	if err != nil {
		return batch.Options{}, err
	}
	verbose, err := queryBoolStrict(q, "verbose", false)
	if err != nil {
		return batch.Options{}, err
	}
	insecureSkipVerify, err := queryBoolStrict(q, "insecure-skip-verify", false)
	if err != nil {
		return batch.Options{}, err
	}
	return batch.Options{
		FromResultPath:            resultDB,
		Target:                    strings.TrimSpace(q.Get("target")),
		DragonflySchedulerAddr:    strings.TrimSpace(schedulerAddr),
		PreheatInsecureSkipVerify: insecureSkipVerify,
		Concurrency:               concurrency,
		Interval:                  interval,
		Timeout:                   timeout,
		Failfast:                  failFast,
		OCI:                       oci,
		Verbose:                   verbose,
	}, nil
}
