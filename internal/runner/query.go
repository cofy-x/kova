package runner

import (
	"fmt"
	"net/url"
	"strings"
)

func BuildQuery(args []string) (string, error) {
	values := url.Values{}
	var target string
	for i := 0; i < len(args); {
		arg := args[i]
		switch {
		case arg == "--image-dirs" || strings.HasPrefix(arg, "--image-dirs=") ||
			arg == "--addrs" || strings.HasPrefix(arg, "--addrs=") ||
			arg == "--result" || strings.HasPrefix(arg, "--result=") ||
			arg == "--logs" || strings.HasPrefix(arg, "--logs="):
			return "", fmt.Errorf("build manages %s automatically", arg)
		case arg == "--registry" || strings.HasPrefix(arg, "--registry="):
			return "", fmt.Errorf("build no longer supports %s; use --var KOVA_*=...", arg)
		case arg == "--var":
			if i+1 >= len(args) {
				return "", fmt.Errorf("--var requires a value")
			}
			values.Add("var", args[i+1])
			i += 2
		case strings.HasPrefix(arg, "--var="):
			values.Add("var", strings.TrimPrefix(arg, "--var="))
			i++
		case arg == "--target":
			if i+1 >= len(args) {
				return "", fmt.Errorf("--target requires a value")
			}
			if target != "" {
				return "", fmt.Errorf("build accepts either --target or a positional target, not both")
			}
			target = args[i+1]
			i += 2
		case strings.HasPrefix(arg, "--target="):
			if target != "" {
				return "", fmt.Errorf("build accepts either --target or a positional target, not both")
			}
			target = strings.TrimPrefix(arg, "--target=")
			i++
		case arg == "--fail-fast" || arg == "--skip-fail" || arg == "--verbose":
			values.Set(strings.TrimPrefix(arg, "--"), "true")
			i++
		case arg == "--format" || arg == "--concurrency" || arg == "--oom-cooldown" || arg == "--timeout" || arg == "--retry":
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s requires a value", arg)
			}
			values.Set(strings.TrimPrefix(arg, "--"), args[i+1])
			i += 2
		case strings.HasPrefix(arg, "--format=") ||
			strings.HasPrefix(arg, "--concurrency=") || strings.HasPrefix(arg, "--oom-cooldown=") ||
			strings.HasPrefix(arg, "--timeout=") || strings.HasPrefix(arg, "--retry="):
			key, value, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
			values.Set(key, value)
			i++
		case strings.HasPrefix(arg, "--"):
			return "", fmt.Errorf("unknown build flag: %s", arg)
		default:
			if target != "" {
				return "", fmt.Errorf("build accepts at most one positional target")
			}
			target = arg
			i++
		}
	}
	if target != "" {
		values.Set("target", target)
	}
	return values.Encode(), nil
}

func ExportQuery(args []string) (string, string, error) {
	values := url.Values{}
	localResult := "result.jsonl"
	for i := 0; i < len(args); {
		arg := args[i]
		switch {
		case arg == "--result":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("--result requires a value")
			}
			localResult = args[i+1]
			i += 2
		case strings.HasPrefix(arg, "--result="):
			localResult = strings.TrimPrefix(arg, "--result=")
			i++
		case arg == "--target":
			if i+1 >= len(args) {
				return "", "", fmt.Errorf("--target requires a value")
			}
			values.Add("target", args[i+1])
			i += 2
		case strings.HasPrefix(arg, "--target="):
			values.Add("target", strings.TrimPrefix(arg, "--target="))
			i++
		case arg == "--oci" || arg == "--with-fail":
			values.Set(strings.TrimPrefix(arg, "--"), "true")
			i++
		case strings.HasPrefix(arg, "--"):
			return "", "", fmt.Errorf("unknown export flag: %s", arg)
		default:
			return "", "", fmt.Errorf("export does not accept positional arguments: %s", arg)
		}
	}
	if strings.TrimSpace(localResult) == "" {
		return "", "", fmt.Errorf("local export result path must not be empty")
	}
	return values.Encode(), localResult, nil
}

func PreheatQuery(args []string) (string, error) {
	values := url.Values{}
	for i := 0; i < len(args); {
		arg := args[i]
		switch {
		case arg == "--fail-fast" || arg == "--oci" || arg == "--verbose":
			values.Set(strings.TrimPrefix(arg, "--"), "true")
			i++
		case arg == "--target" || arg == "--dragonfly-scheduler-addr" || arg == "--concurrency" || arg == "--interval" || arg == "--timeout" || arg == "--insecure-skip-verify":
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s requires a value", arg)
			}
			values.Set(strings.TrimPrefix(arg, "--"), args[i+1])
			i += 2
		case strings.HasPrefix(arg, "--target=") || strings.HasPrefix(arg, "--dragonfly-scheduler-addr=") || strings.HasPrefix(arg, "--concurrency=") ||
			strings.HasPrefix(arg, "--interval=") || strings.HasPrefix(arg, "--timeout=") || strings.HasPrefix(arg, "--insecure-skip-verify="):
			key, value, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
			values.Set(key, value)
			i++
		case strings.HasPrefix(arg, "--"):
			return "", fmt.Errorf("unknown preheat flag: %s", arg)
		default:
			return "", fmt.Errorf("preheat does not accept positional arguments: %s", arg)
		}
	}
	return values.Encode(), nil
}
