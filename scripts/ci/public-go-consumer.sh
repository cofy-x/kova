#!/usr/bin/env bash

set -euo pipefail

source "$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/../common.sh"

ROOT=$(repo_root)
SOURCE=${1:-local}

require_cmd go

if [[ "${SOURCE}" != local ]] &&
  [[ ! "${SOURCE}" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z.-]+))?$ ]]; then
  echo "usage: $0 [local|vX.Y.Z[-prerelease]]" >&2
  exit 2
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/kova-public-go-consumer.XXXXXX")
trap 'rm -rf "${work_dir}"' EXIT

cd "${work_dir}"
go mod init example.com/kova-public-go-consumer >/dev/null

if [[ "${SOURCE}" == local ]]; then
  go mod edit -require=github.com/cofy-x/kova@v0.0.0
  go mod edit -replace="github.com/cofy-x/kova=${ROOT}"
else
  go mod edit -require="github.com/cofy-x/kova@${SOURCE}"
fi

cat >main.go <<'EOF'
package main

import (
	"context"
	"net/http"
	"time"

	apiv1 "github.com/cofy-x/kova/pkg/api/v1"
	"github.com/cofy-x/kova/pkg/client"
)

type publicClient interface {
	Version(context.Context) (apiv1.VersionInfo, error)
	CheckCompatible(context.Context) error
	Ready(context.Context) error
	CreateBuild(context.Context, apiv1.CreateBuildRequest) (apiv1.BuildJob, error)
	GetBuild(context.Context, string) (apiv1.BuildJob, error)
	ListBuilds(context.Context) (apiv1.JobList, error)
	ListBuildsPage(context.Context, int, string) (apiv1.JobList, error)
	GetResults(context.Context, string) (apiv1.BuildResults, error)
	GetLogs(context.Context, string, int64) ([]byte, error)
	CancelBuild(context.Context, string) (apiv1.BuildJob, error)
	WaitBuild(context.Context, string, time.Duration) (apiv1.BuildJob, error)
}

var (
	_ error        = (*client.APIError)(nil)
	_ publicClient = (*client.Client)(nil)
	_              = client.New
)

func main() {
	_, err := client.New(client.Config{
		BaseURL:    "https://kova.example.com",
		HTTPClient: http.DefaultClient,
	})
	if err != nil {
		panic(err)
	}

	_ = apiv1.CreateBuildRequest{
		SourceURI:      "oci://registry.example.com/sources/example@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SourceDigest:   "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Targets:        []apiv1.TargetSpec{{Target: "registry.example.com/team/image:v1", Platform: apiv1.PlatformLinuxAMD64}},
		IdempotencyKey: "external-consumer-smoke",
	}
	_ = apiv1.BuildOutput{
		Image:          "registry.example.com/team/image:v1",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ImmutableRef:   "registry.example.com/team/image@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Platform:       apiv1.PlatformLinuxAMD64,
	}
	_ = client.APIError{
		StatusCode: http.StatusTooManyRequests,
		Code:       apiv1.ErrorCodeQueueCapacityExceeded,
		Retryable:  true,
		RetryAfter: time.Second,
	}
}
EOF

go mod tidy
go build -trimpath -o "${work_dir}/consumer" .

if [[ "${SOURCE}" != local ]]; then
  mkdir -p "${work_dir}/bin"
  GOBIN="${work_dir}/bin" CGO_ENABLED=0 go install "github.com/cofy-x/kova/cmd/kova@${SOURCE}"
  "${work_dir}/bin/kova" version | grep -F "${SOURCE}" >/dev/null
fi
