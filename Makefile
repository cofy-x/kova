SHELL := /bin/bash

GO ?= go
HOST_GOOS := $(shell $(GO) env GOOS)
HOST_GOARCH := $(shell $(GO) env GOARCH)
DOCKER_ARCH := $(shell docker info --format '{{.Architecture}}' 2>/dev/null | sed -e 's/aarch64/arm64/' -e 's/x86_64/amd64/')

BINARY := ./bin/kova
RUNTIME_BINARY := ./bin/kovad
PKG := ./cmd/kova
RUNTIME_PKG := ./cmd/kovad
TAGS ?= netgo,osusergo
INSTALL_LDFLAGS ?=

KOVA_GOOS ?= $(HOST_GOOS)
KOVA_GOARCH ?= $(HOST_GOARCH)
CGO_ENABLED ?= 1
CC ?= cc

ifeq ($(KOVA_GOOS),linux)
LDFLAGS ?= -linkmode external -extldflags '-static'
else
LDFLAGS ?=
endif

BUILDKIT_VERSION ?= v0.30.0
GRPCURL_VERSION ?= v1.9.3
NYDUS_VERSION ?= v2.4.3
DRAGONFLY_CHART_VERSION ?= 1.6.27
NYDUS_SNAPSHOTTER_CHART_VERSION ?= 0.0.10
BUILDKIT_ADDR ?= tcp://kova.kova.svc:9094
GO_VERSION ?= 1.25.5
IMAGE ?= localhost:5002/kova:dev
CONTROLLER_GEN_VERSION ?= v0.19.0

KIND_CLUSTER ?= kova-local
KIND_NODE_IMAGE ?= kindest/node:v1.31.2
KIND_CONFIG ?= deploy/kind-cluster.yaml
KIND_KUBECONFIG ?= .kind/$(KIND_CLUSTER).kubeconfig
KIND_WORKERS ?= 3

REGISTRY_NAME ?= kind-registry
REGISTRY_IMAGE ?= registry:2
REGISTRY_HOST ?= localhost:5002
REGISTRY_PORT ?= 5002
CLUSTER_REGISTRY ?= host.docker.internal:5002

RELEASE_NAME ?= kova
NAMESPACE ?= kova
WORK_DIR ?= .work
RUNNER_NAME ?= e2e
SOURCE_ZIP ?= $(WORK_DIR)/source.zip
RESULT_JSONL ?= $(WORK_DIR)/result.jsonl
CONCURRENT_RUNNER_NAME ?= e2e-concurrent
CONCURRENT_SOURCE_ZIP ?= $(WORK_DIR)/source-concurrent.zip
CONCURRENT_RESULT_JSONL ?= $(WORK_DIR)/result-concurrent.jsonl
NYDUS_RESULT_JSONL ?= $(WORK_DIR)/result-nydus.jsonl
RUNTIME_OCI_SOURCE_ZIP ?= $(WORK_DIR)/source-runtime-oci.zip
RUNTIME_NYDUS_SOURCE_ZIP ?= $(WORK_DIR)/source-runtime-nydus.zip
RUNTIME_OCI_RESULT_JSONL ?= $(WORK_DIR)/result-runtime-oci.jsonl
RUNTIME_NYDUS_RESULT_JSONL ?= $(WORK_DIR)/result-runtime-nydus.jsonl
EXAMPLE_COUNT ?= 12
BUILD_CONCURRENCY ?= 4

export BUILDKIT_VERSION GRPCURL_VERSION NYDUS_VERSION DRAGONFLY_CHART_VERSION NYDUS_SNAPSHOTTER_CHART_VERSION BUILDKIT_ADDR GO_VERSION IMAGE KIND_CLUSTER KIND_NODE_IMAGE KIND_CONFIG KIND_KUBECONFIG KIND_WORKERS
export REGISTRY_NAME REGISTRY_IMAGE REGISTRY_HOST REGISTRY_PORT CLUSTER_REGISTRY
export RELEASE_NAME NAMESPACE WORK_DIR RUNNER_NAME SOURCE_ZIP RESULT_JSONL
export CONCURRENT_RUNNER_NAME CONCURRENT_SOURCE_ZIP CONCURRENT_RESULT_JSONL NYDUS_RESULT_JSONL RUNTIME_OCI_SOURCE_ZIP RUNTIME_NYDUS_SOURCE_ZIP RUNTIME_OCI_RESULT_JSONL RUNTIME_NYDUS_RESULT_JSONL EXAMPLE_COUNT BUILD_CONCURRENCY

.PHONY: all kova kovad kova-linux kova-host install install-kovad generate-crds image kind-registry kind-create kind-load deploy-kind diagnose-kind observability-up observability-down observability-status dragonfly-nydus-install e2e e2e-service e2e-concurrent e2e-dragonfly-nydus e2e-runtime-preflight e2e-runtime e2e-observability clean clean-kind test lint-scripts helm-template package-example package-concurrent-example FORCE

all: kova

kova: $(BINARY)

kovad: $(RUNTIME_BINARY)

kova-linux:
	KOVA_GOOS=linux KOVA_GOARCH=$(or $(DOCKER_ARCH),$(HOST_GOARCH)) ./scripts/build/build-binary-linux.sh

kova-host:
	KOVA_GOOS=$(HOST_GOOS) KOVA_GOARCH=$(HOST_GOARCH) $(MAKE) kova

install:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) CC=$(CC) \
		$(GO) install -tags '$(TAGS)' -ldflags "$(INSTALL_LDFLAGS)" $(PKG)

install-kovad:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) CC=$(CC) \
		$(GO) install -tags '$(TAGS)' -ldflags "$(INSTALL_LDFLAGS)" $(RUNTIME_PKG)

$(BINARY): FORCE
	mkdir -p $(dir $(BINARY))
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(KOVA_GOOS) GOARCH=$(KOVA_GOARCH) CC=$(CC) \
		$(GO) build -tags '$(TAGS)' -ldflags "$(LDFLAGS)" -o $(BINARY) $(PKG)

$(RUNTIME_BINARY): FORCE
	mkdir -p $(dir $(RUNTIME_BINARY))
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(KOVA_GOOS) GOARCH=$(KOVA_GOARCH) CC=$(CC) \
		$(GO) build -tags '$(TAGS)' -ldflags "$(LDFLAGS)" -o $(RUNTIME_BINARY) $(RUNTIME_PKG)

generate-crds:
	$(GO) run sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION) object crd paths=./internal/apis/kova/v1alpha1 output:crd:artifacts:config=charts/kova/crds

FORCE:

image:
	./scripts/build/build-image.sh

kind-registry:
	./scripts/kind/kind-registry.sh

kind-create:
	./scripts/kind/kind-create.sh

kind-load:
	./scripts/kind/kind-load.sh

deploy-kind:
	./scripts/kind/deploy-kind.sh

diagnose-kind:
	./scripts/kind/diagnose-kind.sh

observability-up:
	./scripts/observability/local-up.sh

observability-down:
	./scripts/observability/local-down.sh

observability-status:
	./scripts/observability/local-status.sh

dragonfly-nydus-install:
	./scripts/runtime/dragonfly-nydus-install.sh

package-example:
	./scripts/package/package-example.sh

package-concurrent-example:
	SOURCE_ZIP=$(CONCURRENT_SOURCE_ZIP) ./scripts/package/package-concurrent-example.sh

$(SOURCE_ZIP):
	./scripts/package/package-example.sh

e2e:
	./scripts/e2e/e2e.sh

e2e-service:
	SOURCE_ZIP=$(WORK_DIR)/source-service.zip RESULT_JSONL=$(WORK_DIR)/result-service.jsonl ./scripts/e2e/e2e-service.sh

e2e-concurrent:
	RUNNER_NAME=$(CONCURRENT_RUNNER_NAME) SOURCE_ZIP=$(CONCURRENT_SOURCE_ZIP) RESULT_JSONL=$(CONCURRENT_RESULT_JSONL) ./scripts/e2e/e2e-concurrent.sh

e2e-dragonfly-nydus:
	RESULT_JSONL=$(NYDUS_RESULT_JSONL) ./scripts/e2e/e2e-dragonfly-nydus.sh

e2e-runtime-preflight:
	./scripts/e2e/e2e-runtime-preflight.sh

e2e-runtime:
	OCI_SOURCE_ZIP=$(RUNTIME_OCI_SOURCE_ZIP) NYDUS_SOURCE_ZIP=$(RUNTIME_NYDUS_SOURCE_ZIP) OCI_RESULT_JSONL=$(RUNTIME_OCI_RESULT_JSONL) NYDUS_RESULT_JSONL=$(RUNTIME_NYDUS_RESULT_JSONL) ./scripts/e2e/e2e-runtime.sh

e2e-observability:
	./scripts/e2e/e2e-observability.sh

test:
	$(GO) test ./...

lint-scripts:
	find scripts -name '*.sh' -print0 | xargs -0 -n1 bash -n
	@if command -v shellcheck >/dev/null 2>&1; then \
		find scripts -name '*.sh' -print0 | xargs -0 shellcheck -x -e SC1091; \
	else \
		echo "shellcheck not found; skipping shellcheck"; \
	fi

helm-template:
	helm template $(RELEASE_NAME) ./charts/kova -f deploy/kind-values.yaml >/dev/null
	helm template $(RELEASE_NAME) ./charts/kova -f deploy/cloud-values.yaml >/dev/null
	helm template kova-observability ./charts/kova-observability \
		--namespace kova-observability \
		--set grafana.admin.existingSecret=test-secret >/dev/null

clean:
	rm -rf bin $(WORK_DIR) .generated

clean-kind:
	./scripts/kind/clean-kind.sh
