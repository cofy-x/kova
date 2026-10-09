# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

ARG UBUNTU_IMAGE=ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90
ARG BUILDKIT_ROOTLESS_IMAGE=moby/buildkit:v0.31.2-rootless@sha256:0eeb84626c0cd01aecae7848c5ed8f095aec279dd936d0cdb5a64110f42ca65b

FROM ${BUILDKIT_ROOTLESS_IMAGE} AS buildkit-rootless

FROM ${UBUNTU_IMAGE} AS ubuntu-base
ARG APT_MIRROR=
RUN if [ -n "${APT_MIRROR}" ]; then \
      find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) \
        -exec sed -i -E \
          "s#https?://(archive|ports)\\.ubuntu\\.com/ubuntu(-ports)?/?#${APT_MIRROR%/}/#g" {} +; \
    fi

FROM ubuntu-base AS downloader-base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*

FROM downloader-base AS grpcurl-downloader
ARG GRPCURL_VERSION=v1.9.3
ARG GRPCURL_SHA256_AMD64=a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5
ARG GRPCURL_SHA256_ARM64=b20a00c1cb82ab81ec32696766d4076e99b4cb5ca0823a71767ba64dbea0f263
ARG TARGETOS=linux
ARG TARGETARCH
ARG GRPCURL_DOWNLOAD_BASE_URL=https://github.com/fullstorydev/grpcurl/releases/download
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) grpcurl_arch="x86_64"; grpcurl_sha256="${GRPCURL_SHA256_AMD64}" ;; \
      arm64) grpcurl_arch="arm64"; grpcurl_sha256="${GRPCURL_SHA256_ARM64}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "${GRPCURL_DOWNLOAD_BASE_URL}/${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION#v}_${TARGETOS}_${grpcurl_arch}.tar.gz" \
      -o /tmp/grpcurl.tgz; \
    echo "${grpcurl_sha256}  /tmp/grpcurl.tgz" | sha256sum -c -; \
    mkdir -p /out; \
    tar -xzf /tmp/grpcurl.tgz -C /out grpcurl

FROM downloader-base AS nydus-downloader
ARG NYDUS_VERSION=v2.4.4
ARG NYDUS_SHA256_AMD64=e95a0d1984ef507c0cf4c7766a9cde8433215e1007556e03d4cc211725c68c59
ARG NYDUS_SHA256_ARM64=c9bfe001991b6da8ebb4303d1f7bf24034c913550d318330416cf07527e500f4
ARG TARGETOS=linux
ARG TARGETARCH
ARG NYDUS_DOWNLOAD_BASE_URL=https://github.com/dragonflyoss/image-service/releases/download
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) nydus_arch="amd64"; nydus_sha256="${NYDUS_SHA256_AMD64}" ;; \
      arm64) nydus_arch="arm64"; nydus_sha256="${NYDUS_SHA256_ARM64}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "${NYDUS_DOWNLOAD_BASE_URL}/${NYDUS_VERSION}/nydus-static-${NYDUS_VERSION}-${TARGETOS}-${nydus_arch}.tgz" \
      -o /tmp/nydus.tgz; \
    echo "${nydus_sha256}  /tmp/nydus.tgz" | sha256sum -c -; \
    mkdir -p /out; \
    tar -xzf /tmp/nydus.tgz -C /out --strip-components=1 nydus-static/nydus-image

FROM downloader-base AS go-toolchain
ARG GO_VERSION=1.26.9
ARG GO_SHA256_AMD64=42d158b4d8f7b61ac0a830567c940a86098fb7aac52e467a5ebec03ef5cc2f8d
ARG GO_SHA256_ARM64=4a97373d49fcacdcf3694fea368a500b00ee3e963974f3e7514132717632f052
ARG TARGETOS=linux
ARG TARGETARCH
ARG GO_DOWNLOAD_BASE_URL=https://go.dev/dl
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) go_arch="amd64"; go_sha256="${GO_SHA256_AMD64}" ;; \
      arm64) go_arch="arm64"; go_sha256="${GO_SHA256_ARM64}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "${GO_DOWNLOAD_BASE_URL}/go${GO_VERSION}.${TARGETOS}-${go_arch}.tar.gz" \
      -o /tmp/go.tgz; \
    echo "${go_sha256}  /tmp/go.tgz" | sha256sum -c -; \
    tar -C /usr/local -xzf /tmp/go.tgz
ENV GOPATH=/go

FROM go-toolchain AS nydusify-builder
ARG NYDUSIFY_SOURCE_REVISION=4e908d8c7b6db8e5cac25ecf8b8cc1113dbacccb
ARG NYDUSIFY_SOURCE_SHA256=965888bc400bdae08ca1e26698b4ac81f43ef85ecac6092d91117feeb61558e4
ARG NYDUSIFY_SOURCE_BASE_URL=https://codeload.github.com/dragonflyoss/nydus/tar.gz
ARG TARGETOS=linux
ARG TARGETARCH
ARG GOPROXY=https://proxy.golang.org,direct
RUN apt-get update \
    && apt-get install -y --no-install-recommends patch \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "${NYDUSIFY_SOURCE_BASE_URL}/${NYDUSIFY_SOURCE_REVISION}" -o /tmp/nydus-source.tgz; \
    echo "${NYDUSIFY_SOURCE_SHA256}  /tmp/nydus-source.tgz" | sha256sum -c -; \
    mkdir -p /src; \
    tar -xzf /tmp/nydus-source.tgz -C /src; \
    test -f "/src/nydus-${NYDUSIFY_SOURCE_REVISION}/contrib/nydusify/go.mod"
COPY docker/patches/nydusify-v2.4.4-push-digest.patch /tmp/nydusify-push-digest.patch
RUN patch -p1 -d "/src/nydus-${NYDUSIFY_SOURCE_REVISION}" < /tmp/nydusify-push-digest.patch
COPY docker/patches/nydusify-push-digest_test.go.txt /src/nydus-${NYDUSIFY_SOURCE_REVISION}/contrib/nydusify/pkg/converter/push_digest_test.go
WORKDIR /src/nydus-${NYDUSIFY_SOURCE_REVISION}/contrib/nydusify
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOPROXY="${GOPROXY}" GOMAXPROCS=4 CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      /usr/local/go/bin/go test -p 4 ./pkg/converter -run '^(TestPushedDigestProvider|TestDumpMetric)$' -count=1 && \
    GOPROXY="${GOPROXY}" GOMAXPROCS=4 CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      /usr/local/go/bin/go build -p 4 -trimpath \
        -ldflags "-s -w -X main.revision=${NYDUSIFY_SOURCE_REVISION} -X main.gitVersion=v2.4.4+kova.1" \
        -o /out/nydusify ./cmd/nydusify.go

FROM ubuntu-base AS runtime-builder
ARG TARGETOS=linux
ARG TARGETARCH
ENV GOPATH=/go
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl build-essential liblmdb-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY --from=go-toolchain /usr/local/go /usr/local/go
WORKDIR /src
COPY go.mod go.sum ./
ARG GOPROXY=https://proxy.golang.org,direct
RUN --mount=type=cache,target=/go/pkg/mod \
    GOPROXY="${GOPROXY}" /usr/local/go/bin/go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOPROXY="${GOPROXY}" CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} /usr/local/go/bin/go build \
      -trimpath \
      -tags 'netgo,osusergo' \
      -ldflags "-s -w -linkmode external -extldflags '-static' \
        -X github.com/cofy-x/kova/internal/version.Version=${VERSION} \
        -X github.com/cofy-x/kova/internal/version.Commit=${COMMIT} \
        -X github.com/cofy-x/kova/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/kovad ./cmd/kovad && \
    GOPROXY="${GOPROXY}" CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} /usr/local/go/bin/go build \
      -trimpath \
      -tags 'netgo,osusergo' \
      -ldflags "-s -w -linkmode external -extldflags '-static' \
        -X github.com/cofy-x/kova/internal/version.Version=${VERSION} \
        -X github.com/cofy-x/kova/internal/version.Commit=${COMMIT} \
        -X github.com/cofy-x/kova/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/kova-controller ./cmd/kova-controller

FROM scratch AS runtime-artifact
COPY --from=runtime-builder /out/kovad /kovad

FROM ubuntu-base AS controller
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/cofy-x/kova" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="Kova controller" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 kova \
    && useradd --system --uid 65532 --gid 65532 --key SYS_UID_MAX=65532 --home-dir /home/kova \
        --create-home --shell /usr/sbin/nologin kova
COPY --from=runtime-builder /out/kova-controller /usr/bin/kova-controller
USER 65532:65532
ENTRYPOINT ["kova-controller", "service"]

FROM ubuntu-base AS runner
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/cofy-x/kova" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="Kova runner" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 65532 kova \
    && useradd --system --uid 65532 --gid 65532 --key SYS_UID_MAX=65532 --home-dir /home/kova \
        --create-home --shell /usr/sbin/nologin kova
COPY --from=buildkit-rootless /usr/bin/buildctl /usr/bin/buildctl
COPY --from=grpcurl-downloader /out/grpcurl /usr/bin/grpcurl
COPY --from=nydusify-builder /out/nydusify /usr/bin/nydusify
COPY --from=nydus-downloader /out/nydus-image /usr/bin/nydus-image
COPY --from=nydusify-builder /src/nydus-4e908d8c7b6db8e5cac25ecf8b8cc1113dbacccb/LICENSE-APACHE /usr/share/doc/nydusify/LICENSE-APACHE
COPY --from=runtime-builder /out/kovad /usr/bin/kovad
ENV HOME=/home/kova
USER 65532:65532
ENTRYPOINT ["kovad", "daemon"]

FROM buildkit-rootless AS worker
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/cofy-x/kova" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.title="Kova worker" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
