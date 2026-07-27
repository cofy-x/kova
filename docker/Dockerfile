# syntax=docker/dockerfile:1@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89

ARG UBUNTU_IMAGE=ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90

FROM ${UBUNTU_IMAGE} AS buildkit-downloader
ARG BUILDKIT_VERSION=v0.31.2
ARG BUILDKIT_SHA256_AMD64=fbabdb72433a35f5bb646e4cd424bf8567e5d055710cf55840f7af2020640791
ARG BUILDKIT_SHA256_ARM64=41fba1eed480376934fa4c8177ddd7021036b5168a0eb8e7ab5eccdf75d47a05
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
ARG BUILDKIT_DOWNLOAD_BASE_URL=https://github.com/moby/buildkit/releases/download
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) buildkit_arch="amd64"; buildkit_sha256="${BUILDKIT_SHA256_AMD64}" ;; \
      arm64) buildkit_arch="arm64"; buildkit_sha256="${BUILDKIT_SHA256_ARM64}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "${BUILDKIT_DOWNLOAD_BASE_URL}/${BUILDKIT_VERSION}/buildkit-${BUILDKIT_VERSION}.${TARGETOS}-${buildkit_arch}.tar.gz" \
      -o /tmp/buildkit.tgz; \
    echo "${buildkit_sha256}  /tmp/buildkit.tgz" | sha256sum -c -; \
    mkdir -p /out; \
    tar -xzf /tmp/buildkit.tgz -C /out

FROM ${UBUNTU_IMAGE} AS grpcurl-downloader
ARG GRPCURL_VERSION=v1.9.3
ARG GRPCURL_SHA256_AMD64=a926b62a85787ccf73ef8736b3ae554f1242e39d92bb8767a79d6dd23b11d1d5
ARG GRPCURL_SHA256_ARM64=b20a00c1cb82ab81ec32696766d4076e99b4cb5ca0823a71767ba64dbea0f263
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
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

FROM ${UBUNTU_IMAGE} AS nydus-downloader
ARG NYDUS_VERSION=v2.4.4
ARG NYDUS_SHA256_AMD64=e95a0d1984ef507c0cf4c7766a9cde8433215e1007556e03d4cc211725c68c59
ARG NYDUS_SHA256_ARM64=c9bfe001991b6da8ebb4303d1f7bf24034c913550d318330416cf07527e500f4
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
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
    tar -xzf /tmp/nydus.tgz -C /out --strip-components=1 nydus-static/nydusify nydus-static/nydus-image

FROM ${UBUNTU_IMAGE} AS runtime-builder
ARG GO_VERSION=1.26.5
ARG GO_SHA256_AMD64=5c2c3b16caefa1d968a94c1daca04a7ca301a496d9b086e17ad77bb81393f053
ARG GO_SHA256_ARM64=fe4789e92b1f33358680864bbe8704289e7bb5fc207d80623c308935bd696d49
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl build-essential liblmdb-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
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
WORKDIR /src
COPY go.mod go.sum ./
ARG GOPROXY=https://proxy.golang.org,direct
RUN --mount=type=cache,target=/go/pkg/mod \
    GOPROXY="${GOPROXY}" /usr/local/go/bin/go mod download
COPY cmd ./cmd
COPY internal ./internal
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
      -o /out/kovad ./cmd/kovad

FROM scratch AS runtime-artifact
COPY --from=runtime-builder /out/kovad /kovad

FROM ${UBUNTU_IMAGE}
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.source="https://github.com/cofy-x/kova" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}"
RUN export DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC \
    && apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl iptables liblmdb0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=buildkit-downloader /out/bin/ /usr/bin/
COPY --from=grpcurl-downloader /out/grpcurl /usr/bin/grpcurl
COPY --from=nydus-downloader /out/nydusify /usr/bin/nydusify
COPY --from=nydus-downloader /out/nydus-image /usr/bin/nydus-image
COPY --from=runtime-builder /out/kovad /usr/bin/kovad
ENTRYPOINT []
CMD ["buildkitd", "--addr", "tcp://0.0.0.0:9094"]
