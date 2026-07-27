# syntax=docker/dockerfile:1

ARG UBUNTU_VERSION=24.04

FROM ubuntu:${UBUNTU_VERSION} AS buildkit-downloader
ARG BUILDKIT_VERSION=v0.30.0
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|arm64) buildkit_arch="${TARGETARCH}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "https://github.com/moby/buildkit/releases/download/${BUILDKIT_VERSION}/buildkit-${BUILDKIT_VERSION}.${TARGETOS}-${buildkit_arch}.tar.gz" \
      -o /tmp/buildkit.tgz; \
    mkdir -p /out; \
    tar -xzf /tmp/buildkit.tgz -C /out

FROM ubuntu:${UBUNTU_VERSION} AS grpcurl-downloader
ARG GRPCURL_VERSION=v1.9.3
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) grpcurl_arch="x86_64" ;; \
      arm64) grpcurl_arch="arm64" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "https://github.com/fullstorydev/grpcurl/releases/download/${GRPCURL_VERSION}/grpcurl_${GRPCURL_VERSION#v}_${TARGETOS}_${grpcurl_arch}.tar.gz" \
      -o /tmp/grpcurl.tgz; \
    mkdir -p /out; \
    tar -xzf /tmp/grpcurl.tgz -C /out grpcurl

FROM ubuntu:${UBUNTU_VERSION} AS nydus-downloader
ARG NYDUS_VERSION=v2.4.3
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|arm64) nydus_arch="${TARGETARCH}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "https://github.com/dragonflyoss/image-service/releases/download/${NYDUS_VERSION}/nydus-static-${NYDUS_VERSION}-${TARGETOS}-${nydus_arch}.tgz" \
      -o /tmp/nydus.tgz; \
    mkdir -p /out; \
    tar -xzf /tmp/nydus.tgz -C /out --strip-components=1 nydus-static/nydusify nydus-static/nydus-image

FROM ubuntu:${UBUNTU_VERSION} AS kova-builder
ARG GO_VERSION=1.25.5
ARG TARGETOS=linux
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl build-essential liblmdb-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64|arm64) go_arch="${TARGETARCH}" ;; \
      *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fL --retry 5 --retry-all-errors --connect-timeout 20 --max-time 600 \
      "https://go.dev/dl/go${GO_VERSION}.${TARGETOS}-${go_arch}.tar.gz" \
      -o /tmp/go.tgz; \
    tar -C /usr/local -xzf /tmp/go.tgz
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    /usr/local/go/bin/go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} /usr/local/go/bin/go build \
    -tags 'netgo,osusergo' \
    -ldflags "-linkmode external -extldflags '-static'" \
    -o /out/kova ./cmd/kova \
    && CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} /usr/local/go/bin/go build \
      -tags 'netgo,osusergo' \
      -ldflags "-linkmode external -extldflags '-static'" \
      -o /out/kovad ./cmd/kovad

FROM scratch AS kova-artifact
COPY --from=kova-builder /out/kova /kova
COPY --from=kova-builder /out/kovad /kovad

FROM ubuntu:${UBUNTU_VERSION}
RUN export DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC \
    && apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl iptables liblmdb0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=buildkit-downloader /out/bin/ /usr/bin/
COPY --from=grpcurl-downloader /out/grpcurl /usr/bin/grpcurl
COPY --from=nydus-downloader /out/nydusify /usr/bin/nydusify
COPY --from=nydus-downloader /out/nydus-image /usr/bin/nydus-image
COPY --from=kova-builder /out/kova /usr/bin/kova
COPY --from=kova-builder /out/kovad /usr/bin/kovad
ENTRYPOINT []
CMD ["buildkitd", "--addr", "tcp://0.0.0.0:9094"]
