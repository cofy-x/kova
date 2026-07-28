# Kova Design

Kova is a cloud-provider-neutral Kubernetes image build system. Its public
interfaces and runtime boundaries follow these rules.

## Runtime Roles

Kova publishes three Linux container roles from one source version:

- `controller`: runs `kova-controller`, the authenticated service API and
  `KovaBuild` controller
- `runner`: executes one isolated build job and owns its transient state
- `worker`: runs rootless BuildKit and owns build cache and execution capacity

The `kova` client is not included in runtime images. It is distributed as a
CGO-free workstation binary and as a Go module command.

Each role has an independent image target and least-privilege security
context. Deployments select a role explicitly; no image changes behavior based
on an implicit default command.

## Job API

`KovaBuild` is the canonical job model. Its spec is immutable after creation.
Status reports `observedGeneration`, Kubernetes Conditions, bounded result
summaries, timestamps, and the runner identity. Large logs and result sets are
stored as artifacts rather than embedded without limit in the CRD.

The service API creates and queries `KovaBuild` resources. The CLI may operate
directly on an explicitly selected runner for development, but it uses the
same typed daemon protocol as the controller.

## Storage

Source archives and result artifacts use a provider-neutral object interface.
Supported drivers are:

- `filesystem` for local and shared-filesystem environments
- `s3` for any S3-compatible object service

Jobs identify immutable objects by URI and SHA-256 digest. Runner init logic
materializes the source into a job-local volume, so production scheduling does
not require the controller and runner to share a ReadWriteMany filesystem.
Credentials are referenced through Kubernetes Secrets and never stored in a
`KovaBuild` object.

## Internal Protocol

Runner communication uses one typed Go client for the daemon HTTP API over its
Unix socket. Kubernetes exec invokes a hidden `kovad` transport command; it
does not construct `curl` command lines. API paths, status decoding, error
handling, cancellation, and streaming behavior therefore have one owner.

## Authentication

The service API requires an explicit authentication mode. Production defaults
to Kubernetes TokenReview authentication. Static bearer tokens are available
for controlled integration environments. An unauthenticated mode requires an
explicit unsafe setting and is never the chart default.

## Scheduling And Reliability

Build targets enter a fair queue. Capacity is bounded by the configured worker
slots, and a single batch cannot consume every slot while another runnable
batch waits. Worker health, recent failures, and cooldown state influence
runner placement. Cancellation propagates through every subprocess and queued
task.

## Observability

Kova exposes stable metrics for queue latency, build latency, capacity waits,
cancellations, artifact operations, authentication denials, and terminal
outcomes. Metrics use bounded labels. Health endpoints distinguish process
liveness from dependency readiness.

## Release Contract

Every semantic-version tag publishes all CLI archives and runtime roles from
the same commit. CI verifies the six CLI targets, each runtime image, generated
manifests, and an installation-and-version smoke test. Releases include
checksums, SBOMs, and provenance, and are created only after anonymous pulls of
the published runtime images succeed.
