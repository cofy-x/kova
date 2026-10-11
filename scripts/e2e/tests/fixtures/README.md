# BuildKit JSON fixtures

These synthetic fixtures were generated with Go `encoding/json` from the actual `github.com/moby/buildkit v0.31.2` types, not from Python dictionaries or a live build.
They contain no infrastructure identities or acceptance evidence.
The upstream [buildctl JSON template](https://github.com/moby/buildkit/blob/v0.31.2/cmd/buildctl/common/common.go) uses that same encoder.

The worker fixture serializes one [client.WorkerInfo](https://github.com/moby/buildkit/blob/v0.31.2/client/workers.go) with its pinned [client.BuildkitVersion](https://github.com/moby/buildkit/blob/v0.31.2/client/info.go); unused fields intentionally retain their encoded `null` values.
The history fixture serializes STARTED and COMPLETE [controlapi.BuildHistoryEvent](https://github.com/moby/buildkit/blob/v0.31.2/api/services/control/control.pb.go) values for the same synthetic solve.
Unlike `WorkerInfo`, that generated protobuf record has capitalized JSON tags for `Ref`, `Exporters`, timestamps and `ExporterResponse`, while the event and error tags are lowercase.
The zero-valued STARTED enum is omitted by `omitempty`, and `timestamppb.Timestamp` encodes integer seconds and nanos rather than an RFC3339 string.
Its COMPLETE record follows the [history producer](https://github.com/moby/buildkit/blob/v0.31.2/solver/llbsolver/history.go): `ExporterResponse` copies frontend result metadata, not the image exporter's returned digest map.
The image [exporter returns the same actual root descriptor](https://github.com/moby/buildkit/blob/v0.31.2/exporter/containerimage/export.go) both as `SolveResponse.ExporterResponse["containerimage.digest"]` and as a descriptor reference for history.
The history producer stores that descriptor at `Result.Results[exporterIndex]`, so the single-exporter map encodes as `Result.Results["0"]` with lowercase descriptor tags.
`ResultDeprecated` duplicates that first exporter descriptor, while `Attestations` contains separate provenance descriptors that must not be mistaken for the output.
The [generator example](buildkit-v0.31.2-histories.go.txt) uses those actual protobuf types and the queue's `MarshalVT`/`UnmarshalVT` persistence serialization before JSON encoding.
Rename it to `fixture.go` in a separate Go module pinned to BuildKit v0.31.2 to regenerate the two JSON lines; no live daemon or build is involved.
