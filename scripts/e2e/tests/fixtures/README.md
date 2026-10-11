# BuildKit JSON fixtures

These synthetic fixtures were generated with Go `encoding/json` from the actual `github.com/moby/buildkit v0.31.2` types, not from Python dictionaries or a live build.
They contain no infrastructure identities or acceptance evidence.
The upstream [buildctl JSON template](https://github.com/moby/buildkit/blob/v0.31.2/cmd/buildctl/common/common.go) uses that same encoder.

The worker fixture serializes one [client.WorkerInfo](https://github.com/moby/buildkit/blob/v0.31.2/client/workers.go) with its pinned [client.BuildkitVersion](https://github.com/moby/buildkit/blob/v0.31.2/client/info.go); unused fields intentionally retain their encoded `null` values.
The history fixture serializes STARTED and COMPLETE [controlapi.BuildHistoryEvent](https://github.com/moby/buildkit/blob/v0.31.2/api/services/control/control.pb.go) values for the same synthetic solve.
Unlike `WorkerInfo`, that generated protobuf record has capitalized JSON tags for `Ref`, `Exporters`, timestamps and `ExporterResponse`, while the event and error tags are lowercase.
The zero-valued STARTED enum is omitted by `omitempty`, and `timestamppb.Timestamp` encodes integer seconds and nanos rather than an RFC3339 string.
