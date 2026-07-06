package runner

import (
	"context"
	"log/slog"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// DockerRunner implements Runner using the local Docker daemon.
//
// Implementation plan (Phase 1):
//  - github.com/docker/docker/client with NegotiateAPIVersion.
//  - Pull image (emit PENDING, sparse pull-progress log chunks).
//  - ContainerCreate: no host mounts, no privileged, no host network.
//    If spec.Gpu, add DeviceRequests equivalent to `--gpus all`.
//  - Apply spec.MemoryLimitMb; enforce spec.TimeoutS via context deadline.
//  - Attach logs; forward stdout/stderr as LogChunk updates, coalesced to
//    <= 1 update / 500ms so the stream isn't flooded.
//  - On exit: SUCCEEDED if code 0 else FAILED; always remove the container.
type DockerRunner struct {
	log *slog.Logger
	// TODO(claude): docker client handle
}

func NewDockerRunner(log *slog.Logger) (*DockerRunner, error) {
	// TODO(claude): init docker client, ping daemon, detect nvidia toolkit.
	panic("not implemented")
}

func (d *DockerRunner) Run(ctx context.Context, spec *computev1.JobSpec) (<-chan Update, error) {
	// TODO(claude): implement per the plan above.
	panic("not implemented")
}

var _ Runner = (*DockerRunner)(nil)
