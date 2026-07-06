// Package runner executes JobSpecs as Docker containers. This is the ONLY
// execution path on a provider machine — never run raw host commands.
package runner

import (
	"context"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// Update is a job lifecycle event emitted by a Runner.
type Update struct {
	JobID    string
	State    computev1.JobState
	LogChunk string
	ExitCode int32
	Err      string
}

// Runner runs jobs. Interface exists so tests use a fake instead of Docker.
type Runner interface {
	// Run executes the job and streams Updates until a terminal state
	// (SUCCEEDED / FAILED / CANCELLED) is sent, then closes the channel.
	// Cancellation of ctx must kill the container and emit CANCELLED.
	Run(ctx context.Context, spec *computev1.JobSpec) (<-chan Update, error)
}
