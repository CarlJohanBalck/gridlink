package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// logFlushInterval coalesces container output to <= 1 LogChunk update per
	// interval so a chatty job cannot flood the coordinator stream.
	logFlushInterval = 500 * time.Millisecond
	// cleanupTimeout bounds container removal, which runs on a background
	// context because the job context is usually already dead.
	cleanupTimeout   = 30 * time.Second
	updateBufferSize = 16
)

// dockerAPI is the slice of the Docker SDK the runner uses; tests fake it.
type dockerAPI interface {
	ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerLogs(ctx context.Context, containerID string, options container.LogsOptions) (io.ReadCloser, error)
	ContainerWait(ctx context.Context, containerID string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error)
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
}

// DockerRunner implements Runner using the local Docker daemon. Containers
// get no host mounts, no privileged mode, and no host network — the JobSpec
// is image + command + env + limits, nothing else.
type DockerRunner struct {
	api dockerAPI
	log *slog.Logger
}

func NewDockerRunner(log *slog.Logger) (*DockerRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("docker daemon unreachable: %w", err)
	}
	return &DockerRunner{api: cli, log: log}, nil
}

var _ Runner = (*DockerRunner)(nil)

func (d *DockerRunner) Run(ctx context.Context, spec *computev1.JobSpec) (<-chan Update, error) {
	if spec.GetImage() == "" {
		return nil, errors.New("job spec has no image")
	}
	updates := make(chan Update, updateBufferSize)
	go d.run(ctx, spec, updates)
	return updates, nil
}

// run drives one job from pull to terminal state, then closes updates.
func (d *DockerRunner) run(ctx context.Context, spec *computev1.JobSpec, updates chan<- Update) {
	defer close(updates)

	jobID := spec.GetJobId()
	log := d.log.With("job_id", jobID, "image", spec.GetImage())
	send := func(u Update) {
		u.JobID = jobID
		updates <- u
	}
	fail := func(err error) {
		// A dead context is the job being cancelled or timed out, not a
		// container failure; report it as such even if the underlying Docker
		// call surfaced its own error first.
		send(terminalForCtx(ctx, err))
	}

	if t := spec.GetTimeoutS(); t > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(t)*time.Second)
		defer cancel()
	}

	send(Update{State: computev1.JobState_JOB_STATE_PENDING})

	if err := d.pull(ctx, spec.GetImage(), func(status string) {
		send(Update{State: computev1.JobState_JOB_STATE_PENDING, LogChunk: status})
	}); err != nil {
		fail(err)
		return
	}

	containerID, err := d.create(ctx, spec)
	if err != nil {
		fail(err)
		return
	}
	defer d.cleanup(containerID, log)

	if err := d.api.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		fail(fmt.Errorf("start container: %w", err))
		return
	}
	send(Update{State: computev1.JobState_JOB_STATE_RUNNING})
	log.Info("container running", "container_id", containerID)

	logs := newLogCoalescer(logFlushInterval, func(chunk string) {
		send(Update{State: computev1.JobState_JOB_STATE_RUNNING, LogChunk: chunk})
	})
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)
		d.streamLogs(ctx, containerID, logs)
	}()
	// Every exit path below drains the log stream before the terminal update
	// so trailing output is not lost.
	finishLogs := func() {
		<-logsDone
		logs.stop()
	}

	waitCh, errCh := d.api.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case <-ctx.Done():
		finishLogs()
		fail(ctx.Err())
	case err := <-errCh:
		finishLogs()
		fail(fmt.Errorf("wait for container: %w", err))
	case res := <-waitCh:
		finishLogs()
		switch {
		case res.Error != nil:
			send(Update{State: computev1.JobState_JOB_STATE_FAILED, Err: res.Error.Message})
		case res.StatusCode == 0:
			send(Update{State: computev1.JobState_JOB_STATE_SUCCEEDED, ExitCode: 0})
		default:
			send(Update{
				State:    computev1.JobState_JOB_STATE_FAILED,
				ExitCode: int32(res.StatusCode),
				Err:      fmt.Sprintf("container exited with code %d", res.StatusCode),
			})
		}
	}
}

// terminalForCtx maps a failure to its terminal update: CANCELLED when the
// job was cancelled, FAILED otherwise (including timeout).
func terminalForCtx(ctx context.Context, err error) Update {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return Update{
			State: computev1.JobState_JOB_STATE_FAILED,
			Err:   "job timed out",
		}
	case ctx.Err() != nil:
		return Update{State: computev1.JobState_JOB_STATE_CANCELLED}
	default:
		return Update{State: computev1.JobState_JOB_STATE_FAILED, Err: err.Error()}
	}
}

// pull downloads the image, forwarding each distinct progress status once —
// sparse by design; per-layer byte counts would flood the stream.
func (d *DockerRunner) pull(ctx context.Context, ref string, progress func(string)) error {
	rc, err := d.api.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull %s: %w", ref, err)
	}
	defer rc.Close()

	dec := json.NewDecoder(rc)
	seen := make(map[string]bool)
	for {
		var m struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("pull %s: %w", ref, err)
		}
		if m.Error != "" {
			return fmt.Errorf("pull %s: %s", ref, m.Error)
		}
		if m.Status != "" && !seen[m.Status] {
			seen[m.Status] = true
			progress(m.Status + "\n")
		}
	}
}

func (d *DockerRunner) create(ctx context.Context, spec *computev1.JobSpec) (string, error) {
	env := make([]string, 0, len(spec.GetEnv()))
	for k, v := range spec.GetEnv() {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)

	host := &container.HostConfig{
		Resources: container.Resources{
			Memory: int64(spec.GetMemoryLimitMb()) * 1024 * 1024,
		},
	}
	if spec.GetGpu() {
		// Equivalent of `docker run --gpus all`.
		host.Resources.DeviceRequests = []container.DeviceRequest{{
			Count:        -1,
			Capabilities: [][]string{{"gpu"}},
		}}
	}

	resp, err := d.api.ContainerCreate(ctx,
		&container.Config{
			Image: spec.GetImage(),
			Cmd:   spec.GetCommand(),
			Env:   env,
		},
		host, nil, nil, "gridlink-job-"+spec.GetJobId())
	if err != nil {
		return "", fmt.Errorf("create container: %w", err)
	}
	return resp.ID, nil
}

// streamLogs copies the container's demultiplexed stdout+stderr into w until
// the container exits or ctx dies. Job containers have no TTY, so the stream
// arrives stdcopy-multiplexed.
func (d *DockerRunner) streamLogs(ctx context.Context, containerID string, w io.Writer) {
	rc, err := d.api.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		d.log.Warn("could not attach container logs", "container_id", containerID, "err", err)
		return
	}
	defer rc.Close()
	if _, err := stdcopy.StdCopy(w, w, rc); err != nil && ctx.Err() == nil {
		d.log.Debug("container log stream ended", "container_id", containerID, "err", err)
	}
}

// cleanup force-removes the container (killing it if still running) on a
// background context: the job context is typically already cancelled here.
func (d *DockerRunner) cleanup(containerID string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if err := d.api.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
		log.Warn("failed to remove container", "container_id", containerID, "err", err)
	}
}

// logCoalescer is an io.Writer that batches writes and emits the buffered
// text at most once per interval, plus a final flush on stop.
type logCoalescer struct {
	mu   sync.Mutex
	buf  strings.Builder
	emit func(string)
	done chan struct{}
	wg   sync.WaitGroup
}

func newLogCoalescer(interval time.Duration, emit func(string)) *logCoalescer {
	c := &logCoalescer{emit: emit, done: make(chan struct{})}
	c.wg.Add(1)
	go c.loop(interval)
	return c
}

func (c *logCoalescer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Write(p)
	return len(p), nil
}

func (c *logCoalescer) loop(interval time.Duration) {
	defer c.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			c.flush()
			return
		case <-t.C:
			c.flush()
		}
	}
}

func (c *logCoalescer) flush() {
	c.mu.Lock()
	s := c.buf.String()
	c.buf.Reset()
	c.mu.Unlock()
	if s != "" {
		c.emit(s)
	}
}

// stop flushes remaining output and stops the flusher. Call exactly once,
// after all writers have finished.
func (c *logCoalescer) stop() {
	close(c.done)
	c.wg.Wait()
}
