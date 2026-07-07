package runner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// fakeDocker implements dockerAPI in memory.
type fakeDocker struct {
	mu sync.Mutex

	pullStream string // raw JSON progress lines returned by ImagePull
	pullErr    error
	createErr  error
	startErr   error
	logStream  []byte // stdcopy-multiplexed bytes returned by ContainerLogs
	waitCode   int64
	waitErr    error // delivered on ContainerWait's error channel
	waitBlock  bool  // never resolve ContainerWait (for cancel/timeout tests)

	createdConfig *container.Config
	createdHost   *container.HostConfig
	createdName   string
	removed       bool
}

func (f *fakeDocker) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	if f.pullErr != nil {
		return nil, f.pullErr
	}
	return io.NopCloser(strings.NewReader(f.pullStream)), nil
}

func (f *fakeDocker) ContainerCreate(_ context.Context, config *container.Config, hostConfig *container.HostConfig, _ *network.NetworkingConfig, _ *ocispec.Platform, name string) (container.CreateResponse, error) {
	if f.createErr != nil {
		return container.CreateResponse{}, f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createdConfig = config
	f.createdHost = hostConfig
	f.createdName = name
	return container.CreateResponse{ID: "cid-1"}, nil
}

func (f *fakeDocker) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	return f.startErr
}

func (f *fakeDocker) ContainerLogs(_ context.Context, _ string, _ container.LogsOptions) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.logStream)), nil
}

func (f *fakeDocker) ContainerWait(ctx context.Context, _ string, _ container.WaitCondition) (<-chan container.WaitResponse, <-chan error) {
	waitCh := make(chan container.WaitResponse, 1)
	errCh := make(chan error, 1)
	if f.waitBlock {
		return waitCh, errCh // resolved only by the runner's ctx
	}
	if f.waitErr != nil {
		errCh <- f.waitErr
	} else {
		waitCh <- container.WaitResponse{StatusCode: f.waitCode}
	}
	return waitCh, errCh
}

func (f *fakeDocker) ContainerRemove(_ context.Context, _ string, _ container.RemoveOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = true
	return nil
}

func (f *fakeDocker) wasRemoved() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.removed
}

func testRunner(f *fakeDocker) *DockerRunner {
	return &DockerRunner{api: f, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// collect runs the job and drains all updates until the channel closes.
func collect(t *testing.T, ctx context.Context, d *DockerRunner, spec *computev1.JobSpec) []Update {
	t.Helper()
	ch, err := d.Run(ctx, spec)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	var got []Update
	for u := range ch {
		got = append(got, u)
	}
	if len(got) == 0 {
		t.Fatal("no updates emitted")
	}
	return got
}

func mux(stdout, stderr string) []byte {
	var buf bytes.Buffer
	if stdout != "" {
		stdcopy.NewStdWriter(&buf, stdcopy.Stdout).Write([]byte(stdout))
	}
	if stderr != "" {
		stdcopy.NewStdWriter(&buf, stdcopy.Stderr).Write([]byte(stderr))
	}
	return buf.Bytes()
}

func spec(jobID string) *computev1.JobSpec {
	return &computev1.JobSpec{JobId: jobID, Image: "test/image:latest"}
}

func TestRunLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		fake       fakeDocker
		wantState  computev1.JobState
		wantExit   int32
		wantErrSub string // substring of the terminal Err; "" = must be empty
		wantRemove bool
	}{
		{
			name:       "success",
			fake:       fakeDocker{waitCode: 0},
			wantState:  computev1.JobState_JOB_STATE_SUCCEEDED,
			wantRemove: true,
		},
		{
			name:       "nonzero exit",
			fake:       fakeDocker{waitCode: 2},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantExit:   2,
			wantErrSub: "exited with code 2",
			wantRemove: true,
		},
		{
			name:       "pull error",
			fake:       fakeDocker{pullErr: errors.New("no such image")},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantErrSub: "pull",
		},
		{
			name:       "pull stream reports error",
			fake:       fakeDocker{pullStream: `{"status":"Pulling"}` + "\n" + `{"error":"manifest unknown"}` + "\n"},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantErrSub: "manifest unknown",
		},
		{
			name:       "create error",
			fake:       fakeDocker{createErr: errors.New("invalid config")},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantErrSub: "create container",
		},
		{
			name:       "start error still removes container",
			fake:       fakeDocker{startErr: errors.New("oom")},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantErrSub: "start container",
			wantRemove: true,
		},
		{
			name:       "wait error",
			fake:       fakeDocker{waitErr: errors.New("daemon went away")},
			wantState:  computev1.JobState_JOB_STATE_FAILED,
			wantErrSub: "daemon went away",
			wantRemove: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collect(t, context.Background(), testRunner(&tt.fake), spec("job-1"))

			if first := got[0].State; first != computev1.JobState_JOB_STATE_PENDING {
				t.Errorf("first state = %v, want PENDING", first)
			}
			last := got[len(got)-1]
			if last.State != tt.wantState {
				t.Errorf("terminal state = %v, want %v", last.State, tt.wantState)
			}
			if last.ExitCode != tt.wantExit {
				t.Errorf("exit code = %d, want %d", last.ExitCode, tt.wantExit)
			}
			if tt.wantErrSub == "" && last.Err != "" {
				t.Errorf("terminal Err = %q, want empty", last.Err)
			}
			if tt.wantErrSub != "" && !strings.Contains(last.Err, tt.wantErrSub) {
				t.Errorf("terminal Err = %q, want substring %q", last.Err, tt.wantErrSub)
			}
			for _, u := range got {
				if u.JobID != "job-1" {
					t.Errorf("update has JobID %q, want job-1", u.JobID)
				}
			}
			if tt.fake.wasRemoved() != tt.wantRemove {
				t.Errorf("container removed = %v, want %v", tt.fake.wasRemoved(), tt.wantRemove)
			}
		})
	}
}

func TestRunCancelled(t *testing.T) {
	fake := &fakeDocker{waitBlock: true}
	ctx, cancel := context.WithCancel(context.Background())

	ch, err := testRunner(fake).Run(ctx, spec("job-c"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	// Cancel once the job reports RUNNING.
	var got []Update
	for u := range ch {
		got = append(got, u)
		if u.State == computev1.JobState_JOB_STATE_RUNNING && u.LogChunk == "" {
			cancel()
		}
	}
	last := got[len(got)-1]
	if last.State != computev1.JobState_JOB_STATE_CANCELLED {
		t.Errorf("terminal state = %v, want CANCELLED", last.State)
	}
	if !fake.wasRemoved() {
		t.Error("container was not removed after cancel")
	}
}

func TestRunTimeout(t *testing.T) {
	fake := &fakeDocker{waitBlock: true}
	s := spec("job-t")
	s.TimeoutS = 1

	got := collect(t, context.Background(), testRunner(fake), s)
	last := got[len(got)-1]
	if last.State != computev1.JobState_JOB_STATE_FAILED {
		t.Errorf("terminal state = %v, want FAILED", last.State)
	}
	if !strings.Contains(last.Err, "timed out") {
		t.Errorf("terminal Err = %q, want timeout message", last.Err)
	}
	if !fake.wasRemoved() {
		t.Error("container was not removed after timeout")
	}
}

func TestRunContainerOptions(t *testing.T) {
	fake := &fakeDocker{waitCode: 0}
	s := &computev1.JobSpec{
		JobId:         "job-o",
		Image:         "nvidia/cuda:12.4.1-base-ubuntu22.04",
		Command:       []string{"nvidia-smi"},
		Env:           map[string]string{"B": "2", "A": "1"},
		Gpu:           true,
		MemoryLimitMb: 1024,
	}
	collect(t, context.Background(), testRunner(fake), s)

	if fake.createdName != "gridlink-job-job-o" {
		t.Errorf("container name = %q, want gridlink-job-job-o", fake.createdName)
	}
	cfg, host := fake.createdConfig, fake.createdHost
	if cfg.Image != s.Image {
		t.Errorf("image = %q, want %q", cfg.Image, s.Image)
	}
	if len(cfg.Cmd) != 1 || cfg.Cmd[0] != "nvidia-smi" {
		t.Errorf("cmd = %v, want [nvidia-smi]", cfg.Cmd)
	}
	if want := []string{"A=1", "B=2"}; len(cfg.Env) != 2 || cfg.Env[0] != want[0] || cfg.Env[1] != want[1] {
		t.Errorf("env = %v, want %v", cfg.Env, want)
	}
	if host.Resources.Memory != 1024*1024*1024 {
		t.Errorf("memory = %d, want %d", host.Resources.Memory, 1024*1024*1024)
	}
	if len(host.Resources.DeviceRequests) != 1 {
		t.Fatalf("device requests = %v, want one --gpus all equivalent", host.Resources.DeviceRequests)
	}
	dr := host.Resources.DeviceRequests[0]
	if dr.Count != -1 || len(dr.Capabilities) != 1 || dr.Capabilities[0][0] != "gpu" {
		t.Errorf("device request = %+v, want Count -1 with gpu capability", dr)
	}
	if host.Privileged || len(host.Mounts) != 0 || len(host.Binds) != 0 || host.NetworkMode.IsHost() {
		t.Errorf("host config grants extra access: %+v", host)
	}
}

func TestRunNoGPUNoDeviceRequests(t *testing.T) {
	fake := &fakeDocker{waitCode: 0}
	collect(t, context.Background(), testRunner(fake), spec("job-n"))
	if n := len(fake.createdHost.Resources.DeviceRequests); n != 0 {
		t.Errorf("device requests = %d, want 0 for non-GPU job", n)
	}
}

func TestRunStreamsLogs(t *testing.T) {
	fake := &fakeDocker{
		waitCode:  0,
		logStream: mux("hello stdout\n", "hello stderr\n"),
	}
	got := collect(t, context.Background(), testRunner(fake), spec("job-l"))

	var all strings.Builder
	for _, u := range got {
		if u.State == computev1.JobState_JOB_STATE_RUNNING {
			all.WriteString(u.LogChunk)
		}
	}
	for _, want := range []string{"hello stdout\n", "hello stderr\n"} {
		if !strings.Contains(all.String(), want) {
			t.Errorf("log chunks %q missing %q", all.String(), want)
		}
	}
	last := got[len(got)-1]
	if last.State != computev1.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("terminal state = %v, want SUCCEEDED", last.State)
	}
}

func TestRunPullProgressDeduplicated(t *testing.T) {
	fake := &fakeDocker{
		waitCode: 0,
		pullStream: `{"status":"Pulling from test/image"}` + "\n" +
			`{"status":"Downloading","progress":"1/10"}` + "\n" +
			`{"status":"Downloading","progress":"9/10"}` + "\n" +
			`{"status":"Pull complete"}` + "\n",
	}
	got := collect(t, context.Background(), testRunner(fake), spec("job-p"))

	var chunks []string
	for _, u := range got {
		if u.State == computev1.JobState_JOB_STATE_PENDING && u.LogChunk != "" {
			chunks = append(chunks, u.LogChunk)
		}
	}
	want := []string{"Pulling from test/image\n", "Downloading\n", "Pull complete\n"}
	if len(chunks) != len(want) {
		t.Fatalf("pull progress = %v, want %v", chunks, want)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("pull progress[%d] = %q, want %q", i, chunks[i], want[i])
		}
	}
}

func TestRunRejectsEmptyImage(t *testing.T) {
	if _, err := testRunner(&fakeDocker{}).Run(context.Background(), &computev1.JobSpec{JobId: "x"}); err == nil {
		t.Fatal("Run() with empty image succeeded, want error")
	}
}

// Guard against the coalescer flooding: a burst of writes inside one flush
// interval must arrive as a single chunk.
func TestLogCoalescerBatches(t *testing.T) {
	var mu sync.Mutex
	var chunks []string
	c := newLogCoalescer(time.Hour, func(s string) {
		mu.Lock()
		defer mu.Unlock()
		chunks = append(chunks, s)
	})
	for i := 0; i < 100; i++ {
		c.Write([]byte("x"))
	}
	c.stop()

	mu.Lock()
	defer mu.Unlock()
	if len(chunks) != 1 || chunks[0] != strings.Repeat("x", 100) {
		t.Errorf("chunks = %d (%q...), want one 100-byte chunk", len(chunks), chunks[0][:min(10, len(chunks[0]))])
	}
}
