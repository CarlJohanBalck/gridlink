package deploy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

const (
	// enginePortPrefix is printed by `agent engine` once it is listening. The
	// port is chosen by the kernel, so this is how the parent learns it.
	enginePortPrefix = "GRIDLINK_ENGINE_LISTENING "
	// readyTimeout bounds LOADING. Weight loading is seconds, not minutes,
	// once the file is local — a longer wait means something is wrong.
	readyTimeout = 5 * time.Minute
	// engineStartTimeout bounds how long we wait for the listening line.
	engineStartTimeout = 30 * time.Second
	readyPollInterval  = 2 * time.Second
	// stderrTailLines is how much engine output is kept to explain a failure.
	stderrTailLines = 20
)

// modelSpec is the subset of NativeEngine the downloader needs, so download
// logic does not depend on protobuf types.
type modelSpec struct {
	ModelRef  string
	ModelFile string
	Revision  string
	SHA256    string
}

// NativeManager serves models with the agent's own Metal engine, run as a
// sandboxed subprocess of the agent binary.
type NativeManager struct {
	// agentBin is the path re-executed as `agent engine`. Defaults to this
	// process's own executable.
	agentBin string
	// modelsDir caches downloaded weights (~/.gridlink/models).
	modelsDir string
	// sandbox wraps the engine command. Nil disables sandboxing (tests).
	sandbox func(cmd []string) []string
	log     *slog.Logger
}

var _ Manager = (*NativeManager)(nil)

func NewNativeManager(log *slog.Logger) (*NativeManager, error) {
	bin, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate agent binary: %w", err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("locate home dir: %w", err)
	}
	return &NativeManager{
		agentBin:  bin,
		modelsDir: filepath.Join(home, ".gridlink", "models"),
		sandbox:   sandboxCommand,
		log:       log,
	}, nil
}

func (m *NativeManager) Start(ctx context.Context, spec *computev1.DeploymentSpec) (<-chan Update, error) {
	native := spec.GetNative()
	if native == nil {
		return nil, fmt.Errorf("deployment %s has no native engine spec", spec.GetDeploymentId())
	}
	if native.GetModelFile() == "" {
		return nil, fmt.Errorf("deployment %s: model_file is required", spec.GetDeploymentId())
	}

	updates := make(chan Update, 8)
	go m.run(ctx, spec, native, updates)
	return updates, nil
}

func (m *NativeManager) run(ctx context.Context, spec *computev1.DeploymentSpec, native *computev1.NativeEngine, updates chan<- Update) {
	defer close(updates)
	id := spec.GetDeploymentId()
	log := m.log.With("deployment_id", id, "model", spec.GetServedModelName())

	send := func(u Update) {
		u.DeploymentID = id
		select {
		case updates <- u:
		case <-time.After(time.Second):
			// The consumer is the client's forwarding goroutine; if it is that
			// far behind, dropping a progress tick is better than blocking the
			// engine's lifecycle.
		}
	}
	fail := func(err error) {
		log.Error("deployment failed", "err", err)
		send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_FAILED, Err: err.Error()})
	}

	// ---- PULLING ----
	send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_PULLING})
	modelPath, err := downloadModel(ctx, m.modelsDir, modelSpec{
		ModelRef:  native.GetModelRef(),
		ModelFile: native.GetModelFile(),
		Revision:  native.GetRevision(),
		SHA256:    native.GetSha256(),
	}, func(pct uint32) {
		send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_PULLING, Progress: pct})
	})
	if err != nil {
		if ctx.Err() != nil {
			send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED})
			return
		}
		fail(fmt.Errorf("fetch weights: %w", err))
		return
	}

	// ---- LOADING ----
	send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_LOADING})

	args := []string{m.agentBin, "engine",
		"-model", modelPath,
		"-served-model-name", spec.GetServedModelName(),
		"-addr", "127.0.0.1:0",
	}
	if n := native.GetContextLength(); n > 0 {
		args = append(args, "-context-length", fmt.Sprint(n))
	}
	if m.sandbox != nil {
		args = m.sandbox(args)
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fail(fmt.Errorf("engine stdout: %w", err))
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		fail(fmt.Errorf("engine stderr: %w", err))
		return
	}
	if err := cmd.Start(); err != nil {
		fail(fmt.Errorf("start engine: %w", err))
		return
	}

	tail := newTailBuffer(stderrTailLines)
	go tail.consume(stderr)

	addr, err := readEngineAddr(ctx, stdout)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		fail(fmt.Errorf("engine never reported a port: %w (engine said: %s)", err, tail.String()))
		return
	}
	// Keep draining stdout so a chatty engine cannot block on a full pipe.
	go io.Copy(io.Discard, stdout)

	log.Info("engine started", "addr", addr, "pid", cmd.Process.Pid)

	// ---- READY ----
	if err := waitReady(ctx, addr, spec.GetServedModelName()); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if ctx.Err() != nil {
			send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED})
			return
		}
		fail(fmt.Errorf("engine never became ready: %w (engine said: %s)", err, tail.String()))
		return
	}

	port := portOf(addr)
	log.Info("deployment ready", "port", port)
	send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_READY, HostPort: port})

	// ---- serving ----
	// READY is not terminal: sit here until the engine dies or we are stopped.
	waitErr := cmd.Wait()
	if ctx.Err() != nil {
		log.Info("deployment stopped")
		send(Update{State: computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED})
		return
	}
	// The engine exited on its own, which is always a failure: the coordinator
	// owns restarts and re-placement, not the agent.
	fail(fmt.Errorf("engine exited unexpectedly: %v (engine said: %s)", waitErr, tail.String()))
}

// readEngineAddr waits for the engine's listening line on stdout.
func readEngineAddr(ctx context.Context, stdout io.Reader) (string, error) {
	type result struct {
		addr string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, enginePortPrefix) {
				ch <- result{addr: strings.TrimSpace(strings.TrimPrefix(line, enginePortPrefix))}
				return
			}
		}
		if err := sc.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{err: fmt.Errorf("engine stdout closed before reporting a port")}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			return "", r.err
		}
		if r.addr == "" {
			return "", fmt.Errorf("engine reported an empty address")
		}
		return r.addr, nil
	case <-time.After(engineStartTimeout):
		return "", fmt.Errorf("timed out after %s", engineStartTimeout)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// waitReady polls /v1/models until the engine serves the expected model.
func waitReady(ctx context.Context, addr, modelName string) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()

	client := &http.Client{Timeout: 5 * time.Second}
	url := "http://" + addr + "/v1/models"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			// Checking for the model name, not just a 200, so a half-loaded
			// engine cannot be mistaken for a ready one.
			if resp.StatusCode == http.StatusOK && strings.Contains(string(body), modelName) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readyPollInterval):
		}
	}
}

func portOf(addr string) uint32 {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return 0
	}
	var p uint32
	if _, err := fmt.Sscanf(addr[i+1:], "%d", &p); err != nil {
		return 0
	}
	return p
}

// tailBuffer keeps the last n lines of engine stderr so a failure can be
// explained without shipping megabytes of llama.cpp logging to the coordinator.
type tailBuffer struct {
	mu    sync.Mutex
	lines []string
	n     int
}

func newTailBuffer(n int) *tailBuffer { return &tailBuffer{n: n} }

func (t *tailBuffer) consume(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		t.mu.Lock()
		t.lines = append(t.lines, sc.Text())
		if len(t.lines) > t.n {
			t.lines = t.lines[len(t.lines)-t.n:]
		}
		t.mu.Unlock()
	}
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, " | ")
}
