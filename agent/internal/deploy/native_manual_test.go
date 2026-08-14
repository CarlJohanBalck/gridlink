//go:build darwin

package deploy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

// TestManualNativeDeployment drives the whole native path against real
// hardware: cache hit -> sandboxed engine subprocess -> READY -> serving ->
// clean stop. Skipped unless both env vars are set, so `make test` stays
// GPU-free per CLAUDE.md.
//
//	GRIDLINK_TEST_MODEL=/path/to.gguf GRIDLINK_TEST_AGENT_BIN=./bin/agent \
//	  go test ./internal/deploy/ -run TestManualNativeDeployment -v
func TestManualNativeDeployment(t *testing.T) {
	modelPath := os.Getenv("GRIDLINK_TEST_MODEL")
	agentBin := os.Getenv("GRIDLINK_TEST_AGENT_BIN")
	if modelPath == "" || agentBin == "" {
		t.Skip("set GRIDLINK_TEST_MODEL and GRIDLINK_TEST_AGENT_BIN to run")
	}
	absBin, err := filepath.Abs(agentBin)
	if err != nil {
		t.Fatalf("resolve agent bin: %v", err)
	}

	sum, err := fileSHA256(modelPath)
	if err != nil {
		t.Fatalf("hash model: %v", err)
	}

	spec := &computev1.DeploymentSpec{
		DeploymentId:    "dep-manual-1",
		ServedModelName: "qwen-0.5b",
		Engine: &computev1.DeploymentSpec_Native{
			Native: &computev1.NativeEngine{
				ModelRef:      "test/local",
				ModelFile:     "qwen0.5b.gguf",
				Sha256:        sum,
				ContextLength: 2048,
			},
		},
	}

	// Pre-seed the cache so this test exercises spawn/ready rather than the
	// network. The hash still has to match, so the cache-verification path is
	// covered too.
	dir := t.TempDir()
	ms := modelSpec{ModelRef: "test/local", ModelFile: "qwen0.5b.gguf", SHA256: sum}
	if err := os.Link(modelPath, filepath.Join(dir, cacheName(ms))); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	m := &NativeManager{
		agentBin:  absBin,
		modelsDir: dir,
		sandbox:   sandboxCommand, // the real sandbox-exec profile
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	updates, err := m.Start(ctx, spec)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	var states []computev1.DeploymentState
	var port uint32
	deadline := time.After(2 * time.Minute)
ready:
	for {
		select {
		case u, ok := <-updates:
			if !ok {
				t.Fatalf("updates closed before READY; states = %v", states)
			}
			states = append(states, u.State)
			if u.Err != "" {
				t.Fatalf("deployment failed: %s", u.Err)
			}
			if u.State == computev1.DeploymentState_DEPLOYMENT_STATE_READY {
				port = u.HostPort
				break ready
			}
		case <-deadline:
			t.Fatalf("never reached READY; states = %v", states)
		}
	}

	if port == 0 {
		t.Fatal("READY reported no port")
	}
	t.Logf("states=%v port=%d", states, port)

	// It must actually serve, not merely report READY.
	resp, err := http.Get("http://127.0.0.1:" + itoa(port) + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode, body)
	}
	t.Logf("models: %s", body)

	// Stopping is by context cancellation; STOPPED must follow and the channel
	// must close, or the agent would leak the deployment forever.
	cancel()
	sawStopped := false
	for u := range updates {
		if u.State == computev1.DeploymentState_DEPLOYMENT_STATE_STOPPED {
			sawStopped = true
		}
	}
	if !sawStopped {
		t.Error("no STOPPED update after cancellation")
	}
}

func itoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [10]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
