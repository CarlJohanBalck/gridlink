package sysinfo

import (
	"context"
	"errors"
	"os/exec"
	"testing"
)

// fakeExec returns canned stdout per command name, or an error when listed.
type fakeExec struct {
	outs map[string]string
	errs map[string]error
	seen []string
}

func (f *fakeExec) run(_ context.Context, name string, _ ...string) ([]byte, error) {
	f.seen = append(f.seen, name)
	if err, ok := f.errs[name]; ok {
		return nil, err
	}
	return []byte(f.outs[name]), nil
}

const procMeminfo = `MemTotal:        8318216 kB
MemFree:          194212 kB
MemAvailable:    6002204 kB
`

func TestDetect(t *testing.T) {
	tests := []struct {
		name       string
		goos       string
		goarch     string
		numCPU     int
		exec       *fakeExec
		wantRAMMb  uint64
		wantNoExec bool
	}{
		{
			name:   "darwin reads hw.memsize",
			goos:   "darwin",
			goarch: "arm64",
			numCPU: 10,
			// 17179869184 bytes = 16 GiB, the Mac mini M4 under test.
			exec:      &fakeExec{outs: map[string]string{"sysctl": "17179869184\n"}},
			wantRAMMb: 16384,
		},
		{
			name:      "linux parses /proc/meminfo kB",
			goos:      "linux",
			goarch:    "arm64",
			numCPU:    4,
			exec:      &fakeExec{outs: map[string]string{"cat": procMeminfo}},
			wantRAMMb: 8123, // 8318216 kB / 1024
		},
		{
			name:      "darwin sysctl failure leaves ram zero",
			goos:      "darwin",
			goarch:    "arm64",
			numCPU:    10,
			exec:      &fakeExec{errs: map[string]error{"sysctl": exec.ErrNotFound}},
			wantRAMMb: 0,
		},
		{
			name:      "linux meminfo without MemTotal leaves ram zero",
			goos:      "linux",
			goarch:    "amd64",
			numCPU:    8,
			exec:      &fakeExec{outs: map[string]string{"cat": "Slab: 1234 kB\n"}},
			wantRAMMb: 0,
		},
		{
			name:      "linux garbage MemTotal leaves ram zero",
			goos:      "linux",
			goarch:    "amd64",
			numCPU:    8,
			exec:      &fakeExec{outs: map[string]string{"cat": "MemTotal:  notanumber kB\n"}},
			wantRAMMb: 0,
		},
		{
			// An unsupported platform must not shell out to a probe that cannot
			// exist there.
			name:       "unknown os probes nothing",
			goos:       "plan9",
			goarch:     "arm64",
			numCPU:     2,
			exec:       &fakeExec{},
			wantRAMMb:  0,
			wantNoExec: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detect(context.Background(), tt.exec.run, tt.goos, tt.goarch, tt.numCPU)

			if got.GetOs() != tt.goos {
				t.Errorf("os = %q, want %q", got.GetOs(), tt.goos)
			}
			if got.GetArch() != tt.goarch {
				t.Errorf("arch = %q, want %q", got.GetArch(), tt.goarch)
			}
			if got.GetCpuCores() != uint32(tt.numCPU) {
				t.Errorf("cpu_cores = %d, want %d", got.GetCpuCores(), tt.numCPU)
			}
			if got.GetRamTotalMb() != tt.wantRAMMb {
				t.Errorf("ram_total_mb = %d, want %d", got.GetRamTotalMb(), tt.wantRAMMb)
			}
			if tt.wantNoExec && len(tt.exec.seen) != 0 {
				t.Errorf("ran %v, want no probes", tt.exec.seen)
			}
			// Runners is composed by main from what actually initialised, so
			// detection must never populate it.
			if r := got.GetRunners(); len(r) != 0 {
				t.Errorf("runners = %v, want empty from Detect", r)
			}
			if got.GetDockerVersion() != "" {
				t.Errorf("docker_version = %q, want empty from Detect", got.GetDockerVersion())
			}
		})
	}
}

// Detection is best-effort: it must return usable info rather than fail, since
// a node that cannot report its RAM can still take work.
func TestDetectNeverReturnsNil(t *testing.T) {
	e := &fakeExec{errs: map[string]error{
		"sysctl": errors.New("boom"),
		"cat":    errors.New("boom"),
	}}
	for _, goos := range []string{"darwin", "linux", "windows"} {
		if got := detect(context.Background(), e.run, goos, "arm64", 1); got == nil {
			t.Fatalf("detect(%s) = nil", goos)
		}
	}
}
