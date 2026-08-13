package gpu

import (
	"context"
	"errors"
	"os/exec"
	"testing"

	computev1 "gridlink/contracts/gen/compute/v1"

	"google.golang.org/protobuf/proto"
)

// fakeExec fakes nvidia-smi: query calls (with args) return queryOut/queryErr,
// the bare banner call returns bannerOut/bannerErr.
type fakeExec struct {
	queryOut  string
	queryErr  error
	bannerOut string
	bannerErr error
	calls     int
}

func (f *fakeExec) run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls++
	if name != "nvidia-smi" {
		return nil, errors.New("unexpected command: " + name)
	}
	if len(args) == 0 {
		return []byte(f.bannerOut), f.bannerErr
	}
	return []byte(f.queryOut), f.queryErr
}

func env(vals map[string]string) func(string) string {
	return func(k string) string { return vals[k] }
}

const banner = `
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 550.54.14              Driver Version: 550.54.14      CUDA Version: 12.4     |
|-----------------------------------------+------------------------+----------------------+
`

func TestDetect(t *testing.T) {
	tests := []struct {
		name     string
		exec     fakeExec
		env      map[string]string
		want     *computev1.GpuInfo
		wantErr  error // matched with errors.Is when set
		anyErr   bool  // any non-nil error is acceptable
		maxCalls int   // 0 = unchecked
	}{
		{
			name: "single gpu",
			exec: fakeExec{
				queryOut:  "NVIDIA GeForce RTX 4090, 24564, 550.54.14\n",
				bannerOut: banner,
			},
			want: &computev1.GpuInfo{
				Vendor:        "nvidia",
				Model:         "NVIDIA GeForce RTX 4090",
				VramTotalMb:   24564,
				DriverVersion: "550.54.14",
				CudaVersion:   "12.4",
				GpuCount:      1,
			},
		},
		{
			name: "multi gpu counts all, reports first",
			exec: fakeExec{
				queryOut:  "NVIDIA RTX A6000, 49140, 535.129.03\nNVIDIA RTX A6000, 49140, 535.129.03\n",
				bannerOut: banner,
			},
			want: &computev1.GpuInfo{
				Vendor:        "nvidia",
				Model:         "NVIDIA RTX A6000",
				VramTotalMb:   49140,
				DriverVersion: "535.129.03",
				CudaVersion:   "12.4",
				GpuCount:      2,
			},
		},
		{
			name: "comma in gpu name",
			exec: fakeExec{
				queryOut:  "NVIDIA H100, PCIe, 81559, 535.129.03\n",
				bannerOut: banner,
			},
			want: &computev1.GpuInfo{
				Vendor:        "nvidia",
				Model:         "NVIDIA H100, PCIe",
				VramTotalMb:   81559,
				DriverVersion: "535.129.03",
				CudaVersion:   "12.4",
				GpuCount:      1,
			},
		},
		{
			name: "n/a memory becomes zero, missing banner leaves cuda empty",
			exec: fakeExec{
				queryOut:  "Some GPU, [N/A], 550.54.14\n",
				bannerErr: errors.New("boom"),
			},
			want: &computev1.GpuInfo{
				Vendor:        "nvidia",
				Model:         "Some GPU",
				VramTotalMb:   0,
				DriverVersion: "550.54.14",
				CudaVersion:   "",
				GpuCount:      1,
			},
		},
		{
			name:    "nvidia-smi missing",
			exec:    fakeExec{queryErr: exec.ErrNotFound},
			wantErr: ErrNoGPU,
		},
		{
			name:    "no devices",
			exec:    fakeExec{queryOut: "\n"},
			wantErr: ErrNoGPU,
		},
		{
			name:   "malformed output",
			exec:   fakeExec{queryOut: "garbage\n"},
			anyErr: true,
		},
		{
			name:     "fake gpu env skips nvidia-smi",
			exec:     fakeExec{queryErr: exec.ErrNotFound},
			env:      map[string]string{"GRIDLINK_FAKE_GPU": "1"},
			maxCalls: 0,
			want: &computev1.GpuInfo{
				Vendor:        "nvidia",
				Model:         "FAKE GPU (GRIDLINK_FAKE_GPU=1)",
				VramTotalMb:   24576,
				DriverVersion: "0.0-fake",
				CudaVersion:   "12.4",
				GpuCount:      1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := detect(context.Background(), tt.exec.run, env(tt.env), "linux")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("detect() error = %v, want errors.Is %v", err, tt.wantErr)
				}
				return
			}
			if tt.anyErr {
				if err == nil {
					t.Fatalf("detect() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("detect() error = %v", err)
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("detect() = %v, want %v", got, tt.want)
			}
			if tt.env["GRIDLINK_FAKE_GPU"] == "1" && tt.exec.calls != 0 {
				t.Errorf("fake gpu mode ran nvidia-smi %d times, want 0", tt.exec.calls)
			}
		})
	}
}

func TestUtilization(t *testing.T) {
	tests := []struct {
		name    string
		exec    fakeExec
		env     map[string]string
		want    *computev1.GpuUtilization
		wantErr error
	}{
		{
			name: "normal sample",
			exec: fakeExec{queryOut: "45, 10240, 67\n"},
			want: &computev1.GpuUtilization{GpuPercent: 45, VramUsedMb: 10240, TemperatureC: 67},
		},
		{
			name: "n/a fields become zero",
			exec: fakeExec{queryOut: "[N/A], 512, [N/A]\n"},
			want: &computev1.GpuUtilization{GpuPercent: 0, VramUsedMb: 512, TemperatureC: 0},
		},
		{
			name:    "nvidia-smi missing",
			exec:    fakeExec{queryErr: exec.ErrNotFound},
			wantErr: ErrNoGPU,
		},
		{
			name: "fake gpu env returns idle sample",
			exec: fakeExec{queryErr: exec.ErrNotFound},
			env:  map[string]string{"GRIDLINK_FAKE_GPU": "1"},
			want: &computev1.GpuUtilization{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := utilization(context.Background(), tt.exec.run, env(tt.env), "linux")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("utilization() error = %v, want errors.Is %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("utilization() error = %v", err)
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("utilization() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---- Apple Silicon ----

// darwinExec fakes the three commands the Apple path shells out to. A command
// with no entry in outs returns errNotFaked, so a test that forgets one fails
// loudly instead of silently reporting a zero field.
type darwinExec struct {
	outs map[string]string
	errs map[string]error
	seen []string
}

var errNotFaked = errors.New("command not faked")

func (f *darwinExec) run(_ context.Context, name string, _ ...string) ([]byte, error) {
	f.seen = append(f.seen, name)
	if err, ok := f.errs[name]; ok {
		return nil, err
	}
	out, ok := f.outs[name]
	if !ok {
		return nil, errNotFaked
	}
	return []byte(out), nil
}

// spJSON is trimmed from real `system_profiler SPDisplaysDataType -json`
// output on a Mac mini M4.
const spJSON = `{
  "SPDisplaysDataType" : [
    {
      "_name" : "Apple M4",
      "spdisplays_mtlgpufamilysupport" : "spdisplays_metal4",
      "spdisplays_vendor" : "sppci_vendor_Apple",
      "sppci_bus" : "spdisplays_builtin",
      "sppci_cores" : "10",
      "sppci_device_type" : "spdisplays_gpu",
      "sppci_model" : "Apple M4"
    }
  ]
}`

func darwinOK() *darwinExec {
	return &darwinExec{outs: map[string]string{
		"system_profiler": spJSON,
		"sysctl":          "17179869184\n",
		"sw_vers":         "15.3.1\n",
	}}
}

func TestDetectAppleSilicon(t *testing.T) {
	tests := []struct {
		name    string
		exec    *darwinExec
		want    *computev1.GpuInfo
		wantErr error
		anyErr  bool
	}{
		{
			name: "m4 mac mini",
			exec: darwinOK(),
			want: &computev1.GpuInfo{
				Vendor:        "apple",
				Model:         "Apple M4 (10-core GPU)",
				VramTotalMb:   16384,
				DriverVersion: "macOS 15.3.1",
				GpuCount:      1,
			},
		},
		{
			name: "sysctl and sw_vers are best-effort",
			exec: &darwinExec{
				outs: map[string]string{"system_profiler": spJSON},
				errs: map[string]error{
					"sysctl":  exec.ErrNotFound,
					"sw_vers": exec.ErrNotFound,
				},
			},
			want: &computev1.GpuInfo{
				Vendor:      "apple",
				Model:       "Apple M4 (10-core GPU)",
				VramTotalMb: 0,
				GpuCount:    1,
			},
		},
		{
			name: "non-gpu display entries are skipped",
			exec: &darwinExec{outs: map[string]string{
				"system_profiler": `{"SPDisplaysDataType":[
					{"_name":"Some Display","sppci_device_type":"spdisplays_display"},
					{"sppci_model":"Apple M4","sppci_cores":"10","sppci_device_type":"spdisplays_gpu"}]}`,
				"sysctl":  "17179869184\n",
				"sw_vers": "15.3.1\n",
			}},
			want: &computev1.GpuInfo{
				Vendor:        "apple",
				Model:         "Apple M4 (10-core GPU)",
				VramTotalMb:   16384,
				DriverVersion: "macOS 15.3.1",
				GpuCount:      1,
			},
		},
		{
			name: "entry without device type is still accepted",
			exec: &darwinExec{outs: map[string]string{
				"system_profiler": `{"SPDisplaysDataType":[{"_name":"Apple M1"}]}`,
				"sysctl":          "8589934592\n",
				"sw_vers":         "14.0\n",
			}},
			want: &computev1.GpuInfo{
				Vendor:        "apple",
				Model:         "Apple M1",
				VramTotalMb:   8192,
				DriverVersion: "macOS 14.0",
				GpuCount:      1,
			},
		},
		{
			name:    "system_profiler missing",
			exec:    &darwinExec{errs: map[string]error{"system_profiler": exec.ErrNotFound}},
			wantErr: ErrNoGPU,
		},
		{
			name:    "report lists no gpu",
			exec:    &darwinExec{outs: map[string]string{"system_profiler": `{"SPDisplaysDataType":[]}`}},
			wantErr: ErrNoGPU,
		},
		{
			name:   "malformed json",
			exec:   &darwinExec{outs: map[string]string{"system_profiler": "not json"}},
			anyErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := detect(context.Background(), tt.exec.run, env(nil), "darwin")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("detect() error = %v, want errors.Is %v", err, tt.wantErr)
				}
				return
			}
			if tt.anyErr {
				if err == nil {
					t.Fatalf("detect() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("detect() error = %v", err)
			}
			if !proto.Equal(got, tt.want) {
				t.Errorf("detect() = %v, want %v", got, tt.want)
			}
			if got.GetCudaVersion() != "" {
				t.Errorf("cuda_version = %q, want empty on Apple Silicon", got.GetCudaVersion())
			}
		})
	}
}

// The Apple path must never shell out to nvidia-smi, and the nvidia path must
// never shell out to system_profiler.
func TestDetectDoesNotCrossPlatforms(t *testing.T) {
	d := darwinOK()
	if _, err := detect(context.Background(), d.run, env(nil), "darwin"); err != nil {
		t.Fatalf("detect() error = %v", err)
	}
	for _, name := range d.seen {
		if name == "nvidia-smi" {
			t.Errorf("darwin detection called nvidia-smi")
		}
	}

	n := &fakeExec{queryOut: "NVIDIA GeForce RTX 4090, 24564, 550.54.14\n", bannerOut: banner}
	if _, err := detect(context.Background(), n.run, env(nil), "linux"); err != nil {
		t.Fatalf("detect() error = %v", err)
	}
	// fakeExec rejects any command other than nvidia-smi, so a successful
	// linux detect already proves system_profiler was never invoked.
}

func TestUtilizationUnsupportedOnDarwin(t *testing.T) {
	d := darwinOK()
	got, err := utilization(context.Background(), d.run, env(nil), "darwin")
	if !errors.Is(err, ErrUtilizationUnsupported) {
		t.Fatalf("utilization() error = %v, want errors.Is %v", err, ErrUtilizationUnsupported)
	}
	if got != nil {
		t.Errorf("utilization() = %v, want nil", got)
	}
	if len(d.seen) != 0 {
		t.Errorf("utilization shelled out to %v, want no commands", d.seen)
	}
}
