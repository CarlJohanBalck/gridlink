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
			got, err := detect(context.Background(), tt.exec.run, env(tt.env))
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
			got, err := utilization(context.Background(), tt.exec.run, env(tt.env))
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
