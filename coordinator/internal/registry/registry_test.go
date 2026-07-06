package registry

import (
	"io"
	"log/slog"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newFixed builds a registry with a controllable clock and deterministic IDs.
func newFixed(t *testing.T, base time.Time) (*Registry, *time.Time) {
	t.Helper()
	r := New(testLogger())
	now := base
	r.now = func() time.Time { return now }
	r.newID = func() string { return "assigned-id" }
	return r, &now
}

func TestUpsertAssignsID(t *testing.T) {
	cases := []struct {
		name     string
		inID     string
		wantID   string
		wantHost string
	}{
		{"empty id gets assigned", "", "assigned-id", "host-a"},
		{"existing id preserved", "node-42", "node-42", "host-b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := newFixed(t, time.Unix(1000, 0))
			gotID := r.Upsert(&computev1.Register{NodeId: tc.inID, Hostname: tc.wantHost}, nil)
			if gotID != tc.wantID {
				t.Fatalf("node id = %q, want %q", gotID, tc.wantID)
			}
			n, ok := r.Get(gotID)
			if !ok {
				t.Fatalf("node %q not found after Upsert", gotID)
			}
			if n.Hostname != tc.wantHost {
				t.Errorf("hostname = %q, want %q", n.Hostname, tc.wantHost)
			}
			if n.Status != computev1.NodeStatus_NODE_STATUS_ONLINE {
				t.Errorf("status = %v, want ONLINE", n.Status)
			}
		})
	}
}

func TestUpsertReconnectReusesEntry(t *testing.T) {
	r, clk := newFixed(t, time.Unix(1000, 0))
	id := r.Upsert(&computev1.Register{Hostname: "host-a"}, nil)

	// Reconnect with the assigned id and a new hostname/GPU; same entry updated.
	*clk = clk.Add(5 * time.Second)
	id2 := r.Upsert(&computev1.Register{
		NodeId:   id,
		Hostname: "host-a-renamed",
		Gpu:      &computev1.GpuInfo{Model: "RTX 4090"},
	}, nil)
	if id2 != id {
		t.Fatalf("reconnect id = %q, want %q", id2, id)
	}
	if got := len(r.List()); got != 1 {
		t.Fatalf("node count = %d, want 1", got)
	}
	n, _ := r.Get(id)
	if n.Hostname != "host-a-renamed" || n.GPU.GetModel() != "RTX 4090" {
		t.Errorf("reconnect did not refresh fields: %+v", n)
	}
	if !n.LastSeen.Equal(time.Unix(1005, 0)) {
		t.Errorf("LastSeen = %v, want %v", n.LastSeen, time.Unix(1005, 0))
	}
}

func TestTouchUpdatesLastSeen(t *testing.T) {
	r, clk := newFixed(t, time.Unix(1000, 0))
	id := r.Upsert(&computev1.Register{Hostname: "host-a"}, nil)

	*clk = clk.Add(12 * time.Second)
	r.Touch(id, &computev1.Heartbeat{})

	n, _ := r.Get(id)
	if !n.LastSeen.Equal(time.Unix(1012, 0)) {
		t.Errorf("LastSeen = %v, want %v", n.LastSeen, time.Unix(1012, 0))
	}
	if n.Status != computev1.NodeStatus_NODE_STATUS_ONLINE {
		t.Errorf("status = %v, want ONLINE", n.Status)
	}
}

func TestTouchUnknownNodeIsNoop(t *testing.T) {
	r, _ := newFixed(t, time.Unix(1000, 0))
	r.Touch("nope", &computev1.Heartbeat{}) // must not panic
	if got := len(r.List()); got != 0 {
		t.Fatalf("node count = %d, want 0", got)
	}
}

func TestMarkDisconnected(t *testing.T) {
	r, _ := newFixed(t, time.Unix(1000, 0))
	sent := false
	id := r.Upsert(&computev1.Register{Hostname: "host-a"}, func(*computev1.CoordinatorMessage) error {
		sent = true
		return nil
	})
	r.MarkDisconnected(id)

	n, _ := r.Get(id)
	if n.Status != computev1.NodeStatus_NODE_STATUS_OFFLINE {
		t.Errorf("status = %v, want OFFLINE", n.Status)
	}
	if n.Send != nil {
		t.Errorf("Send should be nil after disconnect")
	}
	_ = sent
}

func TestReaperMarksOffline(t *testing.T) {
	const interval = 10 * time.Second // OFFLINE after 3 missed = >30s silence
	cases := []struct {
		name    string
		silence time.Duration
		want    computev1.NodeStatus
	}{
		{"fresh", 5 * time.Second, computev1.NodeStatus_NODE_STATUS_ONLINE},
		{"one missed", 15 * time.Second, computev1.NodeStatus_NODE_STATUS_ONLINE},
		{"at threshold", 30 * time.Second, computev1.NodeStatus_NODE_STATUS_ONLINE},
		{"just past threshold", 31 * time.Second, computev1.NodeStatus_NODE_STATUS_OFFLINE},
		{"long gone", 5 * time.Minute, computev1.NodeStatus_NODE_STATUS_OFFLINE},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, clk := newFixed(t, time.Unix(1000, 0))
			id := r.Upsert(&computev1.Register{Hostname: "host-a"}, nil)

			*clk = clk.Add(tc.silence)
			r.reap(interval)

			n, _ := r.Get(id)
			if n.Status != tc.want {
				t.Errorf("after %s silence: status = %v, want %v", tc.silence, n.Status, tc.want)
			}
		})
	}
}

func TestReaperLeavesOfflineNodesAlone(t *testing.T) {
	const interval = 10 * time.Second
	r, clk := newFixed(t, time.Unix(1000, 0))
	id := r.Upsert(&computev1.Register{Hostname: "host-a"}, nil)
	r.MarkDisconnected(id)

	*clk = clk.Add(time.Hour)
	r.reap(interval) // should be a no-op on an already-OFFLINE node

	n, _ := r.Get(id)
	if n.Status != computev1.NodeStatus_NODE_STATUS_OFFLINE {
		t.Errorf("status = %v, want OFFLINE", n.Status)
	}
}

func TestListReportsSummary(t *testing.T) {
	r, _ := newFixed(t, time.Unix(1000, 0))
	r.Upsert(&computev1.Register{
		NodeId:   "node-1",
		Hostname: "host-a",
		Gpu:      &computev1.GpuInfo{Model: "RTX 4090"},
	}, nil)

	list := r.List()
	if len(list) != 1 {
		t.Fatalf("len(list) = %d, want 1", len(list))
	}
	got := list[0]
	if got.GetNodeId() != "node-1" || got.GetHostname() != "host-a" {
		t.Errorf("summary = %+v, want node-1/host-a", got)
	}
	if got.GetGpu().GetModel() != "RTX 4090" {
		t.Errorf("gpu model = %q, want RTX 4090", got.GetGpu().GetModel())
	}
	if got.GetLastSeenUnixMs() != time.Unix(1000, 0).UnixMilli() {
		t.Errorf("last seen = %d, want %d", got.GetLastSeenUnixMs(), time.Unix(1000, 0).UnixMilli())
	}
	if got.GetStatus() != computev1.NodeStatus_NODE_STATUS_ONLINE {
		t.Errorf("status = %v, want ONLINE", got.GetStatus())
	}
}
