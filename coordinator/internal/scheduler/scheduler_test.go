package scheduler

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// harness wires a scheduler to a registry with one connected node whose
// outbound messages are captured, and a controllable clock.
type harness struct {
	sched  *Scheduler
	reg    *registry.Registry
	nodeID string
	sent   []*computev1.CoordinatorMessage
	now    time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{now: time.Unix(1000, 0)}
	h.reg = registry.New(testLogger())
	h.sched = New(h.reg, testLogger())
	h.sched.now = func() time.Time { return h.now }
	h.sched.newID = func() string { return "job-fixed" }
	h.nodeID = h.reg.Upsert(&computev1.Register{Hostname: "host-a"}, func(m *computev1.CoordinatorMessage) error {
		h.sent = append(h.sent, m)
		return nil
	})
	return h
}

func spec() *computev1.JobSpec {
	return &computev1.JobSpec{Image: "test/image:latest"}
}

func TestRunJobDispatches(t *testing.T) {
	h := newHarness(t)

	jobID, err := h.sched.RunJob(h.nodeID, spec())
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if jobID != "job-fixed" {
		t.Errorf("jobID = %q, want job-fixed", jobID)
	}

	if len(h.sent) != 1 {
		t.Fatalf("node received %d messages, want 1", len(h.sent))
	}
	rj := h.sent[0].GetRunJob()
	if rj == nil {
		t.Fatalf("node received %T, want RunJob", h.sent[0].Payload)
	}
	if rj.GetSpec().GetJobId() != jobID {
		t.Errorf("dispatched spec.job_id = %q, want %q", rj.GetSpec().GetJobId(), jobID)
	}

	j, ok := h.sched.Get(jobID)
	if !ok {
		t.Fatal("job missing from table")
	}
	if j.State != computev1.JobState_JOB_STATE_PENDING || j.NodeID != h.nodeID {
		t.Errorf("job = %+v, want PENDING on %s", j, h.nodeID)
	}
}

func TestRunJobErrors(t *testing.T) {
	tests := []struct {
		name    string
		prep    func(h *harness)
		nodeID  func(h *harness) string
		wantErr error
	}{
		{
			name:    "unknown node",
			prep:    func(*harness) {},
			nodeID:  func(*harness) string { return "nope" },
			wantErr: ErrUnknownNode,
		},
		{
			name:    "offline node",
			prep:    func(h *harness) { h.reg.MarkDisconnected(h.nodeID) },
			nodeID:  func(h *harness) string { return h.nodeID },
			wantErr: ErrNodeOffline,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			tt.prep(h)
			_, err := h.sched.RunJob(tt.nodeID(h), spec())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("RunJob error = %v, want %v", err, tt.wantErr)
			}
			if len(h.sent) != 0 {
				t.Errorf("node received %d messages, want 0", len(h.sent))
			}
		})
	}
}

func TestRunJobSendFailureRemovesJob(t *testing.T) {
	h := newHarness(t)
	// Rebind the node's stream to one that always fails.
	h.reg.Upsert(&computev1.Register{NodeId: h.nodeID, Hostname: "host-a"},
		func(*computev1.CoordinatorMessage) error { return errors.New("stream gone") })

	jobID, err := h.sched.RunJob(h.nodeID, spec())
	if err == nil {
		t.Fatalf("RunJob = %q, want error", jobID)
	}
	if _, ok := h.sched.Get("job-fixed"); ok {
		t.Error("failed dispatch left job in table")
	}
}

func TestOnJobUpdateTracksStateAndTerminalFields(t *testing.T) {
	h := newHarness(t)
	jobID, _ := h.sched.RunJob(h.nodeID, spec())

	h.sched.OnJobUpdate(h.nodeID, &computev1.JobUpdate{
		JobId: jobID, State: computev1.JobState_JOB_STATE_RUNNING,
	})
	if j, _ := h.sched.Get(jobID); j.State != computev1.JobState_JOB_STATE_RUNNING {
		t.Errorf("state = %v, want RUNNING", j.State)
	}

	// A log-only update (state UNSPECIFIED) must not regress the state.
	h.sched.OnJobUpdate(h.nodeID, &computev1.JobUpdate{JobId: jobID, LogChunk: "hi\n"})
	if j, _ := h.sched.Get(jobID); j.State != computev1.JobState_JOB_STATE_RUNNING {
		t.Errorf("state after log-only update = %v, want RUNNING", j.State)
	}

	h.sched.OnJobUpdate(h.nodeID, &computev1.JobUpdate{
		JobId: jobID, State: computev1.JobState_JOB_STATE_FAILED, ExitCode: 2, Error: "boom",
	})
	j, _ := h.sched.Get(jobID)
	if j.State != computev1.JobState_JOB_STATE_FAILED || j.ExitCode != 2 || j.Error != "boom" {
		t.Errorf("terminal job = %+v, want FAILED exit 2 boom", j)
	}
}

func TestOnJobUpdateAdoptsUnknownJob(t *testing.T) {
	h := newHarness(t)
	// Simulates a coordinator restart: the agent reports a job we never saw.
	h.sched.OnJobUpdate(h.nodeID, &computev1.JobUpdate{
		JobId: "job-old", State: computev1.JobState_JOB_STATE_RUNNING,
	})
	j, ok := h.sched.Get("job-old")
	if !ok {
		t.Fatal("unknown job was not adopted")
	}
	if j.NodeID != h.nodeID || j.State != computev1.JobState_JOB_STATE_RUNNING {
		t.Errorf("adopted job = %+v", j)
	}
}

func TestReconcile(t *testing.T) {
	tests := []struct {
		name      string
		elapsed   time.Duration // time between dispatch and heartbeat
		activeIDs []string
		otherNode bool // heartbeat arrives from a different node
		wantState computev1.JobState
	}{
		{
			name:      "stale missing job fails",
			elapsed:   reconcileGrace + time.Second,
			activeIDs: nil,
			wantState: computev1.JobState_JOB_STATE_FAILED,
		},
		{
			name:      "within grace is untouched",
			elapsed:   reconcileGrace - time.Second,
			activeIDs: nil,
			wantState: computev1.JobState_JOB_STATE_PENDING,
		},
		{
			name:      "still reported is untouched",
			elapsed:   reconcileGrace + time.Second,
			activeIDs: []string{"job-fixed"},
			wantState: computev1.JobState_JOB_STATE_PENDING,
		},
		{
			name:      "other node's heartbeat is ignored",
			elapsed:   reconcileGrace + time.Second,
			activeIDs: nil,
			otherNode: true,
			wantState: computev1.JobState_JOB_STATE_PENDING,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			jobID, _ := h.sched.RunJob(h.nodeID, spec())

			h.now = h.now.Add(tt.elapsed)
			nodeID := h.nodeID
			if tt.otherNode {
				nodeID = "node-other"
			}
			h.sched.Reconcile(nodeID, tt.activeIDs)

			j, _ := h.sched.Get(jobID)
			if j.State != tt.wantState {
				t.Errorf("state = %v, want %v", j.State, tt.wantState)
			}
		})
	}
}

func TestReconcileLeavesTerminalJobsAlone(t *testing.T) {
	h := newHarness(t)
	jobID, _ := h.sched.RunJob(h.nodeID, spec())
	h.sched.OnJobUpdate(h.nodeID, &computev1.JobUpdate{
		JobId: jobID, State: computev1.JobState_JOB_STATE_SUCCEEDED,
	})

	h.now = h.now.Add(reconcileGrace + time.Minute)
	h.sched.Reconcile(h.nodeID, nil)

	if j, _ := h.sched.Get(jobID); j.State != computev1.JobState_JOB_STATE_SUCCEEDED {
		t.Errorf("state = %v, want SUCCEEDED untouched", j.State)
	}
}
