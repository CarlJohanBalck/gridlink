// Package scheduler dispatches jobs to nodes. Phase 1 is deliberately dumb:
// the admin names a node, we generate a job ID, and forward the spec down
// that node's stream. Real matching/bidding arrives in Phase 2+.
package scheduler

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	computev1 "gridlink/contracts/gen/compute/v1"
	"gridlink/coordinator/internal/registry"

	"github.com/google/uuid"
)

var (
	ErrUnknownNode = errors.New("unknown node")
	ErrNodeOffline = errors.New("node is offline")
)

// reconcileGrace is how stale a non-terminal job must be before a heartbeat
// that omits it marks it FAILED. The window absorbs the race where a job was
// dispatched after the agent sampled active_job_ids for that heartbeat.
const reconcileGrace = 30 * time.Second

// Job is the coordinator's view of one dispatched job.
type Job struct {
	ID         string
	NodeID     string
	State      computev1.JobState
	LastUpdate time.Time
	// Error and ExitCode from the most recent terminal update, if any.
	Error    string
	ExitCode int32
}

type Scheduler struct {
	reg *registry.Registry
	log *slog.Logger

	mu   sync.Mutex
	jobs map[string]*Job

	// now and newID are overridable in tests.
	now   func() time.Time
	newID func() string
}

func New(reg *registry.Registry, log *slog.Logger) *Scheduler {
	return &Scheduler{
		reg:   reg,
		log:   log,
		jobs:  make(map[string]*Job),
		now:   time.Now,
		newID: uuid.NewString,
	}
}

// RunJob assigns a job ID, records the job as PENDING, and forwards the spec
// down the named node's stream.
func (s *Scheduler) RunJob(nodeID string, spec *computev1.JobSpec) (string, error) {
	node, ok := s.reg.Get(nodeID)
	if !ok {
		return "", fmt.Errorf("node %s: %w", nodeID, ErrUnknownNode)
	}
	if node.Status != computev1.NodeStatus_NODE_STATUS_ONLINE || node.Send == nil {
		return "", fmt.Errorf("node %s: %w", nodeID, ErrNodeOffline)
	}

	jobID := s.newID()
	spec.JobId = jobID

	// Record before sending so an update racing back cannot miss the table.
	s.mu.Lock()
	s.jobs[jobID] = &Job{
		ID:         jobID,
		NodeID:     nodeID,
		State:      computev1.JobState_JOB_STATE_PENDING,
		LastUpdate: s.now(),
	}
	s.mu.Unlock()

	if err := node.Send(&computev1.CoordinatorMessage{
		Payload: &computev1.CoordinatorMessage_RunJob{
			RunJob: &computev1.RunJob{Spec: spec},
		},
	}); err != nil {
		s.mu.Lock()
		delete(s.jobs, jobID)
		s.mu.Unlock()
		return "", fmt.Errorf("send job to node %s: %w", nodeID, err)
	}

	s.log.Info("job dispatched", "job_id", jobID, "node_id", nodeID, "image", spec.GetImage())
	return jobID, nil
}

// OnJobUpdate records a status/log update from an agent. Updates for jobs the
// scheduler does not know (dispatched before a coordinator restart) are
// adopted rather than dropped — the agent is the source of truth for what it
// is running.
func (s *Scheduler) OnJobUpdate(nodeID string, upd *computev1.JobUpdate) {
	if upd.GetJobId() == "" {
		return
	}

	s.mu.Lock()
	j, ok := s.jobs[upd.GetJobId()]
	if !ok {
		j = &Job{ID: upd.GetJobId(), NodeID: nodeID}
		s.jobs[j.ID] = j
	}
	prev := j.State
	if upd.GetState() != computev1.JobState_JOB_STATE_UNSPECIFIED {
		j.State = upd.GetState()
	}
	if isTerminal(upd.GetState()) {
		j.Error = upd.GetError()
		j.ExitCode = upd.GetExitCode()
	}
	j.LastUpdate = s.now()
	s.mu.Unlock()

	log := s.log.With("job_id", j.ID, "node_id", nodeID)
	if upd.GetState() != prev {
		log.Info("job state changed",
			"state", upd.GetState(), "exit_code", upd.GetExitCode(), "error", upd.GetError())
	}
	// Job output is only observable through the coordinator log in Phase 1.
	if chunk := strings.TrimRight(upd.GetLogChunk(), "\n"); chunk != "" {
		log.Info("job log", "chunk", chunk)
	}
}

// Reconcile compares a heartbeat's active_job_ids against the job table:
// a job the coordinator thinks is live, that the agent stopped reporting, and
// that has been quiet past the grace window, is marked FAILED (the agent
// forgot it — e.g. it restarted and the terminal update was lost).
func (s *Scheduler) Reconcile(nodeID string, activeJobIDs []string) {
	active := make(map[string]bool, len(activeJobIDs))
	for _, id := range activeJobIDs {
		active[id] = true
	}
	cutoff := s.now().Add(-reconcileGrace)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.NodeID != nodeID || isTerminal(j.State) || active[j.ID] {
			continue
		}
		if j.LastUpdate.After(cutoff) {
			continue
		}
		j.State = computev1.JobState_JOB_STATE_FAILED
		j.Error = "agent no longer reports this job"
		j.LastUpdate = s.now()
		s.log.Warn("job lost by agent, marking FAILED", "job_id", j.ID, "node_id", nodeID)
	}
}

// Get returns a snapshot copy of one job.
func (s *Scheduler) Get(jobID string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

func isTerminal(st computev1.JobState) bool {
	switch st {
	case computev1.JobState_JOB_STATE_SUCCEEDED,
		computev1.JobState_JOB_STATE_FAILED,
		computev1.JobState_JOB_STATE_CANCELLED:
		return true
	default:
		return false
	}
}
