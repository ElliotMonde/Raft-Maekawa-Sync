// Manages worker subprocesses launched by the dashboard.
package dashboard

import (
	"fmt"
	"log"
	"os/exec"
	"sync"
)

// WorkerProcess tracks a single running worker subprocess.
type WorkerProcess struct {
	ID    int32
	Addr  string
	Peers string // raw "--peers=..." value
	Raft  string // raw "--raft=..." value
	Dash  string // dashboard addr to pass back to worker
	cmd   *exec.Cmd
}

// ProcessManager spawns and reaps worker binaries.
type ProcessManager struct {
	mu        sync.Mutex
	workerBin string
	procs     map[int32]*WorkerProcess
	dataDir   string
}

func NewProcessManager(workerBin, dataDir string) *ProcessManager {
	return &ProcessManager{
		workerBin: workerBin,
		dataDir:   dataDir,
		procs:     make(map[int32]*WorkerProcess),
	}
}

// StartNode launches a managed node process and returns immediately.
// ProcessManager only supports worker nodes.
func (pm *ProcessManager) StartNode(id int32, role, addr, peers, raftPeers, dashAddr string) error {
	if role != "worker" {
		return fmt.Errorf("process manager can only start worker nodes")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if p, ok := pm.procs[id]; ok && p.cmd != nil && p.cmd.ProcessState == nil {
		return fmt.Errorf("worker %d is already running", id)
	}

	args := []string{
		fmt.Sprintf("--id=%d", id),
		fmt.Sprintf("--addr=%s", addr),
		fmt.Sprintf("--data-dir=%s/%d", pm.dataDir, id),
	}
	if peers != "" {
		args = append(args, fmt.Sprintf("--peers=%s", peers))
	}
	if raftPeers != "" {
		args = append(args, fmt.Sprintf("--raft=%s", raftPeers))
	}
	if dashAddr != "" {
		args = append(args, fmt.Sprintf("--dashboard=%s", dashAddr))
	}

	cmd := exec.Command(pm.workerBin, args...)
	// Inherit stdout/stderr so worker logs appear in the dashboard's terminal.
	cmd.Stdout = log.Writer()
	cmd.Stderr = log.Writer()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker %d: %w", id, err)
	}

	pm.procs[id] = &WorkerProcess{ID: id, Addr: addr, Peers: peers, Raft: raftPeers, Dash: dashAddr, cmd: cmd}

	// Reap the process in the background so it doesn't become a zombie.
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("worker %d exited: %v", id, err)
		} else {
			log.Printf("worker %d exited cleanly", id)
		}
	}()

	log.Printf("dashboard: started worker %d at %s", id, addr)
	return nil
}

// Stop sends SIGTERM to the worker process.
func (pm *ProcessManager) Stop(id int32) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	p, ok := pm.procs[id]
	if !ok {
		return fmt.Errorf("worker %d is not managed by dashboard", id)
	}
	if p.cmd == nil {
		return fmt.Errorf("worker %d is not running", id)
	}
	if p.cmd.Process == nil {
		return fmt.Errorf("worker %d has no running process", id)
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill worker %d: %w", id, err)
	}
	p.cmd = nil
	return nil
}

func (pm *ProcessManager) StopNode(id int32) error {
	return pm.Stop(id)
}

// IsRunning reports whether worker id is currently managed and alive.
func (pm *ProcessManager) IsRunning(id int32) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	p, ok := pm.procs[id]
	return ok && p.cmd != nil && p.cmd.ProcessState == nil
}

func (pm *ProcessManager) IsManaged(id int32) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	_, ok := pm.procs[id]
	return ok
}

// ManagedIDs returns the IDs of all currently managed workers.
func (pm *ProcessManager) ManagedIDs() []int32 {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	ids := make([]int32, 0, len(pm.procs))
	for id := range pm.procs {
		ids = append(ids, id)
	}
	return ids
}

func (pm *ProcessManager) CanAddWorkers() bool {
	return true
}
