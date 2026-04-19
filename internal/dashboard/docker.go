package dashboard

import (
	"fmt"
	"os/exec"
	"sort"
	"strconv"
)

type DockerComposeManager struct {
	composeFile string
	projectName string
	services    map[int32]string
}

func NewDockerComposeManager(composeFile, projectName string, nodes []NodeConfig) *DockerComposeManager {
	services := make(map[int32]string, len(nodes))
	for _, node := range nodes {
		// Map node IDs to service names:
		// IDs 1-3: Raft nodes (raft-node1, raft-node2, raft-node3)
		// IDs 4-6: Worker nodes (worker1, worker2, worker3)
		if node.ID >= 1 && node.ID <= 3 {
			services[node.ID] = "raft-node" + strconv.Itoa(int(node.ID))
		} else {
			services[node.ID] = "worker" + strconv.Itoa(int(node.ID-3))
		}
	}
	return &DockerComposeManager{
		composeFile: composeFile,
		projectName: projectName,
		services:    services,
	}
}

func (m *DockerComposeManager) StartNode(id int32, role, addr, peers, raftPeers, dashAddr string) error {
	service, ok := m.services[id]
	if !ok {
		return fmt.Errorf("docker manager does not know node %d", id)
	}
	if out, err := m.runCompose("start", service); err != nil {
		return fmt.Errorf("docker compose start %s: %w: %s", service, err, out)
	}
	return nil
}

func (m *DockerComposeManager) StopNode(id int32) error {
	service, ok := m.services[id]
	if !ok {
		return fmt.Errorf("docker manager does not know node %d", id)
	}
	if out, err := m.runCompose("stop", service); err != nil {
		return fmt.Errorf("docker compose stop %s: %w: %s", service, err, out)
	}
	return nil
}

func (m *DockerComposeManager) IsManaged(id int32) bool {
	_, ok := m.services[id]
	return ok
}

func (m *DockerComposeManager) ManagedIDs() []int32 {
	ids := make([]int32, 0, len(m.services))
	for id := range m.services {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (m *DockerComposeManager) CanAddWorkers() bool {
	return false
}

func (m *DockerComposeManager) composeArgs(args ...string) []string {
	prefix := []string{"compose"}
	if m.composeFile != "" {
		prefix = append(prefix, "-f", m.composeFile)
	}
	if m.projectName != "" {
		prefix = append(prefix, "-p", m.projectName)
	}
	return append(prefix, args...)
}

func (m *DockerComposeManager) runCompose(args ...string) (string, error) {
	cmd := exec.Command("docker", m.composeArgs(args...)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), nil
	}

	legacyArgs := make([]string, 0, len(args)+4)
	if m.composeFile != "" {
		legacyArgs = append(legacyArgs, "-f", m.composeFile)
	}
	if m.projectName != "" {
		legacyArgs = append(legacyArgs, "-p", m.projectName)
	}
	legacyArgs = append(legacyArgs, args...)
	legacyCmd := exec.Command("docker-compose", legacyArgs...)
	legacyOut, legacyErr := legacyCmd.CombinedOutput()
	if legacyErr == nil {
		return string(legacyOut), nil
	}
	return string(out) + string(legacyOut), err
}
