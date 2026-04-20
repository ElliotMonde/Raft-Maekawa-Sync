package dashboard

type NodeManager interface {
	StartNode(id int32, role, addr, peers, raftPeers, dashAddr string) error
	StopNode(id int32) error
	IsManaged(id int32) bool
	ManagedIDs() []int32
	CanAddWorkers() bool
}
