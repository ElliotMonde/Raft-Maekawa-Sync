package maekawa

import (
	"raft-maekawa-sync/api/maekawa"
)

type ClientManager struct {
	clients map[int32]maekawa.MaekawaClient
}

func (cm *ClientManager) GetClient(id int32) maekawa.MaekawaClient {
	return cm.clients[id]
}