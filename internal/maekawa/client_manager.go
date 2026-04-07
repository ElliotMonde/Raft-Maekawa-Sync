package maekawa

import (
    "log"
    "sync"

    maekawapb "raft-maekawa-sync/api/maekawa"
    "raft-maekawa-sync/internal/rpc"
)

type ClientManager struct {
    mu      sync.RWMutex
    clients map[int32]maekawapb.MaekawaClient
}

func NewClientManager() *ClientManager {
    return &ClientManager{
        clients: make(map[int32]maekawapb.MaekawaClient),
    }
}

func (cm *ClientManager) GetClient(id int32) maekawapb.MaekawaClient {
    cm.mu.RLock()
    defer cm.mu.RUnlock()
    return cm.clients[id]
}

func (cm *ClientManager) RegisterClient(id int32, client maekawapb.MaekawaClient) {
    cm.mu.Lock()
    defer cm.mu.Unlock()
    cm.clients[id] = client
}

func (cm *ClientManager) InitClients(peers map[int32]string, selfID int32) error {
    for id, addr := range peers {
        if id == selfID {
            continue
        }
        conn, err := rpc.Dial(addr)
        if err != nil {
            log.Printf("Failed to connect to peer %d at %s: %v", id, addr, err)
            return err
        }
        cm.RegisterClient(id, maekawapb.NewMaekawaClient(conn))
    }
    return nil
}