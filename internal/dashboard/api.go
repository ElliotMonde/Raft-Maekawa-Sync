// WebSocket hub and HTTP handlers for the dashboard frontend.
package dashboard

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// DashEvent is the JSON envelope pushed to every connected browser client.
type DashEvent struct {
	Type      string `json:"type"`
	From      int32  `json:"from,omitempty"`
	To        int32  `json:"to,omitempty"`
	NodeID    int32  `json:"node_id,omitempty"`
	Role      string `json:"role,omitempty"`
	Term      int64  `json:"term,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	LogIndex  int64  `json:"log_index,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Payload   any    `json:"payload,omitempty"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// Hub broadcasts DashEvents to all connected WebSocket clients.
type Hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

func NewHub() *Hub { return &Hub{clients: make(map[*websocket.Conn]struct{})} }

func (h *Hub) Broadcast(e DashEvent) {
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if err := c.WriteMessage(websocket.TextMessage, b); err != nil {
			log.Printf("ws write: %v", err)
		}
	}
}

// ServeMaekawaEvent accepts POST /api/maekawa-event from worker nodes and
// broadcasts the event to all connected WebSocket clients.
// ServeNodes handles GET /api/nodes — returns the list of all known nodes.
func ServeNodes(c *Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type nodeInfo struct {
			ID      int32  `json:"id"`
			Addr    string `json:"addr"`
			Managed bool   `json:"managed"` // true if launched by dashboard
		}
		type response struct {
			Nodes         []nodeInfo `json:"nodes"`
			CanAddWorkers bool       `json:"can_add_workers"`
		}
		nodes := c.NodeList()
		out := make([]nodeInfo, 0, len(nodes))
		for _, n := range nodes {
			managed := c.Manager != nil && c.Manager.IsManaged(n.ID)
			out = append(out, nodeInfo{ID: n.ID, Addr: n.Addr, Managed: managed})
		}
		canAddWorkers := c.Manager != nil && c.Manager.CanAddWorkers()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response{
			Nodes:         out,
			CanAddWorkers: canAddWorkers,
		})
	}
}

func (h *Hub) ServeMaekawaEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var e DashEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	h.Broadcast(e)
	w.WriteHeader(http.StatusNoContent)
}

// actionRequest is the JSON body accepted by POST /api/action.
type actionRequest struct {
	Action    string `json:"action"`     // "submit_task" | "submit_task_once" | "kill_node" | "start_node" | "stop_node"
	Data      string `json:"data"`       // task payload for submit_task
	NodeID    int32  `json:"node_id"`    // target node for kill_node / stop_node
	NodeAddr  string `json:"node_addr"`  // e.g. "127.0.0.1:5004" for start_node
	NodePeers string `json:"node_peers"` // kept for backward compatibility; ignored by generic start
	DashAddr  string `json:"dash_addr"`  // dashboard addr forwarded to started worker nodes
}

// actionResponse is returned by POST /api/action.
type actionResponse struct {
	OK     bool   `json:"ok"`
	TaskID string `json:"task_id,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ServeAction handles POST /api/action from the frontend
func ServeAction(c *Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req actionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch req.Action {
		case "submit_task":
			taskID, err := c.SubmitTask(context.Background(), req.Data)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				enc.Encode(actionResponse{Error: err.Error()})
				return
			}
			enc.Encode(actionResponse{OK: true, TaskID: taskID})

		case "submit_task_once":
			taskID, err := c.SubmitTaskOnce(context.Background(), req.NodeID, req.Data)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				enc.Encode(actionResponse{Error: err.Error()})
				return
			}
			enc.Encode(actionResponse{OK: true, TaskID: taskID})

		case "kill_node":
			if err := c.KillNode(req.NodeID); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				enc.Encode(actionResponse{Error: err.Error()})
				return
			}
			enc.Encode(actionResponse{OK: true})

		case "start_node", "start_worker":
			if err := c.StartNode(req.NodeID, req.NodeAddr, req.DashAddr); err != nil {
				w.WriteHeader(http.StatusBadGateway)
				enc.Encode(actionResponse{Error: err.Error()})
				return
			}
			enc.Encode(actionResponse{OK: true})

		case "stop_node", "stop_worker":
			if err := c.StopNode(req.NodeID); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				enc.Encode(actionResponse{Error: err.Error()})
				return
			}
			enc.Encode(actionResponse{OK: true})

		default:
			w.WriteHeader(http.StatusBadRequest)
			enc.Encode(actionResponse{Error: "unknown action: " + req.Action})
		}
	}
}

func (h *Hub) ServeWS(c *Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("ws upgrade: %v", err)
			return
		}
		h.mu.Lock()
		h.clients[conn] = struct{}{}
		h.mu.Unlock()

		if c != nil {
			b, err := json.Marshal(DashEvent{
				Type:    "snapshot",
				Payload: c.SnapshotPayload(),
			})
			if err == nil {
				if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
					log.Printf("ws snapshot write: %v", err)
				}
			}
		}

		// Drain incoming messages (clients don't send anything meaningful yet)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}

		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
	}
}
