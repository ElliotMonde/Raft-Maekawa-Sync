import { create } from 'zustand'

export type NodeRole = 'follower' | 'candidate' | 'leader' | 'worker' | 'down'
export type TaskStatus = 'pending' | 'in_progress' | 'completed' | 'failed'

export type DashEventType =
  | 'node_role' | 'vote_request' | 'vote_granted' | 'heartbeat'
  | 'log_replicated' | 'log_committed'
  | 'task_submitted' | 'task_assigned' | 'task_done' | 'task_failed'
  | 'lock_request' | 'lock_grant' | 'lock_defer'
  | 'lock_inquire' | 'lock_yield' | 'lock_release'
  | 'cs_enter' | 'cs_exit'
  | 'node_down' | 'node_up'
  | 'membership_change' | 'snapshot'

export interface DashEvent {
  type: DashEventType
  from?: number
  to?: number
  node_id?: number
  role?: NodeRole
  term?: number
  task_id?: string
  log_index?: number
  timestamp?: number   // Lamport clock
  payload?: unknown
}

export interface NodeState {
  id: number
  role: NodeRole
  term: number
  log_length: number
  commit_index: number
  clock: number
  in_cs: boolean
  quorum: number[]
  voted_for: number
}

export interface TaskState {
  id: string
  data: string
  status: TaskStatus
  assigned_to: number
}

export interface FlyingMessage {
  id: string
  type: DashEventType
  from: number
  to: number
  startedAt: number
}

// Compute Maekawa quorum for a node given sorted active worker IDs
// Uses the grid algorithm: quorum = same row + same column (without duplicate)
export function computeQuorum(selfId: number, activeWorkerIds: number[]): number[] {
  const sorted = [...activeWorkerIds].sort((a, b) => a - b)
  const n = sorted.length
  if (n === 0) return []
  const cols = Math.ceil(Math.sqrt(n))
  const myPos = sorted.indexOf(selfId)
  if (myPos === -1) return []
  const myRow = Math.floor(myPos / cols)
  const myCol = myPos % cols
  const quorum = new Set<number>()
  // same row
  for (let c = 0; c < cols; c++) {
    const pos = myRow * cols + c
    if (pos < n) quorum.add(sorted[pos])
  }
  // same column
  for (let r = 0; r < Math.ceil(n / cols); r++) {
    const pos = r * cols + myCol
    if (pos < n) quorum.add(sorted[pos])
  }
  return Array.from(quorum)
}

interface AppStore {
  nodes: Map<number, NodeState>
  tasks: Map<string, TaskState>
  events: DashEvent[]
  flying: FlyingMessage[]
  wsConnected: boolean
  speed: number

  setSpeed: (v: number) => void
  applyEvent: (e: DashEvent) => void
  addFlying: (msg: FlyingMessage) => void
  removeFlying: (id: string) => void
  setWsConnected: (v: boolean) => void
  reset: () => void
}

export function isRaftNode(id: number): boolean {
  return id >= 1 && id <= 3
}

export function isWorkerNode(id: number): boolean {
  return id >= 4
}

export function workerDisplayId(id: number): number {
  return isWorkerNode(id) ? id - 3 : id
}

export function workerLabel(id: number): string {
  return `W${workerDisplayId(id)}`
}

function defaultRoleForNode(id: number): NodeRole {
  return isWorkerNode(id) ? 'worker' : 'follower'
}

function makeDefaultNode(id: number): NodeState {
  return { id, role: defaultRoleForNode(id), term: 0, log_length: 0, commit_index: 0, clock: 0, in_cs: false, quorum: [], voted_for: -1 }
}

function defaultNodes(): Map<number, NodeState> {
  const m = new Map<number, NodeState>()
  for (const id of [1, 2, 3, 4, 5, 6]) m.set(id, makeDefaultNode(id))
  return m
}

// Recompute quorums for all worker nodes whenever membership changes
function recomputeWorkerQuorums(nodes: Map<number, NodeState>): void {
  const workerIds = Array.from(nodes.values())
    .filter(n => n.id >= 4 && n.role !== 'down')
    .map(n => n.id)
  for (const [id, node] of nodes) {
    if (id >= 4) {
      nodes.set(id, { ...node, quorum: computeQuorum(id, workerIds) })
    }
  }
}

export const useStore = create<AppStore>((set, get) => ({
  nodes: defaultNodes(),
  tasks: new Map(),
  events: [],
  flying: [],
  wsConnected: false,
  speed: 1,

  setSpeed: (v) => set({ speed: v }),
  setWsConnected: (v) => set({ wsConnected: v }),

  addFlying: (msg) => set((st) => ({ flying: [...st.flying, msg] })),
  removeFlying: (id) => set((st) => ({ flying: st.flying.filter(f => f.id !== id) })),

  reset: () => set({ nodes: defaultNodes(), tasks: new Map(), events: [], flying: [] }),

  applyEvent: (e) => {
    set((st) => {
      const nodes = new Map(st.nodes)
      const tasks = new Map(st.tasks)
      const events = [e, ...st.events].slice(0, 500)
      let membershipChanged = false

      switch (e.type) {
        case 'snapshot': {
          const snap = e.payload as { nodes: NodeState[]; tasks: TaskState[] }
          if (snap?.nodes) {
            for (const n of snap.nodes) {
              const existing = nodes.get(n.id)
              nodes.set(n.id, { ...n, clock: existing?.clock && existing.clock > 0 ? existing.clock : n.clock })
            }
            membershipChanged = true
          }
          if (snap?.tasks) for (const t of snap.tasks) tasks.set(t.id, t)
          break
        }

        case 'node_role': {
          if (e.node_id != null) {
            const prev = nodes.get(e.node_id) ?? makeDefaultNode(e.node_id)
            const wasDown = prev.role === 'down'
            nodes.set(e.node_id, { ...prev, role: e.role ?? prev.role, term: e.term ?? prev.term })
            if (wasDown) membershipChanged = true
          }
          break
        }

        case 'node_down': {
          if (e.node_id != null) {
            const prev = nodes.get(e.node_id)
            if (prev) {
              nodes.set(e.node_id, { ...prev, role: 'down', in_cs: false })
              membershipChanged = true
            }
          }
          break
        }

        case 'node_up': {
          if (e.node_id != null) {
            const prev = nodes.get(e.node_id) ?? makeDefaultNode(e.node_id)
            nodes.set(e.node_id, { ...prev, role: defaultRoleForNode(e.node_id) })
            membershipChanged = true
          }
          break
        }

        case 'log_replicated': {
          if (e.node_id != null && e.log_index != null) {
            const prev = nodes.get(e.node_id)
            if (prev) nodes.set(e.node_id, { ...prev, log_length: Math.max(prev.log_length, e.log_index) })
          }
          break
        }

        case 'log_committed': {
          if (e.node_id != null && e.log_index != null) {
            const prev = nodes.get(e.node_id)
            if (prev) nodes.set(e.node_id, { ...prev, commit_index: Math.max(prev.commit_index, e.log_index) })
          }
          break
        }

        case 'task_submitted': {
          if (e.task_id) tasks.set(e.task_id, { id: e.task_id, data: e.task_id.slice(0, 12), status: 'pending', assigned_to: -1 })
          break
        }

        case 'task_assigned': {
          if (e.task_id) {
            const prev = tasks.get(e.task_id) ?? { id: e.task_id, data: '', status: 'pending' as TaskStatus, assigned_to: -1 }
            tasks.set(e.task_id, { ...prev, status: 'in_progress', assigned_to: e.node_id ?? -1 })
          }
          break
        }

        case 'task_done': {
          if (e.task_id) {
            const prev = tasks.get(e.task_id)
            if (prev) tasks.set(e.task_id, { ...prev, status: 'completed' })
          }
          // Safety net: clear in_cs for the worker that finished — cs_exit may have been missed
          if (e.node_id != null && e.node_id >= 4) {
            const prev = nodes.get(e.node_id)
            if (prev?.in_cs) nodes.set(e.node_id, { ...prev, in_cs: false })
          }
          break
        }

        case 'task_failed': {
          if (e.task_id) {
            const prev = tasks.get(e.task_id)
            if (prev) tasks.set(e.task_id, { ...prev, status: 'failed' })
          }
          break
        }

        case 'cs_enter': {
          if (e.node_id != null) {
            const prev = nodes.get(e.node_id)
            if (prev) nodes.set(e.node_id, { ...prev, in_cs: true, clock: e.timestamp ?? prev.clock })
          }
          break
        }

        case 'cs_exit': {
          if (e.node_id != null) {
            const prev = nodes.get(e.node_id)
            if (prev) nodes.set(e.node_id, { ...prev, in_cs: false, clock: e.timestamp ?? prev.clock })
          }
          break
        }

        case 'lock_request':
        case 'lock_grant':
        case 'lock_defer':
        case 'lock_inquire':
        case 'lock_yield':
        case 'lock_release': {
          // Update Lamport clock for the source node
          const clockNode = e.from ?? e.node_id
          if (clockNode != null && e.timestamp != null) {
            const prev = nodes.get(clockNode)
            if (prev) nodes.set(clockNode, { ...prev, clock: e.timestamp })
          }
          break
        }

        case 'vote_request':
        case 'vote_granted': {
          // Update voted_for tracking
          if (e.type === 'vote_granted' && e.from != null && e.to != null) {
            const voter = nodes.get(e.from)
            if (voter) nodes.set(e.from, { ...voter, voted_for: e.to })
          }
          break
        }
      }

      if (membershipChanged) recomputeWorkerQuorums(nodes)

      // Emit flying message for any event with both from and to
      const flyingSrc = e.from ?? e.node_id
      const flyingDst = e.to
      const fly = get().flying
      if (flyingSrc != null && flyingDst != null && flyingSrc !== flyingDst) {
        const flying: FlyingMessage = {
          id: `${Date.now()}-${Math.random()}`,
          type: e.type,
          from: flyingSrc,
          to: flyingDst,
          startedAt: Date.now(),
        }
        return { nodes, tasks, events, flying: [...fly, flying] }
      }

      return { nodes, tasks, events }
    })
  },
}))
