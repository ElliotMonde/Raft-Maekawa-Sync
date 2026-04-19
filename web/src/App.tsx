import { useState, useEffect, useCallback } from 'react'
import { useWebSocket } from './useWebSocket'
import { useStore } from './store'
import { RaftView } from './components/RaftView'
import { MaekawaView } from './components/MaekawaView'
import { EventLog } from './components/EventLog'
import { ScenarioRunner } from './components/ScenarioRunner'
import { CrossPanelOverlay } from './components/CrossPanelOverlay'
import { NodePositionProvider } from './NodePositionContext'
import { submitTask, killNode, startNode, fetchNodes, type NodeInfo } from './api'

// ── Sidebar tab ──────────────────────────────────────────────────────────────
type Tab = 'scenarios' | 'controls'

// ── Toast ────────────────────────────────────────────────────────────────────
function useToast() {
  const [toast, setToast] = useState<{ msg: string; err: boolean } | null>(null)
  const flash = useCallback((msg: string, err = false) => {
    setToast({ msg, err })
    setTimeout(() => setToast(null), 3500)
  }, [])
  return { toast, flash }
}

// ── Node row control ──────────────────────────────────────────────────────────
function NodeRow({
  label, isDown, addr,
  canStart = true,
  onKill, onStart,
}: {
  label: string; isDown: boolean; addr?: string;
  canStart?: boolean;
  onKill: () => void; onStart: () => void;
}) {
  return (
    <div className="node-row">
      <span className="node-row-label">{label}</span>
      <span className={`badge ${isDown ? 'down' : 'follower'}`}>{isDown ? 'down' : 'up'}</span>
      {!isDown ? (
        <button className="danger" style={{ padding: '2px 8px', fontSize: 10 }} onClick={onKill}>
          kill
        </button>
      ) : canStart && addr ? (
        <button className="success" style={{ padding: '2px 8px', fontSize: 10 }} onClick={onStart}>
          start
        </button>
      ) : !canStart ? (
        <span style={{ fontSize: 10, color: 'var(--text-dim)' }}>restart outside UI</span>
      ) : (
        <span style={{ fontSize: 10, color: 'var(--text-dim)' }}>no addr</span>
      )}
    </div>
  )
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  useWebSocket()

  const nodes       = useStore(s => s.nodes)
  const wsConnected = useStore(s => s.wsConnected)
  const speed       = useStore(s => s.speed)
  const setSpeed    = useStore(s => s.setSpeed)
  const reset       = useStore(s => s.reset)

  const { toast, flash } = useToast()
  const [tab, setTab] = useState<Tab>('scenarios')
  const [apiNodes, setApiNodes]       = useState<NodeInfo[]>([])
  const [nodeAddresses, setAddresses] = useState<Map<number, string>>(new Map())

  // Refresh node list when cluster changes
  async function refreshNodes() {
    try {
      const res = await fetchNodes()
      setApiNodes(res.nodes)
      setAddresses(prev => {
        const next = new Map(prev)
        for (const n of res.nodes) next.set(n.id, n.addr)
        return next
      })
    } catch { /* ignore network errors */ }
  }

  useEffect(() => { refreshNodes() }, [nodes.size])

  // ── Actions ──────────────────────────────────────────────────────────────
  async function handleKill(id: number) {
    flash(`Killing node ${id}…`)
    const r = await killNode(id)
    flash(r.ok ? `✓ Node ${id} killed` : `✗ ${r.error}`, !r.ok)
    refreshNodes()
  }

  async function handleStart(id: number) {
    const addr = nodeAddresses.get(id)
    if (!addr) { flash('✗ No address known for this node', true); return }
    flash(`Starting node ${id}…`)
    const r = await startNode(id, addr, window.location.host)
    flash(r.ok ? `✓ Node ${id} started` : `✗ ${r.error}`, !r.ok)
    refreshNodes()
  }

  async function handleSubmit(data: string) {
    if (!data.trim()) return
    flash('Submitting…')
    const r = await submitTask(data)
    flash(r.ok ? `✓ ${r.task_id?.slice(0, 10)} queued` : `✗ ${r.error}`, !r.ok)
  }

  // ── Derived state ────────────────────────────────────────────────────────
  const raftIds   = [1, 2, 3]
  const workerIds = [4, 5, 6]
  const managedIds = new Set(apiNodes.filter(n => n.managed).map(n => n.id))

  return (
    <NodePositionProvider>
    <div className="app-layout">

      {/* ── Sidebar ──────────────────────────────────────────────────────── */}
      <aside className="sidebar">

        {/* Header */}
        <div style={{ padding: '12px 12px 8px', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 14, fontWeight: 800, color: 'var(--accent)', letterSpacing: '-0.02em' }}>
              Raft · Maekawa
            </span>
            <span
              className={`ws-dot ${wsConnected ? 'connected' : 'disconnected'}`}
              style={{ marginLeft: 'auto' }}
              title={wsConnected ? 'WebSocket connected' : 'Disconnected'}
            />
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-dim)', marginTop: 2 }}>
            Distributed Systems Demo
          </div>
        </div>

        {/* Tab switcher */}
        <div style={{ display: 'flex', padding: '6px 8px', gap: 4, flexShrink: 0, borderBottom: '1px solid var(--border)' }}>
          {(['scenarios', 'controls'] as Tab[]).map(t => (
            <button
              key={t}
              className={tab === t ? 'active' : ''}
              style={{ flex: 1, padding: '4px', fontSize: 11, fontWeight: 600, textTransform: 'capitalize' }}
              onClick={() => setTab(t)}
            >
              {t}
            </button>
          ))}
        </div>

        {/* Scrollable body */}
        <div className="sidebar-scroll">
          {tab === 'scenarios' ? (
            <>
              <p className="section-label" style={{ marginBottom: 2 }}>Demo Scenarios</p>
              <ScenarioRunner flash={flash} />
            </>
          ) : (
            <>
              {/* ── Quick submit ── */}
              <p className="section-label">Submit Task</p>
              <TaskInput onSubmit={handleSubmit} />
              <div style={{ display: 'flex', gap: 4 }}>
                <button style={{ flex: 1, fontSize: 10 }} onClick={() => Promise.all(Array.from({length: 3}, (_, i) => submitTask(`batch-${i+1}`)).map(() => flash('3 tasks submitted')))}>
                  × 3
                </button>
                <button style={{ flex: 1, fontSize: 10 }} onClick={() => Promise.all(Array.from({length: 5}, (_, i) => submitTask(`batch-${i+1}`)).map(() => flash('5 tasks submitted')))}>
                  × 5
                </button>
              </div>

              <hr className="divider" />

              {/* ── Animation speed ── */}
              <p className="section-label">Animation Speed</p>
              <div className="speed-row">
                {([0.5, 1, 2] as const).map(v => (
                  <button key={v} className={speed === v ? 'active' : ''} onClick={() => setSpeed(v)}>
                    {v}×
                  </button>
                ))}
              </div>

              <button onClick={reset} style={{ fontSize: 11 }}>↺ Reset View</button>

              <hr className="divider" />

              {/* ── Raft nodes ── */}
              <p className="section-label">Raft Nodes</p>
              {raftIds.map(id => {
                const node = nodes.get(id)
                const isDown = node?.role === 'down'
                return (
                  <NodeRow
                    key={id}
                    label={`N${id}${node?.role === 'leader' ? ' (leader)' : node?.role === 'candidate' ? ' (cand.)' : ''}`}
                    isDown={!!isDown}
                    addr={nodeAddresses.get(id)}
                    canStart={managedIds.has(id)}
                    onKill={() => handleKill(id)}
                    onStart={() => handleStart(id)}
                  />
                )
              })}

              <hr className="divider" />

              {/* ── Workers ── */}
              <p className="section-label">Maekawa Workers</p>
              {workerIds.map(id => {
                const node = nodes.get(id)
                const isDown = !node || node.role === 'down'
                return (
                  <NodeRow
                    key={id}
                    label={`W${id - 3}${node?.in_cs ? ' (CS)' : ''}`}
                    isDown={isDown}
                    addr={nodeAddresses.get(id)}
                    canStart={managedIds.has(id)}
                    onKill={() => handleKill(id)}
                    onStart={() => handleStart(id)}
                  />
                )
              })}

              <hr className="divider" />

              {/* ── Lamport clocks ── */}
              <p className="section-label">Lamport Clocks</p>
              {workerIds.map(id => {
                const node = nodes.get(id)
                if (!node || node.role === 'down') return null
                return (
                  <div key={id} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, padding: '1px 0' }}>
                    <span style={{ color: 'var(--text-dim)' }}>W{id - 3}</span>
                    <span style={{ color: 'var(--accent)', fontFamily: 'var(--mono)' }}>t={node.clock}</span>
                  </div>
                )
              })}
            </>
          )}
        </div>

        {/* Toast */}
        {toast && (
          <div style={{ padding: '8px 12px', flexShrink: 0, borderTop: '1px solid var(--border)' }}>
            <div className={`toast ${toast.err ? 'err' : 'ok'}`}>{toast.msg}</div>
          </div>
        )}
      </aside>

      {/* ── Main canvas ──────────────────────────────────────────────────── */}
      <main className="main-area">
        <div className="canvas-split">

          {/* Left: Raft */}
          <div className="canvas-pane">
            <div className="pane-label">
              Raft Cluster
              {(() => {
                const leader = Array.from(nodes.values()).find(n => n.role === 'leader')
                return leader ? ` · Leader: N${leader.id} term ${leader.term}` : ' · No leader'
              })()}
            </div>
            <RaftView onKill={handleKill} />
          </div>

          {/* Right: Maekawa */}
          <div className="canvas-pane">
            <div className="pane-label">
              Maekawa Workers
              {(() => {
                const cs = Array.from(nodes.values()).find(n => n.in_cs)
                return cs ? ` · W${cs.id - 3} in CS` : ''
              })()}
            </div>
            <MaekawaView onKill={handleKill} />
          </div>
        </div>

        {/* Event log */}
        <EventLog />
      </main>

      {/* Cross-panel arrow overlay — sits above both SVG panes */}
      <CrossPanelOverlay />
    </div>
    </NodePositionProvider>
  )
}

// ── TaskInput ─────────────────────────────────────────────────────────────────
function TaskInput({ onSubmit }: { onSubmit: (data: string) => void }) {
  return (
    <form
      onSubmit={e => {
        e.preventDefault()
        const fd = new FormData(e.currentTarget)
        const val = (fd.get('data') as string).trim()
        if (val) { onSubmit(val); e.currentTarget.reset() }
      }}
      style={{ display: 'flex', flexDirection: 'column', gap: 4 }}
    >
      <input type="text" name="data" placeholder="e.g. render_frame_042" />
      <button type="submit" style={{ fontSize: 11 }}>Submit Task</button>
    </form>
  )
}
