import { useRef, useEffect, useState } from 'react'
import { isWorkerNode, useStore, workerLabel, type DashEvent, type DashEventType } from '../store'

type Filter = 'all' | 'raft' | 'maekawa' | 'tasks'

const RAFT_TYPES = new Set<DashEventType>([
  'node_role', 'vote_request', 'vote_granted', 'heartbeat',
  'log_replicated', 'log_committed', 'node_down', 'node_up',
])
const TASK_TYPES = new Set<DashEventType>([
  'task_submitted', 'task_assigned', 'task_done', 'task_failed',
])
const MAEKAWA_TYPES = new Set<DashEventType>([
  'lock_request', 'lock_grant', 'lock_defer',
  'lock_inquire', 'lock_yield', 'lock_release',
  'cs_enter', 'cs_exit',
])

const COLOR: Partial<Record<DashEventType, string>> = {
  vote_request:   'var(--candidate)',
  vote_granted:   'var(--cs)',
  heartbeat:      'var(--accent)',
  log_replicated: 'var(--accent)',
  log_committed:  'var(--cs)',
  task_submitted: 'var(--text)',
  task_assigned:  'var(--waiting)',
  task_done:      'var(--cs)',
  task_failed:    'var(--danger)',
  lock_request:   'var(--danger)',
  lock_grant:     'var(--cs)',
  lock_defer:     'var(--leader)',
  lock_inquire:   'var(--purple)',
  lock_yield:     'var(--orange)',
  lock_release:   'var(--text-dim)',
  cs_enter:       'var(--cs)',
  cs_exit:        'var(--text-dim)',
  node_down:      'var(--danger)',
  node_up:        'var(--cs)',
}

function eventSummary(e: DashEvent): string {
  switch (e.type) {
    case 'node_role':      return `N${e.node_id} became ${e.role?.toUpperCase()} (term ${e.term})`
    case 'vote_request':   return `N${e.from} → N${e.to}: RequestVote (term ${e.term})`
    case 'vote_granted':   return `N${e.from} → N${e.to}: VoteGranted`
    case 'heartbeat':      return `N${e.from} → N${e.to}: Heartbeat`
    case 'log_replicated': return `N${e.node_id}: log[${e.log_index}] replicated`
    case 'log_committed':  return `N${e.node_id}: log[${e.log_index}] committed`
    case 'task_submitted': return `Task ${e.task_id?.slice(0, 10)} submitted`
    case 'task_assigned':  return `Task ${e.task_id?.slice(0, 10)} → ${workerLabel(e.node_id ?? -1)}`
    case 'task_done':      return `Task ${e.task_id?.slice(0, 10)} DONE`
    case 'task_failed':    return `Task ${e.task_id?.slice(0, 10)} FAILED`
    case 'lock_request':   return `${workerLabel(e.from ?? -1)} → ${workerLabel(e.to ?? -1)}: REQUEST (t=${e.timestamp})`
    case 'lock_grant':     return `${workerLabel(e.from ?? -1)} → ${workerLabel(e.to ?? -1)}: GRANT`
    case 'lock_defer':     return `${workerLabel(e.from ?? -1)} → ${workerLabel(e.to ?? -1)}: DEFER`
    case 'lock_inquire':   return `${workerLabel(e.from ?? -1)} → ${workerLabel(e.to ?? -1)}: INQUIRE`
    case 'lock_yield':     return `${workerLabel(e.from ?? -1)} → ${workerLabel(e.to ?? -1)}: YIELD`
    case 'lock_release':   return `${workerLabel(e.node_id ?? -1)}: RELEASE to quorum`
    case 'cs_enter':       return `${workerLabel(e.node_id ?? -1)}: ▶ ENTERED critical section`
    case 'cs_exit':        return `${workerLabel(e.node_id ?? -1)}: ◀ exited critical section`
    case 'node_down':      return `${isWorkerNode(e.node_id ?? -1) ? workerLabel(e.node_id ?? -1) : `N${e.node_id}`}: DOWN (unreachable)`
    case 'node_up':        return `${isWorkerNode(e.node_id ?? -1) ? workerLabel(e.node_id ?? -1) : `N${e.node_id}`}: UP (recovered)`
    case 'membership_change': return `Cluster membership changed`
    default:               return e.type
  }
}

function matchesFilter(e: DashEvent, filter: Filter): boolean {
  if (filter === 'all') return true
  if (filter === 'raft')    return RAFT_TYPES.has(e.type)
  if (filter === 'tasks')   return TASK_TYPES.has(e.type)
  if (filter === 'maekawa') return MAEKAWA_TYPES.has(e.type)
  return true
}

const FILTERS: { key: Filter; label: string }[] = [
  { key: 'all',     label: 'All' },
  { key: 'raft',    label: 'Raft' },
  { key: 'maekawa', label: 'Maekawa' },
  { key: 'tasks',   label: 'Tasks' },
]

// Format a wall-clock time from event arrival (we tag each event with local time)
const eventTimes = new WeakMap<DashEvent, string>()
function getTime(e: DashEvent): string {
  if (!eventTimes.has(e)) {
    eventTimes.set(e, new Date().toLocaleTimeString('en', { hour12: false }))
  }
  return eventTimes.get(e)!
}

export function EventLog() {
  const events = useStore(s => s.events)
  const [filter, setFilter] = useState<Filter>('all')
  const [pinned, setPinned] = useState(true)  // auto-scroll to bottom
  const bodyRef = useRef<HTMLDivElement>(null)

  const visible = events.filter(e => matchesFilter(e, filter))

  // Auto-scroll to newest (top, since list is newest-first)
  useEffect(() => {
    if (pinned && bodyRef.current) {
      bodyRef.current.scrollTop = 0
    }
  }, [events.length, pinned])

  return (
    <div className="event-log">
      <div className="event-log-header">
        <span className="section-label">Event Log</span>
        <span style={{ color: 'var(--text-dim)', fontSize: 10, marginLeft: 4 }}>({visible.length})</span>
        <div style={{ display: 'flex', gap: 4, marginLeft: 'auto' }}>
          {FILTERS.map(f => (
            <button
              key={f.key}
              className={filter === f.key ? 'active' : ''}
              style={{ padding: '2px 8px', fontSize: 10 }}
              onClick={() => setFilter(f.key)}
            >
              {f.label}
            </button>
          ))}
          <button
            title={pinned ? 'Auto-scroll ON' : 'Auto-scroll OFF'}
            style={{ padding: '2px 8px', fontSize: 10, color: pinned ? 'var(--cs)' : 'var(--text-dim)' }}
            onClick={() => setPinned(p => !p)}
          >
            {pinned ? '⬆ live' : '⏸ paused'}
          </button>
        </div>
      </div>

      <div className="event-log-body" ref={bodyRef}>
        {visible.map((e, i) => (
          <div key={i} className="log-row">
            <span className="log-time">{getTime(e)}</span>
            <span className="log-type" style={{ color: COLOR[e.type] ?? 'var(--text-dim)' }}>
              [{e.type}]
            </span>
            <span className="log-msg">{eventSummary(e)}</span>
          </div>
        ))}
        {visible.length === 0 && (
          <div style={{ padding: '8px 12px', color: 'var(--text-dim)', fontSize: 11 }}>
            No events yet — run a scenario or submit a task.
          </div>
        )}
      </div>
    </div>
  )
}
